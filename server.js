const express = require('express');
const http = require('http');
const path = require('path');
const { Server } = require('socket.io');
const { Client, LocalAuth } = require('whatsapp-web.js');
const qrcode = require('qrcode');

const app = express();
const server = http.createServer(app);
const io = new Server(server);

const PORT = process.env.PORT || 3000;

// Serve static files from the 'public' directory
app.use(express.static(path.join(__dirname, 'public')));

app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// Prevent Node.js from crashing if Puppeteer throws unexpected internal errors
process.on('unhandledRejection', error => {
    console.error('Unhandled Promise Rejection:', error);
});
process.on('uncaughtException', error => {
    console.error('Uncaught Exception:', error);
});

const client = new Client({
    authStrategy: new LocalAuth(),
    puppeteer: {
        // CRITICAL FOR RAILWAY: Use the Chromium path provided by Nixpacks, if available
        executablePath: process.env.PUPPETEER_EXECUTABLE_PATH || undefined,
        args: [
            '--no-sandbox', 
            '--disable-setuid-sandbox', 
            '--disable-dev-shm-usage',
            '--disable-accelerated-2d-canvas',
            '--no-first-run',
            '--no-zygote',
            '--disable-gpu'
            // REMOVED: '--single-process' which causes massive WA Web slowdowns/hangs
        ]
    }
});

io.on('connection', (socket) => {
    console.log('Frontend connected via WebSockets');
    socket.emit('message', 'Connecting to WhatsApp Client...');
    
    // Check if client is already ready upon a new frontend connection
    if (client.info && client.info.pushname) {
        socket.emit('ready', `WhatsApp is ready! Connected as ${client.info.pushname}`);
    }
});

client.on('qr', (qr) => {
    console.log('QR Code generated. Waiting for scan...');
    // Convert raw QR string to a base64 image URL to show on the frontend
    qrcode.toDataURL(qr, (err, url) => {
        if (!err) {
            io.emit('qr', url);
            io.emit('message', 'Please scan the QR code with your WhatsApp app.');
        }
    });
});

client.on('authenticated', () => {
    console.log('WhatsApp successfully authenticated!');
    io.emit('message', 'Authenticated successfully! Loading...');
});

client.on('auth_failure', msg => {
    console.error('AUTHENTICATION FAILURE', msg);
    io.emit('message', 'Authentication failed! Please restart the server.');
});

client.on('disconnected', (reason) => {
    console.log('Client was logged out or disconnected', reason);
    io.emit('message', 'WhatsApp disconnected. Restarting...');
});

client.on('ready', async () => {
    console.log('WhatsApp Client is ready!');
    io.emit('ready', `WhatsApp is ready! Connected as ${client.info?.pushname || 'User'}`);
    io.emit('message', 'Fetching recent chats...');

    try {
        // Direct browser evaluation fetching straight from WAWebCollections
        const rawChats = await client.pupPage.evaluate(() => {
            try {
                const chatModels = window.require('WAWebCollections').Chat.getModelsArray();
                
                return chatModels.map(chat => {
                    try {
                        // Safely extract the ID without breaking the loop for malformed chats
                        let extractedId = null;
                        if (chat.id) {
                            if (chat.id._serialized) {
                                extractedId = chat.id._serialized;
                            } else if (chat.id.$1) {
                                extractedId = chat.id.$1;
                            } else if (chat.id.user && chat.id.server) {
                                extractedId = `${chat.id.user}@${chat.id.server}`;
                            } else if (chat.id.user) {
                                extractedId = chat.id.user;
                            }
                        }

                        // Determine the name based on the specified priority
                        const chatName = chat.formattedTitle || chat.name || (chat.id && chat.id.user) || 'Unknown';
                        
                        // Ensure simple serializeable values
                        return {
                            name: chatName,
                            id: extractedId,
                            unread: Number(chat.unreadCount || 0),
                            timestamp: Number(chat.t || 0)
                        };
                    } catch (err) {
                        // Skip malformed individual chats safely
                        return null; 
                    }
                });
            } catch (err) {
                throw new Error(err.message);
            }
        });

        // Filter valid results, sort by descending timestamp, and pick the top 50
        const recentChats = rawChats
            .filter(chat => chat && chat.id)
            .sort((a, b) => b.timestamp - a.timestamp)
            .slice(0, 50);

        console.log(`Found ${recentChats.length} recent chats`);
        
        io.emit('chats', recentChats);
        io.emit('message', `Successfully loaded ${recentChats.length} recent chats.`);

    } catch (error) {
        console.error('Failed to fetch recent chats:', error);
        io.emit('message', `Failed to fetch recent chats: ${error.message}`);
    }
});

// Catch any initialization errors so they don't crash the Node process
client.initialize().catch(err => {
    console.error("Failed to initialize WhatsApp client:", err);
});

server.listen(PORT, () => {
  console.log(`Server is running on port ${PORT}`);
});