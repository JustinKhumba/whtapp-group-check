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
    io.emit('message', 'Synchronizing data... Please wait. Cloud servers may take several minutes on first login.');

    let attempts = 0;
    const maxAttempts = 36; // Poll 36 times (3 minutes total with 5s delays)

    const fetchChatsSafely = async () => {
        try {
            // 1. Try Native method with a strict timeout to prevent hangs
            const nativeFetch = client.getChats();
            const timeout = new Promise((_, reject) => setTimeout(() => reject(new Error('TIMEOUT')), 8000));
            
            const chats = await Promise.race([nativeFetch, timeout]);
            if (chats && chats.length > 0) {
                return {
                    status: 'success',
                    data: chats.map(chat => ({
                        name: chat.name || (chat.id && chat.id.user) || 'Unknown',
                        id: chat.id && chat.id._serialized,
                        unread: chat.unreadCount || 0,
                        timestamp: chat.timestamp || 0
                    }))
                };
            }
            throw new Error('EMPTY_NATIVE');
        } catch (err) {
            console.log(`Native fetch issue (${err.message}). Inspecting hidden browser DOM...`);
            
            // 2. Fallback: Inspect the actual browser DOM to see exactly what WhatsApp is doing
            return await client.pupPage.evaluate(() => {
                // Check if WhatsApp is stuck on the loading/syncing screen
                const isSyncing = document.querySelector('[role="progressbar"]') || 
                                  document.body.innerText.includes('Downloading recent messages') || 
                                  document.body.innerText.includes('Loading your chats');
                
                if (isSyncing) return { status: 'syncing' };
                
                if (!window.Store || !window.Store.Chat) return { status: 'no_store' };
                
                const rawChats = window.Store.Chat.getModelsArray 
                    ? window.Store.Chat.getModelsArray() 
                    : Object.values(window.Store.Chat._models || {});
                    
                if (!rawChats || rawChats.length === 0) return { status: 'empty_store' };

                // Extract only primitives to bypass Puppeteer serialization crashes
                const parsedChats = rawChats.map(c => ({
                    name: c.name || c.formattedTitle || (c.id && c.id.user) || 'Unknown',
                    id: c.id && c.id._serialized,
                    unread: c.unreadCount || 0,
                    timestamp: c.t || 0
                }));

                return { status: 'success', data: parsedChats };
            });
        }
    };

    const pollInterval = setInterval(async () => {
        attempts++;
        
        try {
            const result = await fetchChatsSafely();
            
            if (result && result.status === 'success' && result.data && result.data.length > 0) {
                clearInterval(pollInterval);
                
                // Sort by timestamp (newest first) and take top 50
                const sortedChats = result.data
                    .sort((a, b) => b.timestamp - a.timestamp)
                    .slice(0, 50);

                io.emit('chats', sortedChats);
                io.emit('message', `Successfully loaded ${sortedChats.length} recent chats.`);
            } else if (result && result.status === 'syncing') {
                io.emit('message', `WhatsApp is still actively downloading messages... (Attempt ${attempts}/${maxAttempts})`);
            } else if (result && (result.status === 'no_store' || result.status === 'empty_store')) {
                io.emit('message', `Waiting for WhatsApp database to initialize... (Attempt ${attempts}/${maxAttempts})`);
            } else if (attempts >= maxAttempts) {
                clearInterval(pollInterval);
                io.emit('message', 'Timeout reached. WhatsApp is taking too long to sync on this server. Please refresh the page to try again.');
            } else {
                io.emit('message', `Fetching chats (Attempt ${attempts}/${maxAttempts})...`);
            }
        } catch (error) {
            console.error('Error fetching chats completely:', error);
            if (attempts >= maxAttempts) {
                clearInterval(pollInterval);
                io.emit('message', `Complete failure fetching chats: ${error.message}`);
            }
        }
    }, 5000); // 5-second polling interval
});

// Catch any initialization errors so they don't crash the Node process
client.initialize().catch(err => {
    console.error("Failed to initialize WhatsApp client:", err);
    // Note: Do not emit here immediately if io isn't bound, rely on logs
});

server.listen(PORT, () => {
  console.log(`Server is running on port ${PORT}`);
});