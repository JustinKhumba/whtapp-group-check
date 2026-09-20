const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const { Client, LocalAuth } = require('whatsapp-web.js');
const qrcode = require('qrcode');

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
    cors: { origin: '*' }
});

const PORT = process.env.PORT || 3000;

// Serve static files (including index.html)
app.use(express.static(__dirname));

app.get('/', (req, res) => {
    res.sendFile(__dirname + '/index.html');
});

// Configure WhatsApp Client with Railway/Docker compatibility
const client = new Client({
    authStrategy: new LocalAuth(),
    puppeteer: {
        args: ['--no-sandbox', '--disable-setuid-sandbox']
    }
});

// Socket.io Connection
io.on('connection', (socket) => {
    console.log('Frontend client connected to Socket.IO');
});

// WhatsApp Events
client.on('qr', async (qr) => {
    console.log('QR Code received');
    try {
        const url = await qrcode.toDataURL(qr);
        io.emit('qr', url);
    } catch (err) {
        console.error('Error generating QR code URL:', err);
        io.emit('message', 'Error displaying QR code');
    }
});

client.on('authenticated', () => {
    console.log('Authenticated successfully');
    io.emit('message', 'Authenticated successfully, initializing...');
});

client.on('auth_failure', msg => {
    console.error('Authentication failure', msg);
    io.emit('message', 'Authentication failure: ' + msg);
});

// --- UPDATED READY EVENT LOGIC ---
client.on('ready', async () => {
    console.log('Client is ready!');
    io.emit('ready', 'WhatsApp is ready! Fetching recent chats...');

    try {
        const result = await client.pupPage.evaluate(() => {
            try {
                let chatModels = [];

                // 1. Attempt the requested WAWebCollections method safely
                if (typeof window.require === 'function') {
                    try {
                        const WAWebCollections = window.require('WAWebCollections');
                        if (WAWebCollections && WAWebCollections.Chat) {
                            chatModels = WAWebCollections.Chat.getModelsArray();
                        }
                    } catch (e) {
                        // If window.require fails, ignore and move to fallback
                    }
                }

                // 2. Safe Fallback: WWebJS automatically injects window.Store when 'ready' is fired.
                // This guarantees we get chats even if Meta removed window.require in your WhatsApp version.
                if ((!chatModels || chatModels.length === 0) && typeof window.Store !== 'undefined' && window.Store.Chat) {
                    chatModels = window.Store.Chat.getModelsArray();
                }

                if (!chatModels || !Array.isArray(chatModels)) {
                    return { error: 'Could not locate WhatsApp chat models (window.require and window.Store are both unavailable).' };
                }

                const parsedChats = chatModels.map(chat => {
                    // 1. Extremely Safe ID extraction
                    let id = null;
                    if (chat.id) {
                        if (typeof chat.id === 'string') {
                            id = chat.id;
                        } else if (typeof chat.id === 'object') {
                            if (chat.id._serialized) id = String(chat.id._serialized);
                            else if (chat.id.$1) id = String(chat.id.$1);
                            else if (chat.id.user && chat.id.server) id = `${chat.id.user}@${chat.id.server}`;
                        }
                    }

                    // 2. Extremely Safe Name extraction
                    let name = 'Unknown';
                    if (typeof chat.formattedTitle === 'string' && chat.formattedTitle) {
                        name = chat.formattedTitle;
                    } else if (typeof chat.name === 'string' && chat.name) {
                        name = chat.name;
                    } else if (chat.id && typeof chat.id === 'object' && chat.id.user) {
                        name = String(chat.id.user);
                    } else if (typeof chat.id === 'string') {
                        name = chat.id;
                    }

                    // 3. Return plain serializable data
                    return {
                        name: name,
                        id: id,
                        unread: Number(chat.unreadCount || 0),
                        timestamp: Number(chat.t || 0)
                    };
                })
                .filter(chat => chat.id) // Filter out invalid chats
                .sort((a, b) => b.timestamp - a.timestamp) // Sort by newest timestamp
                .slice(0, 50); // Keep only the newest 50

                return { data: parsedChats };
            } catch (err) {
                // Catch internal browser errors and pass them back as text
                return { error: 'Browser error: ' + err.toString() };
            }
        });

        // Check if the browser evaluation returned a managed error
        if (result.error) {
            throw new Error(result.error);
        }

        const recentChats = result.data;
        console.log(`Found ${recentChats.length} recent chats`);
        io.emit('chats', recentChats);
        io.emit('message', `Successfully loaded ${recentChats.length} recent chats.`);

    } catch (error) {
        console.error('Failed to fetch recent chats:', error);
        io.emit('message', `Failed to fetch recent chats: ${error.message}`);
    }
});
// ----------------------------------

client.on('disconnected', (reason) => {
    console.log('Client was logged out', reason);
    io.emit('message', 'Client disconnected: ' + reason);
});

// Initialize WhatsApp Client
client.initialize();

// Start server
server.listen(PORT, () => {
    console.log(`Server is running on port ${PORT}`);
});