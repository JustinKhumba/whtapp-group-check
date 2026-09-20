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
            '--single-process', // Heavily reduces memory usage
            '--disable-gpu'
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
    io.emit('message', 'Fetching chats...');

    try {
        // Fetch chats from the account
        const chats = await client.getChats();
        
        // Map down the complex chat objects to simple data for the frontend
        const chatData = chats.map(chat => ({
            name: chat.name || chat.id.user,
            id: chat.id._serialized,
            unread: chat.unreadCount
        }));
        
        // Send chats to the frontend
        io.emit('chats', chatData);
    } catch (error) {
        console.error('Error fetching chats:', error);
        io.emit('message', 'Error fetching chats. Check server logs.');
    }
});

// Catch any initialization errors so they don't crash the Node process
client.initialize().catch(err => {
    console.error("Failed to initialize WhatsApp client:", err);
    io.emit('message', `Startup Error: ${err.message}. Check Server Logs.`);
});

server.listen(PORT, () => {
  console.log(`Server is running on port ${PORT}`);
});