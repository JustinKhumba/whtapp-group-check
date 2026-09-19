const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const { Client, LocalAuth } = require('whatsapp-web.js');

const app = express();
const server = http.createServer(app);
const io = new Server(server);

const PORT = process.env.PORT || 3000;

// This string contains your entire client-side frontend!
// Express will serve this to your browser when you visit the Railway URL.
const frontendHTML = `
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>WhatsApp Library Tester</title>
    <script src="https://cdn.tailwindcss.com"></script>
    <script src="https://cdnjs.cloudflare.com/ajax/libs/qrcodejs/1.0.0/qrcode.min.js"></script>
    <script src="/socket.io/socket.io.js"></script>
    <style>
        body { background-color: #0f172a; color: #f8fafc; font-family: 'Inter', sans-serif; }
        .glass-panel { background: rgba(30, 41, 59, 0.7); backdrop-filter: blur(10px); border: 1px solid rgba(255, 255, 255, 0.1); }
        .log-container { scroll-behavior: smooth; }
        .log-container::-webkit-scrollbar { width: 6px; }
        .log-container::-webkit-scrollbar-thumb { background: #475569; border-radius: 4px; }
    </style>
</head>
<body class="min-h-screen flex items-center justify-center p-6">

    <div class="max-w-5xl w-full grid grid-cols-1 md:grid-cols-2 gap-6">
        
        <!-- Left Column: Status and QR Code -->
        <div class="glass-panel p-8 rounded-2xl flex flex-col items-center justify-center text-center shadow-2xl relative overflow-hidden">
            <h1 class="text-3xl font-bold text-white mb-2">WhatsApp Tester</h1>
            <p class="text-slate-400 text-sm mb-8">Checking if library boots up successfully</p>
            
            <div id="status-badge" class="px-4 py-1.5 rounded-full bg-blue-500/20 text-blue-400 border border-blue-500/30 font-medium text-sm mb-6 transition-colors duration-300">
                Initializing Chromium Browser...
            </div>

            <!-- QR Code Box -->
            <div class="bg-white p-4 rounded-xl shadow-inner min-h-[256px] min-w-[256px] flex items-center justify-center relative overflow-hidden">
                <div id="qrcode"></div>
                <div id="qr-overlay" class="absolute inset-0 bg-white/90 flex flex-col items-center justify-center z-10 transition-opacity duration-500">
                    <svg class="animate-spin h-8 w-8 text-blue-600 mb-3" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
                        <circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"></circle>
                        <path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
                    </svg>
                    <span class="text-slate-800 font-medium text-sm" id="overlay-text">Booting Library...</span>
                </div>
            </div>
            
            <p class="text-xs text-slate-500 mt-6 mt-auto">Test successful if QR code appears below. No need to actually scan it.</p>
        </div>

        <!-- Right Column: Live Server Logs -->
        <div class="glass-panel p-6 rounded-2xl shadow-2xl flex flex-col h-[500px]">
            <div class="flex items-center justify-between mb-4 border-b border-slate-700 pb-4">
                <h2 class="text-lg font-semibold flex items-center gap-2">
                    <svg class="w-5 h-5 text-emerald-400" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M8 9l3 3-3 3m5 0h3M4 15a1 1 0 001 1h14a1 1 0 001-1V9a1 1 0 00-1-1H5a1 1 0 00-1 1v6z"></path></svg>
                    Live Server Logs
                </h2>
                <span class="flex h-3 w-3 relative">
                    <span class="animate-ping absolute inline-flex h-full w-full rounded-full bg-emerald-400 opacity-75"></span>
                    <span class="relative inline-flex rounded-full h-3 w-3 bg-emerald-500"></span>
                </span>
            </div>
            <div id="logs" class="log-container flex-1 overflow-y-auto font-mono text-sm space-y-2 text-slate-300 pr-2">
                <div class="text-slate-500">[System] Connecting to Railway server...</div>
            </div>
        </div>
    </div>

    <!-- Client Side JavaScript Logic -->
    <script>
        const socket = io();
        const logsContainer = document.getElementById('logs');
        const qrcodeContainer = document.getElementById('qrcode');
        const qrOverlay = document.getElementById('qr-overlay');
        const overlayText = document.getElementById('overlay-text');
        const statusBadge = document.getElementById('status-badge');
        
        let qrCodeObj = null;

        function addLog(message, type = 'info') {
            const el = document.createElement('div');
            const time = new Date().toLocaleTimeString();
            let colorClass = 'text-slate-300';
            
            if (type === 'success') colorClass = 'text-emerald-400';
            if (type === 'error') colorClass = 'text-rose-400';
            if (type === 'warn') colorClass = 'text-amber-400';

            el.className = \`border-l-2 border-slate-700 pl-3 py-1 \${colorClass}\`;
            el.innerHTML = \`<span class="text-slate-500 text-xs mr-2">[\${time}]</span> \${message}\`;
            logsContainer.appendChild(el);
            logsContainer.scrollTop = logsContainer.scrollHeight;
        }

        socket.on('connect', () => {
            addLog('Connected to WebSocket server', 'success');
        });

        socket.on('log', (data) => {
            addLog(data.msg, data.type);
        });

        socket.on('status', (status) => {
            statusBadge.innerText = status;
            if(status === 'Library Booted & QR Ready') {
                statusBadge.className = 'px-4 py-1.5 rounded-full bg-amber-500/20 text-amber-400 border border-amber-500/30 font-medium text-sm mb-6 transition-colors duration-300';
            } else if (status === 'Authenticated' || status === 'Ready!') {
                statusBadge.className = 'px-4 py-1.5 rounded-full bg-emerald-500/20 text-emerald-400 border border-emerald-500/30 font-medium text-sm mb-6 transition-colors duration-300';
                qrOverlay.style.opacity = '1';
                qrOverlay.innerHTML = '<span class="text-emerald-600 font-bold text-lg">Successfully Linked!</span>';
            } else {
                statusBadge.className = 'px-4 py-1.5 rounded-full bg-blue-500/20 text-blue-400 border border-blue-500/30 font-medium text-sm mb-6 transition-colors duration-300';
            }
        });

        socket.on('qr', (qrString) => {
            // Success! The library is working and generated a QR code.
            qrOverlay.style.opacity = '0';
            setTimeout(() => { qrOverlay.style.display = 'none'; }, 500);

            qrcodeContainer.innerHTML = ''; // Clear previous
            
            qrCodeObj = new QRCode(qrcodeContainer, {
                text: qrString,
                width: 224,
                height: 224,
                colorDark : "#0f172a",
                colorLight : "#ffffff",
                correctLevel : QRCode.CorrectLevel.L
            });
        });
    </script>
</body>
</html>
`;

// Serve the frontend to the user when they visit the app
app.get('/', (req, res) => {
    res.send(frontendHTML);
});

// Initialize WhatsApp Client with specific arguments needed for Railway/Docker
const client = new Client({
    authStrategy: new LocalAuth(),
    puppeteer: {
        args: [
            '--no-sandbox',
            '--disable-setuid-sandbox',
            '--disable-dev-shm-usage',
            '--disable-accelerated-2d-canvas',
            '--no-first-run',
            '--no-zygote',
            '--single-process', 
            '--disable-gpu'
        ]
    }
});

// Helper function to send logs to the client-side UI
function sendLog(msg, type = 'info') {
    console.log(`[${type.toUpperCase()}] ${msg}`);
    io.emit('log', { msg, type });
}

// WhatsApp Events
client.on('qr', (qr) => {
    sendLog('Library successfully generated a QR Code! The test is a SUCCESS.', 'success');
    io.emit('status', 'Library Booted & QR Ready');
    io.emit('qr', qr);
});

client.on('ready', () => {
    sendLog('WhatsApp Client is Ready!', 'success');
    io.emit('status', 'Ready!');
});

client.on('authenticated', () => {
    sendLog('Session authenticated successfully.', 'success');
    io.emit('status', 'Authenticated');
});

client.on('auth_failure', msg => {
    sendLog('Authentication failure: ' + msg, 'error');
    io.emit('status', 'Auth Failed');
});

// Start HTTP server and trigger WhatsApp bootup
server.listen(PORT, () => {
    console.log(`Server is running on port ${PORT}`);
    
    // Begin initializing the WhatsApp client
    setTimeout(() => {
        sendLog('Booting up whatsapp-web.js inside Chromium...', 'info');
        client.initialize().catch(err => {
            sendLog('Failed to initialize library: ' + err.message, 'error');
        });
    }, 1000);
});