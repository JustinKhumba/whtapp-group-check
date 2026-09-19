const express = require('express');
const { Client, LocalAuth } = require('whatsapp-web.js');
const qrcode = require('qrcode');
const path = require('path');

const app = express();
const port = process.env.PORT || 3000;

let status = 'Idle (Not Started)';
let qrCodeDataUrl = '';
let client = null;

// Serve all static files (like index.html) from the 'public' folder
app.use(express.static(path.join(__dirname, 'public')));

// Route: Initialize the WhatsApp Library
app.get('/api/start', async (req, res) => {
    if (client) {
        return res.json({ message: 'Client is already initializing or initialized.' });
    }

    status = 'Initializing...';
    qrCodeDataUrl = '';
    
    res.json({ message: 'Initialization started' });

    // Ensure puppeteer works in a headless server environment without sandbox
    client = new Client({
        authStrategy: new LocalAuth(),
        puppeteer: {
            args: ['--no-sandbox', '--disable-setuid-sandbox']
        }
    });

    client.on('qr', async (qr) => {
        status = 'QR Code Generated (Scan now!)';
        try {
            // Convert the text QR code to a base64 image URL to send to frontend
            qrCodeDataUrl = await qrcode.toDataURL(qr);
            console.log('QR Code generated.');
        } catch (err) {
            console.error('Failed to generate QR Code image', err);
        }
    });

    client.on('ready', () => {
        status = 'READY';
        qrCodeDataUrl = ''; // Clear the QR code since we are logged in
        console.log('Client is ready!');
    });

    client.on('authenticated', () => {
        status = 'Authenticated';
        console.log('Authenticated successfully.');
    });

    client.on('auth_failure', msg => {
        status = 'Authentication Failed';
        console.error('AUTHENTICATION FAILURE', msg);
    });

    client.on('disconnected', (reason) => {
        status = 'Disconnected: ' + reason;
        client = null;
        console.log('Client was logged out', reason);
    });

    // Start the library
    client.initialize();
});

// Route: Get the current status and QR code
app.get('/api/status', (req, res) => {
    res.json({ status: status, qr: qrCodeDataUrl });
});

app.listen(port, () => {
    console.log(`Server is running on port ${port}`);
});