const express = require('express');
const { Client, LocalAuth } = require('whatsapp-web.js');
const qrcode = require('qrcode');
const path = require('path');

const app = express();
const port = process.env.PORT || 3000;

// State variables to track what the WhatsApp client is doing
let waStatus = 'Idle (Not Started)';
let qrCodeDataUrl = '';
let client = null;

// Route: Serve the Client HTML
app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, 'index.html'));
});

// Route: Initialize the WhatsApp Library
app.get('/api/start', async (req, res) => {
    if (!client) {
        waStatus = 'Initializing Puppeteer...';
        
        try {
            // Configure WhatsApp Web JS
            // The args provided here are CRITICAL for environments like Railway/Docker
            client = new Client({
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

            // Event: Library successfully generated a QR code
            client.on('qr', async (qr) => {
                console.log('QR Code Received');
                waStatus = 'QR Code Generated - Ready to Scan';
                // Convert raw QR string to a Base64 Image URL for the frontend
                qrCodeDataUrl = await qrcode.toDataURL(qr);
            });

            // Event: Client successfully authenticated
            client.on('authenticated', () => {
                console.log('Authenticated!');
                waStatus = 'Authenticated successfully!';
                qrCodeDataUrl = ''; // Clear QR code
            });
            
            // Event: Client authentication failed
            client.on('auth_failure', msg => {
                console.error('Authentication failure', msg);
                waStatus = 'Authentication failed: ' + msg;
            });

            // Event: Client is ready to use
            client.on('ready', () => {
                console.log('Client is ready!');
                waStatus = 'READY';
                qrCodeDataUrl = '';
            });

            // Start the initialization process
            client.initialize().catch(err => {
                console.error('Initialization error:', err);
                waStatus = 'Error during initialization: ' + err.message;
            });

        } catch (error) {
            waStatus = 'Fatal Error: ' + error.message;
        }
    }
    
    res.json({ success: true, status: waStatus });
});

// Route: Get current status (polled by frontend)
app.get('/api/status', (req, res) => {
    res.json({ 
        status: waStatus, 
        qr: qrCodeDataUrl 
    });
});

// Start the Express server
app.listen(port, () => {
    console.log(`Server is running on port ${port}`);
});