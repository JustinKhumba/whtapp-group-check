const express = require('express');
const { Client, LocalAuth } = require('whatsapp-web.js');
const qrcode = require('qrcode');
const path = require('path');

const app = express();
const port = process.env.PORT || 3000;

app.use(express.json());
app.use(express.static(__dirname));

const sessionPath = process.env.RAILWAY_ENVIRONMENT ? '/app/session_data' : './session_data';

let currentQR = '';
let isAuthenticated = false;
let client;

app.listen(port, '0.0.0.0', () => {
    console.log(`Server running on port ${port}`);
    startWhatsAppClient();
});

app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, 'index.html'));
});

app.get('/api/status', (req, res) => {
    res.json({ isAuthenticated, qr: currentQR });
});

app.post('/api/check-member', async (req, res) => {
    if (!isAuthenticated || !client) {
        return res.status(401).json({ error: 'WhatsApp client is not authenticated yet. Please scan the QR code.' });
    }

    const { groupId, phoneNumber } = req.body;

    if (!groupId || !phoneNumber) {
        return res.status(400).json({ error: 'Group ID and Phone Number are required.' });
    }

    try {
        const formattedGroupId = groupId.includes('@g.us') ? groupId : `${groupId}@g.us`;
        let formattedPhoneNumber = phoneNumber.replace(/\D/g, '');
        formattedPhoneNumber = `${formattedPhoneNumber}@c.us`;

        const chat = await client.getChatById(formattedGroupId);

        if (!chat.isGroup) {
            return res.status(400).json({ error: 'The provided ID does not belong to a valid WhatsApp group.' });
        }

        const isMember = chat.participants.some(participant => participant.id._serialized === formattedPhoneNumber);

        res.json({
            success: true,
            groupId: formattedGroupId,
            phoneNumber: formattedPhoneNumber,
            isMember: isMember,
            cost: '₹0.00' 
        });

    } catch (error) {
        res.status(500).json({ error: 'Failed to fetch group data. Ensure the bot is a member of the group.' });
    }
});

function startWhatsAppClient() {
    client = new Client({
        authStrategy: new LocalAuth({ 
            clientId: "session-919863477674",
            dataPath: sessionPath
        }),
        puppeteer: {
            executablePath: process.env.PUPPETEER_EXECUTABLE_PATH || null,
            args: [
                '--no-sandbox', 
                '--disable-setuid-sandbox',
                '--disable-dev-shm-usage',
                '--disable-accelerated-2d-canvas',
                '--no-first-run',
                '--no-zygote',
                '--disable-gpu'
            ]
        }
    });

    client.on('qr', async (qr) => {
        try {
            currentQR = await qrcode.toDataURL(qr);
        } catch (err) {
            console.error('Failed to generate QR code image');
        }
    });

    client.on('ready', () => {
        console.log('WhatsApp Client is ready and authenticated!');
        isAuthenticated = true;
        currentQR = '';
    });

    client.on('authenticated', () => {
        isAuthenticated = true;
        currentQR = '';
    });

    client.on('disconnected', () => {
        isAuthenticated = false;
    });

    client.initialize();
}