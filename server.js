const express = require('express');
const { Client, LocalAuth } = require('whatsapp-web.js');
const qrcode = require('qrcode');
const path = require('path');

const app = express();
const port = process.env.PORT || 3000;

app.use(express.json());
app.use(express.static(__dirname));

const sessionPath =
    process.env.RAILWAY_VOLUME_MOUNT_PATH ||
    path.join(__dirname, 'session_data');

let currentQR = '';
let isAuthenticated = false;
let client = null;
let clientError = '';

app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, 'index.html'));
});

app.get('/api/status', (req, res) => {
    res.json({
        isAuthenticated,
        qr: currentQR,
        error: clientError
    });
});

app.post('/api/check-member', async (req, res) => {
    if (!client || !isAuthenticated) {
        return res.status(401).json({
            error: 'WhatsApp client is not ready yet. Please wait for authentication.'
        });
    }

    const { groupId, phoneNumber } = req.body;

    if (!groupId || !phoneNumber) {
        return res.status(400).json({
            error: 'Group ID and Phone Number are required.'
        });
    }

    try {
        const formattedGroupId = groupId.includes('@g.us')
            ? groupId
            : `${groupId}@g.us`;

        const formattedPhoneNumber =
            phoneNumber.replace(/\D/g, '') + '@c.us';

        const chat = await client.getChatById(formattedGroupId);

        if (!chat || !chat.isGroup) {
            return res.status(400).json({
                error: 'The provided ID does not belong to a valid WhatsApp group.'
            });
        }

        const isMember = chat.participants.some(
            participant =>
                participant.id._serialized === formattedPhoneNumber
        );

        res.json({
            success: true,
            groupId: formattedGroupId,
            phoneNumber: formattedPhoneNumber,
            isMember
        });

    } catch (error) {
        console.error('CHECK MEMBER ERROR:', error);

        res.status(500).json({
            error: 'Failed to fetch group data.'
        });
    }
});

function startWhatsAppClient() {
    console.log('Starting WhatsApp client...');
    console.log('Session path:', sessionPath);

    client = new Client({
        authStrategy: new LocalAuth({
            clientId: 'wa-checker',
            dataPath: sessionPath
        }),

        puppeteer: {
            headless: true,

            args: [
                '--no-sandbox',
                '--disable-setuid-sandbox',
                '--disable-dev-shm-usage',
                '--disable-gpu',
                '--no-first-run',
                '--no-zygote'
            ]
        }
    });

    client.on('qr', async (qr) => {
        console.log('QR received');

        try {
            currentQR = await qrcode.toDataURL(qr);
            clientError = '';
        } catch (error) {
            console.error('QR ERROR:', error);
            clientError = 'Failed to generate QR code.';
        }
    });

    client.on('authenticated', () => {
        console.log('WhatsApp authenticated');
        currentQR = '';
        clientError = '';
    });

    client.on('ready', () => {
        console.log('WhatsApp client READY');
        isAuthenticated = true;
        currentQR = '';
        clientError = '';
    });

    client.on('auth_failure', (message) => {
        console.error('AUTH FAILURE:', message);

        isAuthenticated = false;
        clientError = `Authentication failed: ${message}`;
    });

    client.on('disconnected', (reason) => {
        console.error('WHATSAPP DISCONNECTED:', reason);

        isAuthenticated = false;
        clientError = `WhatsApp disconnected: ${reason}`;
    });

    client.on('change_state', (state) => {
        console.log('WhatsApp state:', state);
    });

    client.initialize().catch((error) => {
        console.error('WHATSAPP INITIALIZE ERROR:', error);

        isAuthenticated = false;
        clientError = error.message || 'WhatsApp initialization failed.';
    });
}
app.get('/health', (req, res) => {
    res.status(200).json({
        status: 'ok'
    });
});

app.listen(port, '0.0.0.0', () => {
    console.log(`Server running on port ${port}`);

});