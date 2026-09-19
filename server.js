// File: ./server.js
const express = require('express');
const { Client, LocalAuth } = require('whatsapp-web.js');
const qrcode = require('qrcode-terminal');
const path = require('path');

const app = express();
const port = 3000;

app.use(express.json());
app.use(express.static(__dirname));

const client = new Client({
    authStrategy: new LocalAuth({ clientId: "session-919863477674" }),
    puppeteer: {
        args: ['--no-sandbox', '--disable-setuid-sandbox']
    }
});

client.on('qr', (qr) => {
    console.log('Scan this QR code with your WhatsApp number 91 9863477674:');
    qrcode.generate(qr, { small: true });
});

client.on('ready', () => {
    console.log('WhatsApp Client is ready and authenticated!');
});

client.initialize();

app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, 'index.html'));
});

app.post('/api/check-member', async (req, res) => {
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
        console.error('Error checking group membership:', error);
        res.status(500).json({ error: 'Failed to fetch group data. Ensure the bot is a member of the group.' });
    }
});

app.listen(port, () => {
    console.log(`Server running at http://localhost:${port}`);
});