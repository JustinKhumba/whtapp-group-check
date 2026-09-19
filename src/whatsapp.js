const { Client, LocalAuth } = require('whatsapp-web.js');
const qrcode = require('qrcode');
const fs = require('fs');

let isClientReady = false;
let isAuthenticated = false;
let qrImageURL = '';
let groupList = [];

// Configure auth path for Railway compatibility (persistent volume)
const authPath = process.env.WHATSAPP_AUTH_PATH || './.wwebjs_auth';

const client = new Client({
    authStrategy: new LocalAuth({
        clientId: 'group-checker',
        dataPath: authPath
    }),
    puppeteer: {
        // Essential flags for running in server environments like Railway
        args: ['--no-sandbox', '--disable-setuid-sandbox']
    }
});

client.on('qr', async (qr) => {
    console.log('New QR Code generated. View it at /setup in your browser.');
    try {
        qrImageURL = await qrcode.toDataURL(qr);
    } catch (err) {
        console.error('Error generating QR image', err);
    }
});

client.on('authenticated', () => {
    isAuthenticated = true;
    qrImageURL = ''; // Clear QR code once logged in
    console.log('WhatsApp authenticated.');
});

client.on('auth_failure', (msg) => {
    console.error('WhatsApp authentication failed:', msg);
    isAuthenticated = false;
    isClientReady = false;
    qrImageURL = '';
});

client.on('ready', async () => {
    isClientReady = true;
    console.log('WhatsApp client is ready.\n');

    try {
        const chats = await client.getChats();
        const groups = chats.filter(chat => chat.isGroup);

        groupList = groups.map(g => ({
            name: g.name,
            id: g.id._serialized
        }));
        
        console.log(`Found ${groups.length} groups. View them at /setup in your browser.`);
    } catch (err) {
        console.error('Error fetching group lists:', err);
    }
});

client.on('disconnected', (reason) => {
    console.log('WhatsApp disconnected:', reason);
    isClientReady = false;
    isAuthenticated = false;
    groupList = [];
});

module.exports = {
    client,
    initialize: () => client.initialize(),
    getStatus: () => ({
        authenticated: isAuthenticated,
        ready: isClientReady
    }),
    getSetupData: () => ({
        qr: qrImageURL,
        groups: groupList,
        authenticated: isAuthenticated,
        ready: isClientReady
    })
};