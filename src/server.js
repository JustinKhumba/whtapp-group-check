require('dotenv').config();
const express = require('express');
const path = require('path');
const whatsapp = require('./whatsapp');
const checker = require('./checker');

const app = express();
const PORT = process.env.PORT || 3000;

app.use(express.json());
// Serve static frontend files safely
app.use(express.static(path.join(__dirname, '../public')));

app.get('/', (req, res) => {
    res.send('WhatsApp Group Checker is running');
});

// Explicit route to serve the testing UI
app.get('/test', (req, res) => {
    res.sendFile(path.join(__dirname, '../public/test/index.html'));
});

// Setup route to show the QR code and Group IDs in the browser
app.get('/setup', (req, res) => {
    res.sendFile(path.join(__dirname, '../public/setup/index.html'));
});

// API endpoint for the setup UI to fetch QR and groups
app.get('/api/setup-data', (req, res) => {
    res.json(whatsapp.getSetupData());
});

app.get('/status', (req, res) => {
    const status = whatsapp.getStatus();
    res.json(status);
});

app.post('/check-member', async (req, res) => {
    const { phone } = req.body;

    if (!phone) {
        return res.status(400).json({ success: false, error: 'Phone number is required' });
    }

    try {
        const isMember = await checker.checkGroupMembership(phone);
        res.json({
            success: true,
            member: isMember
        });
    } catch (error) {
        res.status(500).json({
            success: false,
            error: error.message || 'An unexpected error occurred'
        });
    }
});

app.get('/group-info', async (req, res) => {
    try {
        const info = await checker.getGroupInfo();
        res.json({
            success: true,
            name: info.name,
            participantCount: info.participantCount
        });
    } catch (error) {
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

app.listen(PORT, () => {
    console.log(`Server listening on port ${PORT}`);
    // Initialize WhatsApp client once server is up
    whatsapp.initialize();
});