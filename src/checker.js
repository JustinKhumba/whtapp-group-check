const { client, getStatus } = require('./whatsapp');

function normalizePhone(phone) {
    if (!phone) return '';
    // Strip all non-digit characters (e.g., +, -, spaces)
    return phone.replace(/\D/g, '');
}

async function checkGroupMembership(phoneNumber) {
    const status = getStatus();
    
    // Verify WhatsApp state
    if (!status.ready) {
        throw new Error('WhatsApp client is not ready.');
    }

    const groupId = process.env.WHATSAPP_GROUP_ID;
    if (!groupId) {
        throw new Error('WHATSAPP_GROUP_ID is not configured in environment variables.');
    }

    const normalizedTarget = normalizePhone(phoneNumber);
    if (normalizedTarget.length < 8) {
        throw new Error('Invalid phone number provided.');
    }

    let chat;
    try {
        chat = await client.getChatById(groupId);
    } catch (err) {
        throw new Error('Failed to fetch the configured group. Ensure the ID is correct.');
    }

    if (!chat || !chat.isGroup) {
        throw new Error('The configured ID does not belong to a valid group.');
    }

    const participants = chat.participants;
    if (!participants || !Array.isArray(participants)) {
        throw new Error('Participant list unavailable for this group.');
    }

    // A participant's ID typically looks like '919876543210@c.us'
    // id.user contains just the number portion
    for (const participant of participants) {
        if (participant.id && participant.id.user === normalizedTarget) {
            return true; // Member found
        }
    }

    return false; // Not a member
}

async function getGroupInfo() {
    const status = getStatus();
    if (!status.ready) throw new Error('WhatsApp client is not ready.');

    const groupId = process.env.WHATSAPP_GROUP_ID;
    if (!groupId) throw new Error('WHATSAPP_GROUP_ID is not configured.');

    const chat = await client.getChatById(groupId);
    if (!chat || !chat.isGroup) throw new Error('Configured ID is not a group.');

    return {
        name: chat.name,
        participantCount: chat.participants.length
    };
}

module.exports = {
    normalizePhone,
    checkGroupMembership,
    getGroupInfo
};