const express = require('express');
const http = require('http');
const path = require('path');
const crypto = require('crypto');
const { Server } = require('socket.io');
const { Client, LocalAuth } = require('whatsapp-web.js');
const qrcode = require('qrcode');
const mysql = require('mysql2/promise');

const app = express();
const server = http.createServer(app);
const io = new Server(server);

const PORT = Number(process.env.PORT || 3000);
const CHAT_LIMIT = Math.min(
    Math.max(Number(process.env.CHAT_LIMIT || 50), 1),
    200
);
const MESSAGE_LIMIT = Math.min(
    Math.max(Number(process.env.MESSAGE_LIMIT || 100), 1),
    500
);

// -----------------------------
// Static website
// -----------------------------
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// -----------------------------
// Error protection
// -----------------------------
process.on('unhandledRejection', error => {
    console.error('Unhandled Promise Rejection:', error);
});

process.on('uncaughtException', error => {
    console.error('Uncaught Exception:', error);
});

// -----------------------------
// MySQL
// Supports Railway MYSQL_* variables
// and MYSQL_URL / DATABASE_URL
// -----------------------------
function createDbPool() {
    const connectionString =
        process.env.MYSQL_URL || process.env.DATABASE_URL;

    if (connectionString) {
        const url = new URL(connectionString);

        return mysql.createPool({
            host: url.hostname,
            port: Number(url.port || 3306),
            user: decodeURIComponent(url.username),
            password: decodeURIComponent(url.password),
            database: decodeURIComponent(
                url.pathname.replace(/^\//, '')
            ),
            waitForConnections: true,
            connectionLimit: 10,
            queueLimit: 0,
            charset: 'utf8mb4'
        });
    }

    const host = process.env.MYSQLHOST || process.env.DB_HOST;
    const user = process.env.MYSQLUSER || process.env.DB_USER;
    const password =
        process.env.MYSQLPASSWORD || process.env.DB_PASSWORD;
    const database =
        process.env.MYSQLDATABASE || process.env.DB_NAME;
    const port = Number(
        process.env.MYSQLPORT || process.env.DB_PORT || 3306
    );

    if (!host || !user || database === undefined) {
        throw new Error(
            'MySQL environment variables are missing. Configure MYSQL_URL or MYSQLHOST, MYSQLUSER, MYSQLPASSWORD and MYSQLDATABASE.'
        );
    }

    return mysql.createPool({
        host,
        port,
        user,
        password,
        database,
        waitForConnections: true,
        connectionLimit: 10,
        queueLimit: 0,
        charset: 'utf8mb4'
    });
}

let db;

// -----------------------------
// Database initialization
// -----------------------------
async function initDatabase() {
    db = createDbPool();

    // Website/browser session table
    await db.query(`
        CREATE TABLE IF NOT EXISTS sessions (
            id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
            session_token CHAR(64) NOT NULL,
            account_number VARCHAR(32) NULL,
            created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            last_seen_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
                ON UPDATE CURRENT_TIMESTAMP,
            PRIMARY KEY (id),
            UNIQUE KEY uq_session_token (session_token),
            KEY idx_session_account (account_number)
        ) ENGINE=InnoDB
        DEFAULT CHARSET=utf8mb4
        COLLATE=utf8mb4_unicode_ci
    `);

    // Recent chats table
    await db.query(`
        CREATE TABLE IF NOT EXISTS recent_chats (
            account_number VARCHAR(32) NOT NULL,
            chat_id VARCHAR(128) NOT NULL,
            name VARCHAR(255) NOT NULL DEFAULT 'Unknown',
            unread_count INT UNSIGNED NOT NULL DEFAULT 0,
            chat_timestamp BIGINT UNSIGNED NOT NULL DEFAULT 0,
            is_group TINYINT(1) NOT NULL DEFAULT 0,
            last_message_text TEXT NULL,
            last_message_type VARCHAR(64) NULL,
            updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
                ON UPDATE CURRENT_TIMESTAMP,
            PRIMARY KEY (account_number, chat_id),
            KEY idx_recent_chats (
                account_number,
                chat_timestamp
            )
        ) ENGINE=InnoDB
        DEFAULT CHARSET=utf8mb4
        COLLATE=utf8mb4_unicode_ci
    `);

    // Messages table
    await db.query(`
        CREATE TABLE IF NOT EXISTS chat_messages (
            account_number VARCHAR(32) NOT NULL,
            chat_id VARCHAR(128) NOT NULL,
            message_id VARCHAR(255) NOT NULL,
            from_me TINYINT(1) NOT NULL DEFAULT 0,
            author_id VARCHAR(128) NULL,
            from_id VARCHAR(128) NULL,
            to_id VARCHAR(128) NULL,
            sender_name VARCHAR(255) NULL,
            body MEDIUMTEXT NULL,
            message_type VARCHAR(64) NULL,
            message_timestamp BIGINT UNSIGNED NOT NULL DEFAULT 0,
            created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (
                account_number,
                message_id
            ),
            KEY idx_chat_messages (
                account_number,
                chat_id,
                message_timestamp,
                message_id
            )
        ) ENGINE=InnoDB
        DEFAULT CHARSET=utf8mb4
        COLLATE=utf8mb4_unicode_ci
    `);

    console.log('MySQL database and tables are ready.');
}

// -----------------------------
// Website session handling
// -----------------------------
async function createOrRestoreSession(token) {
    if (token && /^[a-f0-9]{64}$/i.test(token)) {
        const [rows] = await db.execute(
            `
            SELECT
                session_token,
                account_number
            FROM sessions
            WHERE session_token = ?
            LIMIT 1
            `,
            [token]
        );

        if (rows.length) {
            await db.execute(
                `
                UPDATE sessions
                SET last_seen_at = NOW()
                WHERE session_token = ?
                `,
                [token]
            );

            return rows[0];
        }
    }

    const newToken = crypto
        .randomBytes(32)
        .toString('hex');

    await db.execute(
        `
        INSERT INTO sessions (session_token)
        VALUES (?)
        `,
        [newToken]
    );

    return {
        session_token: newToken,
        account_number: null
    };
}

async function setSessionAccount(accountNumber) {
    if (!accountNumber) return;

    await db.execute(
        `
        UPDATE sessions
        SET account_number = ?,
            last_seen_at = NOW()
        `,
        [accountNumber]
    );
}

async function touchSession(token) {
    if (!token) return;

    await db.execute(
        `
        UPDATE sessions
        SET last_seen_at = NOW()
        WHERE session_token = ?
        `,
        [token]
    );
}

// -----------------------------
// Stored recent chats
// -----------------------------
async function getStoredChats(accountNumber) {
    if (!accountNumber) return [];

    const [rows] = await db.execute(
        `
        SELECT
            chat_id AS id,
            name,
            unread_count AS unread,
            chat_timestamp AS timestamp,
            is_group AS isGroup,
            last_message_text AS lastMessageText,
            last_message_type AS lastMessageType
        FROM recent_chats
        WHERE account_number = ?
        ORDER BY
            chat_timestamp DESC,
            updated_at DESC
        LIMIT ${CHAT_LIMIT}
        `,
        [accountNumber]
    );

    return rows;
}

async function storeChat(accountNumber, chat) {
    if (!accountNumber || !chat || !chat.id) {
        return;
    }

    const lastMessageText =
        chat.lastMessageText === null ||
        chat.lastMessageText === undefined
            ? null
            : String(chat.lastMessageText).slice(0, 10000);

    await db.execute(
        `
        INSERT INTO recent_chats (
            account_number,
            chat_id,
            name,
            unread_count,
            chat_timestamp,
            is_group,
            last_message_text,
            last_message_type
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON DUPLICATE KEY UPDATE
            name = VALUES(name),
            unread_count = VALUES(unread_count),
            chat_timestamp = VALUES(chat_timestamp),
            is_group = VALUES(is_group),
            last_message_text = VALUES(last_message_text),
            last_message_type = VALUES(last_message_type),
            updated_at = NOW()
        `,
        [
            accountNumber,
            String(chat.id),
            String(
                chat.name ||
                'Unknown'
            ).slice(0, 255),

            Math.max(
                0,
                Number(chat.unread || 0)
            ),

            Math.max(
                0,
                Number(chat.timestamp || 0)
            ),

            chat.isGroup ? 1 : 0,

            lastMessageText,

            chat.lastMessageType
                ? String(chat.lastMessageType).slice(0, 64)
                : null
        ]
    );
}

async function storeChats(accountNumber, chats) {
    if (!accountNumber || !Array.isArray(chats)) {
        return;
    }

    for (const chat of chats) {
        try {
            await storeChat(accountNumber, chat);
        } catch (error) {
            console.error(
                'Failed to store chat:',
                chat?.id,
                error.message
            );
        }
    }
}

// -----------------------------
// Convert WhatsApp message
// into simple DB-safe object
// -----------------------------
function messageToPlain(
    message,
    fallbackChatId = null
) {
    const messageId =
        message?.id?._serialized ||
        message?.id?.id ||
        null;

    if (!messageId) {
        return null;
    }

    const chatId =
        message?.chatId ||
        fallbackChatId ||
        (
            message?.fromMe
                ? message?.to
                : message?.from
        ) ||
        null;

    if (!chatId) {
        return null;
    }

    return {
        id: String(messageId),

        chatId: String(chatId),

        fromMe: Boolean(
            message.fromMe
        ),

        author: message.author
            ? String(message.author)
            : null,

        from: message.from
            ? String(message.from)
            : null,

        to: message.to
            ? String(message.to)
            : null,

        body:
            message.body === undefined ||
            message.body === null
                ? ''
                : String(message.body),

        type: message.type
            ? String(message.type)
            : 'chat',

        timestamp: Math.max(
            0,
            Number(message.timestamp || 0)
        ),

        senderName: null
    };
}

// -----------------------------
// Store message
// -----------------------------
async function storeMessage(
    accountNumber,
    message,
    fallbackChatId = null
) {
    const plain = messageToPlain(
        message,
        fallbackChatId
    );

    if (!plain) {
        return null;
    }

    // Try getting sender's display name
    if (
        !plain.fromMe &&
        (plain.author || plain.from)
    ) {
        try {
            const senderId =
                plain.author ||
                plain.from;

            const contact =
                await client.getContactById(
                    senderId
                );

            plain.senderName =
                contact?.pushname ||
                contact?.name ||
                contact?.shortName ||
                null;
        } catch (_) {
            // Optional only.
        }
    }

    try {
        await db.execute(
            `
            INSERT INTO chat_messages (
                account_number,
                chat_id,
                message_id,
                from_me,
                author_id,
                from_id,
                to_id,
                sender_name,
                body,
                message_type,
                message_timestamp
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON DUPLICATE KEY UPDATE
                sender_name =
                    COALESCE(
                        VALUES(sender_name),
                        sender_name
                    ),
                body = VALUES(body),
                message_type =
                    VALUES(message_type),
                message_timestamp =
                    VALUES(message_timestamp)
            `,
            [
                accountNumber,
                plain.chatId,
                plain.id,
                plain.fromMe ? 1 : 0,
                plain.author,
                plain.from,
                plain.to,
                plain.senderName,
                plain.body.slice(0, 100000),
                plain.type,
                plain.timestamp
            ]
        );
    } catch (error) {
        console.error(
            'Failed to store message:',
            plain.id,
            error.message
        );
    }

    return plain;
}

// -----------------------------
// Get stored messages
// -----------------------------
async function getStoredMessages(
    accountNumber,
    chatId
) {
    if (!accountNumber || !chatId) {
        return [];
    }

    const [rows] = await db.execute(
        `
        SELECT
            message_id AS id,
            chat_id AS chatId,
            from_me AS fromMe,
            author_id AS author,
            from_id AS \`from\`,
            to_id AS \`to\`,
            sender_name AS senderName,
            body,
            message_type AS type,
            message_timestamp AS timestamp
        FROM chat_messages
        WHERE account_number = ?
          AND chat_id = ?
        ORDER BY
            message_timestamp ASC,
            message_id ASC
        LIMIT ${MESSAGE_LIMIT}
        `,
        [
            accountNumber,
            chatId
        ]
    );

    return rows.map(row => ({
        ...row,

        fromMe: Boolean(
            row.fromMe
        ),

        timestamp: Number(
            row.timestamp || 0
        )
    }));
}

// -----------------------------
// Account number
// -----------------------------
function getAccountNumber() {
    const user =
        client?.info?.wid?.user;

    if (!user) {
        return null;
    }

    return `+${user}`;
}

function getConnectedText() {
    const accountNumber =
        getAccountNumber() ||
        'Unknown';

    return `CONNECTED AS ${accountNumber}`;
}

// -----------------------------
// Fetch live chats
// -----------------------------
async function fetchLiveChats() {
    let lastError = null;

    for (
        let attempt = 1;
        attempt <= 3;
        attempt++
    ) {
        try {
            // Proper whatsapp-web.js API
            const chats =
                await client.getChats();

            const mapped =
                chats
                    .filter(
                        chat =>
                            chat &&
                            chat.id &&
                            chat.id._serialized
                    )
                    .map(chat => {
                        const timestamp =
                            Number(
                                chat.timestamp || 0
                            );

                        return {
                            id: String(
                                chat.id._serialized
                            ),

                            name: String(
                                chat.name ||
                                chat.formattedTitle ||
                                chat.id.user ||
                                'Unknown'
                            ),

                            unread: Number(
                                chat.unreadCount || 0
                            ),

                            timestamp,

                            isGroup: Boolean(
                                chat.isGroup
                            ),

                            lastMessageText:
                                chat.lastMessage
                                    ? String(
                                        chat.lastMessage.body ||
                                        ''
                                    )
                                    : null,

                            lastMessageType:
                                chat.lastMessage?.type
                                    ? String(
                                        chat.lastMessage.type
                                    )
                                    : null
                        };
                    })
                    .sort((a, b) => {
                        if (
                            b.timestamp !==
                            a.timestamp
                        ) {
                            return (
                                b.timestamp -
                                a.timestamp
                            );
                        }

                        return a.name.localeCompare(
                            b.name
                        );
                    })
                    .slice(
                        0,
                        CHAT_LIMIT
                    );

            console.log(
                `Found ${mapped.length} live chats using client.getChats().`
            );

            return mapped;
        } catch (error) {
            lastError = error;

            console.error(
                `getChats attempt ${attempt}/3 failed:`,
                error.message
            );

            if (attempt < 3) {
                await new Promise(
                    resolve =>
                        setTimeout(
                            resolve,
                            1500 * attempt
                        )
                );
            }
        }
    }

    throw (
        lastError ||
        new Error(
            'Unable to fetch chats.'
        )
    );
}

// -----------------------------
// Send chats to browser
// -----------------------------
async function emitChatsToSocket(
    socket,
    accountNumber
) {
    if (!accountNumber) {
        socket.emit(
            'chats',
            []
        );

        return;
    }

    // First restore from MySQL
    // so browser refresh doesn't show empty.
    const stored =
        await getStoredChats(
            accountNumber
        );

    socket.emit(
        'chats',
        stored
    );

    // Then refresh with live WhatsApp data.
    if (isReady) {
        try {
            const liveChats =
                await fetchLiveChats();

            await storeChats(
                accountNumber,
                liveChats
            );

            const latest =
                await getStoredChats(
                    accountNumber
                );

            socket.emit(
                'chats',
                latest
            );
        } catch (error) {
            console.error(
                'Live chat sync failed:',
                error.message
            );

            // Stored chats already sent.
        }
    }
}

// -----------------------------
// Ready state for browser
// -----------------------------
async function emitReadyState(socket) {
    const accountNumber =
        getAccountNumber();

    socket.emit(
        'ready',
        {
            pushName:
                client?.info?.pushname ||
                'WhatsApp',

            accountNumber,

            text:
                getConnectedText()
        }
    );

    if (accountNumber) {
        await setSessionAccount(
            accountNumber
        );

        await emitChatsToSocket(
            socket,
            accountNumber
        );
    }
}

// -----------------------------
// Fetch messages for one chat
// -----------------------------
async function fetchChatMessages(
    chatId
) {
    if (!isReady) {
        return [];
    }

    const chat =
        await client.getChatById(
            chatId
        );

    if (!chat) {
        throw new Error(
            'Chat not found.'
        );
    }

    const messages =
        await chat.fetchMessages({
            limit: MESSAGE_LIMIT
        });

    const accountNumber =
        getAccountNumber();

    const plainMessages = [];

    for (const message of messages || []) {
        const stored =
            await storeMessage(
                accountNumber,
                message,
                chatId
            );

        if (stored) {
            plainMessages.push(
                stored
            );
        }
    }

    plainMessages.sort(
        (a, b) => {
            if (
                a.timestamp !==
                b.timestamp
            ) {
                return (
                    a.timestamp -
                    b.timestamp
                );
            }

            return a.id.localeCompare(
                b.id
            );
        }
    );

    return plainMessages;
}

// -----------------------------
// WhatsApp Client
// -----------------------------
const authPath =
    process.env.WWEBJS_AUTH_PATH ||
    path.join(
        __dirname,
        '.wwebjs_auth'
    );

const client = new Client({
    authStrategy: new LocalAuth({
        dataPath: authPath
    }),

    puppeteer: {
        executablePath:
            process.env.PUPPETEER_EXECUTABLE_PATH ||
            undefined,

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

let isReady = false;

// -----------------------------
// Socket.IO
// -----------------------------
io.on(
    'connection',
    async socket => {
        console.log(
            'Frontend connected via WebSockets:',
            socket.id
        );

        try {
            const suppliedToken =
                socket.handshake.auth
                    ?.sessionToken ||
                null;

            const session =
                await createOrRestoreSession(
                    suppliedToken
                );

            socket.data.sessionToken =
                session.session_token;

            socket.data.accountNumber =
                session.account_number ||
                null;

            socket.emit(
                'session',
                {
                    token:
                        session.session_token,

                    accountNumber:
                        session.account_number ||
                        null
                }
            );

            if (isReady) {
                await emitReadyState(
                    socket
                );
            } else {
                const storedAccount =
                    session.account_number;

                if (storedAccount) {
                    const storedChats =
                        await getStoredChats(
                            storedAccount
                        );

                    if (
                        storedChats.length
                    ) {
                        socket.emit(
                            'chats',
                            storedChats
                        );

                        socket.emit(
                            'message',
                            'Restored recent chats from database. Waiting for WhatsApp...'
                        );
                    } else {
                        socket.emit(
                            'message',
                            'Connecting to WhatsApp Client...'
                        );
                    }
                } else {
                    socket.emit(
                        'message',
                        'Connecting to WhatsApp Client...'
                    );
                }
            }
        } catch (error) {
            console.error(
                'Failed to initialize browser session:',
                error
            );

            socket.emit(
                'message',
                `Session error: ${error.message}`
            );
        }

        // -------------------------
        // Browser asks for chats
        // -------------------------
        socket.on(
            'getChats',
            async () => {
                try {
                    await touchSession(
                        socket.data.sessionToken
                    );

                    const accountNumber =
                        getAccountNumber() ||
                        socket.data.accountNumber;

                    if (!accountNumber) {
                        socket.emit(
                            'chats',
                            []
                        );

                        socket.emit(
                            'message',
                            'WhatsApp is not connected yet.'
                        );

                        return;
                    }

                    socket.data.accountNumber =
                        accountNumber;

                    await emitChatsToSocket(
                        socket,
                        accountNumber
                    );
                } catch (error) {
                    console.error(
                        'getChats failed:',
                        error
                    );

                    socket.emit(
                        'message',
                        `Failed to fetch recent chats: ${error.message}`
                    );
                }
            }
        );

        // -------------------------
        // Browser asks for messages
        // -------------------------
        socket.on(
            'getMessages',
            async chatId => {
                try {
                    await touchSession(
                        socket.data.sessionToken
                    );

                    if (
                        !chatId ||
                        typeof chatId !== 'string'
                    ) {
                        socket.emit(
                            'messagesError',
                            'Invalid chat ID.'
                        );

                        return;
                    }

                    const accountNumber =
                        getAccountNumber() ||
                        socket.data.accountNumber;

                    if (!accountNumber) {
                        socket.emit(
                            'messagesError',
                            'WhatsApp account is not connected.'
                        );

                        return;
                    }

                    socket.data.accountNumber =
                        accountNumber;

                    // First return stored messages
                    const storedMessages =
                        await getStoredMessages(
                            accountNumber,
                            chatId
                        );

                    socket.emit(
                        'messages',
                        {
                            chatId,
                            messages:
                                storedMessages
                        }
                    );

                    // Then refresh from WhatsApp
                    if (isReady) {
                        try {
                            const liveMessages =
                                await fetchChatMessages(
                                    chatId
                                );

                            if (
                                liveMessages.length
                            ) {
                                const latest =
                                    await getStoredMessages(
                                        accountNumber,
                                        chatId
                                    );

                                socket.emit(
                                    'messages',
                                    {
                                        chatId,
                                        messages:
                                            latest
                                    }
                                );
                            }
                        } catch (error) {
                            console.error(
                                `Failed to refresh messages for ${chatId}:`,
                                error.message
                            );

                            // DB copy already sent.
                        }
                    }
                } catch (error) {
                    console.error(
                        'getMessages failed:',
                        error
                    );

                    socket.emit(
                        'messagesError',
                        `Failed to fetch messages: ${error.message}`
                    );
                }
            }
        );

        socket.on(
            'disconnect',
            () => {
                console.log(
                    'Frontend disconnected:',
                    socket.id
                );
            }
        );
    }
);

// -----------------------------
// QR
// -----------------------------
client.on(
    'qr',
    qr => {
        console.log(
            'QR Code generated. Waiting for scan...'
        );

        qrcode.toDataURL(
            qr,
            (err, url) => {
                if (err) {
                    console.error(
                        'QR generation failed:',
                        err
                    );

                    return;
                }

                io.emit(
                    'qr',
                    url
                );

                io.emit(
                    'message',
                    'Please scan the QR code with your WhatsApp app.'
                );
            }
        );
    }
);

// -----------------------------
// Authenticated
// -----------------------------
client.on(
    'authenticated',
    () => {
        console.log(
            'WhatsApp successfully authenticated!'
        );

        io.emit(
            'message',
            'Authenticated successfully! Loading...'
        );
    }
);

// -----------------------------
// Auth failure
// -----------------------------
client.on(
    'auth_failure',
    msg => {
        isReady = false;

        console.error(
            'AUTHENTICATION FAILURE:',
            msg
        );

        io.emit(
            'message',
            'Authentication failed. Please scan the new QR code.'
        );
    }
);

// -----------------------------
// WhatsApp ready
// -----------------------------
client.on(
    'ready',
    async () => {
        isReady = true;

        const accountNumber =
            getAccountNumber();

        console.log(
            'WhatsApp Client is ready.'
        );

        console.log(
            'Connected account:',
            accountNumber
        );

        if (accountNumber) {
            // Store account number in sessions
            await db.execute(
                `
                UPDATE sessions
                SET account_number = ?,
                    last_seen_at = NOW()
                `,
                [accountNumber]
            );
        }

        io.emit(
            'ready',
            {
                pushName:
                    client?.info?.pushname ||
                    'WhatsApp',

                accountNumber,

                text:
                    getConnectedText()
            }
        );

        io.emit(
            'message',
            'Fetching recent chats...'
        );

        try {
            const chats =
                await fetchLiveChats();

            if (accountNumber) {
                await storeChats(
                    accountNumber,
                    chats
                );

                const latest =
                    await getStoredChats(
                        accountNumber
                    );

                io.emit(
                    'chats',
                    latest
                );

                io.emit(
                    'message',
                    `Successfully loaded ${latest.length} recent chats.`
                );
            }
        } catch (error) {
            console.error(
                'Failed to fetch recent chats:',
                error
            );

            io.emit(
                'message',
                `Could not refresh live chats. Saved chats remain available: ${error.message}`
            );

            if (accountNumber) {
                try {
                    io.emit(
                        'chats',
                        await getStoredChats(
                            accountNumber
                        )
                    );
                } catch (_) {
                    // Nothing else to do
                }
            }
        }
    }
);

// -----------------------------
// New message
// -----------------------------
client.on(
    'message_create',
    async message => {
        if (!isReady) return;

        const accountNumber =
            getAccountNumber();

        if (!accountNumber) return;

        const plain =
            await storeMessage(
                accountNumber,
                message
            );

        if (!plain) return;

        try {
            const chat =
                await message.getChat();

            await storeChat(
                accountNumber,
                {
                    id:
                        chat.id._serialized,

                    name:
                        chat.name ||
                        chat.formattedTitle ||
                        chat.id.user ||
                        'Unknown',

                    unread:
                        Number(
                            chat.unreadCount ||
                            0
                        ),

                    timestamp:
                        Number(
                            chat.timestamp ||
                            plain.timestamp ||
                            0
                        ),

                    isGroup:
                        Boolean(
                            chat.isGroup
                        ),

                    lastMessageText:
                        chat.lastMessage
                            ?.body ||
                        plain.body,

                    lastMessageType:
                        chat.lastMessage
                            ?.type ||
                        plain.type
                }
            );
        } catch (error) {
            console.error(
                'Failed to update recent chat after message:',
                error.message
            );
        }

        io.emit(
            'newMessage',
            plain
        );
    }
);

// -----------------------------
// Disconnected
// -----------------------------
client.on(
    'disconnected',
    reason => {
        isReady = false;

        console.log(
            'Client disconnected:',
            reason
        );

        io.emit(
            'whatsappDisconnected',
            String(
                reason || 'Disconnected'
            )
        );

        io.emit(
            'message',
            'WhatsApp disconnected. Please wait for reconnection or scan the QR code.'
        );
    }
);

// -----------------------------
// Start server
// -----------------------------
async function start() {
    await initDatabase();

    client.initialize().catch(
        err => {
            console.error(
                'Failed to initialize WhatsApp client:',
                err
            );
        }
    );

    server.listen(
        PORT,
        () => {
            console.log(
                `Server is running on port ${PORT}`
            );
        }
    );
}

start().catch(
    error => {
        console.error(
            'Fatal startup error:',
            error
        );

        process.exit(1);
    }
);