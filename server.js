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

const PORT = process.env.PORT || 3000;

const CHAT_LIMIT = 50;
const MESSAGE_LIMIT = 100;

app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

process.on('unhandledRejection', err => {
    console.error('Unhandled Promise Rejection:', err);
});

process.on('uncaughtException', err => {
    console.error('Uncaught Exception:', err);
});

// =====================================================
// DATABASE
// =====================================================

let db = null;

function createDatabasePool() {
    const urlString =
        process.env.MYSQL_URL ||
        process.env.DATABASE_URL;

    if (urlString) {
        const url = new URL(urlString);

        return mysql.createPool({
            host: url.hostname,
            port: Number(url.port || 3306),
            user: decodeURIComponent(url.username),
            password: decodeURIComponent(url.password),
            database: decodeURIComponent(
                url.pathname.replace(/^\/+/, '')
            ),
            charset: 'utf8mb4',
            waitForConnections: true,
            connectionLimit: 10,
            queueLimit: 0
        });
    }

    return mysql.createPool({
        host: process.env.MYSQLHOST,
        port: Number(process.env.MYSQLPORT || 3306),
        user: process.env.MYSQLUSER,
        password: process.env.MYSQLPASSWORD,
        database: process.env.MYSQLDATABASE,
        charset: 'utf8mb4',
        waitForConnections: true,
        connectionLimit: 10,
        queueLimit: 0
    });
}

async function initDatabase() {
    try {
        db = createDatabasePool();

        await db.query('SELECT 1');

        console.log('MySQL connected.');

        await db.query(`
            CREATE TABLE IF NOT EXISTS sessions (
                id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
                session_token CHAR(64) NOT NULL,
                account_number VARCHAR(32) DEFAULT NULL,
                created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                last_seen_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
                    ON UPDATE CURRENT_TIMESTAMP,
                PRIMARY KEY (id),
                UNIQUE KEY unique_session_token (session_token)
            )
            ENGINE=InnoDB
            DEFAULT CHARSET=utf8mb4
        `);

        await db.query(`
            CREATE TABLE IF NOT EXISTS recent_chats (
                account_number VARCHAR(32) NOT NULL,
                chat_id VARCHAR(255) NOT NULL,
                chat_name VARCHAR(255) DEFAULT 'Unknown',
                unread_count INT NOT NULL DEFAULT 0,
                chat_timestamp BIGINT NOT NULL DEFAULT 0,
                is_group TINYINT(1) NOT NULL DEFAULT 0,
                PRIMARY KEY (account_number, chat_id),
                INDEX idx_recent (
                    account_number,
                    chat_timestamp
                )
            )
            ENGINE=InnoDB
            DEFAULT CHARSET=utf8mb4
        `);

        await db.query(`
            CREATE TABLE IF NOT EXISTS chat_messages (
                account_number VARCHAR(32) NOT NULL,
                chat_id VARCHAR(255) NOT NULL,
                message_id VARCHAR(255) NOT NULL,
                from_me TINYINT(1) NOT NULL DEFAULT 0,
                sender_id VARCHAR(255) DEFAULT NULL,
                receiver_id VARCHAR(255) DEFAULT NULL,
                body MEDIUMTEXT,
                message_type VARCHAR(100) DEFAULT NULL,
                message_timestamp BIGINT NOT NULL DEFAULT 0,
                PRIMARY KEY (
                    account_number,
                    message_id
                ),
                INDEX idx_chat_messages (
                    account_number,
                    chat_id,
                    message_timestamp
                )
            )
            ENGINE=InnoDB
            DEFAULT CHARSET=utf8mb4
        `);

        console.log('Database tables ready.');
    } catch (error) {
        console.error(
            'MySQL unavailable:',
            error.message
        );

        /*
         * IMPORTANT:
         * Do NOT stop WhatsApp because MySQL is unavailable.
         * Chats can still load.
         */
        db = null;
    }
}

// =====================================================
// SESSION
// =====================================================

async function createBrowserSession(token) {
    if (db && token) {
        try {
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

            if (rows.length > 0) {
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
        } catch (error) {
            console.error(
                'Session lookup failed:',
                error.message
            );
        }
    }

    const newToken =
        crypto.randomBytes(32).toString('hex');

    if (db) {
        try {
            await db.execute(
                `
                INSERT INTO sessions (
                    session_token
                )
                VALUES (?)
                `,
                [newToken]
            );
        } catch (error) {
            console.error(
                'Session insert failed:',
                error.message
            );
        }
    }

    return {
        session_token: newToken,
        account_number: null
    };
}

async function saveAccountNumber(accountNumber) {
    if (!db || !accountNumber) {
        return;
    }

    try {
        await db.execute(
            `
            UPDATE sessions
            SET account_number = ?
            `,
            [accountNumber]
        );
    } catch (error) {
        console.error(
            'Failed to save account:',
            error.message
        );
    }
}

// =====================================================
// WHATSAPP CLIENT
// =====================================================

const authPath =
    process.env.WWEBJS_AUTH_PATH ||
    path.join(__dirname, '.wwebjs_auth');

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
            '--disable-gpu',
            '--disable-accelerated-2d-canvas',
            '--no-first-run',
            '--no-zygote'
        ]
    }
});

let whatsappReady = false;

function getAccountNumber() {
    const user =
        client?.info?.wid?.user;

    if (!user) {
        return null;
    }

    return `+${user}`;
}

// =====================================================
// DIRECT CHAT EXTRACTION
// DO NOT USE client.getChats()
// =====================================================

async function getRecentChatsDirect() {
    if (!client.pupPage) {
        throw new Error(
            'WhatsApp browser page is not available.'
        );
    }

    const chats =
        await client.pupPage.evaluate(
            async () => {
                const collection =
                    window.require(
                        'WAWebCollections'
                    );

                if (
                    !collection ||
                    !collection.Chat
                ) {
                    throw new Error(
                        'WhatsApp Chat collection unavailable.'
                    );
                }

                const models =
                    collection.Chat.getModelsArray();

                return models.map(chat => {
                    let id = null;

                    try {
                        if (
                            chat.id?._serialized
                        ) {
                            id =
                                chat.id._serialized;
                        } else if (
                            chat.id?.user &&
                            chat.id?.server
                        ) {
                            id =
                                `${chat.id.user}@${chat.id.server}`;
                        } else if (
                            chat.id?.user
                        ) {
                            id =
                                String(
                                    chat.id.user
                                );
                        }
                    } catch (_) {
                        id = null;
                    }

                    if (!id) {
                        return null;
                    }

                    let name =
                        'Unknown';

                    try {
                        name =
                            chat.formattedTitle ||
                            chat.name ||
                            chat.pushname ||
                            chat.id?.user ||
                            'Unknown';
                    } catch (_) {}

                    let timestamp = 0;

                    try {
                        /*
                         * WhatsApp stores the chat's latest
                         * activity timestamp in `t`.
                         */
                        timestamp =
                            Number(
                                chat.t || 0
                            );
                    } catch (_) {
                        timestamp = 0;
                    }

                    let unread = 0;

                    try {
                        unread =
                            Number(
                                chat.unreadCount ||
                                0
                            );
                    } catch (_) {
                        unread = 0;
                    }

                    let isGroup = false;

                    try {
                        isGroup =
                            Boolean(
                                chat.groupMetadata
                            );
                    } catch (_) {
                        isGroup = false;
                    }

                    return {
                        id: String(id),

                        name: String(
                            name || 'Unknown'
                        ),

                        timestamp,

                        unread,

                        isGroup
                    };
                });
            }
        );

    return chats
        .filter(Boolean)
        .sort(
            (a, b) =>
                Number(b.timestamp || 0) -
                Number(a.timestamp || 0)
        )
        .slice(0, CHAT_LIMIT);
}

// =====================================================
// SAVE RECENT CHATS
// =====================================================

async function saveRecentChats(
    accountNumber,
    chats
) {
    if (!db || !accountNumber) {
        return;
    }

    for (const chat of chats) {
        try {
            await db.execute(
                `
                INSERT INTO recent_chats (
                    account_number,
                    chat_id,
                    chat_name,
                    unread_count,
                    chat_timestamp,
                    is_group
                )
                VALUES (?, ?, ?, ?, ?, ?)
                ON DUPLICATE KEY UPDATE
                    chat_name = VALUES(chat_name),
                    unread_count = VALUES(unread_count),
                    chat_timestamp = VALUES(chat_timestamp),
                    is_group = VALUES(is_group)
                `,
                [
                    accountNumber,
                    chat.id,
                    chat.name,
                    Number(
                        chat.unread || 0
                    ),
                    Number(
                        chat.timestamp || 0
                    ),
                    chat.isGroup ? 1 : 0
                ]
            );
        } catch (error) {
            console.error(
                'Failed saving chat:',
                error.message
            );
        }
    }
}

async function getStoredChats(
    accountNumber
) {
    if (!db || !accountNumber) {
        return [];
    }

    try {
        const [rows] =
            await db.execute(
                `
                SELECT
                    chat_id AS id,
                    chat_name AS name,
                    unread_count AS unread,
                    chat_timestamp AS timestamp,
                    is_group AS isGroup
                FROM recent_chats
                WHERE account_number = ?
                ORDER BY
                    chat_timestamp DESC
                LIMIT ${CHAT_LIMIT}
                `,
                [accountNumber]
            );

        return rows;
    } catch (error) {
        console.error(
            'Failed reading saved chats:',
            error.message
        );

        return [];
    }
}

// =====================================================
// MESSAGES
// =====================================================

function serializeMessage(
    message,
    chatId
) {
    if (!message) {
        return null;
    }

    const messageId =
        message?.id?._serialized ||
        message?.id?.id;

    if (!messageId) {
        return null;
    }

    return {
        id: String(messageId),

        chatId: String(
            chatId ||
            message.chatId ||
            message.from ||
            message.to ||
            ''
        ),

        fromMe:
            Boolean(
                message.fromMe
            ),

        senderId:
            message.author ||
            message.from ||
            null,

        receiverId:
            message.to ||
            null,

        body:
            message.body == null
                ? ''
                : String(
                    message.body
                ),

        type:
            message.type ||
            'chat',

        timestamp:
            Number(
                message.timestamp ||
                0
            )
    };
}

async function saveMessage(
    accountNumber,
    message,
    chatId
) {
    if (!db || !accountNumber) {
        return;
    }

    const data =
        serializeMessage(
            message,
            chatId
        );

    if (!data) {
        return;
    }

    try {
        await db.execute(
            `
            INSERT INTO chat_messages (
                account_number,
                chat_id,
                message_id,
                from_me,
                sender_id,
                receiver_id,
                body,
                message_type,
                message_timestamp
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON DUPLICATE KEY UPDATE
                body = VALUES(body),
                message_type =
                    VALUES(message_type),
                message_timestamp =
                    VALUES(message_timestamp)
            `,
            [
                accountNumber,
                data.chatId,
                data.id,
                data.fromMe ? 1 : 0,
                data.senderId,
                data.receiverId,
                data.body,
                data.type,
                data.timestamp
            ]
        );
    } catch (error) {
        console.error(
            'Failed saving message:',
            error.message
        );
    }
}

async function getStoredMessages(
    accountNumber,
    chatId
) {
    if (!db || !accountNumber) {
        return [];
    }

    try {
        const [rows] =
            await db.execute(
                `
                SELECT
                    message_id AS id,
                    chat_id AS chatId,
                    from_me AS fromMe,
                    sender_id AS senderId,
                    receiver_id AS receiverId,
                    body,
                    message_type AS type,
                    message_timestamp AS timestamp
                FROM chat_messages
                WHERE account_number = ?
                  AND chat_id = ?
                ORDER BY
                    message_timestamp ASC
                LIMIT ${MESSAGE_LIMIT}
                `,
                [
                    accountNumber,
                    chatId
                ]
            );

        return rows.map(
            row => ({
                ...row,

                fromMe:
                    Boolean(
                        row.fromMe
                    ),

                timestamp:
                    Number(
                        row.timestamp || 0
                    )
            })
        );
    } catch (error) {
        console.error(
            'Failed loading stored messages:',
            error.message
        );

        return [];
    }
}

// =====================================================
// GET MESSAGES FROM WHATSAPP
// =====================================================

async function fetchMessagesForChat(
    accountNumber,
    chatId
) {
    if (!whatsappReady) {
        return [];
    }

    /*
     * We deliberately use the direct Chat collection
     * instead of client.getChatById(), because current
     * whatsapp-web.js builds have known Puppeteer errors
     * around getChats/getChatById after WhatsApp Web changes.
     */
    const result =
        await client.pupPage.evaluate(
            async (
                chatId,
                limit
            ) => {
                const collections =
                    window.require(
                        'WAWebCollections'
                    );

                const widFactory =
                    window.require(
                        'WAWebWidFactory'
                    );

                const wid =
                    widFactory.createWid(
                        chatId
                    );

                let chat =
                    collections.Chat.get(
                        wid
                    );

                if (!chat) {
                    try {
                        chat =
                            (
                                await window
                                    .require(
                                        'WAWebFindChatAction'
                                    )
                                    .findOrCreateLatestChat(
                                        wid
                                    )
                            )?.chat;
                    } catch (_) {}
                }

                if (!chat) {
                    throw new Error(
                        'Chat was not found in WhatsApp.'
                    );
                }

                let messages =
                    chat.msgs?.getModelsArray
                        ? chat.msgs.getModelsArray()
                        : [];

                /*
                 * Load earlier messages when the
                 * currently cached messages are fewer
                 * than requested.
                 */
                if (
                    messages.length <
                    limit
                ) {
                    try {
                        while (
                            messages.length <
                            limit
                        ) {
                            const loaded =
                                await window
                                    .require(
                                        'WAWebChatLoadMessages'
                                    )
                                    .loadEarlierMsgs(
                                        {
                                            chat
                                        }
                                    );

                            if (
                                !loaded ||
                                !loaded.length
                            ) {
                                break;
                            }

                            messages =
                                chat.msgs.getModelsArray();

                            if (
                                messages.length >=
                                limit
                            ) {
                                break;
                            }
                        }
                    } catch (_) {
                        // Use whatever is cached.
                    }
                }

                messages =
                    messages.slice(
                        -limit
                    );

                return messages.map(
                    message => {
                        const serialized =
                            message.serialize();

                        /*
                         * Keep only JSON-safe fields.
                         */
                        return {
                            id:
                                message.id?._serialized ||
                                serialized.id?._serialized ||
                                serialized.id?.id ||
                                null,

                            fromMe:
                                Boolean(
                                    message.id?.fromMe ??
                                    serialized.id?.fromMe ??
                                    message.fromMe
                                ),

                            from:
                                message.from?._serialized ||
                                serialized.from?._serialized ||
                                serialized.from ||
                                null,

                            to:
                                message.to?._serialized ||
                                serialized.to?._serialized ||
                                serialized.to ||
                                null,

                            author:
                                message.author?._serialized ||
                                serialized.author?._serialized ||
                                serialized.author ||
                                null,

                            body:
                                message.body ??
                                serialized.body ??
                                '',

                            type:
                                message.type ||
                                serialized.type ||
                                'chat',

                            timestamp:
                                Number(
                                    message.t ||
                                    message.timestamp ||
                                    serialized.t ||
                                    serialized.timestamp ||
                                    0
                                )
                        };
                    }
                );
            },
            chatId,
            MESSAGE_LIMIT
        );

    const clean =
        result
            .filter(
                message =>
                    message &&
                    message.id
            )
            .map(
                message => ({
                    id:
                        String(
                            message.id
                        ),

                    chatId:
                        String(
                            chatId
                        ),

                    fromMe:
                        Boolean(
                            message.fromMe
                        ),

                    senderId:
                        message.author ||
                        message.from ||
                        null,

                    receiverId:
                        message.to ||
                        null,

                    body:
                        String(
                            message.body || ''
                        ),

                    type:
                        message.type ||
                        'chat',

                    timestamp:
                        Number(
                            message.timestamp ||
                            0
                        )
                })
            )
            .sort(
                (a, b) =>
                    a.timestamp -
                    b.timestamp
            );

    for (const message of clean) {
        await saveMessage(
            accountNumber,
            message,
            chatId
        );
    }

    return clean;
}

// =====================================================
// SEND CHATS TO FRONTEND
// =====================================================

async function sendChats(socket) {
    const accountNumber =
        getAccountNumber() ||
        socket.data.accountNumber ||
        null;

    /*
     * ALWAYS send database chats first.
     * Therefore browser refresh does NOT make
     * the chat list disappear.
     */
    if (accountNumber) {
        const saved =
            await getStoredChats(
                accountNumber
            );

        if (saved.length > 0) {
            socket.emit(
                'chats',
                saved
            );
        }
    }

    /*
     * Then get fresh chats from WhatsApp.
     */
    if (!whatsappReady) {
        return;
    }

    try {
        const liveChats =
            await getRecentChatsDirect();

        console.log(
            `Direct chat extraction found ${liveChats.length} chats.`
        );

        if (accountNumber) {
            await saveRecentChats(
                accountNumber,
                liveChats
            );

            const latest =
                await getStoredChats(
                    accountNumber
                );

            socket.emit(
                'chats',
                latest.length
                    ? latest
                    : liveChats
            );
        } else {
            socket.emit(
                'chats',
                liveChats
            );
        }

        socket.emit(
            'message',
            `Loaded ${
                liveChats.length
            } recent chats.`
        );
    } catch (error) {
        console.error(
            'DIRECT CHAT FETCH ERROR:',
            error
        );

        socket.emit(
            'message',
            `Live chat fetch failed: ${error.message}`
        );
    }
}

// =====================================================
// SOCKET.IO
// =====================================================

io.on(
    'connection',
    async socket => {
        console.log(
            'Browser connected:',
            socket.id
        );

        try {
            const token =
                socket.handshake.auth
                    ?.sessionToken ||
                null;

            const session =
                await createBrowserSession(
                    token
                );

            socket.data.sessionToken =
                session.session_token;

            socket.data.accountNumber =
                session.account_number;

            socket.emit(
                'session',
                {
                    token:
                        session.session_token,

                    accountNumber:
                        session.account_number
                }
            );

            if (whatsappReady) {
                const account =
                    getAccountNumber();

                socket.data.accountNumber =
                    account;

                await saveAccountNumber(
                    account
                );

                socket.emit(
                    'ready',
                    {
                        accountNumber:
                            account,

                        pushName:
                            client.info?.pushname ||
                            'WhatsApp'
                    }
                );

                await sendChats(
                    socket
                );
            } else {
                /*
                 * Even when WhatsApp is not ready,
                 * restore chats from DB.
                 */
                if (
                    session.account_number
                ) {
                    const saved =
                        await getStoredChats(
                            session.account_number
                        );

                    if (saved.length) {
                        socket.emit(
                            'chats',
                            saved
                        );
                    }
                }

                socket.emit(
                    'message',
                    'Connecting to WhatsApp...'
                );
            }
        } catch (error) {
            console.error(
                'Socket initialization error:',
                error
            );

            socket.emit(
                'message',
                `Session error: ${error.message}`
            );
        }

        // ---------------------------------------------
        // MANUAL CHAT REFRESH
        // ---------------------------------------------

        socket.on(
            'getChats',
            async () => {
                console.log(
                    'Browser requested chats.'
                );

                await sendChats(
                    socket
                );
            }
        );

        // ---------------------------------------------
        // GET CHAT MESSAGES
        // ---------------------------------------------

        socket.on(
            'getMessages',
            async chatId => {
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

                const account =
                    getAccountNumber() ||
                    socket.data.accountNumber;

                /*
                 * Return database messages immediately.
                 */
                if (account) {
                    const saved =
                        await getStoredMessages(
                            account,
                            chatId
                        );

                    socket.emit(
                        'messages',
                        {
                            chatId,
                            messages:
                                saved
                        }
                    );
                }

                /*
                 * Then refresh them from WhatsApp.
                 */
                if (!whatsappReady) {
                    return;
                }

                try {
                    const live =
                        await fetchMessagesForChat(
                            account,
                            chatId
                        );

                    socket.emit(
                        'messages',
                        {
                            chatId,
                            messages:
                                live
                        }
                    );
                } catch (error) {
                    console.error(
                        `Message fetch failed for ${chatId}:`,
                        error
                    );

                    socket.emit(
                        'messagesError',
                        error.message
                    );
                }
            }
        );

        socket.on(
            'disconnect',
            () => {
                console.log(
                    'Browser disconnected:',
                    socket.id
                );
            }
        );
    }
);

// =====================================================
// WHATSAPP EVENTS
// =====================================================

client.on(
    'qr',
    qr => {
        console.log(
            'QR generated.'
        );

        qrcode.toDataURL(
            qr,
            (err, url) => {
                if (err) {
                    console.error(
                        'QR error:',
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
                    'Please scan the QR code.'
                );
            }
        );
    }
);

client.on(
    'authenticated',
    () => {
        console.log(
            'WhatsApp authenticated.'
        );

        io.emit(
            'message',
            'Authenticated successfully.'
        );
    }
);

client.on(
    'auth_failure',
    error => {
        whatsappReady = false;

        console.error(
            'WhatsApp authentication failure:',
            error
        );

        io.emit(
            'message',
            'WhatsApp authentication failed.'
        );
    }
);

client.on(
    'ready',
    async () => {
        whatsappReady = true;

        const account =
            getAccountNumber();

        console.log(
            '================================='
        );

        console.log(
            'WHATSAPP READY'
        );

        console.log(
            'CONNECTED AS:',
            account
        );

        console.log(
            '================================='
        );

        await saveAccountNumber(
            account
        );

        io.emit(
            'ready',
            {
                accountNumber:
                    account,

                pushName:
                    client.info?.pushname ||
                    'WhatsApp'
            }
        );

        io.emit(
            'message',
            'WhatsApp connected. Fetching recent chats...'
        );

        /*
         * Fetch once after ready.
         */
        try {
            const liveChats =
                await getRecentChatsDirect();

            console.log(
                `READY: found ${liveChats.length} chats`
            );

            if (account) {
                await saveRecentChats(
                    account,
                    liveChats
                );
            }

            io.emit(
                'chats',
                account
                    ? await getStoredChats(
                        account
                    )
                    : liveChats
            );

            io.emit(
                'message',
                `Loaded ${liveChats.length} recent chats.`
            );
        } catch (error) {
            console.error(
                'READY CHAT FETCH ERROR:',
                error
            );

            /*
             * Send old database copy if available.
             */
            if (account) {
                const saved =
                    await getStoredChats(
                        account
                    );

                if (saved.length) {
                    io.emit(
                        'chats',
                        saved
                    );
                }
            }

            io.emit(
                'message',
                `Could not refresh live chats: ${error.message}`
            );
        }
    }
);

client.on(
    'message_create',
    async message => {
        const account =
            getAccountNumber();

        if (
            !account ||
            !message
        ) {
            return;
        }

        const chatId =
            message.fromMe
                ? message.to
                : message.from;

        await saveMessage(
            account,
            message,
            chatId
        );
    }
);

client.on(
    'disconnected',
    reason => {
        whatsappReady = false;

        console.log(
            'WhatsApp disconnected:',
            reason
        );

        io.emit(
            'message',
            `WhatsApp disconnected: ${reason}`
        );
    }
);

// =====================================================
// START
// =====================================================

async function start() {
    /*
     * Database failure must NOT prevent
     * WhatsApp from starting.
     */
    await initDatabase();

    client.initialize().catch(
        error => {
            console.error(
                'WhatsApp initialize error:',
                error
            );
        }
    );

    server.listen(
        PORT,
        () => {
            console.log(
                `Server running on port ${PORT}`
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