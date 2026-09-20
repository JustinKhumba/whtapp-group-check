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

const CHAT_LIMIT = 50;
const MESSAGE_LIMIT = 100;

// =====================================================
// EXPRESS
// =====================================================

app.use(express.json());
app.use(
    express.static(
        path.join(__dirname, 'public')
    )
);

app.get('/', (req, res) => {
    res.sendFile(
        path.join(
            __dirname,
            'public',
            'index.html'
        )
    );
});

// =====================================================
// ERROR HANDLING
// =====================================================

process.on(
    'unhandledRejection',
    error => {
        console.error(
            'Unhandled Promise Rejection:',
            error
        );
    }
);

process.on(
    'uncaughtException',
    error => {
        console.error(
            'Uncaught Exception:',
            error
        );
    }
);

// =====================================================
// MYSQL
// =====================================================

let db = null;

function createDatabasePool() {
    const connectionUrl =
        process.env.MYSQL_URL ||
        process.env.DATABASE_URL;

    if (connectionUrl) {
        const url =
            new URL(connectionUrl);

        return mysql.createPool({
            host: url.hostname,

            port:
                Number(
                    url.port || 3306
                ),

            user:
                decodeURIComponent(
                    url.username
                ),

            password:
                decodeURIComponent(
                    url.password
                ),

            database:
                decodeURIComponent(
                    url.pathname.replace(
                        /^\/+/,
                        ''
                    )
                ),

            charset: 'utf8mb4',

            waitForConnections: true,

            connectionLimit: 10,

            queueLimit: 0
        });
    }

    return mysql.createPool({
        host:
            process.env.MYSQLHOST ||
            process.env.DB_HOST,

        port:
            Number(
                process.env.MYSQLPORT ||
                process.env.DB_PORT ||
                3306
            ),

        user:
            process.env.MYSQLUSER ||
            process.env.DB_USER,

        password:
            process.env.MYSQLPASSWORD ||
            process.env.DB_PASSWORD,

        database:
            process.env.MYSQLDATABASE ||
            process.env.DB_NAME,

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

        console.log(
            'MySQL connected.'
        );

        // ---------------------------------------------
        // BROWSER SESSIONS
        // ---------------------------------------------

        await db.query(`
            CREATE TABLE IF NOT EXISTS sessions (
                id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,

                session_token CHAR(64) NOT NULL,

                account_number VARCHAR(32)
                    DEFAULT NULL,

                created_at DATETIME
                    NOT NULL DEFAULT CURRENT_TIMESTAMP,

                last_seen_at DATETIME
                    NOT NULL DEFAULT CURRENT_TIMESTAMP
                    ON UPDATE CURRENT_TIMESTAMP,

                PRIMARY KEY (id),

                UNIQUE KEY
                    unique_session_token (
                        session_token
                    ),

                INDEX
                    idx_session_account (
                        account_number
                    )
            )
            ENGINE=InnoDB
            DEFAULT CHARSET=utf8mb4
            COLLATE=utf8mb4_unicode_ci
        `);

        // ---------------------------------------------
        // RECENT CHATS
        // ---------------------------------------------

        await db.query(`
            CREATE TABLE IF NOT EXISTS recent_chats (
                account_number VARCHAR(32) NOT NULL,

                chat_id VARCHAR(255) NOT NULL,

                chat_name VARCHAR(255)
                    NOT NULL DEFAULT 'Unknown',

                unread_count INT
                    NOT NULL DEFAULT 0,

                chat_timestamp BIGINT
                    NOT NULL DEFAULT 0,

                is_group TINYINT(1)
                    NOT NULL DEFAULT 0,

                PRIMARY KEY (
                    account_number,
                    chat_id
                ),

                INDEX idx_recent_chats (
                    account_number,
                    chat_timestamp
                )
            )
            ENGINE=InnoDB
            DEFAULT CHARSET=utf8mb4
            COLLATE=utf8mb4_unicode_ci
        `);

        // ---------------------------------------------
        // CHAT MESSAGES
        // ---------------------------------------------

        await db.query(`
            CREATE TABLE IF NOT EXISTS chat_messages (
                account_number VARCHAR(32) NOT NULL,

                chat_id VARCHAR(255) NOT NULL,

                message_id VARCHAR(255) NOT NULL,

                from_me TINYINT(1)
                    NOT NULL DEFAULT 0,

                sender_id VARCHAR(255)
                    DEFAULT NULL,

                receiver_id VARCHAR(255)
                    DEFAULT NULL,

                body MEDIUMTEXT
                    DEFAULT NULL,

                message_type VARCHAR(100)
                    DEFAULT NULL,

                message_timestamp BIGINT
                    NOT NULL DEFAULT 0,

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
            COLLATE=utf8mb4_unicode_ci
        `);

        // ---------------------------------------------
        // AUTO REPLIES
        // ---------------------------------------------

        await db.query(`
            CREATE TABLE IF NOT EXISTS auto_replies (
                account_number VARCHAR(32)
                    NOT NULL,

                enabled TINYINT(1)
                    NOT NULL DEFAULT 0,

                trigger_text VARCHAR(255)
                    NOT NULL DEFAULT 'START',

                reply_text MEDIUMTEXT
                    NOT NULL,

                updated_at DATETIME
                    NOT NULL DEFAULT CURRENT_TIMESTAMP
                    ON UPDATE CURRENT_TIMESTAMP,

                PRIMARY KEY (
                    account_number
                )
            )
            ENGINE=InnoDB
            DEFAULT CHARSET=utf8mb4
            COLLATE=utf8mb4_unicode_ci
        `);

        console.log(
            'Database tables ready.'
        );
    } catch (error) {
        console.error(
            'MySQL connection failed:',
            error.message
        );

        /*
         * Do not stop WhatsApp just because
         * the database is unavailable.
         */
        db = null;
    }
}

// =====================================================
// SESSION HELPERS
// =====================================================

async function createOrRestoreSession(
    suppliedToken
) {
    if (
        db &&
        suppliedToken &&
        /^[a-f0-9]{64}$/i.test(
            suppliedToken
        )
    ) {
        try {
            const [rows] =
                await db.execute(
                    `
                    SELECT
                        session_token,
                        account_number
                    FROM sessions
                    WHERE session_token = ?
                    LIMIT 1
                    `,
                    [suppliedToken]
                );

            if (rows.length > 0) {
                await db.execute(
                    `
                    UPDATE sessions
                    SET last_seen_at = NOW()
                    WHERE session_token = ?
                    `,
                    [suppliedToken]
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
        crypto
            .randomBytes(32)
            .toString('hex');

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
                'Session creation failed:',
                error.message
            );
        }
    }

    return {
        session_token: newToken,
        account_number: null
    };
}

async function updateSessionAccount(
    sessionToken,
    accountNumber
) {
    if (
        !db ||
        !sessionToken ||
        !accountNumber
    ) {
        return;
    }

    try {
        await db.execute(
            `
            UPDATE sessions
            SET
                account_number = ?,
                last_seen_at = NOW()
            WHERE session_token = ?
            `,
            [
                accountNumber,
                sessionToken
            ]
        );
    } catch (error) {
        console.error(
            'Session account update failed:',
            error.message
        );
    }
}

async function touchSession(
    sessionToken
) {
    if (!db || !sessionToken) {
        return;
    }

    try {
        await db.execute(
            `
            UPDATE sessions
            SET last_seen_at = NOW()
            WHERE session_token = ?
            `,
            [sessionToken]
        );
    } catch (error) {
        console.error(
            'Session touch failed:',
            error.message
        );
    }
}

// =====================================================
// WHATSAPP ACCOUNT
// =====================================================

function getAccountNumber() {
    const user =
        client?.info?.wid?.user;

    if (!user) {
        return null;
    }

    return `+${user}`;
}

// =====================================================
// RECENT CHATS
// =====================================================

async function getStoredChats(
    accountNumber
) {
    if (
        !db ||
        !accountNumber
    ) {
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
                ORDER BY chat_timestamp DESC
                LIMIT ${CHAT_LIMIT}
                `,
                [accountNumber]
            );

        return rows.map(
            row => ({
                id:
                    String(row.id),

                name:
                    String(
                        row.name ||
                        'Unknown'
                    ),

                unread:
                    Number(
                        row.unread || 0
                    ),

                timestamp:
                    Number(
                        row.timestamp || 0
                    ),

                isGroup:
                    Boolean(
                        row.isGroup
                    )
            })
        );
    } catch (error) {
        console.error(
            'getStoredChats error:',
            error.message
        );

        return [];
    }
}

async function saveRecentChats(
    accountNumber,
    chats
) {
    if (
        !db ||
        !accountNumber ||
        !Array.isArray(chats)
    ) {
        return;
    }

    for (const chat of chats) {
        if (
            !chat ||
            !chat.id
        ) {
            continue;
        }

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
                    chat_name =
                        VALUES(chat_name),

                    unread_count =
                        VALUES(unread_count),

                    chat_timestamp =
                        VALUES(chat_timestamp),

                    is_group =
                        VALUES(is_group)
                `,
                [
                    accountNumber,

                    String(
                        chat.id
                    ),

                    String(
                        chat.name ||
                        'Unknown'
                    ).slice(0, 255),

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

// =====================================================
// DIRECT CHAT FETCH
//
// We intentionally do NOT use:
// client.getChats()
//
// =====================================================

async function getRecentChatsDirect() {
    if (!client.pupPage) {
        throw new Error(
            'WhatsApp browser page is not ready.'
        );
    }

    const result =
        await client.pupPage.evaluate(
            () => {
                try {
                    const collections =
                        window.require(
                            'WAWebCollections'
                        );

                    if (
                        !collections ||
                        !collections.Chat
                    ) {
                        throw new Error(
                            'WAWebCollections.Chat is unavailable.'
                        );
                    }

                    const models =
                        collections.Chat
                            .getModelsArray();

                    return models.map(
                        chat => {
                            let id = null;

                            try {
                                if (
                                    chat.id &&
                                    chat.id._serialized
                                ) {
                                    id =
                                        chat.id._serialized;
                                } else if (
                                    chat.id &&
                                    chat.id.user &&
                                    chat.id.server
                                ) {
                                    id =
                                        `${chat.id.user}@${chat.id.server}`;
                                } else if (
                                    chat.id &&
                                    chat.id.user
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
                            } catch (_) {}

                            let isGroup =
                                false;

                            try {
                                isGroup =
                                    String(id)
                                        .endsWith(
                                            '@g.us'
                                        );
                            } catch (_) {
                                isGroup =
                                    false;
                            }

                            return {
                                id:
                                    String(id),

                                name:
                                    String(
                                        name ||
                                        'Unknown'
                                    ),

                                timestamp,

                                unread,

                                isGroup
                            };
                        }
                    );
                } catch (error) {
                    throw new Error(
                        error.message
                    );
                }
            }
        );

    return result
        .filter(Boolean)
        .sort(
            (a, b) =>
                Number(
                    b.timestamp || 0
                ) -
                Number(
                    a.timestamp || 0
                )
        )
        .slice(
            0,
            CHAT_LIMIT
        );
}

// =====================================================
// CHAT MESSAGES
// =====================================================

async function getStoredMessages(
    accountNumber,
    chatId
) {
    if (
        !db ||
        !accountNumber ||
        !chatId
    ) {
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
                id:
                    String(row.id),

                chatId:
                    String(row.chatId),

                fromMe:
                    Boolean(
                        row.fromMe
                    ),

                senderId:
                    row.senderId
                        ? String(
                            row.senderId
                        )
                        : null,

                receiverId:
                    row.receiverId
                        ? String(
                            row.receiverId
                        )
                        : null,

                body:
                    row.body == null
                        ? ''
                        : String(
                            row.body
                        ),

                type:
                    row.type ||
                    'chat',

                timestamp:
                    Number(
                        row.timestamp ||
                        0
                    )
            })
        );
    } catch (error) {
        console.error(
            'getStoredMessages error:',
            error.message
        );

        return [];
    }
}

async function saveMessageToDatabase(
    accountNumber,
    message,
    chatId
) {
    if (
        !db ||
        !accountNumber ||
        !message
    ) {
        return null;
    }

    const messageId =
        message.id?._serialized ||
        message.id?.id ||
        null;

    if (!messageId) {
        return null;
    }

    const actualChatId =
        chatId ||
        message.from ||
        message.to ||
        null;

    if (!actualChatId) {
        return null;
    }

    const data = {
        id:
            String(
                messageId
            ),

        chatId:
            String(
                actualChatId
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
                message.t ||
                0
            )
    };

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
                body =
                    VALUES(body),

                message_type =
                    VALUES(message_type),

                message_timestamp =
                    VALUES(message_timestamp)
            `,
            [
                accountNumber,

                data.chatId,

                data.id,

                data.fromMe
                    ? 1
                    : 0,

                data.senderId,

                data.receiverId,

                data.body,

                data.type,

                data.timestamp
            ]
        );

        return data;
    } catch (error) {
        console.error(
            'saveMessageToDatabase error:',
            error.message
        );

        return null;
    }
}

// =====================================================
// FETCH MESSAGES FOR ONE CHAT
// =====================================================

async function fetchMessagesForChat(
    accountNumber,
    chatId
) {
    if (!client.pupPage) {
        throw new Error(
            'WhatsApp browser page is not ready.'
        );
    }

    const messages =
        await client.pupPage.evaluate(
            async (
                requestedChatId,
                limit
            ) => {
                const collections =
                    window.require(
                        'WAWebCollections'
                    );

                if (
                    !collections ||
                    !collections.Chat
                ) {
                    throw new Error(
                        'WhatsApp Chat collection unavailable.'
                    );
                }

                let models =
                    collections.Chat
                        .getModelsArray();

                const chat =
                    models.find(
                        item => {
                            try {
                                return (
                                    item.id &&
                                    (
                                        item.id._serialized ===
                                        requestedChatId ||
                                        (
                                            item.id.user &&
                                            item.id.server &&
                                            `${item.id.user}@${item.id.server}` ===
                                                requestedChatId
                                        )
                                    )
                                );
                            } catch (_) {
                                return false;
                            }
                        }
                    );

                if (!chat) {
                    throw new Error(
                        'Chat not found in WhatsApp Web.'
                    );
                }

                /*
                 * Use messages already available
                 * in this chat model.
                 */
                let messageModels = [];

                try {
                    if (
                        chat.msgs &&
                        typeof chat.msgs.getModelsArray ===
                            'function'
                    ) {
                        messageModels =
                            chat.msgs
                                .getModelsArray();
                    }
                } catch (_) {
                    messageModels = [];
                }

                messageModels =
                    messageModels.slice(
                        -limit
                    );

                return messageModels.map(
                    message => {
                        let id = null;

                        try {
                            id =
                                message.id?._serialized ||
                                message.id?.id ||
                                null;
                        } catch (_) {}

                        if (!id) {
                            return null;
                        }

                        return {
                            id:
                                String(id),

                            fromMe:
                                Boolean(
                                    message.fromMe ||
                                    message.id?.fromMe
                                ),

                            from:
                                message.from?._serialized ||
                                message.from ||
                                null,

                            to:
                                message.to?._serialized ||
                                message.to ||
                                null,

                            author:
                                message.author?._serialized ||
                                message.author ||
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
                                    message.t ||
                                    0
                                )
                        };
                    }
                )
                .filter(Boolean);
            },
            chatId,
            MESSAGE_LIMIT
        );

    const cleaned =
        messages
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
                            message.body ||
                            ''
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

    for (
        const message of cleaned
    ) {
        if (accountNumber) {
            await saveMessageToDatabase(
                accountNumber,
                message,
                chatId
            );
        }
    }

    return cleaned;
}

// =====================================================
// AUTO REPLY
// =====================================================

async function getAutoReply(
    accountNumber
) {
    if (
        !db ||
        !accountNumber
    ) {
        return {
            enabled: false,
            triggerText: 'START',
            replyText: ''
        };
    }

    try {
        const [rows] =
            await db.execute(
                `
                SELECT
                    enabled,
                    trigger_text AS triggerText,
                    reply_text AS replyText
                FROM auto_replies
                WHERE account_number = ?
                LIMIT 1
                `,
                [accountNumber]
            );

        if (!rows.length) {
            return {
                enabled: false,
                triggerText: 'START',
                replyText: ''
            };
        }

        return {
            enabled:
                Boolean(
                    rows[0].enabled
                ),

            triggerText:
                String(
                    rows[0].triggerText ||
                    'START'
                ),

            replyText:
                String(
                    rows[0].replyText ||
                    ''
                )
        };
    } catch (error) {
        console.error(
            'getAutoReply error:',
            error.message
        );

        return {
            enabled: false,
            triggerText: 'START',
            replyText: ''
        };
    }
}

async function saveAutoReply(
    accountNumber,
    settings
) {
    if (
        !db ||
        !accountNumber
    ) {
        throw new Error(
            'Database or WhatsApp account is unavailable.'
        );
    }

    const triggerText =
        String(
            settings?.triggerText ||
            'START'
        ).trim();

    const replyText =
        String(
            settings?.replyText ||
            ''
        ).trim();

    const enabled =
        Boolean(
            settings?.enabled
        ) &&
        replyText.length > 0;

    if (!triggerText) {
        throw new Error(
            'Trigger cannot be empty.'
        );
    }

    await db.execute(
        `
        INSERT INTO auto_replies (
            account_number,
            enabled,
            trigger_text,
            reply_text
        )
        VALUES (?, ?, ?, ?)

        ON DUPLICATE KEY UPDATE
            enabled =
                VALUES(enabled),

            trigger_text =
                VALUES(trigger_text),

            reply_text =
                VALUES(reply_text),

            updated_at =
                NOW()
        `,
        [
            accountNumber,

            enabled ? 1 : 0,

            triggerText,

            replyText
        ]
    );

    return {
        enabled,
        triggerText,
        replyText
    };
}

// =====================================================
// WHATSAPP CLIENT
// =====================================================

const authPath =
    process.env.WWEBJS_AUTH_PATH ||
    path.join(
        __dirname,
        '.wwebjs_auth'
    );

const client = new Client({
    authStrategy:
        new LocalAuth({
            dataPath:
                authPath
        }),

    puppeteer: {
        executablePath:
            process.env
                .PUPPETEER_EXECUTABLE_PATH ||
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

            // -----------------------------------------
            // Give browser its persistent session token
            // -----------------------------------------

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

            // -----------------------------------------
            // Restore saved chats immediately
            // -----------------------------------------

            if (
                session.account_number
            ) {
                const savedChats =
                    await getStoredChats(
                        session.account_number
                    );

                if (
                    savedChats.length
                ) {
                    socket.emit(
                        'chats',
                        savedChats
                    );
                }
            }

            // -----------------------------------------
            // WhatsApp is already ready
            // -----------------------------------------

            if (
                whatsappReady
            ) {
                const accountNumber =
                    getAccountNumber();

                socket.data.accountNumber =
                    accountNumber;

                await updateSessionAccount(
                    socket.data.sessionToken,
                    accountNumber
                );

                socket.emit(
                    'ready',
                    {
                        accountNumber,
                        pushName:
                            client.info?.pushname ||
                            'WhatsApp'
                    }
                );

                const autoReply =
                    await getAutoReply(
                        accountNumber
                    );

                socket.emit(
                    'autoReply',
                    autoReply
                );

                await sendChats(
                    socket
                );
            } else {
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

        // =================================================
        // GET CHATS
        // =================================================

        socket.on(
            'getChats',
            async () => {
                try {
                    await touchSession(
                        socket.data.sessionToken
                    );

                    await sendChats(
                        socket
                    );
                } catch (error) {
                    console.error(
                        'getChats error:',
                        error
                    );

                    socket.emit(
                        'message',
                        `Failed to fetch chats: ${error.message}`
                    );
                }
            }
        );

        // =================================================
        // GET MESSAGES
        // =================================================

        socket.on(
            'getMessages',
            async chatId => {
                try {
                    await touchSession(
                        socket.data.sessionToken
                    );

                    if (
                        !chatId ||
                        typeof chatId !==
                            'string'
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

                    if (
                        !accountNumber
                    ) {
                        socket.emit(
                            'messagesError',
                            'WhatsApp account is not connected.'
                        );

                        return;
                    }

                    socket.data.accountNumber =
                        accountNumber;

                    // --------------------------------
                    // FIRST: load database messages
                    // --------------------------------

                    const savedMessages =
                        await getStoredMessages(
                            accountNumber,
                            chatId
                        );

                    socket.emit(
                        'messages',
                        {
                            chatId,

                            messages:
                                savedMessages
                        }
                    );

                    // --------------------------------
                    // SECOND: fetch live messages
                    // --------------------------------

                    if (
                        !whatsappReady
                    ) {
                        return;
                    }

                    try {
                        const liveMessages =
                            await fetchMessagesForChat(
                                accountNumber,
                                chatId
                            );

                        socket.emit(
                            'messages',
                            {
                                chatId,

                                messages:
                                    liveMessages
                            }
                        );
                    } catch (error) {
                        console.error(
                            'Live message fetch failed:',
                            error.message
                        );

                        // The stored messages
                        // were already sent.
                    }
                } catch (error) {
                    console.error(
                        'getMessages error:',
                        error
                    );

                    socket.emit(
                        'messagesError',
                        error.message
                    );
                }
            }
        );

        // =================================================
        // GET AUTO REPLY
        // =================================================

        socket.on(
            'getAutoReply',
            async () => {
                try {
                    const accountNumber =
                        getAccountNumber() ||
                        socket.data.accountNumber;

                    if (
                        !accountNumber
                    ) {
                        socket.emit(
                            'autoReply',
                            {
                                enabled:
                                    false,

                                triggerText:
                                    'START',

                                replyText:
                                    ''
                            }
                        );

                        return;
                    }

                    socket.data.accountNumber =
                        accountNumber;

                    const settings =
                        await getAutoReply(
                            accountNumber
                        );

                    socket.emit(
                        'autoReply',
                        settings
                    );
                } catch (error) {
                    console.error(
                        'getAutoReply socket error:',
                        error.message
                    );

                    socket.emit(
                        'autoReplyError',
                        error.message
                    );
                }
            }
        );

        // =================================================
        // SAVE AUTO REPLY
        // =================================================

        socket.on(
            'saveAutoReply',
            async settings => {
                try {
                    const accountNumber =
                        getAccountNumber() ||
                        socket.data.accountNumber;

                    if (
                        !accountNumber
                    ) {
                        socket.emit(
                            'autoReplyError',
                            'WhatsApp account is not connected.'
                        );

                        return;
                    }

                    socket.data.accountNumber =
                        accountNumber;

                    const saved =
                        await saveAutoReply(
                            accountNumber,
                            settings
                        );

                    socket.emit(
                        'autoReply',
                        saved
                    );

                    socket.emit(
                        'message',
                        saved.enabled
                            ? `Auto reply enabled for "${saved.triggerText}".`
                            : 'Auto reply disabled.'
                    );
                } catch (error) {
                    console.error(
                        'saveAutoReply socket error:',
                        error.message
                    );

                    socket.emit(
                        'autoReplyError',
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
// SEND CHATS
// =====================================================

async function sendChats(
    socket
) {
    const accountNumber =
        getAccountNumber() ||
        socket.data.accountNumber ||
        null;

    // ---------------------------------------------
    // Always show saved chats first
    // ---------------------------------------------

    if (accountNumber) {
        const stored =
            await getStoredChats(
                accountNumber
            );

        if (
            stored.length
        ) {
            socket.emit(
                'chats',
                stored
            );
        }
    }

    // ---------------------------------------------
    // Then fetch live chats
    // ---------------------------------------------

    if (!whatsappReady) {
        return;
    }

    try {
        const liveChats =
            await getRecentChatsDirect();

        console.log(
            `Found ${liveChats.length} recent chats.`
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
            `Loaded ${liveChats.length} recent chats.`
        );
    } catch (error) {
        console.error(
            'Live recent-chat fetch failed:',
            error
        );

        socket.emit(
            'message',
            `Live chat fetch failed: ${error.message}`
        );
    }
}

// =====================================================
// QR
// =====================================================

client.on(
    'qr',
    qr => {
        console.log(
            'QR code generated.'
        );

        qrcode.toDataURL(
            qr,
            (error, url) => {
                if (error) {
                    console.error(
                        'QR generation failed:',
                        error
                    );

                    return;
                }

                io.emit(
                    'qr',
                    url
                );

                io.emit(
                    'message',
                    'Please scan the QR code with WhatsApp.'
                );
            }
        );
    }
);

// =====================================================
// AUTHENTICATED
// =====================================================

client.on(
    'authenticated',
    () => {
        console.log(
            'WhatsApp authenticated.'
        );

        io.emit(
            'message',
            'Authenticated successfully. Loading...'
        );
    }
);

// =====================================================
// AUTH FAILURE
// =====================================================

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

// =====================================================
// READY
// =====================================================

client.on(
    'ready',
    async () => {
        whatsappReady = true;

        const accountNumber =
            getAccountNumber();

        console.log(
            '===================================='
        );

        console.log(
            'WHATSAPP READY'
        );

        console.log(
            'CONNECTED AS:',
            accountNumber
        );

        console.log(
            '===================================='
        );

        /*
         * Update every currently connected
         * browser session with this account.
         */
        for (
            const [socketId, socket]
            of io.sockets.sockets
        ) {
            socket.data.accountNumber =
                accountNumber;

            await updateSessionAccount(
                socket.data.sessionToken,
                accountNumber
            );
        }

        io.emit(
            'ready',
            {
                accountNumber,

                pushName:
                    client.info?.pushname ||
                    'WhatsApp'
            }
        );

        // Load auto-reply configuration
        if (
            accountNumber
        ) {
            const settings =
                await getAutoReply(
                    accountNumber
                );

            io.emit(
                'autoReply',
                settings
            );
        }

        io.emit(
            'message',
            'WhatsApp connected. Fetching recent chats...'
        );

        try {
            const liveChats =
                await getRecentChatsDirect();

            await saveRecentChats(
                accountNumber,
                liveChats
            );

            const stored =
                await getStoredChats(
                    accountNumber
                );

            io.emit(
                'chats',
                stored.length
                    ? stored
                    : liveChats
            );

            io.emit(
                'message',
                `Loaded ${liveChats.length} recent chats.`
            );
        } catch (error) {
            console.error(
                'Ready chat fetch error:',
                error
            );

            const stored =
                await getStoredChats(
                    accountNumber
                );

            if (
                stored.length
            ) {
                io.emit(
                    'chats',
                    stored
                );
            }

            io.emit(
                'message',
                `Could not refresh live chats: ${error.message}`
            );
        }
    }
);

// =====================================================
// NEW MESSAGE
//
// Also stores incoming/outgoing messages.
// =====================================================

client.on(
    'message_create',
    async message => {
        try {
            const accountNumber =
                getAccountNumber();

            if (
                !accountNumber ||
                !message
            ) {
                return;
            }

            const chatId =
                message.fromMe
                    ? message.to
                    : message.from;

            if (
                !chatId
            ) {
                return;
            }

            const saved =
                await saveMessageToDatabase(
                    accountNumber,
                    message,
                    chatId
                );

            if (
                saved
            ) {
                io.emit(
                    'newMessage',
                    saved
                );
            }
        } catch (error) {
            console.error(
                'message_create error:',
                error
            );
        }
    }
);

// =====================================================
// AUTO REPLY
//
// Incoming individual users only.
// Groups are ignored.
// =====================================================

client.on(
    'message',
    async message => {
        try {
            if (
                !message ||
                message.fromMe
            ) {
                return;
            }

            const chatId =
                String(
                    message.from ||
                    message.chatId ||
                    ''
                );

            if (
                !chatId
            ) {
                return;
            }

            // -----------------------------------------
            // NEVER AUTO-REPLY TO GROUPS
            // -----------------------------------------

            if (
                chatId.endsWith(
                    '@g.us'
                )
            ) {
                return;
            }

            // -----------------------------------------
            // Individuals can be @c.us OR @lid
            // -----------------------------------------

            const isIndividual =
                chatId.endsWith(
                    '@c.us'
                ) ||
                chatId.endsWith(
                    '@lid'
                );

            if (
                !isIndividual
            ) {
                return;
            }

            const accountNumber =
                getAccountNumber();

            if (
                !accountNumber
            ) {
                return;
            }

            const settings =
                await getAutoReply(
                    accountNumber
                );

            if (
                !settings.enabled
            ) {
                return;
            }

            if (
                !settings.replyText
            ) {
                return;
            }

            const receivedText =
                String(
                    message.body ||
                    ''
                ).trim();

            const triggerText =
                String(
                    settings.triggerText ||
                    'START'
                ).trim();

            if (
                !receivedText ||
                !triggerText
            ) {
                return;
            }

            /*
             * Exact match,
             * case-insensitive.
             *
             * START
             * start
             * Start
             *
             * all match.
             */

            if (
                receivedText.toLowerCase() !==
                triggerText.toLowerCase()
            ) {
                return;
            }

            console.log(
                'AUTO REPLY MATCH:',
                chatId
            );

            await message.reply(
                settings.replyText
            );

            console.log(
                'AUTO REPLY SENT:',
                chatId
            );
        } catch (error) {
            console.error(
                'AUTO REPLY ERROR:',
                error
            );
        }
    }
);

// =====================================================
// DISCONNECTED
// =====================================================

client.on(
    'disconnected',
    reason => {
        whatsappReady = false;

        console.log(
            'WhatsApp disconnected:',
            reason
        );

        io.emit(
            'whatsappDisconnected',
            String(
                reason ||
                'Disconnected'
            )
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
    await initDatabase();

    client.initialize()
        .catch(
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