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

// Group member lists are fetched occasionally and checked locally.
// Change this to control how often WhatsApp member data is refreshed.
const GROUP_MEMBER_CACHE_TTL_MS = 5 * 60 * 1000;
const LID_RESOLVE_CHUNK_SIZE = 20;
const LID_RESOLVE_DELAY_MS = 300;

// =====================================================
// STATE
// =====================================================
let db = null;
let whatsappReady = false;

// Group membership cache:
// groupId -> {
//   groupId,
//   groupName,
//   memberNumbers: Set of full phone digits,
//   fetchedAt: timestamp,
//   memberCount: number,
//   resolvedPhoneCount: number
// }
const groupMemberCache = new Map();

// Prevent multiple simultaneous refreshes for the same group.
const groupMemberRefreshes = new Map();

// IMPORTANT: Keep the latest QR in memory.
let latestQr = null;

// =====================================================
// EXPRESS
// =====================================================
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// =====================================================
// ERROR HANDLING
// =====================================================
process.on('unhandledRejection', error => {
    console.error('Unhandled Promise Rejection:', error);
});

process.on('uncaughtException', error => {
    console.error('Uncaught Exception:', error);
});

// =====================================================
// MYSQL CONNECTION
// =====================================================
function createDatabasePool() {
    const connectionUrl =
        process.env.MYSQL_URL ||
        process.env.DATABASE_URL;

    if (connectionUrl) {
        const url = new URL(connectionUrl);

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
        host:
            process.env.MYSQLHOST ||
            process.env.DB_HOST,

        port: Number(
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

// =====================================================
// DATABASE
// =====================================================
async function initDatabase() {
    try {
        db = createDatabasePool();

        await db.query('SELECT 1');

        console.log('MySQL connected.');

        // =================================================
        // SESSIONS
        // =================================================
        await db.query(`
            CREATE TABLE IF NOT EXISTS sessions (
                id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
                session_token CHAR(64) NOT NULL,
                account_number VARCHAR(32) DEFAULT NULL,
                created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                last_seen_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                PRIMARY KEY (id),
                UNIQUE KEY unique_session_token (session_token),
                INDEX idx_session_account (account_number)
            )
            ENGINE=InnoDB
            DEFAULT CHARSET=utf8mb4
            COLLATE=utf8mb4_unicode_ci
        `);

        // =================================================
        // AUTO REPLIES
        // =================================================
        let autoReplyExists = false;

        try {
            const [tables] = await db.query(
                `SHOW TABLES LIKE 'auto_replies'`
            );

            autoReplyExists = tables.length > 0;
        } catch (_) {
            autoReplyExists = false;
        }

        if (!autoReplyExists) {
            await db.query(`
                CREATE TABLE auto_replies (
                    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
                    account_number VARCHAR(32) NOT NULL,
                    enabled TINYINT(1) NOT NULL DEFAULT 1,
                    trigger_text VARCHAR(255) NOT NULL,
                    reply_text MEDIUMTEXT NOT NULL,
                    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    PRIMARY KEY (id),
                    UNIQUE KEY unique_account_trigger (account_number, trigger_text),
                    INDEX idx_auto_reply_account (account_number)
                )
                ENGINE=InnoDB
                DEFAULT CHARSET=utf8mb4
                COLLATE=utf8mb4_unicode_ci
            `);

            console.log('Created auto_replies table.');
        } else {
            const [columns] = await db.query(
                `SHOW COLUMNS FROM auto_replies`
            );

            const hasId = columns.some(
                column => column.Field === 'id'
            );

            if (!hasId) {
                console.log(
                    'Old auto_replies table detected. Migrating...'
                );

                await db.query(`
                    CREATE TABLE IF NOT EXISTS auto_replies_v2 (
                        id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
                        account_number VARCHAR(32) NOT NULL,
                        enabled TINYINT(1) NOT NULL DEFAULT 1,
                        trigger_text VARCHAR(255) NOT NULL,
                        reply_text MEDIUMTEXT NOT NULL,
                        updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                        PRIMARY KEY (id),
                        UNIQUE KEY unique_account_trigger (account_number, trigger_text),
                        INDEX idx_auto_reply_account (account_number)
                    )
                    ENGINE=InnoDB
                    DEFAULT CHARSET=utf8mb4
                    COLLATE=utf8mb4_unicode_ci
                `);

                await db.query(`
                    INSERT IGNORE INTO auto_replies_v2
                    (
                        account_number,
                        enabled,
                        trigger_text,
                        reply_text
                    )
                    SELECT
                        account_number,
                        enabled,
                        trigger_text,
                        reply_text
                    FROM auto_replies
                `);

                await db.query(
                    'DROP TABLE auto_replies'
                );

                await db.query(
                    'RENAME TABLE auto_replies_v2 TO auto_replies'
                );

                console.log(
                    'auto_replies migration completed.'
                );
            }
        }

        console.log('Database tables ready.');
    } catch (error) {
        console.error(
            'MySQL connection failed:',
            error.message
        );

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
        /^[a-f0-9]{64}$/i.test(suppliedToken)
    ) {
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
        crypto.randomBytes(32).toString('hex');

    if (db) {
        try {
            await db.execute(
                `
                    INSERT INTO sessions
                    (session_token)
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

async function touchSession(sessionToken) {
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
// AUTO REPLIES
// =====================================================
async function getAutoReplies(
    accountNumber
) {
    if (!db || !accountNumber) {
        return [];
    }

    try {
        const [rows] = await db.execute(
            `
                SELECT
                    id,
                    enabled,
                    trigger_text AS triggerText,
                    reply_text AS replyText
                FROM auto_replies
                WHERE account_number = ?
                ORDER BY id ASC
            `,
            [accountNumber]
        );

        return rows.map(row => ({
            id: Number(row.id),
            enabled: Boolean(row.enabled),
            triggerText: String(
                row.triggerText || ''
            ),
            replyText: String(
                row.replyText || ''
            )
        }));
    } catch (error) {
        console.error(
            'getAutoReplies error:',
            error.message
        );

        return [];
    }
}

async function addAutoReply(
    accountNumber,
    settings
) {
    if (!db || !accountNumber) {
        throw new Error(
            'Database or WhatsApp account is unavailable.'
        );
    }

    const triggerText = String(
        settings?.triggerText || ''
    ).trim();

    const replyText = String(
        settings?.replyText || ''
    ).trim();

    const enabled =
        settings?.enabled !== false;

    if (!triggerText) {
        throw new Error(
            'Trigger cannot be empty.'
        );
    }

    if (!replyText) {
        throw new Error(
            'Response cannot be empty.'
        );
    }

    if (triggerText.length > 255) {
        throw new Error(
            'Trigger is too long.'
        );
    }

    try {
        await db.execute(
            `
                INSERT INTO auto_replies
                (
                    account_number,
                    enabled,
                    trigger_text,
                    reply_text
                )
                VALUES (?, ?, ?, ?)
            `,
            [
                accountNumber,
                enabled ? 1 : 0,
                triggerText,
                replyText
            ]
        );
    } catch (error) {
        if (error.code === 'ER_DUP_ENTRY') {
            throw new Error(
                `Trigger "${triggerText}" already exists.`
            );
        }

        throw error;
    }

    return getAutoReplies(accountNumber);
}

async function toggleAutoReply(
    accountNumber,
    id,
    enabled
) {
    if (!db || !accountNumber) {
        throw new Error(
            'Database or WhatsApp account is unavailable.'
        );
    }

    const autoReplyId = Number(id);

    if (
        !Number.isInteger(autoReplyId) ||
        autoReplyId <= 0
    ) {
        throw new Error(
            'Invalid auto reply ID.'
        );
    }

    await db.execute(
        `
            UPDATE auto_replies
            SET enabled = ?
            WHERE id = ?
              AND account_number = ?
        `,
        [
            enabled ? 1 : 0,
            autoReplyId,
            accountNumber
        ]
    );

    return getAutoReplies(accountNumber);
}

async function deleteAutoReply(
    accountNumber,
    id
) {
    if (!db || !accountNumber) {
        throw new Error(
            'Database or WhatsApp account is unavailable.'
        );
    }

    const autoReplyId = Number(id);

    if (
        !Number.isInteger(autoReplyId) ||
        autoReplyId <= 0
    ) {
        throw new Error(
            'Invalid auto reply ID.'
        );
    }

    await db.execute(
        `
            DELETE FROM auto_replies
            WHERE id = ?
              AND account_number = ?
        `,
        [
            autoReplyId,
            accountNumber
        ]
    );

    return getAutoReplies(accountNumber);
}

// =====================================================
// WHATSAPP ACCOUNT & DATA
// =====================================================
function getAccountNumber() {
    const user = client?.info?.wid?.user;

    if (!user) {
        return null;
    }

    return `+${user}`;
}

// =====================================================
// RECENT CHATS
// =====================================================
async function getRecentChatsDirect() {
    if (!client.pupPage) {
        throw new Error(
            'WhatsApp browser page is not ready.'
        );
    }

    const result =
        await client.pupPage.evaluate(() => {
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
                collections.Chat.getModelsArray();

            return models
                .map(chat => {
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

                    let name = 'Unknown';

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
                    } catch (_) {}

                    let unread = 0;

                    try {
                        unread =
                            Number(
                                chat.unreadCount ||
                                0
                            );
                    } catch (_) {}

                    let lastMessage = '';

                    try {
                        if (
                            chat.msgs &&
                            typeof chat.msgs
                                .getModelsArray ===
                                'function'
                        ) {
                            const msgs =
                                chat.msgs
                                    .getModelsArray();

                            if (
                                msgs.length > 0
                            ) {
                                const lastMsg =
                                    msgs[
                                        msgs.length - 1
                                    ];

                                lastMessage =
                                    lastMsg.body !=
                                    null
                                        ? String(
                                              lastMsg.body
                                          )
                                        : String(
                                              lastMsg.type ||
                                              ''
                                          );
                            }
                        }
                    } catch (_) {}

                    const isGroup =
                        String(id).endsWith(
                            '@g.us'
                        );

                    return {
                        id: String(id),
                        name: String(
                            name || 'Unknown'
                        ),
                        timestamp,
                        unread,
                        isGroup,
                        lastMessage
                    };
                })
                .filter(Boolean);
        });

    return result
        .sort(
            (a, b) =>
                Number(b.timestamp || 0) -
                Number(a.timestamp || 0)
        )
        .slice(0, CHAT_LIMIT);
}

// =====================================================
// MESSAGES
// =====================================================
async function fetchMessagesForChat(
    chatId
) {
    if (!client.pupPage) {
        throw new Error(
            'WhatsApp browser page is not ready.'
        );
    }

    const messages =
        await client.pupPage.evaluate(
            (
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
                        'WAWebCollections.Chat is unavailable.'
                    );
                }

                const models =
                    collections.Chat.getModelsArray();

                const chat =
                    models.find(item => {
                        try {
                            return (
                                item.id &&
                                (
                                    item.id
                                        ._serialized ===
                                        requestedChatId ||

                                    (
                                        item.id
                                            .user &&
                                        item.id
                                            .server &&

                                        `${item.id.user}@${item.id.server}` ===
                                            requestedChatId
                                    )
                                )
                            );
                        } catch (_) {
                            return false;
                        }
                    });

                if (!chat) {
                    throw new Error(
                        'Chat not found in WhatsApp Web.'
                    );
                }

                let messageModels = [];

                try {
                    if (
                        chat.msgs &&
                        typeof chat.msgs
                            .getModelsArray ===
                            'function'
                    ) {
                        messageModels =
                            chat.msgs
                                .getModelsArray();
                    }
                } catch (_) {
                    messageModels = [];
                }

                return messageModels
                    .slice(-limit)
                    .map(message => {
                        let id = null;

                        try {
                            id =
                                message.id
                                    ?._serialized ||
                                message.id?.id ||
                                null;
                        } catch (_) {}

                        if (!id) {
                            return null;
                        }

                        return {
                            id: String(id),

                            fromMe:
                                Boolean(
                                    message.fromMe ||
                                    message.id?.fromMe
                                ),

                            from:
                                message.from
                                    ?._serialized ||
                                message.from ||
                                null,

                            to:
                                message.to
                                    ?._serialized ||
                                message.to ||
                                null,

                            author:
                                message.author
                                    ?._serialized ||
                                message.author ||
                                null,

                            body:
                                message.body ==
                                null
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
                    })
                    .filter(Boolean);
            },
            chatId,
            MESSAGE_LIMIT
        );

    return messages
        .map(message => ({
            id: String(message.id),
            chatId: String(chatId),

            fromMe:
                Boolean(message.fromMe),

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
        }))
        .sort(
            (a, b) =>
                a.timestamp -
                b.timestamp
        );
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
            dataPath: authPath
        }),

    puppeteer: {
        executablePath:
            process.env
                .PUPPETEER_EXECUTABLE_PATH ||
            undefined,

        headless: true,

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

// =====================================================
// GROUP MEMBER CACHE
// =====================================================

function sleep(ms) {
    return new Promise(resolve => {
        setTimeout(resolve, ms);
    });
}

// Input is expected to contain the number WITHOUT
// country code.
//
// Example:
// 9876543210
//
// Not:
// +919876543210
//
// A common leading 0 is removed.
function normalizeLocalPhoneNumber(value) {
    let digits =
        String(value ?? '')
            .replace(/\D/g, '');

    digits =
        digits.replace(/^0+/, '');

    if (!digits) {
        throw new Error(
            'Invalid phone number format.'
        );
    }

    if (
        digits.length < 7 ||
        digits.length > 15
    ) {
        throw new Error(
            'Enter a valid local phone number without country code.'
        );
    }

    return digits;
}

// Get the WhatsApp serialized ID from a participant.
function extractSerializedId(
    participant
) {
    try {
        if (
            participant?.id?._serialized
        ) {
            return String(
                participant.id._serialized
            );
        }

        if (
            participant?.id?.user &&
            participant?.id?.server
        ) {
            return (
                `${participant.id.user}@${participant.id.server}`
            );
        }

        if (
            participant?.id?.user
        ) {
            return String(
                participant.id.user
            );
        }
    } catch (_) {}

    return null;
}

// Convert a normal @c.us ID into digits.
function phoneDigitsFromSerializedId(
    serializedId
) {
    if (!serializedId) {
        return null;
    }

    const value =
        String(serializedId);

    if (
        !value.endsWith('@c.us')
    ) {
        return null;
    }

    const digits =
        value
            .slice(
                0,
                -5
            )
            .replace(/\D/g, '');

    return digits || null;
}

// =====================================================
// RESOLVE @LID IDs TO PHONE NUMBERS
// =====================================================
async function resolveLidsToPhones(
    lidIds
) {
    const phoneByLid =
        new Map();

    if (
        !Array.isArray(lidIds) ||
        lidIds.length === 0
    ) {
        return phoneByLid;
    }

    // Only happens while refreshing the cache.
    //
    // The membership lookup itself does NOT call
    // WhatsApp again.
    for (
        let start = 0;
        start < lidIds.length;
        start += LID_RESOLVE_CHUNK_SIZE
    ) {
        const chunk =
            lidIds.slice(
                start,
                start +
                    LID_RESOLVE_CHUNK_SIZE
            );

        try {
            const resolved =
                await client
                    .getContactLidAndPhone(
                        chunk
                    );

            for (
                const item of Array.isArray(
                    resolved
                )
                    ? resolved
                    : []
            ) {
                const lid =
                    String(
                        item?.lid || ''
                    );

                const pn =
                    String(
                        item?.pn || ''
                    );

                if (
                    !lid ||
                    !pn
                ) {
                    continue;
                }

                const phoneDigits =
                    phoneDigitsFromSerializedId(
                        pn
                    );

                if (
                    phoneDigits
                ) {
                    phoneByLid.set(
                        lid,
                        phoneDigits
                    );
                }
            }
        } catch (error) {
            console.error(
                `LID phone resolution failed for chunk starting at ${start}:`,
                error.message
            );
        }

        if (
            start +
                LID_RESOLVE_CHUNK_SIZE <
            lidIds.length
        ) {
            await sleep(
                LID_RESOLVE_DELAY_MS
            );
        }
    }

    return phoneByLid;
}

// =====================================================
// FETCH + CACHE GROUP MEMBERS
// =====================================================
async function fetchGroupMemberCache(
    groupId
) {
    if (!whatsappReady) {
        throw new Error(
            'WhatsApp is not ready. Please wait or relogin.'
        );
    }

    const normalizedGroupId =
        String(groupId || '').trim();

    if (
        !normalizedGroupId.endsWith(
            '@g.us'
        )
    ) {
        throw new Error(
            'Invalid Group ID. It must end with @g.us.'
        );
    }

    // THIS is the occasional WhatsApp fetch.
    const chat =
        await client.getChatById(
            normalizedGroupId
        );

    if (
        !chat ||
        !chat.isGroup
    ) {
        throw new Error(
            'Target chat not found or is not a group.'
        );
    }

    const participants =
        Array.isArray(
            chat.participants
        )
            ? chat.participants
            : [];

    const directNumbers =
        new Set();

    const lidIds = [];

    // Build a local list from the group
    // participant data.
    for (
        const participant
        of participants
    ) {
        const serializedId =
            extractSerializedId(
                participant
            );

        if (!serializedId) {
            continue;
        }

        // Normal WhatsApp number.
        const directPhone =
            phoneDigitsFromSerializedId(
                serializedId
            );

        if (directPhone) {
            directNumbers.add(
                directPhone
            );

            continue;
        }

        // Modern WhatsApp LID.
        if (
            serializedId.endsWith(
                '@lid'
            )
        ) {
            lidIds.push(
                serializedId
            );
        }
    }

    // Resolve LIDs only during refresh.
    const phoneByLid =
        await resolveLidsToPhones(
            lidIds
        );

    for (
        const lidId
        of lidIds
    ) {
        const phoneDigits =
            phoneByLid.get(
                lidId
            );

        if (phoneDigits) {
            directNumbers.add(
                phoneDigits
            );
        }
    }

    const cacheEntry = {
        groupId:
            normalizedGroupId,

        groupName:
            String(
                chat.name ||
                'Unknown Group'
            ),

        memberNumbers:
            directNumbers,

        memberCount:
            participants.length,

        resolvedPhoneCount:
            directNumbers.size,

        fetchedAt:
            Date.now()
    };

    groupMemberCache.set(
        normalizedGroupId,
        cacheEntry
    );

    console.log(
        `GROUP CACHE REFRESHED: ${normalizedGroupId} | participants=${participants.length} | phoneNumbers=${directNumbers.size}`
    );

    return cacheEntry;
}

// =====================================================
// GET CACHED GROUP MEMBER LIST
// =====================================================
async function getGroupMemberCache(
    groupId
) {
    const normalizedGroupId =
        String(groupId || '').trim();

    const existing =
        groupMemberCache.get(
            normalizedGroupId
        );

    const now =
        Date.now();

    // =================================================
    // CACHE HIT
    // =================================================
    if (
        existing &&
        (
            now -
            existing.fetchedAt
        ) <
            GROUP_MEMBER_CACHE_TTL_MS
    ) {
        return {
            ...existing,
            fromCache: true
        };
    }

    // =================================================
    // WAIT FOR EXISTING REFRESH
    // =================================================
    //
    // If multiple browser requests arrive at once,
    // don't make multiple WhatsApp requests.
    //
    if (
        groupMemberRefreshes.has(
            normalizedGroupId
        )
    ) {
        const refreshed =
            await groupMemberRefreshes.get(
                normalizedGroupId
            );

        return {
            ...refreshed,
            fromCache: false
        };
    }

    // =================================================
    // CACHE MISS / EXPIRED
    // =================================================
    const refreshPromise =
        fetchGroupMemberCache(
            normalizedGroupId
        );

    groupMemberRefreshes.set(
        normalizedGroupId,
        refreshPromise
    );

    try {
        const refreshed =
            await refreshPromise;

        return {
            ...refreshed,
            fromCache: false
        };
    } finally {
        groupMemberRefreshes.delete(
            normalizedGroupId
        );
    }
}

// =====================================================
// LOCAL MEMBERSHIP CHECK
// =====================================================
function isPhoneInCachedGroup(
    cacheEntry,
    requestedNumber
) {
    // IMPORTANT:
    //
    // NO WhatsApp request.
    // NO API request.
    // NO group fetch.
    //
    // Only local Set lookup.
    //
    // Exact match first.
    if (
        cacheEntry.memberNumbers.has(
            requestedNumber
        )
    ) {
        return true;
    }

    // Fallback for unusual formatting.
    //
    // Example:
    // cached: 919876543210
    // requested: 9876543210
    //
    for (
        const memberPhone
        of cacheEntry.memberNumbers
    ) {
        if (
            memberPhone.endsWith(
                requestedNumber
            )
        ) {
            return true;
        }
    }

    return false;
}

// =====================================================
// CLEAR CACHE
// =====================================================
function clearGroupMemberCache() {
    groupMemberCache.clear();
    groupMemberRefreshes.clear();

    console.log(
        'GROUP MEMBER CACHE CLEARED.'
    );
}

// =====================================================
// CACHE INFO
// =====================================================
function getCacheAgeSeconds(
    cacheEntry
) {
    if (
        !cacheEntry ||
        !cacheEntry.fetchedAt
    ) {
        return null;
    }

    return Math.max(
        0,
        Math.floor(
            (
                Date.now() -
                cacheEntry.fetchedAt
            ) / 1000
        )
    );
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
            const suppliedToken =
                socket.handshake
                    .auth
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

            // QR already exists.
            if (
                latestQr &&
                !whatsappReady
            ) {
                socket.emit(
                    'qr',
                    latestQr
                );

                socket.emit(
                    'message',
                    'Scan the QR code to connect WhatsApp.'
                );
            }

            // WhatsApp already ready.
            if (whatsappReady) {
                const accountNumber =
                    getAccountNumber();

                socket.data.accountNumber =
                    accountNumber;

                await updateSessionAccount(
                    socket.data
                        .sessionToken,
                    accountNumber
                );

                socket.emit(
                    'ready',
                    {
                        accountNumber,

                        pushName:
                            client.info
                                ?.pushname ||
                            'WhatsApp'
                    }
                );

                const replies =
                    await getAutoReplies(
                        accountNumber
                    );

                socket.emit(
                    'autoReplies',
                    replies
                );

                await sendLiveChats(
                    socket
                );
            } else if (
                !latestQr
            ) {
                socket.emit(
                    'message',
                    'Starting WhatsApp...'
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
                        socket.data
                            .sessionToken
                    );

                    await sendLiveChats(
                        socket
                    );
                } catch (error) {
                    console.error(
                        'getChats error:',
                        error.message
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
                        socket.data
                            .sessionToken
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

                    if (
                        !whatsappReady
                    ) {
                        socket.emit(
                            'messagesError',
                            'WhatsApp is not ready.'
                        );

                        return;
                    }

                    const messages =
                        await fetchMessagesForChat(
                            chatId
                        );

                    socket.emit(
                        'messages',
                        {
                            chatId,
                            messages
                        }
                    );
                } catch (error) {
                    console.error(
                        'getMessages error:',
                        error.message
                    );

                    socket.emit(
                        'messagesError',
                        error.message
                    );
                }
            }
        );

        // =================================================
        // GROUP MEMBERSHIP CHECKER
        // =================================================
        socket.on(
            'checkGroupMembership',
            async data => {
                try {
                    const groupId =
                        String(
                            data?.groupId ||
                                ''
                        ).trim();

                    const number =
                        normalizeLocalPhoneNumber(
                            data?.number
                        );

                    if (
                        !whatsappReady
                    ) {
                        socket.emit(
                            'groupMembershipError',
                            'WhatsApp is not ready. Please wait or relogin.'
                        );

                        return;
                    }

                    if (!groupId) {
                        socket.emit(
                            'groupMembershipError',
                            'Group ID is required.'
                        );

                        return;
                    }

                    // =================================================
                    // IMPORTANT:
                    //
                    // This may fetch the list ONLY if:
                    // - no cache exists
                    // - cache expired
                    //
                    // Otherwise this is entirely local.
                    // =================================================
                    const cache =
                        await getGroupMemberCache(
                            groupId
                        );

                    // =================================================
                    // LOCAL CHECK
                    // =================================================
                    const isMember =
                        isPhoneInCachedGroup(
                            cache,
                            number
                        );

                    const cacheAgeSeconds =
                        getCacheAgeSeconds(
                            cache
                        );

                    socket.emit(
                        'groupMembershipResult',
                        {
                            number,

                            isMember,

                            groupId:
                                cache.groupId,

                            groupName:
                                cache.groupName,

                            memberCount:
                                cache.memberCount,

                            resolvedPhoneCount:
                                cache.resolvedPhoneCount,

                            fromCache:
                                Boolean(
                                    cache.fromCache
                                ),

                            cacheAgeSeconds,

                            cacheTtlSeconds:
                                Math.floor(
                                    GROUP_MEMBER_CACHE_TTL_MS /
                                        1000
                                )
                        }
                    );

                    console.log(
                        `GROUP CHECK: ${number} -> ${isMember ? 'MEMBER' : 'NOT MEMBER'} | cache=${cache.fromCache ? 'HIT' : 'REFRESH'}`
                    );
                } catch (error) {
                    console.error(
                        'checkGroupMembership error:',
                        error.message
                    );

                    socket.emit(
                        'groupMembershipError',
                        `Failed to check membership: ${error.message}`
                    );
                }
            }
        );

        // =================================================
        // MULTIPLE NUMBER CHECK
        // =================================================
        //
        // Example:
        //
        // socket.emit(
        //     'checkGroupMembershipMany',
        //     {
        //         groupId: '120363...@g.us',
        //         numbers: [
        //             '9876543210',
        //             '9123456789',
        //             '9000000000'
        //         ]
        //     }
        // );
        //
        // The member list is fetched once.
        // ALL numbers are then checked locally.
        //
        socket.on(
            'checkGroupMembershipMany',
            async data => {
                try {
                    const groupId =
                        String(
                            data?.groupId ||
                                ''
                        ).trim();

                    if (
                        !whatsappReady
                    ) {
                        socket.emit(
                            'groupMembershipManyError',
                            'WhatsApp is not ready. Please wait or relogin.'
                        );

                        return;
                    }

                    if (!groupId) {
                        socket.emit(
                            'groupMembershipManyError',
                            'Group ID is required.'
                        );

                        return;
                    }

                    const rawNumbers =
                        Array.isArray(
                            data?.numbers
                        )
                            ? data.numbers
                            : [];

                    if (
                        !rawNumbers.length
                    ) {
                        socket.emit(
                            'groupMembershipManyError',
                            'At least one phone number is required.'
                        );

                        return;
                    }

                    if (
                        rawNumbers.length >
                        500
                    ) {
                        socket.emit(
                            'groupMembershipManyError',
                            'Maximum 500 numbers per request.'
                        );

                        return;
                    }

                    // =================================================
                    // ONE CACHE FETCH/REFRESH
                    // =================================================
                    const cache =
                        await getGroupMemberCache(
                            groupId
                        );

                    // =================================================
                    // ALL CHECKS LOCAL
                    // =================================================
                    const results =
                        rawNumbers.map(
                            rawNumber => {
                                try {
                                    const number =
                                        normalizeLocalPhoneNumber(
                                            rawNumber
                                        );

                                    return {
                                        number,

                                        isMember:
                                            isPhoneInCachedGroup(
                                                cache,
                                                number
                                            )
                                    };
                                } catch (
                                    error
                                ) {
                                    return {
                                        number:
                                            String(
                                                rawNumber ??
                                                    ''
                                            ),

                                        isMember:
                                            false,

                                        error:
                                            error.message
                                    };
                                }
                            }
                        );

                    const cacheAgeSeconds =
                        getCacheAgeSeconds(
                            cache
                        );

                    socket.emit(
                        'groupMembershipManyResult',
                        {
                            groupId:
                                cache.groupId,

                            groupName:
                                cache.groupName,

                            results,

                            memberCount:
                                cache.memberCount,

                            resolvedPhoneCount:
                                cache.resolvedPhoneCount,

                            fromCache:
                                Boolean(
                                    cache.fromCache
                                ),

                            cacheAgeSeconds,

                            cacheTtlSeconds:
                                Math.floor(
                                    GROUP_MEMBER_CACHE_TTL_MS /
                                        1000
                                )
                        }
                    );
                } catch (error) {
                    console.error(
                        'checkGroupMembershipMany error:',
                        error.message
                    );

                    socket.emit(
                        'groupMembershipManyError',
                        `Failed to check memberships: ${error.message}`
                    );
                }
            }
        );

        // =================================================
        // GET AUTO REPLIES
        // =================================================
        socket.on(
            'getAutoReplies',
            async () => {
                try {
                    const accountNumber =
                        getAccountNumber() ||
                        socket.data
                            .accountNumber;

                    if (
                        !accountNumber
                    ) {
                        socket.emit(
                            'autoReplies',
                            []
                        );

                        return;
                    }

                    const replies =
                        await getAutoReplies(
                            accountNumber
                        );

                    socket.emit(
                        'autoReplies',
                        replies
                    );
                } catch (error) {
                    socket.emit(
                        'autoReplyError',
                        error.message
                    );
                }
            }
        );

        // =================================================
        // ADD AUTO REPLY
        // =================================================
        socket.on(
            'addAutoReply',
            async settings => {
                try {
                    const accountNumber =
                        getAccountNumber() ||
                        socket.data
                            .accountNumber;

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

                    const replies =
                        await addAutoReply(
                            accountNumber,
                            settings
                        );

                    socket.emit(
                        'autoReplies',
                        replies
                    );

                    socket.emit(
                        'autoReplySaved',
                        true
                    );

                    socket.emit(
                        'message',
                        `Auto reply "${settings?.triggerText || ''}" added.`
                    );
                } catch (error) {
                    console.error(
                        'addAutoReply error:',
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
        // TOGGLE AUTO REPLY
        // =================================================
        socket.on(
            'toggleAutoReply',
            async data => {
                try {
                    const accountNumber =
                        getAccountNumber() ||
                        socket.data
                            .accountNumber;

                    if (
                        !accountNumber
                    ) {
                        socket.emit(
                            'autoReplyError',
                            'WhatsApp account is not connected.'
                        );

                        return;
                    }

                    const replies =
                        await toggleAutoReply(
                            accountNumber,
                            data?.id,
                            data?.enabled
                        );

                    socket.emit(
                        'autoReplies',
                        replies
                    );
                } catch (error) {
                    console.error(
                        'toggleAutoReply error:',
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
        // DELETE AUTO REPLY
        // =================================================
        socket.on(
            'deleteAutoReply',
            async id => {
                try {
                    const accountNumber =
                        getAccountNumber() ||
                        socket.data
                            .accountNumber;

                    if (
                        !accountNumber
                    ) {
                        socket.emit(
                            'autoReplyError',
                            'WhatsApp account is not connected.'
                        );

                        return;
                    }

                    const replies =
                        await deleteAutoReply(
                            accountNumber,
                            id
                        );

                    socket.emit(
                        'autoReplies',
                        replies
                    );
                } catch (error) {
                    console.error(
                        'deleteAutoReply error:',
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
        // DISCONNECT BROWSER
        // =================================================
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
// SEND LIVE CHATS
// =====================================================
async function sendLiveChats(
    socket
) {
    if (!whatsappReady) {
        socket.emit(
            'message',
            'WhatsApp is not ready yet.'
        );

        return;
    }

    try {
        const chats =
            await getRecentChatsDirect();

        socket.emit(
            'chats',
            chats
        );

        socket.emit(
            'message',
            `Loaded ${chats.length} recent chats.`
        );
    } catch (error) {
        console.error(
            'Live chat fetch failed:',
            error.message
        );

        socket.emit(
            'message',
            `Live chat fetch failed: ${error.message}`
        );
    }
}

// =====================================================
// WHATSAPP EVENTS
// =====================================================

// =====================================================
// QR
// =====================================================
client.on(
    'qr',
    async qr => {
        console.log(
            'NEW WHATSAPP QR RECEIVED'
        );

        try {
            const url =
                await qrcode.toDataURL(
                    qr
                );

            latestQr = url;

            io.emit(
                'qr',
                url
            );

            io.emit(
                'message',
                'Scan the QR code to connect WhatsApp.'
            );
        } catch (error) {
            console.error(
                'QR generation failed:',
                error
            );

            io.emit(
                'message',
                'Failed to generate QR code.'
            );
        }
    }
);

// =====================================================
// LOADING
// =====================================================
client.on(
    'loading_screen',
    (percent, message) => {
        console.log(
            `WhatsApp loading: ${percent}% ${message || ''}`
        );

        io.emit(
            'message',
            `WhatsApp loading: ${percent}%`
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

        latestQr = null;

        io.emit(
            'message',
            'WhatsApp authenticated. Loading...'
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
        latestQr = null;

        clearGroupMemberCache();

        console.error(
            'WhatsApp authentication failure:',
            error
        );

        io.emit(
            'message',
            `WhatsApp authentication failed: ${error}`
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

        latestQr = null;

        // Old member cache must never survive
        // a WhatsApp reconnect.
        clearGroupMemberCache();

        const accountNumber =
            getAccountNumber();

        console.log(
            'WHATSAPP READY'
        );

        console.log(
            'CONNECTED AS:',
            accountNumber
        );

        for (
            const [, socket]
            of io.sockets.sockets
        ) {
            socket.data.accountNumber =
                accountNumber;

            await updateSessionAccount(
                socket.data
                    .sessionToken,
                accountNumber
            );
        }

        io.emit(
            'ready',
            {
                accountNumber,

                pushName:
                    client.info
                        ?.pushname ||
                    'WhatsApp'
            }
        );

        io.emit(
            'message',
            'WhatsApp connected.'
        );

        if (accountNumber) {
            const replies =
                await getAutoReplies(
                    accountNumber
                );

            io.emit(
                'autoReplies',
                replies
            );
        }

        try {
            const chats =
                await getRecentChatsDirect();

            io.emit(
                'chats',
                chats
            );

            io.emit(
                'message',
                `Loaded ${chats.length} recent chats.`
            );
        } catch (error) {
            console.error(
                'Ready chat fetch error:',
                error.message
            );

            io.emit(
                'message',
                `Could not load chats: ${error.message}`
            );
        }
    }
);

// =====================================================
// NEW MESSAGE CREATED
// =====================================================
client.on(
    'message_create',
    message => {
        try {
            const chatId =
                message.fromMe
                    ? message.to
                    : message.from;

            if (!chatId) {
                return;
            }

            io.emit(
                'newMessage',
                {
                    chatId:
                        String(chatId)
                }
            );
        } catch (_) {}
    }
);

// =====================================================
// AUTO REPLY
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
                !chatId ||
                chatId.endsWith(
                    '@g.us'
                )
            ) {
                return;
            }

            const isIndividual =
                chatId.endsWith(
                    '@c.us'
                ) ||
                chatId.endsWith(
                    '@lid'
                );

            if (!isIndividual) {
                return;
            }

            const accountNumber =
                getAccountNumber();

            if (!accountNumber) {
                return;
            }

            const rules =
                await getAutoReplies(
                    accountNumber
                );

            if (!rules.length) {
                return;
            }

            const receivedText =
                String(
                    message.body || ''
                )
                    .trim()
                    .toLowerCase();

            if (!receivedText) {
                return;
            }

            const matchedRule =
                rules.find(
                    rule => {
                        if (
                            !rule.enabled
                        ) {
                            return false;
                        }

                        const trigger =
                            String(
                                rule.triggerText ||
                                    ''
                            )
                                .trim()
                                .toLowerCase();

                        return (
                            trigger &&
                            receivedText ===
                                trigger
                        );
                    }
                );

            if (!matchedRule) {
                return;
            }

            console.log(
                'AUTO REPLY MATCH:',
                matchedRule.triggerText,
                '->',
                chatId
            );

            await message.reply(
                matchedRule.replyText
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
        latestQr = null;

        // Do not retain old group members
        // after WhatsApp disconnects.
        clearGroupMemberCache();

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
// STATE CHANGED
// =====================================================
client.on(
    'change_state',
    state => {
        console.log(
            'WhatsApp state:',
            state
        );

        io.emit(
            'whatsappState',
            String(state)
        );
    }
);

// =====================================================
// START SERVER
// =====================================================
async function start() {
    await initDatabase();

    server.listen(
        PORT,
        () => {
            console.log(
                `Server running on port ${PORT}`
            );
        }
    );

    console.log(
        'Initializing WhatsApp...'
    );

    try {
        await client.initialize();
    } catch (error) {
        console.error(
            'WhatsApp initialize error:',
            error
        );

        io.emit(
            'message',
            `WhatsApp initialization failed: ${error.message}`
        );
    }
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