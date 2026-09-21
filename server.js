const express = require('express');
const http = require('http');
const path = require('path');
const crypto = require('crypto');
const net = require('net');
const { Server } = require('socket.io');
const { Client, LocalAuth } = require('whatsapp-web.js');
const qrcode = require('qrcode');
const mysql = require('mysql2/promise');

const app = express();
const server = http.createServer(app);
const io = new Server(server);

const PORT = Number(process.env.PORT || 3000);
const GROUP_MEMBER_CACHE_TTL_MS = Number(
    process.env.GROUP_MEMBER_CACHE_TTL_MS || 5 * 60 * 1000
);
const LOCAL_PHONE_LENGTH = Number(process.env.LOCAL_PHONE_LENGTH || 10);
const DEFAULT_COUNTRY_CODE = String(
    process.env.DEFAULT_COUNTRY_CODE || '91'
).replace(/\D/g, '');
const API_RATE_LIMIT = Number(process.env.API_RATE_LIMIT || 60);
const API_RATE_WINDOW_MS = Number(
    process.env.API_RATE_WINDOW_MS || 60 * 1000
);
const ADMIN_RATE_LIMIT = Number(process.env.ADMIN_RATE_LIMIT || 20);
const ADMIN_RATE_WINDOW_MS = Number(
    process.env.ADMIN_RATE_WINDOW_MS || 60 * 1000
);

if (!DEFAULT_COUNTRY_CODE) {
    throw new Error('DEFAULT_COUNTRY_CODE must contain digits.');
}

if (
    !Number.isInteger(LOCAL_PHONE_LENGTH) ||
    LOCAL_PHONE_LENGTH < 5 ||
    LOCAL_PHONE_LENGTH > 15
) {
    throw new Error('LOCAL_PHONE_LENGTH must be between 5 and 15.');
}

// Railway and other reverse proxies normally pass the real client address
// through X-Forwarded-For. Keep this explicit so the whitelist checks the
// actual client address instead of the proxy address.
app.set('trust proxy', 1);

app.disable('x-powered-by');

app.use(
    express.json({
        limit: '16kb',
        strict: true
    })
);

app.use(
    express.urlencoded({
        extended: false,
        limit: '16kb'
    })
);

app.use((req, res, next) => {
    res.setHeader('X-Content-Type-Options', 'nosniff');
    res.setHeader('X-Frame-Options', 'DENY');
    res.setHeader('Referrer-Policy', 'no-referrer');
    res.setHeader('Cache-Control', 'no-store');
    next();
});

app.use(express.static(path.join(__dirname, 'public')));

app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// =====================================================
// STATE
// =====================================================

let db = null;
let whatsappReady = false;
let latestQr = null;

// groupId -> {
//     groupId,
//     groupName,
//     memberPhones: Set of full international phone digits,
//     memberCount,
//     resolvedPhoneCount,
//     unresolvedLidCount,
//     fetchedAt
// }
const groupMemberCache = new Map();
const groupMemberRefreshes = new Map();

// ip -> { windowStartedAt, count }
const rateLimitBuckets = new Map();
const adminRateLimitBuckets = new Map();

// =====================================================
// DATABASE
// =====================================================

function createDatabasePool() {
    const connectionUrl =
        process.env.MYSQL_URL || process.env.DATABASE_URL;

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
        host: process.env.MYSQLHOST || process.env.DB_HOST,
        port: Number(
            process.env.MYSQLPORT || process.env.DB_PORT || 3306
        ),
        user: process.env.MYSQLUSER || process.env.DB_USER,
        password:
            process.env.MYSQLPASSWORD || process.env.DB_PASSWORD,
        database:
            process.env.MYSQLDATABASE || process.env.DB_NAME,
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

        // Remove tables belonging to features that no longer exist.
        // sessions is also obsolete because WhatsApp LocalAuth handles
        // WhatsApp authentication persistence for this application.
        await db.query(`
            DROP TABLE IF EXISTS auto_replies, auto_replies_v2, sessions
        `);

        await db.query(`
            CREATE TABLE IF NOT EXISTS api_ip_whitelist (
                id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
                ip_address VARCHAR(45) NOT NULL,
                label VARCHAR(100) DEFAULT NULL,
                enabled TINYINT(1) NOT NULL DEFAULT 1,
                created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
                    ON UPDATE CURRENT_TIMESTAMP,
                PRIMARY KEY (id),
                UNIQUE KEY unique_ip_address (ip_address),
                INDEX idx_whitelist_enabled (enabled)
            )
            ENGINE=InnoDB
            DEFAULT CHARSET=utf8mb4
            COLLATE=utf8mb4_unicode_ci
        `);

        console.log(
            'Database ready. Auto-reply and session tables removed.'
        );
    } catch (error) {
        console.error(
            'MySQL connection failed:',
            error.message
        );

        db = null;
    }
}

async function requireDatabase() {
    if (!db) {
        throw new Error('Database is unavailable.');
    }

    return db;
}

async function listWhitelistedIps() {
    const pool = await requireDatabase();

    const [rows] = await pool.execute(`
        SELECT
            id,
            ip_address AS ip,
            label,
            enabled,
            created_at,
            updated_at
        FROM api_ip_whitelist
        ORDER BY id ASC
    `);

    return rows.map(row => ({
        id: Number(row.id),
        ip: String(row.ip),
        label:
            row.label == null
                ? ''
                : String(row.label),
        enabled: Boolean(row.enabled),
        createdAt: row.created_at,
        updatedAt: row.updated_at
    }));
}

async function addWhitelistedIp(ip, label = '') {
    const pool = await requireDatabase();

    const normalizedIp = normalizeIpAddress(ip);
    const cleanLabel = sanitizeLabel(label);

    await pool.execute(
        `
        INSERT INTO api_ip_whitelist (
            ip_address,
            label,
            enabled
        )
        VALUES (?, ?, 1)
        ON DUPLICATE KEY UPDATE
            label = VALUES(label),
            enabled = 1,
            updated_at = CURRENT_TIMESTAMP
        `,
        [normalizedIp, cleanLabel || null]
    );

    return normalizedIp;
}

async function removeWhitelistedIp(ip) {
    const pool = await requireDatabase();

    const normalizedIp = normalizeIpAddress(ip);

    const [result] = await pool.execute(
        `
        DELETE FROM api_ip_whitelist
        WHERE ip_address = ?
        `,
        [normalizedIp]
    );

    return Number(result.affectedRows || 0);
}

async function isWhitelistedIp(ip) {
    const pool = await requireDatabase();

    const normalizedIp = normalizeIpAddress(ip);

    const [rows] = await pool.execute(
        `
        SELECT id
        FROM api_ip_whitelist
        WHERE ip_address = ?
          AND enabled = 1
        LIMIT 1
        `,
        [normalizedIp]
    );

    return rows.length > 0;
}

// =====================================================
// IP / SECURITY HELPERS
// =====================================================

function normalizeIpAddress(value) {
    let ip = String(value ?? '').trim();

    if (ip.startsWith('::ffff:')) {
        ip = ip.slice(7);
    }

    if (!net.isIP(ip)) {
        throw new Error('Invalid IP address.');
    }

    return ip;
}

function getClientIp(req) {
    const candidates = [];

    if (req.ip) {
        candidates.push(req.ip);
    }

    if (req.socket?.remoteAddress) {
        candidates.push(req.socket.remoteAddress);
    }

    for (const value of candidates) {
        try {
            return normalizeIpAddress(value);
        } catch (_) {}
    }

    return '0.0.0.0';
}

function sanitizeLabel(value) {
    const label = String(value ?? '')
        .normalize('NFKC')
        .trim()
        .replace(/\s+/g, ' ');

    if (label.length > 100) {
        throw new Error('Label is too long.');
    }

    return label;
}

function safeSecretEquals(provided, expected) {
    const providedHash = crypto
        .createHash('sha256')
        .update(String(provided || ''), 'utf8')
        .digest();

    const expectedHash = crypto
        .createHash('sha256')
        .update(String(expected || ''), 'utf8')
        .digest();

    return crypto.timingSafeEqual(
        providedHash,
        expectedHash
    );
}

function consumeRateLimit(
    map,
    key,
    limit,
    windowMs
) {
    const now = Date.now();
    const current = map.get(key);

    if (
        !current ||
        now - current.windowStartedAt >= windowMs
    ) {
        map.set(key, {
            windowStartedAt: now,
            count: 1
        });

        return {
            allowed: true,
            retryAfterSeconds: Math.ceil(
                windowMs / 1000
            )
        };
    }

    current.count += 1;

    if (current.count > limit) {
        const retryAfterSeconds = Math.max(
            1,
            Math.ceil(
                (
                    windowMs -
                    (now - current.windowStartedAt)
                ) / 1000
            )
        );

        return {
            allowed: false,
            retryAfterSeconds
        };
    }

    return {
        allowed: true,
        retryAfterSeconds: Math.ceil(
            windowMs / 1000
        )
    };
}

async function requireApiIpWhitelist(
    req,
    res,
    next
) {
    if (req.method === 'OPTIONS') {
        return next();
    }

    const ip = getClientIp(req);

    try {
        if (!(await isWhitelistedIp(ip))) {
            return res.status(403).json({
                success: false,
                error: 'Request IP is not whitelisted.'
            });
        }

        const rate = consumeRateLimit(
            rateLimitBuckets,
            ip,
            API_RATE_LIMIT,
            API_RATE_WINDOW_MS
        );

        if (!rate.allowed) {
            res.setHeader(
                'Retry-After',
                String(rate.retryAfterSeconds)
            );

            return res.status(429).json({
                success: false,
                error:
                    'Too many API requests. Try again later.'
            });
        }

        req.clientIp = ip;

        return next();
    } catch (error) {
        console.error(
            'API whitelist check failed:',
            error.message
        );

        return res.status(503).json({
            success: false,
            error:
                'API whitelist service is unavailable.'
        });
    }
}

function requireAdminKey(req, res, next) {
    const configuredKey = String(
        process.env.WHITELIST_ADMIN_KEY || ''
    );

    if (!configuredKey) {
        return res.status(503).json({
            success: false,
            error:
                'Whitelist administration is disabled. Set WHITELIST_ADMIN_KEY.'
        });
    }

    const ip = getClientIp(req);

    const rate = consumeRateLimit(
        adminRateLimitBuckets,
        ip,
        ADMIN_RATE_LIMIT,
        ADMIN_RATE_WINDOW_MS
    );

    if (!rate.allowed) {
        res.setHeader(
            'Retry-After',
            String(rate.retryAfterSeconds)
        );

        return res.status(429).json({
            success: false,
            error:
                'Too many administration requests. Try again later.'
        });
    }

    const providedKey = req.get('X-Admin-Key') || '';

    if (!safeSecretEquals(providedKey, configuredKey)) {
        return res.status(401).json({
            success: false,
            error: 'Invalid administration key.'
        });
    }

    req.clientIp = ip;

    next();
}

function normalizeGroupId(value) {
    const groupId = String(value ?? '')
        .normalize('NFKC')
        .trim();

    if (!/^[0-9]{10,40}@g\.us$/.test(groupId)) {
        throw new Error(
            'Invalid Group ID. Expected digits followed by @g.us.'
        );
    }

    return groupId;
}

function normalizeLocalPhoneNumber(value) {
    const raw = String(value ?? '')
        .normalize('NFKC')
        .trim();

    if (!raw) {
        throw new Error('Phone number is required.');
    }

    if (raw.includes('+')) {
        throw new Error(
            'Enter the phone number without a country code.'
        );
    }

    if (!/^[0-9\s().-]+$/.test(raw)) {
        throw new Error(
            'Phone number contains invalid characters.'
        );
    }

    let local = raw.replace(/\D/g, '');

    if (local.startsWith('00')) {
        throw new Error(
            'Enter the phone number without a country code.'
        );
    }

    local = local.replace(/^0+/, '');

    if (
        !/^\d+$/.test(local) ||
        local.length !== LOCAL_PHONE_LENGTH
    ) {
        throw new Error(
            `Enter exactly ${LOCAL_PHONE_LENGTH} local digits without a country code.`
        );
    }

    const international =
        `${DEFAULT_COUNTRY_CODE}${local}`;

    if (international.length > 15) {
        throw new Error(
            'The resulting phone number is too long.'
        );
    }

    return {
        local,
        international
    };
}

function clearGroupMemberCache() {
    groupMemberCache.clear();
    groupMemberRefreshes.clear();

    console.log(
        'GROUP MEMBER CACHE CLEARED.'
    );
}

// =====================================================
// WHATSAPP ACCOUNT / GROUP HELPERS
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

async function getGroupList() {
    if (!whatsappReady || !client.pupPage) {
        throw new Error(
            'WhatsApp is not ready.'
        );
    }

    return client.pupPage.evaluate(() => {
        const collections =
            window.require('WAWebCollections');

        if (!collections?.Chat) {
            throw new Error(
                'WAWebCollections.Chat is unavailable.'
            );
        }

        const groups =
            collections.Chat
                .getModelsArray()
                .map(chat => {
                    try {
                        const id =
                            chat?.id?._serialized ||
                            (
                                chat?.id?.user &&
                                chat?.id?.server
                                    ? `${chat.id.user}@${chat.id.server}`
                                    : null
                            );

                        if (
                            !id ||
                            !String(id).endsWith('@g.us')
                        ) {
                            return null;
                        }

                        const name =
                            chat.formattedTitle ||
                            chat.name ||
                            chat.groupMetadata?.subject ||
                            'Unnamed Group';

                        return {
                            id: String(id),
                            name: String(
                                name || 'Unnamed Group'
                            )
                        };
                    } catch (_) {
                        return null;
                    }
                })
                .filter(Boolean);

        const unique = new Map();

        for (const group of groups) {
            unique.set(group.id, group);
        }

        return Array.from(unique.values())
            .sort((a, b) =>
                a.name.localeCompare(b.name)
            );
    });
}

async function fetchGroupParticipantsWithPhones(
    groupId
) {
    if (!client.pupPage) {
        throw new Error(
            'WhatsApp browser page is not ready.'
        );
    }

    return client.pupPage.evaluate(
        async requestedGroupId => {
            const collections =
                window.require('WAWebCollections');

            const widFactory =
                window.require('WAWebWidFactory');

            const groupQuery =
                window.require('WAWebGroupQueryJob');

            if (
                !collections?.Chat ||
                !widFactory ||
                !groupQuery
            ) {
                throw new Error(
                    'Required WhatsApp Web internals are unavailable.'
                );
            }

            const groupWid =
                widFactory.createWid(
                    requestedGroupId
                );

            let group =
                collections.Chat.get(
                    groupWid
                );

            if (!group) {
                group =
                    await collections.Chat.find(
                        groupWid
                    );
            }

            if (!group) {
                throw new Error(
                    'Target group was not found in WhatsApp Web.'
                );
            }

            try {
                await groupQuery
                    .queryAndUpdateGroupMetadataById(
                        {
                            id: requestedGroupId
                        }
                    );
            } catch (error) {
                console.warn(
                    'Group metadata refresh warning:',
                    error?.message ||
                        String(error)
                );
            }

            group =
                collections.Chat.get(groupWid) ||
                group;

            const serializedParticipants =
                group
                    .groupMetadata
                    ?.participants
                    ?.serialize?.() || [];

            const phoneNumbers = [];
            const unresolvedLids = [];

            for (
                const participant of
                serializedParticipants
            ) {
                const id =
                    participant?.id;

                if (!id) {
                    continue;
                }

                const serialized =
                    id._serialized ||
                    (
                        id.user &&
                        id.server
                            ? `${id.user}@${id.server}`
                            : null
                    );

                if (!serialized) {
                    continue;
                }

                if (id.server === 'c.us') {
                    const digits =
                        String(id.user || '')
                            .replace(/\D/g, '');

                    if (digits) {
                        phoneNumbers.push(
                            digits
                        );
                    }

                    continue;
                }

                if (id.server === 'lid') {
                    unresolvedLids.push(
                        String(serialized)
                    );

                    try {
                        let resolved = null;

                        if (
                            window.WWebJS &&
                            typeof window.WWebJS
                                .enforceLidAndPnRetrieval ===
                                'function'
                        ) {
                            resolved =
                                await window.WWebJS
                                    .enforceLidAndPnRetrieval(
                                        serialized
                                    );
                        }

                        const phoneId =
                            resolved?.phone
                                ?._serialized ||
                            (
                                resolved?.phone?.user &&
                                resolved?.phone?.server
                                    ? `${resolved.phone.user}@${resolved.phone.server}`
                                    : null
                            );

                        if (
                            phoneId &&
                            phoneId.endsWith('@c.us')
                        ) {
                            const digits =
                                phoneId
                                    .slice(0, -5)
                                    .replace(/\D/g, '');

                            if (digits) {
                                phoneNumbers.push(
                                    digits
                                );
                            }
                        }
                    } catch (error) {
                        console.warn(
                            `Could not resolve LID ${serialized}:`,
                            error?.message ||
                                String(error)
                        );
                    }
                }
            }

            return {
                groupName: String(
                    group.formattedTitle ||
                    group.name ||
                    group.groupMetadata
                        ?.subject ||
                    'Unknown Group'
                ),

                phoneNumbers,

                unresolvedLidCount:
                    unresolvedLids.length,

                participantCount:
                    serializedParticipants.length
            };
        },
        groupId
    );
}

async function fetchGroupMemberCache(
    groupId
) {
    if (!whatsappReady) {
        throw new Error(
            'WhatsApp is not ready. Please wait or relogin.'
        );
    }

    const normalizedGroupId =
        normalizeGroupId(groupId);

    console.log(
        'Refreshing group member cache:',
        normalizedGroupId
    );

    const data =
        await fetchGroupParticipantsWithPhones(
            normalizedGroupId
        );

    const memberPhones =
        new Set(data.phoneNumbers);

    const cacheEntry = {
        groupId: normalizedGroupId,

        groupName:
            data.groupName ||
            'Unknown Group',

        memberPhones,

        memberCount:
            Number(
                data.participantCount || 0
            ),

        resolvedPhoneCount:
            memberPhones.size,

        unresolvedLidCount:
            Number(
                data.unresolvedLidCount || 0
            ),

        fetchedAt: Date.now()
    };

    groupMemberCache.set(
        normalizedGroupId,
        cacheEntry
    );

    console.log(
        `GROUP CACHE REFRESHED: ${normalizedGroupId} | ` +
        `participants=${cacheEntry.memberCount} | ` +
        `resolvedPhones=${cacheEntry.resolvedPhoneCount} | ` +
        `unresolvedLids=${cacheEntry.unresolvedLidCount}`
    );

    return cacheEntry;
}

async function getGroupMemberCache(
    groupId
) {
    const normalizedGroupId =
        normalizeGroupId(groupId);

    const existing =
        groupMemberCache.get(
            normalizedGroupId
        );

    const now = Date.now();

    if (
        existing &&
        (
            now - existing.fetchedAt
        ) < GROUP_MEMBER_CACHE_TTL_MS
    ) {
        return {
            ...existing,
            fromCache: true
        };
    }

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

function isPhoneInGroup(
    cacheEntry,
    normalizedPhone
) {
    return cacheEntry.memberPhones.has(
        normalizedPhone
    );
}

async function checkGroupMembership(
    groupId,
    phoneValue
) {
    const normalizedGroupId =
        normalizeGroupId(groupId);

    const phone =
        normalizeLocalPhoneNumber(
            phoneValue
        );

    if (!whatsappReady) {
        throw new Error(
            'WhatsApp is not ready. Please wait or relogin.'
        );
    }

    const cache =
        await getGroupMemberCache(
            normalizedGroupId
        );

    const isMember =
        isPhoneInGroup(
            cache,
            phone.international
        );

    return {
        phoneNumber: phone.local,

        groupId:
            cache.groupId,

        groupName:
            cache.groupName,

        isMember,

        fromCache:
            Boolean(cache.fromCache),

        cacheAgeSeconds:
            Math.max(
                0,
                Math.floor(
                    (
                        Date.now() -
                        cache.fetchedAt
                    ) / 1000
                )
            ),

        cacheTtlSeconds:
            Math.floor(
                GROUP_MEMBER_CACHE_TTL_MS / 1000
            )
    };
}

// =====================================================
// CORS
// =====================================================

// The IP whitelist is the API access control.
// CORS is open so a whitelisted website/browser
// can make the POST request directly.
//
// Do not use credentials/cookies for this API.
app.options(
    '/api/check-group-membership',
    (req, res) => {
        res.setHeader(
            'Access-Control-Allow-Origin',
            '*'
        );

        res.setHeader(
            'Access-Control-Allow-Methods',
            'POST, OPTIONS'
        );

        res.setHeader(
            'Access-Control-Allow-Headers',
            'Content-Type'
        );

        return res.sendStatus(204);
    }
);

app.use(
    '/api/check-group-membership',
    (req, res, next) => {
        res.setHeader(
            'Access-Control-Allow-Origin',
            '*'
        );

        res.setHeader(
            'Access-Control-Allow-Methods',
            'POST, OPTIONS'
        );

        res.setHeader(
            'Access-Control-Allow-Headers',
            'Content-Type'
        );

        next();
    }
);

// =====================================================
// PUBLIC API
// =====================================================

app.post(
    '/api/check-group-membership',
    requireApiIpWhitelist,
    async (req, res) => {
        try {
            if (!req.is('application/json')) {
                return res.status(415).json({
                    success: false,
                    error:
                        'Content-Type must be application/json.'
                });
            }

            if (
                !req.body ||
                typeof req.body !== 'object' ||
                Array.isArray(req.body)
            ) {
                return res.status(400).json({
                    success: false,
                    error:
                        'Request body must be a JSON object.'
                });
            }

            const result =
                await checkGroupMembership(
                    req.body.groupId,
                    req.body.phoneNumber
                );

            return res.status(200).json({
                success: true,
                ...result
            });
        } catch (error) {
            console.error(
                'POST /api/check-group-membership:',
                error.message
            );

            const message = String(
                error?.message ||
                    'Request failed.'
            );

            if (
                message.includes(
                    'Invalid Group ID'
                ) ||
                message.includes(
                    'Phone number'
                ) ||
                message.includes(
                    'phone number'
                ) ||
                message.includes(
                    'exactly '
                ) ||
                message.includes(
                    'country code'
                )
            ) {
                return res.status(400).json({
                    success: false,
                    error: message
                });
            }

            if (
                message.includes(
                    'WhatsApp is not ready'
                )
            ) {
                return res.status(503).json({
                    success: false,
                    error: message
                });
            }

            if (
                message.includes(
                    'Target group was not found'
                )
            ) {
                return res.status(404).json({
                    success: false,
                    error:
                        'Target group was not found.'
                });
            }

            return res.status(500).json({
                success: false,
                error:
                    'Membership check failed.'
            });
        }
    }
);

// =====================================================
// ADMIN WHITELIST API
// =====================================================

app.post(
    '/api/admin/whitelist/list',
    requireAdminKey,
    async (req, res) => {
        try {
            const items =
                await listWhitelistedIps();

            return res.status(200).json({
                success: true,
                items
            });
        } catch (error) {
            console.error(
                'Whitelist list error:',
                error.message
            );

            return res.status(503).json({
                success: false,
                error:
                    'Whitelist service is unavailable.'
            });
        }
    }
);

app.post(
    '/api/admin/whitelist/current-ip',
    requireAdminKey,
    async (req, res) => {
        return res.status(200).json({
            success: true,
            ip:
                req.clientIp ||
                getClientIp(req)
        });
    }
);

app.post(
    '/api/admin/whitelist/add',
    requireAdminKey,
    async (req, res) => {
        try {
            const ip =
                normalizeIpAddress(
                    req.body?.ip
                );

            const label =
                sanitizeLabel(
                    req.body?.label
                );

            await addWhitelistedIp(
                ip,
                label
            );

            return res.status(200).json({
                success: true,
                message:
                    'IP address added/enabled.',
                ip,
                items:
                    await listWhitelistedIps()
            });
        } catch (error) {
            console.error(
                'Whitelist add error:',
                error.message
            );

            return res.status(400).json({
                success: false,
                error: error.message
            });
        }
    }
);

app.post(
    '/api/admin/whitelist/remove',
    requireAdminKey,
    async (req, res) => {
        try {
            const ip =
                normalizeIpAddress(
                    req.body?.ip
                );

            const affectedRows =
                await removeWhitelistedIp(
                    ip
                );

            return res.status(200).json({
                success: true,
                removed:
                    affectedRows > 0,
                ip,
                items:
                    await listWhitelistedIps()
            });
        } catch (error) {
            console.error(
                'Whitelist remove error:',
                error.message
            );

            return res.status(400).json({
                success: false,
                error: error.message
            });
        }
    }
);

// =====================================================
// SOCKET.IO
// =====================================================

io.on('connection', socket => {
    console.log(
        'Browser connected:',
        socket.id
    );

    if (
        latestQr &&
        !whatsappReady
    ) {
        socket.emit(
            'qr',
            latestQr
        );

        socket.emit(
            'status',
            {
                type: 'warning',
                message:
                    'Scan the QR code to connect WhatsApp.'
            }
        );
    }

    if (whatsappReady) {
        socket.emit(
            'ready',
            {
                message:
                    'WhatsApp is connected.'
            }
        );

        void sendGroups(socket);
    } else if (!latestQr) {
        socket.emit(
            'status',
            {
                type: 'info',
                message:
                    'Starting WhatsApp...'
            }
        );
    }

    socket.on(
        'getGroups',
        async () => {
            try {
                await sendGroups(
                    socket
                );
            } catch (error) {
                socket.emit(
                    'groupListError',
                    error.message
                );
            }
        }
    );

    socket.on(
        'checkGroupMembership',
        async data => {
            try {
                const result =
                    await checkGroupMembership(
                        data?.groupId,
                        data?.phoneNumber
                    );

                socket.emit(
                    'groupMembershipResult',
                    result
                );
            } catch (error) {
                console.error(
                    'Socket membership check error:',
                    error.message
                );

                socket.emit(
                    'groupMembershipError',
                    error?.message ||
                        'Failed to check membership.'
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
});

async function sendGroups(socket) {
    if (!whatsappReady) {
        socket.emit(
            'groupListError',
            'WhatsApp is not ready yet.'
        );

        return;
    }

    try {
        const groups =
            await getGroupList();

        socket.emit(
            'groups',
            groups
        );

        socket.emit(
            'status',
            {
                type: 'success',
                message:
                    `Loaded ${groups.length} groups.`
            }
        );
    } catch (error) {
        console.error(
            'Group list fetch failed:',
            error.message
        );

        socket.emit(
            'groupListError',
            error.message
        );
    }
}

// =====================================================
// WHATSAPP EVENTS
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
                'status',
                {
                    type: 'warning',
                    message:
                        'Scan the QR code to connect WhatsApp.'
                }
            );
        } catch (error) {
            console.error(
                'QR generation failed:',
                error
            );

            io.emit(
                'status',
                {
                    type: 'error',
                    message:
                        'Failed to generate QR code.'
                }
            );
        }
    }
);

client.on(
    'loading_screen',
    (percent, message) => {
        console.log(
            `WhatsApp loading: ${percent}% ${message || ''}`
        );

        io.emit(
            'status',
            {
                type: 'info',
                message:
                    `WhatsApp loading: ${percent}%`
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

        latestQr = null;

        io.emit(
            'status',
            {
                type: 'info',
                message:
                    'WhatsApp authenticated. Loading...'
            }
        );
    }
);

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
            'status',
            {
                type: 'error',
                message:
                    'WhatsApp authentication failed.'
            }
        );
    }
);

client.on(
    'ready',
    async () => {
        whatsappReady = true;
        latestQr = null;

        clearGroupMemberCache();

        console.log(
            'WHATSAPP READY'
        );

        io.emit(
            'ready',
            {
                message:
                    'WhatsApp is connected.'
            }
        );

        io.emit(
            'status',
            {
                type: 'success',
                message:
                    'WhatsApp is connected.'
            }
        );

        try {
            await Promise.all(
                Array.from(
                    io.sockets.sockets.values()
                ).map(
                    socket =>
                        sendGroups(socket)
                )
            );
        } catch (error) {
            console.error(
                'Initial group list error:',
                error.message
            );
        }
    }
);

client.on(
    'disconnected',
    reason => {
        whatsappReady = false;
        latestQr = null;

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
            'status',
            {
                type: 'error',
                message:
                    `WhatsApp disconnected: ${reason || 'Disconnected'}`
            }
        );
    }
);

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

            console.log(
                'API endpoint: POST /api/check-group-membership'
            );

            console.log(
                `Default country code: ${DEFAULT_COUNTRY_CODE}`
            );

            console.log(
                `Local phone length: ${LOCAL_PHONE_LENGTH}`
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
            'status',
            {
                type: 'error',
                message:
                    'WhatsApp initialization failed.'
            }
        );
    }
}

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

start().catch(
    error => {
        console.error(
            'Fatal startup error:',
            error
        );

        process.exit(1);
    }
);