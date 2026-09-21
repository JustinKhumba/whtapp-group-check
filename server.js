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

const LOCAL_PHONE_LENGTH = Number(
    process.env.LOCAL_PHONE_LENGTH || 10
);

const DEFAULT_COUNTRY_CODE = String(
    process.env.DEFAULT_COUNTRY_CODE || '91'
).replace(/\D/g, '');

const API_RATE_LIMIT = Number(
    process.env.API_RATE_LIMIT || 60
);

const API_RATE_WINDOW_MS = Number(
    process.env.API_RATE_WINDOW_MS || 60 * 1000
);

const ADMIN_RATE_LIMIT = Number(
    process.env.ADMIN_RATE_LIMIT || 20
);

const ADMIN_RATE_WINDOW_MS = Number(
    process.env.ADMIN_RATE_WINDOW_MS || 60 * 1000
);

const PAYLOAD_TTL_MS = Number(
    process.env.PAYLOAD_TTL_MS || 2 * 60 * 1000
);

const PAYLOAD_NONCE_TTL_MS = Number(
    process.env.PAYLOAD_NONCE_TTL_MS || 5 * 60 * 1000
);

const MAX_CLOCK_SKEW_MS = Number(
    process.env.MAX_CLOCK_SKEW_MS || 30 * 1000
);

const MAX_API_BODY_BYTES = 32 * 1024;

if (!DEFAULT_COUNTRY_CODE) {
    throw new Error(
        'DEFAULT_COUNTRY_CODE must contain digits.'
    );
}

if (
    !Number.isInteger(LOCAL_PHONE_LENGTH) ||
    LOCAL_PHONE_LENGTH < 5 ||
    LOCAL_PHONE_LENGTH > 15
) {
    throw new Error(
        'LOCAL_PHONE_LENGTH must be between 5 and 15.'
    );
}

if (
    !process.env.API_ENCRYPTION_SECRET ||
    String(process.env.API_ENCRYPTION_SECRET).length < 32
) {
    throw new Error(
        'API_ENCRYPTION_SECRET is required and must be at least 32 characters.'
    );
}

if (
    !process.env.WHITELIST_ADMIN_KEY ||
    String(process.env.WHITELIST_ADMIN_KEY).length < 32
) {
    throw new Error(
        'WHITELIST_ADMIN_KEY is required and must be at least 32 characters.'
    );
}

/*
 * IMPORTANT:
 *
 * API_ENCRYPTION_SECRET must NEVER be placed in browser JavaScript.
 *
 * The calling website should generate the encrypted payload from its
 * own backend/server using the same secret.
 *
 * WHITELIST_ADMIN_KEY is only for whitelist administration.
 */

app.disable('x-powered-by');

/*
 * Railway normally sits behind a proxy.
 *
 * This trusts one proxy hop so req.ip can represent the original
 * caller IP when the proxy supplies X-Forwarded-For.
 *
 * Make sure your deployment/proxy configuration is trusted.
 */
app.set('trust proxy', 1);

app.use(
    express.json({
        limit: MAX_API_BODY_BYTES,
        strict: true
    })
);

app.use(
    express.static(
        path.join(__dirname, 'public')
    )
);

app.use((req, res, next) => {
    res.setHeader(
        'X-Content-Type-Options',
        'nosniff'
    );

    res.setHeader(
        'X-Frame-Options',
        'DENY'
    );

    res.setHeader(
        'Referrer-Policy',
        'no-referrer'
    );

    res.setHeader(
        'Cache-Control',
        'no-store'
    );

    next();
});

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
// STATE
// =====================================================

let db = null;
let whatsappReady = false;
let latestQr = null;

const groupMemberCache = new Map();
const groupMemberRefreshes = new Map();

const rateLimitBuckets = new Map();
const adminRateLimitBuckets = new Map();

const usedPayloadNonces = new Map();
const usedAdminNonces = new Map();

// =====================================================
// DATABASE
// =====================================================

function createDatabasePool() {
    const connectionUrl =
        process.env.MYSQL_URL ||
        process.env.DATABASE_URL;

    if (connectionUrl) {
        const url = new URL(connectionUrl);

        return mysql.createPool({
            host: url.hostname,
            port: Number(
                url.port || 3306
            ),
            user: decodeURIComponent(
                url.username
            ),
            password: decodeURIComponent(
                url.password
            ),
            database: decodeURIComponent(
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

async function initDatabase() {
    try {
        db = createDatabasePool();

        await db.query('SELECT 1');

        console.log(
            'MySQL connected.'
        );

        /*
         * Remove obsolete tables that are no longer used.
         */
        await db.query(`
            DROP TABLE IF EXISTS
                auto_replies,
                auto_replies_v2,
                sessions
        `);

        /*
         * API whitelist.
         *
         * ONLY the domain is stored and checked.
         * The whitelist table intentionally contains no IP,
         * label, enabled, or other unused columns.
         *
         * Older api_whitelist versions are migrated so existing
         * domains are preserved and duplicate domains are merged.
         */
        await ensureDomainOnlyWhitelistTable();

        console.log(
            'Database ready.'
        );

    } catch (error) {
        console.error(
            'MySQL connection failed:',
            error.message
        );

        db = null;
    }
}

async function ensureDomainOnlyWhitelistTable() {
    const pool =
        await requireDatabaseWithoutCheck();

    const [tableRows] =
        await pool.execute(
            `
            SELECT COUNT(*) AS table_exists
            FROM information_schema.tables
            WHERE table_schema = DATABASE()
              AND table_name = 'api_whitelist'
            `
        );

    const tableExists =
        Number(
            tableRows[0]?.table_exists || 0
        ) > 0;

    const createTable =
        async tableName => {
            await pool.query(`
                CREATE TABLE ${tableName} (
                    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
                    domain_name VARCHAR(253) NOT NULL,
                    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (id),
                    UNIQUE KEY unique_domain_name (domain_name)
                )
                ENGINE=InnoDB
                DEFAULT CHARSET=utf8mb4
                COLLATE=utf8mb4_unicode_ci
            `);
        };

    if (!tableExists) {
        await createTable(
            'api_whitelist'
        );

        return;
    }

    const [columns] =
        await pool.query(
            'SHOW COLUMNS FROM api_whitelist'
        );

    const columnNames =
        new Set(
            columns.map(
                column =>
                    String(
                        column.Field
                    )
            )
        );

    const [indexes] =
        await pool.query(
            'SHOW INDEX FROM api_whitelist'
        );

    const hasUniqueDomain =
        indexes.some(
            index =>
                Number(
                    index.Non_unique
                ) === 0 &&
                String(
                    index.Column_name
                ) ===
                    'domain_name'
        );

    const hasOnlyRequiredColumns =
        columnNames.size === 3 &&
        columnNames.has('id') &&
        columnNames.has('domain_name') &&
        columnNames.has('created_at');

    if (
        hasOnlyRequiredColumns &&
        hasUniqueDomain
    ) {
        return;
    }

    const migrationTable =
        'api_whitelist_domain_only_migration';

    await pool.query(
        `DROP TABLE IF EXISTS ${migrationTable}`
    );

    await createTable(
        migrationTable
    );

    if (
        columnNames.has('domain_name')
    ) {
        const createdAtExpression =
            columnNames.has(
                'created_at'
            )
                ? 'MIN(created_at)'
                : 'CURRENT_TIMESTAMP';

        await pool.query(`
            INSERT IGNORE INTO ${migrationTable} (
                domain_name,
                created_at
            )
            SELECT
                LOWER(TRIM(domain_name)) AS domain_name,
                ${createdAtExpression} AS created_at
            FROM api_whitelist
            WHERE domain_name IS NOT NULL
              AND TRIM(domain_name) <> ''
            GROUP BY LOWER(TRIM(domain_name))
        `);
    }

    await pool.query(
        'DROP TABLE api_whitelist'
    );

    await pool.query(
        `RENAME TABLE ${migrationTable} TO api_whitelist`
    );
}

async function requireDatabaseWithoutCheck() {
    if (!db) {
        throw new Error(
            'Database is unavailable.'
        );
    }

    return db;
}

async function requireDatabase() {
    if (!db) {
        throw new Error(
            'Database is unavailable.'
        );
    }

    return db;
}

// =====================================================
// IP / DOMAIN
// =====================================================

function normalizeIpAddress(value) {
    let ip = String(
        value ?? ''
    ).trim();

    if (
        ip.startsWith(
            '::ffff:'
        )
    ) {
        ip = ip.slice(7);
    }

    if (
        !net.isIP(ip)
    ) {
        throw new Error(
            'Invalid IP address.'
        );
    }

    return ip;
}

function normalizeDomain(value) {
    let domain = String(
        value ?? ''
    )
        .normalize('NFKC')
        .trim()
        .toLowerCase();

    domain = domain.replace(
        /\.$/,
        ''
    );

    if (!domain) {
        throw new Error(
            'Domain is required.'
        );
    }

    if (
        domain.includes(
            '://'
        ) ||
        domain.includes(
            '/'
        ) ||
        domain.includes(
            '\\'
        ) ||
        domain.includes(
            ':'
        ) ||
        domain.includes(
            '@'
        ) ||
        domain.includes(
            ' '
        )
    ) {
        throw new Error(
            'Domain must be hostname only.'
        );
    }

    if (
        domain.length > 253
    ) {
        throw new Error(
            'Domain is too long.'
        );
    }

    /*
     * Exact domain only.
     *
     * Examples:
     *
     * example.com
     * www.example.com
     *
     * Not accepted:
     * https://example.com
     * example.com/path
     * *.example.com
     */
    if (
        !/^(?=.{1,253}$)(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z]{2,63}$/i.test(
            domain
        )
    ) {
        throw new Error(
            'Invalid domain name.'
        );
    }

    return domain;
}

function getClientIp(req) {
    let ip =
        req.ip ||
        req.socket?.remoteAddress ||
        '0.0.0.0';

    try {
        return normalizeIpAddress(
            ip
        );
    } catch (_) {
        return '0.0.0.0';
    }
}

// =====================================================
// WHITELIST DB
// =====================================================

async function isWhitelistedDomain(
    domain
) {
    const pool =
        await requireDatabase();

    const normalizedDomain =
        normalizeDomain(
            domain
        );

    const [rows] =
        await pool.execute(
            `
            SELECT id
            FROM api_whitelist
            WHERE domain_name = ?
            LIMIT 1
            `,
            [
                normalizedDomain
            ]
        );

    return rows.length > 0;
}

async function listWhitelist() {
    const pool =
        await requireDatabase();

    const [rows] =
        await pool.execute(`
            SELECT
                id,
                domain_name AS domain,
                created_at
            FROM api_whitelist
            ORDER BY id ASC
        `);

    return {
        entries:
            rows.map(
                row => ({
                    id:
                        Number(
                            row.id
                        ),

                    domain:
                        String(
                            row.domain
                        ),

                    createdAt:
                        row.created_at
                })
            )
    };
}

async function addWhitelistDomain(
    domain
) {
    const pool =
        await requireDatabase();

    const normalizedDomain =
        normalizeDomain(
            domain
        );

    await pool.execute(
        `
        INSERT INTO api_whitelist (
            domain_name
        )
        VALUES (?)
        ON DUPLICATE KEY UPDATE
            domain_name = VALUES(domain_name)
        `,
        [
            normalizedDomain
        ]
    );

    return normalizedDomain;
}

async function removeWhitelistDomain(
    id
) {
    const pool =
        await requireDatabase();

    const numericId =
        Number(
            id
        );

    if (
        !Number.isSafeInteger(
            numericId
        ) ||
        numericId < 1
    ) {
        throw new Error(
            'Invalid whitelist entry ID.'
        );
    }

    const [result] =
        await pool.execute(
            `
            DELETE FROM api_whitelist
            WHERE id = ?
            `,
            [
                numericId
            ]
        );

    return Number(
        result.affectedRows || 0
    );
}

// =====================================================
// RATE LIMIT
// =====================================================

function consumeRateLimit(
    map,
    key,
    limit,
    windowMs
) {
    const now =
        Date.now();

    const current =
        map.get(key);

    if (
        !current ||
        now -
            current.windowStartedAt >=
            windowMs
    ) {
        map.set(
            key,
            {
                windowStartedAt:
                    now,
                count: 1
            }
        );

        return {
            allowed: true,
            retryAfterSeconds:
                Math.ceil(
                    windowMs /
                        1000
                )
        };
    }

    current.count += 1;

    if (
        current.count >
        limit
    ) {
        return {
            allowed: false,
            retryAfterSeconds:
                Math.max(
                    1,
                    Math.ceil(
                        (
                            windowMs -
                            (
                                now -
                                current.windowStartedAt
                            )
                        ) /
                            1000
                    )
                )
        };
    }

    return {
        allowed: true,
        retryAfterSeconds:
            Math.ceil(
                windowMs /
                    1000
            )
    };
}

function cleanupRateLimitMap(
    map
) {
    if (
        map.size < 5000
    ) {
        return;
    }

    const now =
        Date.now();

    for (
        const [
            key,
            bucket
        ] of map
    ) {
        if (
            now -
                bucket.windowStartedAt >
            10 * 60 * 1000
        ) {
            map.delete(
                key
            );
        }
    }
}

// =====================================================
// ENCRYPTION
// =====================================================

function deriveAesKey(
    secret
) {
    return crypto
        .createHash(
            'sha256'
        )
        .update(
            String(
                secret
            ),
            'utf8'
        )
        .digest();
}

function toBase64Url(
    buffer
) {
    return Buffer.from(
        buffer
    )
        .toString(
            'base64'
        )
        .replace(
            /\+/g,
            '-'
        )
        .replace(
            /\//g,
            '_'
        )
        .replace(
            /=+$/g,
            ''
        );
}

function fromBase64Url(
    value
) {
    const normalized =
        String(
            value || ''
        )
            .replace(
                /-/g,
                '+'
            )
            .replace(
                /_/g,
                '/'
            );

    const remainder =
        normalized.length %
        4;

    const padded =
        remainder === 0
            ? normalized
            : normalized +
              '='.repeat(
                  4 - remainder
              );

    return Buffer.from(
        padded,
        'base64'
    );
}

function encryptObject(
    object,
    secret
) {
    const key =
        deriveAesKey(
            secret
        );

    const iv =
        crypto.randomBytes(
            12
        );

    const cipher =
        crypto.createCipheriv(
            'aes-256-gcm',
            key,
            iv
        );

    const plaintext =
        Buffer.from(
            JSON.stringify(
                object
            ),
            'utf8'
        );

    const ciphertext =
        Buffer.concat([
            cipher.update(
                plaintext
            ),
            cipher.final()
        ]);

    const authTag =
        cipher.getAuthTag();

    return [
        'v1',
        toBase64Url(
            iv
        ),
        toBase64Url(
            authTag
        ),
        toBase64Url(
            ciphertext
        )
    ].join('.');
}

function decryptObject(
    encodedPayload,
    secret
) {
    const value =
        String(
            encodedPayload ||
                ''
        ).trim();

    if (
        !value ||
        value.length > 30000
    ) {
        throw new Error(
            'Invalid encrypted payload.'
        );
    }

    const parts =
        value.split('.');

    if (
        parts.length !== 4 ||
        parts[0] !== 'v1'
    ) {
        throw new Error(
            'Invalid encrypted payload.'
        );
    }

    const iv =
        fromBase64Url(
            parts[1]
        );

    const authTag =
        fromBase64Url(
            parts[2]
        );

    const ciphertext =
        fromBase64Url(
            parts[3]
        );

    if (
        iv.length !== 12 ||
        authTag.length !== 16 ||
        ciphertext.length < 1 ||
        ciphertext.length > 20000
    ) {
        throw new Error(
            'Invalid encrypted payload.'
        );
    }

    try {
        const key =
            deriveAesKey(
                secret
            );

        const decipher =
            crypto.createDecipheriv(
                'aes-256-gcm',
                key,
                iv
            );

        decipher.setAuthTag(
            authTag
        );

        const plaintext =
            Buffer.concat([
                decipher.update(
                    ciphertext
                ),
                decipher.final()
            ]);

        const parsed =
            JSON.parse(
                plaintext.toString(
                    'utf8'
                )
            );

        if (
            !parsed ||
            typeof parsed !==
                'object' ||
            Array.isArray(
                parsed
            )
        ) {
            throw new Error(
                'Invalid payload object.'
            );
        }

        return parsed;

    } catch (_) {
        throw new Error(
            'Invalid encrypted payload.'
        );
    }
}

// =====================================================
// REPLAY PROTECTION
// =====================================================

function cleanupNonceMap(
    map
) {
    const now =
        Date.now();

    for (
        const [
            nonce,
            expiresAt
        ] of map
    ) {
        if (
            expiresAt <= now
        ) {
            map.delete(
                nonce
            );
        }
    }
}

function consumeNonce(
    map,
    nonce
) {
    cleanupNonceMap(
        map
    );

    if (
        typeof nonce !==
            'string' ||
        !/^[A-Za-z0-9_-]{22,128}$/.test(
            nonce
        )
    ) {
        throw new Error(
            'Invalid payload nonce.'
        );
    }

    if (
        map.has(
            nonce
        )
    ) {
        throw new Error(
            'Replay detected.'
        );
    }

    map.set(
        nonce,
        Date.now() +
            PAYLOAD_NONCE_TTL_MS
    );

    if (
        map.size > 10000
    ) {
        cleanupNonceMap(
            map
        );
    }
}

function validateTimestamp(
    timestamp
) {
    const numeric =
        Number(
            timestamp
        );

    if (
        !Number.isSafeInteger(
            numeric
        )
    ) {
        throw new Error(
            'Invalid payload timestamp.'
        );
    }

    const age =
        Math.abs(
            Date.now() -
                numeric
        );

    if (
        age >
        PAYLOAD_TTL_MS +
            MAX_CLOCK_SKEW_MS
    ) {
        throw new Error(
            'Encrypted payload has expired.'
        );
    }

    return numeric;
}

function assertAllowedKeys(
    object,
    allowedKeys
) {
    const keys =
        Object.keys(
            object
        );

    for (
        const key of keys
    ) {
        if (
            !allowedKeys.has(
                key
            )
        ) {
            throw new Error(
                'Unexpected payload field.'
            );
        }
    }
}

// =====================================================
// PAYLOAD VALIDATION
// =====================================================

function normalizeGroupId(
    value
) {
    const groupId =
        String(
            value ?? ''
        )
            .normalize(
                'NFKC'
            )
            .trim();

    if (
        !/^\d{10,40}@g\.us$/.test(
            groupId
        )
    ) {
        throw new Error(
            'Invalid Group ID.'
        );
    }

    return groupId;
}

function normalizeLocalPhoneNumber(
    value
) {
    const raw =
        String(
            value ?? ''
        )
            .normalize(
                'NFKC'
            )
            .trim();

    if (!raw) {
        throw new Error(
            'Phone number is required.'
        );
    }

    if (
        raw.includes(
            '+'
        )
    ) {
        throw new Error(
            'Phone number must not contain a country code.'
        );
    }

    if (
        !/^[0-9\s().-]+$/.test(
            raw
        )
    ) {
        throw new Error(
            'Phone number contains invalid characters.'
        );
    }

    let local =
        raw.replace(
            /\D/g,
            ''
        );

    if (
        local.startsWith(
            '00'
        )
    ) {
        throw new Error(
            'Phone number must not contain a country code.'
        );
    }

    local =
        local.replace(
            /^0+/,
            ''
        );

    if (
        !/^\d+$/.test(
            local
        ) ||
        local.length !==
            LOCAL_PHONE_LENGTH
    ) {
        throw new Error(
            `Phone number must contain exactly ${LOCAL_PHONE_LENGTH} local digits.`
        );
    }

    const international =
        DEFAULT_COUNTRY_CODE +
        local;

    if (
        international.length >
        15
    ) {
        throw new Error(
            'Phone number is too long.'
        );
    }

    return {
        local,
        international
    };
}

function validateMembershipPayload(
    payload
) {
    assertAllowedKeys(
        payload,
        new Set([
            'version',
            'timestamp',
            'nonce',
            'phoneNumber',
            'groupId',
            'clientIp',
            'domain'
        ])
    );

    if (
        Number(
            payload.version
        ) !== 1
    ) {
        throw new Error(
            'Unsupported payload version.'
        );
    }

    validateTimestamp(
        payload.timestamp
    );

    /*
     * The nonce is consumed only after
     * successful decryption.
     */
    consumeNonce(
        usedPayloadNonces,
        payload.nonce
    );

    const phone =
        normalizeLocalPhoneNumber(
            payload.phoneNumber
        );

    const groupId =
        normalizeGroupId(
            payload.groupId
        );

    const clientIp =
        normalizeIpAddress(
            payload.clientIp
        );

    const domain =
        normalizeDomain(
            payload.domain
        );

    return {
        phoneNumber:
            phone.local,

        internationalPhone:
            phone.international,

        groupId,

        clientIp,

        domain
    };
}

function validateAdminPayload(
    payload
) {
    assertAllowedKeys(
        payload,
        new Set([
            'version',
            'timestamp',
            'nonce',
            'action',
            'id',
            'domain'
        ])
    );

    if (
        Number(
            payload.version
        ) !== 1
    ) {
        throw new Error(
            'Unsupported payload version.'
        );
    }

    validateTimestamp(
        payload.timestamp
    );

    consumeNonce(
        usedAdminNonces,
        payload.nonce
    );

    const action =
        String(
            payload.action ||
                ''
        );

    const allowedActions =
        new Set([
            'list',
            'add-domain',
            'remove-domain'
        ]);

    if (
        !allowedActions.has(
            action
        )
    ) {
        throw new Error(
            'Invalid admin action.'
        );
    }

    return {
        action,

        id:
            payload.id == null
                ? null
                : (() => {
                      const numericId =
                          Number(
                              payload.id
                          );

                      if (
                          !Number.isSafeInteger(
                              numericId
                          ) ||
                          numericId < 1
                      ) {
                          throw new Error(
                              'Invalid whitelist entry ID.'
                          );
                      }

                      return numericId;
                  })(),

        domain:
            payload.domain == null
                ? null
                : normalizeDomain(
                      payload.domain
                  )
    };
}

// =====================================================
// WHATSAPP
// =====================================================

const authPath =
    process.env.WWEBJS_AUTH_PATH ||
    path.join(
        __dirname,
        '.wwebjs_auth'
    );

const client =
    new Client({
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
// GROUP LIST
// =====================================================

async function getGroupList() {
    if (
        !whatsappReady ||
        !client.pupPage
    ) {
        throw new Error(
            'WhatsApp is not ready.'
        );
    }

    return client.pupPage.evaluate(
        () => {
            const collections =
                window.require(
                    'WAWebCollections'
                );

            if (
                !collections?.Chat
            ) {
                throw new Error(
                    'WhatsApp group collection is unavailable.'
                );
            }

            const unique =
                new Map();

            for (
                const chat of
                collections.Chat.getModelsArray()
            ) {
                try {
                    const id =
                        chat?.id
                            ?._serialized ||
                        (
                            chat?.id?.user &&
                            chat?.id?.server
                                ? `${chat.id.user}@${chat.id.server}`
                                : null
                        );

                    if (
                        !id ||
                        !String(
                            id
                        ).endsWith(
                            '@g.us'
                        )
                    ) {
                        continue;
                    }

                    const name =
                        chat.formattedTitle ||
                        chat.name ||
                        chat.groupMetadata
                            ?.subject ||
                        'Unnamed Group';

                    unique.set(
                        String(
                            id
                        ),
                        {
                            id:
                                String(
                                    id
                                ),

                            name:
                                String(
                                    name ||
                                        'Unnamed Group'
                                )
                        }
                    );

                } catch (_) {}
            }

            return Array.from(
                unique.values()
            ).sort(
                (
                    a,
                    b
                ) =>
                    a.name.localeCompare(
                        b.name
                    )
            );
        }
    );
}

// =====================================================
// GROUP PARTICIPANTS
// =====================================================

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
                window.require(
                    'WAWebCollections'
                );

            const widFactory =
                window.require(
                    'WAWebWidFactory'
                );

            const groupQuery =
                window.require(
                    'WAWebGroupQueryJob'
                );

            if (
                !collections?.Chat ||
                !widFactory ||
                !groupQuery
            ) {
                throw new Error(
                    'Required WhatsApp group functions are unavailable.'
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
                    'Target group was not found.'
                );
            }

            try {
                await groupQuery
                    .queryAndUpdateGroupMetadataById(
                        {
                            id:
                                requestedGroupId
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
                collections.Chat.get(
                    groupWid
                ) || group;

            const participants =
                group
                    .groupMetadata
                    ?.participants
                    ?.serialize?.() ||
                [];

            const phoneNumbers =
                [];

            const unresolvedLids =
                [];

            for (
                const participant of
                participants
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

                if (
                    id.server ===
                    'c.us'
                ) {
                    const digits =
                        String(
                            id.user ||
                                ''
                        ).replace(
                            /\D/g,
                            ''
                        );

                    if (digits) {
                        phoneNumbers.push(
                            digits
                        );
                    }

                    continue;
                }

                if (
                    id.server ===
                    'lid'
                ) {
                    unresolvedLids.push(
                        String(
                            serialized
                        )
                    );

                    try {
                        let resolved =
                            null;

                        if (
                            window.WWebJS &&
                            typeof window
                                .WWebJS
                                .enforceLidAndPnRetrieval ===
                                'function'
                        ) {
                            resolved =
                                await window
                                    .WWebJS
                                    .enforceLidAndPnRetrieval(
                                        serialized
                                    );
                        }

                        const phoneId =
                            resolved
                                ?.phone
                                ?._serialized ||
                            (
                                resolved
                                    ?.phone
                                    ?.user &&
                                resolved
                                    ?.phone
                                    ?.server
                                    ? `${resolved.phone.user}@${resolved.phone.server}`
                                    : null
                            );

                        if (
                            phoneId &&
                            phoneId.endsWith(
                                '@c.us'
                            )
                        ) {
                            const digits =
                                phoneId
                                    .slice(
                                        0,
                                        -5
                                    )
                                    .replace(
                                        /\D/g,
                                        ''
                                    );

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
                                String(
                                    error
                                )
                        );
                    }
                }
            }

            return {
                groupName:
                    String(
                        group.formattedTitle ||
                        group.name ||
                        group.groupMetadata
                            ?.subject ||
                        'Unknown Group'
                    ),

                phoneNumbers,

                participantCount:
                    participants.length,

                unresolvedLidCount:
                    unresolvedLids.length
            };
        },
        groupId
    );
}

// =====================================================
// GROUP CACHE
// =====================================================

async function fetchGroupMemberCache(
    groupId
) {
    if (
        !whatsappReady
    ) {
        throw new Error(
            'WhatsApp is not ready.'
        );
    }

    const normalizedGroupId =
        normalizeGroupId(
            groupId
        );

    console.log(
        'Refreshing group member cache:',
        normalizedGroupId
    );

    const data =
        await fetchGroupParticipantsWithPhones(
            normalizedGroupId
        );

    const memberPhones =
        new Set(
            data.phoneNumbers
        );

    const cacheEntry = {
        groupId:
            normalizedGroupId,

        groupName:
            data.groupName ||
            'Unknown Group',

        memberPhones,

        memberCount:
            Number(
                data.participantCount ||
                    0
            ),

        resolvedPhoneCount:
            memberPhones.size,

        unresolvedLidCount:
            Number(
                data.unresolvedLidCount ||
                    0
            ),

        fetchedAt:
            Date.now()
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
        normalizeGroupId(
            groupId
        );

    const existing =
        groupMemberCache.get(
            normalizedGroupId
        );

    const now =
        Date.now();

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

// =====================================================
// MEMBERSHIP CHECK
// =====================================================

async function checkGroupMembership(
    groupId,
    phoneValue
) {
    const normalizedGroupId =
        normalizeGroupId(
            groupId
        );

    const phone =
        normalizeLocalPhoneNumber(
            phoneValue
        );

    if (!whatsappReady) {
        throw new Error(
            'WhatsApp is not ready.'
        );
    }

    const cache =
        await getGroupMemberCache(
            normalizedGroupId
        );

    /*
     * Exact full international-number
     * comparison.
     */
    const isMember =
        cache.memberPhones.has(
            phone.international
        );

    return {
        phoneNumber:
            phone.local,

        groupId:
            cache.groupId,

        groupName:
            cache.groupName,

        isMember,

        fromCache:
            Boolean(
                cache.fromCache
            ),

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
                GROUP_MEMBER_CACHE_TTL_MS /
                    1000
            )
    };
}

// =====================================================
// PUBLIC API SECURITY
// =====================================================

async function requireEncryptedMembershipRequest(
    req,
    res,
    next
) {
    if (
        req.method !==
        'POST'
    ) {
        return res
            .status(405)
            .json({
                success: false,
                error:
                    'POST only.'
            });
    }

    const sourceIp =
        getClientIp(req);

    /*
     * RATE LIMIT BY ACTUAL SOURCE IP
     */
    const sourceRate =
        consumeRateLimit(
            rateLimitBuckets,
            `source:${sourceIp}`,
            API_RATE_LIMIT,
            API_RATE_WINDOW_MS
        );

    cleanupRateLimitMap(
        rateLimitBuckets
    );

    if (
        !sourceRate.allowed
    ) {
        res.setHeader(
            'Retry-After',
            String(
                sourceRate.retryAfterSeconds
            )
        );

        return res
            .status(429)
            .json({
                success: false,
                error:
                    'Too many requests from this IP.'
            });
    }

    /*
     * ONLY ONE FIELD ALLOWED:
     *
     * {
     *     "payload": "v1...."
     * }
     */
    if (
        !req.body ||
        typeof req.body !==
            'object' ||
        Array.isArray(
            req.body
        ) ||
        typeof req.body
            .payload !==
            'string' ||
        Object.keys(
            req.body
        ).length !== 1
    ) {
        return res
            .status(400)
            .json({
                success: false,
                error:
                    'Only encrypted payload is accepted.'
            });
    }

    let decrypted;

    try {
        /*
         * DECRYPT PAYLOAD
         */
        decrypted =
            decryptObject(
                req.body.payload,
                process.env
                    .API_ENCRYPTION_SECRET
            );

        /*
         * VALIDATE EVERYTHING
         */
        const data =
            validateMembershipPayload(
                decrypted
            );

        /*
         * DOMAIN WHITELIST ONLY
         *
         * Authorization is based ONLY on the encrypted domain.
         * The request source IP is not consulted for whitelist
         * authorization.
         */
        const domainWhitelisted =
            await isWhitelistedDomain(
                data.domain
            );

        console.log(
            'DOMAIN WHITELIST CHECK:',
            JSON.stringify({
                domain:
                    data.domain,
                whitelisted:
                    domainWhitelisted
            })
        );

        if (
            !domainWhitelisted
        ) {
            return res
                .status(403)
                .json({
                    success:
                        false,
                    error:
                        'Request domain is not whitelisted.',
                    requestDomain:
                        data.domain
                });
        }

        /*
         * RATE LIMIT BY ENCRYPTED CLIENT IP
         *
         * This allows your website backend to provide
         * the end-user IP separately.
         */
        const clientRate =
            consumeRateLimit(
                rateLimitBuckets,
                `client:${data.clientIp}`,
                API_RATE_LIMIT,
                API_RATE_WINDOW_MS
            );

        cleanupRateLimitMap(
            rateLimitBuckets
        );

        if (
            !clientRate.allowed
        ) {
            res.setHeader(
                'Retry-After',
                String(
                    clientRate.retryAfterSeconds
                )
            );

            return res
                .status(429)
                .json({
                    success: false,
                    error:
                        'Too many requests for this client IP.'
                });
        }

        /*
         * Store the validated encrypted payload.
         *
         * The encrypted domain has already been checked
         * against api_whitelist above.
         */
        req.encryptedPayload =
            data;

        return next();

    } catch (error) {
        console.error(
            'Encrypted API validation failed:',
            error.message
        );

        if (
            error.message ===
            'Replay detected.'
        ) {
            return res
                .status(409)
                .json({
                    success: false,
                    error:
                        'Replay detected.'
                });
        }

        return res
            .status(400)
            .json({
                success: false,
                error:
                    'Invalid or expired encrypted request.'
            });
    }
}

// =====================================================
// MEMBERSHIP API
// =====================================================

app.post(
    '/api/check-group-membership',
    requireEncryptedMembershipRequest,
    async (
        req,
        res
    ) => {
        try {
            const payload =
                req.encryptedPayload;

            const result =
                await checkGroupMembership(
                    payload.groupId,
                    payload.phoneNumber
                );

            return res
                .status(200)
                .json({
                    success:
                        true,
                    ...result
                });

        } catch (error) {
            console.error(
                'Membership API error:',
                error.message
            );

            if (
                error.message ===
                'WhatsApp is not ready.'
            ) {
                return res
                    .status(503)
                    .json({
                        success:
                            false,
                        error:
                            'WhatsApp is not ready.'
                    });
            }

            if (
                error.message ===
                'Target group was not found.'
            ) {
                return res
                    .status(404)
                    .json({
                        success:
                            false,
                        error:
                            'Target group was not found.'
                    });
            }

            return res
                .status(500)
                .json({
                    success:
                        false,
                    error:
                        'Membership check failed.'
                });
        }
    }
);

// =====================================================
// ADMIN ENCRYPTED API
// =====================================================

function requireAdminEncryptedRequest(
    req,
    res,
    next
) {
    const sourceIp =
        getClientIp(req);

    const rate =
        consumeRateLimit(
            adminRateLimitBuckets,
            `admin:${sourceIp}`,
            ADMIN_RATE_LIMIT,
            ADMIN_RATE_WINDOW_MS
        );

    cleanupRateLimitMap(
        adminRateLimitBuckets
    );

    if (
        !rate.allowed
    ) {
        res.setHeader(
            'Retry-After',
            String(
                rate.retryAfterSeconds
            )
        );

        return res
            .status(429)
            .json({
                success:
                    false,
                error:
                    'Too many administration requests.'
            });
    }

    if (
        !req.body ||
        typeof req.body !==
            'object' ||
        Array.isArray(
            req.body
        ) ||
        typeof req.body
            .payload !==
            'string' ||
        Object.keys(
            req.body
        ).length !== 1
    ) {
        return res
            .status(400)
            .json({
                success:
                    false,
                error:
                    'Only encrypted payload is accepted.'
            });
    }

    try {
        const decrypted =
            decryptObject(
                req.body.payload,
                process.env
                    .WHITELIST_ADMIN_KEY
            );

        req.adminPayload =
            validateAdminPayload(
                decrypted
            );

        return next();

    } catch (error) {
        console.error(
            'Admin encrypted request failed:',
            error.message
        );

        if (
            error.message ===
            'Replay detected.'
        ) {
            return res
                .status(409)
                .json({
                    success:
                        false,
                    error:
                        'Replay detected.'
                });
        }

        return res
            .status(401)
            .json({
                success:
                    false,
                error:
                    'Invalid or expired admin request.'
            });
    }
}

// =====================================================
// WHITELIST ADMIN API
// =====================================================

app.post(
    '/api/admin/whitelist',
    requireAdminEncryptedRequest,
    async (
        req,
        res
    ) => {
        try {
            const payload =
                req.adminPayload;

            switch (
                payload.action
            ) {

                case 'list':
                    return res
                        .status(200)
                        .json({
                            success:
                                true,
                            whitelist:
                                await listWhitelist()
                        });

                case 'add-domain':

                    if (
                        !payload.domain
                    ) {
                        return res
                            .status(400)
                            .json({
                                success:
                                    false,
                                error:
                                    'Domain is required.'
                            });
                    }

                    await addWhitelistDomain(
                        payload.domain
                    );

                    console.log(
                        'WHITELIST DOMAIN ADDED:',
                        payload.domain
                    );

                    return res
                        .status(200)
                        .json({
                            success:
                                true,
                            whitelist:
                                await listWhitelist()
                        });

                case 'remove-domain':

                    if (
                        !payload.id
                    ) {
                        return res
                            .status(400)
                            .json({
                                success:
                                    false,
                                error:
                                    'Whitelist entry ID is required.'
                            });
                    }

                    await removeWhitelistDomain(
                        payload.id
                    );

                    return res
                        .status(200)
                        .json({
                            success:
                                true,
                            whitelist:
                                await listWhitelist()
                        });

                default:

                    return res
                        .status(400)
                        .json({
                            success:
                                false,
                            error:
                                'Invalid admin action.'
                        });
            }

        } catch (error) {
            console.error(
                'Whitelist admin error:',
                error.message
            );

            return res
                .status(500)
                .json({
                    success:
                        false,
                    error:
                        'Whitelist administration failed.'
                });
        }
    }
);

// =====================================================
// SOCKET.IO
// =====================================================

io.on(
    'connection',
    socket => {

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
                    type:
                        'warning',
                    message:
                        'Scan the QR code to connect WhatsApp.'
                }
            );
        }

        if (
            whatsappReady
        ) {
            socket.emit(
                'ready',
                {
                    message:
                        'WhatsApp is connected.'
                }
            );

            void sendGroups(
                socket
            );

        } else if (
            !latestQr
        ) {
            socket.emit(
                'status',
                {
                    type:
                        'info',
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
                } catch (
                    error
                ) {
                    socket.emit(
                        'groupListError',
                        error.message
                    );
                }
            }
        );

        /*
         * UI-only membership check.
         *
         * The external website should use the encrypted
         * POST API above.
         */
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

                } catch (
                    error
                ) {
                    console.error(
                        'Socket membership check error:',
                        error.message
                    );

                    socket.emit(
                        'groupMembershipError',
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

async function sendGroups(
    socket
) {
    if (
        !whatsappReady
    ) {
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
                type:
                    'success',
                message:
                    `Loaded ${groups.length} groups.`
            }
        );

    } catch (
        error
    ) {
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

            latestQr =
                url;

            io.emit(
                'qr',
                url
            );

            io.emit(
                'status',
                {
                    type:
                        'warning',
                    message:
                        'Scan the QR code to connect WhatsApp.'
                }
            );

        } catch (
            error
        ) {
            console.error(
                'QR generation failed:',
                error
            );

            io.emit(
                'status',
                {
                    type:
                        'error',
                    message:
                        'Failed to generate QR code.'
                }
            );
        }
    }
);

client.on(
    'loading_screen',
    percent => {
        io.emit(
            'status',
            {
                type:
                    'info',
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

        latestQr =
            null;

        io.emit(
            'status',
            {
                type:
                    'info',
                message:
                    'WhatsApp authenticated. Loading...'
            }
        );
    }
);

client.on(
    'auth_failure',
    error => {

        whatsappReady =
            false;

        latestQr =
            null;

        clearGroupMemberCache();

        console.error(
            'WhatsApp authentication failure:',
            error
        );

        io.emit(
            'status',
            {
                type:
                    'error',
                message:
                    'WhatsApp authentication failed.'
            }
        );
    }
);

client.on(
    'ready',
    async () => {

        whatsappReady =
            true;

        latestQr =
            null;

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
                type:
                    'success',
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
                        sendGroups(
                            socket
                        )
                )
            );

        } catch (
            error
        ) {
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

        whatsappReady =
            false;

        latestQr =
            null;

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
                type:
                    'error',
                message:
                    'WhatsApp disconnected.'
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
// CACHE CLEANUP
// =====================================================

function clearGroupMemberCache() {
    groupMemberCache.clear();

    groupMemberRefreshes.clear();

    console.log(
        'GROUP MEMBER CACHE CLEARED.'
    );
}

setInterval(
    () => {

        cleanupNonceMap(
            usedPayloadNonces
        );

        cleanupNonceMap(
            usedAdminNonces
        );

        cleanupRateLimitMap(
            rateLimitBuckets
        );

        cleanupRateLimitMap(
            adminRateLimitBuckets
        );

    },
    60 * 1000
).unref();

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
// START
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
                'Membership API:',
                '/api/check-group-membership'
            );

            console.log(
                'Whitelist API:',
                '/api/admin/whitelist'
            );
        }
    );

    console.log(
        'Initializing WhatsApp...'
    );

    try {

        await client.initialize();

    } catch (
        error
    ) {

        console.error(
            'WhatsApp initialize error:',
            error
        );

        io.emit(
            'status',
            {
                type:
                    'error',
                message:
                    'WhatsApp initialization failed.'
            }
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