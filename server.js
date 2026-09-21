// FILE: server.js
// Railway persistent-volume WhatsApp session + authenticated admin Socket.IO

const express = require('express');
const http = require('http');
const path = require('path');
const crypto = require('crypto');

const { Server } = require('socket.io');
const { Client, LocalAuth } = require('whatsapp-web.js');
const qrcode = require('qrcode');

const app = express();
const server = http.createServer(app);

const io = new Server(server, {
    maxHttpBufferSize: 64 * 1024
});

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
const API_DOMAIN_HEADER = 'X-API-Domain';
const PAYLOAD_VERSION = 2;

const API_ENCRYPTION_SECRET = String(
    process.env.API_ENCRYPTION_SECRET || ''
);

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

if (API_ENCRYPTION_SECRET.length < 32) {
    throw new Error(
        'API_ENCRYPTION_SECRET is required and must be at least 32 characters.'
    );
}

/*
 * IMPORTANT
 *
 * API_ENCRYPTION_SECRET is also the admin key for the protected
 * Socket.IO admin interface.
 *
 * Never place this secret in a normal public website.
 * The admin page asks for it manually and keeps it only in memory.
 *
 * External websites must generate encrypted payloads on their
 * backend/server, never in public browser JavaScript.
 */

app.disable('x-powered-by');

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

let whatsappReady = false;
let latestQr = null;

const groupMemberCache = new Map();
const groupMemberRefreshes = new Map();
const usedPayloadNonces = new Map();

// =====================================================
// DOMAIN
// =====================================================

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
        domain.includes('://') ||
        domain.includes('/') ||
        domain.includes('\\') ||
        domain.includes(':') ||
        domain.includes('@') ||
        domain.includes(' ')
    ) {
        throw new Error(
            'Domain must be hostname only.'
        );
    }

    if (domain.length > 253) {
        throw new Error(
            'Domain is too long.'
        );
    }

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

function parseDomainWhitelist(value) {
    const rawDomains = String(
        value || ''
    )
        .split(',')
        .map(domain => domain.trim())
        .filter(Boolean);

    const domains = new Set();

    for (const rawDomain of rawDomains) {
        try {
            domains.add(
                normalizeDomain(
                    rawDomain
                )
            );
        } catch (error) {
            throw new Error(
                `Invalid API_DOMAIN_WHITELIST entry "${rawDomain}": ${error.message}`
            );
        }
    }

    return domains;
}

const API_DOMAIN_WHITELIST =
    parseDomainWhitelist(
        process.env.API_DOMAIN_WHITELIST
    );

if (!API_DOMAIN_WHITELIST.size) {
    throw new Error(
        'API_DOMAIN_WHITELIST is required and must contain at least one domain.'
    );
}

console.log(
    'API domain whitelist loaded:',
    API_DOMAIN_WHITELIST.size,
    'domain(s)'
);

function isWhitelistedDomain(domain) {
    return API_DOMAIN_WHITELIST.has(
        normalizeDomain(domain)
    );
}

// =====================================================
// ENCRYPTION
// =====================================================

function deriveAesKey(secret) {
    return crypto
        .createHash('sha256')
        .update(
            String(secret),
            'utf8'
        )
        .digest();
}

function toBase64Url(buffer) {
    return Buffer.from(buffer)
        .toString('base64')
        .replace(/\+/g, '-')
        .replace(/\//g, '_')
        .replace(/=+$/g, '');
}

function fromBase64Url(value) {
    const normalized = String(
        value || ''
    )
        .replace(/-/g, '+')
        .replace(/_/g, '/');

    const remainder =
        normalized.length % 4;

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

/*
 * AES-256-GCM with authenticated additional data (AAD).
 *
 * The domain is NOT stored inside the encrypted JSON anymore.
 * Instead, the normalized X-API-Domain header is authenticated
 * as AAD. This prevents an attacker from taking a valid encrypted
 * payload and changing only the domain header.
 */
function encryptObject(
    object,
    secret,
    associatedData
) {
    const key = deriveAesKey(
        secret
    );

    const aad = Buffer.from(
        String(associatedData),
        'utf8'
    );

    const iv = crypto.randomBytes(
        12
    );

    const cipher = crypto.createCipheriv(
        'aes-256-gcm',
        key,
        iv
    );

    cipher.setAAD(aad);

    const plaintext = Buffer.from(
        JSON.stringify(object),
        'utf8'
    );

    const ciphertext = Buffer.concat([
        cipher.update(plaintext),
        cipher.final()
    ]);

    const authTag =
        cipher.getAuthTag();

    return [
        'v2',
        toBase64Url(iv),
        toBase64Url(authTag),
        toBase64Url(ciphertext)
    ].join('.');
}

function decryptObject(
    encodedPayload,
    secret,
    associatedData
) {
    const value = String(
        encodedPayload || ''
    ).trim();

    if (
        !value ||
        value.length > 30000
    ) {
        throw new Error(
            'Invalid encrypted payload.'
        );
    }

    const parts = value.split('.');

    if (
        parts.length !== 4 ||
        parts[0] !== 'v2'
    ) {
        throw new Error(
            'Invalid encrypted payload.'
        );
    }

    const iv = fromBase64Url(
        parts[1]
    );

    const authTag = fromBase64Url(
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
        const key = deriveAesKey(
            secret
        );

        const aad = Buffer.from(
            String(associatedData),
            'utf8'
        );

        const decipher =
            crypto.createDecipheriv(
                'aes-256-gcm',
                key,
                iv
            );

        decipher.setAAD(aad);
        decipher.setAuthTag(authTag);

        const plaintext = Buffer.concat([
            decipher.update(ciphertext),
            decipher.final()
        ]);

        const parsed = JSON.parse(
            plaintext.toString('utf8')
        );

        if (
            !parsed ||
            typeof parsed !== 'object' ||
            Array.isArray(parsed)
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

function cleanupNonceMap(map) {
    const now = Date.now();

    for (const [
        nonce,
        expiresAt
    ] of map) {
        if (expiresAt <= now) {
            map.delete(nonce);
        }
    }
}

function consumeNonce(
    map,
    nonce
) {
    cleanupNonceMap(map);

    if (
        typeof nonce !== 'string' ||
        !/^[A-Za-z0-9_-]{22,128}$/.test(
            nonce
        )
    ) {
        throw new Error(
            'Invalid payload nonce.'
        );
    }

    if (map.has(nonce)) {
        throw new Error(
            'Replay detected.'
        );
    }

    map.set(
        nonce,
        Date.now() +
            PAYLOAD_NONCE_TTL_MS
    );

    if (map.size > 10000) {
        cleanupNonceMap(map);
    }
}

function validateTimestamp(timestamp) {
    const numeric = Number(
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

    const age = Math.abs(
        Date.now() - numeric
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
    const keys = Object.keys(
        object
    );

    for (const key of keys) {
        if (!allowedKeys.has(key)) {
            throw new Error(
                'Unexpected payload field.'
            );
        }
    }
}

// =====================================================
// PAYLOAD VALIDATION
// =====================================================

function normalizeGroupId(value) {
    const groupId = String(
        value ?? ''
    )
        .normalize('NFKC')
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

function normalizeLocalPhoneNumber(value) {
    const raw = String(
        value ?? ''
    )
        .normalize('NFKC')
        .trim();

    if (!raw) {
        throw new Error(
            'Phone number is required.'
        );
    }

    if (raw.includes('+')) {
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

    let local = raw.replace(
        /\D/g,
        ''
    );

    if (local.startsWith('00')) {
        throw new Error(
            'Phone number must not contain a country code.'
        );
    }

    local = local.replace(
        /^0+/,
        ''
    );

    if (
        !/^\d+$/.test(local) ||
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

    if (international.length > 15) {
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
            'groupId'
        ])
    );

    if (
        Number(payload.version) !==
        PAYLOAD_VERSION
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

    return {
        phoneNumber:
            phone.local,

        internationalPhone:
            phone.international,

        groupId
    };
}

// =====================================================
// WHATSAPP
// =====================================================

/*
 * PERSISTENT WHATSAPP SESSION STORAGE
 *
 * Priority:
 * 1. WWEBJS_AUTH_PATH
 * 2. Railway Volume mount path + /.wwebjs_auth
 * 3. Local fallback inside the project
 */

const railwayVolumeMountPath =
    String(
        process.env
            .RAILWAY_VOLUME_MOUNT_PATH ||
            ''
    ).trim();

const authPath =
    process.env.WWEBJS_AUTH_PATH
        ? path.resolve(
            process.env
                .WWEBJS_AUTH_PATH
        )
        : railwayVolumeMountPath
            ? path.join(
                railwayVolumeMountPath,
                '.wwebjs_auth'
            )
            : path.join(
                __dirname,
                '.wwebjs_auth'
            );

if (
    !process.env.WWEBJS_AUTH_PATH &&
    !railwayVolumeMountPath
) {
    console.warn(
        'WARNING: No Railway Volume detected. WhatsApp session storage is not persistent.'
    );
}

console.log(
    'WhatsApp auth storage path:',
    authPath
);

const client = new Client({
    authStrategy: new LocalAuth({
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

            if (!collections?.Chat) {
                throw new Error(
                    'WhatsApp group collection is unavailable.'
                );
            }

            const unique = new Map();

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
                        !String(id).endsWith(
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
                        String(id),
                        {
                            id: String(id),
                            name: String(
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
                (a, b) =>
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

            const phoneNumbers = [];
            const unresolvedLids = [];

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
                            id.user || ''
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
                            typeof
                                window
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
                            String(error)
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
    if (!whatsappReady) {
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

    const memberPhones = new Set(
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

    const now = Date.now();

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
    if (req.method !== 'POST') {
        return res
            .status(405)
            .json({
                success: false,
                error:
                    'POST only.'
            });
    }

    const rawHeaderDomain =
        req.get(
            API_DOMAIN_HEADER
        );

    if (!rawHeaderDomain) {
        return res
            .status(400)
            .json({
                success: false,
                error:
                    `Missing ${API_DOMAIN_HEADER} header.`
            });
    }

    let requestDomain;

    try {
        requestDomain =
            normalizeDomain(
                rawHeaderDomain
            );
    } catch (_) {
        return res
            .status(400)
            .json({
                success: false,
                error:
                    'Invalid request domain header.'
            });
    }

    /*
     * Domain authorization comes from the HTTP header.
     *
     * The same normalized domain is also used as AES-GCM AAD
     * during decryption, so the header is cryptographically
     * bound to the encrypted payload.
     */
    if (
        !isWhitelistedDomain(
            requestDomain
        )
    ) {
        console.warn(
            'DOMAIN WHITELIST REJECTED:',
            requestDomain
        );

        return res
            .status(403)
            .json({
                success: false,
                error:
                    'Request domain is not whitelisted.'
            });
    }

    /*
     * ONLY ONE BODY FIELD:
     *
     * {
     *     "payload": "v2...."
     * }
     */
    if (
        !req.body ||
        typeof req.body !== 'object' ||
        Array.isArray(req.body) ||
        typeof req.body.payload !==
            'string' ||
        Object.keys(req.body).length !== 1
    ) {
        return res
            .status(400)
            .json({
                success: false,
                error:
                    'Only encrypted payload is accepted.'
            });
    }

    try {
        const decrypted =
            decryptObject(
                req.body.payload,
                API_ENCRYPTION_SECRET,
                requestDomain
            );

        const data =
            validateMembershipPayload(
                decrypted
            );

        req.encryptedPayload = data;
        req.requestDomain =
            requestDomain;

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
                    success: true,
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
// SOCKET.IO ADMIN AUTHENTICATION
// =====================================================

function timingSafeSecretEqual(
    provided
) {
    if (
        typeof provided !== 'string'
    ) {
        return false;
    }

    const providedBuffer =
        Buffer.from(
            provided,
            'utf8'
        );

    const expectedBuffer =
        Buffer.from(
            API_ENCRYPTION_SECRET,
            'utf8'
        );

    if (
        providedBuffer.length !==
        expectedBuffer.length
    ) {
        return false;
    }

    return crypto.timingSafeEqual(
        providedBuffer,
        expectedBuffer
    );
}

function isAdminSocket(socket) {
    return Boolean(
        socket?.data
            ?.adminAuthenticated
    );
}

io.use(
    (socket, next) => {
        const providedKey =
            socket.handshake
                ?.auth
                ?.key;

        if (
            !timingSafeSecretEqual(
                providedKey
            )
        ) {
            return next(
                new Error(
                    'Admin authentication failed.'
                )
            );
        }

        socket.data.adminAuthenticated =
            true;

        return next();
    }
);

function emitToAdminSockets(
    event,
    data
) {
    for (
        const socket of
        io.sockets.sockets.values()
    ) {
        if (
            isAdminSocket(
                socket
            )
        ) {
            socket.emit(
                event,
                data
            );
        }
    }
}

// =====================================================
// SOCKET.IO
// =====================================================

io.on(
    'connection',
    socket => {
        console.log(
            'Authenticated admin connected:',
            socket.id
        );

        socket.emit(
            'adminAuthenticated',
            {
                success: true
            }
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

            void sendGroups(
                socket
            );

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
                if (
                    !isAdminSocket(
                        socket
                    )
                ) {
                    return;
                }

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
         * Admin-only UI membership check.
         *
         * This is protected by the authenticated Socket.IO
         * connection. External websites must use:
         *
         * POST /api/check-group-membership
         *
         * with the encrypted API request.
         */
        socket.on(
            'checkGroupMembership',
            async data => {
                if (
                    !isAdminSocket(
                        socket
                    )
                ) {
                    return;
                }

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
                        'Admin membership check error:',
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
                    'Admin disconnected:',
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
        !isAdminSocket(
            socket
        )
    ) {
        return;
    }

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

            latestQr = url;

            emitToAdminSockets(
                'qr',
                url
            );

            emitToAdminSockets(
                'status',
                {
                    type: 'warning',
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

            emitToAdminSockets(
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
    percent => {
        emitToAdminSockets(
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

        emitToAdminSockets(
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

        emitToAdminSockets(
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

        emitToAdminSockets(
            'ready',
            {
                message:
                    'WhatsApp is connected.'
            }
        );

        emitToAdminSockets(
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
                )
                    .filter(
                        isAdminSocket
                    )
                    .map(
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
        whatsappReady = false;
        latestQr = null;

        clearGroupMemberCache();

        console.log(
            'WhatsApp disconnected:',
            reason
        );

        emitToAdminSockets(
            'whatsappDisconnected',
            String(
                reason ||
                'Disconnected'
            )
        );

        emitToAdminSockets(
            'status',
            {
                type: 'error',
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

        emitToAdminSockets(
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
                'Admin Socket.IO authentication: enabled'
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

        emitToAdminSockets(
            'status',
            {
                type: 'error',
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