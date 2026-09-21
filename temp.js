const express = require('express');
const mysql = require('mysql2/promise');

const app = express();

const PORT =
    Number(
        process.env.PORT || 3000
    );

const TABLES = [
    'api_domain_whitelist',
    'api_ip_whitelist',
    'chat_messages',
    'recent_chats'
];

let cleanupResult = {
    finished: false,
    success: false,
    results: []
};


// =====================================================
// DATABASE
// =====================================================

function createDatabaseConfig() {

    const connectionUrl =
        process.env.MYSQL_URL ||
        process.env.DATABASE_URL;

    if (connectionUrl) {

        const url =
            new URL(
                connectionUrl
            );

        return {
            host:
                url.hostname,

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
                )
        };
    }

    return {

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
            process.env.DB_NAME
    };
}


// =====================================================
// DELETE TABLES
// =====================================================

async function deleteTables() {

    let db;

    try {

        db =
            await mysql.createConnection(
                createDatabaseConfig()
            );

        console.log(
            'MySQL connected.'
        );

        for (
            const table of TABLES
        ) {

            try {

                /*
                 * Backticks are used around the fixed table
                 * names for safe SQL identifier handling.
                 */
                await db.query(
                    `DROP TABLE IF EXISTS \`${table}\``
                );

                console.log(
                    `DELETED: ${table}`
                );

                cleanupResult.results.push({
                    table,
                    success: true,
                    message:
                        'Table deleted or did not exist.'
                });

            } catch (error) {

                console.error(
                    `FAILED: ${table}`,
                    error.message
                );

                cleanupResult.results.push({
                    table,
                    success: false,
                    message:
                        error.message
                });

            }
        }

        cleanupResult.finished =
            true;

        cleanupResult.success =
            cleanupResult.results.every(
                item =>
                    item.success
            );

        console.log(
            '================================'
        );

        console.log(
            'DATABASE CLEANUP FINISHED'
        );

        console.log(
            JSON.stringify(
                cleanupResult,
                null,
                2
            )
        );

    } catch (error) {

        console.error(
            'DATABASE CONNECTION ERROR:',
            error.message
        );

        cleanupResult.finished =
            true;

        cleanupResult.success =
            false;

        cleanupResult.results.push({
            table: null,
            success: false,
            message:
                error.message
        });

    } finally {

        if (db) {
            await db.end();
        }

    }
}


// =====================================================
// STATUS PAGE
// =====================================================

app.get(
    '/',
    (req, res) => {

        res.setHeader(
            'Content-Type',
            'text/html; charset=utf-8'
        );

        const rows =
            cleanupResult.results
                .map(
                    item => `
                        <tr>
                            <td>
                                ${item.table || 'DATABASE'}
                            </td>

                            <td>
                                ${
                                    item.success
                                        ? 'SUCCESS'
                                        : 'FAILED'
                                }
                            </td>

                            <td>
                                ${item.message}
                            </td>
                        </tr>
                    `
                )
                .join('');

        res.send(`
            <!DOCTYPE html>

            <html>

            <head>

                <meta
                    charset="UTF-8"
                >

                <meta
                    name="viewport"
                    content="width=device-width, initial-scale=1"
                >

                <title>
                    Database Cleanup
                </title>

                <style>

                    body {
                        font-family:
                            Arial,
                            sans-serif;

                        background:
                            #f5f5f5;

                        padding:
                            30px;
                    }

                    .box {
                        max-width:
                            900px;

                        margin:
                            auto;

                        background:
                            white;

                        padding:
                            25px;

                        border-radius:
                            12px;

                        box-shadow:
                            0 4px 20px
                            rgba(
                                0,
                                0,
                                0,
                                0.08
                            );
                    }

                    table {
                        width:
                            100%;

                        border-collapse:
                            collapse;

                        margin-top:
                            20px;
                    }

                    th,
                    td {
                        text-align:
                            left;

                        padding:
                            12px;

                        border-bottom:
                            1px solid #ddd;
                    }

                    .success {
                        color:
                            green;

                        font-weight:
                            bold;
                    }

                    .failed {
                        color:
                            red;

                        font-weight:
                            bold;
                    }

                </style>

            </head>

            <body>

                <div class="box">

                    <h1>
                        Database Cleanup
                    </h1>

                    <p>
                        Status:
                        ${
                            cleanupResult.finished
                                ? cleanupResult.success
                                    ? '<span class="success">Completed</span>'
                                    : '<span class="failed">Completed with errors</span>'
                                : 'Running...'
                        }
                    </p>

                    <table>

                        <thead>

                            <tr>
                                <th>Table</th>
                                <th>Status</th>
                                <th>Message</th>
                            </tr>

                        </thead>

                        <tbody>

                            ${rows}

                        </tbody>

                    </table>

                    ${
                        cleanupResult.finished
                            ? `
                                <p>
                                    You can now restore
                                    <code>npm start</code>
                                    back to
                                    <code>node server.js</code>.
                                </p>
                            `
                            : `
                                <p>
                                    Cleanup is still running.
                                    Refresh this page.
                                </p>
                            `
                    }

                </div>

            </body>

            </html>
        `);
    }
);


// =====================================================
// START
// =====================================================

async function start() {

    /*
     * Delete the tables FIRST.
     */
    await deleteTables();

    /*
     * Keep Railway process alive so you can
     * open the deployment URL and see the result.
     */
    app.listen(
        PORT,
        () => {

            console.log(
                `Cleanup server running on port ${PORT}`
            );

            console.log(
                `Open your Railway URL to see cleanup status.`
            );

        }
    );
}

start().catch(
    error => {

        console.error(
            'Fatal error:',
            error
        );

        process.exit(1);
    }
);