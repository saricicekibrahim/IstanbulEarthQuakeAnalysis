//first download parquet files
//aws s3 cp --no-sign-request --recursive s3://fsq-os-places-us-east-1/release/dt=2025-02-06/places/parquet/ ./fsq-data/

const { Client } = require('pg');
const duckdb = require('duckdb');

// DuckDB database
const db = new duckdb.Database('duckdb.db');

// PostGIS connection config
const pgClient = new Client({
    user: '*',
    host: '*',
    database: '*',
    password: '*',
    port: 5432
});

const BATCH_SIZE = 5000;

async function setupPostGISTable() {
    await pgClient.connect();

    // Create PostGIS table if it doesn't exist
    await pgClient.query(`
CREATE TABLE IF NOT EXISTS places_turkey (
    id SERIAL PRIMARY KEY,
    fsq_place_id TEXT,
    name TEXT,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    address TEXT,
    locality TEXT,
    region TEXT,
    postcode TEXT,
    admin_region TEXT,
    post_town TEXT,
    po_box TEXT,
    country TEXT,
    date_created DATE,
    date_refreshed DATE,
    date_closed DATE,
    tel TEXT,
    website TEXT,
    email TEXT,
    facebook_id TEXT,
    instagram TEXT,
    twitter TEXT,
    fsq_category_ids TEXT[],
    fsq_category_labels TEXT[],
    placemaker_url TEXT,
    geom GEOMETRY(Point, 4326)
);

    `);
}

async function insertBatch(rows) {
    const insertPromises = rows.map(row => {
        if (row.latitude && row.longitude) {
            return pgClient.query(`
                INSERT INTO places_turkey (
                    fsq_place_id, name, latitude, longitude, address, locality, region, postcode, 
                    admin_region, post_town, po_box, country, date_created, date_refreshed, date_closed, 
                    tel, website, email, facebook_id, instagram, twitter,
                    fsq_category_ids, fsq_category_labels, placemaker_url, geom
                )
                VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8,
                    $9, $10, $11, $12, $13, $14, $15,
                    $16, $17, $18, $19, $20, $21,
                    $22, $23, $24,
                    ST_SetSRID(ST_MakePoint($4, $3), 4326)
                )
            `, [
                row.fsq_place_id || null,
                row.name || null,
                row.latitude,
                row.longitude,
                row.address || null,
                row.locality || null,
                row.region || null,
                row.postcode || null,
                row.admin_region || null,
                row.post_town || null,
                row.po_box || null,
                row.country || null,
                row.date_created || null,
                row.date_refreshed || null,
                row.date_closed || null,
                row.tel || null,
                row.website || null,
                row.email || null,
                row.facebook_id || null,
                row.instagram || null,
                row.twitter || null,
                row.fsq_category_ids || null,
                row.fsq_category_labels || null,
                row.placemaker_url || null
            ]);
        }
    });

    await Promise.all(insertPromises);
}


async function processData() {
    const con = db.connect();
    await con.run("INSTALL spatial; LOAD spatial;");

    await con.run(`
        CREATE TABLE IF NOT EXISTS places AS 
        SELECT * FROM read_parquet('fsq-data/*.parquet');
    `);

    await con.run(`
        CREATE TABLE IF NOT EXISTS places_turkey AS 
        SELECT * FROM places
        WHERE latitude BETWEEN 35.71056077182571 AND 42.237869676245595
          AND longitude BETWEEN 25.283581252371732 AND 45.21968507382431;
    `);

    console.log("✅ Filtering completed. Starting batch insert...");

    let offset = 0;
    let totalInserted = 0;

    while (true) {
        const batch = await new Promise((resolve, reject) => {
            con.all(`
                SELECT * FROM places_turkey
                LIMIT ${BATCH_SIZE} OFFSET ${offset};
            `, (err, data) => {
                if (err) reject(err);
                else resolve(data);
            });
        });

        if (batch.length === 0) break;

        await insertBatch(batch);
        totalInserted += batch.length;
        console.log(`🔁 Inserted batch. Total so far: ${totalInserted}`);
        offset += BATCH_SIZE;
    }

    console.log(`✅ All done! Inserted ${totalInserted} records.`);
    await pgClient.end();
}

// Run everything
(async () => {
    await setupPostGISTable();
    await processData();
})();