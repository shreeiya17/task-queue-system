const { Pool } = require('pg');

// Pool = a group of reusable DB connections (efficient)
const pool = new Pool({
  host: process.env.DB_HOST,
  port: process.env.DB_PORT,
  database: process.env.DB_NAME,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  max: 10,
  idleTimeoutMillis: 3000,
  connectionTimeoutMillis: 2000
});

// Create the jobs table (run once)
async function initDB() {
  const client = await pool.connect();
  try {
    // use client everywhere — not pool
    await client.query(`
      CREATE TABLE IF NOT EXISTS jobs (
        id           VARCHAR(100) PRIMARY KEY,
        type         VARCHAR(50)  NOT NULL,
        status       VARCHAR(20)  DEFAULT 'waiting'
                     CHECK (status IN ('waiting','active','completed','failed','dead','delayed')),
        priority     INTEGER      DEFAULT 5,
        data         JSONB        NOT NULL,
        attempts     INTEGER      DEFAULT 0,
        max_attempts INTEGER      DEFAULT 3,
        result       JSONB,
        error        TEXT,
        error_stack  TEXT,
        progress     INTEGER      DEFAULT 0,
        created_at   TIMESTAMPTZ  DEFAULT NOW(),
        updated_at   TIMESTAMPTZ  DEFAULT NOW(),
        completed_at TIMESTAMPTZ,
        failed_at    TIMESTAMPTZ
      )
    `);
    await client.query(`
      CREATE INDEX IF NOT EXISTS idx_jobs_status  ON jobs(status);
      CREATE INDEX IF NOT EXISTS idx_jobs_created ON jobs(created_at DESC);
      CREATE INDEX IF NOT EXISTS idx_jobs_type    ON jobs(type);
    `);
    await client.query(`
      CREATE TABLE IF NOT EXISTS dlq_jobs (
        id              SERIAL PRIMARY KEY,
        original_job_id VARCHAR(100),
        type            VARCHAR(50),
        data            JSONB,
        error           TEXT,
        error_stack     TEXT,
        attempts        INTEGER,
        failed_at       TIMESTAMPTZ DEFAULT NOW(),
        replayed        BOOLEAN     DEFAULT FALSE,
        replayed_at     TIMESTAMPTZ
      )
    `);
    console.log('[DB] Tables initialised');
  } finally {
    client.release(); // always release
  }
}

async function query(text, params) {
  const start = Date.now();
  const result = await pool.query(text, params);
  if (Date.now() - start > 100)
    console.warn(`[DB] Slow query (${Date.now()-start}ms): ${text.slice(0,60)}`);
  return result;
}

module.exports = { pool, query, initDB };