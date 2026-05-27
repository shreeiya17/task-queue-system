const { Pool } = require('pg');

// Pool = a group of reusable DB connections (efficient)
const pool = new Pool({
  host: process.env.DB_HOST,
  port: process.env.DB_PORT,
  database: process.env.DB_NAME,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
});

// Create the jobs table (run once)
async function initDB() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS jobs (
      id          VARCHAR(50) PRIMARY KEY,
      type        VARCHAR(50) NOT NULL,
      status      VARCHAR(20) DEFAULT 'waiting',
      priority    INTEGER DEFAULT 0,
      data        JSONB,
      attempts    INTEGER DEFAULT 0,
      max_attempts INTEGER DEFAULT 3,
      error       TEXT,
      result      JSONB,
      created_at  TIMESTAMP DEFAULT NOW(),
      updated_at  TIMESTAMP DEFAULT NOW()
    )
  `);
  console.log('Database initialized');
}

module.exports = { pool, initDB };