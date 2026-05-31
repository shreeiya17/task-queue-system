const { Redis } = require('ioredis');

const baseOptions = {
  maxRetriesPerRequest: null,   // REQUIRED by BullMQ — do not remove
  retryStrategy: (times) => {
    if (times > 10) return null;
    return Math.min(times * 200, 2000);
  },
};

// Single ioredis instance — works for both local and Railway
const connection = process.env.REDIS_URL
  ? new Redis(process.env.REDIS_URL, baseOptions)   // Railway injects this automatically
  : new Redis({ host: '127.0.0.1', port: 6379, ...baseOptions }); // local dev

connection.on('connect', () => console.log('[Redis] connected'));
connection.on('error',   (err) => console.error('[Redis] error:', err.message));

// redisConfig is an alias for connection.
// BullMQ accepts an ioredis instance as its `connection` option,
// so workers and queues that do { connection: redisConfig } will work correctly
// in both local and production environments without any other changes.
const redisConfig = connection;

module.exports = { connection, redisConfig };