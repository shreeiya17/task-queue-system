const { Redis } = require('ioredis');

// ioredis is the Redis client library BullMQ requires
const redisConfig = {
  host: process.env.REDIS_HOST || 'localhost',
  port: parseInt(process.env.REDIS_PORT) || 6379,
  maxRetriesPerRequest: null,  // REQUIRED by BullMQ — don't skip
  retryStrategy: (times) => {
    if (times > 10) return null;
    return Math.min(times * 200, 2000);
  },
};

const connection = new Redis(process.env.REDIS_URL || 'redis://127.0.0.1:6379', redisConfig);
connection.on('connect', () => console.log('[Redis] connected'));
connection.on('error', (err) => console.error('[Redis] error:', err.message));

module.exports = { connection, redisConfig };
