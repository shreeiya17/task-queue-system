const { Redis } = require('ioredis');

// ioredis is the Redis client library BullMQ requires
const redisConfig = {
  maxRetriesPerRequest: null,  // REQUIRED by BullMQ — don't skip
  retryStrategy: (times) => {
    if (times > 10) return null;
    return Math.min(times * 200, 2000);
  },
};

const connection = process.env.REDIS_URL
  ? new Redis(process.env.REDIS_URL, redisConfig) // Production (Railway)
  : new Redis({                                  // Localhost fallback
      host: '127.0.0.1',
      port: 6379,
      ...redisConfig
    });
connection.on('connect', () => console.log('[Redis] connected'));
connection.on('error', (err) => console.error('[Redis] error:', err.message));

module.exports = { connection, redisConfig };
