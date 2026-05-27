const { Redis } = require('ioredis');

// ioredis is the Redis client library BullMQ requires
const connection = new Redis({
  host: process.env.REDIS_HOST || 'localhost',
  port: parseInt(process.env.REDIS_PORT) || 6379,
  maxRetriesPerRequest: null,  // REQUIRED by BullMQ — don't skip
});

connection.on('connect', () => console.log('Redis connected'));
connection.on('error', (err) => console.error('Redis error:', err));

module.exports = { connection };
