const { Queue } = require('bullmq');
const { redisConfig } = require('../config/redis');
const { QUEUE_NAMES, RETRY, PRIORITY } = require('../config/constants');

// A Queue is just a named channel in Redis
// All producers and workers use the same name to communicate
const taskQueue = new Queue(QUEUE_NAMES.TASKS, {
    connection: redisConfig,
    defaultJobOptions: {
        attempts: RETRY.MAX_ATTEMPTS,             
        backoff: { type:'exponential', delay:RETRY.BASE_DELAY_MS },
        removeOnComplete: { count: 500, age: 86400 },  
        RemoveOnFail: { count: 1000 },      
    },
});

const dlqQueue = new Queue(QUEUE_NAMES.DLQ, { connection:redisConfig });
async function enqueueJob(type, data, options={}) {
    const job = await taskQueue.add(type, data, {
        priority:  options.priority || PRIORITY.NORMAL,
        delay:     options.delayMs  || 0,
        jobId:     options.idempotencyKey,
    });
    return job.id;
}

async function getQueueStats() {
    const [waiting,active,completed,failed,delayed] = await Promise.all([
        taskQueue.getWaitingCount(),   taskQueue.getActiveCount(),
        taskQueue.getCompletedCount(), taskQueue.getFailedCount(),
        taskQueue.getDelayedCount(),
    ]);
    return { waiting, active, completed, failed, delayed, dlq: await dlqQueue.getWaitingCount() };
}

module.exports = { taskQueue, dlqQueue, enqueueJob, getQueueStats };