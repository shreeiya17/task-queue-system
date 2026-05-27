// src/workers/worker.js  — run: npm run worker

require('dotenv').config();
const { Worker } = require('bullmq');
const { redisConfig }       = require('../config/redis');
const { query }              = require('../config/db');
const { dlqQueue }           = require('../queues/taskQueue');
const { processEmailJob }    = require('../processors/emailProcessor');
const { processReportJob }   = require('../processors/reportProcessor');
const { processTestJob }     = require('../processors/testProcessor');
const { JOB_STATUS, QUEUE_NAMES } = require('../config/constants');

// Main router — called for every job
async function mainProcessor(job) {
    console.log(`\n[Worker] Job ${job.id} | type:${job.name} | attempt:${job.attemptsMade+1}`);
    await query(
        'UPDATE jobs SET status=$1, attempts=$2, updated_at=NOW() WHERE id=$3',
        [JOB_STATUS.ACTIVE, job.attemptsMade+1, job.id]

    );
    switch (job.name) {
        case 'email':  return await processEmailJob(job);
        case 'report': return await processReportJob(job);
        default: throw Object.assign(new Error(`Unknown type: ${job.name}`), {failParent:true});
    }
}

const worker = new Worker(QUEUE_NAMES.TASKS, mainProcessor, {
    connection:  redisConfig,
    concurrency: 5,    // process up to 5 jobs simultaneously

});

worker.on('completed', async (job, result) => {
    console.log(`[Worker] ✓ ${job.id} completed`);
    await query(
        'UPDATE jobs SET status=$1,result=$2,completed_at=NOW(),updated_at=NOW() WHERE id=$3',
        [JOB_STATUS.COMPLETED, JSON.stringify(result), job.id]
    );
    if (global.io) global.io.emit('job:completed', {id:job.id,status:'completed',result});
});

worker.on('failed', async (job, error) => {
    const isDead = job.attemptsMade >= job.opts.attempts;
    const status = isDead ? JOB_STATUS.DEAD : JOB_STATUS.FAILED;
    console.error(`[Worker] ✗ ${job.id} failed (attempt ${job.attemptsMade}): ${error.message}`);
    await query(
        'UPDATE jobs SET status=$1,error=$2,error_stack=$3,updated_at=NOW() WHERE id=$4',
        [status, error.message, error.stack, job.id]
    );
    if (isDead) await moveToDLQ(job, error);
    if (global.io) global.io.emit('job:failed', {id:job.id,status,error:error.message,isDead});
});

worker.on('progress', async (job, progress) => {
    await query('UPDATE jobs SET progress=$1,updated_at=NOW() WHERE id=$2', [progress, job.id]);
    if (global.io) global.io.emit('job:progress', {id:job.id,progress});
});

worker.on('stalled', (jobId) => {
    // Worker crashed mid-job — BullMQ auto-requeues after lock TTL
    console.warn(`[Worker] Job ${jobId} stalled — will auto-requeue`);
});

async function moveToDLQ(job, error) {
    await dlqQueue.add('dead', {
        originalJobId:job.id, originalType:job.name,
        originalData:job.data, error:error.message,
        errorStack:error.stack, attempts:job.attemptsMade,
        failedAt:new Date().toISOString(),
    });
    await query(
        'UPDATE jobs SET status=$1,failed_at=NOW(),updated_at=NOW() WHERE id=$2',
        [JOB_STATUS.DEAD, job.id]
    );
    await query(
        'INSERT INTO dlq_jobs (original_job_id,type,data,error,error_stack,attempts) VALUES($1,$2,$3,$4,$5,$6)',
        [job.id,job.name,JSON.stringify(job.data),error.message,error.stack,job.attemptsMade]
    );
    console.log(`[DLQ] Moved job ${job.id} to Dead Letter Queue`);
}
process.on('SIGTERM', async () => { await worker.close(); process.exit(0); });
console.log('[Worker] Started — concurrency:5');