require('dotenv').config();
const { Worker } = require('bullmq');
const { redisConfig }  = require('./config/redis');
const { enqueueJob, taskQueue } = require('./queues/taskQueue');
const { query, initDB }  = require('./config/db');
const { QUEUE_NAMES }  = require('./config/constants');

const JOB_COUNT    = 200;
const CONCURRENCY_LEVELS = [1, 5, 10, 20, 50];

async function runBenchmark(concurrency) {
    await taskQueue.drain();
    await query("DELETE FROM jobs");
    for (let i=0; i<JOB_COUNT; i++)
        await enqueueJob('email',{to:`u${i}@test.com`,subject:`Test${i}`});

    const worker = new Worker(QUEUE_NAMES.TASKS,
        async () => { await new Promise(r=>setTimeout(r,50)); return {done:true}; },
        { connection:redisConfig, concurrency }
    );

    const start = Date.now();
    let done = 0;
    await new Promise(resolve=>{
        worker.on('completed', ()=>{ if(++done>=JOB_COUNT) resolve(); });
    });

    const secs = (Date.now()-start)/1000;
    await worker.close();
    return { concurrency, duration:secs.toFixed(2), throughput:(JOB_COUNT/secs).toFixed(1) };
}

async function main() {
    await initDB();
    console.log('\n=== Concurrency Benchmark ===');
    const results = [];
    for (const c of CONCURRENCY_LEVELS) {
        process.stdout.write(`concurrency=${c}... `);
        const r = await runBenchmark(c);
        results.push(r);
        console.log(`${r.throughput} jobs/sec (${r.duration}s)`);
    }
    const maxTP = Math.max(...results.map(r=>parseFloat(r.throughput)));
    console.log('\nConcurrency | Duration  | Throughput');
    console.log('------------|-----------|----------');
    results.forEach(r=>{
        const opt = parseFloat(r.throughput)===maxTP;
        console.log(`${String(r.concurrency).padEnd(12)}|${(r.duration+'s').padEnd(11)}| ${r.throughput} jobs/sec${opt?' ← OPTIMAL':''}`);
    });
    process.exit(0);
}
main().catch(console.error);