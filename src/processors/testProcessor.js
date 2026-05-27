// Fails exactly N times, then succeeds — used to test retry logic
async function processTestJob(job) {
    const { failTimes=2, jobName='test' } = job.data;
    console.log(`[Test] "${jobName}" attempt ${job.attemptsMade+1}`);
    if (job.attemptsMade < failTimes)
        throw new Error(`Simulated failure on attempt ${job.attemptsMade+1}`);
    return { success:true, succeededOnAttempt:job.attemptsMade+1 };
}
module.exports = { processTestJob };