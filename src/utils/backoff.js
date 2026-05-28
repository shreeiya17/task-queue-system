function calculateBackoffDelay(attempt, baseDelayMs=1000, jitterFactor=0.2) {
    // Exponential: 1000ms, 2000ms, 4000ms...
    const exponentialDelay = baseDelayMs * Math.pow(2, attempt);
    // Jitter: random ±20%
    const jitter = exponentialDelay * jitterFactor * (Math.random() * 2 - 1);
    // Cap at 30 seconds
    return Math.min(exponentialDelay + jitter, 30000);
}

module.exports = { calculateBackoffDelay };

const { calculateBackoffDelay } = require('../utils/backoff');
async function processEmailJob(job) {
    // On retries: apply jitter before calling the external API
    if (job.attemptsMade > 0) {
        const delay = calculateBackoffDelay(job.attemptsMade - 1);
        console.log(`[Email] Retry ${job.attemptsMade}: waiting ${Math.round(delay)}ms`);
        await new Promise(r => setTimeout(r, delay));
    }
}