const JOB_STATUS = {
    WAITING:'waiting', ACTIVE:'active', COMPLETED:'completed',
    FAILED:'failed', DEAD:'dead', DELAYED:'delayed',
};

const PRIORITY = { CRITICAL:1, HIGH:3, NORMAL:5, LOW:8, BULK:10 };
const JOB_TYPES = { EMAIL:'email', REPORT:'report', TEST:'test', WEBHOOK:'webhook' };
const QUEUE_NAMES = { TASKS:'tasks', DLQ:'tasks-dlq' };
const RETRY = { MAX_ATTEMPTS:3, BASE_DELAY_MS:1000, JITTER_FACTOR:0.2 };

module.exports = { JOB_STATUS, PRIORITY, JOB_TYPES, QUEUE_NAMES, RETRY };