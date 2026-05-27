const express = require('express');
const router  = express.Router();
const crypto  = require('crypto');
const { enqueueJob, getQueueStats, taskQueue } = require('../queues/taskQueue');
const { query } = require('../config/db');
const { PRIORITY, JOB_TYPES } = require('../config/constants');

// ── POST /api/jobs — add a new job ──────────────────────────────
router.post('/', async (req, res) => {
  try {
    const { type, data, priority, delayMs, idempotencyKey } = req.body;

    if (!Object.values(JOB_TYPES).includes(type))
      return res.status(400).json({ error: `Invalid type. Use: ${Object.values(JOB_TYPES).join(', ')}` });

    if (!data || typeof data !== 'object')
      return res.status(400).json({ error: 'data must be a non-empty object' });

    const key = idempotencyKey ||
      crypto.createHash('md5').update(JSON.stringify({ type, data })).digest('hex');

    const jobId = await enqueueJob(type, data, {
      priority: priority || PRIORITY.NORMAL,
      delayMs:  delayMs  || 0,
      idempotencyKey: key,
    });

    await query(
      `INSERT INTO jobs (id, type, status, priority, data)
       VALUES ($1, $2, 'waiting', $3, $4)
       ON CONFLICT (id) DO NOTHING`,
      [jobId, type, priority || PRIORITY.NORMAL, JSON.stringify(data)]
    );

    res.status(201).json({ success: true, jobId });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── GET /api/jobs/stats — MUST be before /:id ───────────────────
// Why: Express matches top to bottom. If /:id comes first,
// it treats "stats" as an id and this route never runs.
router.get('/stats', async (_, res) => {
  try {
    res.json(await getQueueStats());
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── POST /api/jobs/dlq/:id/replay — also before /:id ───────────
router.post('/dlq/:id/replay', async (req, res) => {
  try {
    const { rows } = await query('SELECT * FROM dlq_jobs WHERE id = $1', [req.params.id]);
    if (!rows.length) return res.status(404).json({ error: 'DLQ job not found' });

    const dlqJob = rows[0];
    const newId  = await enqueueJob(dlqJob.type, dlqJob.data, { priority: PRIORITY.HIGH });

    await query(
      'UPDATE dlq_jobs SET replayed = true, replayed_at = NOW() WHERE id = $1',
      [req.params.id]
    );
    res.json({ success: true, newJobId: newId });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── GET /api/jobs — list with filter ───────────────────────────
router.get('/', async (req, res) => {
  try {
    const { status, type, page = 1, limit = 20 } = req.query;
    const params = [];
    let where = 'WHERE 1=1';

    if (status) { params.push(status); where += ` AND status = $${params.length}`; }
    if (type)   { params.push(type);   where += ` AND type = $${params.length}`; }

    params.push(parseInt(limit), (parseInt(page) - 1) * parseInt(limit));

    const { rows } = await query(
      `SELECT * FROM jobs ${where} ORDER BY created_at DESC
       LIMIT $${params.length - 1} OFFSET $${params.length}`,
      params
    );
    res.json({ jobs: rows, page: parseInt(page), limit: parseInt(limit) });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── GET /api/jobs/:id — get one job ────────────────────────────
// Dynamic route LAST — so it doesn't swallow /stats
router.get('/:id', async (req, res) => {
  try {
    const { rows } = await query('SELECT * FROM jobs WHERE id = $1', [req.params.id]);
    if (!rows.length) return res.status(404).json({ error: 'Not found' });
    res.json(rows[0]);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── DELETE /api/jobs/:id — cancel a job ────────────────────────
router.delete('/:id', async (req, res) => {
  try {
    const job = await taskQueue.getJob(req.params.id);
    if (job) await job.remove();
    await query(
      "UPDATE jobs SET status = 'cancelled', updated_at = NOW() WHERE id = $1",
      [req.params.id]
    );
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

module.exports = router;