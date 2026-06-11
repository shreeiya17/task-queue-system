require('dotenv').config();
const express    = require('express');
const cors       = require('cors');
const http       = require('http');
const { Server } = require('socket.io');
const { initDB, query } = require('./config/db');
const jobRoutes  = require('./routes/jobs');

const app        = express();
const httpServer = http.createServer(app);

// ── CORS ──────────────────────────────────────────────────────────
// Allows localhost in dev and ANY *.vercel.app URL in production.
// This is needed because Vercel generates a unique preview URL for
// every deployment — a hardcoded single URL will block all of them.
const ALLOWED_ORIGINS = (process.env.ALLOWED_ORIGINS || '').split(',').filter(Boolean);

const allowedOrigin = (origin, callback) => {
  if (
    !origin ||
    origin === 'http://localhost:5173' ||
    origin === 'http://localhost:3000' ||
    origin.endsWith('.vercel.app') ||
    ALLOWED_ORIGINS.includes(origin)
  ) {
    callback(null, true);
  } else {
    callback(new Error(`CORS blocked: ${origin}`));
  }
};

const io = new Server(httpServer, {
  cors: { origin: allowedOrigin, methods: ['GET', 'POST'] }
});

global.io = io;
app.set('io', io);

app.use(cors({ origin: allowedOrigin }));

app.use(express.json({ limit: '1mb' }));
app.use((req, res, next) => {
  const t = Date.now();
  res.on('finish', () =>
    console.log(`[${req.method}] ${req.path} ${res.statusCode} (${Date.now()-t}ms)`)
  );
  next();
});

app.use('/api/jobs', jobRoutes);
app.get('/health', (_, res) => res.json({ status: 'ok', ts: new Date().toISOString() }));
app.use((err, req, res, next) => res.status(500).json({ error: err.message }));

io.on('connection', socket => {
  console.log('[Socket.io] Dashboard connected:', socket.id);
  socket.on('disconnect', () =>
    console.log('[Socket.io] Disconnected:', socket.id)
  );
});

// ── Broadcast loop ────────────────────────────────────────────────
// worker.js runs in a separate process and cannot access global.io.
// server.js polls PostgreSQL every second and broadcasts everything.

let lastJobUpdates = new Map();

setInterval(async () => {
  if (io.sockets.sockets.size === 0) return;

  try {
    // 1. Broadcast live stats from PostgreSQL (single source of truth)
    const { rows: s } = await query(`
      SELECT
        COUNT(*) FILTER (WHERE status = 'waiting')   AS waiting,
        COUNT(*) FILTER (WHERE status = 'active')    AS active,
        COUNT(*) FILTER (WHERE status = 'completed') AS completed,
        COUNT(*) FILTER (WHERE status = 'failed')    AS failed,
        COUNT(*) FILTER (WHERE status = 'delayed')   AS delayed,
        COUNT(*) FILTER (WHERE status = 'dead')      AS dlq
      FROM jobs
    `);
    io.emit('queue:stats', {
      waiting:   parseInt(s[0].waiting),
      active:    parseInt(s[0].active),
      completed: parseInt(s[0].completed),
      failed:    parseInt(s[0].failed),
      delayed:   parseInt(s[0].delayed),
      dlq:       parseInt(s[0].dlq),
    });

    // 2. Broadcast jobs updated in last 3 seconds
    const { rows: recent } = await query(`
      SELECT * FROM jobs
      WHERE updated_at > NOW() - INTERVAL '3 seconds'
      ORDER BY updated_at DESC
      LIMIT 20
    `);

    for (const job of recent) {
      const key  = job.id;
      const prev = lastJobUpdates.get(key);

      if (prev === job.status) continue;
      lastJobUpdates.set(key, job.status);

      if (job.status === 'completed') {
        io.emit('job:completed', job);
      } else if (job.status === 'active') {
        io.emit('job:progress', { id: job.id, progress: job.progress || 0, type: job.type });
      } else if (job.status === 'failed' || job.status === 'dead') {
        io.emit('job:failed', {
          id:           job.id,
          type:         job.type,
          status:       job.status,
          isDead:       job.status === 'dead',
          error:        job.error,
          attempts:     job.attempts,
          max_attempts: job.max_attempts,
        });
      }
    }

    if (lastJobUpdates.size > 500) lastJobUpdates.clear();

  } catch (err) {
    console.error('[Server] Broadcast error:', err.message);
  }
}, 1000);

async function start() {
  await initDB();
  const PORT = process.env.PORT || 3000;
  httpServer.listen(PORT, () => {
    console.log(`[Server] http://localhost:${PORT}`);
    console.log('[Server] Start worker: npm run worker');
  });
}

start();