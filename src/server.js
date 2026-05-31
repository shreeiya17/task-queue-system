require('dotenv').config();
const express    = require('express');
const cors       = require('cors');
const http       = require('http');
const { Server } = require('socket.io');
const { initDB, query } = require('./config/db');
const jobRoutes  = require('./routes/jobs');

const app        = express();
const httpServer = http.createServer(app);

// ── THE CORS FIX FOR SOCKET.IO ────────────────────────────────────
const io         = new Server(httpServer, {
  cors: { 
    origin: ['http://localhost:5173', 'https://task-queue-frontend.vercel.app'], 
    methods: ['GET','POST'] 
  }
});

global.io = io;
app.set('io', io);

// ── THE CORS FIX FOR EXPRESS HTTP API ──────────────────────────────
app.use(cors({
  origin: ['http://localhost:5173', 'https://task-queue-frontend.vercel.app']
}));

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

// ── THE FIX: all broadcasting lives here in server.js ─────────────
// worker.js runs in a separate process and cannot access global.io.
// server.js polls PostgreSQL every second and broadcasts everything.

let lastJobUpdates = new Map(); // track what we already broadcast

setInterval(async () => {
  if (io.sockets.sockets.size === 0) return;

  try {
    // 1. Broadcast live stats from PostgreSQL (accurate count)
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

      // only emit if status changed since last broadcast
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

    // Clean up old entries (keep map small)
    if (lastJobUpdates.size > 500) lastJobUpdates.clear();

  } catch (err) {
    console.error('[Server] Broadcast error:', err.message);
  }
}, 1000); // every 1 second — fast enough to catch job transitions

async function start() {
  await initDB();
  const PORT = process.env.PORT || 3000;
  httpServer.listen(PORT, () => {
    console.log(`[Server] http://localhost:${PORT}`);
    console.log('[Server] Start worker: npm run worker');
  });
}

start();