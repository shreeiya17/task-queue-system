require('dotenv').config();
const express = require('express');
const cors = require('cors');
const http       = require('http');
const { Server } = require('socket.io');
const { initDB } = require('./config/db');
const jobRoutes = require('./routes/jobs');

const app = express();
const httpServer = http.createServer(app);
const io         = new Server(httpServer, {
    cors: { origin:['http://localhost:5173','http://localhost:3001'], methods:['GET','POST'] }
});
app.set('io', io);
global.io = io;   // worker.js accesses via global
app.use(cors());
app.use(express.json({ limit:'1mb' }));

app.use((req,res,next) => {
    const t = Date.now();
    res.on('finish', ()=>console.log(`[${req.method}] ${req.path} ${res.statusCode} (${Date.now()-t}ms)`));
    next();
});

app.use('/api/jobs', jobRoutes);

app.get('/health', (_,res) => res.json({status:'ok', ts:new Date().toISOString()}));

app.use((err,req,res,next) => res.status(500).json({error:err.message}));
io.on('connection', s => {
    console.log('[Socket.io] Dashboard connected:', s.id);
    s.on('disconnect', () => console.log('[Socket.io] Disconnected:', s.id));
});

async function start() {
  await initDB();
  const PORT = process.env.PORT || 3000;
  httpServer.listen(PORT, () => {
    console.log(`[Server] http://localhost:${PORT}`);
    console.log('[Server] Start worker: npm run worker');
  });
}

start();