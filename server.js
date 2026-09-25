require('dotenv').config();

process.on('uncaughtException', (err) => {
    console.error('[StreamElevate PRO] ⚠️ Error no capturado (uncaughtException):', err?.stack || err);
});
process.on('unhandledRejection', (reason) => {
    console.error('[StreamElevate PRO] ⚠️ Promesa rechazada no controlada (unhandledRejection):', reason?.stack || reason);
});

const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const path = require('path');

const app = express();
const server = http.createServer(app);
const io = new Server(server);

app.set('trust proxy', true);
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));
app.use('/reports', express.static(path.join(__dirname, 'data', 'reports')));

app.get(['/x', '/feed'], (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'x-feed.html'));
});

global.__broadcastFeedPost = (post) => {
    io.emit('new_feed_post', post);
};

const APIFY_TOKEN = process.env.APIFY_TOKEN || '';

// --- API CENTRALIZADA DE TELEMETRÍA (v1) ---
const apiRoutes = require('./src/api-routes');
app.use('/api/v1', apiRoutes);

// --- MOTOR HÍBRIDO EXTRACTOR DE KICK (Compatibilidad) ---
app.get('/api/kick/:username', async (req, res) => {
    const username = req.params.username.trim();
    const targetUrl = `https://kick.com/api/v1/channels/${username}`;

    try {
        const directRes = await fetch(targetUrl, {
            headers: {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
                'Accept': 'application/json'
            }
        });

        if (directRes.ok) {
            const data = await directRes.json();
            if (data && data.chatroom && data.chatroom.id) {
                return res.json({ chatroom_id: data.chatroom.id });
            }
        }
    } catch (e) {}

    try {
        if (!APIFY_TOKEN) throw new Error("Sin token");
        const apifyUrl = `https://api.apify.com/v2/acts/jancurn~url-downloader/run-sync-get-dataset-items?token=${APIFY_TOKEN}`;
        const apifyRes = await fetch(apifyUrl, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                url: targetUrl,
                proxyConfiguration: { useApifyProxy: true }
            })
        });

        if (apifyRes.ok) {
            const dataset = await apifyRes.json();
            if (dataset && dataset.length > 0 && dataset[0].content) {
                const kickData = JSON.parse(dataset[0].content);
                if (kickData && kickData.chatroom && kickData.chatroom.id) {
                    return res.json({ chatroom_id: kickData.chatroom.id });
                }
            }
        }
        throw new Error("Bloqueo Activo");
    } catch (error) {
        res.status(500).json({ error: 'Red protegida. Proceda con el ingreso manual.' });
    }
});

// --- CORE WEBSOCKETS (MULTIPROPÓSITO / OVERLAYS) ---
const roomConfigStates = {}; 

io.on('connection', (socket) => {
    socket.on('joinStreamRoom', (streamId) => {
        socket.join(`stream_${streamId}`);
        if (roomConfigStates[streamId]) {
            if (roomConfigStates[streamId].mode) {
                socket.emit('applyConfig', { type: 'mode', value: roomConfigStates[streamId].mode });
            }
            if (roomConfigStates[streamId].duration) {
                socket.emit('applyConfig', { type: 'duration', value: roomConfigStates[streamId].duration });
            }
        }
    });

    socket.on('configUpdate', (data) => {
        if(data.id) {
            if (!roomConfigStates[data.id]) { roomConfigStates[data.id] = {}; }
            roomConfigStates[data.id][data.type] = data.value;
            io.to(`stream_${data.id}`).emit('applyConfig', data);
        }
    });

    socket.on('customMessage', (data) => {
        if(data.id) { io.to(`stream_${data.id}`).emit('renderCustomMessage', data); }
    });

    socket.on('showSpecificMessage', (data) => {
        if(data.id) { io.to(`stream_${data.id}`).emit('renderSpecificMessage', data); }
    });

    socket.on('clearScreen', (data) => {
        if(data.id) { io.to(`stream_${data.id}`).emit('clearOverlay'); }
    });
});

// --- INICIAR COLECTOR 24/7 ---
const { startCollector } = require('./src/collector');
const { startReportWorker } = require('./src/report-worker');

const PORT = process.env.PORT || 3050;
server.listen(PORT, () => {
    console.log(`====================================================`);
    console.log(`[STREAM ELEVATE PRO] Servidor Activo en Puerto ${PORT}`);
    console.log(`[API TELEMETRÍA] Endpoint: http://localhost:${PORT}/api/v1/health`);
    console.log(`====================================================`);
    startCollector();
    startReportWorker();
});
