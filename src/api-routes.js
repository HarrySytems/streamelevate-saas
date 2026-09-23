const express = require('express');
const router = express.Router();
const { db, stmts, getStreamDetails } = require('./db');
const { getTelemetrySnapshot, memoryState } = require('./collector');

// GET /api/v1/health
router.get('/health', (req, res) => {
  const activeStreamsCount = memoryState.activeStreams.size;
  const subscribedPusherCount = memoryState.pusherSubscribed.size;
  const channelCount = db.prepare(`SELECT COUNT(*) as c FROM channels`).get().c;
  const totalMessages = db.prepare(`SELECT COUNT(*) as c FROM chat_messages`).get().c;
  
  res.json({
    status: 'online',
    service: 'StreamElevate Core Ingestion & Telemetry API',
    uptime_seconds: process.uptime(),
    channels_tracked: channelCount,
    streams_live: activeStreamsCount,
    pusher_chatrooms_active: subscribedPusherCount,
    total_messages_recorded: totalMessages,
    timestamp: new Date().toISOString()
  });
});

// GET /api/v1/channels - Todos los canales con su estado actual
router.get('/channels', (req, res) => {
  const channels = stmts.getAllChannels.all();
  res.json({
    count: channels.length,
    channels: channels.map(ch => ({
      slug: ch.slug,
      platform: ch.platform,
      username: ch.username,
      channel_id: ch.channel_id,
      chatroom_id: ch.chatroom_id,
      is_live: Boolean(ch.is_live),
      viewers: ch.current_viewers,
      category: ch.current_category,
      title: ch.current_title,
      last_checked: ch.last_checked_at ? new Date(ch.last_checked_at).toISOString() : null
    }))
  });
});

// GET /api/v1/streamers/:slug/live - Telemetría en tiempo real a 1ms
router.get('/streamers/:slug/live', (req, res) => {
  const slug = req.params.slug.toLowerCase();
  const platform = req.query.platform || 'kick';
  const snapshot = getTelemetrySnapshot(slug, platform);
  res.json(snapshot);
});

// GET /api/v1/streamers/:slug/session/:sessionId - Sesión consolidada para RadarStream
router.get('/streamers/:slug/session/:sessionId', (req, res) => {
  const sessionId = req.params.sessionId;
  const details = getStreamDetails(sessionId);
  if (!details) {
    return res.status(404).json({ error: 'Sesión de stream no encontrada' });
  }
  res.json(details);
});

// GET /api/v1/streamers/:slug/recent - Últimos directos
router.get('/streamers/:slug/recent', (req, res) => {
  const slug = req.params.slug.toLowerCase();
  const streams = stmts.getRecentStreams.all(slug);
  res.json({
    slug,
    streams: streams.map(s => ({
      id: s.id,
      platform: s.platform,
      title: s.title,
      category: s.category,
      started_at: new Date(s.started_at).toISOString(),
      ended_at: s.ended_at ? new Date(s.ended_at).toISOString() : null,
      peak_viewers: s.peak_viewers,
      avg_viewers: s.avg_viewers,
      total_messages: s.total_messages,
      unique_chatters: s.unique_chatters,
      status: s.status
    }))
  });
});

module.exports = router;
