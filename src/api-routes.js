const express = require('express');
const router = express.Router();
const { db, stmts, getStreamDetails, getSessionChart } = require('./db');
const { getTelemetrySnapshot, memoryState, twitchSubscribed } = require('./collector');

// GET /api/v1/health
router.get('/health', (req, res) => {
  const activeStreamsCount = memoryState.activeStreams.size;
  const subscribedPusherCount = memoryState.pusherSubscribed.size;
  const subscribedTwitchCount = twitchSubscribed ? twitchSubscribed.size : 0;
  const channelCount = db.prepare(`SELECT COUNT(*) as c FROM channels`).get().c;
  const totalMessages = db.prepare(`SELECT COUNT(*) as c FROM chat_messages`).get().c;
  
  res.json({
    status: 'online',
    service: 'StreamElevate Core Ingestion & Telemetry API',
    uptime_seconds: process.uptime(),
    channels_tracked: channelCount,
    streams_live: activeStreamsCount,
    pusher_chatrooms_active: subscribedPusherCount,
    twitch_irc_active: subscribedTwitchCount,
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

// GET /api/v1/streamers/:slug/live - Telemetría en tiempo real
router.get('/streamers/:slug/live', (req, res) => {
  const slug = req.params.slug.toLowerCase();
  const platform = req.query.platform || 'kick';
  const snapshot = getTelemetrySnapshot(slug, platform);
  res.json(snapshot);
});

// GET /api/v1/streamers/:slug/session/:sessionId - Sesión completa desde SQLite
router.get('/streamers/:slug/session/:sessionId', (req, res) => {
  const sessionId = req.params.sessionId;

  // getSessionChart siempre lee de SQLite — no está limitado por el buffer de RAM
  const chart = getSessionChart(sessionId);
  if (!chart || chart.totalSamples === 0) {
    // Fallback a getStreamDetails si no hay muestras en audience_samples (sesión muy nueva)
    const details = getStreamDetails(sessionId);
    if (!details) {
      return res.status(404).json({ error: 'Sesión de stream no encontrada' });
    }
    return res.json(details);
  }

  // Enriquecer con metadatos de la sesión
  const stream = db.prepare(`SELECT * FROM streams WHERE id = ?`).get(sessionId);
  const chat = stmts.getChatStats.get(sessionId);
  const topChatters = stmts.getTopChatters.all(sessionId);
  const chatTimeline = stmts.getChatPerMinute.all(sessionId);

  res.json({
    stream,
    samples: chart.samples,
    gaps: chart.gaps,
    total_samples_recorded: chart.totalSamples,
    first_observed_at: chart.firstObservedAt,
    last_observed_at: chart.lastObservedAt,
    // coverage_insufficient: true cuando avg_viewers es null (no se pudo calcular media)
    coverage_insufficient: stream ? stream.avg_viewers === null : false,
    chat,
    topChatters,
    chatTimeline
  });
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
      coverage_ratio: s.coverage_ratio,
      coverage_insufficient: s.avg_viewers === null,
      total_messages: s.total_messages,
      unique_chatters: s.unique_chatters,
      status: s.status
    }))
  });
});

module.exports = router;
