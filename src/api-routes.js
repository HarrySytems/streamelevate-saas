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

// GET /api/v1/feed - Publicaciones del clon de Twitter/X
router.get('/feed', (req, res) => {
  try {
    const posts = stmts.getRecentFeedPosts.all();
    res.json({ count: posts.length, posts });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// POST /api/v1/feed/simulate - Disparar publicación de prueba
router.post('/feed/simulate', (req, res) => {
  try {
    const { randomUUID } = require('node:crypto');
    const slug = (req.body.slug || 'westcol').toLowerCase();
    const platform = req.body.platform || 'kick';
    const channel = stmts.getChannel.get(platform, slug) || { username: slug };
    const now = Date.now();
    
    const peak = Number(req.body.peak_viewers) || Math.floor(45000 + Math.random() * 20000);
    const avg = Number(req.body.avg_viewers) || Math.floor(peak * 0.72);
    const durSec = 3 * 3600 + 42 * 60;
    const startFoll = 4125000;
    const diff = Math.floor(450 + Math.random() * 800);
    const endFoll = startFoll + diff;
    
    const postText = [
      `REPORTE DE EMISIÓN — ${slug.toUpperCase()} (${platform.toUpperCase()})`,
      `Título: HABLANDO CLARO / ESPECIAL NOCTURNO`,
      `Categoría: Just Chatting`,
      `Duración: 3h 42m`,
      `Pico de viewers: ${peak.toLocaleString('es-ES')}`,
      `Media final observada: ${avg.toLocaleString('es-ES')}`,
      `Horas vistas observadas: ${(Math.round(avg * 3.7)).toLocaleString('es-ES')}`,
      `📈 Seguidores: +${diff.toLocaleString('es-ES')} (${startFoll.toLocaleString('es-ES')} ➔ ${endFoll.toLocaleString('es-ES')})`,
      `Mensajes registrados: ${(Math.floor(18000 + Math.random() * 15000)).toLocaleString('es-ES')}`,
      `Cuentas únicas que comentaron: ${(Math.floor(4200 + Math.random() * 3000)).toLocaleString('es-ES')}`,
      `Cobertura de audiencia: 99.8%`,
      'Media ponderada por tiempo. Audiencia concurrente; no son espectadores únicos.',
      'Los recuentos de chat corresponden a mensajes recibidos; no demuestran uso de bots.'
    ].join('\n');

    const postData = {
      id: randomUUID(),
      session_id: `${platform}:${slug}:mock_${now}`,
      platform,
      slug,
      streamer_name: channel.username || slug,
      avatar_url: `/api/v1/streamers/${slug}/avatar`,
      post_text: postText,
      media_type: 'video',
      media_url: `/sample-replay.mp4`,
      thumbnail_url: `/sample-summary.png`,
      duration_seconds: durSec,
      peak_viewers: peak,
      avg_viewers: avg,
      start_followers: startFoll,
      end_followers: endFoll,
      followers_diff: diff,
      likes_count: Math.floor(120 + Math.random() * 250),
      reposts_count: Math.floor(25 + Math.random() * 60),
      replies_count: Math.floor(12 + Math.random() * 30),
      views_count: Math.floor(3200 + Math.random() * 8000),
      created_at: now
    };

    stmts.insertFeedPost.run(postData);

    if (typeof global.__broadcastFeedPost === 'function') {
      global.__broadcastFeedPost(postData);
    }

    res.json({ success: true, post: postData });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// POST /api/v1/feed/:id/like - Dar like interactivo a un tweet
router.post('/feed/:id/like', (req, res) => {
  try {
    const id = req.params.id;
    db.prepare(`UPDATE feed_posts SET likes_count = likes_count + 1 WHERE id = ?`).run(id);
    const post = db.prepare(`SELECT * FROM feed_posts WHERE id = ?`).get(id);
    res.json({ success: true, likes_count: post ? post.likes_count : 0 });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

module.exports = router;
