const fs = require('fs');
const path = require('path');
const Pusher = require('pusher-js');
const { db, stmts, recordAudience, recordChat, seedChannels } = require('./db');

// In-memory sliding window state for ultra-fast API responses
const memoryState = {
  channels: new Map(),       // slug -> channel metadata
  activeStreams: new Map(),  // key (platform_slug) -> { streamId, viewers, peak, ... }
  chatWindows: new Map(),    // key -> array of { time, senderId }
  pusherSubscribed: new Map()// chatroomId -> channelSubscription
};

let pusher = null;
let pollTimer = null;
let isPolling = false;

function loadStreamersDatabase() {
  const localDb = path.join(__dirname, '..', 'data', 'streamers_database.json');
  const parentDb = path.join(__dirname, '..', '..', 'streamers_database.json');
  const dbFile = fs.existsSync(localDb) ? localDb : parentDb;
  if (fs.existsSync(dbFile)) {
    try {
      const data = JSON.parse(fs.readFileSync(dbFile, 'utf8'));
      const all = [];
      if (Array.isArray(data.kick)) {
        data.kick.forEach(ch => {
          const slug = (ch.slug || ch.channel || '').toLowerCase();
          if (slug) {
            all.push({
              ...ch,
              slug,
              username: ch.username || ch.name || ch.slug,
              platform: 'kick'
            });
          }
        });
      }
      if (Array.isArray(data.twitch)) {
        data.twitch.forEach(ch => {
          const slug = (ch.slug || ch.channel || '').toLowerCase();
          if (slug) {
            all.push({
              ...ch,
              slug,
              username: ch.username || ch.name || ch.channel || slug,
              platform: 'twitch'
            });
          }
        });
      }
      seedChannels(all);
      all.forEach(ch => {
        memoryState.channels.set(`${ch.platform}_${ch.slug.toLowerCase()}`, ch);
      });
      console.log(`[StreamElevate Colector] Base de datos sincronizada: ${all.length} canales cargados.`);
    } catch (e) {
      console.error('[StreamElevate Colector] Error leyendo streamers_database.json:', e.message);
    }
  }
}

function initPusher() {
  const key = process.env.KICK_PUSHER_KEY || '32cbd69e4b950bf97679';
  const cluster = process.env.KICK_PUSHER_CLUSTER || 'us2';
  
  pusher = new Pusher(key, {
    cluster,
    forceTLS: true
  });

  pusher.connection.bind('connected', () => {
    console.log('[StreamElevate Colector] Pusher WebSocket conectado al cluster', cluster);
  });

  pusher.connection.bind('error', (err) => {
    console.warn('[StreamElevate Colector] Pusher WebSocket aviso:', err?.error?.data?.message || err.message);
  });
}

function subscribeChatroom(chatroomId, slug) {
  if (!pusher || !chatroomId || memoryState.pusherSubscribed.has(chatroomId)) return;
  
  const channelName = `chatrooms.${chatroomId}.v2`;
  const channel = pusher.subscribe(channelName);
  memoryState.pusherSubscribed.set(chatroomId, channel);

  channel.bind('App\\Events\\ChatMessageEvent', (data) => {
    const now = Date.now();
    const key = `kick_${slug.toLowerCase()}`;
    const active = memoryState.activeStreams.get(key);

    recordChat({
      message_id: data.id || `kick_${now}_${Math.random().toString(36).slice(2, 7)}`,
      stream_id: active ? active.id : null,
      platform: 'kick',
      slug: slug.toLowerCase(),
      sender_id: String(data.sender?.id || ''),
      sender_username: data.sender?.username || 'Anónimo',
      content: data.content || '',
      timestamp: Date.parse(data.created_at) || now
    });

    // Registrar en ventana deslizante de 60 segundos
    if (!memoryState.chatWindows.has(key)) {
      memoryState.chatWindows.set(key, []);
    }
    const window = memoryState.chatWindows.get(key);
    window.push({ t: now, senderId: data.sender?.id });
    
    // Purgar mensajes más viejos de 60s
    while (window.length > 0 && now - window[0].t > 60000) {
      window.shift();
    }
  });

  console.log(`[StreamElevate Colector] Suscrito a chat en vivo: ${slug} (${channelName})`);
}

function unsubscribeChatroom(chatroomId) {
  if (!pusher || !chatroomId || !memoryState.pusherSubscribed.has(chatroomId)) return;
  pusher.unsubscribe(`chatrooms.${chatroomId}.v2`);
  memoryState.pusherSubscribed.delete(chatroomId);
}

async function pollKickChannel(channel) {
  const slug = channel.slug.toLowerCase();
  const key = `kick_${slug}`;
  const now = Date.now();
  const url = `https://kick.com/api/v2/channels/${encodeURIComponent(slug)}`;

  try {
    const res = await fetch(url, {
      headers: {
        'User-Agent': 'Mozilla/5.0',
        'Accept': 'application/json'
      }
    });

    if (!res.ok) {
      if (res.status === 404) return;
      return;
    }

    const data = await res.json();
    const chatroomId = data.chatroom?.id || channel.chatroom_id;
    const isLive = Boolean(data.livestream && data.livestream.is_live !== false);
    const viewers = isLive ? (data.livestream.viewer_count || 0) : 0;
    const category = isLive ? (data.livestream.categories?.[0]?.name || 'General') : null;
    const title = isLive ? (data.livestream.session_title || '') : null;

    stmts.updateChannelLive.run({
      slug,
      is_live: isLive ? 1 : 0,
      current_viewers: viewers,
      current_category: category,
      current_title: title,
      last_checked_at: now
    });

    if (isLive) {
      const kickStartTime = data.livestream?.start_time || data.livestream?.created_at;
      let realStartedAt = now;
      if (kickStartTime) {
        const parsed = Date.parse(kickStartTime.includes('Z') ? kickStartTime : kickStartTime.replace(' ', 'T') + 'Z');
        if (Number.isFinite(parsed) && parsed > 0) {
          realStartedAt = parsed;
        }
      }

      let active = memoryState.activeStreams.get(key);
      if (!active) {
        // Iniciar nueva sesión de stream con el timestamp real de inicio de Kick
        const streamId = `kick_${slug}_${realStartedAt}`;
        stmts.createStream.run({
          id: streamId,
          platform: 'kick',
          slug,
          title,
          category,
          started_at: realStartedAt,
          peak_viewers: viewers
        });
        active = { id: streamId, slug, platform: 'kick', startedAt: realStartedAt, peak: viewers, viewers, category, title };
        memoryState.activeStreams.set(key, active);
        console.log(`[StreamElevate Colector] ¡STREAMER EN DIRECTO!: ${slug} con ${viewers} viewers. Inicio real: ${new Date(realStartedAt).toISOString()}`);
        
        // Conectar Pusher de inmediato
        if (chatroomId) {
          subscribeChatroom(chatroomId, slug);
        }
      } else {
        active.viewers = viewers;
        active.peak = Math.max(active.peak, viewers);
        active.category = category;
        active.title = title;
        if (realStartedAt && active.startedAt !== realStartedAt) {
          active.startedAt = realStartedAt;
        }
        if (active.offlineSince) {
          console.log(`[StreamElevate Colector] ¡Streamer reconectado tras microcorte IRL!: ${slug}. Continuando sesión ${active.id}.`);
          delete active.offlineSince;
        }
      }

      // Guardar muestra de audiencia en SQLite
      recordAudience(active.id, 'kick', slug, now, viewers, category, title);
    } else {
      const active = memoryState.activeStreams.get(key);
      if (active) {
        // Período de gracia para microcaídas IRL (5 minutos = 300,000 ms)
        if (!active.offlineSince) {
          active.offlineSince = now;
          console.log(`[StreamElevate Colector] Señal no detectada de ${slug}. Iniciando período de gracia (5 min por posible microcaída de WiFi/IRL)...`);
          return;
        }

        const offlineDurationMs = now - active.offlineSince;
        if (offlineDurationMs < 300000) {
          // Aún dentro del período de tolerancia por caída de conexión
          return;
        }

        // Más de 5 minutos offline continuo: el directo concluyó oficialmente
        console.log(`[StreamElevate Colector] Directo finalizado oficialmente: ${slug} (offline > 5min). Calculando estadísticas finales...`);
        const stats = stmts.getChatStats.get(active.id);
        const samples = stmts.getAudienceSamples.all(active.id);

        // Promedio ponderado por tiempo (integral de trapecios)
        let weightedSum = 0;
        let totalDuration = 0;
        for (let i = 0; i < samples.length - 1; i++) {
          const dt = (samples[i + 1].timestamp - samples[i].timestamp) / 1000;
          weightedSum += ((samples[i].viewers + samples[i + 1].viewers) / 2) * dt;
          totalDuration += dt;
        }
        const avgViewers = totalDuration > 0 ? (weightedSum / totalDuration) : active.viewers;

        stmts.closeStream.run({
          id: active.id,
          ended_at: active.offlineSince || now,
          avg_viewers: Math.round(avgViewers),
          total_messages: stats?.total_messages || 0,
          unique_chatters: stats?.unique_chatters || 0
        });

        memoryState.activeStreams.delete(key);
        if (chatroomId) {
          unsubscribeChatroom(chatroomId);
        }
      }
    }
  } catch (err) {
    // Silencioso ante microcortes
  }
}

const TWITCH_GQL_QUERY = `
  query GetStreamer($login: String!) {
    user(login: $login) {
      id
      stream {
        id
        title
        type
        viewersCount
        createdAt
        game {
          name
        }
      }
    }
  }
`;

async function pollTwitchChannel(channel) {
  const slug = channel.slug.toLowerCase();
  const key = `twitch_${slug}`;
  const now = Date.now();

  try {
    const res = await fetch('https://gql.twitch.tv/gql', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Client-ID': 'kimne78kx3ncx6brgo4mv6wki5h1ko'
      },
      body: JSON.stringify({
        query: TWITCH_GQL_QUERY,
        variables: { login: slug }
      })
    });

    if (!res.ok) return;
    const data = await res.json();
    const user = data.data?.user;
    const stream = user?.stream;
    const isLive = Boolean(stream && stream.type === 'live');
    const viewers = isLive ? (Number(stream.viewersCount) || 0) : 0;
    const category = isLive ? (stream.game?.name || 'General') : null;
    const title = isLive ? (stream.title || '') : null;

    let realStartedAt = now;
    if (isLive && stream.createdAt) {
      const parsed = Date.parse(stream.createdAt);
      if (Number.isFinite(parsed) && parsed > 0) {
        realStartedAt = parsed;
      }
    }

    stmts.updateChannelLive.run({
      slug,
      is_live: isLive ? 1 : 0,
      current_viewers: viewers,
      current_category: category,
      current_title: title,
      last_checked_at: now
    });

    if (isLive) {
      let active = memoryState.activeStreams.get(key);
      if (!active) {
        const streamId = `twitch_${slug}_${realStartedAt}`;
        stmts.createStream.run({
          id: streamId,
          platform: 'twitch',
          slug,
          title,
          category,
          started_at: realStartedAt,
          peak_viewers: viewers
        });
        active = { id: streamId, slug, platform: 'twitch', startedAt: realStartedAt, peak: viewers, viewers, category, title };
        memoryState.activeStreams.set(key, active);
        console.log(`[StreamElevate Colector] ¡TWITCH EN DIRECTO!: ${slug} con ${viewers} viewers. Inicio real: ${new Date(realStartedAt).toISOString()}`);
      } else {
        active.viewers = viewers;
        active.peak = Math.max(active.peak, viewers);
        active.category = category;
        active.title = title;
        if (realStartedAt && active.startedAt !== realStartedAt) {
          active.startedAt = realStartedAt;
        }
        if (active.offlineSince) {
          delete active.offlineSince;
        }
      }

      recordAudience(active.id, 'twitch', slug, now, viewers, category, title);
    } else {
      const active = memoryState.activeStreams.get(key);
      if (active) {
        if (!active.offlineSince) {
          active.offlineSince = now;
          return;
        }
        if (now - active.offlineSince < 300000) {
          return;
        }

        const stats = stmts.getChatStats.get(active.id);
        const samples = stmts.getAudienceSamples.all(active.id);

        let weightedSum = 0;
        let totalDuration = 0;
        for (let i = 0; i < samples.length - 1; i++) {
          const dt = (samples[i + 1].timestamp - samples[i].timestamp) / 1000;
          weightedSum += ((samples[i].viewers + samples[i + 1].viewers) / 2) * dt;
          totalDuration += dt;
        }
        const avgViewers = totalDuration > 0 ? (weightedSum / totalDuration) : active.viewers;

        stmts.closeStream.run({
          id: active.id,
          ended_at: active.offlineSince || now,
          avg_viewers: Math.round(avgViewers),
          total_messages: stats?.total_messages || 0,
          unique_chatters: stats?.unique_chatters || 0
        });

        memoryState.activeStreams.delete(key);
      }
    }
  } catch (err) {}
}

async function runPollCycle() {
  if (isPolling) return;
  isPolling = true;

  try {
    const kickChannels = Array.from(memoryState.channels.values()).filter(c => c.platform === 'kick');
    const twitchChannels = Array.from(memoryState.channels.values()).filter(c => c.platform === 'twitch');

    // Sondeo balanceado de Kick (lotes de 6)
    for (let i = 0; i < kickChannels.length; i += 6) {
      const chunk = kickChannels.slice(i, i + 6);
      await Promise.all(chunk.map(ch => pollKickChannel(ch)));
    }

    // Sondeo balanceado de Twitch (lotes de 8)
    for (let i = 0; i < twitchChannels.length; i += 8) {
      const chunk = twitchChannels.slice(i, i + 8);
      await Promise.all(chunk.map(ch => pollTwitchChannel(ch)));
    }
  } finally {
    isPolling = false;
  }
}

function startCollector() {
  loadStreamersDatabase();
  initPusher();

  const interval = Number(process.env.POLL_INTERVAL_MS) || 30000;
  console.log(`[StreamElevate Colector] Ciclo de sondeo iniciado cada ${interval / 1000}s`);
  
  // Primer ciclo inmediato
  runPollCycle();
  pollTimer = setInterval(runPollCycle, interval);
}

function stopCollector() {
  if (pollTimer) clearInterval(pollTimer);
  if (pusher) pusher.disconnect();
}

function getTelemetrySnapshot(slug, platform = 'kick') {
  const key = `${platform}_${slug.toLowerCase()}`;
  const channel = stmts.getChannel.get(slug.toLowerCase());
  const active = memoryState.activeStreams.get(key);
  const window = memoryState.chatWindows.get(key) || [];
  
  // Calcular métricas de chat por minuto en RAM al instante
  const now = Date.now();
  const recentChats = window.filter(m => now - m.t <= 60000);
  const uniqueChattersSet = new Set(recentChats.map(m => m.senderId).filter(Boolean));

  return {
    slug: slug.toLowerCase(),
    platform,
    is_live: Boolean(active && !active.offlineSince),
    is_reconnecting: Boolean(active && active.offlineSince),
    stream_id: active ? active.id : null,
    current_viewers: active ? active.viewers : (channel?.current_viewers || 0),
    peak_viewers: active ? active.peak : 0,
    category: active ? active.category : (channel?.current_category || 'N/D'),
    title: active ? active.title : (channel?.current_title || 'N/D'),
    started_at: active ? active.startedAt : null,
    uptime_seconds: active ? Math.floor((now - active.startedAt) / 1000) : 0,
    chat_velocity: {
      msgs_per_min: recentChats.length,
      chatters_per_min: uniqueChattersSet.size
    },
    last_checked_at: channel?.last_checked_at || null
  };
}

module.exports = {
  startCollector,
  stopCollector,
  getTelemetrySnapshot,
  memoryState,
  subscribeChatroom,
  pollKickChannel
};
