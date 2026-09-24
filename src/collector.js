const fs = require('fs');
const path = require('path');
const Pusher = require('pusher-js');
const { db, stmts, recordAudience, recordChat, seedChannels } = require('./db');

const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

// In-memory sliding window state for ultra-fast API responses
const memoryState = {
  channels: new Map(),       // key (platform_slug) -> channel metadata
  activeStreams: new Map(),  // key (platform_slug) -> { streamId, viewers, peak, ... }
  chatWindows: new Map(),    // key -> array of { t, senderId }
  pusherSubscribed: new Map()// chatroomId -> channelSubscription
};

const kickAvatarCache = new Map(); // broadcaster_user_id -> profile_picture URL

let pusher = null;
let twitchWs = null;
const twitchSubscribed = new Set();
let twitchReconnectTimer = null;

let fastPollTimer = null;
let fullSweepTimer = null;
let isFullSweepRunning = false;
let isFastPollRunning = false;

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

      // Restaurar streams activos desde SQLite si el proceso se reinició
      try {
        const activeRows = db.prepare("SELECT * FROM streams WHERE status = 'live'").all();
        activeRows.forEach(row => {
          const key = `${row.platform}_${row.slug.toLowerCase()}`;
          memoryState.activeStreams.set(key, {
            id: row.id,
            slug: row.slug.toLowerCase(),
            platform: row.platform,
            startedAt: row.started_at,
            peak: row.peak_viewers || 0,
            viewers: row.peak_viewers || 0,
            category: row.category,
            title: row.title
          });
          if (row.platform === 'twitch') {
            twitchSubscribed.add(row.slug.toLowerCase());
          }
        });
        console.log(`[StreamElevate Colector] Estado activo restaurado: ${activeRows.length} streams en curso reanudados.`);
      } catch (e) {
        console.warn('[StreamElevate Colector] Aviso restaurando streams:', e.message);
      }

      console.log(`[StreamElevate Colector] Base de datos sincronizada: ${all.length} canales cargados (${all.filter(c => c.platform === 'kick').length} Kick, ${all.filter(c => c.platform === 'twitch').length} Twitch).`);
    } catch (e) {
      console.error('[StreamElevate Colector] Error leyendo streamers_database.json:', e.message);
    }
  }
}

// ==========================================
// 1. KICK CHAT (PUSHER WEBSOCKET)
// ==========================================
function initPusher() {
  const key = process.env.KICK_PUSHER_KEY || '32cbd69e4b950bf97679';
  const cluster = process.env.KICK_PUSHER_CLUSTER || 'us2';
  
  pusher = new Pusher(key, {
    cluster,
    forceTLS: true
  });

  pusher.connection.bind('connected', () => {
    console.log('[StreamElevate Colector] Pusher WebSocket (Kick) conectado al cluster', cluster);
    // Auto-suscribir a streams de Kick que ya estén activos
    for (const [key, active] of memoryState.activeStreams.entries()) {
      if (active.platform === 'kick') {
        const ch = memoryState.channels.get(key);
        const chatroomId = ch?.chatroom_id || ch?.channel_id;
        if (chatroomId) {
          subscribeChatroom(chatroomId, active.slug);
        }
      }
    }
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

    if (!memoryState.chatWindows.has(key)) {
      memoryState.chatWindows.set(key, []);
    }
    const window = memoryState.chatWindows.get(key);
    window.push({ t: now, senderId: data.sender?.id });
    
    while (window.length > 0 && now - window[0].t > 60000) {
      window.shift();
    }
  });

  console.log(`[StreamElevate Colector] Suscrito a Kick Chat en vivo: ${slug} (${channelName})`);
}

function unsubscribeChatroom(chatroomId) {
  if (!pusher || !chatroomId || !memoryState.pusherSubscribed.has(chatroomId)) return;
  pusher.unsubscribe(`chatrooms.${chatroomId}.v2`);
  memoryState.pusherSubscribed.delete(chatroomId);
}

// ==========================================
// 2. TWITCH CHAT (IRC WEBSOCKET - 0 HTTP CALLS)
// ==========================================
function initTwitchIrc() {
  if (twitchWs && (twitchWs.readyState === 0 || twitchWs.readyState === 1)) return;

  const randNick = 'justinfan' + Math.floor(Math.random() * 80000 + 10000);
  try {
    twitchWs = new WebSocket('wss://irc-ws.chat.twitch.tv:443');

    twitchWs.onopen = () => {
      console.log('[StreamElevate Colector] Twitch IRC WebSocket conectado como', randNick);
      twitchWs.send('CAP REQ :twitch.tv/tags\r\n');
      twitchWs.send(`NICK ${randNick}\r\n`);
      for (const slug of twitchSubscribed) {
        twitchWs.send(`JOIN #${slug}\r\n`);
      }
    };

    twitchWs.onmessage = (event) => {
      const data = event.data.toString();
      const lines = data.split('\r\n');
      for (const line of lines) {
        if (!line) continue;
        if (line.startsWith('PING ')) {
          twitchWs.send('PONG :tmi.twitch.tv\r\n');
          continue;
        }

        const idxPrivmsg = line.indexOf(' PRIVMSG ');
        if (idxPrivmsg !== -1) {
          const prefix = line.slice(0, idxPrivmsg);
          const rest = line.slice(idxPrivmsg + 9);
          const channel = rest.slice(0, rest.indexOf(' ')).replace('#', '').toLowerCase();
          const text = rest.slice(rest.indexOf(' :') + 2);

          const tags = {};
          if (prefix.startsWith('@')) {
            const rawTags = prefix.slice(1, prefix.indexOf(' '));
            for (const item of rawTags.split(';')) {
              const eq = item.indexOf('=');
              if (eq !== -1) tags[item.slice(0, eq)] = item.slice(eq + 1);
            }
          }

          const now = Date.now();
          const key = `twitch_${channel}`;
          const active = memoryState.activeStreams.get(key);

          const msgId = tags['id'] || `twitch_${now}_${Math.random().toString(36).slice(2, 7)}`;
          const senderId = tags['user-id'] || '';
          const senderUsername = tags['display-name'] || 'Anónimo';

          recordChat({
            message_id: msgId,
            stream_id: active ? active.id : null,
            platform: 'twitch',
            slug: channel,
            sender_id: String(senderId),
            sender_username: senderUsername,
            content: text || '',
            timestamp: now
          });

          if (!memoryState.chatWindows.has(key)) {
            memoryState.chatWindows.set(key, []);
          }
          const window = memoryState.chatWindows.get(key);
          window.push({ t: now, senderId });

          while (window.length > 0 && now - window[0].t > 60000) {
            window.shift();
          }
        }
      }
    };

    twitchWs.onerror = (err) => {
      console.warn('[StreamElevate Colector] Twitch IRC WebSocket aviso:', err?.message || 'Error de socket');
    };

    twitchWs.onclose = () => {
      console.warn('[StreamElevate Colector] Twitch IRC WebSocket cerrado. Reconectando en 3s...');
      if (!twitchReconnectTimer) {
        twitchReconnectTimer = setTimeout(() => {
          twitchReconnectTimer = null;
          initTwitchIrc();
        }, 3000);
      }
    };
  } catch (err) {
    console.error('[StreamElevate Colector] Error iniciando Twitch IRC:', err.message);
  }
}

function subscribeTwitchChat(slug) {
  const norm = slug.toLowerCase();
  twitchSubscribed.add(norm);
  if (twitchWs && twitchWs.readyState === 1) {
    twitchWs.send(`JOIN #${norm}\r\n`);
    console.log(`[StreamElevate Colector] Suscrito a Twitch Chat en vivo: #${norm}`);
  }
}

function unsubscribeTwitchChat(slug) {
  const norm = slug.toLowerCase();
  twitchSubscribed.delete(norm);
  if (twitchWs && twitchWs.readyState === 1) {
    twitchWs.send(`PART #${norm}\r\n`);
  }
}

// ==========================================
// 3. KICK OFFICIAL OAUTH API (api.kick.com - CERO BLOQUEOS)
// ==========================================
let kickToken = null;
let kickTokenExpiresAt = 0;

async function getKickAppToken() {
  if (kickToken && Date.now() < kickTokenExpiresAt - 60000) {
    return kickToken;
  }
  const clientId = process.env.KICK_CLIENT_ID || '01M35HMD769KDSDKJNZFQR6D40';
  const clientSecret = process.env.KICK_CLIENT_SECRET || '5b495a5faa45c5b25ada0076c2a70eeb6cc5477b15f299116c88568fa1a2ae59';

  try {
    const res = await fetch('https://id.kick.com/oauth/token', {
      method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      body: new URLSearchParams({
        grant_type: 'client_credentials',
        client_id: clientId,
        client_secret: clientSecret
      }),
      signal: AbortSignal.timeout(10000)
    });
    if (!res.ok) throw new Error('OAuth Kick failed: ' + res.status);
    const data = await res.json();
    kickToken = data.access_token;
    kickTokenExpiresAt = Date.now() + (data.expires_in || 3600) * 1000;
    return kickToken;
  } catch (err) {
    console.error('[StreamElevate Colector] Error obteniendo Kick OAuth token:', err.message);
    return null;
  }
}

async function pollKickBatch(channels) {
  if (!channels || channels.length === 0) return;
  const token = await getKickAppToken();
  if (!token) return;

  const slugs = channels.map(c => c.slug.toLowerCase());
  const queryStr = slugs.map(s => 'slug=' + encodeURIComponent(s)).join('&');
  const now = Date.now();

  try {
    const res = await fetch('https://api.kick.com/public/v1/channels?' + queryStr, {
      headers: {
        'Authorization': 'Bearer ' + token,
        'Accept': 'application/json'
      },
      signal: AbortSignal.timeout(10000)
    });

    if (!res.ok) {
      console.warn('[StreamElevate Colector] Kick API HTTP', res.status);
      return;
    }

    const json = await res.json();
    const items = Array.isArray(json.data) ? json.data : [];

    // Cargar fotos de avatar oficiales de los streamers en Kick
    const userIdsToFetch = items.map(it => it.broadcaster_user_id).filter(id => id && !kickAvatarCache.has(id));
    if (userIdsToFetch.length > 0) {
      try {
        const q = userIdsToFetch.slice(0, 30).map(id => 'id=' + id).join('&');
        const uRes = await fetch('https://api.kick.com/public/v1/users?' + q, {
          headers: { Authorization: 'Bearer ' + token },
          signal: AbortSignal.timeout(5000)
        });
        if (uRes.ok) {
          const uData = await uRes.json();
          (uData.data || []).forEach(u => {
            if (u.user_id && u.profile_picture) kickAvatarCache.set(u.user_id, u.profile_picture);
          });
        }
      } catch(e) {}
    }

    for (const item of items) {
      const slug = (item.slug || '').toLowerCase();
      if (!slug) continue;
      const key = `kick_${slug}`;
      const channel = memoryState.channels.get(key);

      const stream = item.stream;
      const isLive = Boolean(stream && stream.is_live);
      const viewers = isLive ? (Number(stream.viewer_count) || 0) : 0;
      const category = isLive ? (item.category?.name || 'General') : null;
      const title = isLive ? (item.stream_title || '') : null;
      const avatarUrl = kickAvatarCache.get(item.broadcaster_user_id) || item.banner_picture || null;
      const chatroomId = channel?.chatroom_id;

      let realStartedAt = now;
      if (isLive && stream.start_time) {
        const parsed = Date.parse(stream.start_time.includes('Z') ? stream.start_time : stream.start_time.replace(' ', 'T') + 'Z');
        if (Number.isFinite(parsed) && parsed > 0) {
          realStartedAt = parsed;
        }
      }

      stmts.updateChannelLive.run({
        platform: 'kick',
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
          active = { id: streamId, slug, platform: 'kick', startedAt: realStartedAt, peak: viewers, viewers, category, title, avatarUrl };
          memoryState.activeStreams.set(key, active);
          console.log(`[StreamElevate Colector] ¡STREAMER KICK EN DIRECTO!: ${slug} con ${viewers} viewers. Inicio real: ${new Date(realStartedAt).toISOString()}`);

          if (chatroomId) {
            subscribeChatroom(chatroomId, slug);
          }
        } else {
          active.viewers = viewers;
          active.peak = Math.max(active.peak, viewers);
          active.category = category;
          active.title = title;
          active.avatarUrl = avatarUrl;
          if (realStartedAt && active.startedAt !== realStartedAt) {
            active.startedAt = realStartedAt;
          }
          if (active.offlineSince) {
            console.log(`[StreamElevate Colector] ¡Streamer Kick reconectado tras microcorte IRL!: ${slug}. Continuando sesión ${active.id}.`);
            delete active.offlineSince;
          }
        }

        recordAudience(active.id, 'kick', slug, now, viewers, category, title);
      } else {
        const active = memoryState.activeStreams.get(key);
        if (active) {
          if (!active.offlineSince) {
            active.offlineSince = now;
            console.log(`[StreamElevate Colector] Señal perdida de ${slug} (Kick). Iniciando gracia (5 min por posible microcaída de WiFi/IRL)...`);
            continue;
          }

          if (now - active.offlineSince < 300000) {
            continue;
          }

          console.log(`[StreamElevate Colector] Directo Kick finalizado: ${slug} (offline > 5min).`);
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
          if (chatroomId) {
            unsubscribeChatroom(chatroomId);
          }
        }
      }
    }
  } catch (err) {
    console.error('[StreamElevate Colector] Error en pollKickBatch:', err.message);
  }
}

// ==========================================
// 4. TWITCH BATCH GQL (1 PAGO HTTP PARA MÚLTIPLES CANALES)
// ==========================================
const TWITCH_GQL_QUERY = `
  query GetStreamer($login: String!) {
    user(login: $login) {
      id
      profileImageURL(width: 300)
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

async function pollTwitchBatch(channels) {
  if (!channels || channels.length === 0) return;
  const now = Date.now();
  const body = channels.map(ch => ({
    query: TWITCH_GQL_QUERY,
    variables: { login: ch.slug.toLowerCase() }
  }));

  try {
    const res = await fetch('https://gql.twitch.tv/gql', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Client-ID': 'kimne78kx3ncx6brgo4mv6wki5h1ko'
      },
      body: JSON.stringify(body)
    });

    if (!res.ok) return;
    const batchData = await res.json();
    if (!Array.isArray(batchData)) return;

    for (let i = 0; i < batchData.length; i++) {
      const item = batchData[i];
      const channel = channels[i];
      const slug = channel.slug.toLowerCase();
      const key = `twitch_${slug}`;

      const user = item?.data?.user;
      const stream = user?.stream;
      const isLive = Boolean(stream && stream.type === 'live');
      const viewers = isLive ? (Number(stream.viewersCount) || 0) : 0;
      const category = isLive ? (stream.game?.name || 'General') : null;
      const title = isLive ? (stream.title || '') : null;
      const avatarUrl = user?.profileImageURL || null;

      let realStartedAt = now;
      if (isLive && stream.createdAt) {
        const parsed = Date.parse(stream.createdAt);
        if (Number.isFinite(parsed) && parsed > 0) {
          realStartedAt = parsed;
        }
      }

      stmts.updateChannelLive.run({
        platform: 'twitch',
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
          active = { id: streamId, slug, platform: 'twitch', startedAt: realStartedAt, peak: viewers, viewers, category, title, avatarUrl };
          memoryState.activeStreams.set(key, active);
          console.log(`[StreamElevate Colector] ¡TWITCH EN DIRECTO!: ${slug} con ${viewers} viewers. Inicio real: ${new Date(realStartedAt).toISOString()}`);
          subscribeTwitchChat(slug);
        } else {
          active.viewers = viewers;
          active.peak = Math.max(active.peak, viewers);
          active.category = category;
          active.title = title;
          if (avatarUrl) active.avatarUrl = avatarUrl;
          if (realStartedAt && active.startedAt !== realStartedAt) {
            active.startedAt = realStartedAt;
          }
          if (active.offlineSince) {
            console.log(`[StreamElevate Colector] ¡Twitch streamer reconectado tras microcorte IRL!: ${slug}. Continuando sesión ${active.id}.`);
            delete active.offlineSince;
          }
        }

        recordAudience(active.id, 'twitch', slug, now, viewers, category, title);
      } else {
        const active = memoryState.activeStreams.get(key);
        if (active) {
          if (!active.offlineSince) {
            active.offlineSince = now;
            console.log(`[StreamElevate Colector] Señal perdida de ${slug} (Twitch). Iniciando gracia (5 min por posible microcaída de WiFi/IRL)...`);
            continue;
          }

          if (now - active.offlineSince < 300000) {
            continue;
          }

          console.log(`[StreamElevate Colector] Directo Twitch finalizado: ${slug} (offline > 5min).`);
          const stats = stmts.getChatStats.get(active.id);
          const samples = stmts.getAudienceSamples.all(active.id);

          let weightedSum = 0;
          let totalDuration = 0;
          for (let s = 0; s < samples.length - 1; s++) {
            const dt = (samples[s + 1].timestamp - samples[s].timestamp) / 1000;
            weightedSum += ((samples[s].viewers + samples[s + 1].viewers) / 2) * dt;
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
          unsubscribeTwitchChat(slug);
        }
      }
    }
  } catch (err) {
    console.error('[StreamElevate Colector] Error en pollTwitchBatch:', err.message);
  }
}

// ==========================================
// 5. CADENCIA DUAL INTELIGENTE (CERO BLOQUEOS)
// ==========================================
async function runFastCycle() {
  if (isFastPollRunning) return;
  isFastPollRunning = true;

  try {
    const activeKeys = Array.from(memoryState.activeStreams.keys());
    if (activeKeys.length === 0) return;

    const activeTwitch = [];
    const activeKick = [];

    for (const key of activeKeys) {
      const active = memoryState.activeStreams.get(key);
      if (!active) continue;
      const channel = memoryState.channels.get(key);
      if (!channel) continue;
      if (active.platform === 'twitch') activeTwitch.push(channel);
      else if (active.platform === 'kick') activeKick.push(channel);
    }

    // Twitch: 1 sola petición HTTP para todos los streams activos a la vez
    if (activeTwitch.length > 0) {
      await pollTwitchBatch(activeTwitch);
    }

    // Kick: 1 sola petición oficial a api.kick.com para streams activos
    if (activeKick.length > 0) {
      await pollKickBatch(activeKick);
    }
  } finally {
    isFastPollRunning = false;
  }
}

async function runFullSweep() {
  if (isFullSweepRunning) return;
  isFullSweepRunning = true;

  try {
    const twitchChannels = Array.from(memoryState.channels.values()).filter(c => c.platform === 'twitch');
    const kickChannels = Array.from(memoryState.channels.values()).filter(c => c.platform === 'kick');

    // 1. Twitch: lotes de 20 (máximo 2 peticiones HTTP para consultar todos los 35 canales)
    for (let i = 0; i < twitchChannels.length; i += 20) {
      const chunk = twitchChannels.slice(i, i + 20);
      await pollTwitchBatch(chunk);
      if (i + 20 < twitchChannels.length) await sleep(200);
    }

    // 2. Kick: 1 sola petición oficial para todos los 28 canales
    await pollKickBatch(kickChannels);
  } finally {
    isFullSweepRunning = false;
  }
}

function startCollector() {
  loadStreamersDatabase();
  initPusher();
  initTwitchIrc();

  // Primer barrido completo al arrancar
  runFullSweep();

  // Fast cycle cada 3 segundos para streams en directo (viewers instantáneos en tiempo real)
  fastPollTimer = setInterval(runFastCycle, 3000);

  // Full sweep cada 30 segundos para detectar inicios/apagados de streams
  fullSweepTimer = setInterval(runFullSweep, 30000);
  console.log('[StreamElevate Colector] Cadencia Inteligente Dual iniciada (Fast: 3s en vivo, Full: 30s general)');
}

function stopCollector() {
  if (fastPollTimer) clearInterval(fastPollTimer);
  if (fullSweepTimer) clearInterval(fullSweepTimer);
  if (pusher) pusher.disconnect();
  if (twitchWs) twitchWs.close();
}

function getTelemetrySnapshot(slug, platform = 'kick') {
  const normSlug = slug.toLowerCase();
  const normPlatform = platform.toLowerCase();
  const key = `${normPlatform}_${normSlug}`;
  const channel = stmts.getChannel.get(normPlatform, normSlug) || stmts.getChannelBySlug.get(normSlug);
  const active = memoryState.activeStreams.get(key);
  const window = memoryState.chatWindows.get(key) || [];
  
  // Calcular métricas de chat por minuto en RAM al instante
  const now = Date.now();
  const recentChats = window.filter(m => now - m.t <= 60000);
  const uniqueChattersSet = new Set(recentChats.map(m => m.senderId).filter(Boolean));

  return {
    slug: normSlug,
    platform: normPlatform,
    is_live: Boolean(active && !active.offlineSince),
    is_reconnecting: Boolean(active && active.offlineSince),
    stream_id: active ? active.id : null,
    current_viewers: active ? active.viewers : (channel?.current_viewers || 0),
    peak_viewers: active ? active.peak : 0,
    category: active ? active.category : (channel?.current_category || 'N/D'),
    title: active ? active.title : (channel?.current_title || 'N/D'),
    avatar_url: active?.avatarUrl || channel?.avatar_url || null,
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
  pollKickBatch,
  pollTwitchBatch,
  twitchSubscribed
};
