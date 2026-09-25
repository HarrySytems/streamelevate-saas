const fs = require('fs');
const path = require('path');
const Pusher = require('pusher-js');
const { db, stmts, recordAudience, recordChat, seedChannels, downsampleSamplesForChart, recordCaptureGap, closeAndEnqueue } = require('./db');
const { resolveSession, calculateObservedStats } = require('./session-state');

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
      // Sincronizar estado persistido de canales desde SQLite
      try {
        const savedChannels = stmts.getAllChannels.all();
        savedChannels.forEach(ch => {
          const key = `${ch.platform}_${ch.slug.toLowerCase()}`;
          const existing = memoryState.channels.get(key);
          if (existing) {
            existing.initial_stream_handled = ch.initial_stream_handled || 0;
          } else {
            memoryState.channels.set(key, ch);
          }
        });
      } catch (e) {}

      // Restaurar streams activos desde SQLite si el proceso se reinició
      try {
        const activeRows = db.prepare("SELECT * FROM streams WHERE status = 'live'").all();
        activeRows.forEach(row => {
          const key = `${row.platform}_${row.slug.toLowerCase()}`;
          let initialSamples = [];
          try {
            initialSamples = stmts.getAudienceSamples.all(row.id).map(s => ({ timestamp: s.timestamp, viewers: s.viewers }));
          } catch(e) {}
          // Conservar broadcastId oficial o null si es provisional/desconocido (NUNCA derivar del ID interno UUID)
          let broadcastId = row.broadcast_id || null;
          if (broadcastId && broadcastId.startsWith('prov_')) {
            broadcastId = null;
          }

          memoryState.activeStreams.set(key, {
            id: row.id,
            broadcastId,
            isProvisional: !broadcastId,
            slug: row.slug.toLowerCase(),
            platform: row.platform,
            startedAt: row.started_at,
            lastLiveAt: row.last_live_at || row.started_at,
            peak: row.peak_viewers || 0,
            viewers: row.peak_viewers || 0,
            category: row.category,
            title: row.title,
            ignoreReports: Boolean(row.ignore_reports),
            recentSamples: initialSamples
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

  try {
    channel.bind('App\\Events\\ChatMessageEvent', (data) => {
      try {
        const now = Date.now();
        const key = `kick_${slug.toLowerCase()}`;
        const active = memoryState.activeStreams.get(key);

        recordChat({
          message_id: data?.id || `kick_${now}_${Math.random().toString(36).slice(2, 7)}`,
          stream_id: active ? active.id : null,
          platform: 'kick',
          slug: slug.toLowerCase(),
          sender_id: String(data?.sender?.id || ''),
          sender_username: data?.sender?.username || 'Anónimo',
          content: data?.content || '',
          timestamp: (data?.created_at ? Date.parse(data.created_at) : now) || now
        });

        if (!memoryState.chatWindows.has(key)) {
          memoryState.chatWindows.set(key, []);
        }
        const window = memoryState.chatWindows.get(key);
        window.push({ t: now, senderId: data?.sender?.id });
        
        while (window.length > 0 && now - window[0].t > 60000) {
          window.shift();
        }
      } catch (err) {
        console.warn(`[StreamElevate Colector] Error procesando chat Kick (${slug}):`, err.message);
      }
    });

    channel.bind('pusher:subscription_error', (status) => {
      console.warn(`[StreamElevate Colector] Error suscripción Pusher (${channelName}):`, status);
    });
  } catch (err) {
    console.warn(`[StreamElevate Colector] Error configurando canal Pusher (${channelName}):`, err.message);
  }

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
      try {
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
      } catch (err) {
        console.warn('[StreamElevate Colector] Error procesando mensaje Twitch IRC:', err.message);
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

function resolveBroadcastIdentity(slug, platform, stream, active, now) {
  const platformId = stream?.id ? String(stream.id) : null;
  
  // 1. Prioridad: ID oficial de emisión entregado por la plataforma
  if (platformId) {
    return {
      broadcastId: platformId,
      isProvisional: false,
      startedAt: null
    };
  }

  // 2. Segunda opción: Fecha de inicio válida y estable
  let validStartTime = null;
  const rawStart = stream?.start_time || stream?.createdAt;
  if (rawStart) {
    const parsed = Date.parse(typeof rawStart === 'string' && rawStart.includes('Z') ? rawStart : String(rawStart).replace(' ', 'T') + 'Z');
    if (Number.isFinite(parsed) && parsed > 0) {
      validStartTime = parsed;
    }
  }

  if (validStartTime) {
    return {
      broadcastId: `start_${validStartTime}`,
      isProvisional: false,
      startedAt: validStartTime
    };
  }

  // 3. Si faltan ambos:
  if (active && active.broadcastId) {
    // Conservar la sesión activa como identidad pendiente de confirmar (sin fragmentar en cada tick)
    return {
      broadcastId: active.broadcastId,
      isProvisional: Boolean(active.isProvisional),
      startedAt: active.startedAt
    };
  }

  // Si no existía sesión previa, generar ID provisional estable una sola vez
  return {
    broadcastId: `prov_${slug}_${now}`,
    isProvisional: true,
    startedAt: now
  };
}

function closeStreamSession(active) {
  if (!active || !active.id) return false;
  try {
    const stats = stmts.getChatStats.get(active.id);
    const samples = stmts.getAudienceSamples.all(active.id);
    const gaps = stmts.getGaps.all(active.id);

    // Usar calculateObservedStats desde session-state (función de producción real)
    const { averageViewers, observedSeconds } = calculateObservedStats(samples, gaps);

    // El final de la emisión se sella con la última observación real en directo
    const finalEndedAt = active.lastLiveAt || active.offlineSince || Date.now();
    const totalDurationSeconds = Math.max(1, (finalEndedAt - active.startedAt) / 1000);
    const coverageRatio = Math.min(1.0, observedSeconds / totalDurationSeconds);

    // null = cobertura insuficiente.
    // NO sustituir por active.viewers: ese valor no es una media, es el último dato puntual.

    const endFollowers = active.endFollowers != null ? active.endFollowers : (active.startFollowers || null);
    const startFollowers = active.startFollowers != null ? active.startFollowers : null;
    const followersDiff = (startFollowers != null && endFollowers != null) ? (endFollowers - startFollowers) : null;

    if (active.ignoreReports) {
      // Política de primera emisión completa: cerrar en BD pero OMITIR reporte
      stmts.closeStream.run({
        id: active.id,
        ended_at: finalEndedAt,
        avg_viewers: averageViewers !== null ? Math.round(averageViewers) : null,
        coverage_ratio: Number(coverageRatio.toFixed(4)),
        last_live_at: active.lastLiveAt || finalEndedAt,
        first_offline_at: active.firstOfflineAt || active.offlineSince || finalEndedAt,
        total_messages: stats?.total_messages || 0,
        unique_chatters: stats?.unique_chatters || 0,
        end_followers: endFollowers,
        followers_diff: followersDiff
      });
      console.log(`[StreamElevate Colector] Sesión ${active.id} (${active.slug}) cerrada sin reporte (política de primera emisión completa).`);
    } else {
      // Cerrar sesión + encolar reporte en transacción atómica
      closeAndEnqueue(active.id, () => {
        stmts.closeStream.run({
          id: active.id,
          ended_at: finalEndedAt,
          avg_viewers: averageViewers !== null ? Math.round(averageViewers) : null,
          coverage_ratio: Number(coverageRatio.toFixed(4)),
          last_live_at: active.lastLiveAt || finalEndedAt,
          first_offline_at: active.firstOfflineAt || active.offlineSince || finalEndedAt,
          total_messages: stats?.total_messages || 0,
          unique_chatters: stats?.unique_chatters || 0,
          end_followers: endFollowers,
          followers_diff: followersDiff
        });
      });
    }

    if (active.platform === 'kick' && active.chatroomId) {
      unsubscribeChatroom(active.chatroomId);
    } else if (active.platform === 'twitch') {
      unsubscribeTwitchChat(active.slug);
    }

    active.closed = true;
    delete active.pendingClose;
    return true;
  } catch (err) {
    console.error(`[StreamElevate Colector] Error cerrando sesión ${active.id}: ${err.message}. Conservando en memoria para reintento.`);
    active.pendingClose = true;
    return false;
  }
}

const kickFollowersCache = new Map(); // slug -> { count, at }

async function fetchKickFollowers(slug) {
  const norm = slug.toLowerCase();
  const cached = kickFollowersCache.get(norm);
  if (cached && (Date.now() - cached.at < 60000)) {
    return cached.count;
  }
  try {
    const res = await fetch(`https://kick.com/api/v2/channels/${encodeURIComponent(norm)}`, {
      headers: { 'Accept': 'application/json', 'User-Agent': 'Mozilla/5.0' },
      signal: AbortSignal.timeout(4000)
    });
    if (res.ok) {
      const d = await res.json();
      const count = Number(d.followers_count) || null;
      if (count != null) {
        kickFollowersCache.set(norm, { count, at: Date.now() });
        return count;
      }
    }
  } catch (e) {}
  return null;
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
      // Estado DESCONOCIDO: error HTTP o rate limit. NO marcar offline ni cerrar streams.
      console.warn('[StreamElevate Colector] Kick API HTTP', res.status, '- Estado DESCONOCIDO, reintentando.');
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

      let active = memoryState.activeStreams.get(key);

      // Si había un cierre pendiente que falló antes, reintentar primero
      if (active && active.pendingClose) {
        const ok = closeStreamSession(active);
        if (ok) {
          memoryState.activeStreams.delete(key);
          active = null;
        }
      }

      // Identificación segura de emisión — usando resolveSession (producción real)
      const broadcastId = stream?.id ? String(stream.id) : null;
      let parsedStart = null;
      if (isLive && stream?.start_time) {
        const p = Date.parse(stream.start_time.includes('Z') ? stream.start_time : stream.start_time.replace(' ', 'T') + 'Z');
        if (Number.isFinite(p) && p > 0) parsedStart = p;
      }
      const sessionAction = resolveSession(active, {
        broadcastId,
        startedAt: parsedStart,
        observedAt: now
      });

      const realStartedAt = parsedStart || active?.startedAt || now;

      stmts.updateChannelLive.run({
        platform: 'kick',
        slug,
        is_live: isLive ? 1 : 0,
        current_viewers: viewers,
        current_category: category,
        current_title: title,
        last_checked_at: now
      });

      // Política de primera emisión completa post-limpieza
      const chMeta = memoryState.channels.get(key);
      const isFirstCheck = !chMeta || (chMeta.initial_stream_handled !== 1);
      let ignoreReportsForNewSession = false;

      if (isFirstCheck) {
        if (isLive) {
          ignoreReportsForNewSession = true;
          console.log(`[StreamElevate Colector] [Política Primera Emisión] ${slug} (Kick) ya estaba en directo al iniciar. Se vigila telemetría pero se omitirá su informe final.`);
        } else {
          console.log(`[StreamElevate Colector] [Política Primera Emisión] ${slug} (Kick) confirmado offline. Queda listo para medir su próxima emisión completa.`);
        }
        if (chMeta) chMeta.initial_stream_handled = 1;
        try { stmts.setInitialStreamHandled.run('kick', slug); } catch(e) {}
      }

      if (isLive) {
        // Procesar acción de identidad
        if (sessionAction.action === 'close_and_create') {
          console.log(`[StreamElevate Colector] ¡Nueva emisión B detectada para ${slug} (Kick)! Cerrando emisión previa...`);
          const closed = closeStreamSession(active);
          if (closed) { memoryState.activeStreams.delete(key); active = null; }
        } else if (sessionAction.action === 'update' && active) {
          console.log(`[StreamElevate Colector] Matrícula Kick oficial confirmada para ${slug}: ${sessionAction.patch.broadcastId}. Asociando a sesión.`);
          active.broadcastId = sessionAction.patch.broadcastId;
          active.isProvisional = false;
          try { db.prepare("UPDATE streams SET broadcast_id = ? WHERE id = ?").run(active.broadcastId, active.id); } catch(e) {}
        } else if (sessionAction.action === 'create') {
          // Crear nueva sesión con el id interno del nuevo objeto de sesión
          if (active) { closeStreamSession(active); memoryState.activeStreams.delete(key); active = null; }
        }
        // identity_pending: conservar sesión actual sin fragmentar; esperar más evidencia

        const effectiveStreamId = active ? active.id : (sessionAction.session ? `kick:${slug}:${sessionAction.session.id}` : `kick:${slug}:fallback_${now}`);
        const actualBroadcastId = active ? active.broadcastId : (sessionAction.session ? sessionAction.session.broadcastId : null);

        if (!active) {
          const ignoreReportsVal = ignoreReportsForNewSession ? 1 : 0;
          let initFollowers = null;
          try { initFollowers = await fetchKickFollowers(slug); } catch(e) {}

          stmts.createStream.run({
            id: effectiveStreamId,
            broadcast_id: actualBroadcastId,
            platform: 'kick',
            slug,
            title,
            category,
            started_at: realStartedAt,
            peak_viewers: viewers,
            last_live_at: now,
            ignore_reports: ignoreReportsVal,
            start_followers: initFollowers,
            end_followers: initFollowers,
            followers_diff: 0
          });
          active = {
            id: effectiveStreamId,
            broadcastId: actualBroadcastId,
            isProvisional: !actualBroadcastId,
            slug,
            platform: 'kick',
            startedAt: realStartedAt,
            lastLiveAt: now,
            peak: viewers,
            viewers,
            category,
            title,
            avatarUrl,
            chatroomId,
            startFollowers: initFollowers,
            endFollowers: initFollowers,
            ignoreReports: Boolean(ignoreReportsVal)
          };
          memoryState.activeStreams.set(key, active);
          console.log(`[StreamElevate Colector] ¡STREAMER KICK EN DIRECTO! [Matrícula ${actualBroadcastId || 'Provisional'}]: ${slug} con ${viewers} viewers.`);
          if (chatroomId) subscribeChatroom(chatroomId, slug);
        } else {
          // Continúa la misma emisión
          active.viewers = viewers;
          active.peak = Math.max(active.peak, viewers);
          active.category = category;
          active.title = title;
          active.avatarUrl = avatarUrl;
          active.lastLiveAt = now;
          if (active.offlineSince) {
            // Reconectó: registrar cierre del hueco
            const gapEnd = now;
            recordCaptureGap(active.id, active.offlineSince, gapEnd, 'reconnect');
            console.log(`[StreamElevate Colector] ¡Streamer Kick reconectado: ${slug}! Hueco de ${Math.round((gapEnd - active.offlineSince)/1000)}s registrado.`);
            delete active.offlineSince;
            delete active.firstOfflineAt;
          }
        }

        if (!active.recentSamples) {
          try {
            active.recentSamples = stmts.getAudienceSamples.all(active.id).map(s => ({ timestamp: s.timestamp, viewers: s.viewers }));
          } catch(e) { active.recentSamples = []; }
        }
        const lastSample = active.recentSamples[active.recentSamples.length - 1];
        if (!lastSample || lastSample.viewers !== viewers || (now - lastSample.timestamp >= 20000)) {
          active.recentSamples.push({ timestamp: now, viewers });
          if (active.recentSamples.length > 50000) active.recentSamples.shift();
          recordAudience(active.id, 'kick', slug, now, viewers, category, title);
        }
      } else {
        // Respuesta válida confirmando OFFLINE
        if (active) {
          if (!active.offlineSince) {
            active.offlineSince = now;
            active.firstOfflineAt = now;
            console.log(`[StreamElevate Colector] Señal perdida de ${slug} (Kick) [Matrícula ${active.broadcastId}]. Esperando 5 min por microcorte de red...`);
            continue;
          }

          if (now - active.offlineSince < 300000) {
            continue;
          }

          // Hueco confirmado por timeout (>5min) — registrar antes de cerrar
          recordCaptureGap(active.id, active.offlineSince, now, 'stream_ended');
          console.log(`[StreamElevate Colector] Directo Kick finalizado: ${slug} [Matrícula ${active.broadcastId}] (offline confirmado >5min).`);
          const closed = closeStreamSession(active);
          if (closed) {
            memoryState.activeStreams.delete(key);
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
      followers {
        totalCount
      }
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
      if (!channel) continue;
      const slug = channel.slug.toLowerCase();
      const key = `twitch_${slug}`;

      if (!item || !item.data) {
        console.warn(`[StreamElevate Colector] Twitch GQL error para ${slug}: conservando estado pendiente.`);
        continue;
      }

      const user = item.data.user;
      const followers = Number(user?.followers?.totalCount) || null;
      const stream = user?.stream;
      const isLive = Boolean(stream && stream.type === 'live');
      const viewers = isLive ? (Number(stream.viewersCount) || 0) : 0;
      const category = isLive ? (stream.game?.name || 'General') : null;
      const title = isLive ? (stream.title || '') : null;
      const avatarUrl = user?.profileImageURL || null;


      // Recuperar sesión activa PRIMERO
      let active = memoryState.activeStreams.get(key);

      // Si había un cierre pendiente que falló antes, reintentar primero
      if (active && active.pendingClose) {
        const ok = closeStreamSession(active);
        if (ok) {
          memoryState.activeStreams.delete(key);
          active = null;
        }
      }

      // Fecha de inicio real
      let realStartedAt = now;
      let parsedStart = null;
      if (isLive && stream.createdAt) {
        const parsed = Date.parse(stream.createdAt);
        if (Number.isFinite(parsed) && parsed > 0) {
          realStartedAt = parsed;
          parsedStart = parsed;
        }
      }

      // Identificación segura de emisión — usando resolveSession (producción real)
      const broadcastId = stream?.id ? String(stream.id) : null;
      const sessionAction = resolveSession(active, {
        broadcastId,
        startedAt: parsedStart,
        observedAt: now
      });

      stmts.updateChannelLive.run({
        platform: 'twitch',
        slug,
        is_live: isLive ? 1 : 0,
        current_viewers: viewers,
        current_category: category,
        current_title: title,
        last_checked_at: now
      });

      // Política de primera emisión completa post-limpieza
      const chMeta = memoryState.channels.get(key);
      const isFirstCheck = !chMeta || (chMeta.initial_stream_handled !== 1);
      let ignoreReportsForNewSession = false;

      if (isFirstCheck) {
        if (isLive) {
          ignoreReportsForNewSession = true;
          console.log(`[StreamElevate Colector] [Política Primera Emisión] ${slug} (Twitch) ya estaba en directo al iniciar. Se vigila telemetría pero se omitirá su informe final.`);
        } else {
          console.log(`[StreamElevate Colector] [Política Primera Emisión] ${slug} (Twitch) confirmado offline. Queda listo para medir su próxima emisión completa.`);
        }
        if (chMeta) chMeta.initial_stream_handled = 1;
        try { stmts.setInitialStreamHandled.run('twitch', slug); } catch(e) {}
      }

      if (isLive) {
        // Procesar acción de identidad
        if (sessionAction.action === 'close_and_create') {
          console.log(`[StreamElevate Colector] ¡Nueva emisión B detectada para ${slug} (Twitch)! Cerrando emisión previa...`);
          const closed = closeStreamSession(active);
          if (closed) { memoryState.activeStreams.delete(key); active = null; }
        } else if (sessionAction.action === 'update' && active) {
          console.log(`[StreamElevate Colector] Matrícula Twitch oficial confirmada para ${slug}: ${sessionAction.patch.broadcastId}. Asociando a sesión.`);
          active.broadcastId = sessionAction.patch.broadcastId;
          active.isProvisional = false;
          try { db.prepare("UPDATE streams SET broadcast_id = ? WHERE id = ?").run(active.broadcastId, active.id); } catch(e) {}
        } else if (sessionAction.action === 'create') {
          if (active) { closeStreamSession(active); memoryState.activeStreams.delete(key); active = null; }
        }
        // identity_pending: conservar sesión actual sin fragmentar; esperar más evidencia

        const effectiveStreamId = active ? active.id : (sessionAction.session ? `twitch:${slug}:${sessionAction.session.id}` : `twitch:${slug}:fallback_${now}`);
        const actualBroadcastId = active ? active.broadcastId : (sessionAction.session ? sessionAction.session.broadcastId : null);

        if (!active) {
          const ignoreReportsVal = ignoreReportsForNewSession ? 1 : 0;
          stmts.createStream.run({
            id: effectiveStreamId,
            broadcast_id: actualBroadcastId,
            platform: 'twitch',
            slug,
            title,
            category,
            started_at: realStartedAt,
            peak_viewers: viewers,
            last_live_at: now,
            ignore_reports: ignoreReportsVal,
            start_followers: followers,
            end_followers: followers,
            followers_diff: 0
          });
          active = {
            id: effectiveStreamId,
            broadcastId: actualBroadcastId,
            isProvisional: !actualBroadcastId,
            slug,
            platform: 'twitch',
            startedAt: realStartedAt,
            lastLiveAt: now,
            peak: viewers,
            viewers,
            category,
            title,
            avatarUrl,
            startFollowers: followers,
            endFollowers: followers,
            ignoreReports: Boolean(ignoreReportsVal)
          };
          memoryState.activeStreams.set(key, active);
          console.log(`[StreamElevate Colector] ¡TWITCH EN DIRECTO! [Matrícula ${actualBroadcastId || 'Provisional'}]: ${slug} con ${viewers} viewers.`);
          subscribeTwitchChat(slug);
        } else {
          // Continúa la misma emisión
          active.viewers = viewers;
          active.peak = Math.max(active.peak, viewers);
          active.category = category;
          active.title = title;
          if (avatarUrl) active.avatarUrl = avatarUrl;
          active.lastLiveAt = now;
          if (followers != null) {
            active.endFollowers = followers;
          }
          if (active.offlineSince) {
            const gapEnd = now;
            recordCaptureGap(active.id, active.offlineSince, gapEnd, 'reconnect');
            console.log(`[StreamElevate Colector] ¡Twitch reconectado: ${slug}! Hueco de ${Math.round((gapEnd - active.offlineSince)/1000)}s registrado.`);
            delete active.offlineSince;
            delete active.firstOfflineAt;
          }
        }

        if (!active.recentSamples) {
          try {
            active.recentSamples = stmts.getAudienceSamples.all(active.id).map(s => ({ timestamp: s.timestamp, viewers: s.viewers }));
          } catch(e) { active.recentSamples = []; }
        }
        const lastSample = active.recentSamples[active.recentSamples.length - 1];
        if (!lastSample || lastSample.viewers !== viewers || (now - lastSample.timestamp >= 20000)) {
          active.recentSamples.push({ timestamp: now, viewers });
          if (active.recentSamples.length > 50000) active.recentSamples.shift();
          recordAudience(active.id, 'twitch', slug, now, viewers, category, title);
        }
      } else {
        // Respuesta válida confirmando OFFLINE
        if (active) {
          if (!active.offlineSince) {
            active.offlineSince = now;
            active.firstOfflineAt = now;
            console.log(`[StreamElevate Colector] Señal perdida de ${slug} (Twitch) [Matrícula ${active.broadcastId}]. Esperando 5 min por microcorte de red...`);
            continue;
          }

          if (now - active.offlineSince < 300000) {
            continue;
          }

          recordCaptureGap(active.id, active.offlineSince, now, 'stream_ended');
          console.log(`[StreamElevate Colector] Directo Twitch finalizado: ${slug} [Matrícula ${active.broadcastId}] (offline confirmado >5min).`);
          const closed = closeStreamSession(active);
          if (closed) {
            memoryState.activeStreams.delete(key);
          }
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

  // Fast cycle cada 4 segundos para streams en directo (frecuencia óptima en tiempo real)
  fastPollTimer = setInterval(runFastCycle, 4000);

  // Full sweep cada 30 segundos para detectar inicios/apagados de streams
  fullSweepTimer = setInterval(runFullSweep, 30000);
  console.log('[StreamElevate Colector] Cadencia Inteligente Dual iniciada (Fast: 4s en vivo, Full: 30s general)');
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

  // Muestras adaptadas para gráfica si superan 1.500 puntos (para respuesta en <2ms a 60fps)
  const chartSamples = (active?.recentSamples && active.recentSamples.length > 1500)
    ? downsampleSamplesForChart(active.recentSamples, 1500)
    : (active?.recentSamples || []);

  return {
    slug: normSlug,
    platform: normPlatform,
    is_live: Boolean(active && !active.offlineSince),
    is_reconnecting: Boolean(active && active.offlineSince),
    stream_id: active ? active.id : null,
    broadcast_id: active ? active.broadcastId : null,
    current_viewers: active ? active.viewers : (channel?.current_viewers || 0),
    peak_viewers: active ? active.peak : 0,
    category: active ? active.category : (channel?.current_category || 'N/D'),
    title: active ? active.title : (channel?.current_title || 'N/D'),
    avatar_url: active?.avatarUrl || channel?.avatar_url || null,
    started_at: active ? active.startedAt : null,
    last_live_at: active ? active.lastLiveAt : null,
    uptime_seconds: active ? Math.floor((now - active.startedAt) / 1000) : 0,
    chat_velocity: {
      msgs_per_min: recentChats.length,
      chatters_per_min: uniqueChattersSet.size
    },
    samples: chartSamples,
    total_samples: active?.recentSamples?.length || 0,
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
