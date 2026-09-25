const path = require('path');
const fs = require('fs');
const Database = require('better-sqlite3');

const dataDir = path.join(__dirname, '..', 'data');
if (!fs.existsSync(dataDir)) {
  fs.mkdirSync(dataDir, { recursive: true });
}

const dbPath = process.env.TEST_DB_PATH || path.join(dataDir, 'streamelevate.sqlite');
const db = new Database(dbPath);

// Fast WAL mode for concurrency and durability
db.pragma('journal_mode = WAL');
db.pragma('synchronous = NORMAL');

function initDb() {
  db.exec(`
    CREATE TABLE IF NOT EXISTS channels (
      slug TEXT NOT NULL,
      platform TEXT NOT NULL,
      username TEXT,
      channel_id INTEGER,
      chatroom_id INTEGER,
      is_live INTEGER DEFAULT 0,
      current_viewers INTEGER DEFAULT 0,
      current_category TEXT,
      current_title TEXT,
      last_checked_at INTEGER,
      PRIMARY KEY (platform, slug)
    );

    CREATE TABLE IF NOT EXISTS streams (
      id TEXT PRIMARY KEY,
      broadcast_id TEXT,
      platform TEXT NOT NULL,
      slug TEXT NOT NULL,
      title TEXT,
      category TEXT,
      started_at INTEGER NOT NULL,
      ended_at INTEGER,
      last_live_at INTEGER,
      first_offline_at INTEGER,
      peak_viewers INTEGER DEFAULT 0,
      avg_viewers REAL DEFAULT 0,
      coverage_ratio REAL DEFAULT 1.0,
      total_messages INTEGER DEFAULT 0,
      unique_chatters INTEGER DEFAULT 0,
      status TEXT DEFAULT 'live'
    );

    CREATE TABLE IF NOT EXISTS audience_samples (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      stream_id TEXT NOT NULL,
      platform TEXT NOT NULL,
      slug TEXT NOT NULL,
      timestamp INTEGER NOT NULL,
      viewers INTEGER NOT NULL,
      category TEXT
    );

    CREATE TABLE IF NOT EXISTS chat_messages (
      message_id TEXT PRIMARY KEY,
      stream_id TEXT,
      platform TEXT NOT NULL,
      slug TEXT NOT NULL,
      sender_id TEXT,
      sender_username TEXT,
      content TEXT,
      timestamp INTEGER NOT NULL
    );

    CREATE INDEX IF NOT EXISTS idx_audience_stream ON audience_samples(stream_id, timestamp);
    CREATE INDEX IF NOT EXISTS idx_chat_stream ON chat_messages(stream_id, timestamp);
    CREATE INDEX IF NOT EXISTS idx_chat_sender ON chat_messages(stream_id, sender_id);

    CREATE TABLE IF NOT EXISTS capture_gaps (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      stream_id TEXT NOT NULL,
      started_at INTEGER NOT NULL,
      ended_at INTEGER,
      reason TEXT
    );

    CREATE TABLE IF NOT EXISTS report_jobs (
      session_id TEXT NOT NULL,
      report_version INTEGER NOT NULL DEFAULT 1,
      status TEXT NOT NULL DEFAULT 'pending',
      attempts INTEGER NOT NULL DEFAULT 0,
      next_attempt_at INTEGER NOT NULL,
      lease_until INTEGER,
      last_error TEXT,
      PRIMARY KEY (session_id, report_version)
    );

    CREATE INDEX IF NOT EXISTS idx_capture_gaps_stream ON capture_gaps(stream_id, started_at);
    CREATE INDEX IF NOT EXISTS idx_report_jobs_status ON report_jobs(status, next_attempt_at);
  `);

  // Migration check: ensure composite PRIMARY KEY (platform, slug)
  try {
    const info = db.pragma('table_info(channels)');
    const pkCols = info.filter(c => c.pk > 0);
    if (pkCols.length === 1 && pkCols[0].name === 'slug') {
      db.exec(`
        CREATE TABLE channels_migration (
          slug TEXT NOT NULL,
          platform TEXT NOT NULL,
          username TEXT,
          channel_id INTEGER,
          chatroom_id INTEGER,
          is_live INTEGER DEFAULT 0,
          current_viewers INTEGER DEFAULT 0,
          current_category TEXT,
          current_title TEXT,
          last_checked_at INTEGER,
          PRIMARY KEY (platform, slug)
        );
        INSERT OR IGNORE INTO channels_migration SELECT * FROM channels;
        DROP TABLE channels;
        ALTER TABLE channels_migration RENAME TO channels;
      `);
    }
  } catch (e) {
    console.warn('[DB] Aviso migracion canales:', e.message);
  }
  try {
    db.exec(`ALTER TABLE streams ADD COLUMN broadcast_id TEXT;`);
  } catch (e) {}
  try {
    db.exec(`ALTER TABLE streams ADD COLUMN last_live_at INTEGER;`);
  } catch (e) {}
  try {
    db.exec(`ALTER TABLE streams ADD COLUMN first_offline_at INTEGER;`);
  } catch (e) {}
  try {
    db.exec(`ALTER TABLE streams ADD COLUMN coverage_ratio REAL DEFAULT 1.0;`);
  } catch (e) {}
  try {
    db.exec(`ALTER TABLE streams ADD COLUMN ignore_reports INTEGER DEFAULT 0;`);
  } catch (e) {}
  try {
    db.exec(`ALTER TABLE streams ADD COLUMN start_followers INTEGER;`);
  } catch (e) {}
  try {
    db.exec(`ALTER TABLE streams ADD COLUMN end_followers INTEGER;`);
  } catch (e) {}
  try {
    db.exec(`ALTER TABLE streams ADD COLUMN followers_diff INTEGER;`);
  } catch (e) {}
  try {
    db.exec(`ALTER TABLE channels ADD COLUMN initial_stream_handled INTEGER DEFAULT 0;`);
  } catch (e) {}
  try {
    db.exec(`CREATE INDEX IF NOT EXISTS idx_audience_samples_stream ON audience_samples(stream_id, timestamp);`);
  } catch (e) {}
  try {
    db.exec(`
      CREATE TABLE IF NOT EXISTS feed_posts (
        id TEXT PRIMARY KEY,
        session_id TEXT NOT NULL,
        platform TEXT NOT NULL,
        slug TEXT NOT NULL,
        streamer_name TEXT,
        avatar_url TEXT,
        post_text TEXT NOT NULL,
        media_type TEXT DEFAULT 'video',
        media_url TEXT,
        thumbnail_url TEXT,
        duration_seconds INTEGER,
        peak_viewers INTEGER,
        avg_viewers INTEGER,
        start_followers INTEGER,
        end_followers INTEGER,
        followers_diff INTEGER,
        likes_count INTEGER DEFAULT 142,
        reposts_count INTEGER DEFAULT 19,
        replies_count INTEGER DEFAULT 7,
        views_count INTEGER DEFAULT 2500,
        created_at INTEGER NOT NULL
      );
      CREATE INDEX IF NOT EXISTS idx_feed_posts_created ON feed_posts(created_at DESC);
    `);
  } catch (e) {}
}

initDb();

// Prepared statements for maximum performance
const stmts = {
  upsertChannel: db.prepare(`
    INSERT INTO channels (slug, platform, username, channel_id, chatroom_id)
    VALUES (@slug, @platform, @username, @channel_id, @chatroom_id)
    ON CONFLICT(platform, slug) DO UPDATE SET
      channel_id = coalesce(excluded.channel_id, channels.channel_id),
      chatroom_id = coalesce(excluded.chatroom_id, channels.chatroom_id),
      username = coalesce(excluded.username, channels.username)
  `),
  
  updateChannelLive: db.prepare(`
    UPDATE channels SET
      is_live = @is_live,
      current_viewers = @current_viewers,
      current_category = @current_category,
      current_title = @current_title,
      last_checked_at = @last_checked_at
    WHERE platform = @platform AND slug = @slug
  `),

  getAllChannels: db.prepare(`SELECT * FROM channels ORDER BY platform, slug`),
  
  getChannel: db.prepare(`SELECT * FROM channels WHERE platform = ? AND slug = ?`),

  getChannelBySlug: db.prepare(`SELECT * FROM channels WHERE slug = ? LIMIT 1`),

  setInitialStreamHandled: db.prepare(`
    UPDATE channels SET initial_stream_handled = 1 WHERE platform = ? AND slug = ?
  `),

  getChannelHandled: db.prepare(`
    SELECT initial_stream_handled FROM channels WHERE platform = ? AND slug = ?
  `),

  getActiveStream: db.prepare(`
    SELECT * FROM streams WHERE platform = ? AND slug = ? AND status = 'live' ORDER BY started_at DESC LIMIT 1
  `),

  createStream: db.prepare(`
    INSERT INTO streams (id, broadcast_id, platform, slug, title, category, started_at, peak_viewers, last_live_at, status, ignore_reports, start_followers, end_followers, followers_diff)
    VALUES (@id, @broadcast_id, @platform, @slug, @title, @category, @started_at, @peak_viewers, @last_live_at, 'live', coalesce(@ignore_reports, 0), @start_followers, @end_followers, coalesce(@followers_diff, 0))
    ON CONFLICT(id) DO UPDATE SET
      broadcast_id = coalesce(excluded.broadcast_id, streams.broadcast_id),
      title = coalesce(excluded.title, streams.title),
      category = coalesce(excluded.category, streams.category),
      peak_viewers = max(streams.peak_viewers, excluded.peak_viewers),
      last_live_at = max(coalesce(streams.last_live_at, 0), coalesce(excluded.last_live_at, 0)),
      end_followers = coalesce(excluded.end_followers, streams.end_followers),
      status = 'live'
  `),

  updateStreamStats: db.prepare(`
    UPDATE streams SET
      title = coalesce(@title, title),
      category = coalesce(@category, category),
      peak_viewers = max(peak_viewers, @viewers),
      last_live_at = max(coalesce(last_live_at, 0), coalesce(@last_live_at, 0))
    WHERE id = @id
  `),

  closeStream: db.prepare(`
    UPDATE streams SET
      ended_at = @ended_at,
      status = 'ended',
      avg_viewers = @avg_viewers,
      coverage_ratio = coalesce(@coverage_ratio, 1.0),
      last_live_at = coalesce(@last_live_at, last_live_at),
      first_offline_at = coalesce(@first_offline_at, first_offline_at),
      total_messages = @total_messages,
      unique_chatters = @unique_chatters,
      end_followers = coalesce(@end_followers, end_followers),
      followers_diff = coalesce(@followers_diff, CASE WHEN start_followers IS NOT NULL AND coalesce(@end_followers, end_followers) IS NOT NULL THEN (coalesce(@end_followers, end_followers) - start_followers) ELSE followers_diff END)
    WHERE id = @id
  `),

  insertFeedPost: db.prepare(`
    INSERT INTO feed_posts (
      id, session_id, platform, slug, streamer_name, avatar_url, post_text,
      media_type, media_url, thumbnail_url, duration_seconds, peak_viewers, avg_viewers,
      start_followers, end_followers, followers_diff, likes_count, reposts_count,
      replies_count, views_count, created_at
    ) VALUES (
      @id, @session_id, @platform, @slug, @streamer_name, @avatar_url, @post_text,
      @media_type, @media_url, @thumbnail_url, @duration_seconds, @peak_viewers, @avg_viewers,
      @start_followers, @end_followers, @followers_diff, @likes_count, @reposts_count,
      @replies_count, @views_count, @created_at
    )
  `),

  getRecentFeedPosts: db.prepare(`
    SELECT * FROM feed_posts ORDER BY created_at DESC LIMIT 50
  `),

  insertAudience: db.prepare(`
    INSERT INTO audience_samples (stream_id, platform, slug, timestamp, viewers, category)
    VALUES (@stream_id, @platform, @slug, @timestamp, @viewers, @category)
  `),

  insertChat: db.prepare(`
    INSERT OR IGNORE INTO chat_messages (message_id, stream_id, platform, slug, sender_id, sender_username, content, timestamp)
    VALUES (@message_id, @stream_id, @platform, @slug, @sender_id, @sender_username, @content, @timestamp)
  `),

  getAudienceSamples: db.prepare(`
    SELECT timestamp, viewers, category FROM audience_samples WHERE stream_id = ? ORDER BY timestamp ASC
  `),

  getChatStats: db.prepare(`
    SELECT 
      COUNT(*) AS total_messages,
      COUNT(DISTINCT sender_id) AS unique_chatters
    FROM chat_messages WHERE stream_id = ?
  `),

  getTopChatters: db.prepare(`
    SELECT sender_username, COUNT(*) as messages
    FROM chat_messages WHERE stream_id = ?
    GROUP BY sender_id, sender_username
    ORDER BY messages DESC LIMIT 10
  `),

  getChatPerMinute: db.prepare(`
    SELECT 
      (timestamp / 60000) * 60000 as minute_slot,
      COUNT(*) as messages,
      COUNT(DISTINCT sender_id) as chatters
    FROM chat_messages WHERE stream_id = ?
    GROUP BY minute_slot
    ORDER BY minute_slot ASC
  `),

  getRecentStreams: db.prepare(`
    SELECT * FROM streams WHERE slug = ? ORDER BY started_at DESC LIMIT 10
  `),

  insertCaptureGap: db.prepare(`
    INSERT INTO capture_gaps (stream_id, started_at, ended_at, reason)
    VALUES (@stream_id, @started_at, @ended_at, @reason)
  `),

  getGaps: db.prepare(`
    SELECT started_at, ended_at, reason FROM capture_gaps
    WHERE stream_id = ? ORDER BY started_at ASC
  `),

  getPendingJobs: db.prepare(`
    SELECT session_id, report_version, attempts FROM report_jobs
    WHERE status = 'pending' AND next_attempt_at <= ?
    ORDER BY next_attempt_at ASC LIMIT 10
  `)
};

function downsampleSamplesForChart(rawSamples, targetPoints = 1500) {
  if (!rawSamples || rawSamples.length <= targetPoints) {
    return rawSamples || [];
  }
  
  const n = rawSamples.length;
  const result = [];
  result.push(rawSamples[0]); // Conservar inicio exacto siempre

  const bucketCount = Math.floor(targetPoints / 2);
  const bucketSize = (n - 2) / bucketCount;
  
  for (let i = 0; i < bucketCount; i++) {
    const start = Math.floor(1 + i * bucketSize);
    const end = Math.min(n - 1, Math.floor(1 + (i + 1) * bucketSize));
    if (start >= end) continue;

    let minSample = rawSamples[start];
    let maxSample = rawSamples[start];

    for (let j = start + 1; j < end; j++) {
      const s = rawSamples[j];
      if (s.viewers < minSample.viewers) minSample = s;
      if (s.viewers > maxSample.viewers) maxSample = s;
    }

    if (minSample.timestamp <= maxSample.timestamp) {
      if (minSample !== result[result.length - 1]) result.push(minSample);
      if (maxSample !== minSample) result.push(maxSample);
    } else {
      if (maxSample !== result[result.length - 1]) result.push(maxSample);
      if (minSample !== maxSample) result.push(minSample);
    }
  }

  const last = rawSamples[n - 1];
  if (result[result.length - 1] !== last) {
    result.push(last);
  }

  return result;
}

module.exports = {
  db,
  stmts,
  downsampleSamplesForChart,
  seedChannels(list) {
    const insertMany = db.transaction((channels) => {
      for (const ch of channels) {
        const slug = (ch.slug || ch.channel || '').toLowerCase();
        if (!slug) continue;
        stmts.upsertChannel.run({
          slug: slug,
          platform: ch.platform || 'kick',
          username: ch.username || ch.name || ch.channel || ch.slug,
          channel_id: (ch.channel_id || ch.twitch_id) ? String(ch.channel_id || ch.twitch_id) : null,
          chatroom_id: ch.chatroom_id ? String(ch.chatroom_id) : null
        });
      }
    });
    insertMany(list);
  },
  
  recordAudience(streamId, platform, slug, timestamp, viewers, category, title) {
    stmts.insertAudience.run({
      stream_id: streamId,
      platform,
      slug: slug.toLowerCase(),
      timestamp,
      viewers,
      category: category || 'General'
    });
    stmts.updateStreamStats.run({
      id: streamId,
      title,
      category,
      viewers,
      last_live_at: timestamp
    });
  },

  recordChat(msg) {
    stmts.insertChat.run(msg);
  },

  getStreamDetails(streamId, maxChartPoints = 1500) {
    const stream = db.prepare(`SELECT * FROM streams WHERE id = ?`).get(streamId);
    if (!stream) return null;
    const allSamples = stmts.getAudienceSamples.all(streamId);
    const gaps = stmts.getGaps.all(streamId);
    const chat = stmts.getChatStats.get(streamId);
    const topChatters = stmts.getTopChatters.all(streamId);
    const chatTimeline = stmts.getChatPerMinute.all(streamId);

    // Muestras adaptadas para pantalla (conservando picos y valles)
    const chartSamples = downsampleSamplesForChart(allSamples, maxChartPoints);

    return {
      stream,
      samples: chartSamples,
      gaps,
      total_samples_recorded: allSamples.length,
      chat,
      topChatters,
      chatTimeline
    };
  },

  /**
   * getSessionChart — siempre lee de SQLite (no de RAM).
   * Devuelve muestras, huecos, total y rango temporal exacto de la sesión.
   */
  getSessionChart(sessionId, targetPoints = 1500) {
    const samples = db.prepare(`
      SELECT timestamp, viewers
      FROM audience_samples
      WHERE stream_id = ?
      ORDER BY timestamp ASC, id ASC
    `).all(sessionId);

    const gaps = stmts.getGaps.all(sessionId);

    return {
      samples: downsampleSamplesForChart(samples, targetPoints),
      gaps,
      totalSamples: samples.length,
      firstObservedAt: samples[0]?.timestamp ?? null,
      lastObservedAt: samples[samples.length - 1]?.timestamp ?? null
    };
  },

  /**
   * Registra un hueco de captura (perdida de conectividad, error HTTP, etc.)
   */
  recordCaptureGap(streamId, startedAt, endedAt, reason = 'unknown') {
    try {
      stmts.insertCaptureGap.run({
        stream_id: streamId,
        started_at: startedAt,
        ended_at: endedAt,
        reason
      });
    } catch (e) {
      console.warn('[DB] Error registrando hueco de captura:', e.message);
    }
  },

  /**
   * Cierra la sesión y encola el trabajo de generación de reporte en una sola transacción atómica.
   */
  closeAndEnqueue(sessionId, closeFn) {
    const tx = db.transaction(() => {
      closeFn();
      db.prepare(`
        INSERT INTO report_jobs (session_id, report_version, next_attempt_at)
        VALUES (?, 1, ?)
        ON CONFLICT(session_id, report_version) DO NOTHING
      `).run(sessionId, Date.now());
    });
    tx();
  }
};
