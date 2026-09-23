const path = require('path');
const fs = require('fs');
const Database = require('better-sqlite3');

const dataDir = path.join(__dirname, '..', 'data');
if (!fs.existsSync(dataDir)) {
  fs.mkdirSync(dataDir, { recursive: true });
}

const dbPath = path.join(dataDir, 'streamelevate.sqlite');
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
      platform TEXT NOT NULL,
      slug TEXT NOT NULL,
      title TEXT,
      category TEXT,
      started_at INTEGER NOT NULL,
      ended_at INTEGER,
      peak_viewers INTEGER DEFAULT 0,
      avg_viewers REAL DEFAULT 0,
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

  getActiveStream: db.prepare(`
    SELECT * FROM streams WHERE platform = ? AND slug = ? AND status = 'live' ORDER BY started_at DESC LIMIT 1
  `),

  createStream: db.prepare(`
    INSERT INTO streams (id, platform, slug, title, category, started_at, peak_viewers, status)
    VALUES (@id, @platform, @slug, @title, @category, @started_at, @peak_viewers, 'live')
    ON CONFLICT(id) DO UPDATE SET
      title = coalesce(excluded.title, streams.title),
      category = coalesce(excluded.category, streams.category),
      peak_viewers = max(streams.peak_viewers, excluded.peak_viewers),
      status = 'live'
  `),

  updateStreamStats: db.prepare(`
    UPDATE streams SET
      title = coalesce(@title, title),
      category = coalesce(@category, category),
      peak_viewers = max(peak_viewers, @viewers)
    WHERE id = @id
  `),

  closeStream: db.prepare(`
    UPDATE streams SET
      ended_at = @ended_at,
      status = 'ended',
      avg_viewers = @avg_viewers,
      total_messages = @total_messages,
      unique_chatters = @unique_chatters
    WHERE id = @id
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
  `)
};

module.exports = {
  db,
  stmts,
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
      viewers
    });
  },

  recordChat(msg) {
    stmts.insertChat.run(msg);
  },

  getStreamDetails(streamId) {
    const stream = db.prepare(`SELECT * FROM streams WHERE id = ?`).get(streamId);
    if (!stream) return null;
    const samples = stmts.getAudienceSamples.all(streamId);
    const chat = stmts.getChatStats.get(streamId);
    const topChatters = stmts.getTopChatters.all(streamId);
    const chatTimeline = stmts.getChatPerMinute.all(streamId);
    return {
      stream,
      samples,
      chat,
      topChatters,
      chatTimeline
    };
  }
};
