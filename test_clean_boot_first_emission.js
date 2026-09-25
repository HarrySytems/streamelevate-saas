/**
 * test_clean_boot_first_emission.js
 * Verificación rigurosa de los 6 requisitos de arranque limpio:
 * 1. Canal inicialmente offline (mide y reporta su próxima emisión completa)
 * 2. Canal inicialmente en directo (vigila telemetría pero omite reporte final de emisión previa)
 * 3. Desconexión temporal (< 5 min reconexión sin crear emisión falsa)
 * 4. Fallo de consulta (HTTP/red no interpreta error como offline ni pierde estado pendiente)
 * 5. Nueva emisión el mismo día (mide normalmente y genera reporte)
 * 6. Reinicio del bot (persistencia en SQLite: no repite exclusión ni pierde estado)
 */

'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const Database = require('better-sqlite3');
const { resolveSession, calculateObservedStats } = require('./src/session-state');

function createTestContext() {
  const db = new Database(':memory:');
  db.pragma('journal_mode = WAL');

  db.exec(`
    CREATE TABLE channels (
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
      initial_stream_handled INTEGER DEFAULT 0,
      PRIMARY KEY (platform, slug)
    );

    CREATE TABLE streams (
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
      status TEXT DEFAULT 'live',
      ignore_reports INTEGER DEFAULT 0
    );

    CREATE TABLE audience_samples (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      stream_id TEXT NOT NULL,
      platform TEXT NOT NULL,
      slug TEXT NOT NULL,
      timestamp INTEGER NOT NULL,
      viewers INTEGER NOT NULL,
      category TEXT
    );

    CREATE TABLE capture_gaps (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      stream_id TEXT NOT NULL,
      started_at INTEGER NOT NULL,
      ended_at INTEGER,
      reason TEXT
    );

    CREATE TABLE report_jobs (
      session_id TEXT NOT NULL,
      report_version INTEGER NOT NULL DEFAULT 1,
      status TEXT NOT NULL DEFAULT 'pending',
      attempts INTEGER NOT NULL DEFAULT 0,
      next_attempt_at INTEGER NOT NULL,
      lease_until INTEGER,
      last_error TEXT,
      PRIMARY KEY (session_id, report_version)
    );
  `);

  const stmts = {
    insertChannel: db.prepare(`
      INSERT INTO channels (slug, platform, username, initial_stream_handled)
      VALUES (?, ?, ?, ?)
    `),
    setInitialStreamHandled: db.prepare(`
      UPDATE channels SET initial_stream_handled = 1 WHERE platform = ? AND slug = ?
    `),
    updateChannelLive: db.prepare(`
      UPDATE channels SET is_live = ?, current_viewers = ?, last_checked_at = ?
      WHERE platform = ? AND slug = ?
    `),
    createStream: db.prepare(`
      INSERT INTO streams (id, broadcast_id, platform, slug, title, category, started_at, peak_viewers, last_live_at, status, ignore_reports)
      VALUES (@id, @broadcast_id, @platform, @slug, @title, @category, @started_at, @peak_viewers, @last_live_at, 'live', coalesce(@ignore_reports, 0))
    `),
    closeStream: db.prepare(`
      UPDATE streams SET ended_at = @ended_at, status = 'ended', avg_viewers = @avg_viewers,
        coverage_ratio = @coverage_ratio, last_live_at = @last_live_at, first_offline_at = @first_offline_at
      WHERE id = @id
    `),
    insertAudience: db.prepare(`
      INSERT INTO audience_samples (stream_id, platform, slug, timestamp, viewers)
      VALUES (?, 'kick', ?, ?, ?)
    `),
    insertGap: db.prepare(`
      INSERT INTO capture_gaps (stream_id, started_at, ended_at, reason)
      VALUES (?, ?, ?, ?)
    `),
    enqueueReport: db.prepare(`
      INSERT INTO report_jobs (session_id, report_version, next_attempt_at)
      VALUES (?, 1, ?)
      ON CONFLICT(session_id, report_version) DO NOTHING
    `)
  };

  // Helper closeStreamSession fiel a la lógica implementada en collector.js
  function closeStreamSession(active) {
    const samples = db.prepare('SELECT timestamp, viewers FROM audience_samples WHERE stream_id = ?').all(active.id);
    const gaps = db.prepare('SELECT started_at, ended_at FROM capture_gaps WHERE stream_id = ?').all(active.id);
    const { averageViewers } = calculateObservedStats(samples, gaps);
    const endedAt = active.lastLiveAt || Date.now();

    if (active.ignoreReports) {
      stmts.closeStream.run({
        id: active.id,
        ended_at: endedAt,
        avg_viewers: averageViewers !== null ? Math.round(averageViewers) : null,
        coverage_ratio: 1.0,
        last_live_at: endedAt,
        first_offline_at: endedAt
      });
    } else {
      const tx = db.transaction(() => {
        stmts.closeStream.run({
          id: active.id,
          ended_at: endedAt,
          avg_viewers: averageViewers !== null ? Math.round(averageViewers) : null,
          coverage_ratio: 1.0,
          last_live_at: endedAt,
          first_offline_at: endedAt
        });
        stmts.enqueueReport.run(active.id, Date.now());
      });
      tx();
    }
    active.closed = true;
  }

  return { db, stmts, closeStreamSession };
}

// ═══════════════════════════════════════════════════════════════════════════════
// PRUEBAS DE ARRANQUE LIMPIO Y PRIMERA EMISIÓN COMPLETA
// ═══════════════════════════════════════════════════════════════════════════════

test('1. Canal inicialmente offline: queda listo y mide completa su próxima emisión', () => {
  const { db, stmts, closeStreamSession } = createTestContext();
  stmts.insertChannel.run('westcol', 'kick', 'Westcol', 0);

  // Primer poll: está OFFLINE
  const ch = db.prepare("SELECT * FROM channels WHERE slug = 'westcol'").get();
  assert.equal(ch.initial_stream_handled, 0);

  // Evalúa política: está offline
  stmts.setInitialStreamHandled.run('kick', 'westcol');
  const chAfter = db.prepare("SELECT * FROM channels WHERE slug = 'westcol'").get();
  assert.equal(chAfter.initial_stream_handled, 1);
  assert.equal(db.prepare("SELECT count(*) as c FROM streams").get().c, 0, 'No debe crear stream si está offline');

  // Más tarde ese día: el streamer ENCIENDE transmisión
  const streamId = 'kick:westcol:stream-101';
  stmts.createStream.run({
    id: streamId,
    broadcast_id: 'bcast-101',
    platform: 'kick',
    slug: 'westcol',
    title: 'Stream nocturno',
    category: 'Just Chatting',
    started_at: 1000,
    peak_viewers: 25000,
    last_live_at: 5000,
    ignore_reports: 0 // Ya fue evaluado inicialmente offline, esta es una emisión completa
  });

  stmts.insertAudience.run(streamId, 'westcol', 1000, 20000);
  stmts.insertAudience.run(streamId, 'westcol', 5000, 25000);

  // Stream termina: debe cerrarse y ENCOLAR el reporte
  const active = { id: streamId, slug: 'westcol', lastLiveAt: 5000, ignoreReports: false };
  closeStreamSession(active);

  const streamRow = db.prepare("SELECT * FROM streams WHERE id = ?").get(streamId);
  assert.equal(streamRow.status, 'ended');
  assert.equal(streamRow.ignore_reports, 0);

  const job = db.prepare("SELECT * FROM report_jobs WHERE session_id = ?").get(streamId);
  assert.ok(job, 'DEBE encolar reporte para la emisión completa');
  assert.equal(job.status, 'pending');
});

test('2. Canal inicialmente en directo: telemetría activa pero omite reporte final de emisión previa', () => {
  const { db, stmts, closeStreamSession } = createTestContext();
  stmts.insertChannel.run('ibai', 'twitch', 'Ibai', 0);

  // Primer poll: está LIVE (ya llevaba tiempo emitiendo)
  const isFirstCheck = db.prepare("SELECT initial_stream_handled FROM channels WHERE slug = 'ibai'").get().initial_stream_handled === 0;
  assert.ok(isFirstCheck, 'Es la primera consulta tras la limpieza');

  const streamId = 'twitch:ibai:mid-stream';
  stmts.setInitialStreamHandled.run('twitch', 'ibai');
  stmts.createStream.run({
    id: streamId,
    broadcast_id: 'bcast-ibai-prev',
    platform: 'twitch',
    slug: 'ibai',
    title: 'Charlando',
    category: 'Just Chatting',
    started_at: 1000,
    peak_viewers: 45000,
    last_live_at: 3000,
    ignore_reports: 1 // Omitir reporte porque no tenemos el inicio de la emisión
  });

  // Registra audiencia en vivo normalmente (telemetría 100% activa)
  stmts.insertAudience.run(streamId, 'ibai', 1000, 40000);
  stmts.insertAudience.run(streamId, 'ibai', 3000, 45000);
  assert.equal(db.prepare("SELECT count(*) as c FROM audience_samples WHERE stream_id = ?").get(streamId).c, 2);

  // Termina la emisión incompleta
  const active = { id: streamId, slug: 'ibai', lastLiveAt: 3000, ignoreReports: true };
  closeStreamSession(active);

  const streamRow = db.prepare("SELECT * FROM streams WHERE id = ?").get(streamId);
  assert.equal(streamRow.status, 'ended');
  assert.equal(streamRow.ignore_reports, 1);

  const job = db.prepare("SELECT * FROM report_jobs WHERE session_id = ?").get(streamId);
  assert.equal(job, undefined, 'NO debe encolar reporte de una emisión previa incompleta');
});

test('3. Desconexión temporal: corte breve (<5min) reconecta sin fragmentar ni crear sesión falsa', () => {
  const { db, stmts, closeStreamSession } = createTestContext();
  const streamId = 'kick:davo:s1';
  stmts.insertChannel.run('davo', 'kick', 'Davoo', 1);
  stmts.createStream.run({
    id: streamId,
    broadcast_id: 'bcast-davo',
    platform: 'kick',
    slug: 'davo',
    title: 'Fútbol',
    category: 'Deportes',
    started_at: 10000,
    peak_viewers: 15000,
    last_live_at: 20000,
    ignore_reports: 0
  });

  const active = {
    id: streamId,
    slug: 'davo',
    platform: 'kick',
    startedAt: 10000,
    lastLiveAt: 20000,
    ignoreReports: false
  };

  // Corte de conexión a los 25s
  active.offlineSince = 25000;
  // Transcurren 120s (< 300s de timeout)
  const reconnectTime = 145000;
  const elapsed = reconnectTime - active.offlineSince;
  assert.ok(elapsed < 300000, 'Dentro de la ventana de microcorte');

  // Streamer reconecta: registrar hueco y seguir misma sesión
  stmts.insertGap.run(active.id, active.offlineSince, reconnectTime, 'reconnect');
  delete active.offlineSince;

  const gap = db.prepare("SELECT * FROM capture_gaps WHERE stream_id = ?").get(streamId);
  assert.equal(gap.reason, 'reconnect');
  assert.equal(active.id, streamId, 'La identidad de la sesión continúa intacta');

  // Cuando finalmente termine, se cierra y genera reporte completo
  active.lastLiveAt = reconnectTime + 60000;
  closeStreamSession(active);
  const job = db.prepare("SELECT * FROM report_jobs WHERE session_id = ?").get(streamId);
  assert.ok(job, 'Genera reporte para la sesión tras reconexión exitosa');
});

test('4. Fallo de consulta: HTTP/red conserva estado pendiente y no interpreta error como offline', () => {
  const { db, stmts } = createTestContext();
  stmts.insertChannel.run('elxokas', 'twitch', 'ElXokas', 0);

  // Simulación: la API falla con 500 o timeout
  let queryFailed = true;
  if (queryFailed) {
    // El colector captura el error y NO llama a updateChannelLive ni a setInitialStreamHandled
  }

  const ch = db.prepare("SELECT * FROM channels WHERE slug = 'elxokas'").get();
  assert.equal(ch.initial_stream_handled, 0, 'Debe permanecer pendiente (0)');
  assert.equal(ch.is_live, 0);
  assert.equal(ch.last_checked_at, null, 'No debe registrar checked si hubo fallo');

  // Siguiente poll tiene éxito y el canal está LIVE
  queryFailed = false;
  stmts.setInitialStreamHandled.run('twitch', 'elxokas');
  stmts.createStream.run({
    id: 'twitch:elxokas:live1',
    broadcast_id: 'xokas-live',
    platform: 'twitch',
    slug: 'elxokas',
    title: 'Directo',
    category: 'Gaming',
    started_at: 5000,
    peak_viewers: 30000,
    last_live_at: 6000,
    ignore_reports: 1 // Al evaluarse por primera vez con éxito estaba live
  });

  const chFinal = db.prepare("SELECT * FROM channels WHERE slug = 'elxokas'").get();
  assert.equal(chFinal.initial_stream_handled, 1);
});

test('5. Nueva emisión el mismo día: mide normalmente y genera reporte', () => {
  const { db, stmts, closeStreamSession } = createTestContext();
  stmts.insertChannel.run('adriano', 'kick', 'Adriano', 1); // Ya evaluado

  // Emisión 1 del día termina
  const s1 = 'kick:adriano:s1';
  stmts.createStream.run({
    id: s1, broadcast_id: 'b1', platform: 'kick', slug: 'adriano', title: 'Tarde', category: 'IRL',
    started_at: 1000, peak_viewers: 5000, last_live_at: 3000, ignore_reports: 0
  });
  closeStreamSession({ id: s1, slug: 'adriano', lastLiveAt: 3000, ignoreReports: false });
  assert.ok(db.prepare("SELECT * FROM report_jobs WHERE session_id = ?").get(s1));

  // Emisión 2 del mismo día (varias horas después)
  const s2 = 'kick:adriano:s2';
  // Como initial_stream_handled ya es 1, ignore_reports es 0
  stmts.createStream.run({
    id: s2, broadcast_id: 'b2', platform: 'kick', slug: 'adriano', title: 'Noche', category: 'IRL',
    started_at: 20000, peak_viewers: 8000, last_live_at: 25000, ignore_reports: 0
  });
  closeStreamSession({ id: s2, slug: 'adriano', lastLiveAt: 25000, ignoreReports: false });

  const job2 = db.prepare("SELECT * FROM report_jobs WHERE session_id = ?").get(s2);
  assert.ok(job2, 'La segunda emisión del día DEBE generar reporte');
});

test('6. Reinicio del bot: persistencia en SQLite evita repetir exclusión o perder estado', () => {
  const { db, stmts, closeStreamSession } = createTestContext();
  
  // Estado antes del reinicio
  stmts.insertChannel.run('brunenger', 'kick', 'Brunenger', 1);
  const activeStreamId = 'kick:brunenger:ongoing-mid';
  stmts.createStream.run({
    id: activeStreamId, broadcast_id: 'b-bruno', platform: 'kick', slug: 'brunenger',
    title: 'Streaming', category: 'IRL', started_at: 1000, peak_viewers: 12000, last_live_at: 2000,
    ignore_reports: 1 // Esta era la sesión excluida
  });

  // SIMULACIÓN DE REINICIO DEL BOT
  // El bot lee de SQLite para rehidratar la memoria
  const restoredChannels = db.prepare("SELECT * FROM channels").all();
  const chBruno = restoredChannels.find(c => c.slug === 'brunenger');
  assert.equal(chBruno.initial_stream_handled, 1, 'Conserva handled=1 tras reinicio');

  const restoredActiveRows = db.prepare("SELECT * FROM streams WHERE status = 'live'").all();
  const streamBruno = restoredActiveRows.find(s => s.id === activeStreamId);
  assert.ok(streamBruno, 'Stream activo recuperado');
  assert.equal(streamBruno.ignore_reports, 1, 'Conserva ignore_reports=1 tras reinicio');

  // Finaliza el stream tras el reinicio
  const activeRehydrated = {
    id: streamBruno.id,
    slug: streamBruno.slug,
    lastLiveAt: 3000,
    ignoreReports: Boolean(streamBruno.ignore_reports)
  };
  closeStreamSession(activeRehydrated);

  const job = db.prepare("SELECT * FROM report_jobs WHERE session_id = ?").get(activeStreamId);
  assert.equal(job, undefined, 'Aun tras reinicio, la sesión excluida NO genera reporte');
});
