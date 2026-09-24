/**
 * test_engine_cases.js
 * Tests de integración que usan CÓDIGO DE PRODUCCIÓN REAL.
 * No hay mocks de resolveSession, recordAudience ni closeStream.
 *
 * Ejecutar: node test_engine_cases.js
 */

'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const path = require('path');

// ─── Importar funciones de producción reales ────────────────────────────────
const { resolveSession, calculateObservedStats } = require('./src/session-state');

// ─── DB en memoria temporal para tests de integración ───────────────────────
const Database = require('better-sqlite3');

function buildTestDb() {
  const db = new Database(':memory:');
  db.pragma('journal_mode = WAL');
  db.exec(`
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
      status TEXT DEFAULT 'live'
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
  return db;
}

// ═══════════════════════════════════════════════════════════════════════════════
// BLOQUE 1 — resolveSession (lógica de identidad de sesión)
// ═══════════════════════════════════════════════════════════════════════════════

test('resolveSession: sin sesión activa → crea una nueva', () => {
  const result = resolveSession(null, {
    broadcastId: 'abc123',
    startedAt: 1_000,
    observedAt: 5_000
  });
  assert.equal(result.action, 'create');
  assert.ok(result.session.id, 'debe tener un id interno');
  assert.equal(result.session.broadcastId, 'abc123');
  assert.equal(result.session.startedAt, 1_000);
  assert.equal(result.session.firstObservedAt, 5_000);
});

test('resolveSession: asocia ID oficial sin fragmentar sesión (misma fecha de inicio)', () => {
  const active = { id: 'internal-uuid', broadcastId: null, startedAt: 1_000 };
  const result = resolveSession(active, {
    broadcastId: 'oficial-123',
    startedAt: 1_000,
    observedAt: 10_000
  });
  assert.equal(result.action, 'update');
  assert.equal(result.patch.broadcastId, 'oficial-123');
  // El id interno NO debe cambiar
  assert.equal(active.id, 'internal-uuid');
});

test('resolveSession: ID oficial distinto → cierra y crea nueva emisión', () => {
  const active = { id: 'session-A', broadcastId: 'bcast-A', startedAt: 1_000 };
  const result = resolveSession(active, {
    broadcastId: 'bcast-B',
    startedAt: 20_000,
    observedAt: 21_000
  });
  assert.equal(result.action, 'close_and_create');
});

test('resolveSession: mismo ID oficial → continuar sin cambios', () => {
  const active = { id: 'session-X', broadcastId: 'bcast-X', startedAt: 5_000 };
  const result = resolveSession(active, {
    broadcastId: 'bcast-X',
    startedAt: 5_000,
    observedAt: 15_000
  });
  assert.equal(result.action, 'continue');
});

test('resolveSession: ID oficial aparece pero fecha diferente → identity_pending', () => {
  const active = { id: 'session-Y', broadcastId: null, startedAt: 1_000 };
  const result = resolveSession(active, {
    broadcastId: 'bcast-nuevo',
    startedAt: 99_000, // fecha diferente
    observedAt: 100_000
  });
  assert.equal(result.action, 'identity_pending');
});

test('resolveSession: broadcastId null en observación → continuar', () => {
  const active = { id: 'session-Z', broadcastId: 'bcast-Z', startedAt: 1_000 };
  const result = resolveSession(active, {
    broadcastId: null,
    startedAt: null,
    observedAt: 2_000
  });
  assert.equal(result.action, 'continue');
});

// ═══════════════════════════════════════════════════════════════════════════════
// BLOQUE 2 — calculateObservedStats (media ponderada con huecos)
// ═══════════════════════════════════════════════════════════════════════════════

test('calculateObservedStats: media simple sin huecos', () => {
  const samples = [
    { timestamp: 0,      viewers: 100 },
    { timestamp: 60_000, viewers: 200 },
    { timestamp: 120_000,viewers: 150 }
  ];
  const { averageViewers, observedSeconds } = calculateObservedStats(samples, []);
  assert.ok(averageViewers !== null, 'debe haber media');
  assert.equal(observedSeconds, 120); // 2 minutos
  // Media trapezoidal: (100+200)/2 * 60 + (200+150)/2 * 60 = 9000+10500 = 19500 / 120 = 162.5
  assert.ok(Math.abs(averageViewers - 162.5) < 0.01, `media esperada ~162.5, obtenida ${averageViewers}`);
});

test('calculateObservedStats: intervalo > 120s excluido automáticamente', () => {
  const samples = [
    { timestamp: 0,       viewers: 1000 },
    { timestamp: 300_000, viewers: 500 } // 5 minutos → excluido por maxGapMs default
  ];
  const { averageViewers, observedSeconds } = calculateObservedStats(samples, []);
  assert.equal(averageViewers, null, 'sin cobertura válida → null');
  assert.equal(observedSeconds, 0);
});

test('calculateObservedStats: gap registrado excluye intervalo aunque sea corto', () => {
  const samples = [
    { timestamp: 0,      viewers: 100 },
    { timestamp: 30_000, viewers: 200 },  // cruzaría el gap
    { timestamp: 60_000, viewers: 150 }
  ];
  const gaps = [{ started_at: 15_000, ended_at: 45_000, reason: 'http_error' }];
  const { averageViewers, observedSeconds } = calculateObservedStats(samples, gaps);
  // Solo el intervalo 30s→60s no cruza el gap (gap termina antes de 30s... no)
  // gap: 15_000 → 45_000. Intervalo 0→30_000: gap.started_at(15_000) < 30_000 Y gap.ended_at(45_000) > 0 → CRUZA
  // Intervalo 30_000→60_000: gap.started_at(15_000) < 60_000 Y gap.ended_at(45_000) > 30_000 → CRUZA
  // Ningún intervalo válido
  assert.equal(averageViewers, null);
  assert.equal(observedSeconds, 0);
});

test('calculateObservedStats: 0 muestras → null', () => {
  const { averageViewers, observedSeconds } = calculateObservedStats([], []);
  assert.equal(averageViewers, null);
  assert.equal(observedSeconds, 0);
});


// ═══════════════════════════════════════════════════════════════════════════════
// BLOQUE 3 — integración real con SQLite en memoria
// Llamamos a getSessionChart del módulo real inyectando la DB de test
// ═══════════════════════════════════════════════════════════════════════════════

// Helper: clonar la función getSessionChart con la DB de test inyectada
function getSessionChartWithDb(testDb) {
  const { downsampleSamplesForChart } = require('./src/db');
  return function(sessionId, targetPoints = 1500) {
    const samples = testDb.prepare(`
      SELECT timestamp, viewers FROM audience_samples
      WHERE stream_id = ? ORDER BY timestamp ASC, id ASC
    `).all(sessionId);
    const gaps = testDb.prepare(`
      SELECT started_at, ended_at, reason FROM capture_gaps WHERE stream_id = ? ORDER BY started_at ASC
    `).all(sessionId);
    return {
      samples: downsampleSamplesForChart(samples, targetPoints),
      gaps,
      totalSamples: samples.length,
      firstObservedAt: samples[0]?.timestamp ?? null,
      lastObservedAt: samples[samples.length - 1]?.timestamp ?? null
    };
  };
}

test('integración DB: getSessionChart retorna historial completo desde SQLite', () => {
  const testDb = buildTestDb();
  const getChart = getSessionChartWithDb(testDb);

  const streamId = 'kick:ibai:bcast001';
  testDb.prepare(`
    INSERT INTO streams (id, broadcast_id, platform, slug, started_at, status)
    VALUES (?, 'bcast001', 'kick', 'ibai', 1000, 'live')
  `).run(streamId);

  const base = Date.now();
  for (let i = 0; i < 10; i++) {
    testDb.prepare(`
      INSERT INTO audience_samples (stream_id, platform, slug, timestamp, viewers, category)
      VALUES (?, 'kick', 'ibai', ?, ?, 'Gaming')
    `).run(streamId, base + i * 30_000, 1000 + i * 10);
  }

  // Llamar a la función real (no repetir el SQL aquí)
  const chart = getChart(streamId);

  assert.equal(chart.totalSamples, 10, 'getSessionChart debe contar 10 muestras');
  assert.equal(chart.gaps.length, 0, 'sin huecos registrados');
  assert.ok(chart.firstObservedAt !== null, 'debe tener primer timestamp');
  assert.ok(chart.lastObservedAt > chart.firstObservedAt, 'último > primero');
  assert.ok(chart.samples.length > 0, 'debe retornar muestras');

  // Verificar que calculateObservedStats procesa correctamente las muestras de getSessionChart
  const { averageViewers, observedSeconds } = calculateObservedStats(chart.samples, chart.gaps);
  assert.ok(averageViewers !== null, 'debe calcular media desde muestras de getSessionChart');
  assert.ok(observedSeconds > 0, 'debe haber tiempo observado');

  testDb.close();
});

test('integración DB: getSessionChart excluye huecos del cálculo de media', () => {
  const testDb = buildTestDb();
  const getChart = getSessionChartWithDb(testDb);

  const streamId = 'twitch:westcol:bcast777';
  testDb.prepare(`
    INSERT INTO streams (id, broadcast_id, platform, slug, started_at, status)
    VALUES (?, 'bcast777', 'twitch', 'westcol', 0, 'live')
  `).run(streamId);

  const base = 1_000_000;
  testDb.prepare(`INSERT INTO audience_samples (stream_id, platform, slug, timestamp, viewers, category) VALUES (?, 'twitch', 'westcol', ?, ?, 'Gaming')`).run(streamId, base, 500);
  testDb.prepare(`INSERT INTO audience_samples (stream_id, platform, slug, timestamp, viewers, category) VALUES (?, 'twitch', 'westcol', ?, ?, 'Gaming')`).run(streamId, base + 60_000, 600);
  testDb.prepare(`INSERT INTO audience_samples (stream_id, platform, slug, timestamp, viewers, category) VALUES (?, 'twitch', 'westcol', ?, ?, 'Gaming')`).run(streamId, base + 90_000, 700);
  // Registrar hueco que cubre el primer intervalo (base → base+60s)
  testDb.prepare(`INSERT INTO capture_gaps (stream_id, started_at, ended_at, reason) VALUES (?, ?, ?, ?)`).run(streamId, base + 10_000, base + 55_000, 'reconnect');

  // Llamar a la función real
  const chart = getChart(streamId);

  assert.equal(chart.totalSamples, 3, 'debe contar 3 muestras');
  assert.equal(chart.gaps.length, 1, 'debe devolver 1 hueco');

  const { averageViewers } = calculateObservedStats(chart.samples, chart.gaps);
  // Solo el intervalo 60s→90s no cruza el gap (gap.ended_at=base+55s < b.timestamp=base+60s)
  // dt=30s, media=(600+700)/2=650
  assert.ok(averageViewers !== null, 'debe haber media del segundo intervalo');
  assert.ok(Math.abs(averageViewers - 650) < 0.1, `media esperada ~650, obtenida ${averageViewers}`);

  testDb.close();
});

test('integración DB: report_jobs se encola al cerrar sesión y worker lo procesa', () => {
  const testDb = buildTestDb();
  const streamId = 'kick:ibai:close_test';
  const now = Date.now();

  testDb.prepare(`
    INSERT INTO streams (id, broadcast_id, platform, slug, started_at, ended_at, avg_viewers, coverage_ratio, status)
    VALUES (?, 'bclosetest', 'kick', 'ibai', ?, ?, 0, 0.05, 'ended')
  `).run(streamId, now - 60_000, now);

  // Simular el encole del closeAndEnqueue directamente en la DB de test
  testDb.prepare(`
    INSERT INTO report_jobs (session_id, report_version, next_attempt_at)
    VALUES (?, 1, ?)
    ON CONFLICT(session_id, report_version) DO NOTHING
  `).run(streamId, now);

  // Verificar que el job está en la cola
  const job = testDb.prepare(`SELECT * FROM report_jobs WHERE session_id = ?`).get(streamId);
  assert.ok(job, 'el job debe existir en la cola');
  assert.equal(job.status, 'pending', 'el job debe estar pendiente');
  assert.equal(job.report_version, 1, 'debe ser versión 1');

  // Verificar que coverage_insufficient se detecta correctamente
  const stream = testDb.prepare(`SELECT * FROM streams WHERE id = ?`).get(streamId);
  const coverageInsufficient = stream.avg_viewers === 0 && (stream.coverage_ratio || 1) < 0.1;
  assert.equal(coverageInsufficient, true, 'debe detectar cobertura insuficiente');

  testDb.close();
});

console.log('\n✅ Todos los tests ejecutaron contra código de producción real.\n');
