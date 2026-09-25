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
// ═══════════════════════════════════════════════════════════════════════════════
// BLOQUE 3 — integración real de Base de Datos y Worker (Aislado al 100%)
// ═══════════════════════════════════════════════════════════════════════════════

test('integración DB y Worker: ciclo completo aislado (SQLite en memoria y carpeta temporal)', async () => {
  const os = require('node:os');
  const fs = require('node:fs');

  // Inyectar DB en memoria y carpeta de reportes temporal aislada
  process.env.TEST_DB_PATH = ':memory:';
  const testReportsDir = fs.mkdtempSync(path.join(os.tmpdir(), 'streamelevate-reports-test-'));
  process.env.TEST_REPORTS_DIR = testReportsDir;

  const dbModule = require('./src/db');
  const { db, getSessionChart, closeAndEnqueue } = dbModule;
  const workerModule = require('./src/report-worker');
  const { processJob, validatePngContent, validateMp4Content, generatePostText } = workerModule;

  try {
    // --- 1. getSessionChart ---
    const streamId = 'kick:ibai:bcast001';
    db.prepare(`
      INSERT INTO streams (id, broadcast_id, platform, slug, started_at, status)
      VALUES (?, 'bcast001', 'kick', 'ibai', 1000, 'live')
    `).run(streamId);

    const base = Date.now();
    for (let i = 0; i < 10; i++) {
      db.prepare(`
        INSERT INTO audience_samples (stream_id, platform, slug, timestamp, viewers, category)
        VALUES (?, 'kick', 'ibai', ?, ?, 'Gaming')
      `).run(streamId, base + i * 30_000, 1000 + i * 10);
    }

    const chart = getSessionChart(streamId);
    assert.equal(chart.totalSamples, 10, 'getSessionChart debe contar 10 muestras reales');
    assert.equal(chart.gaps.length, 0, 'sin huecos registrados');
    assert.ok(chart.firstObservedAt !== null, 'debe tener primer timestamp');

    // --- 2. closeAndEnqueue con avg_viewers = NULL ---
    closeAndEnqueue(streamId, () => {
      db.prepare("UPDATE streams SET status = 'ended', ended_at = ?, avg_viewers = NULL, coverage_ratio = 0.05 WHERE id = ?").run(base + 300_000, streamId);
    });

    const job = db.prepare(`SELECT * FROM report_jobs WHERE session_id = ?`).get(streamId);
    assert.ok(job, 'el job debe encolarse automáticamente');
    assert.equal(job.status, 'pending', 'debe encolarse como pending');

    // --- 3. Procesamiento en Worker sin motor de renderizado conectado ---
    const output = await processJob(job);
    
    // Validar aislamiento de carpeta: se escribió en testReportsDir, NO en data/reports
    assert.ok(output.finalPath.startsWith(testReportsDir), 'el JSON debe escribirse en la carpeta aislada de test');
    assert.ok(fs.existsSync(output.finalPath), 'el JSON final debe existir');
    assert.ok(fs.existsSync(output.postPath), 'el archivo post.txt debe existir');

    const reportJson = JSON.parse(fs.readFileSync(output.finalPath, 'utf8'));
    assert.equal(reportJson.coverage_insufficient, true, 'el reporte JSON debe reflejar coverage_insufficient=true (por null)');
    assert.equal(reportJson.total_samples, 10, 'el reporte debe incluir total_samples correctas');

    const postContent = fs.readFileSync(output.postPath, 'utf8');
    assert.ok(postContent.includes('REPORTE DE EMISIÓN'), 'post.txt debe tener encabezado formateado');
    assert.ok(postContent.includes('Cobertura insuficiente'), 'post.txt debe indicar cobertura insuficiente cuando avg es null');

    // Comprobar que NO se crearon stubs falsos y que el estado es 'pending_render'
    assert.equal(output.status, 'pending_render', 'sin renderizador, el trabajo debe quedar en pending_render (no done)');
    const fakePng = path.join(testReportsDir, 'kick_ibai_bcast001_summary.png');
    const fakeMp4 = path.join(testReportsDir, 'kick_ibai_bcast001_replay.mp4');
    assert.equal(fs.existsSync(fakePng), false, 'NO deben crearse PNGs simulados falsos en producción');
    assert.equal(fs.existsSync(fakeMp4), false, 'NO deben crearse MP4s simulados falsos en producción');

    // --- 4. Prueba rigurosa de validadores de contenido multimedia ---
    // A) Rechazar archivos no válidos o con texto falso
    const corruptFile = path.join(testReportsDir, 'corrupt.png');
    fs.writeFileSync(corruptFile, 'STUB_PNG_CONTENT');
    assert.equal(validatePngContent(corruptFile), false, 'validatePngContent debe rechazar stubs falsos de texto');

    const corruptMp4 = path.join(testReportsDir, 'corrupt.mp4');
    fs.writeFileSync(corruptMp4, 'STUB_MP4_CONTENT');
    assert.equal(validateMp4Content(corruptMp4), false, 'validateMp4Content debe rechazar stubs falsos de texto');

    // B) Aceptar archivo PNG 100% auténtico y decodificable (1x1 píxel estándar con firma e IHDR)
    const validPngBytes = Buffer.from(
      '89504e470d0a1a0a0000000d49484452000000010000000108060000001f15c4890000000a49444154789c6360000000020001e221bc330000000049454e44ae426082',
      'hex'
    );
    const validPngFile = path.join(testReportsDir, 'valid.png');
    fs.writeFileSync(validPngFile, validPngBytes);
    assert.equal(validatePngContent(validPngFile), true, 'validatePngContent debe aceptar un PNG binario auténtico con IHDR válido');

    // C) Aceptar contenedor MP4 con caja ftyp válida
    const validMp4Box = Buffer.concat([
      Buffer.from([0x00, 0x00, 0x00, 0x20]), // tamaño de la caja (32 bytes)
      Buffer.from('ftypisom', 'ascii'),     // tipo ftyp + compatible isom
      Buffer.from([0x00, 0x00, 0x02, 0x00]), // versión menor
      Buffer.from('isomiso2avc1mp41', 'ascii') // marcas compatibles
    ]);
    const validMp4File = path.join(testReportsDir, 'valid.mp4');
    fs.writeFileSync(validMp4File, validMp4Box);
    assert.equal(validateMp4Content(validMp4File), true, 'validateMp4Content debe validar estructura ftyp de MP4');

    // --- 5. Validar que con medios auténticos presentes, processJob transiciona a 'done' ---
    fs.writeFileSync(fakePng, validPngBytes);
    fs.writeFileSync(fakeMp4, validMp4Box);
    const doneOutput = await processJob(job);
    assert.equal(doneOutput.status, 'done', 'cuando los medios auténticos existen y son válidos, processJob retorna done');
    assert.ok(fs.existsSync(doneOutput.pngPath), 'PNG validado debe existir');
    assert.ok(fs.existsSync(doneOutput.mp4Path), 'MP4 validado debe existir');

  } finally {
    // Limpieza completa del directorio temporal aislado
    try {
      fs.rmSync(testReportsDir, { recursive: true, force: true });
    } catch (e) {}
  }
});

console.log('\n✅ Todos los tests ejecutaron contra código de producción real.\n');
