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

test('integración: render real, validación completa y recuperación de cola', async () => {
  const fs = require('node:fs'), os = require('node:os');
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'streamelevate-render-test-'));
  process.env.TEST_DB_PATH = ':memory:';
  process.env.TEST_REPORTS_DIR = dir;
  const { db, getSessionChart, closeAndEnqueue } = require('./src/db');
  const { createReportWorker, validatePngContent, validateMp4Content } = require('./src/report-worker');
  const quiet = { log(){}, error(){} };
  try {
    const id = 'kick:qa:session'; const base = 1700000000000;
    db.prepare(`INSERT INTO streams (id,broadcast_id,platform,slug,title,started_at,peak_viewers,status)
      VALUES (?,'qa-id','kick','Prueba aislada','Validación de render',?,1090,'live')`).run(id,base);
    for(let i=0;i<10;i++)db.prepare('INSERT INTO audience_samples (stream_id,platform,slug,timestamp,viewers) VALUES (?,\'kick\',\'qa\',?,?)')
      .run(id,base+i*30000,1000+i*10);
    assert.equal(getSessionChart(id).totalSamples,10);
    closeAndEnqueue(id,()=>db.prepare("UPDATE streams SET status='ended',ended_at=?,avg_viewers=1045,coverage_ratio=1 WHERE id=?").run(base+270000,id));
    db.prepare("UPDATE report_jobs SET status='pending_render' WHERE session_id=?").run(id);
    const worker = createReportWorker({db,outputDir:dir,logger:quiet});
    // Real queue, renderer, Chromium and FFmpeg; no fake media or mocked production logic.
    await worker.runWorkerCycle();
    const job=db.prepare('SELECT * FROM report_jobs WHERE session_id=?').get(id);
    assert.equal(job.status,'done',job.last_error);
    const png=path.join(dir,'kick_qa_session_summary.png'),mp4=path.join(dir,'kick_qa_session_replay.mp4');
    assert.equal(validatePngContent(png,{width:1280,height:720}),true);
    assert.equal(await validateMp4Content(mp4,{width:1280,height:720,frames:480,seconds:8}),true);
    const trace=JSON.parse(fs.readFileSync(path.join(dir,'kick_qa_session_frames.json'),'utf8'));
    assert.equal(trace.length,480);
    assert.ok(trace.every(f=>f.average===1045));
    assert.equal(trace[0].progress,0);
    assert.equal(trace.at(-1).progress,1);
    assert.equal(trace.at(-1).tracerX,trace.at(-1).chartRight);
    const before=fs.statSync(mp4).mtimeMs;
    await worker.runWorkerCycle();
    assert.equal(fs.statSync(mp4).mtimeMs,before,'done jobs must not render twice');

    // Reject header-only and genuinely truncated files, even when magic bytes are correct.
    const badPng=path.join(dir,'bad.png');fs.writeFileSync(badPng,fs.readFileSync(png).subarray(0,24));
    assert.equal(validatePngContent(badPng),false);
    const badMp4=path.join(dir,'bad.mp4');const header=Buffer.alloc(32);header.write('ftyp',4);fs.writeFileSync(badMp4,header);
    assert.equal(await validateMp4Content(badMp4),false);
    fs.writeFileSync(badMp4,fs.readFileSync(mp4).subarray(0,256));
    assert.equal(await validateMp4Content(badMp4),false);
    const old=process.env.FFPROBE_PATH;process.env.FFPROBE_PATH=path.join(dir,'missing-tool');
    try {assert.equal(await validateMp4Content(mp4),false);}finally{if(old)process.env.FFPROBE_PATH=old;else delete process.env.FFPROBE_PATH;}

    // Exercise real lease reclamation and retry code, injecting only the external renderer failure.
    let clock=1000,calls=0;
    db.prepare("UPDATE report_jobs SET status='processing',lease_owner='crashed',lease_until=2000,next_attempt_at=0 WHERE session_id=?").run(id);
    const recovery=createReportWorker({db,outputDir:dir,now:()=>clock,logger:quiet,render:async()=>{calls++;throw Error('encoder unavailable');}});
    await recovery.runWorkerCycle();assert.equal(calls,0,'unexpired reservation must remain owned');
    clock=2001;await recovery.runWorkerCycle();assert.equal(calls,1);
    const retry=db.prepare('SELECT * FROM report_jobs WHERE session_id=?').get(id);
    assert.equal(retry.status,'pending_render');assert.equal(retry.attempts,1);assert.ok(retry.next_attempt_at>clock);
    await recovery.runWorkerCycle();assert.equal(calls,1,'backoff must be respected');
    assert.equal(fs.statSync(mp4).mtimeMs,before,'failed render must not overwrite valid output');
    assert.ok(!fs.readdirSync(dir).some(n=>n.startsWith('.render-')));
  } finally {
    db.close();
    const resolved=path.resolve(dir);
    assert.equal(path.dirname(resolved),path.resolve(os.tmpdir()));
    assert.ok(path.basename(resolved).startsWith('streamelevate-render-test-'));
    fs.rmSync(resolved,{recursive:true,force:true});
  }
});
