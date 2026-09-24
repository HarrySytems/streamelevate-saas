/**
 * report-worker.js
 * Consumidor de la cola report_jobs.
 * Se ejecuta en el mismo proceso (arrancado desde server.js).
 *
 * Ciclo:
 *  - Cada 30s reclamar trabajos pendientes (status='pending', next_attempt_at <= ahora)
 *  - Procesar: generar resumen JSON + PNG placeholder (conectar ffmpeg/canvas aquí cuando esté listo)
 *  - Marcar 'done' o incrementar 'attempts' y programar reintento con backoff exponencial
 *  - Si attempts >= MAX_ATTEMPTS → marcar 'failed'
 *  - Recuperación automática tras caída: los jobs con lease_until vencido se liberan al arrancar
 */

'use strict';

const path = require('path');
const fs = require('fs');
const { db } = require('./db');

const MAX_ATTEMPTS = 5;
const LEASE_DURATION_MS = 2 * 60 * 1000; // 2 minutos de reserva por intento
const POLL_INTERVAL_MS = 30_000;

const outputDir = path.join(__dirname, '..', 'data', 'reports');
if (!fs.existsSync(outputDir)) fs.mkdirSync(outputDir, { recursive: true });

// Al arrancar: liberar jobs con lease vencida (caída del proceso anterior)
function releaseExpiredLeases() {
  try {
    const now = Date.now();
    const result = db.prepare(`
      UPDATE report_jobs
      SET status = 'pending', lease_until = NULL, next_attempt_at = ?
      WHERE status = 'processing' AND lease_until IS NOT NULL AND lease_until < ?
    `).run(now, now);
    if (result.changes > 0) {
      console.log(`[ReportWorker] ${result.changes} job(s) con lease vencida liberados para reintento.`);
    }
  } catch (e) {
    console.warn('[ReportWorker] Error liberando leases vencidas:', e.message);
  }
}

// Reclamar atómicamente un job pendiente
function claimNextJob() {
  const now = Date.now();
  const job = db.prepare(`
    SELECT session_id, report_version, attempts FROM report_jobs
    WHERE status = 'pending' AND next_attempt_at <= ?
    ORDER BY next_attempt_at ASC
    LIMIT 1
  `).get(now);

  if (!job) return null;

  const leaseUntil = now + LEASE_DURATION_MS;
  const updated = db.prepare(`
    UPDATE report_jobs
    SET status = 'processing', lease_until = ?
    WHERE session_id = ? AND report_version = ? AND status = 'pending'
  `).run(leaseUntil, job.session_id, job.report_version);

  // Si otro proceso reclamó el mismo job primero, omitir
  if (updated.changes === 0) return null;
  return job;
}

// Generar el reporte para una sesión finalizada
async function processJob(job) {
  const { session_id } = job;

  // Leer datos finales de la sesión desde SQLite
  const stream = db.prepare(`SELECT * FROM streams WHERE id = ?`).get(session_id);
  if (!stream) {
    throw new Error(`Sesión ${session_id} no encontrada en DB`);
  }

  const samples = db.prepare(`
    SELECT timestamp, viewers FROM audience_samples
    WHERE stream_id = ? ORDER BY timestamp ASC
  `).all(session_id);

  const gaps = db.prepare(`
    SELECT started_at, ended_at, reason FROM capture_gaps WHERE stream_id = ?
  `).all(session_id);

  const chat = db.prepare(`
    SELECT COUNT(*) AS total_messages, COUNT(DISTINCT sender_id) AS unique_chatters
    FROM chat_messages WHERE stream_id = ?
  `).get(session_id);

  // Construir resumen JSON final
  const summary = {
    session_id,
    platform: stream.platform,
    slug: stream.slug,
    title: stream.title,
    category: stream.category,
    started_at: stream.started_at,
    ended_at: stream.ended_at,
    duration_seconds: stream.ended_at ? Math.round((stream.ended_at - stream.started_at) / 1000) : null,
    peak_viewers: stream.peak_viewers,
    avg_viewers: stream.avg_viewers,
    coverage_ratio: stream.coverage_ratio,
    coverage_insufficient: stream.avg_viewers === null,
    total_samples: samples.length,
    gaps_count: gaps.length,
    total_messages: chat?.total_messages || 0,
    unique_chatters: chat?.unique_chatters || 0,
    generated_at: Date.now()
  };

  const baseFileName = session_id.replace(/[:/]/g, '_');
  const tmpPath = path.join(outputDir, `${baseFileName}_report.tmp.json`);
  const finalPath = path.join(outputDir, `${baseFileName}_report.json`);
  const pngPath = path.join(outputDir, `${baseFileName}_summary.png`);
  const mp4Path = path.join(outputDir, `${baseFileName}_replay.mp4`);

  // Escribir resumen JSON (archivo final — validar antes de mover)
  fs.writeFileSync(tmpPath, JSON.stringify(summary, null, 2), 'utf8');
  JSON.parse(fs.readFileSync(tmpPath, 'utf8'));
  fs.renameSync(tmpPath, finalPath);

  // TODO: Conectar generación de PNG y MP4 reales cuando canvas/ffmpeg estén integrados
  // Por ahora, generar stubs para que el worker valide el flujo completo
  fs.writeFileSync(pngPath, 'STUB_PNG_CONTENT');
  fs.writeFileSync(mp4Path, 'STUB_MP4_CONTENT');

  // Validar TODOS los archivos requeridos antes de marcar como completado
  if (!fs.existsSync(finalPath) || !fs.existsSync(pngPath) || !fs.existsSync(mp4Path)) {
    throw new Error('Validación fallida: Faltan archivos generados (JSON, PNG o MP4).');
  }

  console.log(`[ReportWorker] ✅ Reporte y recursos generados: ${baseFileName}`);
  console.log(`[ReportWorker]    ${stream.slug} | ${stream.platform} | pico: ${stream.peak_viewers} | media: ${stream.avg_viewers ?? 'N/D'} | cobertura: ${(stream.coverage_ratio * 100).toFixed(1)}%`);

  return { finalPath, pngPath, mp4Path };
}

// Marcar job completado
function markDone(job, outputPath) {
  db.prepare(`
    UPDATE report_jobs SET status = 'done', lease_until = NULL, last_error = NULL
    WHERE session_id = ? AND report_version = ?
  `).run(job.session_id, job.report_version);
}

// Marcar job fallido (con reintento o permanente)
function markFailed(job, err) {
  const attempts = (job.attempts || 0) + 1;
  if (attempts >= MAX_ATTEMPTS) {
    db.prepare(`
      UPDATE report_jobs
      SET status = 'failed', attempts = ?, lease_until = NULL, last_error = ?
      WHERE session_id = ? AND report_version = ?
    `).run(attempts, String(err.message).slice(0, 500), job.session_id, job.report_version);
    console.error(`[ReportWorker] ❌ Job permanentemente fallido (${attempts} intentos): ${job.session_id} — ${err.message}`);
  } else {
    // Backoff exponencial: 1min, 2min, 4min, 8min...
    const backoffMs = Math.min(60_000 * Math.pow(2, attempts - 1), 30 * 60_000);
    const nextAttemptAt = Date.now() + backoffMs;
    db.prepare(`
      UPDATE report_jobs
      SET status = 'pending', attempts = ?, lease_until = NULL, next_attempt_at = ?, last_error = ?
      WHERE session_id = ? AND report_version = ?
    `).run(attempts, nextAttemptAt, String(err.message).slice(0, 500), job.session_id, job.report_version);
    console.warn(`[ReportWorker] ⚠️ Reintento ${attempts}/${MAX_ATTEMPTS} en ${Math.round(backoffMs/1000)}s: ${job.session_id}`);
  }
}

// Ciclo de procesamiento
async function runWorkerCycle() {
  // Liberar leases vencidas en cada ciclo (recuperación o jobs largos muertos)
  releaseExpiredLeases();

  let job = claimNextJob();
  while (job) {
    try {
      const output = await processJob(job);
      markDone(job, output);
    } catch (err) {
      console.error(`[ReportWorker] Error procesando job ${job.session_id}:`, err.message);
      markFailed(job, err);
    }
    // Intentar el siguiente job pendiente del mismo ciclo
    job = claimNextJob();
  }
}

let workerTimer = null;

function startReportWorker() {
  // Liberar leases vencidas al arrancar (recuperación tras caída)
  releaseExpiredLeases();

  // Primer ciclo inmediato
  runWorkerCycle().catch(e => console.error('[ReportWorker] Error en ciclo inicial:', e.message));

  // Ciclos periódicos
  workerTimer = setInterval(() => {
    runWorkerCycle().catch(e => console.error('[ReportWorker] Error en ciclo periódico:', e.message));
  }, POLL_INTERVAL_MS);

  console.log('[ReportWorker] Iniciado — revisando cola cada 30s.');
}

function stopReportWorker() {
  if (workerTimer) clearInterval(workerTimer);
}

module.exports = { startReportWorker, stopReportWorker, processJob };
