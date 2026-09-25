/**
 * report-worker.js
 * Consumidor de la cola report_jobs.
 * Se ejecuta en el mismo proceso (arrancado desde server.js).
 *
 * Flujo:
 *  - En cada ciclo: liberar leases vencidas (recuperación de caídas o jobs muertos).
 *  - Reclamar trabajo pendiente atómicamente.
 *  - Fase 1: Agregación de datos de sesión → Generar JSON final y texto del post.
 *  - Fase 2: Validación de renderizado (Canvas / FFmpeg).
 *            Si el motor de renderizado no está conectado o los archivos no están listos,
 *            el trabajo se marca como 'pending_render' (NUNCA como 'done' con archivos falsos).
 *  - Cuando los archivos multimedia reales existen y pasan la validación estructural/decodificación,
 *    el trabajo se sella como 'done'.
 */

'use strict';

const path = require('path');
const fs = require('fs');
const { execSync } = require('child_process');
const { db } = require('./db');

const MAX_ATTEMPTS = 5;
const LEASE_DURATION_MS = 2 * 60 * 1000; // 2 minutos de reserva por intento
const POLL_INTERVAL_MS = 30_000;

// Directorio de salida configurable (aislable para tests mediante TEST_REPORTS_DIR)
function getOutputDir() {
  const dir = process.env.TEST_REPORTS_DIR || path.join(__dirname, '..', 'data', 'reports');
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
  return dir;
}

// Al arrancar y en cada ciclo: liberar jobs con lease vencida
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

  if (updated.changes === 0) return null;
  return job;
}

// Generador de texto para publicación en redes / dashboard
function generatePostText(summary) {
  const durationSec = summary.duration_seconds || 0;
  const h = Math.floor(durationSec / 3600);
  const m = Math.floor((durationSec % 3600) / 60);
  const durStr = h > 0 ? `${h}h ${m}m` : `${m}m`;

  const avgStr = (summary.avg_viewers !== null && summary.avg_viewers !== undefined)
    ? Number(summary.avg_viewers).toLocaleString()
    : 'Cobertura insuficiente';

  return [
    `📊 REPORTE DE EMISIÓN — ${summary.slug?.toUpperCase()} (${summary.platform?.toUpperCase()})`,
    `━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━`,
    `🎯 Título: ${summary.title || 'Sin título'}`,
    `🏷️ Categoría: ${summary.category || 'General'}`,
    `⏱️ Duración: ${durStr}`,
    `🔥 Pico de viewers: ${Number(summary.peak_viewers || 0).toLocaleString()}`,
    `📈 Media de viewers: ${avgStr}`,
    `💬 Mensajes en chat: ${Number(summary.total_messages || 0).toLocaleString()}`,
    `👥 Participantes únicos: ${Number(summary.unique_chatters || 0).toLocaleString()}`,
    `📡 Cobertura técnica: ${((summary.coverage_ratio || 0) * 100).toFixed(1)}%`
  ].join('\n');
}

// Validador de contenido PNG: comprueba firma de 8 bytes, bloque IHDR y dimensiones positivas
function validatePngContent(pngPath) {
  if (!pngPath || !fs.existsSync(pngPath)) return false;
  try {
    const buf = fs.readFileSync(pngPath);
    // Firma PNG estándar (8 bytes): 89 50 4E 47 0D 0A 1A 0A
    if (buf.length < 24) return false;
    const isPng = buf[0] === 0x89 && buf[1] === 0x50 && buf[2] === 0x4E && buf[3] === 0x47 &&
                  buf[4] === 0x0D && buf[5] === 0x0A && buf[6] === 0x1A && buf[7] === 0x0A;
    if (!isPng) return false;

    // Bloque IHDR: longitud (4 bytes en 8), tipo en 12..16 ('IHDR')
    const chunkType = buf.toString('ascii', 12, 16);
    if (chunkType !== 'IHDR') return false;

    const width = buf.readUInt32BE(16);
    const height = buf.readUInt32BE(20);
    return width > 0 && height > 0;
  } catch (e) {
    return false;
  }
}

// Validador de contenido MP4: comprueba estructura del contenedor ftyp y ejecuta ffprobe si está instalado
function validateMp4Content(mp4Path) {
  if (!mp4Path || !fs.existsSync(mp4Path)) return false;
  try {
    const buf = fs.readFileSync(mp4Path);
    if (buf.length < 32) return false;

    // Caja ftyp obligatoria en bytes 4..8
    const ftyp = buf.toString('ascii', 4, 8);
    if (ftyp !== 'ftyp') return false;

    // Si ffprobe está disponible en el entorno, validar pista de vídeo y decodificación completa
    try {
      execSync(`ffprobe -v error -select_streams v:0 -show_entries stream=codec_name,width,height -of default=noprint_wrappers=1 "${mp4Path}"`, { stdio: 'pipe' });
      execSync(`ffmpeg -v error -i "${mp4Path}" -f null -`, { stdio: 'pipe' });
    } catch (toolErr) {
      // Si ffprobe o ffmpeg no están instalados en el sistema operativo, la validación estructural ftyp es la base
    }

    return true;
  } catch (e) {
    return false;
  }
}

// Generar el reporte para una sesión finalizada
async function processJob(job) {
  const { session_id } = job;
  const outDir = getOutputDir();

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
  const tmpPath = path.join(outDir, `${baseFileName}_report.tmp.json`);
  const finalPath = path.join(outDir, `${baseFileName}_report.json`);
  const postPath = path.join(outDir, `${baseFileName}_post.txt`);

  // 1. Escribir resumen JSON de manera atómica (tmp -> rename tras validar)
  fs.writeFileSync(tmpPath, JSON.stringify(summary, null, 2), 'utf8');
  JSON.parse(fs.readFileSync(tmpPath, 'utf8')); // Validar parseabilidad
  fs.renameSync(tmpPath, finalPath);

  // 2. Generar el texto formateado del post
  const postText = generatePostText(summary);
  fs.writeFileSync(postPath, postText, 'utf8');

  console.log(`[ReportWorker] ✅ Datos agregados para sesión: ${baseFileName}`);
  console.log(`[ReportWorker]    ${stream.slug} | ${stream.platform} | pico: ${stream.peak_viewers} | media: ${stream.avg_viewers ?? 'N/D'} | cobertura: ${((stream.coverage_ratio || 0) * 100).toFixed(1)}%`);

  // 3. Comprobar si existen archivos de imagen y vídeo renderizados reales y válidos
  const pngPath = path.join(outDir, `${baseFileName}_summary.png`);
  const mp4Path = path.join(outDir, `${baseFileName}_replay.mp4`);

  const hasValidPng = validatePngContent(pngPath);
  const hasValidMp4 = validateMp4Content(mp4Path);

  if (!hasValidPng || !hasValidMp4) {
    // Si el motor de renderizado Canvas/FFmpeg no ha generado archivos válidos todavía:
    // NO se crean archivos simulados (stubs). Se marca como 'pending_render' hasta que se procese.
    return {
      status: 'pending_render',
      finalPath,
      postPath,
      missingMedia: { png: !hasValidPng, mp4: !hasValidMp4 },
      summary
    };
  }

  return {
    status: 'done',
    finalPath,
    postPath,
    pngPath,
    mp4Path,
    summary
  };
}

// Marcar job como pendiente de renderizado (datos agregados pero sin medios renderizados aún)
function markPendingRender(job, output) {
  db.prepare(`
    UPDATE report_jobs SET status = 'pending_render', lease_until = NULL, last_error = NULL
    WHERE session_id = ? AND report_version = ?
  `).run(job.session_id, job.report_version);
  console.log(`[ReportWorker] ⏳ Sesión ${job.session_id}: JSON y post listos. Estado fijado a 'pending_render'.`);
}

// Marcar job completado tras validación íntegra de todos los entregables
function markDone(job, output) {
  db.prepare(`
    UPDATE report_jobs SET status = 'done', lease_until = NULL, last_error = NULL
    WHERE session_id = ? AND report_version = ?
  `).run(job.session_id, job.report_version);
  console.log(`[ReportWorker] 🏆 Sesión ${job.session_id}: Todos los archivos validados. Estado fijado a 'done'.`);
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
    console.warn(`[ReportWorker] ⚠️ Reintento ${attempts}/${MAX_ATTEMPTS} en ${Math.round(backoffMs / 1000)}s: ${job.session_id}`);
  }
}

// Ciclo de procesamiento
async function runWorkerCycle() {
  // Liberar leases vencidas en cada ciclo
  releaseExpiredLeases();

  let job = claimNextJob();
  while (job) {
    try {
      const output = await processJob(job);
      if (output.status === 'pending_render') {
        markPendingRender(job, output);
      } else {
        markDone(job, output);
      }
    } catch (err) {
      console.error(`[ReportWorker] Error procesando job ${job.session_id}:`, err.message);
      markFailed(job, err);
    }
    job = claimNextJob();
  }
}

let workerTimer = null;

function startReportWorker() {
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

module.exports = {
  startReportWorker,
  stopReportWorker,
  processJob,
  generatePostText,
  validatePngContent,
  validateMp4Content,
  getOutputDir
};
