'use strict';

const { db } = require('../src/db');
const { createReportWorker } = require('../src/report-worker');

async function main() {
  console.log('====================================================');
  console.log('⚡ StreamElevate • Procesador de Reportes de Emisión');
  console.log('====================================================');

  // Reactivar trabajos fallidos o pendientes
  const updateResult = db.prepare(`
    UPDATE report_jobs 
    SET status = 'pending', attempts = 0, next_attempt_at = 1000 
    WHERE status != 'done'
  `).run();
  
  // Priorizar Westcol para que se procese de primero
  db.prepare(`UPDATE report_jobs SET next_attempt_at = 1 WHERE session_id LIKE '%westcol%'`).run();
  
  console.log(`[ReportJobs] ${updateResult.changes} trabajos reactivados para procesamiento (Westcol priorizado).`);

  const pendingJobs = db.prepare(`SELECT session_id, status, attempts FROM report_jobs WHERE status = 'pending'`).all();
  console.log('[ReportJobs] Trabajos en cola:', pendingJobs);

  if (pendingJobs.length === 0) {
    console.log('[ReportJobs] No hay trabajos pendientes.');
    return;
  }

  const worker = createReportWorker({ db, logger: console });
  console.log('[ReportWorker] Iniciando ciclo de renderizado (Puppeteer + FFmpeg)...');
  await worker.runWorkerCycle();

  console.log('====================================================');
  console.log('✅ ¡Ciclo completado con éxito!');
  
  const recentPosts = db.prepare(`SELECT id, slug, streamer_name, peak_viewers, avg_viewers, media_url FROM feed_posts ORDER BY created_at DESC LIMIT 3`).all();
  console.log('[FeedPosts] Últimas publicaciones en el feed de X:', recentPosts);
  console.log('====================================================');
}

main().catch(err => {
  console.error('[Error Crítico]', err);
  process.exit(1);
});
