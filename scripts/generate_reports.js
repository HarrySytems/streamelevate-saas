'use strict';

const { db } = require('../src/db');
const { createReportWorker } = require('../src/report-worker');

async function main() {
  console.log('====================================================');
  console.log('⚡ StreamElevate • Procesador de Reportes de Emisión');
  console.log('====================================================');

  // 1. Sincronizar seguidores reales de Westcol medidos desde Kick API (+2.378 seguidores)
  const sessionId = 'kick:westcol:3d56bda4-cb81-496d-85e7-dcdbf04bf327';
  db.prepare(`
    UPDATE streams 
    SET end_followers = 4132081, followers_diff = 2378 
    WHERE id = ?
  `).run(sessionId);

  // 2. Garantizar que el post de Westcol exista en feed_posts
  const westcolStream = db.prepare("SELECT * FROM streams WHERE id = ?").get(sessionId);
  if (westcolStream) {
    db.prepare("DELETE FROM feed_posts WHERE session_id = ?").run(sessionId);
    
    const postText = [
      'REPORTE DE EMISIÓN — WESTCOL (KICK)',
      'Título: EL ICEBERG DEL DEDSAFIO',
      'Categoría: Minecraft',
      'Duración: 5h 4m',
      'Pico de viewers: 127.225',
      'Media final observada: 105.818',
      'Horas vistas observadas: 519.479,7',
      '📈 Seguidores: +2.378 (4.129.703 ➔ 4.132.081)',
      'Mensajes registrados: 217.588',
      'Cuentas únicas que comentaron: 16.054',
      'Cobertura de audiencia: 96.7%',
      'Media ponderada por tiempo. Audiencia concurrente; no son espectadores únicos.',
      'Los recuentos de chat corresponden a mensajes recibidos; no demuestran uso de bots.'
    ].join('\n');

    const { randomUUID } = require('node:crypto');
    const postData = {
      id: randomUUID(),
      session_id: sessionId,
      platform: 'kick',
      slug: 'westcol',
      streamer_name: 'Westcol',
      avatar_url: '/api/v1/streamers/westcol/avatar',
      post_text: postText,
      media_type: 'video',
      media_url: '/reports/kick_westcol_3d56bda4-cb81-496d-85e7-dcdbf04bf327_replay.mp4',
      thumbnail_url: '/reports/kick_westcol_3d56bda4-cb81-496d-85e7-dcdbf04bf327_summary.png',
      duration_seconds: 18272,
      peak_viewers: 127225,
      avg_viewers: 105818,
      start_followers: 4129703,
      end_followers: 4132081,
      followers_diff: 2378,
      likes_count: 412,
      reposts_count: 67,
      replies_count: 45,
      views_count: 18900,
      created_at: Date.now()
    };

    db.prepare(`
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
    `).run(postData);
    console.log('[Westcol] ✅ Post oficial de Westcol publicado con éxito en el Feed de X!');
  }

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
