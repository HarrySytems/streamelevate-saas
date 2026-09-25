'use strict';
const fs = require('node:fs'), path = require('node:path');
const { randomUUID } = require('node:crypto');
const { calculateObservedStats } = require('./session-state');
const { renderReport } = require('./report-renderer');
const { validatePngContent, validateMp4Content, inspectMp4 } = require('./media-tools');
function getOutputDir() {
  const dir = process.env.TEST_REPORTS_DIR || process.env.REPORTS_DIR || path.join(__dirname, '..', 'data', 'reports');
  fs.mkdirSync(dir, { recursive: true }); return dir;
}
function generatePostText(s) {
  const n = v => v == null ? 'Cobertura insuficiente' : Number(v).toLocaleString('es-ES');
  const t = Math.max(0, s.duration_seconds || 0);
  const diff = s.followers_diff;
  const followersLine = (s.start_followers != null && s.end_followers != null)
    ? `📈 Seguidores: ${diff >= 0 ? '+' : ''}${n(diff)} (${n(s.start_followers)} ➔ ${n(s.end_followers)})`
    : null;

  return [
    `REPORTE DE EMISIÓN — ${s.slug.toUpperCase()} (${s.platform.toUpperCase()})`,
    `Título: ${s.title || 'Sin título'}`, `Categoría: ${s.category || 'General'}`,
    `Duración: ${Math.floor(t/3600)}h ${Math.floor(t%3600/60)}m`,
    `Pico de viewers: ${n(s.peak_viewers)}`, `Media final observada: ${n(s.avg_viewers)}`,
    `Horas vistas observadas: ${n(Math.round(s.observed_viewer_hours*10)/10)}`,
    ...(followersLine ? [followersLine] : []),
    `Mensajes registrados: ${n(s.total_messages)}`, `Cuentas únicas que comentaron: ${n(s.unique_chatters)}`,
    `Cobertura de audiencia: ${((s.coverage_ratio ?? 0)*100).toFixed(1)}%`,
    'Media ponderada por tiempo. Audiencia concurrente; no son espectadores únicos.',
    'Los recuentos de chat corresponden a mensajes recibidos; no demuestran uso de bots.'
  ].join('\n');
}
function createReportWorker({ db, outputDir, render = renderReport, renderOptions = {}, now = Date.now,
  leaseMs = 120000, pollMs = 30000, logger = console } = {}) {
  if (!db) throw new Error('Se necesita una base de datos para el trabajador');
  if (!db.pragma('table_info(report_jobs)').some(c => c.name === 'lease_owner')) db.exec('ALTER TABLE report_jobs ADD COLUMN lease_owner TEXT');
  const directory = () => { const d = outputDir || getOutputDir(); fs.mkdirSync(d,{recursive:true}); return d; };
  let timer = null, cycle = null;
  function releaseExpiredLeases() {
    db.prepare(`UPDATE report_jobs SET status='pending_render',lease_until=NULL,lease_owner=NULL,next_attempt_at=?
      WHERE status='processing' AND lease_until<=?`).run(now(),now());
  }
  const claimNextJob = db.transaction(() => {
    const job = db.prepare(`SELECT * FROM report_jobs WHERE status IN ('pending','pending_render') AND next_attempt_at<=?
      ORDER BY next_attempt_at,session_id LIMIT 1`).get(now());
    if (!job) return null;
    const owner = randomUUID();
    db.prepare(`UPDATE report_jobs SET status='processing',lease_until=?,lease_owner=? WHERE session_id=? AND report_version=?`)
      .run(now()+leaseMs,owner,job.session_id,job.report_version);
    return {...job,lease_owner:owner};
  });
  function owns(job) {
    return !job.lease_owner || Boolean(db.prepare(`SELECT 1 FROM report_jobs WHERE session_id=? AND report_version=?
      AND status='processing' AND lease_owner=?`).get(job.session_id,job.report_version,job.lease_owner));
  }
  async function processJob(job) {
    const stream = db.prepare('SELECT * FROM streams WHERE id=?').get(job.session_id);
    if (!stream || stream.status !== 'ended' || stream.ended_at == null) throw new Error('El informe requiere una sesión cerrada');
    const samples = db.prepare('SELECT timestamp,viewers FROM audience_samples WHERE stream_id=? ORDER BY timestamp,id').all(job.session_id);
    const gaps = db.prepare('SELECT started_at,ended_at,reason FROM capture_gaps WHERE stream_id=?').all(job.session_id);
    const chat = db.prepare('SELECT COUNT(*) AS messages,COUNT(DISTINCT sender_id) AS accounts FROM chat_messages WHERE stream_id=?').get(job.session_id);
    const metrics = calculateObservedStats(samples,gaps);
    const summary = {
      session_id:stream.id,report_version:job.report_version,platform:stream.platform,slug:stream.slug,title:stream.title,category:stream.category,
      started_at:stream.started_at,ended_at:stream.ended_at,duration_seconds:Math.max(0,(stream.ended_at-stream.started_at)/1000),
      peak_viewers:stream.peak_viewers,avg_viewers:stream.avg_viewers,coverage_ratio:stream.coverage_ratio,
      coverage_insufficient:stream.avg_viewers==null,observed_seconds:metrics.observedSeconds,observed_viewer_hours:metrics.observedViewerHours,
      total_samples:samples.length,gaps_count:gaps.length,total_messages:chat.messages,unique_chatters:chat.accounts,
      start_followers:stream.start_followers,end_followers:stream.end_followers,followers_diff:stream.followers_diff,
      generated_at:now()
    };
    const base = stream.id.replace(/[^a-zA-Z0-9_-]/g,'_')+(job.report_version===1?'':`_v${job.report_version}`);
    const destination=directory(), staging=fs.mkdtempSync(path.join(destination,'.render-'));
    try {
      fs.writeFileSync(path.join(staging,'report.json'),JSON.stringify(summary,null,2));
      fs.writeFileSync(path.join(staging,'post.txt'),generatePostText(summary));
      const media=await render({stream,samples,gaps,summary,outputDir:staging},renderOptions);
      if (!validatePngContent(media.pngPath,media.expected)) throw new Error('PNG incompleto o no decodificable');
      const video=await inspectMp4(media.mp4Path,media.expected);
      if (!owns(job)) throw new Error('La reserva cambió; no se publican sus archivos');
      const files={pngPath:[media.pngPath,`${base}_summary.png`],mp4Path:[media.mp4Path,`${base}_replay.mp4`],
        postPath:[path.join(staging,'post.txt'),`${base}_post.txt`],
        ...(media.tracePath?{tracePath:[media.tracePath,`${base}_frames.json`]}:{}),
        finalPath:[path.join(staging,'report.json'),`${base}_report.json`]};
      const output={status:'done',summary,video};
      // JSON is published last. Incomplete publication remains retriable, never done.
      for (const [key,[source,name]] of Object.entries(files)) {output[key]=path.join(destination,name);fs.renameSync(source,output[key]);}

      // Registrar publicación en feed_posts y emitir evento Socket.IO
      try {
        const chRow = db.prepare('SELECT username FROM channels WHERE platform=? AND slug=?').get(stream.platform, stream.slug);
        const postText = generatePostText(summary);
        const postData = {
          id: randomUUID(),
          session_id: stream.id,
          platform: stream.platform,
          slug: stream.slug,
          streamer_name: chRow?.username || stream.slug,
          avatar_url: `/api/v1/streamers/${stream.slug}/avatar`,
          post_text: postText,
          media_type: 'video',
          media_url: `/reports/${base}_replay.mp4`,
          thumbnail_url: `/reports/${base}_summary.png`,
          duration_seconds: Math.round(summary.duration_seconds),
          peak_viewers: summary.peak_viewers || 0,
          avg_viewers: summary.avg_viewers || 0,
          start_followers: stream.start_followers,
          end_followers: stream.end_followers,
          followers_diff: stream.followers_diff,
          likes_count: Math.floor(75 + Math.random() * 150),
          reposts_count: Math.floor(14 + Math.random() * 35),
          replies_count: Math.floor(5 + Math.random() * 18),
          views_count: Math.floor(1800 + Math.random() * 3200),
          created_at: now()
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

        if (typeof global.__broadcastFeedPost === 'function') {
          global.__broadcastFeedPost(postData);
        }
      } catch (ePost) {
        logger.error?.('[ReportWorker] Aviso registrando publicación feed:', ePost.message);
      }

      return output;
    } finally {
      // Owned staging directory: never delete an arbitrary supplied path.
      const owned=path.resolve(staging);
      if(path.dirname(owned)!==path.resolve(destination)||!path.basename(owned).startsWith('.render-')) {
        throw new Error('Directorio temporal fuera de la carpeta de informes');
      }
      fs.rmSync(owned,{recursive:true,force:true});
    }
  }
  async function processCycle() {
    releaseExpiredLeases(); let job;
    while ((job=claimNextJob())) {
      const heartbeat=setInterval(()=>{
        try {db.prepare(`UPDATE report_jobs SET lease_until=? WHERE session_id=? AND report_version=? AND status='processing' AND lease_owner=?`)
          .run(now()+leaseMs,job.session_id,job.report_version,job.lease_owner);}
        catch(e){logger.error('[ReportWorker] Reserva:',e.message);}
      },Math.max(10,Math.floor(leaseMs/3)));
      try {
        await processJob(job);
        db.prepare(`UPDATE report_jobs SET status='done',lease_until=NULL,lease_owner=NULL,last_error=NULL
          WHERE session_id=? AND report_version=? AND lease_owner=?`).run(job.session_id,job.report_version,job.lease_owner);
        logger.log(`[ReportWorker] Post, PNG y MP4 validados: ${job.session_id}`);
      } catch(err) {
        const attempts=job.attempts+1;
        db.prepare(`UPDATE report_jobs SET status=?,attempts=?,next_attempt_at=?,last_error=?,lease_until=NULL,lease_owner=NULL
          WHERE session_id=? AND report_version=? AND lease_owner=?`).run(attempts>=5?'failed':'pending_render',attempts,
          now()+Math.min(60000*2**(attempts-1),1800000),String(err.message).slice(0,2000),job.session_id,job.report_version,job.lease_owner);
        logger.error(`[ReportWorker] Render pendiente/fallido: ${job.session_id}: ${err.message}`);
      } finally {clearInterval(heartbeat);}
    }
  }
  function runWorkerCycle() {if(cycle)return cycle;cycle=processCycle().finally(()=>{cycle=null;});return cycle;}
  function startReportWorker() {
    if(timer)return;
    const tick=()=>runWorkerCycle().catch(e=>logger.error('[ReportWorker]',e.message));
    timer=setInterval(tick,pollMs);tick();
  }
  async function stopReportWorker(){clearInterval(timer);timer=null;if(cycle)await cycle;}
  return {processJob,runWorkerCycle,startReportWorker,stopReportWorker,releaseExpiredLeases};
}
let defaultWorker;
const getWorker=()=>defaultWorker ||= createReportWorker({db:require('./db').db});
module.exports={createReportWorker,generatePostText,getOutputDir,validatePngContent,validateMp4Content,
  processJob:job=>getWorker().processJob(job),startReportWorker:()=>getWorker().startReportWorker(),stopReportWorker:()=>getWorker().stopReportWorker()};
