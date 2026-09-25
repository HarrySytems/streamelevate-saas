'use strict';
const path = require('path');
const fs = require('fs');
const { renderReport } = require('../src/report-renderer');

async function generateSample() {
  const reportsDir = path.join(__dirname, '..', 'data', 'reports');
  fs.mkdirSync(reportsDir, { recursive: true });

  const baseTime = Date.now() - (3 * 3600 * 1000);
  const samples = [];
  // 40 muestras simulando un directo real de Westcol
  for (let i = 0; i < 40; i++) {
    const t = baseTime + (i * 270000); // cada 4.5 minutos
    const v = Math.round(5000 + Math.sin(i / 5) * 12000 + (i * 900));
    samples.push({ timestamp: t, viewers: Math.max(1200, v) });
  }

  const stream = {
    id: 'kick:westcol:sample_preview',
    platform: 'kick',
    slug: 'westcol',
    title: 'HABLANDO CLARO / ESPECIAL NOCTURNO',
    category: 'Just Chatting',
    started_at: baseTime,
    ended_at: Date.now(),
    peak_viewers: 48985,
    avg_viewers: 35269,
    start_followers: 4125000,
    end_followers: 4125519,
    followers_diff: 519
  };

  const summary = {
    ...stream,
    duration_seconds: 10800,
    observed_viewer_hours: 105.8,
    coverage_ratio: 0.998,
    total_messages: 31407,
    unique_chatters: 6336
  };

  console.log('[SampleGenerator] Renderizando video replay.mp4 y summary.png con Puppeteer y FFmpeg...');
  const res = await renderReport({
    stream,
    samples,
    gaps: [],
    summary,
    outputDir: reportsDir
  }, { seconds: 6, fps: 30 });

  console.log('[SampleGenerator] ¡Éxito! Archivos generados en:', res);
}

generateSample().catch(err => {
  console.error('[SampleGenerator] Error:', err);
  process.exit(1);
});
