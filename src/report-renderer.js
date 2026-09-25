'use strict';

const fs = require('node:fs');
const path = require('node:path');
const { spawn } = require('node:child_process');
const { once } = require('node:events');
const { mediaPaths } = require('./media-tools');

// Reuse the user's existing RadarStream design; no card/layout/style is redrawn here.
async function renderReport({ stream, samples, gaps, summary, outputDir }, options = {}) {
  const fps = options.fps ?? 60;
  const seconds = options.seconds ?? 8;
  const frames = Math.round(fps * seconds);
  const points = samples.filter(s => Number.isFinite(s.timestamp) &&
    Number.isFinite(s.viewers) && s.viewers >= 0).map(s => ({ t: s.timestamp, v: s.viewers }));
  if (points.length < 2 || points.at(-1).t <= points[0].t) {
    throw new Error('No hay dos muestras temporales válidas para renderizar esta sesión');
  }
  const pngPath = path.join(outputDir, 'summary.png');
  const mp4Path = path.join(outputDir, 'replay.mp4');
  const tracePath = path.join(outputDir, 'frames.json');
  let browser, encoder;
  try {
    const puppeteer = require('puppeteer');
    const edge = 'C:/Program Files (x86)/Microsoft/Edge/Application/msedge.exe';
    const executablePath = process.env.BROWSER_PATH ||
      (process.platform === 'win32' && fs.existsSync(edge) ? edge : undefined);
    browser = await puppeteer.launch({ headless: true,
      ...(executablePath ? { executablePath } : {}),
      args: ['--disable-dev-shm-usage', ...(process.env.RADAR_NO_SANDBOX === '1' ? ['--no-sandbox'] : [])]
    });
    const page = await browser.newPage();
    await page.setViewport({ width: 1280, height: 720 });
    await page.emulateTimezone(process.env.RADAR_TIMEZONE || 'America/Lima');
    const html = fs.readFileSync(path.join(__dirname, '../public/radar-live.html'), 'utf8');
    await page.setContent(html.replace('<head>', '<head><script>window.__RADAR_EXPORT__=true;</script>'));
    await page.evaluate(payload => window.RadarExport.prepare(payload), { stream, samples, summary });

    encoder = spawn(mediaPaths().ffmpeg, [
      '-y', '-hide_banner', '-loglevel', 'error', '-f', 'image2pipe', '-vcodec', 'png',
      '-framerate', String(fps), '-i', 'pipe:0', '-an', '-c:v', 'libx264',
      '-threads', '2', '-preset', 'veryfast', '-crf', '20', '-pix_fmt', 'yuv420p',
      '-movflags', '+faststart', mp4Path
    ], { windowsHide: true });
    let stderr = '';
    encoder.stderr.on('data', chunk => { stderr = (stderr + chunk).slice(-16000); });
    encoder.stdin.on('error', () => {});
    const finished = new Promise((resolve, reject) => {
      encoder.on('error', reject);
      encoder.on('close', code => code === 0 ? resolve() : reject(new Error(`FFmpeg ${code}: ${stderr}`)));
    });
    finished.catch(() => {});
    const trace = [];
    for (let frame = 0; frame < frames; frame++) {
      const result = await page.evaluate((frame, fps, seconds) => ({
        metrics: window.RadarExport.drawFrame(frame, fps, seconds),
        png: document.querySelector('canvas').toDataURL('image/png').split(',')[1]
      }), frame, fps, seconds);
      trace.push(result.metrics);
      const buffer = Buffer.from(result.png, 'base64');
      if (encoder.exitCode !== null) throw new Error(`FFmpeg terminó prematuramente: ${stderr}`);
      if (!encoder.stdin.write(buffer)) await Promise.race([
        once(encoder.stdin, 'drain'), finished.then(() => { throw new Error('Encoder cerrado antes del último frame'); })
      ]);
      if (frame === frames - 1) fs.writeFileSync(pngPath, buffer);
    }
    encoder.stdin.end();
    await finished;
    fs.writeFileSync(tracePath, JSON.stringify(trace));
    return { pngPath, mp4Path, tracePath, expected: { width: 1280, height: 720, frames, seconds } };
  } finally {
    if (encoder && encoder.exitCode === null) encoder.kill();
    if (browser) await browser.close();
  }
}

module.exports = { renderReport };
