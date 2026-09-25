'use strict';

/**
 * equalizer.js - Motor de Ecualizador Monótono PCHIP para RadarStream
 * Adaptado para Node.js y navegador (Puppeteer canvas).
 */

function cleanSamples(rawSamples) {
  const seen = new Set();
  const cleaned = (rawSamples || [])
    .map(s => ({
      t: Number(s.timestamp ?? s.t),
      v: Number(s.viewers ?? s.v)
    }))
    .filter(s => Number.isFinite(s.t) && Number.isFinite(s.v) && s.v >= 0)
    .sort((a, b) => a.t - b.t)
    .filter(s => {
      if (seen.has(s.t)) return false;
      seen.add(s.t);
      return true;
    });

  if (cleaned.length < 2) {
    if (cleaned.length === 1) {
      // Si solo hay una muestra, duplicarla desplazada 1 segundo para interpolar
      return [cleaned[0], { t: cleaned[0].t + 1000, v: cleaned[0].v }];
    }
    throw new Error('Se necesitan al menos 2 muestras válidas para interpolar.');
  }
  return cleaned;
}

function computeTimeWeightedStats(samples) {
  const s = cleanSamples(samples);

  let peak = -Infinity;
  let min = Infinity;
  let peakTime = s[0].t;
  let minTime = s[0].t;
  let weightedSum = 0;

  for (const point of s) {
    if (point.v > peak) { peak = point.v; peakTime = point.t; }
    if (point.v < min) { min = point.v; minTime = point.t; }
  }

  for (let i = 0; i < s.length - 1; i++) {
    const dt = s[i + 1].t - s[i].t;
    const avgSegment = (s[i].v + s[i + 1].v) / 2;
    weightedSum += avgSegment * dt;
  }

  const durationMs = s[s.length - 1].t - s[0].t;
  const average = durationMs > 0 ? (weightedSum / durationMs) : s[0].v;
  const hoursWatched = weightedSum / 3600000;

  return {
    average: Math.round(average),
    peak,
    peakTime,
    min,
    minTime,
    durationMs,
    hoursWatched,
    sampleCount: s.length
  };
}

function computePchipSlopes(xs, ys) {
  const n = xs.length;
  const h = new Array(n - 1);
  const delta = new Array(n - 1);

  for (let i = 0; i < n - 1; i++) {
    h[i] = xs[i + 1] - xs[i];
    delta[i] = (ys[i + 1] - ys[i]) / (h[i] || 1);
  }

  const m = new Array(n);
  if (n === 2) {
    m[0] = m[1] = delta[0];
    return m;
  }

  for (let i = 1; i < n - 1; i++) {
    const d0 = delta[i - 1];
    const d1 = delta[i];
    if (d0 === 0 || d1 === 0 || (d0 > 0) !== (d1 > 0)) {
      m[i] = 0;
    } else {
      const w0 = 2 * h[i] + h[i - 1];
      const w1 = h[i] + 2 * h[i - 1];
      m[i] = (w0 + w1) / (w0 / d0 + w1 / d1);
    }
  }

  function endpointSlope(h0, h1, d0, d1) {
    let sl = ((2 * h0 + h1) * d0 - h0 * d1) / (h0 + h1 || 1);
    if ((sl > 0) !== (d0 > 0)) sl = 0;
    else if ((d0 > 0) !== (d1 > 0) && Math.abs(sl) > 3 * Math.abs(d0)) sl = 3 * d0;
    return sl;
  }

  m[0] = endpointSlope(h[0], h[1], delta[0], delta[1]);
  m[n - 1] = endpointSlope(h[n - 2], h[n - 3], delta[n - 2], delta[n - 3]);
  return m;
}

function hermiteSegment(x0, x1, y0, y1, m0, m1, x) {
  const h = x1 - x0;
  if (h <= 0) return y0;
  const t = (x - x0) / h;
  const t2 = t * t;
  const t3 = t2 * t;
  const h00 = 2 * t3 - 3 * t2 + 1;
  const h10 = t3 - 2 * t2 + t;
  const h01 = -2 * t3 + 3 * t2;
  const h11 = t3 - t2;
  return h00 * y0 + h10 * h * m0 + h01 * y1 + h11 * h * m1;
}

function createMonotonicInterpolator(samples) {
  const s = cleanSamples(samples);
  const xs = s.map(p => p.t);
  const ys = s.map(p => p.v);
  const slopes = computePchipSlopes(xs, ys);

  return function interpolate(t) {
    if (t <= xs[0]) return ys[0];
    if (t >= xs[xs.length - 1]) return ys[ys.length - 1];

    let lo = 0;
    let hi = xs.length - 1;
    while (hi - lo > 1) {
      const mid = (lo + hi) >> 1;
      if (xs[mid] <= t) lo = mid;
      else hi = mid;
    }
    return Math.max(0, hermiteSegment(xs[lo], xs[hi], ys[lo], ys[hi], slopes[lo], slopes[hi], t));
  };
}

function easeOutCubic(p) {
  return 1 - Math.pow(1 - p, 3);
}

/**
 * Remuestreo denso para generar exactamente totalFrames de animación continua.
 */
function resampleToDenseFrames(samples, totalFrames = 480, introRampRatio = 0.0625) {
  const s = cleanSamples(samples);
  const interpolator = createMonotonicInterpolator(s);
  const firstT = s[0].t;
  const lastT = s[s.length - 1].t;
  const dataSpan = lastT - firstT;

  const introFrames = Math.max(1, Math.round(totalFrames * introRampRatio));
  const mainFrames = totalFrames - introFrames;

  const frames = [];

  // Rampa de inicio (0.5s en 60fps = 30 frames de 480)
  for (let f = 0; f < introFrames; f++) {
    const p = f / introFrames;
    frames.push({
      frameIndex: f,
      t: firstT,
      v: easeOutCubic(p) * s[0].v,
      phase: 'intro'
    });
  }

  // Trazado continuo hasta el ancla exacta del final (100%)
  for (let f = 0; f <= mainFrames; f++) {
    const p = f / mainFrames;
    const t = firstT + p * dataSpan;
    frames.push({
      frameIndex: introFrames + f,
      t,
      v: interpolator(t),
      phase: 'main'
    });
  }

  return frames;
}

if (typeof module !== 'undefined') {
  module.exports = {
    cleanSamples,
    computeTimeWeightedStats,
    createMonotonicInterpolator,
    resampleToDenseFrames
  };
}
if (typeof window !== 'undefined') {
  window.RadarEqualizer = {
    cleanSamples,
    computeTimeWeightedStats,
    createMonotonicInterpolator,
    resampleToDenseFrames
  };
}
