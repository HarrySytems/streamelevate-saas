// RadarStream Broadcast Card - TVTOP Standard Edition
window.prepare = function(session, options, analysis = {}) {
  const model = buildTimeline(session.points, options.maxGapMs), final = model.final;
  const canvas = document.querySelector('canvas'), ctx = canvas.getContext('2d');
  const isKick = session.platform === 'kick';
  const neon = isKick ? '#53fc18' : '#c084fc';
  const gx = 35, gy = 120, gw = 890, gh = 393, left = 100, top = 149, pw = 801, ph = 313, bottom = top + ph;
  const rough = Math.max(1, final.peak * 1.08 / 11), base = 10 ** Math.floor(Math.log10(rough));
  const scale = [1, 2, 2.5, 5, 10].map(v => v * base).find(v => v >= rough) * 11;
  
  // Exact observed timeline. Never manufacture pre-stream ramps or post-stream zeros.
  const pts = model.points;
  const visualStart = model.start, visualEnd = model.end;
  const segments = [];
  for (const p of pts) {
    const last = segments.at(-1)?.at(-1);
    if (!last || p.breakBefore || p.t-last.t>model.maxGapMs) segments.push([p]);
    else segments.at(-1).push(p);
  }

  const x = t => left + (t - visualStart) / Math.max(1, visualEnd - visualStart) * pw;
  const y = v => bottom - (Math.max(0, v) / scale) * ph;
  const n = (v, d = 0) => v == null ? 'N/D' : v.toLocaleString('es-ES', { maximumFractionDigits: d, useGrouping: true, minimumFractionDigits: 0 });
  const time = t => new Date(t).toLocaleTimeString('es-ES', { timeZone: options.timeZone, hour: '2-digit', minute: '2-digit' });
  const day = t => new Date(t).toLocaleDateString('es-ES', { timeZone: options.timeZone });
  const duration = () => {
    const mins = Math.floor((model.end - model.start) / 60000);
    if(mins<1)return Math.floor((model.end-model.start)/1000)+' s';
    return Math.floor(mins / 60) + 'h ' + String(mins % 60).padStart(2, '0') + 'min';
  };

  function text(value, xx, yy, size = 14, color = '#f1f5f9', weight = '400', align = 'left', maxWidth) {
    ctx.fillStyle = color;
    ctx.font = weight + ' ' + size + 'px Arial, -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif';
    ctx.textAlign = align;
    let str = String(value);
    if (maxWidth) while (str.length > 1 && ctx.measureText(str).width > maxWidth) str = str.slice(0, -2) + '…';
    ctx.fillText(str, xx, yy);
  }

  function line(coords) {
    ctx.beginPath();
    coords.forEach((p, i) => i ? ctx.lineTo(p.x, p.y) : ctx.moveTo(p.x, p.y));
    ctx.stroke();
  }

  function panel(xx, yy, w, h) {
    ctx.fillStyle = '#0e1620';
    ctx.fillRect(xx, yy, w, h);
    ctx.strokeStyle = '#1e2b38';
    ctx.lineWidth = 1.4;
    ctx.strokeRect(xx, yy, w, h);
  }

  // Pre-render measured segments and explicitly distinguished missing intervals.
  const curveCanvas = document.createElement('canvas');
  curveCanvas.width = 1280;
  curveCanvas.height = 720;
  const curve = curveCanvas.getContext('2d');

  const area = curve.createLinearGradient(0, top, 0, bottom);
  area.addColorStop(0, isKick ? 'rgba(83, 252, 24, 0.38)' : 'rgba(192, 132, 252, 0.38)');
  area.addColorStop(0.7, isKick ? 'rgba(83, 252, 24, 0.08)' : 'rgba(192, 132, 252, 0.08)');
  area.addColorStop(1, 'rgba(10, 16, 22, 0)');

  // Equalizer: Remuestreo denso continuo PCHIP (sin huecos, sin costuras amarillas)
  const densePoints = (typeof window !== 'undefined' && window.RadarEqualizer)
    ? window.RadarEqualizer.resampleToDenseFrames(pts, 1200, 0.04)
    : pts;

  if (densePoints.length >= 2) {
    const first = densePoints[0], last = densePoints.at(-1);
    curve.fillStyle = area;
    curve.beginPath();
    curve.moveTo(x(first.t), bottom);
    for (const p of densePoints) {
      curve.lineTo(x(p.t), y(p.v));
    }
    curve.lineTo(x(last.t), bottom);
    curve.closePath();
    curve.fill();

    for (const [color, width, glow] of [[neon, 3.4, 14], ['#ffffff', 0.8, 0]]) {
      curve.save();
      curve.strokeStyle = color;
      curve.lineWidth = width;
      curve.shadowColor = neon;
      curve.shadowBlur = glow;
      curve.beginPath();
      densePoints.forEach((p, i) => i ? curve.lineTo(x(p.t), y(p.v)) : curve.moveTo(x(p.t), y(p.v)));
      curve.stroke();
      curve.restore();
    }
  }

  window.draw = function(frame, fps = 60, seconds = 8) {
    // Trazado continuo hasta el 100% exacto de la curva, dejando 1.5s final para inspección
    const totalFrames = fps * seconds;
    const progress = Math.max(0, Math.min(1, frame / Math.max(1, totalFrames - Math.round(fps * 1.5))));
    const sampleIdx = Math.min(densePoints.length - 1, Math.floor(progress * (densePoints.length - 1)));
    const currentSample = densePoints[sampleIdx];
    const t = currentSample.t;
    const tracerValue = currentSample.v;

    // Dark Broadcast Background
    const bg = ctx.createLinearGradient(0, 0, 1280, 720);
    bg.addColorStop(0, '#090f15');
    bg.addColorStop(1, '#060a0e');
    ctx.fillStyle = bg;
    ctx.fillRect(0, 0, 1280, 720);

    // Subtle Grid Pattern
    ctx.strokeStyle = '#ffffff04';
    ctx.lineWidth = 1;
    for (let xx = 0; xx < 1280; xx += 35) line([{ x: xx, y: 0 }, { x: xx, y: 720 }]);

    // Top Neon Accent Bar
    const accent = ctx.createLinearGradient(0, 0, 1280, 0);
    accent.addColorStop(0, 'transparent');
    accent.addColorStop(0.2, neon);
    accent.addColorStop(0.5, '#ffffff');
    accent.addColorStop(0.8, neon);
    accent.addColorStop(1, 'transparent');
    ctx.fillStyle = accent;
    ctx.fillRect(0, 0, 1280, 3);

    // ── AVATAR CIRCLE ──
    const avX = 70, avY = 62, avR = 34;
    ctx.save();
    ctx.shadowColor = neon;
    ctx.shadowBlur = 14;
    ctx.strokeStyle = neon;
    ctx.lineWidth = 2.4;
    ctx.beginPath();
    ctx.arc(avX, avY, avR, 0, Math.PI * 2);
    ctx.stroke();
    ctx.restore();

    ctx.save();
    ctx.beginPath();
    ctx.arc(avX, avY, avR - 1, 0, Math.PI * 2);
    ctx.clip();
    if (window.radarAvatar) {
      ctx.drawImage(window.radarAvatar, avX - avR, avY - avR, avR * 2, avR * 2);
    } else {
      ctx.fillStyle = '#14202c';
      ctx.fillRect(avX - avR, avY - avR, avR * 2, avR * 2);
      text((session.streamerName || '?')[0].toUpperCase(), avX, avY + 12, 34, neon, '900', 'center');
    }
    ctx.restore();

    // Platform Badge
    ctx.save();
    ctx.fillStyle = neon;
    ctx.beginPath();
    ctx.arc(avX + 24, avY + 24, 11, 0, Math.PI * 2);
    ctx.fill();
    ctx.fillStyle = '#000000';
    ctx.font = '900 11px Arial';
    ctx.textAlign = 'center';
    ctx.fillText(isKick ? 'K' : 'T', avX + 24, avY + 28);
    ctx.restore();

    // Header Meta
    text((session.streamerName || session.slug || 'STREAMER').toUpperCase(), 125, 48, 32, '#ffffff', '900', 'left', 840);
    text('RADAR', 1055, 45, 24, '#ffffff', '900');
    text('STREAM', 1147, 45, 24, neon, '900');
    text('INFORME DE EMISIÓN', 1244, 64, 9, '#78889a', '700', 'right');

    ctx.fillStyle = '#1c2633';
    ctx.fillRect(125, 61, 109, 20);
    ctx.strokeStyle = neon;
    ctx.lineWidth = 1;
    ctx.strokeRect(125, 61, 109, 20);
    text('TÍTULO DEL DIRECTO:', 131, 75, 9, neon, '700');
    text(String(session.title || 'Emisión en directo').replace(/[\r\n]/g, ' '), 243, 76, 14, '#f5f7fa', '700', 'left', 970);

    const followers = session.finalFollowers == null ? '' : '  •  ' + n(Number(session.finalFollowers)) + ' SEGUIDORES';
    text(`${String(session.category || session.platform || '').toUpperCase()}  •  INICIO OBSERVADO: ${time(model.start)}  •  DURACIÓN OBS.: ${duration()}${followers}`, 125, 100, 10, '#9aa9bb', '400', 'left', 1090);

    // ── MAIN CHART CONTAINER ──
    panel(gx, gy, gw, gh);

    // Watermark
    ctx.save();
    ctx.globalAlpha = 0.04;
    text(isKick ? 'KICK' : 'TWITCH', left + pw / 2, top + ph * 0.60, 110, neon, '900', 'center');
    ctx.restore();

    // Horizontal Level Grid
    ctx.strokeStyle = '#ffffff08';
    ctx.lineWidth = 1;
    for (let i = 0; i < 12; i++) {
      const yy = top + ph * i / 11;
      line([{ x: left, y: yy }, { x: left + pw, y: yy }]);
      text(n(scale * (1 - i / 11)), left - 10, yy + 4, 10, '#a9b7c7', '400', 'right');
    }

    // Vertical Columns & Angled Timestamps
    for (let i = 0; i < 19; i++) {
      const xx = left + pw * i / 18;
      line([{ x: xx, y: top }, { x: xx, y: bottom }]);
      ctx.save();
      ctx.translate(xx, bottom + 12);
      ctx.rotate(-Math.PI / 4);
      text(time(visualStart + (visualEnd - visualStart) * i / 18), 0, 0, 9, '#a9b7c7', '400', 'right');
      ctx.restore();
    }

    // Curve Sweep Animation
    ctx.save();
    ctx.beginPath();
    ctx.rect(left - 3, top - 5, pw + 6, ph + 8);
    ctx.clip();
    {
      ctx.beginPath();
      ctx.rect(left - 3, top - 5, x(t) - left + (progress === 1 ? 6 : 3), ph + 8);
      ctx.clip();
      ctx.drawImage(curveCanvas, 0, 0);
    }
    ctx.restore();

    // Lead Glowing Tracer Orb
    if (tracerValue !== null && tracerValue !== undefined) {
      ctx.save();
      ctx.shadowColor = neon;
      ctx.shadowBlur = 14;
      ctx.fillStyle = '#ffffff';
      ctx.beginPath();
      ctx.arc(x(t), y(tracerValue), 4.5, 0, Math.PI * 2);
      ctx.fill();
      ctx.restore();
    }

    // ── MINUTO DE ORO PEAK BADGE ──
    if (t >= final.peakTime) {
      const px = x(final.peakTime), py = y(final.peak);
      const lx = Math.max(left, Math.min(left + pw - 84, px - 42));

      // Vertical Guide Line to Base
      ctx.save();
      ctx.setLineDash([3, 3]);
      ctx.strokeStyle = neon;
      ctx.globalAlpha = 0.35;
      ctx.lineWidth = 1;
      ctx.beginPath();
      ctx.moveTo(px, py);
      ctx.lineTo(px, bottom);
      ctx.stroke();
      ctx.restore();

      // Peak Box
      ctx.save();
      ctx.shadowColor = 'rgba(0,0,0,0.7)';
      ctx.shadowBlur = 10;
      ctx.fillStyle = '#080f14';
      ctx.fillRect(lx, py - 36, 84, 26);
      ctx.strokeStyle = neon;
      ctx.lineWidth = 1.8;
      ctx.strokeRect(lx, py - 36, 84, 26);
      ctx.restore();

      text(n(final.peak), lx + 42, py - 19, 12, neon, '700', 'center');

      // Glowing Orb at Peak
      ctx.beginPath();
      ctx.arc(px, py, 5, 0, Math.PI * 2);
      ctx.fillStyle = neon;
      ctx.fill();
      ctx.strokeStyle = '#ffffff';
      ctx.lineWidth = 2;
      ctx.stroke();
    }

    // ── TVTOP STATS CARDS (RIGHT SIDE) ──
    const cardX = 945, cardW = 300;

    // Card 1: Pico / Minuto de Oro
    panel(cardX, 120, cardW, 80);
    text('MÁXIMO USUARIOS ▲', cardX + 14, 141, 11, '#9dabbc', '700');
    text(n(final.peak), cardX + 14, 175, 29, '#ffffff', '900');
    text('@ ' + time(final.peakTime) + ' · MINUTO DE ORO', cardX + 14, 192, 10, neon, '700');

    // Card 2: Horas Vistas
    panel(cardX, 210, cardW, 80);
    text('HORAS VISTAS', cardX + 14, 231, 11, '#9dabbc', '700');
    text(n(final.hoursWatched, 1), cardX + 14, 266, 29, '#ffffff', '900');
    text('ESTIMADAS SOBRE LAS MUESTRAS VÁLIDAS', cardX + 14, 282, 9, '#7d8da0');

    // Card 3: Retención de Audiencia
    panel(cardX, 300, cardW, 80);
    text('RELACIÓN MEDIA / PICO', cardX + 14, 322, 11, '#9dabbc', '700');
    const ratio = analysis.averageToPeakPercent ?? (final.peak && final.average !== null ? final.average / final.peak * 100 : null);
    text(ratio === null ? 'N/D' : n(ratio, 1) + '%', cardX + 14, 356, 29, '#39d7aa', '900');
    text('NO MIDE RETENCIÓN DE PERSONAS', cardX + 14, 372, 9, '#7d8da0');

    // Card 4: Chatters o Duración
    panel(cardX, 390, cardW, 123);
    const chat = analysis.chat;
    if (chat?.available) {
      text('CHATTERS ÚNICOS & MENSAJES', cardX + 14, 412, 11, neon, '700');
      text(n(chat.uniqueAccounts) + ' CUENTAS', cardX + 14, 442, 22, '#ffffff', '900');
      text(n(chat.messages) + ' MENSAJES REGISTRADOS', cardX + 14, 466, 11, '#9dabbc', '700');
      text(n(chat.messagesPerMinute, 1) + ' MSG/MIN DEL PERÍODO OBSERVADO', cardX + 14, 488, 9, neon, '700');
      text(chat.coverage == null ? 'CHAT: COBERTURA NO MEDIDA' : 'CHAT: '+n(chat.coverage*100,1)+'% VENTANAS COMPROBADAS', cardX + 14, 504, 8, '#9dabbc');
    } else {
      text('DURACIÓN DE TRANSMISIÓN', cardX + 14, 412, 11, neon, '700');
      text(duration(), cardX + 14, 444, 26, '#ffffff', '900');
      text(String(session.category || session.platform || 'VARIEDAD').toUpperCase(), cardX + 14, 472, 11, '#9dabbc', '700');
      text('CHAT: SIN CAPTURA EN ESTE PERÍODO', cardX + 14, 495, 9, '#7d8da0');
    }

    // ── GIANT BOTTOM BANNER ──
    panel(35, 530, 1210, 142);
    text('PROMEDIO DE AUDIENCIA OBSERVADA · RESULTADO FINAL', 640, 560, 13, neon, '700', 'center');
    const avg = final.average === null ? 'N/D' : n(Math.round(final.average));
    text(avg + ' USUARIOS', 640, 623, 50, '#ffffff', '900', 'center');
    text(`${day(model.start)}  •  ${duration()} OBSERVADAS  •  ${n(final.coverage*100,1)}% COBERTURA OBSERVADA  •  ${options.timeZone}`, 640, 654, 11, '#8594a6', '400', 'center');

    // Footer
    text('RadarStream · Curva continua de audiencia verificada', 35, 694, 11, '#8493a6');
    text('INICIO MEDIDO: ' + n(pts[0].v), left + 10, top + 18, 10, neon, '700');
    if (progress === 1) text('RECORRIDO COMPLETO', left + pw - 10, top + 18, 10, neon, '700', 'right');
    text('RADARSTREAM.OFICIAL', 1245, 694, 12, neon, '700', 'right');

    return {
      frame,
      t,
      progress,
      tracerX: x(t),
      tracerValue,
      tracerMeasured: true,
      chartRight: left + pw,
      curveValue: tracerValue,
      value: tracerValue,
      average: final.average,
      hoursWatched: final.hoursWatched,
      peak: final.peak,
      coverage: final.coverage,
      reportMode: 'post-stream',
      averageToPeakPercent: ratio ?? null
    };
  };
};
