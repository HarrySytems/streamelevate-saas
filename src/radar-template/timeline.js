'use strict';
// Shared by Node and the browser. All integrations use milliseconds, never sample indices.
function buildTimeline(raw, maxGapMs = 60000) {
  const byTime = new Map();
  for (const p of raw || []) {
    const t = p.timestamp ?? p.t;
    if (Number.isFinite(t) && Number.isFinite(p.v) && p.v >= 0) byTime.set(t, {t, v:p.v, breakBefore:!!p.breakBefore});
  }
  const points = [...byTime.values()].sort((a,b) => a.t-b.t);
  if (points.length < 2) throw new Error('Se necesitan dos muestras válidas con tiempos distintos');
  const start = points[0].t, end = points.at(-1).t;
  function at(time) {
    const t = Math.max(start, Math.min(end, time));
    let area=0, covered=0, peak=points[0].v, peakTime=start, value=points[0].v;
    for(let i=1;i<points.length;i++) {
      const a=points[i-1], b=points[i];
      if(t<=a.t) break;
      const dt=Math.min(t,b.t)-a.t;
      if(!b.breakBefore && b.t-a.t<=maxGapMs) {
        value=a.v+(b.v-a.v)*dt/(b.t-a.t);
        area+=(a.v+value)*0.5*dt; covered+=dt;
        if(value>peak) {peak=value; peakTime=Math.min(t,b.t);}
      } else value=t<b.t?null:b.v;
      if(t>=b.t && b.v>peak) {peak=b.v;peakTime=b.t;}
      if(t<b.t) break;
    }
    return {t,value,area,covered,average:covered?area/covered:null,
      hoursWatched:area/3600000,peak,peakTime,coverage:t===start?1:covered/(t-start)};
  }
  return {points,start,end,maxGapMs,at,final:at(end)};
}
if(typeof module!=='undefined') module.exports={buildTimeline};
