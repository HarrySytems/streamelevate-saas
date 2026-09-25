'use strict';
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm');
const html=fs.readFileSync(require('node:path').join(__dirname,'../public/radar-live.html'),'utf8');
const replay=html.slice(html.indexOf('    async function startReplay()'),html.indexOf('    function updateStatusText()'));
function page(response){
  const status={textContent:'',style:{}};const ctx={currentStreamerData:{stream_id:'test:1',slug:'westcol'},currentSlug:'westcol',currentMode:'live',replayAnimId:null,
    document:{getElementById:()=>status},performance:{now:()=>0},requestAnimationFrame:()=>1,cancelAnimationFrame(){},encodeURIComponent,Number,
    setMode(mode){ctx.currentMode=mode;},fetch:async url=>{ctx.url=url;return response;}};
  vm.createContext(ctx);vm.runInContext(replay,ctx);return {ctx,status};
}
test('replay precarga media final cero antes de empezar, sin variable inexistente',async()=>{
  const {ctx}=page({ok:true,json:async()=>({stream:{status:'ended',avg_viewers:0}})});
  await ctx.startReplay();assert.equal(ctx.currentMode,'replay');assert.equal(ctx.currentStreamerData.avg_viewers,0);assert.ok(ctx.url.includes('test%3A1'));
});
test('replay mantiene explícitamente null y no lo convierte a cero',async()=>{
  const {ctx}=page({ok:true,json:async()=>({stream:{status:'ended',avg_viewers:null}})});
  await ctx.startReplay();assert.equal(ctx.currentMode,'replay');assert.equal(ctx.currentStreamerData.avg_viewers,null);
});
test('replay no empieza con resumen fallido o sesión todavía abierta',async()=>{
  for(const res of [{ok:false},{ok:true,json:async()=>({stream:{status:'live',avg_viewers:0}})}]){
    const {ctx,status}=page(res);await ctx.startReplay();assert.equal(ctx.currentMode,'live');assert.ok(status.textContent.length>0);
  }
});
