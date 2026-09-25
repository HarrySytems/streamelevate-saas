Estas tres plantillas se copiaron del motor original de RadarStream
(`radar_engine/src/{canvas,timeline,equalizer}.js`). Conservan su diseño.
La única adaptación del canvas muestra «cobertura no medida» cuando no existe
medición de cobertura del chat, en lugar de inventar un 0 %. No cambia su disposición.
El adaptador `report-renderer.js` carga las cifras finales de la sesión y exporta
el canvas con Puppeteer/FFmpeg. No depende de rutas del equipo del desarrollador.
