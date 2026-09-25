const Database = require('better-sqlite3');
const path = require('path');

const dbPath = path.join(__dirname, '..', 'data', 'streamelevate.sqlite');
const db = new Database(dbPath);

console.log('=== ESTADO ANTES DE LIMPIEZA ===');
console.log('Canales configurados:', db.prepare('SELECT count(*) as c FROM channels').get().c);
console.log('Streams acumulados:', db.prepare('SELECT count(*) as c FROM streams').get().c);
console.log('Muestras de audiencia:', db.prepare('SELECT count(*) as c FROM audience_samples').get().c);
console.log('Mensajes de chat:', db.prepare('SELECT count(*) as c FROM chat_messages').get().c);
console.log('Huecos de captura:', db.prepare('SELECT count(*) as c FROM capture_gaps').get().c);
console.log('Trabajos de reporte:', db.prepare('SELECT count(*) as c FROM report_jobs').get().c);

db.exec(`
  DELETE FROM streams;
  DELETE FROM audience_samples;
  DELETE FROM chat_messages;
  DELETE FROM capture_gaps;
  DELETE FROM report_jobs;
  UPDATE channels SET is_live = 0, current_viewers = 0, current_category = NULL, current_title = NULL, last_checked_at = NULL;
`);

db.pragma('vacuum');

console.log('\n=== ESTADO DESPUÉS DE LIMPIEZA ===');
console.log('Canales conservados (100%):', db.prepare('SELECT count(*) as c FROM channels').get().c);
console.log('Streams activos/pasados:', db.prepare('SELECT count(*) as c FROM streams').get().c);
console.log('Muestras de audiencia:', db.prepare('SELECT count(*) as c FROM audience_samples').get().c);
console.log('Mensajes de chat:', db.prepare('SELECT count(*) as c FROM chat_messages').get().c);
console.log('Huecos de captura:', db.prepare('SELECT count(*) as c FROM capture_gaps').get().c);
console.log('Trabajos de reporte:', db.prepare('SELECT count(*) as c FROM report_jobs').get().c);

db.close();
console.log('\nLimpieza completada con éxito.');
