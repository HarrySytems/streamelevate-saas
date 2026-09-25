# Generación de informes de emisiones cerradas

El trabajador consume `pending` y `pending_render`. Crea texto, JSON, PNG y
MP4 con el canvas existente de RadarStream, usando un directorio temporal por
trabajo. Solo marca `done` después de decodificar la imagen y todos los fotogramas
del vídeo. Fallos de herramientas o archivos corruptos producen reintentos con
espera, no éxito. Tras cinco fallos el error queda registrado en la cola.

La media final almacenada se conserva durante todos los fotogramas. Las horas
vistas se calculan con las muestras originales y los huecos registrados. No se
modifica el colector ni se reconstruyen mediciones históricas en esta corrección.
El exportador carga directamente `public/radar-live.html`: página y MP4 usan el mismo canvas del ecualizador. No existe una segunda plantilla. El reloj del vídeo es determinista: progreso = frame / (frames - 1); la última muestra llega al borde derecho en el último fotograma. Las barras son decorativas y la curva interpola las mediciones. No se reconstruye audiencia no capturada.

## Ejecución

- Instalar dependencias con `npm ci` (requiere permitir sus scripts de instalación).
- `npm test`: SQLite en memoria y salida temporal; incluye render real de 480
  fotogramas/8 segundos, reproducción no duplicada de trabajos, recuperación de
  reservas caducadas y rechazo de medios truncados.
- `npm start`: arranca el colector existente y el trabajador integrado.
- Windows: se puede reutilizar Edge instalado. En otro equipo, Puppeteer necesita
  su Chromium y las dependencias del sistema operativo.
- `BROWSER_PATH`, `FFMPEG_PATH`, `FFPROBE_PATH`: rutas alternativas explícitas.
- `RADAR_NO_SANDBOX=1`: únicamente para contenedores que requieran esta configuración.
- `REPORTS_DIR`: directorio persistente de entregables; por defecto `data/reports`.
- `TEST_DB_PATH` y `TEST_REPORTS_DIR`: solo para pruebas, no configurar en producción.

Una caída durante el render deja el trabajo recuperable. Cada intento tiene una
reserva renovable con propietario; un intento que perdió su reserva no publica
archivos. El proceso no solapa ciclos de render.

El HTML de replay conserva el dibujo. Su carga de resumen comprueba la respuesta
y el cierre de la sesión antes de iniciar una reproducción con media final.

Esta modificación se verificó localmente. Subir a GitHub o desplegar en Render
es una operación separada; el servidor remoto necesita almacenamiento persistente
y un navegador ejecutable. Las pruebas de exportación no certifican retrospectivamente
la exactitud de las muestras obtenidas por versiones anteriores del colector.

El resumen exportado integra los paneles finales y la media grande bajo el mismo ecualizador. La curva usa un único trazo de 0,8 px y una punta de 2,5 px. Los paneles leen el resumen guardado; no cambian los cálculos. Se comprobó igualdad de píxeles de cabecera, paneles y media entre el inicio y el final, además de la reproducción completa del MP4.
