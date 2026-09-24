/**
 * session-state.js
 * Lógica pura de estado de sesión — sin efectos secundarios, sin DB, testeable directamente.
 * Importada por collector.js (producción) y test_engine_cases.js (tests reales).
 */

const { randomUUID } = require('node:crypto');

/**
 * Resuelve la acción a tomar para una sesión dado el estado activo y una nueva observación.
 *
 * @param {object|null} active  - Sesión activa en memoria (puede ser null si no hay ninguna)
 * @param {object} observation  - { broadcastId, startedAt, observedAt }
 * @returns {{ action: string, session?: object, patch?: object }}
 *
 * Acciones posibles:
 *  - 'create'          → nueva sesión (ninguna activa)
 *  - 'close_and_create'→ ID oficial distinto detectado; cerrar actual y abrir otra
 *  - 'identity_pending'→ aparece ID oficial pero no podemos confirmar continuidad aún
 *  - 'update'          → asociar ID oficial a sesión existente (misma emisión confirmada)
 *  - 'continue'        → sin cambios; seguir con la sesión activa
 */
function resolveSession(active, observation) {
  const broadcastId =
    observation.broadcastId == null
      ? null
      : String(observation.broadcastId);

  const startedAt = Number.isFinite(observation.startedAt)
    ? observation.startedAt
    : null;

  // No hay sesión activa → crear una nueva
  if (!active) {
    return {
      action: 'create',
      session: {
        id: randomUUID(),
        broadcastId,
        startedAt,
        firstObservedAt: observation.observedAt
      }
    };
  }

  // Dos IDs oficiales distintos → otra emisión
  if (
    active.broadcastId &&
    broadcastId &&
    active.broadcastId !== broadcastId
  ) {
    return { action: 'close_and_create' };
  }

  // Aparece por primera vez el ID oficial en una sesión provisional o sin ID
  if (!active.broadcastId && broadcastId) {
    const sameStart =
      active.startedAt != null &&
      startedAt != null &&
      active.startedAt === startedAt;

    // Sin la misma fecha de inicio no podemos confirmar continuidad
    if (!sameStart) {
      return { action: 'identity_pending' };
    }

    return {
      action: 'update',
      patch: { broadcastId } // El active.id interno NO cambia
    };
  }

  return { action: 'continue' };
}

/**
 * Calcula media ponderada y horas-viewer observadas excluyendo huecos conocidos.
 *
 * @param {Array<{timestamp:number, viewers:number}>} samples
 * @param {Array<{started_at:number, ended_at:number|null}>} gaps - intervalos sin cobertura
 * @param {number} maxGapMs  - ms máximos entre muestras antes de considerar hueco implícito (default 120 000)
 * @returns {{ averageViewers: number|null, observedSeconds: number, observedViewerHours: number }}
 */
function calculateObservedStats(samples, gaps = [], maxGapMs = 120_000) {
  let viewerSeconds = 0;
  let observedSeconds = 0;

  for (let i = 1; i < samples.length; i++) {
    const a = samples[i - 1];
    const b = samples[i];
    const dtMs = b.timestamp - a.timestamp;

    const validValues = [a.viewers, b.viewers].every(
      value => Number.isFinite(value) && value >= 0
    );

    // Excluir si este intervalo cruza un hueco registrado
    const crossesGap = gaps.some(gap =>
      gap.started_at < b.timestamp &&
      (gap.ended_at ?? Infinity) > a.timestamp
    );

    if (!validValues || dtMs <= 0 || dtMs > maxGapMs || crossesGap) continue;

    const seconds = dtMs / 1000;
    viewerSeconds += ((a.viewers + b.viewers) / 2) * seconds;
    observedSeconds += seconds;
  }

  return {
    // null = cobertura insuficiente (no cero — el front debe mostrarlo diferente)
    averageViewers: observedSeconds > 0 ? viewerSeconds / observedSeconds : null,
    observedSeconds,
    observedViewerHours: viewerSeconds / 3600
  };
}

module.exports = { resolveSession, calculateObservedStats };
