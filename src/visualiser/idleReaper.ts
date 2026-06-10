/**
 * Viewer-aware inactivity reaper for visualiser runs.
 *
 * A `streaming` visualiser run pins a GPU + an Unreal Engine process on a
 * workstation for as long as it stays live. Browser viewers connect over the
 * signalling WS proxy (`server/src/ws/signallingProxy.ts`) and are tracked
 * authoritatively in `signallingProxyRegistry` — one entry per
 * `(runId, viewerId)`.
 *
 * What counts as "activity"
 * -------------------------
 * The ONLY safe activity signal is the AUTHORITATIVE one: at least one
 * browser viewer holding an open signalling socket on the run.
 *
 * In PRISM's non-SFU topology the WebRTC media stream AND the input data
 * channel go peer-to-peer (browser ⇄ UE via STUN/TURN) and do NOT traverse
 * the server. So the signalling stream goes quiet a few seconds after WebRTC
 * negotiation completes — even while a viewer is actively WATCHING and
 * interacting. Measuring "recent signalling frames" (or UE input events)
 * would therefore treat a viewer who is merely watching without moving the
 * mouse/keyboard as idle — the classic false positive. Counting OPEN viewer
 * signalling sockets does not have that flaw: the Pixel Streaming frontend
 * keeps its signalling socket open for the entire session (for ICE trickle,
 * renegotiation and keepalive), so a watching viewer always registers as
 * connected and is never reaped.
 *
 * Semantics
 * ---------
 *   - While ≥1 viewer is connected to a run, the run is NEVER reaped.
 *   - When the LAST viewer disconnects, a per-run timer starts. If no viewer
 *     (re)connects within `VISUALISER_IDLE_TIMEOUT_MS`, the run is ended with
 *     a clear reason: `no viewers connected for Ns`.
 *   - Any viewer (re)connect cancels the pending timer, so brief reconnect
 *     gaps (tab reload, signalling-token refresh) never accumulate.
 *   - `VISUALISER_IDLE_TIMEOUT_MS <= 0` disables reaping entirely.
 *
 * This is INDEPENDENT of the START timeout (`VISUALISER_START_TIMEOUT_MS` /
 * `runRegistry.waitFor`), which guards the pre-`streaming` bring-up before a
 * streamer registers. The two timers must not be conflated: the start
 * timeout fails a run that never became live; this reaper ends a run that
 * WAS live but has had no viewers for a while.
 */
import { randomUUID } from 'node:crypto';
import { eq } from 'drizzle-orm';
import {
  db, visualiserRuns,
  sessionRegistry, signallingProxyRegistry,
  broadcastWorkstationUpdate,
  envelope, type CancelVisualisationData,
} from '@rebus-industries/prism-shared';
import { releaseVisualiserSlot } from '../jobs/dispatcher.js';

/**
 * Default idle window: end a run that has had ZERO connected viewers for this
 * long. Chosen to be far longer than any legitimate viewer reconnect gap
 * (which is measured in seconds) so an actively-watched run is never at risk,
 * while still reclaiming a GPU from a genuinely abandoned session. Override
 * (or disable with `0`) via `VISUALISER_IDLE_TIMEOUT_MS`.
 */
const DEFAULT_IDLE_TIMEOUT_MS = 600_000; // 10 minutes

function resolveIdleTimeoutMs(): number {
  const raw = process.env.VISUALISER_IDLE_TIMEOUT_MS;
  if (raw === undefined || raw === '') return DEFAULT_IDLE_TIMEOUT_MS;
  const n = Number(raw);
  if (!Number.isFinite(n) || n < 0) return DEFAULT_IDLE_TIMEOUT_MS;
  return Math.floor(n);
}

/** Minimal logger surface (Fastify's `app.log` satisfies this). */
interface ReaperLogger {
  info(obj: unknown, msg?: string): void;
  warn(obj: unknown, msg?: string): void;
}

const consoleLogger: ReaperLogger = {
  info: (obj, msg) => console.info(msg ?? '', obj),
  warn: (obj, msg) => console.warn(msg ?? '', obj),
};

class VisualiserIdleReaper {
  private timers = new Map<string, NodeJS.Timeout>();
  private idleTimeoutMs = resolveIdleTimeoutMs();
  private log: ReaperLogger = consoleLogger;

  /** Effective idle timeout in ms (0 = disabled). Exposed for diagnostics/tests. */
  get timeoutMs(): number {
    return this.idleTimeoutMs;
  }

  /**
   * Wire up the reaper once at startup: adopt the app logger and subscribe to
   * the signalling registry's per-run viewer-count changes. Idempotent.
   */
  init(log?: ReaperLogger): void {
    if (log) this.log = log;
    signallingProxyRegistry.setViewerCountListener((runId, count) => {
      this.onViewerCount(runId, count);
    });
    this.log.info(
      { idleTimeoutMs: this.idleTimeoutMs, enabled: this.idleTimeoutMs > 0 },
      'visualiser idle reaper initialised',
    );
  }

  /**
   * React to a change in the number of connected viewers for a run.
   *   count > 0 → genuine activity; cancel any pending reap.
   *   count === 0 → no viewers; arm the reap timer (if enabled and not armed).
   */
  onViewerCount(runId: string, count: number): void {
    if (this.idleTimeoutMs <= 0) return; // reaping disabled
    if (count > 0) {
      this.cancel(runId);
      return;
    }
    // Zero viewers — arm the timer if it isn't already counting down.
    if (this.timers.has(runId)) return;
    const timer = setTimeout(() => {
      this.timers.delete(runId);
      void this.reap(runId).catch((err) => {
        this.log.warn({ err, runId }, 'visualiser idle reap failed');
      });
    }, this.idleTimeoutMs);
    // Don't keep the event loop alive solely for a reap timer.
    if (typeof timer.unref === 'function') timer.unref();
    this.timers.set(runId, timer);
  }

  /** Cancel a pending reap (a viewer (re)connected, or the run ended elsewhere). */
  cancel(runId: string): void {
    const t = this.timers.get(runId);
    if (t) {
      clearTimeout(t);
      this.timers.delete(runId);
    }
  }

  /** Number of runs currently counting down (test/diagnostic helper). */
  pendingCount(): number {
    return this.timers.size;
  }

  /**
   * End a run because it has had no connected viewers for the idle window.
   * No-op unless the run is still `streaming` (a run that already ended/failed
   * — or that a viewer rejoined — must not be touched).
   */
  private async reap(runId: string): Promise<void> {
    const seconds = Math.round(this.idleTimeoutMs / 1000);
    const reason = `no viewers connected for ${seconds}s`;

    const row = await db.query.visualiserRuns.findFirst({
      where: eq(visualiserRuns.id, runId),
    });
    if (!row) return;
    if (row.status !== 'streaming') return;

    // Defence-in-depth: if a viewer slipped back in between the timer firing
    // and this query, the registry still knows about them — skip the reap.
    if (signallingProxyRegistry.viewerIds(runId).length > 0) return;

    this.log.info({ runId, reason, idleTimeoutMs: this.idleTimeoutMs }, 'visualiser run reaped (idle)');

    // Best-effort: tell the agent to tear down the orchestrator/UE/Cirrus so
    // the GPU is reclaimed. The agent emits `visualisationEnded`, but we also
    // finalise the row here so the admin UI reflects the reap immediately even
    // if the agent is offline.
    if (row.agentSessionId) {
      const conn = sessionRegistry.getAgent(row.agentSessionId);
      if (conn && conn.socket.readyState === conn.socket.OPEN) {
        const cancel: CancelVisualisationData = { runId, reason };
        try {
          conn.socket.send(JSON.stringify(envelope('cancelVisualisation', cancel, randomUUID())));
        } catch (err) {
          this.log.warn({ err, runId }, 'idle reap: cancelVisualisation send failed');
        }
      }
    }

    await db
      .update(visualiserRuns)
      .set({ status: 'ended', failureReason: 'idle_no_viewers', endedAt: new Date(), updatedAt: new Date() })
      .where(eq(visualiserRuns.id, runId));

    if (row.workstationId) {
      await releaseVisualiserSlot(row.workstationId).catch(() => null);
      broadcastWorkstationUpdate({ id: row.workstationId, visualiserRunEnded: runId, reason });
    }

    signallingProxyRegistry.closeRun(runId, 1000, reason);
  }
}

export const visualiserIdleReaper = new VisualiserIdleReaper();

/** Convenience wrapper used by the WS gateway at startup. */
export function initVisualiserIdleReaper(log?: ReaperLogger): void {
  visualiserIdleReaper.init(log);
}
