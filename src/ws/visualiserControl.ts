/**
 * Visualiser control channel.
 *
 * Surface:
 *   /ws/visualiser/:runId/control?token=<jwt>
 *
 * Separate from the Pixel Streaming signalling proxy so PRISM-specific
 * control messages never pollute the opaque PS sub-protocol stream the
 * frontend lib consumes. This channel:
 *   - pushes the authoritative single-controller lock state to every
 *     viewer UI (`{ type:'controller', controllerViewerId, you,
 *     youAreController, canControl }`), and
 *   - accepts `{ type:'take' }` / `{ type:'release' }` commands.
 *
 * Auth: the same short-lived signalling JWT used by the signalling proxy
 * (carries `runId`, `tier`, `viewerId`). A `view`-tier token may observe
 * the controller state but its take command is rejected server-side — the
 * lock is authoritative here, not in the client.
 *
 * When the lock changes we notify the agent via `sendSetViewerControlToAgent`
 * for both the demoted and promoted viewer so the agent's per-viewer
 * bridge updates its input gate.
 */
import type { FastifyPluginAsync } from 'fastify';
import { eq } from 'drizzle-orm';
import { db, visualiserRuns, signallingProxyRegistry, type ControlSub, type ViewerTier, type ControlChange } from '@rebus-industries/prism-shared';
import { verifySignallingToken } from '../visualiser/signallingToken.js';
import { sendSetViewerControlToAgent } from './agentSend.js';

function applyChangeToAgent(change: ControlChange): void {
  if (!change.changed || !change.agentSessionId) return;
  if (change.demoted)  sendSetViewerControlToAgent(change.agentSessionId, { runId: change.runId, viewerId: change.demoted,  canControl: false });
  if (change.promoted) sendSetViewerControlToAgent(change.agentSessionId, { runId: change.runId, viewerId: change.promoted, canControl: true });
}

const plugin: FastifyPluginAsync = async (app) => {
  app.get<{ Params: { runId: string }; Querystring: { token?: string } }>(
    '/ws/visualiser/:runId/control',
    { websocket: true },
    async (socket, req) => {
      const childLog = req.log.child({ ws: 'visualiser-control', runId: req.params.runId });
      const token = req.query.token;
      if (typeof token !== 'string' || token.length === 0) {
        socket.close(4401, 'missing token');
        return;
      }
      const verified = verifySignallingToken(token, { expectedRunId: req.params.runId });
      if (!verified.ok) {
        socket.close(4401, verified.error);
        return;
      }
      const row = await db.query.visualiserRuns.findFirst({ where: eq(visualiserRuns.id, req.params.runId) });
      if (!row) { socket.close(4404, 'run not found'); return; }
      if (row.status !== 'streaming') { socket.close(4409, `run is ${row.status}`); return; }

      const runId = row.id;
      const viewerId = verified.payload.viewerId ?? '';
      const tier: ViewerTier = verified.payload.tier === 'view' ? 'view' : 'control';
      if (!viewerId) { socket.close(4400, 'token missing viewerId'); return; }

      const sub: ControlSub = { socket, viewerId, tier };
      signallingProxyRegistry.addControlSub(runId, sub);
      childLog.info({ viewerId, tier }, 'control channel connected');

      socket.on('message', (data) => {
        let msg: { type?: string };
        try { msg = JSON.parse(data.toString()); } catch { return; }
        if (msg.type === 'take') {
          const change = signallingProxyRegistry.takeControl(runId, viewerId, tier);
          if (!change.ok) {
            try { socket.send(JSON.stringify({ type: 'controlError', reason: change.reason ?? 'denied' })); } catch { /* ignore */ }
            return;
          }
          applyChangeToAgent(change);
        } else if (msg.type === 'release') {
          const change = signallingProxyRegistry.releaseControl(runId, viewerId);
          applyChangeToAgent(change);
        }
      });

      socket.on('close', () => {
        signallingProxyRegistry.removeControlSub(runId, sub);
        childLog.info({ viewerId }, 'control channel closed');
      });
      socket.on('error', (err) => childLog.warn({ err }, 'control channel error'));
    },
  );
};

export default plugin;
