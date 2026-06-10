/**
 * Pixel Streaming signalling proxy.
 *
 * Surface:
 *   /ws/visualiser/:runId/signalling?token=<jwt>
 *
 * Auth: short-lived HS256 JWT minted by
 *   POST /api/visualiser/streams/:runId/signalling-token
 * (see ../visualiser/signallingToken.ts). Rejected with 401 on any
 * sig/exp/runId mismatch.
 *
 * Pipeline:
 *   Browser  ⇄  PRISM server  ⇄  Agent WS  ⇄  local Cirrus on workstation
 *
 * PRISM does not parse the Pixel Streaming WebRTC sub-protocol. Every
 * browser frame is wrapped into a `signallingFrame` envelope (with
 * either `payload` for text or `payloadB64` for binary) and forwarded
 * to the agent. The agent unwraps the envelope and writes to its
 * local Cirrus WS; the reverse direction does the same. See
 * `PRISM/agent/src/PRISM.Agent/Ws/AgentMessageDispatcher.cs`.
 *
 * Lifecycle:
 *   - On WS open we authenticate, then look up the run row.
 *   - We refuse to connect unless `status='streaming'` (no point
 *     attempting WebRTC negotiation against an importing run).
 *   - We register the browser socket in the proxy registry, keyed by
 *     (runId, viewerId). Each viewer is an INDEPENDENT Pixel Streaming
 *     player (its own local Cirrus/Wilbur WS on the agent), so inbound
 *     agent frames are routed to the single matching viewer, never
 *     broadcast — that fan-out was what froze a second viewer.
 *   - On close we drop the socket, tell the agent to tear down that
 *     viewer's Wilbur player, and release the controller lock if the
 *     departing viewer held it.
 *
 * The companion `agentProtocol.ts` calls
 * `signallingProxyRegistry.forwardAgentToBrowser(frame)` for every
 * inbound `signallingFrame` from the agent.
 */
import type { FastifyPluginAsync } from 'fastify';
import { eq } from 'drizzle-orm';
import { randomUUID } from 'node:crypto';
import { db, visualiserRuns, signallingProxyRegistry, type BrowserConn, type ViewerTier, type SignallingFrameData } from '@rebus-industries/prism-shared';
import {
  sendSignallingFrameToAgent,
  sendSignallingViewerCloseToAgent,
  sendSetViewerControlToAgent,
} from './agentSend.js';
import { verifySignallingToken } from '../visualiser/signallingToken.js';

const plugin: FastifyPluginAsync = async (app) => {
  app.get<{ Params: { runId: string }; Querystring: { token?: string } }>(
    '/ws/visualiser/:runId/signalling',
    { websocket: true },
    async (socket, req) => {
      const childLog = req.log.child({ ws: 'signalling-proxy', runId: req.params.runId });
      const token = req.query.token;
      if (typeof token !== 'string' || token.length === 0) {
        childLog.warn('missing signalling token');
        socket.close(4401, 'missing token');
        return;
      }
      const verified = verifySignallingToken(token, { expectedRunId: req.params.runId });
      if (!verified.ok) {
        childLog.warn({ error: verified.error }, 'signalling token rejected');
        socket.close(4401, verified.error);
        return;
      }

      const row = await db.query.visualiserRuns.findFirst({
        where: eq(visualiserRuns.id, req.params.runId),
      });
      if (!row) {
        socket.close(4404, 'run not found');
        return;
      }
      if (row.status !== 'streaming') {
        socket.close(4409, `run is ${row.status}`);
        return;
      }
      if (!row.agentSessionId) {
        socket.close(4503, 'no agent assigned to run');
        return;
      }

      // Identity from the JWT: each token represents one viewer "seat".
      // Legacy tokens (pre-multi-viewer) carry no viewerId/tier — mint a
      // per-socket viewerId and default the tier to `control` so the
      // owner/admin viewer keeps driving the viewport as before.
      const viewerId: string = verified.payload.viewerId ?? randomUUID();
      const tier: ViewerTier = verified.payload.tier === 'view' ? 'view' : 'control';

      const conn: BrowserConn = {
        socket,
        agentSessionId: row.agentSessionId,
        runId: row.id,
        viewerId,
        tier,
      };
      signallingProxyRegistry.add(conn);
      childLog.info({ agentSessionId: conn.agentSessionId, viewerId, tier }, 'browser signalling ws connected');

      // If this is a control-tier viewer and nobody holds the lock yet,
      // auto-grant it so the first/owner viewer can drive immediately.
      // Always push the explicit control state for this viewer to the
      // agent so its per-viewer bridge gates input correctly (the bridge
      // defaults to "allow" for legacy single-viewer runs, so a
      // non-controller MUST be told `false`).
      const granted = signallingProxyRegistry.autoGrantIfVacant(conn.runId, viewerId, tier);
      const isController = signallingProxyRegistry.controllerState(conn.runId).controllerViewerId === viewerId;
      sendSetViewerControlToAgent(conn.agentSessionId, { runId: conn.runId, viewerId, canControl: isController });
      void granted;

      socket.on('message', (data, isBinary) => {
        const frame: SignallingFrameData = isBinary
          ? { runId: conn.runId, viewerId: conn.viewerId, payloadB64: (data as Buffer).toString('base64') }
          : { runId: conn.runId, viewerId: conn.viewerId, payload: data.toString() };
        const ok = sendSignallingFrameToAgent(conn.agentSessionId, frame);
        if (!ok) {
          childLog.warn('agent send failed; closing browser socket');
          try { socket.close(1011, 'agent unreachable'); } catch { /* ignore */ }
        }
      });

      socket.on('close', (code, reason) => {
        const { wasController } = signallingProxyRegistry.remove(conn);
        // Tear down this viewer's dedicated Wilbur player on the agent so
        // the streamer drops the peer (no stale players across tabs).
        sendSignallingViewerCloseToAgent(conn.agentSessionId, { runId: conn.runId, viewerId: conn.viewerId });
        if (wasController) {
          // The controller left — clear its input gate on the agent too.
          sendSetViewerControlToAgent(conn.agentSessionId, { runId: conn.runId, viewerId: conn.viewerId, canControl: false });
        }
        childLog.info({ code, reason: reason.toString(), viewerId: conn.viewerId }, 'browser signalling ws closed');
      });

      socket.on('error', (err) => {
        childLog.warn({ err }, 'browser signalling ws error');
      });
    },
  );
};

export default plugin;
