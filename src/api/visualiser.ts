/**
 * /api/visualiser/* — start, poll, stop, list Pixel Streaming runs.
 *
 * Surface (see [.cursor/plans/prism_visualiser_role.plan.md], "Portal → PRISM API"):
 *
 *   POST   /api/visualiser/streams
 *     Auth: requireApiKey + requireScope('visualiser:create_stream')
 *     Synchronous: blocks until the agent reports the run ready
 *     (warm ~2-3 s, cold ~60-90 s, timeout default 180 s). Returns the
 *     `prism-visualiser/ready/v1` envelope.
 *
 *   GET    /api/visualiser/streams
 *     Auth: requireAuth
 *     List recent runs (newest first); admin SPA polls this for the
 *     Visualiser page.
 *
 *   GET    /api/visualiser/streams/:runId
 *     Auth: requireAuth
 *     Single-row status poll; surfaces the latest persisted state.
 *
 *   DELETE /api/visualiser/streams/:runId
 *     Auth: requireApiKey (matching `requested_by_api_key_id`) OR admin
 *     Sends `cancelVisualisation` to the agent and marks the row `ended`.
 *
 *   POST   /api/visualiser/streams/:runId/signalling-token
 *     Auth: requireApiKey OR admin (must own the run)
 *     Mints a short-lived HS256 JWT the browser passes to the
 *     signalling WS at `?token=…`. See ws/signallingProxy.ts.
 *
 *   GET    /api/visualiser/workstations
 *     Auth: requireAdmin
 *     Lists eligible workstations (`can_visualise = true` + online),
 *     feeding the admin UI "Start new stream" dropdown.
 *
 * Lifecycle (POST happy path):
 *   1. Validate body.
 *   2. Insert `visualiser_runs` row with `status: 'queued'`,
 *      `requestedByApiKeyId` if applicable.
 *   3. `tryDispatchVisualisation()` reserves a workstation atomically
 *      and sends the `startVisualisation` envelope.
 *   4. Register a Promise waiter (see `runRegistry.ts`); the inbound
 *      `visualisationReady` / `visualisationFailed` WS handler resolves
 *      or rejects it (see ws/agentProtocol.ts).
 *   5. On resolve: build the `prism-visualiser/ready/v1` response,
 *      persist `streamerId` / `signallingUrl` / `playerUrl`, return 200.
 *   6. On reject: persist failureReason + return 502 / 504.
 */
import { randomUUID } from 'node:crypto';
import type { FastifyPluginAsync, FastifyRequest } from 'fastify';
import { z } from 'zod';
import { and, asc, desc, eq, gt, inArray, isNull } from 'drizzle-orm';
import {
  db, agentSessions, visualiserRunLogs, visualiserRuns, visualiserShareLinks, workstations, type VisualiserRun,
  requireAdmin, requireAuth, requireScope,
  resolveProvenance,
  appendVisualiserRunLog,
  envelope, type CancelVisualisationData,
  sessionRegistry,
  broadcastWorkstationUpdate,
  visualiserRunRegistry,
} from '@rebus-industries/prism-shared';
import { releaseVisualiserSlot, tryDispatchVisualisation } from '../jobs/dispatcher.js';
import { visualiserIdleReaper } from '../visualiser/idleReaper.js';
import { generateTurnCredential } from '../visualiser/turnCredentials.js';
import { issueSignallingToken } from '../visualiser/signallingToken.js';
import { mintShareToken, hashShareToken } from '../visualiser/shareLinks.js';

// Cold full-editor runs open the heavyweight Unreal Editor and, on a first
// open of a freshly-cached C++ project, pay the shader-compile + DDC build
// cost before the streamer registers — several minutes. The old 180 s default
// failed those runs ("start exceeded 180000ms") even though the editor opened.
// Default is now 600 s; still overridable via VISUALISER_START_TIMEOUT_MS.
const START_TIMEOUT_MS = Number(process.env.VISUALISER_START_TIMEOUT_MS ?? 600_000);

const PUBLIC_BASE_URL =
  process.env.PUBLIC_BASE_URL
  ?? process.env.PRISM_PUBLIC_URL
  ?? 'https://prism.rebus.industries';

const READY_SCHEMA_VERSION = 'prism-visualiser/ready/v1';
const FAILED_SCHEMA_VERSION = 'prism-visualiser/failed/v1';

const startBody = z.object({
  projectId:   z.string().min(1),
  modelId:     z.string().min(1),
  /** Human-readable ORBIT model path (e.g. 'building'). Required when importMode='tree'. */
  modelName:   z.string().optional(),
  /**
   * 'tree'   = the modelId is a parent with no versions of its own; the UE connector
   *            calls OrbitImportTree(projectId, modelName) to pull all submodels.
   * 'single' = default; a specific version is resolved and imported.
   */
  importMode:  z.enum(['single', 'tree']).default('single'),
  versionId:   z.string().min(1).optional(),
  /** Optional ORBIT target — defaults to `prod` to match the jobs surface. */
  orbitTarget: z.enum(['prod', 'dev']).default('prod'),
  /** Pin the run to a specific workstation; when omitted the dispatcher picks the least-loaded eligible box. */
  preferredWorkstationId: z.string().uuid().optional(),
  /** Reserved for future use; the portal contract documents this for status callbacks. */
  callbackUrl: z.string().url().optional(),
  templateTag: z.string().optional(),
  ttlSeconds:  z.number().int().positive().optional(),
});

const listQuery = z.object({
  status: z.string().optional(),  // comma-separated list of statuses
  limit:  z.coerce.number().int().min(1).max(500).default(50),
  offset: z.coerce.number().int().min(0).default(0),
});

const shareBody = z.object({
  tier: z.enum(['view', 'control']).default('view'),
  // Optional TTL on top of the run-lifetime auto-expiry. Capped at 24h.
  expiresInSeconds: z.number().int().positive().max(86_400).optional(),
});

const exchangeBody = z.object({
  shareToken: z.string().min(1),
  // Caller-supplied stable per-session viewer id so identity survives JWT
  // refreshes (the player re-mints every ~5 min; a new viewerId each time
  // would orphan its Wilbur player + controller lock). Optional — a random
  // one is minted when absent.
  viewerId: z.string().min(1).max(64).optional(),
});

const tokenBody = z.object({
  viewerId: z.string().min(1).max(64).optional(),
});

/* -------------------------------------------------------------------------- */
/* Helpers                                                                    */
/* -------------------------------------------------------------------------- */

function buildPlayerUrl(runId: string): string {
  // Admin SPA uses hash-history routing (see web/src/admin/main.ts), so the
  // deep-link is `…/admin/#/visualiser/<runId>`. Phase I will swap this for a
  // dedicated `/visualiser/<runId>/player` static page; until then the admin
  // UI's VisualiserViewer.vue handles the embed.
  return `${PUBLIC_BASE_URL.replace(/\/+$/, '')}/admin/#/visualiser/${runId}`;
}

function buildSignallingUrl(runId: string): string {
  const base = PUBLIC_BASE_URL.replace(/\/+$/, '');
  // Swap http://… → ws://…, https://… → wss://…. Leave non-http schemes
  // alone so the override env var can point at a development relay.
  const wsBase = base.replace(/^http:\/\//, 'ws://').replace(/^https:\/\//, 'wss://');
  return `${wsBase}/ws/visualiser/${runId}/signalling`;
}

/** Public URL of the PRISM-hosted standalone viewer page for a share link. */
function buildShareViewerUrl(runId: string, shareToken: string): string {
  const base = PUBLIC_BASE_URL.replace(/\/+$/, '');
  return `${base}/viewer/#/${runId}?st=${encodeURIComponent(shareToken)}`;
}

function toPublicRun(row: VisualiserRun, opts?: { withTurn?: boolean; workstationName?: string | null }) {
  // Phase I: when a caller is about to open the live player (i.e. the
  // single-row GET on `/streams/:runId`), mint a fresh TURN bundle and
  // attach it to the response. We deliberately do NOT mint credentials
  // for the list endpoint — that path is admin polling and the bundle
  // would be unused (and would leak into shared SSE caches if we ever
  // broadcast it). The TURN secret has a 24h TTL by default, so the
  // admin clicking "Refresh" naturally renews it.
  const turn = opts?.withTurn && row.status === 'streaming'
    ? generateTurnCredential({ runId: row.id })
    : undefined;
  return {
    id: row.id,
    status: row.status,
    orbitTarget: row.orbitTarget,
    projectId: row.projectId,
    modelId: row.modelId,
    modelName: row.modelName,
    importMode: row.importMode,
    versionId: row.versionId,
    templateTag: row.templateTag,
    workstationId: row.workstationId,
    workstationName: opts?.workstationName ?? null,
    agentSessionId: row.agentSessionId,
    signallingUrl: row.signallingUrl,
    playerUrl: row.playerUrl,
    streamerId: row.streamerId,
    failureReason: row.failureReason,
    error: row.error,
    ttlSeconds: row.ttlSeconds,
    submittedBy: row.submittedBy,
    requestedByApiKeyId: row.requestedByApiKeyId,
    originKind: row.originKind,
    originAddress: row.originAddress,
    originPrincipal: row.originPrincipal,
    createdAt: row.createdAt,
    updatedAt: row.updatedAt,
    dispatchedAt: row.dispatchedAt,
    readyAt: row.readyAt,
    endedAt: row.endedAt,
    ...(turn !== undefined ? { turn } : {}),
  };
}

function principalSubject(req: FastifyRequest): { submittedBy: string; requestedByApiKeyId: string | null } {
  const p = req.principal;
  if (!p) return { submittedBy: 'anonymous', requestedByApiKeyId: null };
  switch (p.kind) {
    case 'apiKey':       return { submittedBy: `apiKey:${p.apiKeyId}`, requestedByApiKeyId: p.apiKeyId };
    case 'adminSession': return { submittedBy: `admin:${p.username}`, requestedByApiKeyId: null };
    case 'orbitUser':    return { submittedBy: `orbit:${p.userId}`, requestedByApiKeyId: null };
  }
}

async function ownerCanCancel(run: VisualiserRun, req: FastifyRequest): Promise<boolean> {
  const p = req.principal;
  if (!p) return false;
  if (p.kind === 'adminSession') return true;
  if (p.kind === 'apiKey') {
    // The strict-FK path; pre-Phase-G runs may not have the column set
    // (they predate the migration), so fall back to `submittedBy`
    // string match for backwards compat.
    if (run.requestedByApiKeyId) return run.requestedByApiKeyId === p.apiKeyId;
    return run.submittedBy === `apiKey:${p.apiKeyId}`;
  }
  return false;
}

/* -------------------------------------------------------------------------- */
/* Plugin                                                                     */
/* -------------------------------------------------------------------------- */

const plugin: FastifyPluginAsync = async (app) => {
  /* ---------- POST /api/visualiser/streams ---------- */
  // Portal-facing route. Requires the visualiser:create_stream scope —
  // admin sessions and ORBIT bearers bypass scope checks (see
  // requireScope() docs), so the admin SPA "Start new stream" button
  // hits this same endpoint via cookie auth.
  app.post('/streams', {
    preHandler: [requireAuth, requireScope('visualiser:create_stream')],
  }, async (req, reply) => {
    const parsed = startBody.safeParse(req.body);
    if (!parsed.success) {
      return reply.code(400).send({ error: 'invalid body', issues: parsed.error.issues });
    }

    const { submittedBy, requestedByApiKeyId } = principalSubject(req);
    const provenance = resolveProvenance(req);

    const inserted = await db
      .insert(visualiserRuns)
      .values({
        status: 'queued',
        orbitTarget: parsed.data.orbitTarget,
        projectId: parsed.data.projectId,
        modelId: parsed.data.modelId,
        modelName: parsed.data.modelName ?? null,
        importMode: parsed.data.importMode ?? 'single',
        versionId: parsed.data.versionId ?? null,
        templateTag: parsed.data.templateTag ?? null,
        ttlSeconds: parsed.data.ttlSeconds ?? null,
        callbackUrl: parsed.data.callbackUrl ?? null,
        submittedBy,
        requestedByApiKeyId,
        originKind: provenance.originKind,
        originAddress: provenance.originAddress,
        originPrincipal: provenance.originPrincipal,
      })
      .returning();
    const run = inserted[0]!;
    const runId = run.id;

    const originLabel = provenance.originPrincipal
      ? `${provenance.originKind} (${provenance.originPrincipal})`
      : provenance.originKind;
    await appendVisualiserRunLog(
      runId,
      `run requested by ${originLabel}${provenance.originAddress ? ` from ${provenance.originAddress}` : ''} — project ${parsed.data.projectId} / model ${parsed.data.modelId}`,
      { log: req.log },
    );

    // Register the waiter BEFORE dispatching so an extremely fast
    // agent (or a test double) can't resolve the runId before we have
    // a listener.
    const waiter = visualiserRunRegistry.waitFor(runId, START_TIMEOUT_MS);

    const dispatch = await tryDispatchVisualisation(runId, req.log, parsed.data.preferredWorkstationId);
    if (!dispatch.dispatched) {
      visualiserRunRegistry.abandon(runId);
      const failureReason = dispatch.error;
      await appendVisualiserRunLog(runId, `dispatch failed (${dispatch.error}): ${dispatch.reason}`, { level: 'error', log: req.log });
      await db
        .update(visualiserRuns)
        .set({ status: 'failed', failureReason, error: dispatch.reason, updatedAt: new Date(), endedAt: new Date() })
        .where(eq(visualiserRuns.id, runId));
      const status =
        dispatch.error === 'no_workstation_available' || dispatch.error === 'all_workstations_busy' ? 503
        : dispatch.error === 'misconfigured' ? 500
        // The caller asked for a project/model/version that ORBIT can't
        // resolve (bad id, wrong target, not shared, or no committed
        // version). 422 = well-formed request, unresolvable reference —
        // distinct from 500 so the portal stops reporting "PRISM
        // misconfigured / check the workstations" for a bad project id.
        : dispatch.error === 'version_unavailable' ? 422
        : 502;
      return reply.code(status).send({
        schema: FAILED_SCHEMA_VERSION,
        runId,
        error: 'dispatch_failed',
        code: dispatch.error,
        message: dispatch.reason,
      });
    }

    // Block until the agent reports ready / failed or the timeout fires.
    let readyEvent;
    try {
      readyEvent = await waiter;
    } catch (failure) {
      const f = failure as { code: string; message: string; stack?: string };
      const isTimeout = f.code === 'start_timeout';
      await appendVisualiserRunLog(runId, `start failed (${f.code}): ${f.message}`, { level: 'error', log: req.log });
      await db
        .update(visualiserRuns)
        .set({
          status: 'failed',
          failureReason: f.code,
          error: f.message,
          endedAt: new Date(),
          updatedAt: new Date(),
        })
        .where(eq(visualiserRuns.id, runId));

      // Roll back the workstation slot reservation either way — the
      // agent may have crashed mid-import, or simply not responded
      // within the deadline.
      if (dispatch.workstationId) await releaseVisualiserSlot(dispatch.workstationId).catch(() => null);

      if (isTimeout) {
        // Best-effort cancel — the agent may eventually wake up and
        // hand us a `visualisationReady` that we then ignore. The
        // registry's `abandon` keeps state tidy.
        try {
          const conn = sessionRegistry.getAgent(dispatch.agentSessionId);
          if (conn) {
            const cancel: CancelVisualisationData = { runId, reason: 'start_timeout' };
            conn.socket.send(JSON.stringify(envelope('cancelVisualisation', cancel, randomUUID())));
          }
        } catch (err) {
          req.log.warn({ err, runId }, 'cancelVisualisation send failed after timeout');
        }
        return reply.code(504).send({
          schema: FAILED_SCHEMA_VERSION,
          runId,
          error: 'visualisation_failed',
          code: 'start_timeout',
          message: `start exceeded ${START_TIMEOUT_MS}ms`,
        });
      }

      return reply.code(502).send({
        schema: FAILED_SCHEMA_VERSION,
        runId,
        error: 'visualisation_failed',
        code: f.code,
        message: f.message,
      });
    }

    // Happy path: build the portal contract response. The agent gave
    // us the local Cirrus URL in `readyEvent.signallingUrl` — we
    // intentionally do not surface that to the caller; the portal
    // talks to PRISM's server-side proxy.
    const signallingUrl = buildSignallingUrl(runId);
    const playerUrl = buildPlayerUrl(runId);
    const turn = generateTurnCredential({ runId });

    await db
      .update(visualiserRuns)
      .set({
        status: 'streaming',
        signallingUrl,
        playerUrl,
        streamerId: readyEvent.streamerId ?? null,
        readyAt: new Date(),
        updatedAt: new Date(),
      })
      .where(eq(visualiserRuns.id, runId));
    await appendVisualiserRunLog(runId, `stream is live (streamerId ${readyEvent.streamerId ?? 'unknown'})`, { log: req.log });

    if (!turn) {
      req.log.warn({ runId }, 'TURN_SECRET unset; returning turn: null sentinel (Phase H wires the real secret)');
    }

    return reply.send({
      schema: READY_SCHEMA_VERSION,
      runId,
      status: 'streaming',
      signallingUrl,
      playerUrl,
      streamerId: readyEvent.streamerId,
      turn,
    });
  });

  /* ---------- GET /api/visualiser/streams ---------- */
  app.get<{ Querystring: unknown }>('/streams', {
    preHandler: requireAuth,
  }, async (req, reply) => {
    const parsed = listQuery.safeParse(req.query);
    if (!parsed.success) return reply.code(400).send({ error: 'invalid query', issues: parsed.error.issues });
    const filterStatuses = parsed.data.status
      ? parsed.data.status.split(',').map((s) => s.trim()).filter(Boolean)
      : null;
    const whereClause = filterStatuses && filterStatuses.length > 0
      ? inArray(visualiserRuns.status, filterStatuses)
      : undefined;
    const rows = await db
      .select()
      .from(visualiserRuns)
      .where(whereClause)
      .orderBy(desc(visualiserRuns.createdAt))
      .limit(parsed.data.limit)
      .offset(parsed.data.offset);
    // Resolve the friendly node name for each run's workstation in one query
    // so the admin table can show "PC02" instead of a bare UUID prefix.
    const wsIds = [...new Set(rows.map((r) => r.workstationId).filter((id): id is string => !!id))];
    const wsNameById = new Map<string, string>();
    if (wsIds.length > 0) {
      const wsRows = await db
        .select({ id: workstations.id, nodeName: workstations.nodeName })
        .from(workstations)
        .where(inArray(workstations.id, wsIds));
      for (const w of wsRows) wsNameById.set(w.id, w.nodeName);
    }
    return {
      runs: rows.map((row) => toPublicRun(row, {
        workstationName: row.workstationId ? wsNameById.get(row.workstationId) ?? null : null,
      })),
      limit: parsed.data.limit,
      offset: parsed.data.offset,
    };
  });

  /* ---------- GET /api/visualiser/streams/:runId ---------- */
  app.get<{ Params: { runId: string } }>('/streams/:runId', {
    preHandler: requireAuth,
  }, async (req, reply) => {
    const row = await db.query.visualiserRuns.findFirst({ where: eq(visualiserRuns.id, req.params.runId) });
    if (!row) return reply.code(404).send({ error: 'not found' });
    const workstationName = row.workstationId
      ? (await db.query.workstations.findFirst({ where: eq(workstations.id, row.workstationId) }))?.nodeName ?? null
      : null;
    // Phase I: include a freshly-minted TURN bundle so the admin viewer
    // can wire it into the browser RTCPeerConnection. See toPublicRun
    // for the rationale on why we only mint here and not on the list
    // endpoint.
    return toPublicRun(row, { withTurn: true, workstationName });
  });

  /* ---------- GET /api/visualiser/streams/:runId/logs ---------- */
  // Per-run lifecycle log lines (server + agent). Backs the expandable log
  // panel in the admin Visualiser viewer. `since` is a numeric cursor on the
  // monotonic id so the UI can poll for just the new lines.
  app.get<{ Params: { runId: string }; Querystring: { since?: string } }>('/streams/:runId/logs', {
    preHandler: requireAuth,
  }, async (req, reply) => {
    const row = await db.query.visualiserRuns.findFirst({ where: eq(visualiserRuns.id, req.params.runId) });
    if (!row) return reply.code(404).send({ error: 'not found' });
    const sinceId = req.query.since ? Number(req.query.since) : 0;
    const lines = await db
      .select()
      .from(visualiserRunLogs)
      .where(
        sinceId > 0
          ? and(eq(visualiserRunLogs.runId, req.params.runId), gt(visualiserRunLogs.id, sinceId))
          : eq(visualiserRunLogs.runId, req.params.runId),
      )
      .orderBy(asc(visualiserRunLogs.id))
      .limit(2000);
    return { logs: lines };
  });

  /* ---------- DELETE /api/visualiser/streams/:runId ---------- */
  app.delete<{ Params: { runId: string } }>('/streams/:runId', {
    preHandler: requireAuth,
  }, async (req, reply) => {
    const row = await db.query.visualiserRuns.findFirst({ where: eq(visualiserRuns.id, req.params.runId) });
    if (!row) return reply.code(404).send({ error: 'not found' });
    if (!(await ownerCanCancel(row, req))) return reply.code(403).send({ error: 'forbidden' });
    if (row.status === 'ended' || row.status === 'failed') {
      return reply.code(409).send({ error: `run is already ${row.status}` });
    }

    // Best-effort: send cancelVisualisation to the agent. The agent
    // emits `visualisationEnded` when the orchestrator exits; that
    // hits the WS handler and finalises the row. We optimistically
    // set status=ended here so the admin SPA reflects the click
    // immediately even if the agent is offline.
    if (row.agentSessionId) {
      const conn = sessionRegistry.getAgent(row.agentSessionId);
      if (conn && conn.socket.readyState === conn.socket.OPEN) {
        const cancel: CancelVisualisationData = { runId: row.id, reason: 'cancelled by operator' };
        try {
          conn.socket.send(JSON.stringify(envelope('cancelVisualisation', cancel, randomUUID())));
        } catch (err) {
          req.log.warn({ err, runId: row.id }, 'cancelVisualisation send failed');
        }
      }
    }
    // Operator cancel is terminal — drop any pending idle-reap countdown.
    visualiserIdleReaper.cancel(row.id);
    const stopBy = resolveProvenance(req);
    await appendVisualiserRunLog(
      row.id,
      `stopped by ${stopBy.originPrincipal ? `${stopBy.originKind} (${stopBy.originPrincipal})` : stopBy.originKind}`,
      { level: 'warn', log: req.log },
    );
    await db
      .update(visualiserRuns)
      .set({ status: 'ended', endedAt: new Date(), updatedAt: new Date() })
      .where(eq(visualiserRuns.id, row.id));
    if (row.workstationId) {
      await releaseVisualiserSlot(row.workstationId).catch(() => null);
      broadcastWorkstationUpdate({ id: row.workstationId, visualiserRunEnded: row.id });
    }
    return { ok: true };
  });

  /* ---------- POST /api/visualiser/streams/:runId/signalling-token ---------- */
  app.post<{ Params: { runId: string } }>('/streams/:runId/signalling-token', {
    preHandler: requireAuth,
  }, async (req, reply) => {
    const row = await db.query.visualiserRuns.findFirst({ where: eq(visualiserRuns.id, req.params.runId) });
    if (!row) return reply.code(404).send({ error: 'not found' });
    if (!(await ownerCanCancel(row, req))) return reply.code(403).send({ error: 'forbidden' });
    if (row.status !== 'streaming' && row.status !== 'importing') {
      return reply.code(409).send({ error: `run is ${row.status}` });
    }
    const tokenParsed = tokenBody.safeParse(req.body ?? {});
    if (!tokenParsed.success) return reply.code(400).send({ error: 'invalid body', issues: tokenParsed.error.issues });
    try {
      const subject = req.principal?.kind === 'apiKey' ? req.principal.apiKeyId
                    : req.principal?.kind === 'adminSession' ? `admin:${req.principal.username}`
                    : undefined;
      // Owner/admin tokens default to `control` tier so the admin viewer
      // keeps driving the viewport (the proxy auto-grants the lock to the
      // first control-tier viewer). A caller-supplied viewerId keeps the
      // seat stable across token refreshes.
      const { token, exp, viewerId, tier } = issueSignallingToken({
        runId: row.id, subject, tier: 'control', viewerId: tokenParsed.data.viewerId,
      });
      return { token, exp, viewerId, tier };
    } catch (err) {
      const msg = (err as Error).message;
      req.log.error({ err, runId: row.id }, 'failed to mint signalling token');
      return reply.code(503).send({ error: 'signalling_token_unavailable', message: msg });
    }
  });

  /* ---------- POST /api/visualiser/streams/:runId/shares ---------- */
  // Mint a share link for a streaming run. Auth: run creator (matching
  // api key) OR admin session OR an api key with `visualiser:join_stream`.
  app.post<{ Params: { runId: string }; Body: unknown }>('/streams/:runId/shares', {
    preHandler: requireAuth,
  }, async (req, reply) => {
    const row = await db.query.visualiserRuns.findFirst({ where: eq(visualiserRuns.id, req.params.runId) });
    if (!row) return reply.code(404).send({ error: 'not found' });
    const canMint = (await ownerCanCancel(row, req))
      || (req.principal?.kind === 'apiKey' && req.principal.scopes.includes('visualiser:join_stream'));
    if (!canMint) return reply.code(403).send({ error: 'forbidden' });
    if (row.status !== 'streaming') return reply.code(409).send({ error: `run is ${row.status}` });

    const body = shareBody.safeParse(req.body ?? {});
    if (!body.success) return reply.code(400).send({ error: 'invalid body', issues: body.error.issues });

    const { plaintext, hash } = mintShareToken();
    const expiresAt = body.data.expiresInSeconds
      ? new Date(Date.now() + body.data.expiresInSeconds * 1000)
      : null;
    const createdBy = req.principal?.kind === 'apiKey' ? `apiKey:${req.principal.apiKeyId}`
                    : req.principal?.kind === 'adminSession' ? `admin:${req.principal.username}`
                    : null;
    const inserted = await db.insert(visualiserShareLinks).values({
      runId: row.id, tokenHash: hash, tier: body.data.tier, createdBy, expiresAt,
    }).returning();
    const link = inserted[0]!;
    return reply.code(201).send({
      id: link.id,
      tier: link.tier,
      url: buildShareViewerUrl(row.id, plaintext),
      shareToken: plaintext,   // shown once — embedded in the URL
      expiresAt: link.expiresAt,
      createdAt: link.createdAt,
    });
  });

  /* ---------- POST /api/visualiser/streams/:runId/shares/exchange ---------- */
  // PUBLIC (no owner auth). A shared viewer with no portal account posts
  // the opaque share token and receives a signalling JWT carrying the
  // link's tier. Validates: token matches, not revoked/expired, and the
  // run is still streaming (share links auto-die with the run).
  app.post<{ Params: { runId: string }; Body: unknown }>('/streams/:runId/shares/exchange', async (req, reply) => {
    const body = exchangeBody.safeParse(req.body);
    if (!body.success) return reply.code(400).send({ error: 'invalid body', issues: body.error.issues });
    const hash = hashShareToken(body.data.shareToken);
    const link = await db.query.visualiserShareLinks.findFirst({
      where: and(eq(visualiserShareLinks.tokenHash, hash), eq(visualiserShareLinks.runId, req.params.runId)),
    });
    if (!link) return reply.code(404).send({ error: 'invalid share token' });
    if (link.revokedAt) return reply.code(410).send({ error: 'share link revoked' });
    if (link.expiresAt && link.expiresAt.getTime() <= Date.now()) return reply.code(410).send({ error: 'share link expired' });

    const row = await db.query.visualiserRuns.findFirst({ where: eq(visualiserRuns.id, req.params.runId) });
    if (!row || row.status !== 'streaming') return reply.code(409).send({ error: 'stream is not active' });

    try {
      const tier = link.tier === 'control' ? 'control' : 'view';
      const { token, exp, viewerId } = issueSignallingToken({ runId: row.id, subject: `share:${link.id}`, tier, viewerId: body.data.viewerId });
      return {
        token, exp, viewerId, tier,
        runId: row.id,
        signallingUrl: buildSignallingUrl(row.id),
        turn: generateTurnCredential({ runId: row.id }),
      };
    } catch (err) {
      req.log.error({ err, runId: row.id }, 'failed to mint signalling token for share exchange');
      return reply.code(503).send({ error: 'signalling_token_unavailable', message: (err as Error).message });
    }
  });

  /* ---------- GET /api/visualiser/streams/:runId/shares ---------- */
  app.get<{ Params: { runId: string } }>('/streams/:runId/shares', {
    preHandler: requireAuth,
  }, async (req, reply) => {
    const row = await db.query.visualiserRuns.findFirst({ where: eq(visualiserRuns.id, req.params.runId) });
    if (!row) return reply.code(404).send({ error: 'not found' });
    if (!(await ownerCanCancel(row, req))) return reply.code(403).send({ error: 'forbidden' });
    const links = await db
      .select()
      .from(visualiserShareLinks)
      .where(eq(visualiserShareLinks.runId, row.id))
      .orderBy(desc(visualiserShareLinks.createdAt));
    // Never surface the token hash; the plaintext is irretrievable by design.
    return { shares: links.map((l) => ({
      id: l.id, tier: l.tier, createdBy: l.createdBy, createdAt: l.createdAt,
      expiresAt: l.expiresAt, revokedAt: l.revokedAt,
    })) };
  });

  /* ---------- DELETE /api/visualiser/streams/:runId/shares/:id ---------- */
  app.delete<{ Params: { runId: string; id: string } }>('/streams/:runId/shares/:id', {
    preHandler: requireAuth,
  }, async (req, reply) => {
    const row = await db.query.visualiserRuns.findFirst({ where: eq(visualiserRuns.id, req.params.runId) });
    if (!row) return reply.code(404).send({ error: 'not found' });
    if (!(await ownerCanCancel(row, req))) return reply.code(403).send({ error: 'forbidden' });
    const updated = await db
      .update(visualiserShareLinks)
      .set({ revokedAt: new Date() })
      .where(and(
        eq(visualiserShareLinks.id, req.params.id),
        eq(visualiserShareLinks.runId, row.id),
        isNull(visualiserShareLinks.revokedAt),
      ))
      .returning({ id: visualiserShareLinks.id });
    if (updated.length === 0) return reply.code(404).send({ error: 'not found or already revoked' });
    return { revoked: updated[0]!.id };
  });

  /* ---------- GET /api/visualiser/workstations ---------- */
  app.get('/workstations', {
    preHandler: requireAdmin,
  }, async () => {
    // Returns every can_visualise workstation, online OR offline, so
    // the admin UI can show the full pool and grey out offline rows.
    const wsRows = await db
      .select()
      .from(workstations)
      .where(eq(workstations.canVisualise, true))
      .orderBy(desc(workstations.lastSeenAt));
    const sessions = await db.select().from(agentSessions);
    const sessByWs = new Map<string, typeof sessions[number][]>();
    for (const s of sessions) {
      const arr = sessByWs.get(s.workstationId) ?? [];
      arr.push(s);
      sessByWs.set(s.workstationId, arr);
    }
    return {
      workstations: wsRows.map((w) => ({
        id: w.id,
        nodeName: w.nodeName,
        machineId: w.machineId,
        canVisualise: w.canVisualise,
        currentVisualiserLoad: w.currentVisualiserLoad,
        slotsTotal: w.slotsTotal,
        agentVersion: w.agentVersion,
        online: (sessByWs.get(w.id) ?? []).length > 0,
      })),
    };
  });

};

export default plugin;
