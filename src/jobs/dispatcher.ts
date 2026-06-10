/**
 * Job dispatcher.
 *
 * Given a queued job, find an eligible agent slot and push the appropriate
 * envelope over its WS:
 *
 *   - `assign`             for regular convert / receive jobs
 *   - `pollLayers`         for the first phase of a two-phase layer-selection
 *                          convert (job.selectLayers=true && job.layersJson IS NULL)
 *   - `startVisualisation` for visualiser runs (Phase A: routed through
 *                          `tryDispatchVisualisation` below, NOT BullMQ —
 *                          visualiser runs are long-lived streaming
 *                          sessions, not file-conversion jobs.)
 *
 * Returns true if dispatched, false if no eligible agent was available
 * (the worker should requeue or hold).
 *
 * Eligibility:
 *   - workstation.is_enabled  = true
 *   - workstation.can_convert  = true (for convert jobs)
 *   - workstation.can_layer    = true (for pollLayers jobs)
 *   - workstation.can_receive  = true (for receive jobs)
 *   - workstation.can_visualise = true (for visualiser runs; see
 *                                tryDispatchVisualisation)
 *   - workstation.supported_formats includes job.format (NOT checked
 *                                for visualiser runs — UE imports model
 *                                bytes directly without going through the
 *                                Rhino converter pool)
 *   - agent has at least one free slot (slotsBusy < slotsTotal)
 */
import { randomUUID } from 'node:crypto';
import type { FastifyBaseLogger } from 'fastify';
import { and, desc, eq, isNull, sql } from 'drizzle-orm';
import {
  db, jobs, projectAttachments, visualiserRuns, workstations,
  getSetting,
  envelope,
  type AssignData,
  type PollLayersData,
  type ProjectAttachmentRef,
  type StartVisualisationData,
  getLatestVersionId, listModels, OrbitClientError,
  sessionRegistry, type AgentConn,
  broadcastJobUpdate,
  appendVisualiserRunLog,
} from '@rebus-industries/prism-shared';
import { issueDownloadToken } from '../api/internal.js';

const PUBLIC_BASE_URL =
  process.env.PUBLIC_BASE_URL
  ?? process.env.PRISM_PUBLIC_URL
  ?? 'http://localhost:8765';

export interface DispatchOutcome {
  dispatched: boolean;
  agentSessionId?: string;
  nodeName?: string;
  reason?: string;
}

export async function tryDispatch(jobId: string, log: FastifyBaseLogger): Promise<DispatchOutcome> {
  const job = await db.query.jobs.findFirst({ where: eq(jobs.id, jobId) });
  if (!job) return { dispatched: false, reason: 'job not found' };
  if (job.status !== 'queued' && job.status !== 'dispatched') {
    return { dispatched: false, reason: `job is ${job.status}` };
  }

  const isReceive = job.jobType === 'receive';
  // A pollLayers dispatch is the first phase of the two-phase
  // layer-selection flow. Once the agent replies with `layers`, the job
  // moves to `awaiting_selection`; when the caller POSTs the chosen
  // layers the job re-enters this dispatcher with layersJson set, at
  // which point we fall through to the regular convert path.
  const isPollLayers = !isReceive && !!job.selectLayers && !job.layersJson;

  // Resolve workstations table once; live conn state from sessionRegistry.
  const wsRows = await db.select().from(workstations);
  const wsByMachine = new Map(wsRows.map((w) => [w.machineId, w]));

  const eligible: AgentConn[] = [];
  for (const conn of await sessionRegistry.allAgents()) {
    const w = wsByMachine.get(conn.machineId);
    if (!w || !w.isEnabled) continue;
    if (isReceive) {
      if (!w.canReceive) continue;
    } else if (isPollLayers) {
      if (!w.canLayer) continue;
    } else {
      if (!w.canConvert) continue;
    }
    // For receive jobs, supportedFormats gates the OUTPUT format (e.g. '3dm');
    // for convert / pollLayers it gates the INPUT format.
    const supported = (w.supportedFormats as string[] | null) ?? [];
    if (!isReceive && !supported.includes(job.format)) continue;
    if (conn.slotsBusy >= conn.hello.slots) continue;
    eligible.push(conn);
  }

  if (eligible.length === 0) {
    return { dispatched: false, reason: 'no eligible agent available' };
  }

  // Trivial selection: least-loaded first, then earliest connected.
  eligible.sort((a, b) =>
    a.slotsBusy !== b.slotsBusy
      ? a.slotsBusy - b.slotsBusy
      : a.connectedAt.getTime() - b.connectedAt.getTime()
  );
  const agent = eligible[0]!;

  const orbitServerUrl = await getSetting(job.orbitTarget === 'dev' ? 'orbit_dev_server_url' : 'orbit_server_url');
  if (!orbitServerUrl && !isPollLayers) {
    // pollLayers doesn't actually need ORBIT creds — but we still resolve
    // them for an eventual convert phase to fail loudly here rather than
    // later.
    log.error({ orbitTarget: job.orbitTarget }, 'dispatch failed: no ORBIT server URL configured for target');
    return { dispatched: false, reason: `no ORBIT URL set for target=${job.orbitTarget}` };
  }

  const orbitToken =
    job.submittedBy?.startsWith('orbit:')
      // For Phase 3 we don't yet persist the bearer that was used at submit time.
      // The convert/async route in Phase 7 will stash it (encrypted) on the job row.
      ? (await getSetting('orbit_token')) ?? ''
      : (await getSetting('orbit_token')) ?? '';
  if (!orbitToken && !isPollLayers) {
    log.warn({ jobId }, 'no shared orbit_token; agent will get an empty bearer (Phase 7 fixes per-user tokens)');
  }

  // pollLayers and convert both need a download URL; receive does not.
  let fileUrl: string | undefined;
  if (!isReceive) {
    const fileToken = await issueDownloadToken(job.id);
    fileUrl = `${PUBLIC_BASE_URL.replace(/\/$/, '')}/internal/files/${job.id}?token=${fileToken}`;
  }

  if (isPollLayers) {
    const poll: PollLayersData = {
      jobId: job.id,
      fileUrl: fileUrl!,
      format: job.format,
    };
    const pollSent = await sessionRegistry.sendToAgent(agent.workstationId, JSON.stringify(envelope('pollLayers', poll, randomUUID())));
    if (!pollSent) {
      log.warn({ agentSessionId: agent.sessionId }, 'agent ws send (pollLayers) failed; will requeue');
      return { dispatched: false, reason: 'agent send failed' };
    }
    await sessionRegistry.reserveSlot(agent.workstationId);

    await db
      .update(jobs)
      .set({
        status: 'dispatched',
        nodeName: agent.nodeName,
        agentSessionId: agent.sessionId,
        currentStage: 'polling-layers',
        lastMessage: 'extracting layer tree on agent',
        updatedAt: new Date(),
      })
      .where(eq(jobs.id, job.id));

    broadcastJobUpdate(job.id, {
      status: 'dispatched',
      currentStage: 'polling-layers',
      lastMessage: 'extracting layer tree on agent',
      nodeName: agent.nodeName,
    });

    log.info({ jobId: job.id, nodeName: agent.nodeName, sessionId: agent.sessionId }, 'job dispatched as pollLayers');
    return { dispatched: true, agentSessionId: agent.sessionId, nodeName: agent.nodeName };
  }

  // Always provide an upload-back URL so the agent can deliver non-ORBIT outputs
  // (3DM, GLB, IFC, STEP, or the receive primary).
  const outputBaseUrl = `${PUBLIC_BASE_URL.replace(/\/$/, '')}/internal/outputs/${job.id}`;
  const outputFormats = (job.outputFormats as string[] | null) ?? [];

  // Compose the AssignOptions, expanding includedLayers with the descendant
  // set if requested AND we have a layer tree to expand from (two-phase
  // flow). Direct callers that pass `includedLayers` + `includeLayerDescendants`
  // without going through pollLayers get the agent-side fallback expansion in
  // ConvertJob.AssignToCard.
  const persistedOptions = (job.options as AssignData['options']) ?? {};
  const includedLayers = (job.includedLayers as string[] | null) ?? persistedOptions.includedLayers ?? [];
  const includeLayerDescendants = job.includeLayerDescendants ?? persistedOptions.includeLayerDescendants ?? false;
  const expandedLayers = includeLayerDescendants && job.layersJson
    ? expandLayerSelection(includedLayers, job.layersJson as LayerNode[])
    : includedLayers;

  const options: AssignData['options'] = {
    ...persistedOptions,
    includedLayers: expandedLayers.length ? expandedLayers : undefined,
    includeLayerDescendants: includeLayerDescendants || undefined,
  };

  const assign: AssignData = {
    jobId: job.id,
    jobType: isReceive ? 'receive' : 'convert',
    slot: agent.slotsBusy,
    format: job.format,
    fileUrl,
    fileName: job.fileName,
    orbitServerUrl: orbitServerUrl ?? '',
    orbitToken,
    projectId: job.projectId,
    modelId: job.modelId,
    modelName: job.modelName ?? undefined,
    receiveVersionId: job.receiveVersionId ?? undefined,
    outputFormats: outputFormats.length ? outputFormats : undefined,
    outputUploadUrl: (outputFormats.length || isReceive) ? outputBaseUrl : undefined,
    options,
  };

  const assignSent = await sessionRegistry.sendToAgent(agent.workstationId, JSON.stringify(envelope('assign', assign, randomUUID())));
  if (!assignSent) {
    log.warn({ agentSessionId: agent.sessionId }, 'agent ws send failed; will requeue');
    return { dispatched: false, reason: 'agent send failed' };
  }
  await sessionRegistry.reserveSlot(agent.workstationId);

  await db
    .update(jobs)
    .set({
      status: 'dispatched',
      nodeName: agent.nodeName,
      agentSessionId: agent.sessionId,
      currentStage: 'dispatched',
      updatedAt: new Date(),
    })
    .where(eq(jobs.id, job.id));

  broadcastJobUpdate(job.id, { status: 'dispatched', nodeName: agent.nodeName });

  log.info({ jobId: job.id, nodeName: agent.nodeName, sessionId: agent.sessionId }, 'job dispatched');
  return { dispatched: true, agentSessionId: agent.sessionId, nodeName: agent.nodeName };
}

/* -------------------------------------------------------------------------- */
/* Visualiser dispatch                                                         */
/*                                                                             */
/* Phase A: this function is exported and unit-testable but no API caller       */
/* invokes it yet. Phase G will wire it from `POST /v1/visualiser/streams`.    */
/*                                                                             */
/* Visualiser runs intentionally bypass BullMQ — a visualiser session is a    */
/* long-lived streaming connection, not a queued job that gets retried on     */
/* failure. We pick an eligible workstation, send `startVisualisation`, mark   */
/* the row `importing`, and rely on the agent's reverse-channel               */
/* `visualisationReady` / `visualisationFailed` envelopes to drive the row    */
/* to terminal state. The orchestrator also enforces a TTL hard tear-down.   */
/* -------------------------------------------------------------------------- */

/**
 * Phase G visualiser dispatch outcome — narrower than the convert
 * {@link DispatchOutcome} so the api/visualiser.ts caller can `switch`
 * on the failure mode and surface a precise HTTP status code (503 vs
 * 502 vs 500) without parsing a free-form reason string.
 */
export type VisualiserDispatchOutcome =
  | { dispatched: true; workstationId: string; agentSessionId: string; nodeName: string }
  | { dispatched: false; error: 'no_workstation_available'; reason: string }
  | { dispatched: false; error: 'all_workstations_busy';    reason: string }
  | { dispatched: false; error: 'agent_send_failed';        reason: string }
  | { dispatched: false; error: 'misconfigured';            reason: string }
  | { dispatched: false; error: 'version_unavailable';      reason: string }
  | { dispatched: false; error: 'invalid_state';            reason: string };

/**
 * Pick an eligible visualiser agent for the given run id and send it a
 * `startVisualisation` envelope. Eligibility:
 *
 *   - workstation.is_enabled    = true
 *   - workstation.can_visualise = true
 *   - agent is currently connected
 *   - workstation.current_visualiser_load < agent.hello.slots
 *
 * Selection: when `preferredWorkstationId` is supplied the dispatcher
 * restricts eligibility to that single workstation (the operator picked a
 * specific box in the admin UI). Otherwise it picks least-loaded by
 * `current_visualiser_load` (the in-memory sessionRegistry's `slotsBusy`
 * is shared with the conversion pool and doesn't reflect long-lived UE
 * sessions reliably). Ties broken by earliest-connected.
 *
 * Atomic load bump: we run a `SELECT ... FOR UPDATE SKIP LOCKED`
 * against the eligible row in a single transaction so two concurrent
 * `POST /api/visualiser/streams` requests cannot both pick the same
 * workstation past its slot cap. The `SKIP LOCKED` clause means a
 * concurrent dispatcher that already holds the row's lock won't block
 * us — it'll simply consider the next eligible row.
 *
 * The load counter is decremented by {@link releaseVisualiserSlot}
 * on `visualisationEnded` / `visualisationFailed`.
 *
 * Note: `supported_formats` is intentionally NOT checked — UE imports
 * the ORBIT version's bytes directly via the orchestrator. The
 * visualiser agent does not load Rhino.
 */
export async function tryDispatchVisualisation(
  runId: string,
  log: FastifyBaseLogger,
  preferredWorkstationId?: string,
): Promise<VisualiserDispatchOutcome> {
  const run = await db.query.visualiserRuns.findFirst({ where: eq(visualiserRuns.id, runId) });
  if (!run) return { dispatched: false, error: 'invalid_state', reason: 'visualiser run not found' };
  if (run.status !== 'queued') {
    return { dispatched: false, error: 'invalid_state', reason: `visualiser run is ${run.status}` };
  }

  // Find live agent connections backed by visualiser-capable workstations.
  // Order matters: we sort by reported `current_visualiser_load` and then
  // by earliest connect time so the dispatcher is deterministic.
  const wsRows = await db.select().from(workstations);
  const wsByMachine = new Map(wsRows.map((w) => [w.machineId, w]));

  type Candidate = { conn: AgentConn; workstationId: string; load: number };
  const candidates: Candidate[] = [];
  let sawAnyVisualiserCapable = false;
  // Whether the operator-requested workstation was seen online + enabled +
  // visualiser-capable (regardless of whether it had a free slot).
  let sawPreferred = false;
  for (const conn of await sessionRegistry.allAgents()) {
    const w = wsByMachine.get(conn.machineId);
    if (!w || !w.isEnabled) continue;
    if (!w.canVisualise) continue;
    sawAnyVisualiserCapable = true;
    // Honor an explicit workstation choice: skip every box except the one
    // the caller asked for. Without this filter the dispatcher always fell
    // through to the least-loaded agent (typically PC01), ignoring the
    // selection made in the admin UI.
    if (preferredWorkstationId && w.id !== preferredWorkstationId) continue;
    if (preferredWorkstationId) sawPreferred = true;
    if (w.currentVisualiserLoad >= conn.hello.slots) continue;
    candidates.push({ conn, workstationId: w.id, load: w.currentVisualiserLoad });
  }

  if (candidates.length === 0) {
    if (preferredWorkstationId) {
      if (!sawPreferred) {
        return { dispatched: false, error: 'no_workstation_available', reason: `requested workstation ${preferredWorkstationId} is not online or not can_visualise = true` };
      }
      return { dispatched: false, error: 'all_workstations_busy', reason: `requested workstation ${preferredWorkstationId} is at capacity` };
    }
    if (!sawAnyVisualiserCapable) {
      return { dispatched: false, error: 'no_workstation_available', reason: 'no workstation has can_visualise = true and an agent online' };
    }
    return { dispatched: false, error: 'all_workstations_busy', reason: 'every eligible workstation is at capacity' };
  }

  candidates.sort((a, b) =>
    a.load !== b.load
      ? a.load - b.load
      : a.conn.connectedAt.getTime() - b.conn.connectedAt.getTime()
  );

  // Atomic reserve: lock the workstations row and increment the load
  // counter only if it's still below capacity, in a single statement.
  // Repeat-and-bail when the optimistic update returns zero rows.
  let reserved: Candidate | null = null;
  for (const cand of candidates) {
    const slotCap = cand.conn.hello.slots;
    const updated = await db
      .update(workstations)
      .set({ currentVisualiserLoad: sql`${workstations.currentVisualiserLoad} + 1` })
      .where(and(
        eq(workstations.id, cand.workstationId),
        sql`${workstations.currentVisualiserLoad} < ${slotCap}`,
      ))
      .returning({ id: workstations.id, currentVisualiserLoad: workstations.currentVisualiserLoad });
    if (updated.length > 0) {
      reserved = cand;
      break;
    }
    // Lost the race against a sibling dispatcher; try the next candidate.
  }

  if (!reserved) {
    return { dispatched: false, error: 'all_workstations_busy', reason: 'lost reservation race to a concurrent dispatcher' };
  }

  const agent = reserved.conn;
  const orbitServerUrl = await getSetting(run.orbitTarget === 'dev' ? 'orbit_dev_server_url' : 'orbit_server_url');
  if (!orbitServerUrl) {
    await releaseVisualiserSlot(reserved.workstationId).catch(() => null);
    log.error({ orbitTarget: run.orbitTarget }, 'visualiser dispatch failed: no ORBIT server URL configured for target');
    return { dispatched: false, error: 'misconfigured', reason: `no ORBIT URL set for target=${run.orbitTarget}` };
  }

  // For Phase G we still use the shared service token. Per-user token
  // pass-through is parked behind the portal contract — the portal
  // signs its own bearer and the dispatcher hands the agent that
  // bearer instead of `orbit_token`. Tracked in the Phase G open
  // items list.
  const orbitToken = (await getSetting('orbit_token')) ?? '';
  if (!orbitToken) {
    log.warn({ runId }, 'no shared orbit_token; visualiser agent will get an empty bearer');
  }

  // Phase J — pull live (non-soft-deleted) project attachments and forward
  // the download URLs to the orchestrator so its MvrGdtfDetector can stage
  // them under stage/{runId}/attachments/. The orchestrator hits these
  // URLs with its existing PRISM bearer; we don't mint a per-attachment
  // signed URL here (unlike convert /internal/files) because the
  // /api/projects/:id/attachments/:id surface is itself authenticated.
  const attachmentRefs = await loadAttachmentRefs(run.projectId);
  if (attachmentRefs.length > 0) {
    log.info(
      { runId: run.id, projectId: run.projectId, count: attachmentRefs.length },
      'forwarding project attachments to visualiser agent',
    );
  }

  // If the caller omitted versionId ("use the latest"), resolve it from ORBIT
  // now. For tree imports the orchestrator uses OrbitImportTree and never
  // needs a versionId, so skip the resolution entirely for that mode.
  let resolvedVersionId = run.versionId ?? null;
  let isTreeImport = run.importMode === 'tree';
  // Effective model name: may be updated by auto-detection below.
  let effectiveModelName: string | undefined = run.modelName ?? undefined;
  // Explicit submodel IDs resolved during auto-detection — forwarded to the
  // agent so the connector skips the orbit-cli models --under lookup.
  let resolvedSubmodelIds: string[] | undefined;

  if (!resolvedVersionId && !isTreeImport) {
    try {
      const latestId = await getLatestVersionId(
        run.orbitTarget as 'prod' | 'dev',
        run.projectId,
        run.modelId,
      );
      if (!latestId) {
        // Model has no versions. Before rejecting, check whether it is a
        // parent container whose submodels hold the actual geometry. A parent
        // model has no committed versions of its own but has child models
        // whose names start with `<parentName>/`. If detected, switch to tree
        // import so the UE connector calls OrbitImportTree instead of
        // OrbitImport — identical to the caller explicitly setting
        // importMode='tree' on the POST body.
        let detectedAsTree = false;
        try {
          const { items: allModels } = await listModels(
            run.orbitTarget as 'prod' | 'dev',
            run.projectId,
            { limit: 200 },
          );
          // Resolve the model name: prefer what's already on the row (the
          // caller may have supplied it), otherwise find the name by ID.
          let modelName = run.modelName ?? null;
          if (!modelName) {
            const match = allModels.find((m) => m.id === run.modelId);
            modelName = match?.name ?? null;
          }
          if (modelName) {
            const prefix = modelName + '/';
            const submodels = allModels.filter((m) => m.name.startsWith(prefix));
            if (submodels.length > 0) {
              detectedAsTree = true;
              isTreeImport = true;
              effectiveModelName = modelName;
              // Collect up to 50 submodel IDs so the connector can skip the
              // orbit-cli models --under lookup (which has a low default page
              // size that misses models beyond the first page).
              resolvedSubmodelIds = submodels.slice(0, 50).map((m) => m.id);
              // Persist the auto-detected tree mode and resolved name so the
              // admin UI shows the correct state and re-dispatches reuse it.
              await db
                .update(visualiserRuns)
                .set({ importMode: 'tree', modelName, updatedAt: new Date() })
                .where(eq(visualiserRuns.id, run.id));
              log.info(
                {
                  runId: run.id, projectId: run.projectId, modelId: run.modelId,
                  modelName, target: run.orbitTarget, submodelCount: submodels.length,
                  submodelIds: resolvedSubmodelIds,
                },
                'visualiser dispatch: auto-detected parent model with submodels; switching to tree import',
              );
              await appendVisualiserRunLog(
                run.id,
                `auto-detected parent model "${modelName}" with ${submodels.length} submodels; switching to tree import`,
                { log },
              );
            }
          }
        } catch (subErr) {
          // Non-fatal: submodel detection failed. Fall through to
          // version_unavailable so the caller gets an actionable error.
          log.warn(
            { err: subErr, runId: run.id, projectId: run.projectId, modelId: run.modelId },
            'visualiser dispatch: submodel auto-detection failed; falling back to version_unavailable',
          );
        }

        if (!detectedAsTree) {
          await releaseVisualiserSlot(reserved.workstationId).catch(() => null);
          log.warn(
            { runId: run.id, projectId: run.projectId, modelId: run.modelId, target: run.orbitTarget },
            'visualiser dispatch: model has no versions yet',
          );
          // Caller-data problem, NOT a server misconfiguration: the model
          // exists but has no committed version to stream. Surface a precise
          // code so the portal doesn't think PRISM itself is broken.
          return {
            dispatched: false,
            error: 'version_unavailable',
            reason: `model ${run.modelId} in project ${run.projectId} has no versions on ORBIT ${run.orbitTarget} yet`,
          };
        }
      } else {
        resolvedVersionId = latestId;
        log.info(
          { runId: run.id, resolvedVersionId },
          'visualiser dispatch: resolved latest versionId (none supplied by caller)',
        );
        await appendVisualiserRunLog(run.id, `resolved latest version ${resolvedVersionId} (none supplied by caller)`, { log });
        // Persist so the admin UI shows which version is actually running.
        await db
          .update(visualiserRuns)
          .set({ versionId: resolvedVersionId, updatedAt: new Date() })
          .where(eq(visualiserRuns.id, run.id));
      }
    } catch (err) {
      await releaseVisualiserSlot(reserved.workstationId).catch(() => null);
      const msg = err instanceof OrbitClientError
        ? err.message
        : (err as Error).message ?? 'failed to resolve latest version';

      if (err instanceof OrbitClientError) {
        // Classify by the status the ORBIT client attached:
        //   400/404 → the caller's projectId/modelId doesn't resolve in
        //             ORBIT for this target (bad id, wrong server, or the
        //             project isn't shared with PRISM's service token).
        //             This is a CALLER-DATA error, not a PRISM/workstation
        //             fault — emit a distinct code so the portal shows an
        //             actionable "check your project/model id" message
        //             instead of "server misconfigured".
        //   401/412 → ORBIT credentials are missing or were rejected; that
        //             genuinely IS a server-side misconfiguration the admin
        //             must fix (set/repair orbit_token / orbit_*_server_url).
        if (err.status === 400 || err.status === 404) {
          log.warn(
            { err, runId: run.id, projectId: run.projectId, modelId: run.modelId, target: run.orbitTarget },
            'visualiser dispatch: ORBIT could not resolve project/model (caller-supplied id)',
          );
          return {
            dispatched: false,
            error: 'version_unavailable',
            reason: `ORBIT ${run.orbitTarget} could not resolve project ${run.projectId} / model ${run.modelId}: ${msg}`,
          };
        }
        if (err.status === 401 || err.status === 412) {
          log.error({ err, runId: run.id }, 'visualiser dispatch: ORBIT credentials missing/rejected');
          return {
            dispatched: false,
            error: 'misconfigured',
            reason: `ORBIT ${run.orbitTarget} credentials missing or rejected: ${msg}`,
          };
        }
      }

      // Unknown / upstream failure (ORBIT unreachable, 5xx, non-JSON, …).
      log.error({ err, runId: run.id }, 'visualiser dispatch: failed to resolve latest versionId');
      return { dispatched: false, error: 'misconfigured', reason: msg };
    }
  }

  const payload: StartVisualisationData = {
    runId: run.id,
    slot: reserved.load,
    orbitServerUrl,
    orbitToken,
    projectId: run.projectId,
    modelId: run.modelId,
    modelName: effectiveModelName,
    importMode: isTreeImport ? 'tree' : undefined,
    versionId: resolvedVersionId ?? undefined,
    templateTag: run.templateTag ?? undefined,
    signallingUrl: run.signallingUrl ?? undefined,
    ttlSeconds: run.ttlSeconds ?? undefined,
    attachments: attachmentRefs.length > 0 ? attachmentRefs : undefined,
    submodelIds: resolvedSubmodelIds && resolvedSubmodelIds.length > 0 ? resolvedSubmodelIds : undefined,
  };

  const visSent = await sessionRegistry.sendToAgent(agent.workstationId, JSON.stringify(envelope('startVisualisation', payload, randomUUID())));
  if (!visSent) {
    // Roll back the reservation; the API caller will see `agent_send_failed`
    // and the next dispatcher cycle (or the next POST) can retry.
    await releaseVisualiserSlot(reserved.workstationId).catch(() => null);
    log.warn({ agentSessionId: agent.sessionId }, 'visualiser ws send failed; rolling back reservation');
    return { dispatched: false, error: 'agent_send_failed', reason: 'agent ws send failed' };
  }

  await db
    .update(visualiserRuns)
    .set({
      status: 'importing',
      workstationId: reserved.workstationId,
      agentSessionId: agent.sessionId,
      dispatchedAt: new Date(),
      updatedAt: new Date(),
    })
    .where(eq(visualiserRuns.id, run.id));

  log.info({ runId: run.id, nodeName: agent.nodeName, sessionId: agent.sessionId }, 'visualiser run dispatched');
  await appendVisualiserRunLog(
    run.id,
    isTreeImport
      ? `dispatched to workstation ${agent.nodeName}; tree import (model: ${effectiveModelName ?? run.modelId})`
      : `dispatched to workstation ${agent.nodeName}; orchestrator importing version ${resolvedVersionId}`,
    { log },
  );
  return {
    dispatched: true,
    workstationId: reserved.workstationId,
    agentSessionId: agent.sessionId,
    nodeName: agent.nodeName,
  };
}

/**
 * Build the project-attachment ref array the visualiser dispatcher hands
 * to the agent in {@link StartVisualisationData}. Exported so the
 * visualiser API tests can assert on the exact shape without spinning up
 * a full dispatcher.
 *
 * Returns newest-first, soft-deletes excluded. The `downloadUrl` is
 * derived from `PUBLIC_BASE_URL` so the agent can hit the same hostname
 * it uses for its WS connection.
 */
export async function loadAttachmentRefs(projectId: string): Promise<ProjectAttachmentRef[]> {
  const rows = await db
    .select()
    .from(projectAttachments)
    .where(and(
      eq(projectAttachments.projectId, projectId),
      isNull(projectAttachments.deletedAt),
    ))
    .orderBy(desc(projectAttachments.uploadedAt));
  const base = PUBLIC_BASE_URL.replace(/\/$/, '');
  return rows.map((row) => ({
    id: row.id,
    filename: row.filename,
    contentType: row.contentType,
    sizeBytes: row.sizeBytes,
    downloadUrl: `${base}/api/projects/${encodeURIComponent(projectId)}/attachments/${row.id}`,
  }));
}

/**
 * Decrement `workstations.current_visualiser_load` after a run reaches
 * terminal state. Floored at zero so a double-fire (agent emits both
 * `failed` and `ended`) can't drive the counter negative.
 *
 * Called by the agent protocol handler on `visualisationReady`-failure
 * paths, `visualisationFailed`, and `visualisationEnded`, plus the
 * api/visualiser.ts DELETE handler (best-effort cleanup if the agent
 * never replies).
 */
export async function releaseVisualiserSlot(workstationId: string): Promise<void> {
  await db
    .update(workstations)
    .set({
      currentVisualiserLoad: sql`GREATEST(${workstations.currentVisualiserLoad} - 1, 0)`,
    })
    .where(eq(workstations.id, workstationId));
}

/* -------------------------------------------------------------------------- */
/* Layer descendant expansion                                                  */
/* -------------------------------------------------------------------------- */

interface LayerNode {
  name: string;
  fullPath?: string;
  children?: LayerNode[];
}

/**
 * Given a flat list of selected `FullPath` strings and a nested layer tree,
 * return the full set of selected layers PLUS every descendant. Order is
 * preserved (selected first, then descendants in tree order).
 *
 * The agent's `LayerMode.ByLayer` filter does an exact `FullPath` match
 * (see RhinoSendPipeline.GetFilteredObjects), so the server is responsible
 * for spelling out every layer name the user actually wants included.
 */
export function expandLayerSelection(selected: string[], tree: LayerNode[]): string[] {
  const selectedSet = new Set(selected);
  const out = new Set<string>(selected);

  function walk(node: LayerNode, parentSelected: boolean) {
    const fp = node.fullPath ?? node.name;
    const isSelected = parentSelected || selectedSet.has(fp);
    if (isSelected) out.add(fp);
    if (node.children?.length) {
      for (const child of node.children) walk(child, isSelected);
    }
  }
  for (const root of tree) walk(root, false);
  return [...out];
}
