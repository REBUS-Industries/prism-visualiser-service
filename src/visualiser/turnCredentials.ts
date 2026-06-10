/**
 * Short-lived HMAC-signed TURN credentials, per RFC 7635 §3
 * ("Long-Term Credential Mechanism" / coturn's `use-auth-secret` mode).
 *
 * coturn validates an incoming `USERNAME` / `MESSAGE-INTEGRITY` pair by
 * recomputing `base64(HMAC-SHA1(static-auth-secret, username))` and
 * comparing it to the offered `credential`. The username embeds the
 * UNIX expiry timestamp so credentials self-rotate without server-side
 * state; the format is `<unix-epoch-seconds>:<opaque-tag>` and coturn
 * rejects USTUN binds where the timestamp is in the past.
 *
 * We deliberately do NOT mint coturn credentials from the orchestrator
 * — the portal-facing API surface is the only place a stream
 * descriptor crosses a trust boundary, so credentials live exactly as
 * long as the `POST /api/visualiser/streams` round-trip.
 *
 * Env:
 *   TURN_SECRET        Required for live credentials. When unset the
 *                      helper returns `null` so the portal can still
 *                      receive a `signallingUrl` + `playerUrl` for
 *                      same-LAN bring-up (Phase H wires the real
 *                      secret + matching coturn deploy).
 *   TURN_REALM         Realm advertised by coturn. Defaults to the
 *                      planned hostname `visualiser.rebus.industries`.
 *   TURN_URLS_OVERRIDE Comma-separated full ICE-server URL list. When
 *                      set this wins over the default
 *                      `turn:<realm>:3478` / `turns:<realm>:5349` pair.
 *                      Useful for local dev (single-node `turn:` with
 *                      no TLS) and for staging where the realm and the
 *                      DNS record diverge.
 */
import { createHmac } from 'node:crypto';

export interface TurnCredentials {
  urls: string[];
  username: string;
  credential: string;
  /** Lifetime in seconds (matches what coturn will accept). */
  ttl: number;
}

export interface GenerateOpts {
  runId: string;
  ttlSeconds?: number;
  /** Override env at call time; primarily used by tests. */
  now?: () => Date;
  env?: NodeJS.ProcessEnv;
}

const DEFAULT_REALM = 'visualiser.rebus.industries';
const DEFAULT_TTL_SECONDS = 24 * 60 * 60;  // 24h, matches the plan contract

/**
 * Generate a TURN credential bundle for `runId`. Returns `null` when
 * `TURN_SECRET` is unset (Phase H wires the real secret) so the API
 * caller can still receive the rest of the ready response.
 */
export function generateTurnCredential(opts: GenerateOpts): TurnCredentials | null {
  const env = opts.env ?? process.env;
  const secret = env.TURN_SECRET ?? '';
  if (!secret) {
    // The portal-facing contract documents `turn: null` as the
    // sentinel meaning "no TURN configured server-side; the caller
    // should fall back to STUN or LAN-only WebRTC". Phase H replaces
    // the sentinel with real coturn credentials.
    return null;
  }

  const ttlSeconds = opts.ttlSeconds ?? DEFAULT_TTL_SECONDS;
  if (!Number.isInteger(ttlSeconds) || ttlSeconds <= 0) {
    throw new Error(`generateTurnCredential: ttlSeconds must be a positive integer, got ${ttlSeconds}`);
  }

  const nowMs = (opts.now ?? (() => new Date()))().getTime();
  const exp = Math.floor(nowMs / 1000) + ttlSeconds;
  const username = `${exp}:${runIdToTag(opts.runId)}`;

  const credential = createHmac('sha1', secret).update(username).digest('base64');

  const realm = env.TURN_REALM ?? DEFAULT_REALM;
  const urls = parseUrlsOverride(env.TURN_URLS_OVERRIDE) ?? [
    `turn:${realm}:3478`,
    `turns:${realm}:5349`,
  ];

  return { urls, username, credential, ttl: ttlSeconds };
}

/**
 * Reduce a uuid-shaped runId to its first segment so the TURN username
 * stays well under the 513-byte STUN attribute limit even after the
 * timestamp prefix. Non-uuid strings (tests, legacy callers) pass
 * through unchanged so the function is also safe outside production.
 */
function runIdToTag(runId: string): string {
  if (!runId) throw new Error('generateTurnCredential: runId is required');
  const firstDash = runId.indexOf('-');
  return firstDash > 0 ? runId.slice(0, firstDash) : runId;
}

function parseUrlsOverride(value: string | undefined): string[] | null {
  if (!value) return null;
  const parts = value.split(',').map((s) => s.trim()).filter(Boolean);
  return parts.length > 0 ? parts : null;
}
