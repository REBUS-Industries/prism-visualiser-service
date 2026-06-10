/**
 * Short-lived HS256 JWT used by the Pixel Streaming browser client to
 * authenticate against `/ws/visualiser/:runId/signalling`.
 *
 * Flow:
 *   1. SPA / portal hits `POST /api/visualiser/streams/:runId/signalling-token`
 *      (authenticated via X-API-Key OR admin session). Server issues a
 *      JWT containing the runId + an exp (default 1h, see
 *      JWT_SIGNALLING_TTL_SEC).
 *   2. Browser opens `wss://prism.rebus.industries/ws/visualiser/<runId>/signalling?token=<jwt>`.
 *   3. The WS gateway verifies the JWT and rejects with 401 on any
 *      mismatch (bad sig, expired, wrong runId).
 *
 * Why a hand-rolled JWT rather than `jsonwebtoken` or `jose`: we
 * already vendor `node:crypto` everywhere, the token surface is one
 * symmetric algorithm, and adding a new dep for ~30 lines of HMAC
 * isn't worth the supply-chain noise. The implementation deliberately
 * tracks the relevant slice of RFC 7519 — header + payload + signature,
 * base64url, no nesting, no JWE.
 *
 * Env:
 *   JWT_SIGNALLING_SECRET   Symmetric secret (any length; HMAC clamps to
 *                           block size internally). When unset the server
 *                           refuses to mint tokens (the route returns
 *                           503 with a clear error so operators notice).
 *   JWT_SIGNALLING_TTL_SEC  Default token lifetime in seconds. Defaults to
 *                           3600 (1 hour). This MUST comfortably exceed a
 *                           viewing session: the PS frontend re-presents the
 *                           token on every signalling-WS (auto-)reconnect, so
 *                           a TTL shorter than the session dropped an
 *                           actively-watched viewer the first time the socket
 *                           reconnected after expiry (the old 300s default ==
 *                           the "~5-minute inactivity" disconnect). The
 *                           browser also refreshes the token proactively, so
 *                           this is headroom rather than the sole guard.
 */
import { createHmac, randomUUID, timingSafeEqual } from 'node:crypto';

// 1 hour. Long enough that a normal session + reconnects never outlive a
// single token, while still bounding the replay window for a leaked token
// (which is also scoped to one ephemeral runId + viewer seat). Override via
// JWT_SIGNALLING_TTL_SEC.
const DEFAULT_TTL_SECONDS = 60 * 60;
const ALG = 'HS256';

/** Permission tier carried by a signalling token. */
export type SignallingTier = 'view' | 'control';

export interface SignallingTokenPayload {
  /** The visualiser_runs row id this token authorises a signalling WS for. */
  runId: string;
  /** Unix epoch seconds; matches RFC 7519 §4.1.4. */
  exp: number;
  /** Unix epoch seconds; matches RFC 7519 §4.1.6. */
  iat: number;
  /** Subject — currently the issuing api_keys.id, admin user id, or `admin:<name>`. */
  sub?: string;
  /**
   * Permission tier. `control` viewers may take the single-controller
   * lock and drive input; `view` viewers are spectators. Absent on
   * pre-multi-viewer tokens — the verifier defaults those to `control`
   * to preserve the historical "owner/admin drives the viewport"
   * behaviour.
   */
  tier?: SignallingTier;
  /**
   * Stable per-viewer demux key. Each minted token represents one viewer
   * "seat"; the signalling proxy uses it to route frames 1:1 and the
   * control channel uses it to identify who holds the lock. Absent on
   * legacy tokens — the verifier/proxy mints a fallback per socket.
   */
  viewerId?: string;
}

export interface IssueOpts {
  runId: string;
  ttlSeconds?: number;
  subject?: string;
  /** Permission tier; defaults to `control` (owner/admin-minted tokens). */
  tier?: SignallingTier;
  /** Stable per-viewer id; a random uuid is minted when omitted. */
  viewerId?: string;
  now?: () => Date;
  env?: NodeJS.ProcessEnv;
}

export interface VerifyOpts {
  expectedRunId: string;
  now?: () => Date;
  env?: NodeJS.ProcessEnv;
}

export type VerifyResult =
  | { ok: true; payload: SignallingTokenPayload }
  | { ok: false; error: string };

function b64urlEncode(buf: Buffer): string {
  return buf.toString('base64')
    .replace(/=+$/g, '')
    .replace(/\+/g, '-')
    .replace(/\//g, '_');
}

function b64urlDecode(s: string): Buffer {
  // Restore standard base64 padding before decoding.
  const pad = s.length % 4 === 0 ? '' : '='.repeat(4 - (s.length % 4));
  return Buffer.from(s.replace(/-/g, '+').replace(/_/g, '/') + pad, 'base64');
}

function requireSecret(env: NodeJS.ProcessEnv): string | { error: string } {
  const secret = env.JWT_SIGNALLING_SECRET ?? '';
  if (!secret) return { error: 'JWT_SIGNALLING_SECRET is not set; refuse to mint or verify signalling tokens' };
  return secret;
}

/**
 * Mint a signalling token. Throws when `JWT_SIGNALLING_SECRET` is unset
 * — the route handler catches that and returns a 503 so the caller
 * sees a clear failure mode rather than a malformed token.
 */
export function issueSignallingToken(opts: IssueOpts): { token: string; exp: number; viewerId: string; tier: SignallingTier } {
  const env = opts.env ?? process.env;
  const secretOrErr = requireSecret(env);
  if (typeof secretOrErr !== 'string') throw new Error(secretOrErr.error);
  const ttl = opts.ttlSeconds ?? Number(env.JWT_SIGNALLING_TTL_SEC ?? DEFAULT_TTL_SECONDS);
  if (!Number.isInteger(ttl) || ttl <= 0) {
    throw new Error(`issueSignallingToken: ttlSeconds must be a positive integer, got ${ttl}`);
  }
  const nowMs = (opts.now ?? (() => new Date()))().getTime();
  const iat = Math.floor(nowMs / 1000);
  const exp = iat + ttl;
  const tier: SignallingTier = opts.tier ?? 'control';
  const viewerId = opts.viewerId ?? randomUUID();
  const header = { alg: ALG, typ: 'JWT' };
  const payload: SignallingTokenPayload = { runId: opts.runId, iat, exp, sub: opts.subject, tier, viewerId };

  const headerB64  = b64urlEncode(Buffer.from(JSON.stringify(header),  'utf8'));
  const payloadB64 = b64urlEncode(Buffer.from(JSON.stringify(payload), 'utf8'));
  const signingInput = `${headerB64}.${payloadB64}`;
  const sig = createHmac('sha256', secretOrErr).update(signingInput).digest();
  const sigB64 = b64urlEncode(sig);
  return { token: `${signingInput}.${sigB64}`, exp, viewerId, tier };
}

/**
 * Verify a signalling token against the expected runId.
 *
 * Returns `{ ok: true, payload }` on success. Returns `{ ok: false,
 * error }` on every failure — caller maps to 401 / 403.
 *
 * Note: `expectedRunId` is required so a token minted for run A can
 * never be replayed against run B. Without this guard a leaked token
 * would grant access to any concurrent stream until its exp.
 */
export function verifySignallingToken(token: string, opts: VerifyOpts): VerifyResult {
  const env = opts.env ?? process.env;
  const secretOrErr = requireSecret(env);
  if (typeof secretOrErr !== 'string') return { ok: false, error: secretOrErr.error };

  const parts = token.split('.');
  if (parts.length !== 3) return { ok: false, error: 'malformed token' };
  const [headerB64, payloadB64, sigB64] = parts as [string, string, string];

  // Verify signature first so we never trust the payload's claims on a
  // forged token. Use timingSafeEqual to avoid leaking byte-position
  // information about the secret.
  const expected = createHmac('sha256', secretOrErr).update(`${headerB64}.${payloadB64}`).digest();
  let actual: Buffer;
  try { actual = b64urlDecode(sigB64); } catch { return { ok: false, error: 'malformed signature' }; }
  if (expected.length !== actual.length) return { ok: false, error: 'bad signature' };
  if (!timingSafeEqual(expected, actual)) return { ok: false, error: 'bad signature' };

  let header: { alg?: string; typ?: string };
  let payload: SignallingTokenPayload;
  try {
    header = JSON.parse(b64urlDecode(headerB64).toString('utf8'));
    payload = JSON.parse(b64urlDecode(payloadB64).toString('utf8'));
  } catch { return { ok: false, error: 'malformed token body' }; }

  if (header.alg !== ALG) return { ok: false, error: `unexpected alg: ${header.alg}` };
  if (!payload || typeof payload !== 'object') return { ok: false, error: 'malformed payload' };
  if (typeof payload.runId !== 'string') return { ok: false, error: 'missing runId claim' };
  if (typeof payload.exp !== 'number')   return { ok: false, error: 'missing exp claim' };
  if (payload.runId !== opts.expectedRunId) return { ok: false, error: 'token runId mismatch' };
  const nowSec = Math.floor(((opts.now ?? (() => new Date()))()).getTime() / 1000);
  if (nowSec >= payload.exp) return { ok: false, error: 'token expired' };

  return { ok: true, payload };
}
