/**
 * Visualiser share-link tokens.
 *
 * A share link embeds an opaque random token; the server stores only its
 * SHA-256 hash (same stance as api_keys — plaintext shown once, at mint
 * time, inside the share URL). The PRISM-hosted viewer page exchanges the
 * token for a short-lived signalling JWT carrying the link's tier.
 *
 * Links auto-die with the run: the exchange path refuses any run that is
 * not currently `streaming`, and the FK cascade reaps rows if the run row
 * is ever deleted.
 */
import { createHash, randomBytes } from 'node:crypto';

/** Mint a fresh share token. Returns the plaintext (for the URL) and its hash (for storage). */
export function mintShareToken(): { plaintext: string; hash: string } {
  // 32 bytes base64url ≈ 43 chars — comfortably unguessable, URL-safe.
  const plaintext = randomBytes(32).toString('base64')
    .replace(/=+$/g, '').replace(/\+/g, '-').replace(/\//g, '_');
  return { plaintext, hash: hashShareToken(plaintext) };
}

export function hashShareToken(plaintext: string): string {
  return createHash('sha256').update(plaintext, 'utf8').digest('hex');
}
