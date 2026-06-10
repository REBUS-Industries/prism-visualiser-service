/**
 * Lightweight send helpers for agent WS messages needed by the
 * visualiser-service (signalling proxy + control channel).
 *
 * The full agent protocol handler lives in prism-agent-service.
 * These helpers use the shared sessionRegistry to route frames
 * to the correct agent socket.
 */
import { randomUUID } from 'node:crypto';
import {
  sessionRegistry, envelope,
  type SignallingFrameData, type SignallingViewerCloseData, type SetViewerControlData,
} from '@rebus-industries/prism-shared';

export function sendSignallingFrameToAgent(agentSessionId: string, frame: SignallingFrameData): boolean {
  const conn = sessionRegistry.getAgent(agentSessionId);
  if (!conn || conn.socket.readyState !== conn.socket.OPEN) return false;
  try { conn.socket.send(JSON.stringify(envelope('signallingFrame', frame, randomUUID()))); return true; } catch { return false; }
}

export function sendSignallingViewerCloseToAgent(agentSessionId: string, data: SignallingViewerCloseData): boolean {
  const conn = sessionRegistry.getAgent(agentSessionId);
  if (!conn || conn.socket.readyState !== conn.socket.OPEN) return false;
  try { conn.socket.send(JSON.stringify(envelope('signallingViewerClose', data, randomUUID()))); return true; } catch { return false; }
}

export function sendSetViewerControlToAgent(agentSessionId: string, data: SetViewerControlData): boolean {
  const conn = sessionRegistry.getAgent(agentSessionId);
  if (!conn || conn.socket.readyState !== conn.socket.OPEN) return false;
  try { conn.socket.send(JSON.stringify(envelope('setViewerControl', data, randomUUID()))); return true; } catch { return false; }
}
