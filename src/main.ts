/**
 * PRISM Visualiser Service entry point.
 *
 * Runs as a standalone process sharing the same container image as
 * `main.ts`. Handles everything related to live Pixel Streaming sessions:
 *   - /api/visualiser/* REST surface
 *   - /ws/admin          WebSocket (admin browser pushes + fan-out)
 *   - /ws/visualiser/*   signalling proxy + control channel
 *   - viewer-aware idle reaper
 *
 * Admin broadcasts from the core server or agent-service reach this
 * process via `prism:registry:admin:broadcast` Redis pub/sub; the
 * subscriber calls broadcastAdminLocal() so only process-local admin
 * sockets are notified (no re-publish loop).
 *
 * Port: process.env.PORT ?? 8768
 */
import 'dotenv/config';
import Fastify from 'fastify';
import cookie from '@fastify/cookie';
import cors from '@fastify/cors';
import multipart from '@fastify/multipart';
import fastifyWebsocket from '@fastify/websocket';
import { runBootstrap, tryAuthAdminSession, handleAdminSocket, redisRegistry, sessionRegistry } from '@rebus-industries/prism-shared';
import signallingProxyPlugin from './ws/signallingProxy.js';
import visualiserControlPlugin from './ws/visualiserControl.js';
import { initVisualiserIdleReaper } from './visualiser/idleReaper.js';

const PORT = Number(process.env.PORT ?? 8768);
const HOST = process.env.HOST ?? '0.0.0.0';
const LOG_LEVEL = process.env.LOG_LEVEL ?? 'info';

async function buildApp() {
  const app = Fastify({
    logger: {
      level: LOG_LEVEL,
      transport: process.env.NODE_ENV === 'production'
        ? undefined
        : { target: 'pino-pretty', options: { translateTime: 'SYS:HH:MM:ss.l', ignore: 'pid,hostname' } },
    },
    bodyLimit: 64 * 1024 * 1024,
    trustProxy: true,
  });

  const sessionSecret = process.env.SESSION_SECRET;
  if (!sessionSecret) {
    app.log.warn('SESSION_SECRET is not set — admin login cookies will not be signable. Set this in production!');
  }
  await app.register(cookie, { secret: sessionSecret ?? 'unsafe-dev-only-do-not-use-in-prod' });
  await app.register(cors, {
    origin: (origin, cb) => {
      if (!origin) return cb(null, true);
      if (process.env.NODE_ENV !== 'production') return cb(null, true);
      const allowed = (process.env.CORS_ALLOWED_ORIGINS ?? '').split(',').map((s) => s.trim()).filter(Boolean);
      cb(null, allowed.includes(origin));
    },
    credentials: true,
  });
  await app.register(multipart, {
    limits: { fileSize: 1024 * 1024 * 1024, files: 1, fields: 32 },
  });

  app.get('/health', async () => ({ status: 'ok', service: 'prism-visualiser' }));

  // WS plugins must be registered before REST routes so the upgrade handler
  // wins for /ws/* paths.
  await app.register(fastifyWebsocket, {
    options: { maxPayload: 16 * 1024 * 1024 },
  });

  // Arm the viewer-aware idle reaper so streaming runs with zero connected
  // viewers are reclaimed after VISUALISER_IDLE_TIMEOUT_MS.
  initVisualiserIdleReaper(app.log);

  app.get('/ws/admin', {
    websocket: true,
    preHandler: async (req, reply) => {
      const ok = await tryAuthAdminSession(req);
      if (!ok) reply.code(401).send({ error: 'admin session required' });
    },
  }, (socket, req) => {
    handleAdminSocket(socket, req.log);
  });

  // Pixel Streaming signalling proxy: /ws/visualiser/:runId/signalling
  await app.register(signallingProxyPlugin);

  // Visualiser control channel (controller lock): /ws/visualiser/:runId/control
  await app.register(visualiserControlPlugin);

  // Full visualiser REST surface.
  await app.register(import('./api/visualiser.js'), { prefix: '/api/visualiser' });

  return app;
}

async function main() {
  const app = await buildApp();
  try {
    await runBootstrap(app.log);
  } catch (err) {
    app.log.error({ err }, 'bootstrap failed');
    process.exit(1);
  }

  try {
    await app.listen({ host: HOST, port: PORT });
  } catch (err) {
    app.log.error(err);
    process.exit(1);
  }

  // Subscribe to cross-process admin broadcasts published by the core server
  // or agent-service. Fan out to admin sockets that are local to this process.
  // broadcastAdminLocal avoids re-publishing to Redis, preventing a feedback loop.
  void redisRegistry.subscribeToAdminBroadcast((topic, frame) => {
    sessionRegistry.broadcastAdminLocal(topic, frame);
  });

  for (const sig of ['SIGINT', 'SIGTERM'] as const) {
    process.on(sig, async () => {
      app.log.info({ sig }, 'shutdown');
      await app.close();
      process.exit(0);
    });
  }
}

main();
