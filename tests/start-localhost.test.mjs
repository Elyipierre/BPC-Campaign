import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import net from "node:net";
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";
import { afterEach, describe, expect, it } from "vitest";

const THIS_DIR = path.dirname(fileURLToPath(import.meta.url));
const ROOT_DIR = path.resolve(THIS_DIR, "..");
const START_LOCALHOST_SCRIPT_PATH = path.resolve(ROOT_DIR, "scripts/start-localhost.cjs");

const resourcesToCleanup = [];

function createTempDir(prefix) {
  return fs.mkdtempSync(path.join(os.tmpdir(), prefix));
}

async function getFreePort() {
  return new Promise((resolve, reject) => {
    const server = net.createServer();
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => {
      const address = server.address();
      server.close(() => resolve(address.port));
    });
  });
}

async function waitFor(predicate, timeoutMs = 15000) {
  const startedAt = Date.now();
  while ((Date.now() - startedAt) < timeoutMs) {
    if (await predicate()) return true;
    await new Promise(resolve => setTimeout(resolve, 120));
  }
  return false;
}

async function stopChildProcess(child) {
  if (!child || child.exitCode !== null) return;
  child.kill("SIGTERM");
  const exited = await waitFor(() => child.exitCode !== null, 4000);
  if (!exited) {
    child.kill("SIGKILL");
    await waitFor(() => child.exitCode !== null, 2000);
  }
}

async function getJson(url, timeoutMs = 1800) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch(url, { signal: controller.signal });
    const payload = await response.json();
    return { response, payload };
  } finally {
    clearTimeout(timer);
  }
}

function startLauncherProcess({ staticPort, apiPort, cacheDbPath }) {
  let output = "";
  const child = spawn(process.execPath, [START_LOCALHOST_SCRIPT_PATH], {
    cwd: ROOT_DIR,
    env: {
      ...process.env,
      TERRITORY_NO_AUTO_OPEN: "1",
      OVERTURE_UNIT_SYNC_DISABLED: "1",
      OVERTURE_CACHE_DB_PATH: cacheDbPath,
      PORT: String(staticPort),
      OVERTURE_API_PORT: String(apiPort)
    },
    stdio: ["ignore", "pipe", "pipe"]
  });
  child.stdout.on("data", chunk => {
    output += chunk.toString();
  });
  child.stderr.on("data", chunk => {
    output += chunk.toString();
  });
  return {
    child,
    readOutput: () => output
  };
}

function startHungApiProcess(port) {
  const script = `
    const http = require("node:http");
    const targetPort = Number(process.argv[1]);
    const server = http.createServer(() => {});
    server.listen(targetPort, "127.0.0.1");
    process.on("SIGTERM", () => server.close(() => process.exit(0)));
    process.on("SIGINT", () => server.close(() => process.exit(0)));
  `;
  return spawn(process.execPath, ["-e", script, String(port)], {
    cwd: ROOT_DIR,
    stdio: ["ignore", "pipe", "pipe"]
  });
}

afterEach(async () => {
  while (resourcesToCleanup.length) {
    const disposer = resourcesToCleanup.pop();
    try {
      await disposer();
    } catch (_) {}
  }
});

describe("start-localhost launcher", () => {
  it("serves the app and waits for the real API health and status endpoints", async () => {
    const tempDir = createTempDir("territory-start-localhost-");
    const staticPort = await getFreePort();
    const apiPort = await getFreePort();
    const cacheDbPath = path.join(tempDir, "runtime-cache", "overture-cache.db");

    const launcher = startLauncherProcess({ staticPort, apiPort, cacheDbPath });
    resourcesToCleanup.push(async () => {
      await stopChildProcess(launcher.child);
    });

    const ready = await waitFor(async () => {
      try {
        const health = await getJson(`http://127.0.0.1:${apiPort}/health`);
        const status = await getJson(`http://127.0.0.1:${apiPort}/api/local-data/state/status?state=NY`);
        return health.response.ok && health.payload.ok === true && status.response.ok && status.payload.ok !== false;
      } catch (_) {
        return false;
      }
    }, 20000);
    expect(ready).toBe(true);

    const appResponse = await fetch(`http://127.0.0.1:${staticPort}/Territory%20Management.html`);
    const appHtml = await appResponse.text();
    expect(appResponse.ok).toBe(true);
    expect(appHtml).toContain("Territory Management PRO");
  }, 30000);

  it("restarts an unhealthy API listener instead of silently reusing it", async () => {
    const tempDir = createTempDir("territory-start-localhost-restart-");
    const staticPort = await getFreePort();
    const apiPort = await getFreePort();
    const cacheDbPath = path.join(tempDir, "runtime-cache", "overture-cache.db");

    const hungApi = startHungApiProcess(apiPort);
    resourcesToCleanup.push(async () => {
      await stopChildProcess(hungApi);
    });
    const occupied = await waitFor(async () => {
      try {
        const socket = net.createConnection({ host: "127.0.0.1", port: apiPort });
        return await new Promise(resolve => {
          socket.once("connect", () => {
            socket.destroy();
            resolve(true);
          });
          socket.once("error", () => resolve(false));
        });
      } catch (_) {
        return false;
      }
    }, 4000);
    expect(occupied).toBe(true);

    const launcher = startLauncherProcess({ staticPort, apiPort, cacheDbPath });
    resourcesToCleanup.push(async () => {
      await stopChildProcess(launcher.child);
    });

    const ready = await waitFor(async () => {
      try {
        const health = await getJson(`http://127.0.0.1:${apiPort}/health`);
        return health.response.ok && health.payload.ok === true && String(health.payload.service || "") === "local-data-api";
      } catch (_) {
        return false;
      }
    }, 22000);
    expect(ready).toBe(true);
    expect(await waitFor(() => hungApi.exitCode !== null, 6000)).toBe(true);
    expect(launcher.readOutput()).toMatch(/occupied but unhealthy|cleared stale overture api listener/i);
  }, 35000);
});
