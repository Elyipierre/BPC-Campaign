#!/usr/bin/env node
const http = require("node:http");
const fs = require("node:fs");
const path = require("node:path");
const net = require("node:net");
const { spawn, execFileSync } = require("node:child_process");

const ROOT_DIR = path.resolve(__dirname, "..");
const DEFAULT_ENTRY = "Territory Management.html";
const DEFAULT_PORT = Number(process.env.PORT || 4173);
const API_PORT = Number(process.env.OVERTURE_API_PORT || 8787);
const HOST = "127.0.0.1";
const AUTO_OPEN_BROWSER = String(process.env.TERRITORY_NO_AUTO_OPEN || "").trim() !== "1";
const HEALTH_PROBE_TIMEOUT_MS = Math.max(1000, Number(process.env.TERRITORY_HEALTH_TIMEOUT_MS) || 3000);
const HEALTH_RETRY_DELAY_MS = Math.max(120, Number(process.env.TERRITORY_HEALTH_RETRY_DELAY_MS) || 450);
const STATIC_HEALTH_MAX_ATTEMPTS = Math.max(2, Number(process.env.TERRITORY_STATIC_HEALTH_ATTEMPTS) || 16);
const API_HEALTH_MAX_ATTEMPTS = Math.max(4, Number(process.env.TERRITORY_API_HEALTH_ATTEMPTS) || 32);

let apiProcess = null;
let staticServer = null;

const MIME_TYPES = {
  ".html": "text/html; charset=utf-8",
  ".css": "text/css; charset=utf-8",
  ".js": "application/javascript; charset=utf-8",
  ".json": "application/json; charset=utf-8",
  ".ndjson": "application/x-ndjson; charset=utf-8",
  ".csv": "text/csv; charset=utf-8",
  ".txt": "text/plain; charset=utf-8",
  ".png": "image/png",
  ".jpg": "image/jpeg",
  ".jpeg": "image/jpeg",
  ".svg": "image/svg+xml",
  ".ico": "image/x-icon",
  ".map": "application/json; charset=utf-8"
};

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, Math.max(0, Number(ms) || 0)));
}

function getEntryUrl(port = DEFAULT_PORT) {
  return `http://${HOST}:${port}/${encodeURIComponent(DEFAULT_ENTRY).replace(/%2F/g, "/")}`;
}

function getApiHealthUrl(port = API_PORT) {
  return `http://${HOST}:${port}/health`;
}

function getApiStatusUrl(port = API_PORT) {
  return `http://${HOST}:${port}/api/local-data/state/status?state=NY`;
}

function openBrowser(url) {
  const platform = process.platform;
  try {
    if (platform === "win32") {
      spawn("cmd", ["/c", "start", "", url], { detached: true, stdio: "ignore" }).unref();
      return;
    }
    if (platform === "darwin") {
      spawn("open", [url], { detached: true, stdio: "ignore" }).unref();
      return;
    }
    spawn("xdg-open", [url], { detached: true, stdio: "ignore" }).unref();
  } catch (err) {
    console.warn(`[start:local] could not auto-open browser: ${String((err && err.message) || err || "unknown")}`);
  }
}

function normalizeRequestPath(urlPath) {
  let decoded = "/";
  try {
    decoded = decodeURIComponent(String(urlPath || "/"));
  } catch (_) {
    decoded = "/";
  }
  const clean = decoded.split("?")[0].split("#")[0];
  if (clean === "/" || !clean) return DEFAULT_ENTRY;
  return clean.replace(/^\/+/, "");
}

function resolveFilePath(urlPath) {
  const relativePath = normalizeRequestPath(urlPath);
  const absPath = path.resolve(ROOT_DIR, relativePath);
  const relative = path.relative(ROOT_DIR, absPath);
  if (relative.startsWith("..") || path.isAbsolute(relative)) return null;
  return absPath;
}

function serveFile(filePath, res) {
  fs.stat(filePath, (statErr, stat) => {
    if (statErr || !stat || !stat.isFile()) {
      res.writeHead(404, { "Content-Type": "text/plain; charset=utf-8" });
      res.end("Not found");
      return;
    }
    const ext = path.extname(filePath).toLowerCase();
    const contentType = MIME_TYPES[ext] || "application/octet-stream";
    res.writeHead(200, {
      "Content-Type": contentType,
      "Cache-Control": "no-cache"
    });
    fs.createReadStream(filePath).pipe(res);
  });
}

function createStaticServer() {
  return http.createServer((req, res) => {
    const filePath = resolveFilePath(req.url || "/");
    if (!filePath) {
      res.writeHead(400, { "Content-Type": "text/plain; charset=utf-8" });
      res.end("Bad request");
      return;
    }
    serveFile(filePath, res);
  });
}

function listenServer(server, port) {
  return new Promise((resolve, reject) => {
    const onError = (err) => {
      server.off("listening", onListening);
      reject(err);
    };
    const onListening = () => {
      server.off("error", onError);
      resolve(server);
    };
    server.once("error", onError);
    server.once("listening", onListening);
    server.listen(port, HOST);
  });
}

async function fetchWithTimeout(url, options = {}) {
  const timeoutMs = Math.max(250, Number(options.timeoutMs) || HEALTH_PROBE_TIMEOUT_MS);
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    return await fetch(url, {
      method: options.method || "GET",
      headers: options.headers || undefined,
      body: options.body,
      signal: controller.signal
    });
  } catch (error) {
    if (error && error.name === "AbortError") throw new Error("timeout");
    throw error;
  } finally {
    clearTimeout(timer);
  }
}

async function probeStaticHost(port = DEFAULT_PORT) {
  try {
    const response = await fetchWithTimeout(getEntryUrl(port));
    const text = response && typeof response.text === "function" ? await response.text() : "";
    const ok = !!response.ok && /Territory Management PRO/i.test(String(text || ""));
    return {
      ok,
      status: response.status,
      detail: ok ? "" : `unexpected response from ${getEntryUrl(port)}`
    };
  } catch (error) {
    return {
      ok: false,
      error,
      detail: String((error && error.message) || error || "request failed")
    };
  }
}

async function probeApiReadiness(port = API_PORT) {
  try {
    const healthResponse = await fetchWithTimeout(getApiHealthUrl(port));
    const healthPayload = healthResponse && typeof healthResponse.json === "function" ? await healthResponse.json() : null;
    if (!healthResponse.ok || !healthPayload || healthPayload.ok !== true) {
      return {
        ok: false,
        status: healthResponse ? healthResponse.status : 0,
        detail: `health check failed with HTTP ${healthResponse ? healthResponse.status : "error"}`
      };
    }
    const statusResponse = await fetchWithTimeout(getApiStatusUrl(port));
    const statusPayload = statusResponse && typeof statusResponse.json === "function" ? await statusResponse.json() : null;
    const statusOk = !!statusResponse.ok && !!statusPayload && statusPayload.ok !== false && String(statusPayload.state || "").toUpperCase() === "NY";
    return {
      ok: statusOk,
      status: statusResponse ? statusResponse.status : 0,
      detail: statusOk ? "" : `state status probe failed with HTTP ${statusResponse ? statusResponse.status : "error"}`
    };
  } catch (error) {
    return {
      ok: false,
      error,
      detail: String((error && error.message) || error || "request failed")
    };
  }
}

async function waitForProbe(probeFn, options = {}) {
  const attempts = Math.max(1, Number(options.attempts) || 1);
  const delayMs = Math.max(0, Number(options.delayMs) || 0);
  let lastResult = { ok: false, detail: "probe not run" };
  for (let attempt = 1; attempt <= attempts; attempt += 1) {
    lastResult = await probeFn();
    if (lastResult && lastResult.ok) return lastResult;
    if (attempt < attempts) await sleep(delayMs);
  }
  return lastResult;
}

function listPidsOnPort(port) {
  const targetPort = Math.max(1, Number(port) || 0);
  if (!targetPort) return [];
  try {
    if (process.platform === "win32") {
      const output = execFileSync("cmd", ["/c", "netstat -ano -p tcp"], { encoding: "utf8" });
      return Array.from(new Set(output.split(/\r?\n/).map(line => line.trim()).filter(Boolean).reduce((pids, line) => {
        const parts = line.split(/\s+/);
        if (parts.length < 5 || String(parts[0] || "").toUpperCase() !== "TCP") return pids;
        const localAddress = String(parts[1] || "");
        const state = String(parts[3] || "").toUpperCase();
        const pid = Number(parts[4]);
        if (!localAddress.endsWith(`:${targetPort}`) || state !== "LISTENING" || !Number.isFinite(pid)) return pids;
        pids.push(pid);
        return pids;
      }, [])));
    }
    const output = execFileSync("lsof", ["-nP", `-iTCP:${targetPort}`, "-sTCP:LISTEN", "-t"], { encoding: "utf8" });
    return Array.from(new Set(output.split(/\r?\n/).map(value => Number(value.trim())).filter(pid => Number.isFinite(pid) && pid > 0)));
  } catch (_) {
    return [];
  }
}

function killPid(pid) {
  const targetPid = Number(pid);
  if (!Number.isFinite(targetPid) || targetPid <= 0 || targetPid === process.pid) return false;
  try {
    if (process.platform === "win32") {
      execFileSync("taskkill", ["/PID", String(targetPid), "/T", "/F"], { stdio: "ignore" });
    } else {
      process.kill(targetPid, "SIGTERM");
    }
    return true;
  } catch (_) {
    return false;
  }
}

async function isPortBusy(port) {
  return new Promise((resolve) => {
    const socket = net.createConnection({ host: HOST, port: Number(port) });
    let settled = false;
    const finish = (value) => {
      if (settled) return;
      settled = true;
      try { socket.destroy(); } catch (_) {}
      resolve(value);
    };
    socket.once("connect", () => finish(true));
    socket.once("error", () => finish(false));
    socket.setTimeout(500, () => finish(true));
  });
}

async function waitForPortState(port, inUse, timeoutMs = 5000) {
  const startedAt = Date.now();
  while ((Date.now() - startedAt) < timeoutMs) {
    const busy = await isPortBusy(port);
    if (busy === inUse) return true;
    await sleep(120);
  }
  return false;
}

async function killPortListeners(port, label) {
  const pids = listPidsOnPort(port).filter(pid => pid !== process.pid);
  if (!pids.length) {
    const stillBusy = await isPortBusy(port);
    if (!stillBusy) return { killed: 0, pids: [] };
    throw new Error(`Unable to identify the ${label} listener on port ${port}.`);
  }
  pids.forEach(pid => killPid(pid));
  let freed = await waitForPortState(port, false, 6000);
  if (!freed && process.platform !== "win32") {
    pids.forEach(pid => {
      try { process.kill(pid, "SIGKILL"); } catch (_) {}
    });
    freed = await waitForPortState(port, false, 3000);
  }
  if (!freed) throw new Error(`Unable to free ${label} on port ${port}.`);
  return { killed: pids.length, pids };
}

function spawnApiDaemon(staticPort, options = {}) {
  const apiScriptPath = path.join("server", "overture-api.cjs");
  const manifestUrl = `http://${HOST}:${staticPort}/data/packages/manifest.json`;
  const detached = !!options.detached;
  const env = {
    ...process.env,
    PORT: String(API_PORT),
    HOST,
    STATIC_PORT: String(staticPort),
    PACKAGE_MANIFEST_URL: String(process.env.PACKAGE_MANIFEST_URL || manifestUrl)
  };
  const child = spawn(process.execPath, [apiScriptPath], {
    cwd: ROOT_DIR,
    env,
    detached,
    stdio: detached ? "ignore" : "inherit"
  });
  if (detached) child.unref();
  return child;
}

function trackApiProcess(child) {
  apiProcess = child;
  child.on("exit", (code, signal) => {
    if (apiProcess !== child) return;
    apiProcess = null;
    if (signal === "SIGTERM" || signal === "SIGINT") return;
    if (code && Number(code) !== 0) {
      console.warn(`[start:local] overture api exited with code ${code}.`);
    }
  });
}

function stopTrackedApiDaemon() {
  if (!apiProcess || !apiProcess.pid) return;
  killPid(apiProcess.pid);
  apiProcess = null;
}

async function ensureStaticHostReady(port = DEFAULT_PORT) {
  const server = createStaticServer();
  try {
    await listenServer(server, port);
    staticServer = server;
    console.log(`[start:local] serving ${ROOT_DIR}`);
    console.log(`[start:local] ${getEntryUrl(port)}`);
    return { reused: false, restarted: false, server };
  } catch (err) {
    if (err && err.code !== "EADDRINUSE") throw err;
    const healthyExisting = await waitForProbe(() => probeStaticHost(port), {
      attempts: 2,
      delayMs: 180
    });
    if (healthyExisting.ok) {
      console.log(`[start:local] reusing existing static host: ${getEntryUrl(port)}`);
      return { reused: true, restarted: false, server: null };
    }
    console.warn(`[start:local] static host port ${port} is occupied but unhealthy. Restarting it.`);
    await killPortListeners(port, "static host");
    const replacement = createStaticServer();
    await listenServer(replacement, port);
    staticServer = replacement;
    console.log(`[start:local] recovered static host: ${getEntryUrl(port)}`);
    return { reused: false, restarted: true, server: replacement };
  }
}

async function ensureApiReady(staticPort, options = {}) {
  const detached = !!options.detached;
  const healthyExisting = await waitForProbe(() => probeApiReadiness(API_PORT), {
    attempts: 2,
    delayMs: 180
  });
  if (healthyExisting.ok) {
    console.log(`[start:local] reusing healthy overture api on ${getApiHealthUrl(API_PORT)}`);
    return { reused: true, restarted: false };
  }

  const apiPortBusy = await isPortBusy(API_PORT);
  if (apiPortBusy) {
    console.warn(`[start:local] api port ${API_PORT} is occupied but unhealthy. Restarting daemon.`);
    stopTrackedApiDaemon();
    const restartInfo = await killPortListeners(API_PORT, "overture api");
    console.log(`[start:local] cleared stale overture api listener(s): ${restartInfo.pids.join(", ")}`);
  }

  console.log(`[start:local] launching overture api on http://${HOST}:${API_PORT}`);
  const child = spawnApiDaemon(staticPort, { detached });
  if (!detached) trackApiProcess(child);
  const apiReady = await waitForProbe(() => probeApiReadiness(API_PORT), {
    attempts: API_HEALTH_MAX_ATTEMPTS,
    delayMs: HEALTH_RETRY_DELAY_MS
  });
  if (!apiReady.ok) {
    if (detached) killPid(child.pid);
    else stopTrackedApiDaemon();
    throw new Error(`Local Data API failed health checks on ${getApiHealthUrl(API_PORT)} (${apiReady.detail || "no response"})`);
  }
  console.log(`[start:local] overture api ready on ${getApiHealthUrl(API_PORT)}`);
  return { reused: false, restarted: apiPortBusy };
}

function registerShutdownHandlers() {
  const shutdown = () => {
    stopTrackedApiDaemon();
    if (staticServer && typeof staticServer.close === "function") {
      try { staticServer.close(); } catch (_) {}
      staticServer = null;
    }
    process.exit(0);
  };
  process.on("SIGINT", shutdown);
  process.on("SIGTERM", shutdown);
  process.on("exit", () => {
    stopTrackedApiDaemon();
  });
}

async function main() {
  const staticHost = await ensureStaticHostReady(DEFAULT_PORT);
  const staticReady = await waitForProbe(() => probeStaticHost(DEFAULT_PORT), {
    attempts: STATIC_HEALTH_MAX_ATTEMPTS,
    delayMs: HEALTH_RETRY_DELAY_MS
  });
  if (!staticReady.ok) {
    throw new Error(`Static host failed health checks on ${getEntryUrl(DEFAULT_PORT)} (${staticReady.detail || "no response"})`);
  }

  await ensureApiReady(DEFAULT_PORT, { detached: !staticHost.server });

  const url = getEntryUrl(DEFAULT_PORT);
  console.log(`[start:local] ready: ${url}`);
  if (AUTO_OPEN_BROWSER) openBrowser(url);

  if (staticHost.server) {
    registerShutdownHandlers();
    return;
  }
}

main().catch(err => {
  stopTrackedApiDaemon();
  if (staticServer && typeof staticServer.close === "function") {
    try { staticServer.close(); } catch (_) {}
    staticServer = null;
  }
  console.error(`[start:local] ${String((err && err.message) || err || "unknown")}`);
  process.exit(1);
});
