#!/usr/bin/env node
"use strict";

const fs = require("node:fs");
const path = require("node:path");
const net = require("node:net");
const { spawn } = require("node:child_process");

const ROOT_DIR = path.resolve(__dirname, "..");
const API_SCRIPT = path.resolve(ROOT_DIR, "server/overture-api.cjs");
const API_HOST = "127.0.0.1";
const MAX_PASSES = 5;
const DEFAULT_OPTIONS = Object.freeze({
  shared_edge_tolerance_m: 2.8,
  edge_tolerance_m: 1.4,
  max_vertex_move_m: 7.5,
  max_area_drift_pct: 1.6,
  min_shared_edge_len_m: 8.0
});

function parseArgs(argv) {
  const args = { input: "", output: "", port: 0 };
  for (let i = 0; i < argv.length; i += 1) {
    const token = String(argv[i] || "");
    const next = String(argv[i + 1] || "");
    if ((token === "-i" || token === "--input") && next) {
      args.input = next;
      i += 1;
      continue;
    }
    if ((token === "-o" || token === "--output") && next) {
      args.output = next;
      i += 1;
      continue;
    }
    if ((token === "-p" || token === "--port") && next) {
      args.port = Number(next) || 0;
      i += 1;
    }
  }
  if (!args.input) {
    throw new Error("Missing --input path.");
  }
  const inputPath = path.resolve(args.input);
  const outputPath = args.output
    ? path.resolve(args.output)
    : inputPath.replace(/\.json$/i, ".aligned.json");
  return { inputPath, outputPath, port: args.port > 0 ? args.port : 0 };
}

function readBackup(inputPath) {
  const raw = fs.readFileSync(inputPath, "utf8");
  const payload = JSON.parse(raw);
  if (!payload || typeof payload !== "object") throw new Error("Backup JSON must be an object.");
  const territories = Array.isArray(payload.territories) ? payload.territories : [];
  if (!territories.length) throw new Error("Backup has no territories.");
  return { payload, territories };
}

function normalizePolygon(polygon) {
  if (!Array.isArray(polygon)) return [];
  return polygon
    .map(point => {
      if (!Array.isArray(point) || point.length < 2) return null;
      const lat = Number(point[0]);
      const lng = Number(point[1]);
      if (!Number.isFinite(lat) || !Number.isFinite(lng)) return null;
      return [lat, lng];
    })
    .filter(Boolean);
}

function toAlignmentTerritories(territories) {
  return territories
    .map((territory, index) => ({
      id: String((territory && territory.id) || `terr_${index + 1}`),
      polygon: normalizePolygon(territory && territory.polygon)
    }))
    .filter(territory => territory.id && territory.polygon.length >= 3);
}

function applyPatchesToTerritories(territories, patches = []) {
  const byId = new Map(territories.map(territory => [String(territory.id || ""), territory]));
  let changed = 0;
  for (const patch of Array.isArray(patches) ? patches : []) {
    const id = String((patch && (patch.territoryId || patch.id)) || "");
    const target = byId.get(id);
    if (!target) continue;
    const polygon = normalizePolygon(patch && patch.polygon);
    if (polygon.length < 3) continue;
    target.polygon = polygon;
    changed += 1;
  }
  return changed;
}

function getFreePort() {
  return new Promise((resolve, reject) => {
    const server = net.createServer();
    server.once("error", reject);
    server.listen(0, API_HOST, () => {
      const address = server.address();
      server.close(() => resolve(address && address.port ? address.port : 8787));
    });
  });
}

function startApi(port) {
  const child = spawn(process.execPath, [API_SCRIPT], {
    cwd: ROOT_DIR,
    env: {
      ...process.env,
      HOST: API_HOST,
      PORT: String(port),
      OVERTURE_UNIT_SYNC_DISABLED: "1"
    },
    stdio: ["ignore", "pipe", "pipe"]
  });
  child.stdout.on("data", chunk => process.stdout.write(String(chunk)));
  child.stderr.on("data", chunk => process.stderr.write(String(chunk)));
  return child;
}

async function waitForHealth(baseUrl, timeoutMs = 12000) {
  const started = Date.now();
  while (Date.now() - started < timeoutMs) {
    try {
      const response = await fetch(`${baseUrl}/health`);
      if (response.ok) {
        const payload = await response.json();
        if (payload && payload.ok) return;
      }
    } catch (_) {
      // retry
    }
    await new Promise(resolve => setTimeout(resolve, 250));
  }
  throw new Error("Timed out waiting for alignment API health.");
}

async function postJson(url, body) {
  const response = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body)
  });
  let payload = null;
  try {
    payload = await response.json();
  } catch (_) {
    payload = null;
  }
  if (!response.ok || !payload || payload.ok === false) {
    const message = String((payload && payload.error) || `HTTP ${response.status}`);
    throw new Error(message);
  }
  return payload;
}

async function runAlignment(baseUrl, territories) {
  const report = {
    startedAt: new Date().toISOString(),
    passes: [],
    totalTerritoriesChanged: 0,
    totalVerticesAligned: 0,
    totalInsertedVertices: 0,
    maxShiftMeters: 0
  };
  for (let pass = 1; pass <= MAX_PASSES; pass += 1) {
    const preview = await postJson(`${baseUrl}/api/local-data/territories/align/preview`, {
      territories: toAlignmentTerritories(territories),
      options: DEFAULT_OPTIONS
    });
    const summary = preview && preview.summary && typeof preview.summary === "object" ? preview.summary : {};
    const impacted = Math.max(0, Number(summary.impactedTerritories) || 0);
    report.passes.push({
      pass,
      impactedTerritories: impacted,
      verticesAligned: Math.max(0, Number(summary.verticesAligned) || 0),
      insertedVertices: Math.max(0, Number(summary.insertedVertices) || 0),
      revertedTerritories: Math.max(0, Number(summary.revertedTerritories) || 0),
      maxShiftMeters: Math.max(0, Number(summary.maxShiftMeters) || 0)
    });
    if (!impacted) break;
    const apply = await postJson(`${baseUrl}/api/local-data/territories/align/apply`, {
      territories: toAlignmentTerritories(territories),
      options: DEFAULT_OPTIONS,
      confirmToken: String((preview && (preview.confirmToken || preview.previewId)) || "")
    });
    const applySummary = apply && apply.summary && typeof apply.summary === "object" ? apply.summary : summary;
    const changedThisPass = applyPatchesToTerritories(territories, apply && apply.patches);
    report.totalTerritoriesChanged += changedThisPass;
    report.totalVerticesAligned += Math.max(0, Number(applySummary.verticesAligned) || 0);
    report.totalInsertedVertices += Math.max(0, Number(applySummary.insertedVertices) || 0);
    report.maxShiftMeters = Math.max(report.maxShiftMeters, Math.max(0, Number(applySummary.maxShiftMeters) || 0));
    if (!changedThisPass) break;
  }
  report.finishedAt = new Date().toISOString();
  report.changed = report.totalTerritoriesChanged > 0;
  return report;
}

async function main() {
  const { inputPath, outputPath, port } = parseArgs(process.argv.slice(2));
  const { payload, territories } = readBackup(inputPath);
  const apiPort = port || (await getFreePort());
  const baseUrl = `http://${API_HOST}:${apiPort}`;
  const api = startApi(apiPort);
  let exited = false;
  api.once("exit", () => { exited = true; });
  try {
    await waitForHealth(baseUrl);
    const report = await runAlignment(baseUrl, territories);
    const outputPayload = {
      ...payload,
      territories,
      alignmentReport: {
        mode: "shared-edge-harmonize",
        options: { ...DEFAULT_OPTIONS },
        ...report
      }
    };
    fs.mkdirSync(path.dirname(outputPath), { recursive: true });
    fs.writeFileSync(outputPath, `${JSON.stringify(outputPayload, null, 2)}\n`, "utf8");
    process.stdout.write(
      `Aligned backup written: ${outputPath}\n` +
      `Changed territories: ${report.totalTerritoriesChanged}\n` +
      `Vertices aligned: ${report.totalVerticesAligned}\n` +
      `Inserted vertices: ${report.totalInsertedVertices}\n` +
      `Max shift (m): ${report.maxShiftMeters.toFixed(2)}\n`
    );
  } finally {
    if (!exited) {
      api.kill("SIGTERM");
      await new Promise(resolve => api.once("exit", resolve));
    }
  }
}

main().catch(error => {
  process.stderr.write(`align-territory-backup failed: ${String((error && error.message) || error || "unknown")}\n`);
  process.exit(1);
});
