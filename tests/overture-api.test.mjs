import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import http from "node:http";
import net from "node:net";
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";
import { DatabaseSync } from "node:sqlite";
import { afterEach, describe, expect, it } from "vitest";

const THIS_DIR = path.dirname(fileURLToPath(import.meta.url));
const ROOT_DIR = path.resolve(THIS_DIR, "..");
const API_SCRIPT_PATH = path.resolve(ROOT_DIR, "server/overture-api.cjs");

function createTempDir(prefix) {
  return fs.mkdtempSync(path.join(os.tmpdir(), prefix));
}

function writeFile(filePath, text) {
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  fs.writeFileSync(filePath, text, "utf8");
}

function createFixturePackage(rootDir, options = {}) {
  const includeNearEdgeResidential = options.includeNearEdgeResidential === true;
  const manifestPath = path.join(rootDir, "manifest.json");
  const addressesPath = path.join(rootDir, "states", "NY", "addresses-ny-0001.ndjson");
  const buildingsPath = path.join(rootDir, "states", "NY", "buildings-ny-0001.ndjson");

  const addressesRows = [
    {
      id: "addr-1",
      house_number: "10",
      street: "Main St",
      unit: "",
      city: "Queens",
      region: "NY",
      postcode: "11101",
      country_code: "US",
      full_address: "10 Main St, Queens, NY 11101",
      lat: 40.705,
      lng: -73.895
    },
    {
      id: "addr-2",
      house_number: "99",
      street: "Market St",
      unit: "",
      city: "Queens",
      region: "NY",
      postcode: "11101",
      country_code: "US",
      full_address: "99 Market St, Queens, NY 11101",
      lat: 40.705,
      lng: -73.907
    }
  ];
  if (includeNearEdgeResidential) {
    addressesRows.push({
      id: "addr-3",
      house_number: "12",
      street: "Main St",
      unit: "2B",
      city: "Queens",
      region: "NY",
      postcode: "11101",
      country_code: "US",
      full_address: "12 Main St Apt 2B, Queens, NY 11101",
      lat: 40.7053,
      lng: -73.895
    });
  }

  const buildingsRows = [
    {
      id: "b-res",
      building_class: "apartments",
      levels: "3",
      name: "Residential",
      geom_wkt: "POLYGON ((-73.900 40.700, -73.900 40.710, -73.890 40.710, -73.890 40.700, -73.900 40.700))"
    },
    {
      id: "b-com",
      building_class: "commercial",
      levels: "1",
      name: "Commercial",
      geom_wkt: "POLYGON ((-73.910 40.700, -73.910 40.710, -73.905 40.710, -73.905 40.700, -73.910 40.700))"
    }
  ];
  if (includeNearEdgeResidential) {
    buildingsRows.push({
      id: "b-res-edge",
      building_class: "apartments",
      levels: "4",
      name: "Residential Edge",
      geom_wkt: "POLYGON ((-73.89505 40.70525, -73.89505 40.70535, -73.89495 40.70535, -73.89495 40.70525, -73.89505 40.70525))"
    });
  }

  const manifest = {
    schema_version: 2,
    release: "2026-01-21.0",
    states: {
      NY: {
        state: "NY",
        release: "2026-01-21.0",
        datasets: {
          addresses: {
            format: "ndjson",
            count: addressesRows.length,
            chunks: [{ path: "states/NY/addresses-ny-0001.ndjson", count: addressesRows.length }]
          },
          buildings: {
            format: "ndjson",
            count: buildingsRows.length,
            chunks: [{ path: "states/NY/buildings-ny-0001.ndjson", count: buildingsRows.length }]
          }
        }
      }
    }
  };

  writeFile(manifestPath, JSON.stringify(manifest, null, 2));
  writeFile(addressesPath, `${addressesRows.map(row => JSON.stringify(row)).join("\n")}\n`);
  writeFile(buildingsPath, `${buildingsRows.map(row => JSON.stringify(row)).join("\n")}\n`);
}

function getMimeType(filePath) {
  if (filePath.endsWith(".json")) return "application/json; charset=utf-8";
  if (filePath.endsWith(".ndjson")) return "application/x-ndjson; charset=utf-8";
  return "text/plain; charset=utf-8";
}

async function getFreePort() {
  return new Promise((resolve, reject) => {
    const server = net.createServer();
    server.on("error", reject);
    server.listen(0, "127.0.0.1", () => {
      const address = server.address();
      server.close(() => resolve(address.port));
    });
  });
}

async function startStaticServer(rootDir) {
  const port = await getFreePort();
  const host = "127.0.0.1";
  const server = http.createServer((req, res) => {
    const reqUrl = new URL(req.url || "/", `http://${host}:${port}`);
    const target = path.resolve(rootDir, `.${decodeURIComponent(reqUrl.pathname)}`);
    const safeRoot = `${path.resolve(rootDir)}${path.sep}`;
    if (!(target + path.sep).startsWith(safeRoot) && target !== path.resolve(rootDir)) {
      res.writeHead(400, { "Content-Type": "text/plain; charset=utf-8" });
      res.end("Bad request");
      return;
    }
    fs.stat(target, (err, stat) => {
      if (err || !stat || !stat.isFile()) {
        res.writeHead(404, { "Content-Type": "text/plain; charset=utf-8" });
        res.end("Not found");
        return;
      }
      res.writeHead(200, { "Content-Type": getMimeType(target) });
      fs.createReadStream(target).pipe(res);
    });
  });
  await new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen(port, host, resolve);
  });
  return {
    host,
    port,
    server,
    close: async () => new Promise(resolve => server.close(resolve))
  };
}

async function startSocrataMockServer(datasetId, rows = []) {
  const port = await getFreePort();
  const host = "127.0.0.1";
  const server = http.createServer((req, res) => {
    const reqUrl = new URL(req.url || "/", `http://${host}:${port}`);
    if (reqUrl.pathname !== `/resource/${datasetId}.json`) {
      res.writeHead(404, { "Content-Type": "application/json; charset=utf-8" });
      res.end(JSON.stringify({ error: "not_found" }));
      return;
    }
    const limit = Math.max(1, Number(reqUrl.searchParams.get("$limit")) || 50000);
    const offset = Math.max(0, Number(reqUrl.searchParams.get("$offset")) || 0);
    const page = rows.slice(offset, offset + limit);
    res.writeHead(200, { "Content-Type": "application/json; charset=utf-8" });
    res.end(JSON.stringify(page));
  });
  await new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen(port, host, resolve);
  });
  return {
    host,
    port,
    close: async () => new Promise(resolve => server.close(resolve))
  };
}

async function waitFor(predicate, timeoutMs = 6000) {
  const start = Date.now();
  while ((Date.now() - start) < timeoutMs) {
    if (await predicate()) return true;
    await new Promise(resolve => setTimeout(resolve, 80));
  }
  return false;
}

function startApiProcess({ apiPort, manifestUrl, dbPath, unitSyncDisabled = true, unitSourceProfilePath = "" }) {
  const host = "127.0.0.1";
  const child = spawn(process.execPath, [API_SCRIPT_PATH], {
    cwd: ROOT_DIR,
    env: {
      ...process.env,
      HOST: host,
      PORT: String(apiPort),
      PACKAGE_MANIFEST_URL: manifestUrl,
      OVERTURE_CACHE_DB_PATH: dbPath,
      OVERTURE_UNIT_SYNC_DISABLED: unitSyncDisabled ? "1" : "0",
      ...(unitSourceProfilePath ? { OVERTURE_UNIT_SOURCE_PROFILE_PATH: unitSourceProfilePath } : {})
    },
    stdio: ["ignore", "pipe", "pipe"]
  });
  return { child, host, port: apiPort };
}

async function postJson(url, body) {
  const response = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body || {})
  });
  const payload = await response.json();
  return { response, payload };
}

async function getJson(url) {
  const response = await fetch(url);
  const payload = await response.json();
  return { response, payload };
}

const resourcesToCleanup = [];

afterEach(async () => {
  while (resourcesToCleanup.length) {
    const disposer = resourcesToCleanup.pop();
    try {
      await disposer();
    } catch (_) {}
  }
});

describe("overture-api local cache", () => {
  it("installs NY from manifest, reports ready status, and enforces strict residential filtering", async () => {
    const tempDir = createTempDir("territory-overture-api-");
    const packageRoot = path.join(tempDir, "packages");
    createFixturePackage(packageRoot);

    const staticServer = await startStaticServer(packageRoot);
    resourcesToCleanup.push(() => staticServer.close());

    const apiPort = await getFreePort();
    const apiDbPath = path.join(tempDir, "runtime-cache", "overture-cache.db");
    const manifestUrl = `http://${staticServer.host}:${staticServer.port}/manifest.json`;
    const api = startApiProcess({ apiPort, manifestUrl, dbPath: apiDbPath });
    resourcesToCleanup.push(async () => {
      api.child.kill("SIGTERM");
      await new Promise(resolve => api.child.once("exit", resolve));
    });

    const healthReady = await waitFor(async () => {
      try {
        const { payload } = await getJson(`http://${api.host}:${api.port}/health`);
        return payload && payload.ok === true;
      } catch (_) {
        return false;
      }
    }, 8000);
    expect(healthReady).toBe(true);

    const ensure = await postJson(`http://${api.host}:${api.port}/api/local-data/state/ensure`, { state: "NY" });
    expect([200, 202]).toContain(ensure.response.status);
    expect(ensure.payload.ok).toBe(true);
    expect(ensure.payload.state).toBe("NY");

    const statusReady = await waitFor(async () => {
      const { payload } = await getJson(`http://${api.host}:${api.port}/api/local-data/state/status?state=NY`);
      return payload && payload.phase === "ready" && payload.strictResidentialReady === true;
    }, 12000);
    expect(statusReady).toBe(true);

    const { payload: statusPayload } = await getJson(`http://${api.host}:${api.port}/api/local-data/state/status?state=NY`);
    expect(statusPayload.phase).toBe("ready");
    expect(statusPayload.strictResidentialReady).toBe(true);
    expect(statusPayload.datasetCounts.addresses).toBe(2);
    expect(statusPayload.datasetCounts.buildings).toBe(2);

    const polygon = [
      [40.699, -73.911],
      [40.711, -73.911],
      [40.711, -73.889],
      [40.699, -73.889]
    ];
    const search = await postJson(`http://${api.host}:${api.port}/api/local-data/addresses/search`, {
      state: "NY",
      polygon,
      limit: 100,
      strictResidential: true
    });
    expect(search.response.status).toBe(200);
    expect(search.payload.ok).toBe(true);
    expect(search.payload.count).toBe(1);
    expect(search.payload.rows[0].id).toBe("addr-1");
  });

  it("keeps cached ready state across API restart", async () => {
    const tempDir = createTempDir("territory-overture-api-restart-");
    const packageRoot = path.join(tempDir, "packages");
    createFixturePackage(packageRoot);

    const staticServer = await startStaticServer(packageRoot);
    resourcesToCleanup.push(() => staticServer.close());

    const apiPort = await getFreePort();
    const apiDbPath = path.join(tempDir, "runtime-cache", "overture-cache.db");
    const manifestUrl = `http://${staticServer.host}:${staticServer.port}/manifest.json`;

    const firstApi = startApiProcess({ apiPort, manifestUrl, dbPath: apiDbPath });
    const stopFirst = async () => {
      firstApi.child.kill("SIGTERM");
      await new Promise(resolve => firstApi.child.once("exit", resolve));
    };
    resourcesToCleanup.push(stopFirst);

    const firstReady = await waitFor(async () => {
      try {
        const { payload } = await getJson(`http://${firstApi.host}:${firstApi.port}/health`);
        return payload && payload.ok === true;
      } catch (_) {
        return false;
      }
    }, 8000);
    expect(firstReady).toBe(true);

    await postJson(`http://${firstApi.host}:${firstApi.port}/api/local-data/state/ensure`, { state: "NY" });
    const installed = await waitFor(async () => {
      const { payload } = await getJson(`http://${firstApi.host}:${firstApi.port}/api/local-data/state/status?state=NY`);
      return payload && payload.phase === "ready" && payload.strictResidentialReady === true;
    }, 12000);
    expect(installed).toBe(true);

    await stopFirst();
    resourcesToCleanup.pop();

    const secondApi = startApiProcess({ apiPort, manifestUrl, dbPath: apiDbPath });
    resourcesToCleanup.push(async () => {
      secondApi.child.kill("SIGTERM");
      await new Promise(resolve => secondApi.child.once("exit", resolve));
    });

    const secondReady = await waitFor(async () => {
      try {
        const { payload } = await getJson(`http://${secondApi.host}:${secondApi.port}/health`);
        return payload && payload.ok === true;
      } catch (_) {
        return false;
      }
    }, 8000);
    expect(secondReady).toBe(true);

    const { payload: statusAfterRestart } = await getJson(`http://${secondApi.host}:${secondApi.port}/api/local-data/state/status?state=NY`);
    expect(statusAfterRestart.phase).toBe("ready");
    expect(statusAfterRestart.strictResidentialReady).toBe(true);
    expect(statusAfterRestart.release).toBe("2026-01-21.0");
  });

  it("includes near-edge residential points via strict proximity fallback", async () => {
    const tempDir = createTempDir("territory-overture-api-edge-");
    const packageRoot = path.join(tempDir, "packages");
    createFixturePackage(packageRoot, { includeNearEdgeResidential: true });

    const staticServer = await startStaticServer(packageRoot);
    resourcesToCleanup.push(() => staticServer.close());

    const apiPort = await getFreePort();
    const apiDbPath = path.join(tempDir, "runtime-cache", "overture-cache.db");
    const manifestUrl = `http://${staticServer.host}:${staticServer.port}/manifest.json`;
    const api = startApiProcess({ apiPort, manifestUrl, dbPath: apiDbPath });
    resourcesToCleanup.push(async () => {
      api.child.kill("SIGTERM");
      await new Promise(resolve => api.child.once("exit", resolve));
    });

    const healthReady = await waitFor(async () => {
      try {
        const { payload } = await getJson(`http://${api.host}:${api.port}/health`);
        return payload && payload.ok === true;
      } catch (_) {
        return false;
      }
    }, 8000);
    expect(healthReady).toBe(true);

    await postJson(`http://${api.host}:${api.port}/api/local-data/state/ensure`, { state: "NY" });
    const statusReady = await waitFor(async () => {
      const { payload } = await getJson(`http://${api.host}:${api.port}/api/local-data/state/status?state=NY`);
      return payload && payload.phase === "ready" && payload.strictResidentialReady === true;
    }, 12000);
    expect(statusReady).toBe(true);

    const polygon = [
      [40.70495, -73.89505],
      [40.70505, -73.89505],
      [40.70505, -73.89495],
      [40.70495, -73.89495]
    ];
    const search = await postJson(`http://${api.host}:${api.port}/api/local-data/addresses/search`, {
      state: "NY",
      polygon,
      limit: 100,
      strictResidential: true
    });
    expect(search.response.status).toBe(200);
    expect(search.payload.ok).toBe(true);
    expect(search.payload.count).toBe(2);
    const ids = search.payload.rows.map(row => row.id).sort();
    expect(ids).toEqual(["addr-1", "addr-3"]);
    expect(search.payload.relaxedByProximity).toBe(true);
    expect(Number(search.payload.territoryToleranceMeters)).toBeGreaterThanOrEqual(35);
  });

  it("merges unit evidence rows and suppresses matching parent unitless rows", async () => {
    const tempDir = createTempDir("territory-local-data-unit-sync-");
    const packageRoot = path.join(tempDir, "packages");
    createFixturePackage(packageRoot);

    const staticServer = await startStaticServer(packageRoot);
    resourcesToCleanup.push(() => staticServer.close());

    const apiPort = await getFreePort();
    const apiDbPath = path.join(tempDir, "runtime-cache", "overture-cache.db");
    const manifestUrl = `http://${staticServer.host}:${staticServer.port}/manifest.json`;
    const api = startApiProcess({ apiPort, manifestUrl, dbPath: apiDbPath, unitSyncDisabled: true });
    resourcesToCleanup.push(async () => {
      api.child.kill("SIGTERM");
      await new Promise(resolve => api.child.once("exit", resolve));
    });

    const healthReady = await waitFor(async () => {
      try {
        const { payload } = await getJson(`http://${api.host}:${api.port}/health`);
        return payload && payload.ok === true;
      } catch (_) {
        return false;
      }
    }, 8000);
    expect(healthReady).toBe(true);

    await postJson(`http://${api.host}:${api.port}/api/local-data/state/ensure`, { state: "NY" });
    const datasetReady = await waitFor(async () => {
      const { payload } = await getJson(`http://${api.host}:${api.port}/api/local-data/state/status?state=NY`);
      return payload && payload.phase === "ready" && payload.strictResidentialReady === true;
    }, 12000);
    expect(datasetReady).toBe(true);

    const directDb = new DatabaseSync(apiDbPath);
    try {
      directDb.prepare(`
        INSERT INTO unit_address_evidence (
          state, source_id, source_record_id, house_number, street, unit, city, region, postcode, lat, lng, geom_wkt,
          building_id, confidence, observed_at, updated_at, raw_json, house_norm, street_norm, postcode_norm, unit_norm
        ) VALUES (
          @state, @source_id, @source_record_id, @house_number, @street, @unit, @city, @region, @postcode, @lat, @lng, @geom_wkt,
          @building_id, @confidence, @observed_at, @updated_at, @raw_json, @house_norm, @street_norm, @postcode_norm, @unit_norm
        )
      `).run({
        state: "NY",
        source_id: "test_open_data",
        source_record_id: "u1",
        house_number: "10",
        street: "Main St",
        unit: "2A",
        city: "Queens",
        region: "NY",
        postcode: "11101",
        lat: 40.705,
        lng: -73.895,
        geom_wkt: "POINT (-73.895 40.705)",
        building_id: "b-res",
        confidence: 0.95,
        observed_at: "2026-03-01T00:00:00.000Z",
        updated_at: "2026-03-01T00:00:00.000Z",
        raw_json: "{}",
        house_norm: "10",
        street_norm: "main st",
        postcode_norm: "11101",
        unit_norm: "2a"
      });
    } finally {
      directDb.close();
    }

    const polygon = [
      [40.699, -73.911],
      [40.711, -73.911],
      [40.711, -73.889],
      [40.699, -73.889]
    ];
    const search = await postJson(`http://${api.host}:${api.port}/api/local-data/addresses/search`, {
      state: "NY",
      polygon,
      limit: 100,
      strictResidential: true
    });
    expect(search.response.status).toBe(200);
    expect(search.payload.ok).toBe(true);
    expect(search.payload.rows.length).toBeGreaterThan(0);
    expect(search.payload.rows.some(row => row.house_number === "10" && row.street === "Main St" && row.unit === "2A")).toBe(true);
    expect(search.payload.rows.some(row => row.house_number === "10" && row.street === "Main St" && String(row.unit || "").trim() === "")).toBe(false);
    expect(search.payload.unitCoverage && search.payload.unitCoverage.unitRowsReturned).toBeGreaterThanOrEqual(1);
  });

  it("supports alignment preview/apply with confirmation token", async () => {
    const tempDir = createTempDir("territory-local-data-align-");
    const packageRoot = path.join(tempDir, "packages");
    createFixturePackage(packageRoot);

    const staticServer = await startStaticServer(packageRoot);
    resourcesToCleanup.push(() => staticServer.close());

    const apiPort = await getFreePort();
    const apiDbPath = path.join(tempDir, "runtime-cache", "overture-cache.db");
    const manifestUrl = `http://${staticServer.host}:${staticServer.port}/manifest.json`;
    const api = startApiProcess({ apiPort, manifestUrl, dbPath: apiDbPath, unitSyncDisabled: true });
    resourcesToCleanup.push(async () => {
      api.child.kill("SIGTERM");
      await new Promise(resolve => api.child.once("exit", resolve));
    });

    const healthReady = await waitFor(async () => {
      try {
        const { payload } = await getJson(`http://${api.host}:${api.port}/health`);
        return payload && payload.ok === true;
      } catch (_) {
        return false;
      }
    }, 8000);
    expect(healthReady).toBe(true);

    const territories = [
      {
        id: "t-1",
        polygon: [
          [40.7000, -73.9000],
          [40.7100, -73.9000],
          [40.7100, -73.8900],
          [40.7000, -73.8900]
        ]
      },
      {
        id: "t-2",
        polygon: [
          [40.7000, -73.89002],
          [40.7100, -73.89002],
          [40.7100, -73.8800],
          [40.7000, -73.8800]
        ]
      }
    ];
    const preview = await postJson(`http://${api.host}:${api.port}/api/local-data/territories/align/preview`, {
      territories,
      options: {
        shared_edge_tolerance_m: 2.5,
        max_vertex_move_m: 6,
        max_area_drift_pct: 1.5
      }
    });
    expect(preview.response.status).toBe(200);
    expect(preview.payload.ok).toBe(true);
    expect(typeof preview.payload.confirmToken).toBe("string");

    const apply = await postJson(`http://${api.host}:${api.port}/api/local-data/territories/align/apply`, {
      territories,
      options: {
        shared_edge_tolerance_m: 2.5,
        max_vertex_move_m: 6,
        max_area_drift_pct: 1.5
      },
      confirmToken: preview.payload.confirmToken
    });
    expect(apply.response.status).toBe(200);
    expect(apply.payload.ok).toBe(true);
    expect(apply.payload.applied).toBe(true);
    expect(Array.isArray(apply.payload.patches)).toBe(true);
  });

  it("returns 404 for removed enrichment routes", async () => {
    const tempDir = createTempDir("territory-local-data-no-enrichment-");
    const packageRoot = path.join(tempDir, "packages");
    createFixturePackage(packageRoot);

    const staticServer = await startStaticServer(packageRoot);
    resourcesToCleanup.push(() => staticServer.close());

    const apiPort = await getFreePort();
    const apiDbPath = path.join(tempDir, "runtime-cache", "overture-cache.db");
    const manifestUrl = `http://${staticServer.host}:${staticServer.port}/manifest.json`;
    const api = startApiProcess({ apiPort, manifestUrl, dbPath: apiDbPath, unitSyncDisabled: true });
    resourcesToCleanup.push(async () => {
      api.child.kill("SIGTERM");
      await new Promise(resolve => api.child.once("exit", resolve));
    });

    const healthReady = await waitFor(async () => {
      try {
        const { payload } = await getJson(`http://${api.host}:${api.port}/health`);
        return payload && payload.ok === true;
      } catch (_) {
        return false;
      }
    }, 8000);
    expect(healthReady).toBe(true);

    const run = await postJson(`http://${api.host}:${api.port}/api/local-data/enrichment/run`, {
      state: "NY",
      territoryId: "t-1"
    });
    expect(run.response.status).toBe(404);
    expect(run.payload.ok).toBe(false);
  });
});

