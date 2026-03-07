#!/usr/bin/env node
const http = require("node:http");
const fs = require("node:fs");
const path = require("node:path");
const { chromium } = require("playwright");

const ROOT_DIR = path.resolve(__dirname, "..");
const ENTRY_FILE = "Territory Management.html";
const PORT = Number(process.env.PORT || 4181);
const HOST = "127.0.0.1";
const STORAGE_KEY_DB = "terr_final_db";

function resolveRequestPath(urlPath) {
  let decoded = "/";
  try {
    decoded = decodeURIComponent(String(urlPath || "/"));
  } catch (_) {
    decoded = "/";
  }
  const clean = decoded.split("?")[0].split("#")[0];
  const relativePath = (clean === "/" || !clean ? `/${ENTRY_FILE}` : clean).replace(/^\/+/, "");
  const absolutePath = path.resolve(ROOT_DIR, relativePath);
  if (!absolutePath.startsWith(path.resolve(ROOT_DIR))) return null;
  return absolutePath;
}

function createStaticServer() {
  return http.createServer((req, res) => {
    const absolutePath = resolveRequestPath(req.url);
    if (!absolutePath) {
      res.writeHead(400, { "Content-Type": "text/plain; charset=utf-8" });
      res.end("Bad request");
      return;
    }
    fs.stat(absolutePath, (statErr, stat) => {
      if (statErr || !stat || !stat.isFile()) {
        res.writeHead(404, { "Content-Type": "text/plain; charset=utf-8" });
        res.end("Not found");
        return;
      }
      res.writeHead(200, { "Cache-Control": "no-cache" });
      fs.createReadStream(absolutePath).pipe(res);
    });
  });
}

function assertCondition(condition, message) {
  if (!condition) throw new Error(message);
}

function buildSeededDb() {
  return [
    {
      id: "t-1",
      territoryNo: "1",
      locality: "Alpha",
      polygon: [[40.71, -74.0], [40.72, -74.02], [40.73, -74.01]],
      labelAnchor: { lat: 40.72, lng: -74.01 },
      addresses: [],
      city: "Queens",
      state: "NY",
      zip: "11101"
    }
  ];
}

async function main() {
  const server = createStaticServer();
  await new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen(PORT, HOST, resolve);
  });

  const browser = await chromium.launch({ headless: true });
  const context = await browser.newContext();
  const page = await context.newPage();
  const requests = [];
  page.on("request", req => requests.push(req.url()));
  await page.addInitScript(({ storageKey, dbJson }) => {
    try {
      localStorage.setItem(storageKey, dbJson);
    } catch (_) { }
  }, {
    storageKey: STORAGE_KEY_DB,
    dbJson: JSON.stringify(buildSeededDb())
  });
  await page.route("http://127.0.0.1:8787/**", async route => {
    const requestUrl = route.request().url();
    let payload = { ok: true };
    if (/\/api\/local-data\/state\/status\?/i.test(requestUrl)) {
      payload = {
        ok: true,
        state: "NY",
        release: "2026-01-21.0",
        phase: "ready",
        progress: { current: 1, total: 1, pct: 100 },
        datasetsInstalled: { addresses: true, buildings: true },
        datasetCounts: { addresses: 100, buildings: 100 },
        strictResidentialReady: true,
        error: "",
        jobId: "smoke-job"
      };
    } else if (/\/api\/local-data\/state\/ensure$/i.test(requestUrl) || /\/api\/local-data\/state\/upgrade$/i.test(requestUrl)) {
      payload = {
        ok: true,
        state: "NY",
        release: "2026-01-21.0",
        phase: "ready",
        jobId: "smoke-job",
        cached: true
      };
    } else if (/\/api\/local-data\/addresses\/search$/i.test(requestUrl)) {
      payload = {
        ok: true,
        count: 1,
        rows: [
          {
            id: "a1",
            house_number: "10",
            street: "Main St",
            unit: "",
            city: "Queens",
            region: "NY",
            postcode: "11101",
            country_code: "US",
            full_address: "10 Main St, Queens, NY 11101",
            lat: 40.75,
            lng: -73.95
          }
        ],
        release: "2026-01-21.0",
        source: "local-open-data-cache"
      };
    }
    await route.fulfill({
      status: 200,
      headers: { "Content-Type": "application/json; charset=utf-8", "Access-Control-Allow-Origin": "*" },
      body: JSON.stringify(payload)
    });
  });
  await page.route("https://overpass-api.de/api/interpreter*", async route => {
    await route.fulfill({
      status: 200,
      headers: { "Content-Type": "application/json; charset=utf-8", "Access-Control-Allow-Origin": "*" },
      body: JSON.stringify({
        elements: [
          {
            type: "node",
            id: 1,
            lat: 40.72,
            lon: -74.01,
            tags: {
              "addr:housenumber": "10",
              "addr:street": "Main St",
              "addr:postcode": "11101",
              "addr:city": "Queens",
              building: "house"
            }
          }
        ]
      })
    });
  });

  try {
    const localhostUrl = `http://${HOST}:${PORT}/${encodeURIComponent(ENTRY_FILE).replace(/%2F/g, "/")}`;
    await page.goto(localhostUrl, { waitUntil: "domcontentloaded" });
    await page.waitForTimeout(1800);

    const title = await page.title();
    const hasInstall = (await page.$("#btnInstallStateData")) !== null;
    const hasUpdate = (await page.$("#btnUpdateAddressDb")) !== null;
    const hasImport = (await page.$("#btnImportPackageBundle")) !== null;
    const hasAdvanced = (await page.$("#btnStateAdvanced")) !== null;
    const hasEnrichment = (await page.$("#btnDataEnrichment")) !== null;
    const hasOperations = (await page.$("#opsActiveJob")) !== null;
    const hasResultsPane = (await page.$(".results-pane")) !== null;
    const hasDatasetInfo = (await page.$("#datasetInfo")) !== null;
    const hasDatasetProgress = (await page.$("#datasetProgress")) !== null;
    const hasStateSelector = (await page.$("#stateSelector")) !== null;
    const mapViewHeadingCount = await page.locator("#menuTerritoryTools .tools-section").evaluateAll((nodes) => nodes.filter((node) => String(node.textContent || "").trim() === "Map View").length);
    const duplicateMapViewLabelCount = await page.locator("#menuTerritoryTools .tools-control-label").evaluateAll((nodes) => nodes.filter((node) => String(node.textContent || "").trim() === "Map View").length);
    const selectionCardText = await page.locator("#selectionCard").innerText();
    const refreshLabel = await page.locator("#btnFetch .btn-label").innerText();

    assertCondition(title === "Territory Management PRO", `Unexpected title: ${title}`);
    assertCondition(!hasInstall && !hasUpdate && !hasImport, "Manual install/update/import controls should not exist.");
    assertCondition(!hasAdvanced, "State advanced UI should not exist.");
    assertCondition(!hasEnrichment, "Data enrichment action should not exist.");
    assertCondition(!hasOperations, "Operations panel should not exist.");
    assertCondition(!hasResultsPane, "Right-side results pane should not exist.");
    assertCondition(!hasDatasetInfo && !hasDatasetProgress && !hasStateSelector, "Removed dataset/state chrome still exists.");
    assertCondition(!/Address Data:\s*NY/i.test(selectionCardText), "Legacy Address Data state chrome is still visible.");
    assertCondition(mapViewHeadingCount === 1, `Expected one Map View heading, found ${mapViewHeadingCount}.`);
    assertCondition(duplicateMapViewLabelCount === 0, `Unexpected duplicate Map View label count: ${duplicateMapViewLabelCount}.`);
    assertCondition(refreshLabel === "Refresh Selected", `Unexpected refresh button label: ${refreshLabel}`);

    await page.selectOption("#territorySelector", "t-1");
    await page.click("#btnFetch");
    await page.waitForFunction(() => /residential addresses inside this boundary/i.test(String(document.querySelector("#status")?.textContent || "")));

    const localhostDb = await page.evaluate((storageKey) => {
      try {
        return JSON.parse(localStorage.getItem(storageKey) || "[]");
      } catch (_) {
        return [];
      }
    }, STORAGE_KEY_DB);
    assertCondition(Array.isArray(localhostDb) && localhostDb[0] && Array.isArray(localhostDb[0].addresses) && localhostDb[0].addresses.length === 1, "Expected one refreshed address in localhost mode.");
    assertCondition(/10 Main St/i.test(String(localhostDb[0].addresses[0] && localhostDb[0].addresses[0].full || "")), "Expected normalized Overpass address in localhost mode.");
    assertCondition(requests.some(url => /overpass-api\.de\/api\/interpreter/i.test(url)), "Expected Overpass refresh request was not made.");
    assertCondition(!requests.some(url => /api\/local-data\/addresses\/search/i.test(url)), "Unexpected legacy local-data address search request detected.");

    const fileUrl = `file:///${path.join(ROOT_DIR, ENTRY_FILE).replace(/\\/g, "/")}`;
    requests.length = 0;
    await page.goto(fileUrl, { waitUntil: "domcontentloaded" });
    await page.waitForTimeout(1800);
    await page.selectOption("#territorySelector", "t-1");
    await page.click("#btnFetch");
    await page.waitForFunction(() => /residential addresses inside this boundary/i.test(String(document.querySelector("#status")?.textContent || "")));
    const fileDb = await page.evaluate((storageKey) => {
      try {
        return JSON.parse(localStorage.getItem(storageKey) || "[]");
      } catch (_) {
        return [];
      }
    }, STORAGE_KEY_DB);
    assertCondition(
      Array.isArray(fileDb) && fileDb[0] && Array.isArray(fileDb[0].addresses) && fileDb[0].addresses.length === 1,
      "Expected one refreshed address in file mode."
    );
    assertCondition(requests.some(url => /overpass-api\.de\/api\/interpreter/i.test(url)), "Expected Overpass refresh request in file mode was not made.");
    assertCondition(!requests.some(url => /api\/local-data\/addresses\/search/i.test(url)), "Unexpected legacy local-data address search request detected in file mode.");

    console.log("[playwright-local-smoke] PASS");
  } finally {
    await browser.close();
    await new Promise(resolve => server.close(resolve));
  }
}

main().catch(err => {
  console.error(`[playwright-local-smoke] FAIL: ${String((err && err.message) || err || "unknown")}`);
  process.exit(1);
});


