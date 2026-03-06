#!/usr/bin/env node
const http = require("node:http");
const fs = require("node:fs");
const path = require("node:path");
const { chromium } = require("playwright");

const ROOT_DIR = path.resolve(__dirname, "..");
const ENTRY_FILE = "Territory Management.html";
const PORT = Number(process.env.PORT || 4181);
const HOST = "127.0.0.1";

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

  try {
    const localhostUrl = `http://${HOST}:${PORT}/${encodeURIComponent(ENTRY_FILE).replace(/%2F/g, "/")}`;
    await page.goto(localhostUrl, { waitUntil: "domcontentloaded" });
    await page.waitForTimeout(2500);

    const title = await page.title();
    const hasInstall = (await page.$("#btnInstallStateData")) !== null;
    const hasUpdate = (await page.$("#btnUpdateAddressDb")) !== null;
    const hasImport = (await page.$("#btnImportPackageBundle")) !== null;
    const hasAdvanced = (await page.$("#btnStateAdvanced")) !== null;
    const hasEnrichment = (await page.$("#btnDataEnrichment")) !== null;
    const hasOperations = (await page.$("#opsActiveJob")) !== null;
    const hasResultsPane = (await page.$(".results-pane")) !== null;

    assertCondition(title === "Territory Management PRO", `Unexpected title: ${title}`);
    assertCondition(!hasInstall && !hasUpdate && !hasImport, "Manual install/update/import controls should not exist.");
    assertCondition(!hasAdvanced, "State advanced UI should not exist.");
    assertCondition(!hasEnrichment, "Data enrichment action should not exist.");
    assertCondition(!hasOperations, "Operations panel should not exist.");
    assertCondition(!hasResultsPane, "Right-side results pane should not exist.");
    assertCondition(requests.some(url => /api\/local-data\/state\/status/i.test(url)), "Expected local-data state status request was not made.");
    assertCondition(!requests.some(url => /overpass/i.test(url)), "Unexpected Overpass request detected.");

    const fileUrl = `file:///${path.join(ROOT_DIR, ENTRY_FILE).replace(/\\/g, "/")}`;
    await page.goto(fileUrl, { waitUntil: "domcontentloaded" });
    await page.waitForTimeout(1200);
    const infoText = await page.locator("#datasetInfo").innerText();
    assertCondition(
      /\bready\b|state package not yet published|localhost|start-territory-app\.cmd|npm run start:local/i.test(infoText),
      `Expected concise ready/state guidance in file mode, got: ${infoText}`
    );

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


