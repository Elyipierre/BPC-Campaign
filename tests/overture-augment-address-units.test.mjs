import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import http from "node:http";
import net from "node:net";
import { spawn, spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";

const THIS_DIR = path.dirname(fileURLToPath(import.meta.url));
const ROOT_DIR = path.resolve(THIS_DIR, "..");
const SCRIPT_PATH = path.resolve(ROOT_DIR, "scripts/overture-augment-address-units.py");
const PYTHON_BIN = process.env.PYTHON || "python";

function createTempDir(prefix) {
  return fs.mkdtempSync(path.join(os.tmpdir(), prefix));
}

function writeFile(filePath, text) {
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  fs.writeFileSync(filePath, text, "utf8");
}

function runScript(args) {
  return spawnSync(PYTHON_BIN, [SCRIPT_PATH, ...args], {
    cwd: ROOT_DIR,
    encoding: "utf8"
  });
}

async function runScriptAsync(args) {
  return await new Promise((resolve) => {
    const child = spawn(PYTHON_BIN, [SCRIPT_PATH, ...args], {
      cwd: ROOT_DIR,
      stdio: ["ignore", "pipe", "pipe"]
    });
    let stdout = "";
    let stderr = "";
    child.stdout.on("data", chunk => {
      stdout += String(chunk || "");
    });
    child.stderr.on("data", chunk => {
      stderr += String(chunk || "");
    });
    child.on("close", status => {
      resolve({ status, stdout, stderr });
    });
  });
}

async function getFreePort() {
  return await new Promise((resolve, reject) => {
    const server = net.createServer();
    server.on("error", reject);
    server.listen(0, "127.0.0.1", () => {
      const address = server.address();
      server.close(() => resolve(address.port));
    });
  });
}

async function startFakeSocrataServer(datasetId, rows) {
  const host = "127.0.0.1";
  const port = await getFreePort();
  const server = http.createServer((req, res) => {
    const url = new URL(req.url || "/", `http://${host}:${port}`);
    if (url.pathname !== `/resource/${datasetId}.json`) {
      res.writeHead(404, { "Content-Type": "application/json; charset=utf-8" });
      res.end(JSON.stringify({ error: "not_found" }));
      return;
    }
    const limit = Math.max(1, Number(url.searchParams.get("$limit")) || 1000);
    const offset = Math.max(0, Number(url.searchParams.get("$offset")) || 0);
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

function readCsvRows(filePath) {
  const raw = fs.readFileSync(filePath, "utf8").trim();
  if (!raw) return [];
  const [headerLine, ...lines] = raw.split(/\r?\n/);
  const headers = headerLine.split(",");
  return lines
    .filter(Boolean)
    .map((line) => {
      const values = [];
      let current = "";
      let inQuote = false;
      for (let i = 0; i < line.length; i += 1) {
        const ch = line[i];
        if (ch === "\"") {
          if (inQuote && line[i + 1] === "\"") {
            current += "\"";
            i += 1;
          } else {
            inQuote = !inQuote;
          }
        } else if (ch === "," && !inQuote) {
          values.push(current);
          current = "";
        } else {
          current += ch;
        }
      }
      values.push(current);
      const row = {};
      headers.forEach((header, idx) => {
        row[header] = values[idx] ?? "";
      });
      return row;
    });
}

describe("overture-augment-address-units.py", () => {
  it("merges supplemental unit rows and skips duplicates/missing-unit rows by default", () => {
    const tempDir = createTempDir("territory-augment-units-");
    try {
      const baseCsv = path.join(tempDir, "addresses-base.csv");
      const suppCsv = path.join(tempDir, "units-supp.csv");
      const outCsv = path.join(tempDir, "addresses-merged.csv");

      writeFile(baseCsv, [
        "id,source_dataset,house_number,street,unit,city,region,postcode,country_code,full_address,geom_wkt,raw_json",
        "base-1,overture,20-27,SEAGIRT BLVD,,Queens,NY,11691,US,20-27 SEAGIRT BLVD 11691,\"POINT (-73.7555692 40.5943657)\",{}",
        "base-2,overture,711,SEAGIRT AVE,A,Queens,NY,11691,US,711 SEAGIRT AVE Unit A 11691,\"POINT (-73.7424616 40.5957817)\",{}"
      ].join("\n"));

      writeFile(suppCsv, [
        "number,street,apartment,zip,city,state,latitude,longitude,source",
        "711,SEAGIRT AVE,A,11691,Queens,NY,40.5957817,-73.7424616,nyc-units",
        "711,SEAGIRT AVE,B,11691,Queens,NY,40.5956402,-73.7424306,nyc-units",
        "20-27,SEAGIRT BLVD,,11691,Queens,NY,40.5943657,-73.7555692,nyc-units",
        "99,TEST ST,C,11691,Queens,NY,,,nyc-units",
        "711,SEAGIRT AVE,B,11691,Queens,NY,40.5956402,-73.7424306,nyc-units",
        "711,SEAGIRT AVE,C,11691,Queens,NY,40.5955787,-73.7420993,nyc-units"
      ].join("\n"));

      const result = runScript([
        "--base-csv", baseCsv,
        "--supplement-csv", suppCsv,
        "--output-csv", outCsv,
        "--state", "NY",
        "--overwrite"
      ]);

      expect(result.status).toBe(0);
      const rows = readCsvRows(outCsv);
      expect(rows).toHaveLength(4);

      const byId = new Map(rows.map((row) => [row.id, row]));
      expect(byId.has("base-1")).toBe(true);
      expect(byId.has("base-2")).toBe(true);

      const added = rows.filter((row) => String(row.id || "").startsWith("supp-"));
      expect(added).toHaveLength(2);
      const units = added.map((row) => row.unit).sort();
      expect(units).toEqual(["B", "C"]);

      const combinedLog = `${result.stdout}\n${result.stderr}`;
      expect(combinedLog).toMatch(/added_rows=2/);
      expect(combinedLog).toMatch(/skipped_missing_unit=1/);
      expect(combinedLog).toMatch(/skipped_duplicate_unit=1/);
      expect(combinedLog).toMatch(/skipped_duplicate_supp=1/);
      expect(combinedLog).toMatch(/skipped_missing_point=1/);
    } finally {
      fs.rmSync(tempDir, { recursive: true, force: true });
    }
  });

  it("supports allow-missing-unit mode for supplemental rows", () => {
    const tempDir = createTempDir("territory-augment-units-");
    try {
      const baseCsv = path.join(tempDir, "addresses-base.csv");
      const suppCsv = path.join(tempDir, "units-supp.csv");
      const outCsv = path.join(tempDir, "addresses-merged.csv");

      writeFile(baseCsv, [
        "id,source_dataset,house_number,street,unit,city,region,postcode,country_code,full_address,geom_wkt,raw_json",
        "base-1,overture,10,MAIN ST,,Queens,NY,11101,US,10 MAIN ST 11101,\"POINT (-73.9000000 40.7000000)\",{}"
      ].join("\n"));

      writeFile(suppCsv, [
        "number,street,zip,city,state,latitude,longitude",
        "12,MAIN ST,11101,Queens,NY,40.7005000,-73.9005000"
      ].join("\n"));

      const result = runScript([
        "--base-csv", baseCsv,
        "--supplement-csv", suppCsv,
        "--output-csv", outCsv,
        "--state", "NY",
        "--allow-missing-unit",
        "--overwrite"
      ]);

      expect(result.status).toBe(0);
      const rows = readCsvRows(outCsv);
      expect(rows).toHaveLength(2);

      const added = rows.find((row) => String(row.id || "").startsWith("supp-"));
      expect(added).toBeTruthy();
      expect(added.unit).toBe("");
      expect(added.geom_wkt).toMatch(/^POINT\s+\(/);
    } finally {
      fs.rmSync(tempDir, { recursive: true, force: true });
    }
  });

  it("downloads NYC Socrata rows and merges them without manual supplement files", async () => {
    const tempDir = createTempDir("territory-augment-units-");
    const datasetId = "unit-test";
    const socrataRows = [
      {
        house_number: "20-27",
        street_name: "SEAGIRT BLVD",
        apartment: "2A",
        zipcode: "11691",
        borough: "Queens",
        latitude: "40.5943657",
        longitude: "-73.7555692",
        unique_key: "u1"
      },
      {
        house_number: "20-27",
        street_name: "SEAGIRT BLVD",
        apartment: "",
        zipcode: "11691",
        borough: "Queens",
        latitude: "40.5943657",
        longitude: "-73.7555692",
        unique_key: "u2"
      },
      {
        house_number: "20-27",
        street_name: "SEAGIRT BLVD",
        apartment: "2B",
        zipcode: "11691",
        borough: "Queens",
        the_geom: { type: "Point", coordinates: "-73.7555692 40.5943657" },
        unique_key: "u3"
      }
    ];
    const server = await startFakeSocrataServer(datasetId, socrataRows);
    try {
      const baseCsv = path.join(tempDir, "addresses-base.csv");
      const outCsv = path.join(tempDir, "addresses-merged.csv");

      writeFile(baseCsv, [
        "id,source_dataset,house_number,street,unit,city,region,postcode,country_code,full_address,geom_wkt,raw_json",
        "base-1,overture,20-27,SEAGIRT BLVD,,Queens,NY,11691,US,20-27 SEAGIRT BLVD 11691,\"POINT (-73.7555692 40.5943657)\",{}"
      ].join("\n"));

      const result = await runScriptAsync([
        "--base-csv", baseCsv,
        "--output-csv", outCsv,
        "--download-nyc-socrata-dataset", datasetId,
        "--nyc-socrata-domain", `${server.host}:${server.port}`,
        "--nyc-socrata-scheme", "http",
        "--nyc-socrata-batch-size", "2",
        "--state", "NY",
        "--overwrite"
      ]);

      expect(result.status).toBe(0);
      const rows = readCsvRows(outCsv);
      expect(rows).toHaveLength(3);
      const added = rows.filter((row) => row.id !== "base-1");
      expect(added).toHaveLength(2);
      expect(added.map((row) => row.unit).sort()).toEqual(["2A", "2B"]);
      expect(added.every((row) => row.source_dataset === `nyc-open-data:${datasetId}`)).toBe(true);

      const combinedLog = `${result.stdout}\n${result.stderr}`;
      expect(combinedLog).toMatch(/source_id=unit-test/);
      expect(combinedLog).toMatch(/dataset_id=unit-test/);
      expect(combinedLog).toMatch(/rows_downloaded=3/);
      expect(combinedLog).toMatch(/rows_written=2/);
    } finally {
      await server.close();
      fs.rmSync(tempDir, { recursive: true, force: true });
    }
  });

  it("supports source-profile verify mode for multi-source diagnostics", async () => {
    const tempDir = createTempDir("territory-augment-units-");
    const datasetId = "profile-test";
    const socrataRows = [
      {
        house_number: "20-27",
        street_name: "SEAGIRT BLVD",
        apartment: "5C",
        zip_code: "11691",
        borough: "Queens",
        latitude: "40.5943657",
        longitude: "-73.7555692",
        record_id: "p1",
        updated_at: "2026-03-01T00:00:00.000Z"
      }
    ];
    const server = await startFakeSocrataServer(datasetId, socrataRows);
    try {
      const profilePath = path.join(tempDir, "ny-open-unit-sources.json");
      writeFile(profilePath, JSON.stringify({
        schemaVersion: 1,
        profileId: "ny_open_units",
        state: "NY",
        domain: `${server.host}:${server.port}`,
        scheme: "http",
        sources: [
          {
            enabled: true,
            state: "NY",
            sourceId: "profile_source",
            datasetId,
            confidenceBase: 0.85,
            fieldMap: {
              houseNumber: "house_number",
              street: "street_name",
              unit: "apartment",
              postcode: "zip_code",
              city: "borough",
              lat: "latitude",
              lng: "longitude",
              sourceRecordId: "record_id",
              updatedAt: "updated_at"
            }
          }
        ]
      }, null, 2));

      const result = await runScriptAsync([
        "--source-profile", "ny_open_units",
        "--source-profile-file", profilePath,
        "--verify-only"
      ]);
      expect(result.status).toBe(0);
      const combinedLog = `${result.stdout}\n${result.stderr}`;
      expect(combinedLog).toMatch(/source_profile=ny_open_units/);
      expect(combinedLog).toMatch(/sources_checked=1/);
      expect(combinedLog).toMatch(/source_id=profile_source/);
      expect(combinedLog).toMatch(/rows_downloaded=1/);
      expect(combinedLog).toMatch(/rows_written=1/);
    } finally {
      await server.close();
      fs.rmSync(tempDir, { recursive: true, force: true });
    }
  });
});
