import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import { describe, it, expect } from "vitest";

const THIS_DIR = path.dirname(fileURLToPath(import.meta.url));
const ROOT_DIR = path.resolve(THIS_DIR, "..");
const SCRIPT_PATH = path.resolve(ROOT_DIR, "scripts/overture-build-state-package.py");
const PYTHON_BIN = process.env.PYTHON || "python";

function createTempDir(prefix) {
  return fs.mkdtempSync(path.join(os.tmpdir(), prefix));
}

function writeFile(filePath, text) {
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  fs.writeFileSync(filePath, text, "utf8");
}

function runBuilder(args, cwd) {
  return spawnSync(PYTHON_BIN, [SCRIPT_PATH, ...args], {
    cwd,
    encoding: "utf8"
  });
}

function countChunkRows(filePath) {
  const raw = fs.readFileSync(filePath, "utf8");
  if (!raw.trim()) return 0;
  return raw.split(/\r?\n/).filter(Boolean).length;
}

describe("overture-build-state-package.py", () => {
  it("emits schema v2 manifest with addresses and buildings datasets", () => {
    const tempDir = createTempDir("territory-package-builder-");
    try {
      const addressesCsv = path.join(tempDir, "addresses.csv");
      const buildingsCsv = path.join(tempDir, "buildings.csv");
      const outputRoot = path.join(tempDir, "packages");

      writeFile(addressesCsv, [
        "id,source_dataset,house_number,street,unit,city,region,postcode,country_code,full_address,geom_wkt,raw_json",
        "a1,overture,10,Main St,,Queens,NY,11101,US,10 Main St Queens NY 11101,\"POINT (-73.9500 40.7500)\",{}",
        "a2,overture,20,Pine Ave,2B,Queens,NY,11102,US,20 Pine Ave Apt 2B Queens NY 11102,\"POINT (-73.9400 40.7600)\",{}",
        "a3,overture,30,Elm Rd,,Queens,NY,11103,US,30 Elm Rd Queens NY 11103,\"POINT (-73.9300 40.7700)\",{}",
        "bad,overture,40,Broken St,,Queens,NY,11104,US,40 Broken St Queens NY 11104,\"LINESTRING (-73.9 40.7, -73.8 40.8)\",{}"
      ].join("\n"));
      writeFile(buildingsCsv, [
        "id,source_dataset,building_class,levels,name,geom_wkt,raw_json",
        "b1,overture,apartments,3,Alpha,\"POLYGON ((-73.9500 40.7500, -73.9500 40.7510, -73.9490 40.7510, -73.9490 40.7500, -73.9500 40.7500))\",{}",
        "b2,overture,residential,2,Beta,\"MULTIPOLYGON (((-73.9600 40.7600, -73.9600 40.7610, -73.9590 40.7610, -73.9590 40.7600, -73.9600 40.7600)))\",{}",
        "b3,overture,commercial,8,Invalid,\"POINT (-73.9400 40.7400)\",{}"
      ].join("\n"));

      const result = runBuilder([
        "--addresses-csv", addressesCsv,
        "--buildings-csv", buildingsCsv,
        "--state", "NY",
        "--release", "2026-01-21.0",
        "--output-root", outputRoot,
        "--chunk-size", "2",
        "--overwrite"
      ], ROOT_DIR);

      expect(result.status).toBe(0);
      const manifestPath = path.join(outputRoot, "manifest.json");
      const manifest = JSON.parse(fs.readFileSync(manifestPath, "utf8"));

      expect(manifest.schema_version).toBe(2);
      expect(manifest.states.NY).toBeTruthy();
      const entry = manifest.states.NY;
      expect(entry.datasets.addresses.count).toBe(3);
      expect(entry.datasets.buildings.count).toBe(2);
      expect(entry.datasets.addresses.chunks).toHaveLength(2);
      expect(entry.datasets.buildings.chunks).toHaveLength(1);
      expect(entry.chunks).toEqual(entry.datasets.addresses.chunks);

      const addressChunkCounts = entry.datasets.addresses.chunks.map((chunk) => {
        const chunkPath = path.join(outputRoot, chunk.path);
        expect(fs.existsSync(chunkPath)).toBe(true);
        return countChunkRows(chunkPath);
      });
      const buildingChunkCounts = entry.datasets.buildings.chunks.map((chunk) => {
        const chunkPath = path.join(outputRoot, chunk.path);
        expect(fs.existsSync(chunkPath)).toBe(true);
        return countChunkRows(chunkPath);
      });

      expect(addressChunkCounts.reduce((a, b) => a + b, 0)).toBe(3);
      expect(buildingChunkCounts.reduce((a, b) => a + b, 0)).toBe(2);
      expect(addressChunkCounts).toEqual([2, 1]);
      expect(buildingChunkCounts).toEqual([2]);
    } finally {
      fs.rmSync(tempDir, { recursive: true, force: true });
    }
  });

  it("fails by default when buildings csv is missing", () => {
    const tempDir = createTempDir("territory-package-builder-");
    try {
      const addressesCsv = path.join(tempDir, "addresses.csv");
      const outputRoot = path.join(tempDir, "packages");
      writeFile(addressesCsv, [
        "id,source_dataset,house_number,street,unit,city,region,postcode,country_code,full_address,geom_wkt,raw_json",
        "a1,overture,10,Main St,,Queens,NY,11101,US,10 Main St Queens NY 11101,\"POINT (-73.9500 40.7500)\",{}"
      ].join("\n"));

      const result = runBuilder([
        "--addresses-csv", addressesCsv,
        "--state", "NY",
        "--release", "2026-01-21.0",
        "--output-root", outputRoot,
        "--overwrite"
      ], ROOT_DIR);

      expect(result.status).toBe(1);
      const combinedLog = `${result.stdout}\n${result.stderr}`;
      expect(combinedLog).toMatch(/Missing buildings CSV/i);
    } finally {
      fs.rmSync(tempDir, { recursive: true, force: true });
    }
  });

  it("supports explicit addresses-only package generation with strict mode unavailable", () => {
    const tempDir = createTempDir("territory-package-builder-");
    try {
      const addressesCsv = path.join(tempDir, "addresses.csv");
      const outputRoot = path.join(tempDir, "packages");
      writeFile(addressesCsv, [
        "id,source_dataset,house_number,street,unit,city,region,postcode,country_code,full_address,geom_wkt,raw_json",
        "a1,overture,10,Main St,,Queens,NY,11101,US,10 Main St Queens NY 11101,\"POINT (-73.9500 40.7500)\",{}"
      ].join("\n"));

      const result = runBuilder([
        "--addresses-csv", addressesCsv,
        "--state", "NY",
        "--release", "2026-01-21.0",
        "--output-root", outputRoot,
        "--allow-addresses-only",
        "--overwrite"
      ], ROOT_DIR);

      expect(result.status).toBe(0);
      const manifest = JSON.parse(fs.readFileSync(path.join(outputRoot, "manifest.json"), "utf8"));
      expect(manifest.schema_version).toBe(2);
      expect(manifest.states.NY.datasets.addresses.count).toBe(1);
      expect(manifest.states.NY.datasets.buildings.count).toBe(0);
      expect(manifest.states.NY.datasets.buildings.chunks).toEqual([]);
    } finally {
      fs.rmSync(tempDir, { recursive: true, force: true });
    }
  });
});
