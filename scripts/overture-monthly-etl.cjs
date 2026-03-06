"use strict";

const fs = require("node:fs");
const path = require("node:path");
const { spawnSync } = require("node:child_process");

function parseArgs(argv) {
  const options = {
    release: "",
    sourceUri: "",
    notes: "",
    addressesCsv: "",
    buildingsCsv: "",
    applySchema: false,
    schemaFile: "scripts/overture-postgis-schema.sql",
    databaseUrl: process.env.DATABASE_URL || ""
  };

  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    if (arg === "--apply-schema") options.applySchema = true;
    else if (arg === "--release" && argv[i + 1]) options.release = String(argv[++i]).trim();
    else if (arg === "--source-uri" && argv[i + 1]) options.sourceUri = String(argv[++i]).trim();
    else if (arg === "--notes" && argv[i + 1]) options.notes = String(argv[++i]).trim();
    else if (arg === "--addresses-csv" && argv[i + 1]) options.addressesCsv = String(argv[++i]).trim();
    else if (arg === "--buildings-csv" && argv[i + 1]) options.buildingsCsv = String(argv[++i]).trim();
    else if (arg === "--schema-file" && argv[i + 1]) options.schemaFile = String(argv[++i]).trim();
    else if (arg === "--database-url" && argv[i + 1]) options.databaseUrl = String(argv[++i]).trim();
  }

  return options;
}

function printUsage() {
  console.log("Usage:");
  console.log("  node scripts/overture-monthly-etl.cjs --release <release_id> --source-uri <uri> [options]");
  console.log("");
  console.log("Options:");
  console.log("  --apply-schema                  Apply scripts/overture-postgis-schema.sql first");
  console.log("  --schema-file <path>            Schema SQL file path (default scripts/overture-postgis-schema.sql)");
  console.log("  --addresses-csv <path>          CSV file for addresses import");
  console.log("  --buildings-csv <path>          CSV file for buildings import");
  console.log("  --notes <text>                  Optional notes saved on release row");
  console.log("  --database-url <url>            PostgreSQL connection string (or use DATABASE_URL)");
  console.log("");
  console.log("Expected addresses CSV headers:");
  console.log("  id,source_dataset,house_number,street,unit,city,region,postcode,country_code,full_address,geom_wkt,raw_json");
  console.log("Expected buildings CSV headers:");
  console.log("  id,source_dataset,building_class,levels,name,geom_wkt,raw_json");
}

function quoteSql(value) {
  return `'${String(value ?? "").replace(/'/g, "''")}'`;
}

function ensureFile(filePath, label) {
  if (!filePath) return "";
  const resolved = path.resolve(process.cwd(), filePath);
  if (!fs.existsSync(resolved)) {
    throw new Error(`${label} not found: ${resolved}`);
  }
  return resolved;
}

function runPsql(databaseUrl, args, options = {}) {
  const result = spawnSync("psql", args, {
    encoding: "utf8",
    stdio: options.stdio || "pipe",
    env: {
      ...process.env,
      PGPASSWORD: process.env.PGPASSWORD || ""
    }
  });
  if (result.status !== 0) {
    const stderr = String(result.stderr || "").trim();
    const stdout = String(result.stdout || "").trim();
    throw new Error(stderr || stdout || `psql exited with status ${result.status}`);
  }
  return result;
}

function runSql(databaseUrl, sql) {
  runPsql(databaseUrl, ["-X", "-v", "ON_ERROR_STOP=1", databaseUrl, "-c", sql], { stdio: "inherit" });
}

function runSqlQuery(databaseUrl, sql) {
  const result = runPsql(databaseUrl, ["-X", "-qAt", databaseUrl, "-c", sql], { stdio: "pipe" });
  return String(result.stdout || "").trim();
}

function applySchema(databaseUrl, schemaFile) {
  runPsql(databaseUrl, ["-X", "-v", "ON_ERROR_STOP=1", databaseUrl, "-f", schemaFile], { stdio: "inherit" });
}

function copyCsv(databaseUrl, tableName, absolutePath, columns) {
  const escapedPath = absolutePath.replace(/\\/g, "/").replace(/'/g, "''");
  const copySql = `\\copy ${tableName} (${columns.join(", ")}) FROM '${escapedPath}' WITH (FORMAT csv, HEADER true)`;
  runPsql(databaseUrl, ["-X", "-v", "ON_ERROR_STOP=1", databaseUrl, "-c", copySql], { stdio: "inherit" });
}

function getReleaseCount(databaseUrl, tableName, releaseId) {
  const out = runSqlQuery(
    databaseUrl,
    `SELECT COUNT(*) FROM ${tableName} WHERE release_id = ${quoteSql(releaseId)};`
  );
  const value = Number(out);
  return Number.isFinite(value) ? value : 0;
}

function insertAudit(databaseUrl, releaseId, theme, rowCount, status, details = "{}") {
  runSql(
    databaseUrl,
    `INSERT INTO territory.overture_import_audit (release_id, theme, row_count, status, completed_at, details)
     VALUES (${quoteSql(releaseId)}, ${quoteSql(theme)}, ${Number(rowCount) || 0}, ${quoteSql(status)}, NOW(), ${quoteSql(details)}::jsonb);`
  );
}

function importAddresses(databaseUrl, releaseId, addressesCsv) {
  console.log(`\n[etl] importing addresses from ${addressesCsv}`);
  const before = getReleaseCount(databaseUrl, "territory.overture_address", releaseId);
  runSql(
    databaseUrl,
    `DROP TABLE IF EXISTS territory._overture_address_stage;
     CREATE UNLOGGED TABLE territory._overture_address_stage (
       id TEXT,
       source_dataset TEXT,
       house_number TEXT,
       street TEXT,
       unit TEXT,
       city TEXT,
       region TEXT,
       postcode TEXT,
       country_code TEXT,
       full_address TEXT,
       geom_wkt TEXT,
       raw_json TEXT
     );`
  );
  copyCsv(databaseUrl, "territory._overture_address_stage", addressesCsv, [
    "id", "source_dataset", "house_number", "street", "unit", "city", "region",
    "postcode", "country_code", "full_address", "geom_wkt", "raw_json"
  ]);
  runSql(
    databaseUrl,
    `INSERT INTO territory.overture_address (
       id, release_id, source_dataset, house_number, street, unit, city, region,
       postcode, country_code, full_address, geom, raw
     )
     SELECT
       s.id,
       ${quoteSql(releaseId)},
       NULLIF(s.source_dataset, ''),
       NULLIF(s.house_number, ''),
       NULLIF(s.street, ''),
       NULLIF(s.unit, ''),
       NULLIF(s.city, ''),
       NULLIF(s.region, ''),
       NULLIF(s.postcode, ''),
       NULLIF(s.country_code, ''),
       NULLIF(s.full_address, ''),
       ST_SetSRID(ST_GeomFromText(s.geom_wkt), 4326),
       COALESCE(NULLIF(s.raw_json, ''), '{}')::jsonb
     FROM territory._overture_address_stage s
     WHERE NULLIF(s.id, '') IS NOT NULL
       AND NULLIF(s.geom_wkt, '') IS NOT NULL
     ON CONFLICT (id, release_id) DO UPDATE SET
       source_dataset = EXCLUDED.source_dataset,
       house_number = EXCLUDED.house_number,
       street = EXCLUDED.street,
       unit = EXCLUDED.unit,
       city = EXCLUDED.city,
       region = EXCLUDED.region,
       postcode = EXCLUDED.postcode,
       country_code = EXCLUDED.country_code,
       full_address = EXCLUDED.full_address,
       geom = EXCLUDED.geom,
       raw = EXCLUDED.raw,
       imported_at = NOW();
     DROP TABLE IF EXISTS territory._overture_address_stage;`
  );
  const after = getReleaseCount(databaseUrl, "territory.overture_address", releaseId);
  const loaded = Math.max(0, after - before);
  insertAudit(databaseUrl, releaseId, "addresses", loaded, "completed", JSON.stringify({ file: addressesCsv }));
  console.log(`[etl] addresses loaded: ${loaded}`);
}

function importBuildings(databaseUrl, releaseId, buildingsCsv) {
  console.log(`\n[etl] importing buildings from ${buildingsCsv}`);
  const before = getReleaseCount(databaseUrl, "territory.overture_building", releaseId);
  runSql(
    databaseUrl,
    `DROP TABLE IF EXISTS territory._overture_building_stage;
     CREATE UNLOGGED TABLE territory._overture_building_stage (
       id TEXT,
       source_dataset TEXT,
       building_class TEXT,
       levels TEXT,
       name TEXT,
       geom_wkt TEXT,
       raw_json TEXT
     );`
  );
  copyCsv(databaseUrl, "territory._overture_building_stage", buildingsCsv, [
    "id", "source_dataset", "building_class", "levels", "name", "geom_wkt", "raw_json"
  ]);
  runSql(
    databaseUrl,
    `INSERT INTO territory.overture_building (
       id, release_id, source_dataset, building_class, levels, name, geom, raw
     )
     SELECT
       s.id,
       ${quoteSql(releaseId)},
       NULLIF(s.source_dataset, ''),
       NULLIF(s.building_class, ''),
       NULLIF(s.levels, '')::numeric,
       NULLIF(s.name, ''),
       ST_SetSRID(ST_GeomFromText(s.geom_wkt), 4326),
       COALESCE(NULLIF(s.raw_json, ''), '{}')::jsonb
     FROM territory._overture_building_stage s
     WHERE NULLIF(s.id, '') IS NOT NULL
       AND NULLIF(s.geom_wkt, '') IS NOT NULL
     ON CONFLICT (id, release_id) DO UPDATE SET
       source_dataset = EXCLUDED.source_dataset,
       building_class = EXCLUDED.building_class,
       levels = EXCLUDED.levels,
       name = EXCLUDED.name,
       geom = EXCLUDED.geom,
       raw = EXCLUDED.raw,
       imported_at = NOW();
     DROP TABLE IF EXISTS territory._overture_building_stage;`
  );
  const after = getReleaseCount(databaseUrl, "territory.overture_building", releaseId);
  const loaded = Math.max(0, after - before);
  insertAudit(databaseUrl, releaseId, "buildings", loaded, "completed", JSON.stringify({ file: buildingsCsv }));
  console.log(`[etl] buildings loaded: ${loaded}`);
}

function main() {
  const options = parseArgs(process.argv.slice(2));
  if (process.argv.includes("--help")) {
    printUsage();
    return;
  }

  if (!options.databaseUrl) throw new Error("Missing database URL. Set DATABASE_URL or pass --database-url.");
  if (!options.release) throw new Error("Missing --release <release_id>.");
  if (!options.sourceUri) throw new Error("Missing --source-uri <uri>.");

  const schemaFile = ensureFile(options.schemaFile, "Schema file");
  const addressesCsv = ensureFile(options.addressesCsv, "Addresses CSV");
  const buildingsCsv = ensureFile(options.buildingsCsv, "Buildings CSV");

  if (!addressesCsv && !buildingsCsv) {
    throw new Error("Nothing to import. Provide --addresses-csv and/or --buildings-csv.");
  }

  if (options.applySchema) {
    console.log(`[etl] applying schema: ${schemaFile}`);
    applySchema(options.databaseUrl, schemaFile);
  }

  runSql(
    options.databaseUrl,
    `INSERT INTO territory.overture_release (release_id, source_uri, notes)
     VALUES (${quoteSql(options.release)}, ${quoteSql(options.sourceUri)}, ${quoteSql(options.notes)})
     ON CONFLICT (release_id) DO UPDATE SET
       source_uri = EXCLUDED.source_uri,
       notes = CASE
         WHEN EXCLUDED.notes IS NULL OR EXCLUDED.notes = '' THEN territory.overture_release.notes
         ELSE EXCLUDED.notes
       END;`
  );

  if (addressesCsv) importAddresses(options.databaseUrl, options.release, addressesCsv);
  if (buildingsCsv) importBuildings(options.databaseUrl, options.release, buildingsCsv);

  console.log(`\n[etl] complete for release ${options.release}`);
}

main().catch(error => {
  console.error(error && error.message ? error.message : error);
  process.exitCode = 1;
});

