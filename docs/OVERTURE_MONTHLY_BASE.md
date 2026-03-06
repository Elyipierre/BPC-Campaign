## Overture Monthly Base Setup

This project now includes a baseline Overture ingestion and query scaffold:

- PostGIS schema: `scripts/overture-postgis-schema.sql`
- Monthly loader: `scripts/overture-monthly-etl.cjs`
- API server: `server/overture-api.cjs`
- NY exporter (DuckDB): `scripts/overture-ny-export.py`
- Unit-level supplemental merger: `scripts/overture-augment-address-units.py`
- State package builder (no API frontend mode): `scripts/overture-build-state-package.py`

### 1. Prerequisites

- PostgreSQL + PostGIS
- `psql` available on PATH
- Overture monthly exports prepared as CSV files
- Python 3 + `duckdb` package for direct Overture export (`python -m pip install duckdb`)

### 2. Export New York monthly CSVs locally

Generate ETL-ready files for New York directly from Overture:

```bash
python scripts/overture-ny-export.py --release 2026-01-21.0 --skip-buildings --overwrite
```

Outputs:

- `data/overture/2026-01-21.0/ny/addresses-ny.csv`
- `data/overture/2026-01-21.0/ny/buildings-ny.csv` (if buildings export is enabled)
- `data/overture/2026-01-21.0/ny/manifest.json`

Fast test run (smaller sample):

```bash
python scripts/overture-ny-export.py \
  --release 2026-01-21.0 \
  --addresses-limit 250000 \
  --buildings-limit 100000 \
  --overwrite
```

### 2.5 Optional: merge unit-level supplemental rows (recommended for apartment completeness)

If you have a NYC supplemental CSV with apartment/unit rows, merge it before packaging:

```bash
python scripts/overture-augment-address-units.py \
  --base-csv data/overture/2026-01-21.0/ny/addresses-ny.csv \
  --supplement-csv data/supplement/nyc-units-template.csv \
  --output-csv data/overture/2026-01-21.0/ny/addresses-ny-augmented.csv \
  --state NY \
  --overwrite
```

Notes:

- Default behavior adds only rows with non-empty `unit` values.
- Supplemental rows require either `geom_wkt` (`POINT (...)`) or `latitude/longitude`.
- You can pass `--supplement-csv` multiple times.
- Template starter file: `data/supplement/nyc-units-template.csv`.

You can also auto-download from NYC Open Data (Socrata) directly:

```bash
python scripts/overture-augment-address-units.py \
  --base-csv data/overture/2026-01-21.0/ny/addresses-ny.csv \
  --download-nyc-socrata-dataset <dataset-id> \
  --nyc-socrata-where "<optional where clause>" \
  --output-csv data/overture/2026-01-21.0/ny/addresses-ny-augmented.csv \
  --state NY \
  --overwrite
```

If your dataset uses non-standard column names, pass explicit mappings:
- `--nyc-socrata-house-field`
- `--nyc-socrata-street-field`
- `--nyc-socrata-unit-field`
- `--nyc-socrata-zip-field`
- `--nyc-socrata-city-field`
- `--nyc-socrata-state-field`
- `--nyc-socrata-lat-field` / `--nyc-socrata-lng-field`
- `--nyc-socrata-geom-field`

### 3. Build a hostable state package (no API mode)

Convert the state CSV into app-downloadable NDJSON chunks and a manifest:

```bash
python scripts/overture-build-state-package.py \
  --addresses-csv data/overture/2026-01-21.0/ny/addresses-ny-augmented.csv \
  --buildings-csv data/overture/2026-01-21.0/ny/buildings-ny.csv \
  --state NY \
  --release 2026-01-21.0 \
  --output-root data/packages \
  --chunk-size 50000 \
  --overwrite
```

Outputs:

- `data/packages/manifest.json`
- `data/packages/states/NY/addresses-ny-*.ndjson`
- `data/packages/states/NY/buildings-ny-*.ndjson`

Frontend runtime (no API required):

- Host `data/packages` on the same website/static origin.
- Optional explicit manifest URL:
  - `window.TERRITORY_PACKAGE_MANIFEST_URL = "https://your-cdn.example.com/data/packages/manifest.json"`
- Auto local-API workflow defaults:
  - `window.TERRITORY_AUTO_STATE_INSTALL = true`
  - `window.TERRITORY_AUTO_BACKFILL_ON_INSTALL = false`
- In-app flow:
  1. Select state in startup dropdown.
  2. App auto-downloads and indexes that state package.
  3. Draw/edit a territory and fetch addresses (strict residential filtering runs locally).

### 4. Apply PostGIS schema (optional backend mode)

```bash
psql "$DATABASE_URL" -f scripts/overture-postgis-schema.sql
```

Tables created:

- `territory.overture_release`
- `territory.overture_import_audit`
- `territory.overture_address`
- `territory.overture_building`

Views created:

- `territory.overture_active_release`
- `territory.overture_addresses_current`
- `territory.overture_buildings_current`

### 5. Monthly import (optional backend mode)

Run one import per Overture release:

```bash
node scripts/overture-monthly-etl.cjs \
  --apply-schema \
  --release 2026-01-21.0 \
  --source-uri "overture://release/2026-01-21.0/us-ny" \
  --addresses-csv data/overture/2026-01-21.0/ny/addresses-ny-augmented.csv
```

If you exported buildings too, append:

```bash
--buildings-csv data/overture/2026-01-21.0/ny/buildings-ny.csv
```

Addresses CSV headers expected:

`id,source_dataset,house_number,street,unit,city,region,postcode,country_code,full_address,geom_wkt,raw_json`

Buildings CSV headers expected:

`id,source_dataset,building_class,levels,name,geom_wkt,raw_json`

Notes:

- `geom_wkt` must be EPSG:4326 WKT.
- `raw_json` should be a JSON string (or empty).
- Loader upserts by `(id, release_id)`.

### 6. Run API server (optional backend mode)

```bash
node server/overture-api.cjs
```

Environment:

- `DATABASE_URL` (required)
- `PORT` (optional, default `8787`)

Endpoints:

- `GET /health`
- `GET /api/overture/release`
- `POST /api/overture/addresses/search`
- `POST /api/overture/buildings/search`

Frontend integration switch:

- Overture local API mode is the default path.
- Set `window.TERRITORY_OVERTURE_API_BASE_URL` (for example `http://localhost:8787`) before app startup when using a non-default API host.
- State readiness is automatic via `/api/overture/state/ensure` and `/api/overture/state/status`.
- Strict residential filtering requires both addresses and buildings datasets.
- `http://localhost` is recommended. `file://` also works when the local API is running and reachable at `window.TERRITORY_OVERTURE_API_BASE_URL`.

Request body for polygon search:

```json
{
  "polygon": [[40.71, -74.01], [40.72, -74.00], [40.70, -73.99]],
  "limit": 1000
}
```

### 7. Production workflow recommendation

1. Use Overture monthly data as the primary nationwide base.
2. Keep one release active in `territory.overture_active_release`.
3. Track apartment completeness with your own QA metrics per ZIP/territory.
