#!/usr/bin/env python3
"""
Export New York Overture monthly data into ETL-ready CSV files.

Outputs:
  data/overture/<release>/ny/addresses-ny.csv
  data/overture/<release>/ny/buildings-ny.csv (optional)
  data/overture/<release>/ny/manifest.json
"""

from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path
import sys

import duckdb


DEFAULT_RELEASE = "2026-01-21.0"
DEFAULT_BUCKET = "overturemaps-us-west-2"
DEFAULT_STATE_REGION = "US-NY"
DEFAULT_STATE_ABBR = "NY"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download and export New York Overture data to local CSV files."
    )
    parser.add_argument(
        "--release",
        default=DEFAULT_RELEASE,
        help=f"Overture release id (default: {DEFAULT_RELEASE})",
    )
    parser.add_argument(
        "--bucket",
        default=DEFAULT_BUCKET,
        help=f"S3 bucket for Overture monthly releases (default: {DEFAULT_BUCKET})",
    )
    parser.add_argument(
        "--output-root",
        default="data/overture",
        help="Root directory for exported files (default: data/overture)",
    )
    parser.add_argument(
        "--state-region",
        default=DEFAULT_STATE_REGION,
        help=f"Overture region code for state boundary filter (default: {DEFAULT_STATE_REGION})",
    )
    parser.add_argument(
        "--state-abbr",
        default=DEFAULT_STATE_ABBR,
        help=f"State abbreviation written to addresses CSV region column (default: {DEFAULT_STATE_ABBR})",
    )
    parser.add_argument(
        "--addresses-limit",
        type=int,
        default=0,
        help="Optional row cap for addresses export (0 = no cap)",
    )
    parser.add_argument(
        "--buildings-limit",
        type=int,
        default=0,
        help="Optional row cap for buildings export (0 = no cap)",
    )
    parser.add_argument(
        "--skip-buildings",
        action="store_true",
        help="Export addresses only.",
    )
    parser.add_argument(
        "--skip-addresses",
        action="store_true",
        help="Export buildings only.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing output files.",
    )
    return parser.parse_args()


def install_extensions(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("INSTALL httpfs;")
    conn.execute("LOAD httpfs;")
    conn.execute("INSTALL spatial;")
    conn.execute("LOAD spatial;")
    conn.execute("SET s3_region = 'us-west-2';")


def ensure_output_paths(base_dir: Path, overwrite: bool, include_addresses: bool, include_buildings: bool) -> dict[str, Path]:
    base_dir.mkdir(parents=True, exist_ok=True)
    paths = {
        "addresses": base_dir / "addresses-ny.csv",
        "buildings": base_dir / "buildings-ny.csv",
        "manifest": base_dir / "manifest.json",
    }
    blocked: list[str] = []
    if include_addresses:
        blocked.append(str(paths["addresses"]))
    if include_buildings:
        blocked.append(str(paths["buildings"]))
    if not overwrite:
        existing = [item for item in blocked if Path(item).exists()]
        if existing:
            raise FileExistsError(
                "Output file already exists. Use --overwrite to replace: " + ", ".join(existing)
            )
    return paths


def sql_quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def get_state_geom_sql(divisions_glob: str, state_region: str) -> str:
    return f"""
      SELECT geometry AS geom
      FROM read_parquet({sql_quote(divisions_glob)})
      WHERE country = 'US'
        AND region = {sql_quote(state_region)}
        AND subtype = 'region'
        AND class = 'land'
      LIMIT 1
    """


def get_addresses_query(addresses_glob: str, state_abbr: str, limit: int) -> str:
    limit_sql = f"LIMIT {int(limit)}" if int(limit) > 0 else ""
    return f"""
SELECT
  a.id,
  COALESCE(a.sources[1].dataset, '') AS source_dataset,
  COALESCE(a.number, '') AS house_number,
  COALESCE(a.street, '') AS street,
  COALESCE(a.unit, '') AS unit,
  COALESCE(a.postal_city, '') AS city,
  {sql_quote(state_abbr)} AS region,
  COALESCE(a.postcode, '') AS postcode,
  COALESCE(a.country, '') AS country_code,
  TRIM(CONCAT_WS(
    ' ',
    NULLIF(a.number, ''),
    NULLIF(a.street, ''),
    CASE
      WHEN NULLIF(a.unit, '') IS NOT NULL THEN CONCAT('Unit ', a.unit)
      ELSE NULL
    END,
    NULLIF(a.postal_city, ''),
    NULLIF(a.postcode, '')
  )) AS full_address,
  ST_AsText(a.geometry) AS geom_wkt,
  to_json(struct_pack(
    id := a.id,
    country := a.country,
    postcode := a.postcode,
    street := a.street,
    number := a.number,
    unit := a.unit,
    postal_city := a.postal_city,
    address_levels := a.address_levels,
    sources := a.sources
  )) AS raw_json
FROM read_parquet({sql_quote(addresses_glob)}) a
WHERE a.country = 'US'
  AND (
    (a.postcode >= '10000' AND a.postcode < '15000')
    OR a.postcode IN ('00501', '00544', '06390')
  )
  AND (
    list_contains(list_transform(a.address_levels, x -> upper(x.value)), {sql_quote(state_abbr)})
    OR a.address_levels IS NULL
  )
{limit_sql}
"""


def get_buildings_query(buildings_glob: str, divisions_glob: str, state_region: str, limit: int) -> str:
    state_sql = get_state_geom_sql(divisions_glob, state_region)
    limit_sql = f"LIMIT {int(limit)}" if int(limit) > 0 else ""
    return f"""
WITH state AS (
  {state_sql}
),
candidates AS (
  SELECT b.*
  FROM read_parquet({sql_quote(buildings_glob)}) b, state s
  WHERE b.bbox.xmax >= ST_XMin(s.geom)
    AND b.bbox.xmin <= ST_XMax(s.geom)
    AND b.bbox.ymax >= ST_YMin(s.geom)
    AND b.bbox.ymin <= ST_YMax(s.geom)
)
SELECT
  b.id,
  COALESCE(b.sources[1].dataset, '') AS source_dataset,
  COALESCE(b.class, '') AS building_class,
  COALESCE(CAST(b.num_floors AS VARCHAR), CAST(b.level AS VARCHAR), '') AS levels,
  COALESCE(b.names.primary, '') AS name,
  ST_AsText(b.geometry) AS geom_wkt,
  to_json(struct_pack(
    id := b.id,
    class := b.class,
    subtype := b.subtype,
    level := b.level,
    num_floors := b.num_floors,
    names := b.names,
    sources := b.sources
  )) AS raw_json
FROM candidates b, state s
WHERE ST_Intersects(b.geometry, s.geom)
{limit_sql}
"""


def copy_query_to_csv(conn: duckdb.DuckDBPyConnection, query: str, output_csv: Path) -> int:
    quoted_output = sql_quote(str(output_csv.resolve()))
    copy_sql = f"COPY ({query}) TO {quoted_output} (HEADER, DELIMITER ',');"
    result = conn.execute(copy_sql).fetchone()
    if not result:
        return 0
    count = int(result[0])
    return count


def main() -> int:
    args = parse_args()
    release = str(args.release).strip()
    bucket = str(args.bucket).strip()
    state_region = str(args.state_region).strip()
    state_abbr = str(args.state_abbr).strip().upper()
    include_addresses = not bool(args.skip_addresses)
    include_buildings = not bool(args.skip_buildings)
    if not include_addresses and not include_buildings:
        raise ValueError("Both datasets are disabled. Remove --skip-addresses or --skip-buildings.")

    if not release:
        raise ValueError("Release cannot be empty.")
    if not bucket:
        raise ValueError("Bucket cannot be empty.")
    if not state_region:
        raise ValueError("State region cannot be empty.")
    if not state_abbr:
        raise ValueError("State abbreviation cannot be empty.")
    if int(args.addresses_limit) < 0 or int(args.buildings_limit) < 0:
        raise ValueError("Row limits must be >= 0.")

    release_dir = Path(args.output_root) / release / "ny"
    paths = ensure_output_paths(release_dir, args.overwrite, include_addresses, include_buildings)

    base = f"s3://{bucket}/release/{release}"
    addresses_glob = f"{base}/theme=addresses/type=address/*"
    buildings_glob = f"{base}/theme=buildings/type=building/*"
    divisions_glob = f"{base}/theme=divisions/type=division_area/*"

    print(f"[overture-ny] release: {release}")
    print(f"[overture-ny] output: {release_dir.resolve()}")

    conn = duckdb.connect()
    install_extensions(conn)
    threads = max(1, (os.cpu_count() or 2) - 1)
    conn.execute(f"PRAGMA threads={threads};")

    existing_manifest = {}
    if paths["manifest"].exists():
        try:
            existing_manifest = json.loads(paths["manifest"].read_text(encoding="utf-8"))
        except Exception:
            existing_manifest = {}

    previous_counts = existing_manifest.get("row_counts", {}) if isinstance(existing_manifest, dict) else {}
    addresses_rows = int(previous_counts.get("addresses") or 0)
    if include_addresses:
        addresses_query = get_addresses_query(
            addresses_glob=addresses_glob,
            state_abbr=state_abbr,
            limit=int(args.addresses_limit),
        )
        print("[overture-ny] exporting addresses...")
        addresses_rows = copy_query_to_csv(conn, addresses_query, paths["addresses"])
        print(f"[overture-ny] addresses rows: {addresses_rows}")
    else:
        print("[overture-ny] addresses export skipped.")

    buildings_rows = int(previous_counts.get("buildings") or 0)
    if include_buildings:
        buildings_query = get_buildings_query(
            buildings_glob=buildings_glob,
            divisions_glob=divisions_glob,
            state_region=state_region,
            limit=int(args.buildings_limit),
        )
        print("[overture-ny] exporting buildings...")
        buildings_rows = copy_query_to_csv(conn, buildings_query, paths["buildings"])
        print(f"[overture-ny] buildings rows: {buildings_rows}")
    else:
        print("[overture-ny] buildings export skipped.")

    manifest = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "release": release,
        "bucket": bucket,
        "state_region": state_region,
        "state_abbr": state_abbr,
        "inputs": {
            "addresses_glob": addresses_glob,
            "buildings_glob": buildings_glob,
            "divisions_glob": divisions_glob,
        },
        "outputs": {
            "addresses_csv": str(paths["addresses"].resolve()) if paths["addresses"].exists() else "",
            "buildings_csv": str(paths["buildings"].resolve()) if paths["buildings"].exists() else "",
        },
        "limits": {
            "addresses_limit": int(args.addresses_limit),
            "buildings_limit": int(args.buildings_limit),
        },
        "row_counts": {
            "addresses": addresses_rows,
            "buildings": buildings_rows,
        },
        "notes": "Generated for local ETL testing with Territory App.",
    }
    paths["manifest"].write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    print(f"[overture-ny] manifest: {paths['manifest'].resolve()}")
    print("[overture-ny] complete.")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:  # pragma: no cover
        print(f"[overture-ny] failed: {exc}", file=sys.stderr)
        raise SystemExit(1)
