#!/usr/bin/env python3
"""
Build a hostable state package (NDJSON chunks + manifest entry) from Overture CSV exports.

Input CSV headers expected:
  Addresses:
    id,source_dataset,house_number,street,unit,city,region,postcode,country_code,full_address,geom_wkt,raw_json
  Buildings:
    id,source_dataset,building_class,levels,name,geom_wkt,raw_json
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path


POINT_RE = re.compile(r"^POINT\s*\(\s*(-?\d+(?:\.\d+)?)\s+(-?\d+(?:\.\d+)?)\s*\)$", re.IGNORECASE)
POLYGON_WKT_RE = re.compile(r"^\s*(?:MULTI)?POLYGON\s*\(", re.IGNORECASE)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build Overture state package chunks for the Territory app.")
    parser.add_argument("--input-csv", help="Legacy alias for --addresses-csv.")
    parser.add_argument("--addresses-csv", help="State addresses CSV generated from Overture export.")
    parser.add_argument("--buildings-csv", help="State buildings CSV generated from Overture export.")
    parser.add_argument(
        "--allow-addresses-only",
        action="store_true",
        help="Allow package generation without buildings dataset (strict residential mode will be unavailable).",
    )
    parser.add_argument("--state", required=True, help="State code, e.g. NY.")
    parser.add_argument("--release", required=True, help="Overture release id, e.g. 2026-01-21.0.")
    parser.add_argument("--output-root", default="data/packages", help="Output root directory (default: data/packages).")
    parser.add_argument("--chunk-size", type=int, default=50000, help="Rows per NDJSON chunk (default: 50000).")
    parser.add_argument("--manifest-name", default="manifest.json", help="Manifest file name under output root.")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing chunk files for this state.")
    return parser.parse_args()


def parse_point_wkt(wkt: str) -> tuple[float, float] | None:
    match = POINT_RE.match((wkt or "").strip())
    if not match:
        return None
    lng = float(match.group(1))
    lat = float(match.group(2))
    if abs(lat) > 90 or abs(lng) > 180:
        return None
    return lat, lng


def normalize_state_code(value: str) -> str:
    return (value or "").strip().upper()


def build_row_payload(row: dict, fallback_state: str) -> dict | None:
    point = parse_point_wkt(row.get("geom_wkt", ""))
    if not point:
        return None
    lat, lng = point
    row_id = str(row.get("id", "")).strip()
    if not row_id:
        return None
    house_number = str(row.get("house_number", "")).strip()
    street = str(row.get("street", "")).strip()
    unit = str(row.get("unit", "")).strip()
    city = str(row.get("city", "")).strip()
    region = normalize_state_code(str(row.get("region", "")).strip() or fallback_state)
    postcode = str(row.get("postcode", "")).strip()
    full_address = str(row.get("full_address", "")).strip()
    if not full_address:
        components = [house_number, street, f"Unit {unit}" if unit else "", city, postcode]
        full_address = " ".join(part for part in components if part).strip()
    return {
        "id": row_id,
        "source_dataset": str(row.get("source_dataset", "")).strip(),
        "house_number": house_number,
        "street": street,
        "unit": unit,
        "city": city,
        "region": region,
        "postcode": postcode,
        "country_code": str(row.get("country_code", "")).strip() or "US",
        "full_address": full_address,
        "lat": lat,
        "lng": lng,
    }


def normalize_building_geom_wkt(raw: str) -> str:
    geom_wkt = str(raw or "").strip()
    if not geom_wkt:
        return ""
    if not POLYGON_WKT_RE.match(geom_wkt):
        return ""
    return geom_wkt


def build_building_row_payload(row: dict, fallback_state: str) -> dict | None:
    row_id = str(row.get("id", "")).strip()
    if not row_id:
        return None
    geom_wkt = normalize_building_geom_wkt(row.get("geom_wkt", ""))
    if not geom_wkt:
        return None
    return {
        "id": row_id,
        "source_dataset": str(row.get("source_dataset", "")).strip(),
        "building_class": str(row.get("building_class", "")).strip(),
        "levels": str(row.get("levels", "")).strip(),
        "name": str(row.get("name", "")).strip(),
        "geom_wkt": geom_wkt,
        "region": normalize_state_code(str(row.get("region", "")).strip() or fallback_state),
    }


def read_manifest(manifest_path: Path) -> dict:
    if not manifest_path.exists():
        return {}
    try:
        return json.loads(manifest_path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def write_manifest(manifest_path: Path, manifest: dict) -> None:
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")


def remove_existing_chunks(state_dir: Path, overwrite: bool) -> None:
    existing = list(state_dir.glob("addresses-*.ndjson")) + list(state_dir.glob("buildings-*.ndjson"))
    if not existing:
        return
    if overwrite:
        for chunk in existing:
            chunk.unlink()
        return
    raise FileExistsError(f"Existing chunk files found in {state_dir}. Use --overwrite.")


def write_dataset_chunks(
    *,
    csv_path: Path,
    state: str,
    chunk_size: int,
    state_dir: Path,
    dataset_prefix: str,
    payload_builder,
) -> tuple[int, int, list[dict]]:
    total_rows = 0
    skipped_rows = 0
    chunk_rows = 0
    chunk_index = 0
    chunk_file = None
    chunk_paths: list[dict] = []

    def open_next_chunk() -> object:
        nonlocal chunk_index
        chunk_index += 1
        file_name = f"{dataset_prefix}-{state.lower()}-{chunk_index:04d}.ndjson"
        chunk_path = state_dir / file_name
        handle = chunk_path.open("w", encoding="utf-8", newline="\n")
        chunk_paths.append(
            {
                "path": str(Path("states") / state / file_name).replace("\\", "/"),
                "count": 0,
            }
        )
        return handle

    with csv_path.open("r", encoding="utf-8", newline="") as csv_file:
        reader = csv.DictReader(csv_file)
        for row in reader:
            payload = payload_builder(row, state)
            if not payload:
                skipped_rows += 1
                continue
            if chunk_file is None or chunk_rows >= chunk_size:
                if chunk_file is not None:
                    chunk_file.close()
                chunk_file = open_next_chunk()
                chunk_rows = 0
            chunk_file.write(json.dumps(payload, ensure_ascii=True) + "\n")
            chunk_rows += 1
            total_rows += 1
            chunk_paths[-1]["count"] += 1

    if chunk_file is not None:
        chunk_file.close()

    return total_rows, skipped_rows, chunk_paths


def main() -> int:
    args = parse_args()
    addresses_csv_arg = str(args.addresses_csv or args.input_csv or "").strip()
    if not addresses_csv_arg:
        raise ValueError("Missing addresses CSV. Provide --addresses-csv (or legacy --input-csv).")
    addresses_csv = Path(addresses_csv_arg).resolve()
    if not addresses_csv.exists():
        raise FileNotFoundError(f"Addresses CSV not found: {addresses_csv}")

    buildings_csv = None
    if args.buildings_csv:
        candidate = Path(args.buildings_csv).resolve()
        if not candidate.exists():
            raise FileNotFoundError(f"Buildings CSV not found: {candidate}")
        buildings_csv = candidate
    elif not args.allow_addresses_only:
        raise ValueError("Missing buildings CSV. Provide --buildings-csv or pass --allow-addresses-only.")

    if args.chunk_size <= 0:
        raise ValueError("--chunk-size must be >= 1")

    state = normalize_state_code(args.state)
    if not state or len(state) != 2:
        raise ValueError("--state must be a 2-letter code")

    output_root = Path(args.output_root).resolve()
    state_dir = output_root / "states" / state
    state_dir.mkdir(parents=True, exist_ok=True)

    remove_existing_chunks(state_dir, args.overwrite)

    address_rows, address_skipped, address_chunks = write_dataset_chunks(
        csv_path=addresses_csv,
        state=state,
        chunk_size=args.chunk_size,
        state_dir=state_dir,
        dataset_prefix="addresses",
        payload_builder=build_row_payload,
    )
    building_rows = 0
    building_skipped = 0
    building_chunks: list[dict] = []
    if buildings_csv:
        building_rows, building_skipped, building_chunks = write_dataset_chunks(
            csv_path=buildings_csv,
            state=state,
            chunk_size=args.chunk_size,
            state_dir=state_dir,
            dataset_prefix="buildings",
            payload_builder=build_building_row_payload,
        )

    manifest_path = output_root / args.manifest_name
    manifest = read_manifest(manifest_path)
    states = manifest.get("states")
    if not isinstance(states, dict):
        states = {}

    release = str(args.release).strip()
    now = datetime.now(timezone.utc).isoformat()
    dataset_addresses = {
        "format": "ndjson",
        "count": address_rows,
        "chunks": address_chunks,
    }
    dataset_buildings = {
        "format": "ndjson",
        "count": building_rows,
        "chunks": building_chunks,
    }

    states[state] = {
        "state": state,
        "release": release,
        "format": "ndjson",
        "count": address_rows,
        "updatedAt": now,
        # Legacy compatibility path (addresses-only readers).
        "chunks": address_chunks,
        "datasets": {
            "addresses": dataset_addresses,
            "buildings": dataset_buildings,
        },
    }
    manifest["schema_version"] = 2
    manifest["release"] = release
    manifest["generated_at"] = now
    manifest["states"] = states
    write_manifest(manifest_path, manifest)

    print(f"[package] state={state}")
    print(f"[package] release={args.release}")
    print(f"[package] addresses.rows={address_rows}")
    print(f"[package] addresses.skipped={address_skipped}")
    print(f"[package] addresses.chunks={len(address_chunks)}")
    print(f"[package] buildings.rows={building_rows}")
    print(f"[package] buildings.skipped={building_skipped}")
    print(f"[package] buildings.chunks={len(building_chunks)}")
    print(f"[package] manifest={manifest_path}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"[package] failed: {exc}", file=sys.stderr)
        raise SystemExit(1)
