#!/usr/bin/env python3
"""
Generate a buildings CSV from Overture addresses CSV by buffering each address point.

This is a local fallback when full buildings export is unavailable in time.
Output headers match overture-build-state-package.py buildings input contract:
  id,source_dataset,building_class,levels,name,geom_wkt,raw_json
"""

from __future__ import annotations

import argparse
import csv
import math
import re
import sys
from pathlib import Path


POINT_RE = re.compile(r"^POINT\s*\(\s*(-?\d+(?:\.\d+)?)\s+(-?\d+(?:\.\d+)?)\s*\)$", re.IGNORECASE)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create proxy buildings CSV from Overture addresses CSV.")
    parser.add_argument("--addresses-csv", required=True, help="Path to addresses CSV (POINT geom_wkt required).")
    parser.add_argument("--output-csv", required=True, help="Output buildings CSV path.")
    parser.add_argument(
        "--buffer-meters",
        type=float,
        default=9.0,
        help="Half-size buffer around point for square polygon (default: 9m).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Optional max number of generated building rows (0 = all).",
    )
    parser.add_argument("--overwrite", action="store_true", help="Overwrite output file if it exists.")
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


def build_square_wkt(lat: float, lng: float, half_meters: float) -> str:
    lat_delta = half_meters / 111320.0
    cos_lat = max(0.15, math.cos(math.radians(lat)))
    lng_delta = half_meters / (111320.0 * cos_lat)
    x1 = lng - lng_delta
    y1 = lat - lat_delta
    x2 = lng + lng_delta
    y2 = lat + lat_delta
    return (
        f"POLYGON (({x1:.7f} {y1:.7f}, {x1:.7f} {y2:.7f}, {x2:.7f} {y2:.7f}, "
        f"{x2:.7f} {y1:.7f}, {x1:.7f} {y1:.7f}))"
    )


def main() -> int:
    args = parse_args()
    src = Path(args.addresses_csv).resolve()
    dst = Path(args.output_csv).resolve()
    if not src.exists():
        raise FileNotFoundError(f"Addresses CSV not found: {src}")
    if dst.exists() and not args.overwrite:
        raise FileExistsError(f"Output exists: {dst}. Use --overwrite.")
    if args.buffer_meters <= 0:
        raise ValueError("--buffer-meters must be > 0")
    if args.limit < 0:
        raise ValueError("--limit must be >= 0")

    dst.parent.mkdir(parents=True, exist_ok=True)
    generated = 0
    skipped = 0
    seen_coords: set[str] = set()

    with src.open("r", encoding="utf-8", newline="") as in_file, dst.open("w", encoding="utf-8", newline="") as out_file:
      reader = csv.DictReader(in_file)
      writer = csv.DictWriter(
          out_file,
          fieldnames=["id", "source_dataset", "building_class", "levels", "name", "geom_wkt", "raw_json"],
      )
      writer.writeheader()

      for idx, row in enumerate(reader, start=1):
          point = parse_point_wkt(row.get("geom_wkt", ""))
          if not point:
              skipped += 1
              continue
          lat, lng = point
          coord_key = f"{lat:.6f}|{lng:.6f}"
          if coord_key in seen_coords:
              continue
          seen_coords.add(coord_key)

          source = str(row.get("source_dataset", "")).strip()
          source_dataset = f"{source}:proxy-building" if source else "overture:proxy-building"
          address_id = str(row.get("id", "")).strip() or f"addr-{idx}"
          writer.writerow(
              {
                  "id": f"bldg-{address_id}",
                  "source_dataset": source_dataset,
                  "building_class": "residential",
                  "levels": "",
                  "name": "",
                  "geom_wkt": build_square_wkt(lat, lng, float(args.buffer_meters)),
                  "raw_json": "{}",
              }
          )
          generated += 1

          if generated % 200000 == 0:
              print(f"[proxy-buildings] generated={generated:,} scanned={idx:,} skipped={skipped:,}", flush=True)
          if args.limit and generated >= args.limit:
              break

    print(f"[proxy-buildings] output={dst}")
    print(f"[proxy-buildings] generated={generated}")
    print(f"[proxy-buildings] skipped={skipped}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"[proxy-buildings] failed: {exc}", file=sys.stderr)
        raise SystemExit(1)
