#!/usr/bin/env python3
"""
Merge supplemental unit-level address rows into an Overture addresses CSV.

Base CSV contract (expected output shape):
  id,source_dataset,house_number,street,unit,city,region,postcode,country_code,full_address,geom_wkt,raw_json

Supplement CSV can use alternate column names (for convenience), including:
  - id
  - house_number|number|housenumber
  - street|street_name|streetname|road
  - unit|apartment|apt|suite|addr_unit
  - city|postal_city|locality|borough
  - region|state|state_code
  - postcode|zip|zip_code|postal_code
  - country_code|country
  - full_address
  - geom_wkt OR latitude/longitude (lat/lng aliases)
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import re
import shutil
import sys
import tempfile
import urllib.parse
import urllib.request
from pathlib import Path


BASE_HEADERS = [
    "id",
    "source_dataset",
    "house_number",
    "street",
    "unit",
    "city",
    "region",
    "postcode",
    "country_code",
    "full_address",
    "geom_wkt",
    "raw_json",
]

POINT_RE = re.compile(r"^POINT\s*\(\s*(-?\d+(?:\.\d+)?)\s+(-?\d+(?:\.\d+)?)\s*\)$", re.IGNORECASE)

UNIT_PREFIX_RE = re.compile(r"^(?:apt|apartment|unit|suite|ste|#|rm|room|fl|floor)\s*", re.IGNORECASE)
SOC_HOUSE_ALIASES = ["house_number", "housenumber", "house_num", "number", "addr_num", "house_no", "hnum"]
SOC_STREET_ALIASES = ["street", "street_name", "streetname", "full_street_name", "street_nam", "addr_street"]
SOC_UNIT_ALIASES = ["unit", "apartment", "apt", "suite", "addr_unit", "addr_apartment", "addr_flat", "apartment_number"]
SOC_ZIP_ALIASES = ["zip", "zipcode", "zip_code", "postal_code", "postcode", "incident_zip"]
SOC_CITY_ALIASES = ["city", "postal_city", "borough", "boro", "locality", "neighborhood"]
SOC_STATE_ALIASES = ["state", "state_code", "region", "province"]
SOC_LAT_ALIASES = ["latitude", "lat", "y"]
SOC_LNG_ALIASES = ["longitude", "long", "lon", "lng", "x"]
SOC_GEOM_ALIASES = ["the_geom", "location", "point", "georeference", "geometry"]
SOC_ID_ALIASES = ["id", "record_id", "unique_key", "objectid", "addresspointid"]
NO_PROXY_OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Merge supplemental unit addresses into a base Overture CSV.")
    parser.add_argument("--base-csv", default="", help="Base addresses CSV (e.g. addresses-ny.csv).")
    parser.add_argument(
        "--supplement-csv",
        action="append",
        default=[],
        help="Supplement CSV path. Repeat for multiple files.",
    )
    parser.add_argument("--output-csv", default="", help="Output merged CSV.")
    parser.add_argument("--state", default="NY", help="Default state code for supplemental rows (default: NY).")
    parser.add_argument("--country", default="US", help="Default country code for supplemental rows (default: US).")
    parser.add_argument(
        "--source-dataset",
        default="supplemental/unit-addresses",
        help="Default source_dataset for supplemental rows.",
    )
    parser.add_argument(
        "--allow-missing-unit",
        action="store_true",
        help="Allow supplemental rows with empty unit values (default is to skip them).",
    )
    parser.add_argument(
        "--download-nyc-socrata-dataset",
        default="",
        help="Optional NYC Open Data dataset id to auto-download supplemental rows (e.g. abcd-1234).",
    )
    parser.add_argument(
        "--nyc-socrata-domain",
        default="data.cityofnewyork.us",
        help="NYC Socrata domain (default: data.cityofnewyork.us).",
    )
    parser.add_argument(
        "--nyc-socrata-scheme",
        default="https",
        choices=["https", "http"],
        help="Socrata URL scheme (default: https).",
    )
    parser.add_argument(
        "--nyc-socrata-app-token",
        default="",
        help="Optional Socrata app token. Falls back to NYC_OPEN_DATA_APP_TOKEN env var.",
    )
    parser.add_argument(
        "--nyc-socrata-where",
        default="",
        help="Optional Socrata $where clause to limit downloaded rows.",
    )
    parser.add_argument(
        "--nyc-socrata-batch-size",
        type=int,
        default=50000,
        help="Socrata page size (default: 50000).",
    )
    parser.add_argument(
        "--nyc-socrata-max-rows",
        type=int,
        default=0,
        help="Optional max rows to download from Socrata (0 = all).",
    )
    parser.add_argument(
        "--nyc-socrata-house-field",
        default="",
        help="Optional explicit Socrata field name for house number.",
    )
    parser.add_argument(
        "--nyc-socrata-street-field",
        default="",
        help="Optional explicit Socrata field name for street name.",
    )
    parser.add_argument(
        "--nyc-socrata-unit-field",
        default="",
        help="Optional explicit Socrata field name for apartment/unit.",
    )
    parser.add_argument(
        "--nyc-socrata-zip-field",
        default="",
        help="Optional explicit Socrata field name for ZIP/postcode.",
    )
    parser.add_argument(
        "--nyc-socrata-city-field",
        default="",
        help="Optional explicit Socrata field name for city/borough.",
    )
    parser.add_argument(
        "--nyc-socrata-state-field",
        default="",
        help="Optional explicit Socrata field name for state.",
    )
    parser.add_argument(
        "--nyc-socrata-lat-field",
        default="",
        help="Optional explicit Socrata field name for latitude.",
    )
    parser.add_argument(
        "--nyc-socrata-lng-field",
        default="",
        help="Optional explicit Socrata field name for longitude.",
    )
    parser.add_argument(
        "--nyc-socrata-geom-field",
        default="",
        help="Optional explicit Socrata field name containing point geometry.",
    )
    parser.add_argument(
        "--nyc-socrata-id-field",
        default="",
        help="Optional explicit Socrata field name for source record id.",
    )
    parser.add_argument(
        "--source-profile",
        default="",
        help="Optional source profile id (e.g. ny_open_units) from a registry file.",
    )
    parser.add_argument(
        "--source-profile-file",
        default="data/supplement/source-profiles/ny-open-unit-sources.json",
        help="Path to source profile registry JSON.",
    )
    parser.add_argument(
        "--verify-only",
        action="store_true",
        help="Only download/validate sources and print stats; skip CSV merge output.",
    )
    parser.add_argument(
        "--profile-export-dir",
        default="",
        help="Optional directory to copy per-source downloaded CSV snapshots.",
    )
    parser.add_argument("--overwrite", action="store_true", help="Overwrite output if it exists.")
    return parser.parse_args()


def as_text(value: object, fallback: str = "") -> str:
    text = str(fallback if value is None else value)
    text = re.sub(r"\s+", " ", text).strip()
    return text or fallback


def normalize_state(value: object, fallback: str) -> str:
    state = as_text(value, fallback).upper()
    if len(state) == 2 and state.isalpha():
        return state
    return as_text(fallback, "NY").upper()


def normalize_zip(value: object) -> str:
    raw = as_text(value, "")
    digits = re.sub(r"\D", "", raw)
    if len(digits) >= 5:
        return digits[:5]
    return raw


def parse_point_wkt(geom_wkt: str) -> tuple[float, float] | None:
    match = POINT_RE.match(as_text(geom_wkt, ""))
    if not match:
        return None
    lng = float(match.group(1))
    lat = float(match.group(2))
    if abs(lat) > 90 or abs(lng) > 180:
        return None
    return lat, lng


def point_to_wkt(lat: float, lng: float) -> str:
    return f"POINT ({lng:.7f} {lat:.7f})"


def normalize_unit(value: object) -> str:
    return as_text(value, "")


def canonical_unit(value: str) -> str:
    text = as_text(value, "").lower()
    text = UNIT_PREFIX_RE.sub("", text)
    text = re.sub(r"[^a-z0-9]+", "", text)
    return text


def normalize_street_for_key(value: object) -> str:
    text = as_text(value, "").lower()
    return re.sub(r"[^a-z0-9]+", " ", text).strip()


def normalize_number_for_key(value: object) -> str:
    text = as_text(value, "").lower()
    return re.sub(r"[^a-z0-9-]+", "", text)


def address_unit_signature(
    house_number: str,
    street: str,
    unit: str,
    postcode: str,
    lat: float,
    lng: float,
) -> str:
    return "|".join(
        [
            normalize_number_for_key(house_number),
            normalize_street_for_key(street),
            canonical_unit(unit),
            normalize_zip(postcode),
            f"{float(lat):.6f}",
            f"{float(lng):.6f}",
        ]
    )


def build_full_address(
    house_number: str,
    street: str,
    unit: str,
    city: str,
    region: str,
    postcode: str,
) -> str:
    street_parts = [as_text(house_number, ""), as_text(street, "")]
    unit_clean = as_text(unit, "")
    if unit_clean:
        street_parts.append(unit_clean)
    street_line = " ".join(part for part in street_parts if part).strip()
    locality = ", ".join(part for part in [as_text(city, ""), as_text(region, "")] if part)
    tail = " ".join(part for part in [locality, normalize_zip(postcode)] if part).strip()
    if street_line and tail:
        return f"{street_line}, {tail}"
    return street_line or tail


def stable_supplement_id(
    house_number: str,
    street: str,
    unit: str,
    postcode: str,
    lat: float,
    lng: float,
) -> str:
    payload = "|".join(
        [
            normalize_number_for_key(house_number),
            normalize_street_for_key(street),
            canonical_unit(unit),
            normalize_zip(postcode),
            f"{float(lat):.7f}",
            f"{float(lng):.7f}",
        ]
    )
    digest = hashlib.sha1(payload.encode("utf-8")).hexdigest()[:24]
    return f"supp-{digest}"


def read_field(row: dict[str, object], aliases: list[str]) -> str:
    if not row:
        return ""
    lowered = {str(k).strip().lower(): k for k in row.keys()}
    for alias in aliases:
        key = lowered.get(alias.lower())
        if key is None:
            continue
        value = as_text(row.get(key, ""), "")
        if value:
            return value
    return ""


def normalize_key_lookup(row: dict[str, object]) -> dict[str, str]:
    return {str(key).strip().lower(): str(key) for key in row.keys()}


def pick_field_name(
    key_lookup: dict[str, str],
    explicit: str,
    aliases: list[str],
) -> str:
    explicit_clean = as_text(explicit, "")
    if explicit_clean:
        found = key_lookup.get(explicit_clean.lower())
        return found or explicit_clean
    for alias in aliases:
        found = key_lookup.get(alias.lower())
        if found:
            return found
    return ""


def read_named_field(row: dict[str, object], field_name: str) -> str:
    if not field_name:
        return ""
    lowered = normalize_key_lookup(row)
    actual = lowered.get(field_name.lower(), field_name)
    return as_text(row.get(actual, ""), "")


def parse_point_from_geom_value(value: object) -> tuple[float, float] | None:
    if value is None:
        return None
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return None
        parsed = parse_point_wkt(raw)
        if parsed:
            return parsed
        try:
            obj = json.loads(raw)
            return parse_point_from_geom_value(obj)
        except Exception:
            return None
    if isinstance(value, dict):
        coords = value.get("coordinates")
        if isinstance(coords, str):
            parts = [piece for piece in coords.replace(",", " ").split(" ") if piece]
            if len(parts) >= 2:
                try:
                    lng = float(parts[0])
                    lat = float(parts[1])
                    if abs(lat) <= 90 and abs(lng) <= 180:
                        return lat, lng
                except ValueError:
                    pass
        elif isinstance(coords, list) and len(coords) >= 2:
            try:
                lng = float(coords[0])
                lat = float(coords[1])
                if abs(lat) <= 90 and abs(lng) <= 180:
                    return lat, lng
            except ValueError:
                pass
        latitude = value.get("latitude")
        longitude = value.get("longitude")
        if latitude is not None and longitude is not None:
            try:
                lat = float(latitude)
                lng = float(longitude)
                if abs(lat) <= 90 and abs(lng) <= 180:
                    return lat, lng
            except ValueError:
                return None
    return None


def read_lat_lng(row: dict[str, object]) -> tuple[float, float] | None:
    geom = read_field(row, ["geom_wkt", "geometry", "wkt", "point_wkt"])
    parsed = parse_point_wkt(geom)
    if parsed:
        return parsed

    lat_raw = read_field(row, ["lat", "latitude", "y"])
    lng_raw = read_field(row, ["lng", "lon", "long", "longitude", "x"])
    if not lat_raw or not lng_raw:
        return None
    try:
        lat = float(lat_raw)
        lng = float(lng_raw)
    except ValueError:
        return None
    if abs(lat) > 90 or abs(lng) > 180:
        return None
    return lat, lng


def read_socrata_lat_lng(
    row: dict[str, object],
    lat_field: str,
    lng_field: str,
    geom_field: str,
) -> tuple[float, float] | None:
    lat_raw = read_named_field(row, lat_field)
    lng_raw = read_named_field(row, lng_field)
    if lat_raw and lng_raw:
        try:
            lat = float(lat_raw)
            lng = float(lng_raw)
            if abs(lat) <= 90 and abs(lng) <= 180:
                return lat, lng
        except ValueError:
            pass
    geom_value = None
    if geom_field:
        lowered = normalize_key_lookup(row)
        actual = lowered.get(geom_field.lower(), geom_field)
        geom_value = row.get(actual)
    if geom_value is None:
        for alias in SOC_GEOM_ALIASES:
            lowered = normalize_key_lookup(row)
            actual = lowered.get(alias.lower())
            if actual:
                geom_value = row.get(actual)
                break
    return parse_point_from_geom_value(geom_value)


def http_get_json(url: str, headers: dict[str, str] | None = None) -> object:
    req = urllib.request.Request(url=url, method="GET", headers=headers or {})
    with NO_PROXY_OPENER.open(req, timeout=120) as response:
        body = response.read().decode("utf-8")
    return json.loads(body)


def build_socrata_page_url(
    scheme: str,
    domain: str,
    dataset_id: str,
    batch_size: int,
    offset: int,
    where_clause: str,
) -> str:
    params: dict[str, object] = {
        "$limit": int(batch_size),
        "$offset": int(offset),
    }
    if where_clause:
        params["$where"] = where_clause
    query = urllib.parse.urlencode(params)
    return f"{scheme}://{domain}/resource/{dataset_id}.json?{query}"


def write_socrata_supplement_csv(
    args: argparse.Namespace,
    output_path: Path,
) -> tuple[int, int]:
    dataset_id = as_text(args.download_nyc_socrata_dataset, "")
    if not dataset_id:
        return 0, 0

    domain = as_text(args.nyc_socrata_domain, "data.cityofnewyork.us")
    scheme = as_text(args.nyc_socrata_scheme, "https")
    batch_size = max(1, min(50000, int(args.nyc_socrata_batch_size or 50000)))
    max_rows = max(0, int(args.nyc_socrata_max_rows or 0))
    where_clause = as_text(args.nyc_socrata_where, "")
    token = as_text(args.nyc_socrata_app_token, "") or as_text(os.environ.get("NYC_OPEN_DATA_APP_TOKEN", ""), "")

    headers = {"Accept": "application/json"}
    if token:
        headers["X-App-Token"] = token

    downloaded = 0
    written = 0
    offset = 0
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "number",
                "street",
                "apartment",
                "zip",
                "city",
                "state",
                "latitude",
                "longitude",
                "source",
                "record_id",
            ],
        )
        writer.writeheader()
        stop_download = False

        while True:
            page_url = build_socrata_page_url(
                scheme=scheme,
                domain=domain,
                dataset_id=dataset_id,
                batch_size=batch_size,
                offset=offset,
                where_clause=where_clause,
            )
            payload = http_get_json(page_url, headers=headers)
            if not isinstance(payload, list) or not payload:
                break

            for idx, row in enumerate(payload):
                if max_rows and (downloaded + idx) >= max_rows:
                    stop_download = True
                    break
                if not isinstance(row, dict):
                    continue
                key_lookup = normalize_key_lookup(row)
                house_field = pick_field_name(key_lookup, args.nyc_socrata_house_field, SOC_HOUSE_ALIASES)
                street_field = pick_field_name(key_lookup, args.nyc_socrata_street_field, SOC_STREET_ALIASES)
                unit_field = pick_field_name(key_lookup, args.nyc_socrata_unit_field, SOC_UNIT_ALIASES)
                zip_field = pick_field_name(key_lookup, args.nyc_socrata_zip_field, SOC_ZIP_ALIASES)
                city_field = pick_field_name(key_lookup, args.nyc_socrata_city_field, SOC_CITY_ALIASES)
                state_field = pick_field_name(key_lookup, args.nyc_socrata_state_field, SOC_STATE_ALIASES)
                lat_field = pick_field_name(key_lookup, args.nyc_socrata_lat_field, SOC_LAT_ALIASES)
                lng_field = pick_field_name(key_lookup, args.nyc_socrata_lng_field, SOC_LNG_ALIASES)
                geom_field = pick_field_name(key_lookup, args.nyc_socrata_geom_field, SOC_GEOM_ALIASES)
                id_field = pick_field_name(key_lookup, args.nyc_socrata_id_field, SOC_ID_ALIASES)

                house = read_named_field(row, house_field)
                street = read_named_field(row, street_field)
                unit = read_named_field(row, unit_field)
                if not house and not street:
                    continue
                if (not args.allow_missing_unit) and not unit:
                    continue

                point = read_socrata_lat_lng(row, lat_field, lng_field, geom_field)
                if not point:
                    continue
                lat, lng = point

                zip_code = normalize_zip(read_named_field(row, zip_field))
                city = read_named_field(row, city_field)
                state = normalize_state(read_named_field(row, state_field), as_text(args.state, "NY"))
                record_id = read_named_field(row, id_field)
                if not record_id:
                    record_id = stable_supplement_id(house, street, unit, zip_code, lat, lng)

                writer.writerow(
                    {
                        "number": house,
                        "street": street,
                        "apartment": unit,
                        "zip": zip_code,
                        "city": city,
                        "state": state,
                        "latitude": f"{lat:.7f}",
                        "longitude": f"{lng:.7f}",
                        "source": f"nyc-open-data:{dataset_id}",
                        "record_id": record_id,
                    }
                )
                written += 1

            downloaded += len(payload)
            offset += len(payload)
            if stop_download:
                break
            if len(payload) < batch_size:
                break
            if max_rows and downloaded >= max_rows:
                break

    return downloaded, written


def load_source_profile(profile_file: Path, profile_id: str) -> dict:
    if not profile_file.exists():
        raise FileNotFoundError(f"Source profile file not found: {profile_file}")
    parsed = json.loads(profile_file.read_text(encoding="utf-8"))
    profile = parsed
    if isinstance(parsed, dict) and isinstance(parsed.get("profiles"), list):
        match = next(
            (entry for entry in parsed.get("profiles", []) if as_text(entry.get("profileId", ""), "") == as_text(profile_id, "")),
            None,
        )
        if match is None:
            raise ValueError(f"Profile id not found: {profile_id}")
        profile = match
    if not isinstance(profile, dict):
        raise ValueError("Invalid source profile format.")
    profile_name = as_text(profile.get("profileId", ""), "")
    if profile_id and profile_name and profile_name != profile_id:
        raise ValueError(f"Requested profile '{profile_id}' does not match file profile '{profile_name}'.")
    if not isinstance(profile.get("sources"), list):
        raise ValueError("Profile is missing sources[].")
    return profile


def build_profile_source_namespace(args: argparse.Namespace, profile: dict, source: dict) -> argparse.Namespace:
    field_map = source.get("fieldMap", {}) if isinstance(source.get("fieldMap"), dict) else {}
    dataset_id = as_text(source.get("datasetId", "") or source.get("dataset_id", ""), "")
    if not dataset_id:
        raise ValueError("Profile source missing datasetId.")
    namespace = argparse.Namespace(**vars(args))
    namespace.download_nyc_socrata_dataset = dataset_id
    namespace.nyc_socrata_domain = as_text(source.get("domain", "") or profile.get("domain", "") or args.nyc_socrata_domain, "data.cityofnewyork.us")
    namespace.nyc_socrata_scheme = as_text(source.get("scheme", "") or profile.get("scheme", "") or args.nyc_socrata_scheme, "https")
    namespace.nyc_socrata_where = as_text(source.get("where", "") or profile.get("where", "") or args.nyc_socrata_where, "")
    namespace.nyc_socrata_house_field = as_text(field_map.get("houseNumber", "") or field_map.get("house_number", ""), "")
    namespace.nyc_socrata_street_field = as_text(field_map.get("street", ""), "")
    namespace.nyc_socrata_unit_field = as_text(field_map.get("unit", ""), "")
    namespace.nyc_socrata_zip_field = as_text(field_map.get("postcode", "") or field_map.get("zip", ""), "")
    namespace.nyc_socrata_city_field = as_text(field_map.get("city", ""), "")
    namespace.nyc_socrata_state_field = as_text(field_map.get("region", "") or field_map.get("state", ""), "")
    namespace.nyc_socrata_lat_field = as_text(field_map.get("lat", "") or field_map.get("latitude", ""), "")
    namespace.nyc_socrata_lng_field = as_text(field_map.get("lng", "") or field_map.get("longitude", ""), "")
    namespace.nyc_socrata_geom_field = as_text(field_map.get("geom", "") or field_map.get("geometry", "") or field_map.get("point", ""), "")
    namespace.nyc_socrata_id_field = as_text(field_map.get("sourceRecordId", "") or field_map.get("source_record_id", ""), "")
    return namespace


def normalize_base_row(row: dict[str, object]) -> dict[str, str]:
    normalized: dict[str, str] = {}
    for header in BASE_HEADERS:
        normalized[header] = as_text(row.get(header, ""), "")
    return normalized


def normalize_supplement_row(
    row: dict[str, object],
    default_state: str,
    default_country: str,
    default_source: str,
) -> dict[str, str] | None:
    house_number = read_field(row, ["house_number", "number", "housenumber", "addr_housenumber"])
    street = read_field(row, ["street", "street_name", "streetname", "road", "addr_street"])
    unit = normalize_unit(read_field(row, ["unit", "apartment", "apt", "suite", "addr_unit", "addr_apartment", "addr_flat"]))
    city = read_field(row, ["city", "postal_city", "locality", "borough"])
    region = normalize_state(read_field(row, ["region", "state", "state_code", "province"]), default_state)
    postcode = normalize_zip(read_field(row, ["postcode", "zip", "zip_code", "postal_code"]))
    country_code = as_text(read_field(row, ["country_code", "country"]), default_country).upper()
    source_dataset = as_text(read_field(row, ["source_dataset", "source"]), default_source)
    full_address = read_field(row, ["full_address", "address", "display_address"])
    row_id = read_field(row, ["id", "record_id"])

    point = read_lat_lng(row)
    if not point:
        return None
    lat, lng = point
    geom_wkt = point_to_wkt(lat, lng)

    if not house_number and not street:
        if full_address and "," in full_address:
            house_and_street = full_address.split(",", 1)[0]
            parts = [part for part in house_and_street.split(" ") if part]
            if len(parts) >= 2:
                house_number = parts[0]
                street = " ".join(parts[1:])
        if not house_number and not street:
            return None

    if not full_address:
        full_address = build_full_address(house_number, street, unit, city, region, postcode)

    if not row_id:
        row_id = stable_supplement_id(house_number, street, unit, postcode, lat, lng)

    raw_json = read_field(row, ["raw_json"])
    if not raw_json:
        raw_json = json.dumps({"supplement": dict(row)}, ensure_ascii=True)

    return {
        "id": row_id,
        "source_dataset": source_dataset,
        "house_number": as_text(house_number, ""),
        "street": as_text(street, ""),
        "unit": unit,
        "city": as_text(city, ""),
        "region": region,
        "postcode": postcode,
        "country_code": country_code or default_country,
        "full_address": as_text(full_address, ""),
        "geom_wkt": geom_wkt,
        "raw_json": raw_json,
    }


def iter_csv_rows(csv_path: Path):
    with csv_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            yield row


def resolve_output_paths(base_csv: Path, output_csv: Path, overwrite: bool) -> tuple[Path, Path, bool]:
    in_place = base_csv.resolve() == output_csv.resolve()
    if in_place:
        fd, temp_name = tempfile.mkstemp(prefix="overture-augmented-", suffix=".csv")
        os.close(fd)
        temp_file = Path(temp_name)
        return temp_file, output_csv, True
    if output_csv.exists() and not overwrite:
        raise FileExistsError(f"Output already exists: {output_csv}. Use --overwrite.")
    return output_csv, output_csv, False


def main() -> int:
    args = parse_args()
    base_csv = Path(args.base_csv).resolve() if as_text(args.base_csv, "") else None
    output_csv = Path(args.output_csv).resolve() if as_text(args.output_csv, "") else None
    supplement_csvs = [Path(item).resolve() for item in args.supplement_csv]
    temp_download_csvs: list[Path] = []
    source_download_stats: list[dict[str, object]] = []
    default_state = normalize_state(args.state, "NY")
    default_country = as_text(args.country, "US").upper()
    default_source = as_text(args.source_dataset, "supplemental/unit-addresses")
    require_unit = not args.allow_missing_unit

    dataset_id = as_text(args.download_nyc_socrata_dataset, "")
    profile_id = as_text(args.source_profile, "")
    profile_file = Path(args.source_profile_file).resolve() if as_text(args.source_profile_file, "") else None
    verify_only = bool(args.verify_only)

    if not supplement_csvs and not dataset_id and not profile_id:
        raise ValueError("Provide --source-profile, --supplement-csv, or --download-nyc-socrata-dataset.")

    if verify_only and (base_csv or output_csv):
        raise ValueError("--verify-only cannot be used with --base-csv/--output-csv.")

    if not verify_only and (base_csv is None or output_csv is None):
        raise ValueError("--base-csv and --output-csv are required unless --verify-only is used.")

    if base_csv and not base_csv.exists():
        raise FileNotFoundError(f"Base CSV not found: {base_csv}")
    for supp in supplement_csvs:
        if not supp.exists():
            raise FileNotFoundError(f"Supplement CSV not found: {supp}")

    if profile_id:
        if profile_file is None:
            raise ValueError("--source-profile-file is required when --source-profile is used.")
        profile = load_source_profile(profile_file, profile_id)
        profile_sources = profile.get("sources", [])
        enabled_sources = [source for source in profile_sources if source.get("enabled", True)]
        if not enabled_sources:
            raise ValueError(f"No enabled sources found in profile: {profile_id}")
        for source in enabled_sources:
            source_id = as_text(source.get("sourceId", "") or source.get("source_id", ""), "")
            fd, temp_name = tempfile.mkstemp(prefix=f"overture-profile-{source_id or 'source'}-", suffix=".csv")
            os.close(fd)
            temp_csv = Path(temp_name).resolve()
            temp_download_csvs.append(temp_csv)
            source_args = build_profile_source_namespace(args, profile, source)
            rows_downloaded, rows_written = write_socrata_supplement_csv(source_args, temp_csv)
            supplement_csvs.append(temp_csv)
            source_download_stats.append({
                "source_id": source_id,
                "dataset_id": as_text(source.get("datasetId", "") or source.get("dataset_id", ""), ""),
                "rows_downloaded": int(rows_downloaded),
                "rows_written": int(rows_written),
            })

    if dataset_id:
        fd, temp_name = tempfile.mkstemp(prefix="overture-nyc-socrata-", suffix=".csv")
        os.close(fd)
        downloaded_temp_csv = Path(temp_name).resolve()
        temp_download_csvs.append(downloaded_temp_csv)
        downloaded_rows, downloaded_written = write_socrata_supplement_csv(args, downloaded_temp_csv)
        supplement_csvs.append(downloaded_temp_csv)
        source_download_stats.append({
            "source_id": as_text(dataset_id, ""),
            "dataset_id": as_text(dataset_id, ""),
            "rows_downloaded": int(downloaded_rows),
            "rows_written": int(downloaded_written),
        })

    if as_text(args.profile_export_dir, ""):
        export_dir = Path(args.profile_export_dir).resolve()
        export_dir.mkdir(parents=True, exist_ok=True)
        for csv_path in temp_download_csvs:
            if csv_path.exists():
                shutil.copy2(str(csv_path), str(export_dir / csv_path.name))

    if verify_only:
        print(f"[augment-units] source_profile={profile_id or '(none)'}")
        print(f"[augment-units] sources_checked={len(source_download_stats)}")
        for entry in source_download_stats:
            print(
                "[augment-units] "
                f"source_id={entry.get('source_id','')} "
                f"dataset_id={entry.get('dataset_id','')} "
                f"rows_downloaded={entry.get('rows_downloaded',0)} "
                f"rows_written={entry.get('rows_written',0)}"
            )
        for temp_csv in temp_download_csvs:
            temp_csv.unlink(missing_ok=True)
        return 0

    assert base_csv is not None
    assert output_csv is not None
    write_path, final_path, in_place = resolve_output_paths(base_csv, output_csv, args.overwrite)
    write_path.parent.mkdir(parents=True, exist_ok=True)

    base_count = 0
    added_count = 0
    skipped_missing_point = 0
    skipped_missing_unit = 0
    skipped_duplicate_unit = 0
    skipped_duplicate_supp = 0

    existing_unit_signatures: set[str] = set()
    supplemental_seen_signatures: set[str] = set()

    try:
        with base_csv.open("r", encoding="utf-8", newline="") as base_handle, write_path.open(
            "w", encoding="utf-8", newline=""
        ) as out_handle:
            base_reader = csv.DictReader(base_handle)
            writer = csv.DictWriter(out_handle, fieldnames=BASE_HEADERS)
            writer.writeheader()

            for raw in base_reader:
                base_row = normalize_base_row(raw)
                writer.writerow(base_row)
                base_count += 1

                unit = normalize_unit(base_row.get("unit", ""))
                if not unit:
                    continue
                parsed = parse_point_wkt(base_row.get("geom_wkt", ""))
                if not parsed:
                    continue
                lat, lng = parsed
                signature = address_unit_signature(
                    base_row.get("house_number", ""),
                    base_row.get("street", ""),
                    unit,
                    base_row.get("postcode", ""),
                    lat,
                    lng,
                )
                existing_unit_signatures.add(signature)

            for supp_csv in supplement_csvs:
                for raw in iter_csv_rows(supp_csv):
                    row = normalize_supplement_row(raw, default_state, default_country, default_source)
                    if row is None:
                        skipped_missing_point += 1
                        continue

                    unit = normalize_unit(row.get("unit", ""))
                    if require_unit and not unit:
                        skipped_missing_unit += 1
                        continue

                    parsed = parse_point_wkt(row.get("geom_wkt", ""))
                    if not parsed:
                        skipped_missing_point += 1
                        continue
                    lat, lng = parsed

                    signature = address_unit_signature(
                        row.get("house_number", ""),
                        row.get("street", ""),
                        row.get("unit", ""),
                        row.get("postcode", ""),
                        lat,
                        lng,
                    )
                    if unit and signature in existing_unit_signatures:
                        skipped_duplicate_unit += 1
                        continue
                    if signature in supplemental_seen_signatures:
                        skipped_duplicate_supp += 1
                        continue

                    supplemental_seen_signatures.add(signature)
                    writer.writerow({key: as_text(row.get(key, ""), "") for key in BASE_HEADERS})
                    added_count += 1

        if in_place:
            shutil.move(str(write_path), str(final_path))

        for entry in source_download_stats:
            print(
                "[augment-units] "
                f"source_id={entry.get('source_id','')} "
                f"dataset_id={entry.get('dataset_id','')} "
                f"rows_downloaded={entry.get('rows_downloaded',0)} "
                f"rows_written={entry.get('rows_written',0)}"
            )
        print(f"[augment-units] base_rows={base_count}")
        print(f"[augment-units] supplement_files={len(supplement_csvs)}")
        print(f"[augment-units] added_rows={added_count}")
        print(f"[augment-units] skipped_missing_point={skipped_missing_point}")
        print(f"[augment-units] skipped_missing_unit={skipped_missing_unit}")
        print(f"[augment-units] skipped_duplicate_unit={skipped_duplicate_unit}")
        print(f"[augment-units] skipped_duplicate_supp={skipped_duplicate_supp}")
        print(f"[augment-units] output={final_path}")
    finally:
        for temp_csv in temp_download_csvs:
            if temp_csv.exists():
                temp_csv.unlink(missing_ok=True)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"[augment-units] failed: {exc}", file=sys.stderr)
        raise SystemExit(1)
