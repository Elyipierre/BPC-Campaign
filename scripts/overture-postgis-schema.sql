-- Overture monthly-base schema for Territory App
-- Apply with:
--   psql "$DATABASE_URL" -f scripts/overture-postgis-schema.sql

BEGIN;

CREATE EXTENSION IF NOT EXISTS postgis;

CREATE SCHEMA IF NOT EXISTS territory;

CREATE TABLE IF NOT EXISTS territory.overture_release (
  release_id TEXT PRIMARY KEY,
  imported_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  source_uri TEXT NOT NULL,
  notes TEXT
);

CREATE TABLE IF NOT EXISTS territory.overture_import_audit (
  id BIGSERIAL PRIMARY KEY,
  release_id TEXT NOT NULL REFERENCES territory.overture_release(release_id) ON DELETE CASCADE,
  theme TEXT NOT NULL,
  row_count BIGINT NOT NULL DEFAULT 0,
  started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  completed_at TIMESTAMPTZ,
  status TEXT NOT NULL DEFAULT 'running',
  details JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS territory.overture_address (
  id TEXT NOT NULL,
  release_id TEXT NOT NULL REFERENCES territory.overture_release(release_id) ON DELETE CASCADE,
  source_dataset TEXT,
  house_number TEXT,
  street TEXT,
  unit TEXT,
  city TEXT,
  region TEXT,
  postcode TEXT,
  country_code TEXT,
  full_address TEXT,
  geom GEOMETRY(Point, 4326) NOT NULL,
  raw JSONB NOT NULL DEFAULT '{}'::jsonb,
  imported_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (id, release_id)
);

CREATE TABLE IF NOT EXISTS territory.overture_building (
  id TEXT NOT NULL,
  release_id TEXT NOT NULL REFERENCES territory.overture_release(release_id) ON DELETE CASCADE,
  source_dataset TEXT,
  building_class TEXT,
  levels NUMERIC,
  name TEXT,
  geom GEOMETRY(Geometry, 4326) NOT NULL,
  raw JSONB NOT NULL DEFAULT '{}'::jsonb,
  imported_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (id, release_id)
);

CREATE INDEX IF NOT EXISTS overture_address_geom_gix
  ON territory.overture_address
  USING GIST (geom);

CREATE INDEX IF NOT EXISTS overture_address_postcode_idx
  ON territory.overture_address (postcode);

CREATE INDEX IF NOT EXISTS overture_address_city_idx
  ON territory.overture_address ((lower(city)));

CREATE INDEX IF NOT EXISTS overture_address_street_idx
  ON territory.overture_address ((lower(street)));

CREATE INDEX IF NOT EXISTS overture_address_release_idx
  ON territory.overture_address (release_id);

CREATE INDEX IF NOT EXISTS overture_address_raw_gin
  ON territory.overture_address
  USING GIN (raw);

CREATE INDEX IF NOT EXISTS overture_building_geom_gix
  ON territory.overture_building
  USING GIST (geom);

CREATE INDEX IF NOT EXISTS overture_building_release_idx
  ON territory.overture_building (release_id);

CREATE INDEX IF NOT EXISTS overture_building_raw_gin
  ON territory.overture_building
  USING GIN (raw);

CREATE OR REPLACE VIEW territory.overture_active_release AS
SELECT release_id, imported_at, source_uri, notes
FROM territory.overture_release
ORDER BY imported_at DESC, release_id DESC
LIMIT 1;

CREATE OR REPLACE VIEW territory.overture_addresses_current AS
SELECT a.*
FROM territory.overture_address a
JOIN territory.overture_active_release r ON r.release_id = a.release_id;

CREATE OR REPLACE VIEW territory.overture_buildings_current AS
SELECT b.*
FROM territory.overture_building b
JOIN territory.overture_active_release r ON r.release_id = b.release_id;

COMMIT;

