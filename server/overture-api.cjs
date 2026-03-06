"use strict";

const fs = require("node:fs");
const path = require("node:path");
const http = require("node:http");
const crypto = require("node:crypto");
const { URL } = require("node:url");
const { DatabaseSync } = require("node:sqlite");

const PORT = Number(process.env.PORT || 8787);
const STATIC_PORT = Number(process.env.STATIC_PORT || 4173);
const HOST = String(process.env.HOST || "127.0.0.1").trim() || "127.0.0.1";
const PACKAGE_MANIFEST_URL = String(
  process.env.PACKAGE_MANIFEST_URL || `http://${HOST}:${STATIC_PORT}/data/packages/manifest.json`
).trim();
const PACKAGE_MANIFEST_PATH = path.resolve(String(process.env.PACKAGE_MANIFEST_PATH || "data/packages/manifest.json").trim());
const CACHE_DB_PATH = path.resolve(String(process.env.OVERTURE_CACHE_DB_PATH || "data/runtime-cache/overture-cache.db").trim());
const UNIT_SOURCE_PROFILE_PATH = path.resolve(
  String(process.env.OVERTURE_UNIT_SOURCE_PROFILE_PATH || "data/supplement/source-profiles/ny-open-unit-sources.json").trim()
);
const SUPPORTED_STATES = new Set(["NY"]);
const MAX_LIMIT = 5000;
const DEFAULT_LIMIT = 1000;
const TILE_SIZE = 0.01;
const DOWNLOAD_TIMEOUT_MS = 180000;
const DOWNLOAD_RETRIES = 3;
const RETRY_BASE_MS = 800;
const UNIT_SYNC_INTERVAL_MS = Math.max(
  5 * 60 * 1000,
  Number(process.env.OVERTURE_UNIT_SYNC_INTERVAL_MS || 24 * 60 * 60 * 1000)
);
const UNIT_SYNC_STALE_MS = Math.max(
  5 * 60 * 1000,
  Number(process.env.OVERTURE_UNIT_SYNC_STALE_MS || 24 * 60 * 60 * 1000)
);
const UNIT_SYNC_BATCH_SIZE = Math.max(1000, Math.min(50000, Number(process.env.OVERTURE_UNIT_SYNC_BATCH_SIZE || 5000)));
const UNIT_SYNC_MAX_RETRIES = Math.max(1, Number(process.env.OVERTURE_UNIT_SYNC_MAX_RETRIES || 4));
const UNIT_SYNC_RETRY_BASE_MS = Math.max(200, Number(process.env.OVERTURE_UNIT_SYNC_RETRY_BASE_MS || 750));
const UNIT_SYNC_DISABLED = /^(1|true|yes|on)$/i.test(String(process.env.OVERTURE_UNIT_SYNC_DISABLED || "").trim());
const UNIT_STRICT_CONFIDENCE_THRESHOLD = 0.45;
const UNIT_POINT_MATCH_METERS = 30;
const UNIT_NEAREST_BASE_MATCH_METERS = 25;
const ALIGN_PREVIEW_TTL_MS = Math.max(30_000, Number(process.env.TERRITORY_ALIGN_PREVIEW_TTL_MS || 10 * 60 * 1000));
const DEFAULT_TERRITORY_EDGE_TOLERANCE_METERS = Math.max(
  0,
  Number(process.env.OVERTURE_TERRITORY_EDGE_TOLERANCE_METERS || 18)
);
const RELAXED_TERRITORY_EDGE_TOLERANCE_METERS = Math.max(
  DEFAULT_TERRITORY_EDGE_TOLERANCE_METERS,
  Number(process.env.OVERTURE_RELAXED_TERRITORY_EDGE_TOLERANCE_METERS || 35)
);
const PHASES = Object.freeze({
  idle: "idle",
  downloading: "downloading",
  indexing: "indexing",
  ready: "ready",
  error: "error"
});
const RESIDENTIAL_BUILDING_TYPES = new Set([
  "apartments",
  "residential",
  "house",
  "detached",
  "semidetached_house",
  "terrace",
  "bungalow",
  "dormitory"
]);

const ensureJobs = new Map();
const unitSyncJobs = new Map();
const alignmentPreviewJobs = new Map();
let unitSyncTimer = null;
let unitSourceProfileCache = { loadedAt: 0, profile: null };
const buildingGeometryCache = new Map();

function ensureDir(dirPath) {
  fs.mkdirSync(dirPath, { recursive: true });
}
function nowIso() {
  return new Date().toISOString();
}
function asText(value, fallback = "") {
  const text = String(value == null ? fallback : value).replace(/\s+/g, " ").trim();
  return text || String(fallback || "");
}
function normalizeStateCode(value) {
  const state = String(value || "").trim().toUpperCase();
  return /^[A-Z]{2}$/.test(state) ? state : "";
}
function clampLimit(value) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed <= 0) return DEFAULT_LIMIT;
  return Math.max(1, Math.min(MAX_LIMIT, Math.floor(parsed)));
}
function computeTileKey(lat, lng) {
  const latIdx = Math.floor((Number(lat) + 90) / TILE_SIZE);
  const lngIdx = Math.floor((Number(lng) + 180) / TILE_SIZE);
  return `${latIdx}:${lngIdx}`;
}
function tileIndexLat(lat) {
  return Math.floor((Number(lat) + 90) / TILE_SIZE);
}
function tileIndexLng(lng) {
  return Math.floor((Number(lng) + 180) / TILE_SIZE);
}

function sendJson(res, statusCode, payload) {
  const body = JSON.stringify(payload);
  res.writeHead(statusCode, {
    "Content-Type": "application/json; charset=utf-8",
    "Content-Length": Buffer.byteLength(body),
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type"
  });
  res.end(body);
}
function parseRequestBody(req) {
  return new Promise((resolve, reject) => {
    let data = "";
    req.on("data", chunk => {
      data += chunk;
      if (data.length > 8_000_000) {
        reject(new Error("Request body too large."));
        req.destroy();
      }
    });
    req.on("end", () => {
      const trimmed = String(data || "").trim();
      if (!trimmed) return resolve({});
      try {
        resolve(JSON.parse(trimmed));
      } catch {
        reject(new Error("Invalid JSON body."));
      }
    });
    req.on("error", reject);
  });
}
function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, Math.max(0, Number(ms) || 0)));
}

function normalizePoint(point) {
  if (!Array.isArray(point) || point.length < 2) return null;
  const lat = Number(point[0]);
  const lng = Number(point[1]);
  if (!Number.isFinite(lat) || !Number.isFinite(lng)) return null;
  if (Math.abs(lat) > 90 || Math.abs(lng) > 180) return null;
  return [lat, lng];
}
function normalizePolygon(polygon) {
  if (!Array.isArray(polygon)) throw new Error("polygon must be an array of [lat, lng] coordinates.");
  const points = polygon.map(normalizePoint).filter(Boolean);
  if (points.length < 3) throw new Error("polygon requires at least 3 valid points.");
  return points;
}
function getPolygonBounds(polygon) {
  const points = normalizePolygon(polygon);
  const first = points[0];
  const bounds = { minLat: first[0], maxLat: first[0], minLng: first[1], maxLng: first[1] };
  for (let i = 1; i < points.length; i += 1) {
    const [lat, lng] = points[i];
    if (lat < bounds.minLat) bounds.minLat = lat;
    if (lat > bounds.maxLat) bounds.maxLat = lat;
    if (lng < bounds.minLng) bounds.minLng = lng;
    if (lng > bounds.maxLng) bounds.maxLng = lng;
  }
  return bounds;
}
function metersPerDegreeLngAtLat(lat) {
  const cosLat = Math.cos((Number(lat) * Math.PI) / 180);
  const safeCos = Math.max(0.1, Math.abs(cosLat));
  return 111320 * safeCos;
}
function segmentDistanceSq(px, py, ax, ay, bx, by) {
  let dx = bx - ax;
  let dy = by - ay;
  if (dx !== 0 || dy !== 0) {
    const t = ((px - ax) * dx + (py - ay) * dy) / ((dx * dx) + (dy * dy));
    if (t > 1) {
      ax = bx;
      ay = by;
    } else if (t > 0) {
      ax += dx * t;
      ay += dy * t;
    }
  }
  dx = px - ax;
  dy = py - ay;
  return (dx * dx) + (dy * dy);
}
function distanceToPolygonMeters(point, polygon) {
  if (!Array.isArray(point) || point.length < 2 || !Array.isArray(polygon) || polygon.length < 3) return Infinity;
  const lat = Number(point[0]);
  const lng = Number(point[1]);
  if (!Number.isFinite(lat) || !Number.isFinite(lng)) return Infinity;
  const metersPerLat = 111320;
  const metersPerLng = metersPerDegreeLngAtLat(lat);
  const px = lng * metersPerLng;
  const py = lat * metersPerLat;
  let minDistSq = Infinity;
  for (let i = 0, j = polygon.length - 1; i < polygon.length; j = i++) {
    const a = polygon[j];
    const b = polygon[i];
    const ax = Number(a[1]) * metersPerLng;
    const ay = Number(a[0]) * metersPerLat;
    const bx = Number(b[1]) * metersPerLng;
    const by = Number(b[0]) * metersPerLat;
    minDistSq = Math.min(minDistSq, segmentDistanceSq(px, py, ax, ay, bx, by));
  }
  return Number.isFinite(minDistSq) ? Math.sqrt(minDistSq) : Infinity;
}
function pointInPolygonOrNear(point, polygon, toleranceMeters = 0) {
  if (!Array.isArray(point) || point.length < 2) return false;
  if (pointInPolygon(point, polygon)) return true;
  const tolerance = Math.max(0, Number(toleranceMeters) || 0);
  if (tolerance <= 0) return false;
  const distance = distanceToPolygonMeters(point, polygon);
  return Number.isFinite(distance) && distance <= tolerance;
}
function expandBoundsByMeters(bounds, paddingMeters = 0) {
  const pad = Math.max(0, Number(paddingMeters) || 0);
  if (!pad) return { ...bounds };
  const centerLat = (Number(bounds.minLat) + Number(bounds.maxLat)) / 2;
  const latDelta = pad / 111320;
  const lngDelta = pad / metersPerDegreeLngAtLat(centerLat);
  return {
    minLat: Number(bounds.minLat) - latDelta,
    maxLat: Number(bounds.maxLat) + latDelta,
    minLng: Number(bounds.minLng) - lngDelta,
    maxLng: Number(bounds.maxLng) + lngDelta
  };
}
function pointInPolygon(point, polygon) {
  const lat = Number(point[0]);
  const lng = Number(point[1]);
  let inside = false;
  for (let i = 0, j = polygon.length - 1; i < polygon.length; j = i++) {
    const yi = polygon[i][0];
    const xi = polygon[i][1];
    const yj = polygon[j][0];
    const xj = polygon[j][1];
    const intersects = ((yi > lat) !== (yj > lat))
      && (lng < ((xj - xi) * (lat - yi) / ((yj - yi) || Number.EPSILON)) + xi);
    if (intersects) inside = !inside;
  }
  return inside;
}
function approxDistanceMeters(a, b) {
  if (!a || !b) return Infinity;
  const aLat = Number(a[0]);
  const aLng = Number(a[1]);
  const bLat = Number(b[0]);
  const bLng = Number(b[1]);
  if (!Number.isFinite(aLat) || !Number.isFinite(aLng) || !Number.isFinite(bLat) || !Number.isFinite(bLng)) return Infinity;
  const dy = (aLat - bLat) * 111320;
  const avgLat = (aLat + bLat) / 2;
  const dx = (aLng - bLng) * metersPerDegreeLngAtLat(avgLat);
  return Math.sqrt((dx * dx) + (dy * dy));
}
function normalizeZip(value) {
  const raw = asText(value, "");
  const digits = raw.replace(/\D/g, "");
  if (digits.length >= 5) return digits.slice(0, 5);
  return raw.toUpperCase();
}
function normalizeHouseForKey(value) {
  return asText(value, "").toLowerCase().replace(/[^a-z0-9-]+/g, "");
}
function normalizeStreetForKey(value) {
  return asText(value, "").toLowerCase().replace(/[^a-z0-9]+/g, " ").trim();
}
function normalizeUnitForKey(value) {
  return asText(value, "").toLowerCase().replace(/^(apt|apartment|unit|suite|ste|#)\s*/i, "").replace(/[^a-z0-9]+/g, "");
}
function addressKeyParts(houseNumber, street, postcode) {
  return {
    house: normalizeHouseForKey(houseNumber),
    street: normalizeStreetForKey(street),
    postcode: normalizeZip(postcode)
  };
}
function addressKey(houseNumber, street, postcode) {
  const parts = addressKeyParts(houseNumber, street, postcode);
  return `${parts.house}|${parts.street}|${parts.postcode}`;
}
function hasAnyText(value) {
  return !!asText(value, "");
}
function parsePossibleLatLng(value) {
  if (value && typeof value === "object" && !Array.isArray(value)) {
    const lat = Number(value.lat ?? value.latitude);
    const lng = Number(value.lng ?? value.lon ?? value.longitude);
    if (Number.isFinite(lat) && Number.isFinite(lng) && Math.abs(lat) <= 90 && Math.abs(lng) <= 180) return [lat, lng];
  }
  if (!Array.isArray(value)) return null;
  if (value.length >= 2) {
    const a = Number(value[0]);
    const b = Number(value[1]);
    if (Number.isFinite(a) && Number.isFinite(b) && Math.abs(a) <= 90 && Math.abs(b) <= 180) return [a, b];
    if (Number.isFinite(a) && Number.isFinite(b) && Math.abs(b) <= 90 && Math.abs(a) <= 180) return [b, a];
  }
  return null;
}
function parseSocrataPoint(value) {
  if (!value) return null;
  if (typeof value === "object") {
    if (Array.isArray(value.coordinates) && value.coordinates.length >= 2) {
      const lng = Number(value.coordinates[0]);
      const lat = Number(value.coordinates[1]);
      if (Number.isFinite(lat) && Number.isFinite(lng) && Math.abs(lat) <= 90 && Math.abs(lng) <= 180) return [lat, lng];
    }
    const nested = parsePossibleLatLng(value);
    if (nested) return nested;
    if (typeof value.latitude !== "undefined" || typeof value.longitude !== "undefined") {
      const lat = Number(value.latitude);
      const lng = Number(value.longitude);
      if (Number.isFinite(lat) && Number.isFinite(lng) && Math.abs(lat) <= 90 && Math.abs(lng) <= 180) return [lat, lng];
    }
  }
  const raw = asText(value, "");
  if (!raw) return null;
  const pointMatch = raw.match(/POINT\s*\(\s*(-?\d+(?:\.\d+)?)\s+(-?\d+(?:\.\d+)?)\s*\)/i);
  if (pointMatch) {
    const lng = Number(pointMatch[1]);
    const lat = Number(pointMatch[2]);
    if (Number.isFinite(lat) && Number.isFinite(lng) && Math.abs(lat) <= 90 && Math.abs(lng) <= 180) return [lat, lng];
  }
  const pairMatch = raw.match(/^\s*(-?\d+(?:\.\d+)?)\s*[,\s]\s*(-?\d+(?:\.\d+)?)\s*$/);
  if (pairMatch) {
    const a = Number(pairMatch[1]);
    const b = Number(pairMatch[2]);
    if (Number.isFinite(a) && Number.isFinite(b)) {
      if (Math.abs(a) <= 90 && Math.abs(b) <= 180) return [a, b];
      if (Math.abs(b) <= 90 && Math.abs(a) <= 180) return [b, a];
    }
  }
  return null;
}
function parseHouseStreet(value) {
  const text = asText(value, "");
  if (!text) return { house_number: "", street: "" };
  const cleaned = text.split(",")[0].replace(/\s+/g, " ").trim();
  const match = cleaned.match(/^([0-9A-Za-z-]+)\s+(.+)$/);
  if (!match) return { house_number: "", street: cleaned };
  return {
    house_number: asText(match[1], ""),
    street: asText(match[2], "")
  };
}
function readFieldByName(row, key) {
  if (!row || typeof row !== "object") return "";
  const name = asText(key, "");
  if (!name) return "";
  if (Object.prototype.hasOwnProperty.call(row, name)) return row[name];
  const lowerName = name.toLowerCase();
  const entry = Object.entries(row).find(([k]) => asText(k, "").toLowerCase() === lowerName);
  return entry ? entry[1] : "";
}
function parseSocrataIsoTime(value) {
  const text = asText(value, "");
  if (!text) return "";
  const ts = Date.parse(text);
  if (!Number.isFinite(ts)) return "";
  return new Date(ts).toISOString();
}
function buildSocrataUrl(source, watermark = "", offset = 0, limit = UNIT_SYNC_BATCH_SIZE) {
  const scheme = asText(source.scheme || "https", "https");
  const domain = asText(source.domain, "data.cityofnewyork.us");
  const datasetId = asText(source.datasetId, "");
  if (!datasetId) throw new Error("Missing source datasetId.");
  const updatedField = asText(source.fieldMap && source.fieldMap.updatedAt, ":updated_at");
  const unitField = asText(source.fieldMap && source.fieldMap.unit, "");
  const clauses = [];
  if (unitField) clauses.push(`${unitField} is not null`, `${unitField} != ''`);
  if (watermark && updatedField) clauses.push(`${updatedField} > '${watermark.replace(/'/g, "''")}'`);
  const where = clauses.length ? clauses.join(" AND ") : "";
  const params = new URLSearchParams();
  params.set("$limit", String(Math.max(1, Math.min(50000, Number(limit) || UNIT_SYNC_BATCH_SIZE))));
  params.set("$offset", String(Math.max(0, Number(offset) || 0)));
  if (updatedField) params.set("$order", `${updatedField} ASC`);
  if (where) params.set("$where", where);
  return `${scheme}://${domain}/resource/${datasetId}.json?${params.toString()}`;
}

function parseWktRingCoords(ringText) {
  const coords = [];
  String(ringText || "")
    .split(",")
    .forEach(token => {
      const parts = String(token || "").trim().split(/\s+/).filter(Boolean);
      if (parts.length < 2) return;
      const lng = Number(parts[0]);
      const lat = Number(parts[1]);
      if (!Number.isFinite(lat) || !Number.isFinite(lng)) return;
      if (Math.abs(lat) > 90 || Math.abs(lng) > 180) return;
      coords.push([lat, lng]);
    });
  if (coords.length >= 3) {
    const first = coords[0];
    const last = coords[coords.length - 1];
    if (Math.abs(first[0] - last[0]) < 1e-12 && Math.abs(first[1] - last[1]) < 1e-12) coords.pop();
  }
  return coords.length >= 3 ? coords : [];
}
function parsePolygonRingsText(text) {
  return String(text || "")
    .split(/\)\s*,\s*\(/g)
    .map(parseWktRingCoords)
    .filter(ring => ring.length >= 3);
}
function parseBuildingGeometryWkt(geomWkt) {
  const raw = String(geomWkt || "").trim();
  if (!raw) return null;
  let polygonRingSets = [];
  if (/^POLYGON\s*\(\(/i.test(raw)) {
    const match = raw.match(/^POLYGON\s*\(\(([\s\S]+)\)\)$/i);
    if (!match) return null;
    const rings = parsePolygonRingsText(match[1]);
    if (!rings.length) return null;
    polygonRingSets = [rings];
  } else if (/^MULTIPOLYGON\s*\(\(\(/i.test(raw)) {
    const match = raw.match(/^MULTIPOLYGON\s*\(\(\(([\s\S]+)\)\)\)$/i);
    if (!match) return null;
    const polygonTexts = String(match[1] || "").split(/\)\)\s*,\s*\(\(/g);
    polygonRingSets = polygonTexts.map(parsePolygonRingsText).filter(rings => rings.length >= 1);
    if (!polygonRingSets.length) return null;
  } else {
    return null;
  }
  let minLat = Infinity;
  let maxLat = -Infinity;
  let minLng = Infinity;
  let maxLng = -Infinity;
  const polygons = [];
  polygonRingSets.forEach(rings => {
    const outer = rings[0];
    const holes = rings.slice(1);
    outer.forEach(([lat, lng]) => {
      if (lat < minLat) minLat = lat;
      if (lat > maxLat) maxLat = lat;
      if (lng < minLng) minLng = lng;
      if (lng > maxLng) maxLng = lng;
    });
    polygons.push({ outer, holes });
  });
  if (!Number.isFinite(minLat) || !Number.isFinite(maxLat) || !Number.isFinite(minLng) || !Number.isFinite(maxLng)) return null;
  return { polygons, bounds: { minLat, maxLat, minLng, maxLng } };
}
function normalizeBuildingClass(value) {
  return String(value || "").trim().toLowerCase();
}
function isResidentialBuildingClass(buildingClass) {
  const normalized = normalizeBuildingClass(buildingClass);
  if (!normalized) return false;
  if (RESIDENTIAL_BUILDING_TYPES.has(normalized)) return true;
  return normalized.includes("residential");
}
function isPointInsideBuildingGeometry(point, buildingGeometry) {
  if (!point || !buildingGeometry || !Array.isArray(buildingGeometry.polygons)) return false;
  for (const polygon of buildingGeometry.polygons) {
    if (!polygon || !Array.isArray(polygon.outer) || polygon.outer.length < 3) continue;
    if (!pointInPolygon(point, polygon.outer)) continue;
    const holes = Array.isArray(polygon.holes) ? polygon.holes : [];
    let inHole = false;
    for (const hole of holes) {
      if (Array.isArray(hole) && hole.length >= 3 && pointInPolygon(point, hole)) {
        inHole = true;
        break;
      }
    }
    if (!inHole) return true;
  }
  return false;
}

function isRetryableFetchError(error) {
  const message = String((error && error.message) || error || "").toLowerCase();
  if (!message) return false;
  if (/timeout|network|failed to fetch|econn|socket|connection|temporar|abort/.test(message)) return true;
  if (/http_408|http_425|http_429|http_500|http_502|http_503|http_504/.test(message)) return true;
  return false;
}
async function fetchWithTimeout(url, timeoutMs, options = {}) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), Math.max(1, Number(timeoutMs) || DOWNLOAD_TIMEOUT_MS));
  try {
    return await fetch(url, { ...options, signal: controller.signal });
  } finally {
    clearTimeout(timer);
  }
}
async function fetchJsonWithRetry(url, timeoutMs = DOWNLOAD_TIMEOUT_MS) {
  let lastError = null;
  for (let attempt = 1; attempt <= DOWNLOAD_RETRIES; attempt += 1) {
    try {
      const response = await fetchWithTimeout(url, timeoutMs);
      if (!response || typeof response.ok !== "boolean") throw new Error("invalid_response");
      if (!response.ok) throw new Error(`http_${response.status || "error"}`);
      return await response.json();
    } catch (error) {
      lastError = error;
      if (attempt >= DOWNLOAD_RETRIES || !isRetryableFetchError(error)) break;
      await sleep(Math.min(4000, RETRY_BASE_MS * (2 ** (attempt - 1))));
    }
  }
  throw lastError || new Error("fetch_failed");
}
async function fetchTextWithRetry(url, timeoutMs = DOWNLOAD_TIMEOUT_MS) {
  let lastError = null;
  for (let attempt = 1; attempt <= DOWNLOAD_RETRIES; attempt += 1) {
    try {
      const response = await fetchWithTimeout(url, timeoutMs);
      if (!response || typeof response.ok !== "boolean") throw new Error("invalid_response");
      if (!response.ok) throw new Error(`http_${response.status || "error"}`);
      return await response.text();
    } catch (error) {
      lastError = error;
      if (attempt >= DOWNLOAD_RETRIES || !isRetryableFetchError(error)) break;
      await sleep(Math.min(4000, RETRY_BASE_MS * (2 ** (attempt - 1))));
    }
  }
  throw lastError || new Error("fetch_failed");
}

function normalizeManifestChunk(chunk) {
  if (typeof chunk === "string") {
    const url = asText(chunk, "");
    if (!url) return null;
    return { url, count: 0 };
  }
  if (!chunk || typeof chunk !== "object") return null;
  const url = asText(chunk.url || chunk.path || chunk.file || "", "");
  if (!url) return null;
  return { url, count: Math.max(0, Number(chunk.count) || 0) };
}
function normalizeManifestDataset(dataset, fallbackEntry = null) {
  const source = dataset && typeof dataset === "object" ? dataset : {};
  const fallback = fallbackEntry && typeof fallbackEntry === "object" ? fallbackEntry : {};
  const chunks = (Array.isArray(source.chunks) ? source.chunks : Array.isArray(fallback.chunks) ? fallback.chunks : [])
    .map(normalizeManifestChunk)
    .filter(Boolean);
  return {
    format: asText(source.format || fallback.format || "ndjson", "ndjson"),
    count: Math.max(0, Number(source.count || source.rowCount || fallback.count || fallback.rowCount || 0) || 0),
    chunks
  };
}
function getStatePackageFromManifest(manifest, stateCode) {
  if (!manifest || typeof manifest !== "object") return null;
  const normalizedState = normalizeStateCode(stateCode);
  if (!normalizedState) return null;
  const rootRelease = asText(manifest.release || "", "");
  const states = manifest.states && typeof manifest.states === "object" && !Array.isArray(manifest.states)
    ? manifest.states
    : {};
  const entry = states[normalizedState] || states[normalizedState.toLowerCase()] || null;
  if (!entry || typeof entry !== "object") return null;
  const release = asText(entry.release || rootRelease || "", "");
  if (!release) return null;
  const datasets = entry.datasets && typeof entry.datasets === "object" ? entry.datasets : {};
  return {
    state: normalizedState,
    release,
    datasets: {
      addresses: normalizeManifestDataset(datasets.addresses, entry),
      buildings: normalizeManifestDataset(datasets.buildings, null)
    }
  };
}
function resolveChunkUrl(chunkUrl, manifestUrl) {
  const raw = asText(chunkUrl, "");
  if (!raw) return "";
  try {
    return new URL(raw, manifestUrl).toString();
  } catch {
    return raw;
  }
}
async function loadManifest() {
  if (PACKAGE_MANIFEST_URL) {
    try {
      const manifest = await fetchJsonWithRetry(PACKAGE_MANIFEST_URL, DOWNLOAD_TIMEOUT_MS);
      if (manifest && typeof manifest === "object") return { manifest, manifestUrl: PACKAGE_MANIFEST_URL, source: "remote" };
    } catch (_) {}
  }
  if (!fs.existsSync(PACKAGE_MANIFEST_PATH)) {
    throw new Error("Unable to load package manifest from remote host or local file.");
  }
  const text = fs.readFileSync(PACKAGE_MANIFEST_PATH, "utf8");
  const manifest = JSON.parse(text);
  return {
    manifest,
    manifestUrl: `file://${PACKAGE_MANIFEST_PATH.replace(/\\/g, "/")}`,
    source: "local-file"
  };
}
function forEachNdjsonLine(text, onLine) {
  if (typeof onLine !== "function") return;
  const raw = String(text || "");
  if (!raw) return;
  let lineStart = 0;
  for (let i = 0; i <= raw.length; i += 1) {
    const isLineBreak = i === raw.length || raw[i] === "\n" || raw[i] === "\r";
    if (!isLineBreak) continue;
    const line = raw.slice(lineStart, i).trim();
    if (line) onLine(line);
    if (raw[i] === "\r" && raw[i + 1] === "\n") i += 1;
    lineStart = i + 1;
  }
}

ensureDir(path.dirname(CACHE_DB_PATH));
const db = new DatabaseSync(CACHE_DB_PATH);
db.exec("PRAGMA journal_mode = WAL;");
db.exec("PRAGMA synchronous = NORMAL;");
db.exec("PRAGMA temp_store = MEMORY;");
db.exec(`
  CREATE TABLE IF NOT EXISTS addresses (
    state TEXT NOT NULL,
    release_id TEXT NOT NULL,
    id TEXT NOT NULL,
    source_dataset TEXT,
    house_number TEXT,
    street TEXT,
    unit TEXT,
    city TEXT,
    region TEXT,
    postcode TEXT,
    country_code TEXT,
    full_address TEXT,
    lat REAL NOT NULL,
    lng REAL NOT NULL,
    tile_key TEXT NOT NULL,
    PRIMARY KEY (state, release_id, id)
  );
  CREATE TABLE IF NOT EXISTS buildings (
    state TEXT NOT NULL,
    release_id TEXT NOT NULL,
    id TEXT NOT NULL,
    source_dataset TEXT,
    building_class TEXT,
    levels TEXT,
    name TEXT,
    geom_wkt TEXT NOT NULL,
    min_lat REAL NOT NULL,
    max_lat REAL NOT NULL,
    min_lng REAL NOT NULL,
    max_lng REAL NOT NULL,
    tile_key TEXT NOT NULL,
    PRIMARY KEY (state, release_id, id)
  );
  CREATE TABLE IF NOT EXISTS dataset_state (
    state TEXT PRIMARY KEY,
    release_id TEXT,
    pinned_release TEXT,
    phase TEXT NOT NULL DEFAULT 'idle',
    phase_detail TEXT,
    progress_current INTEGER NOT NULL DEFAULT 0,
    progress_total INTEGER NOT NULL DEFAULT 0,
    strict_residential_ready INTEGER NOT NULL DEFAULT 0,
    addresses_installed INTEGER NOT NULL DEFAULT 0,
    buildings_installed INTEGER NOT NULL DEFAULT 0,
    addresses_count INTEGER NOT NULL DEFAULT 0,
    buildings_count INTEGER NOT NULL DEFAULT 0,
    error TEXT,
    last_job_id TEXT,
    install_started_at TEXT,
    install_completed_at TEXT,
    backfill_completed_release TEXT,
    updated_at TEXT NOT NULL
  );
  CREATE TABLE IF NOT EXISTS unit_address_evidence (
    state TEXT NOT NULL,
    source_id TEXT NOT NULL,
    source_record_id TEXT NOT NULL,
    house_number TEXT,
    street TEXT,
    unit TEXT,
    city TEXT,
    region TEXT,
    postcode TEXT,
    lat REAL,
    lng REAL,
    geom_wkt TEXT,
    building_id TEXT,
    confidence REAL NOT NULL DEFAULT 0,
    observed_at TEXT,
    updated_at TEXT,
    raw_json TEXT,
    house_norm TEXT,
    street_norm TEXT,
    postcode_norm TEXT,
    unit_norm TEXT,
    PRIMARY KEY (state, source_id, source_record_id)
  );
  CREATE TABLE IF NOT EXISTS unit_source_state (
    source_id TEXT PRIMARY KEY,
    state TEXT NOT NULL,
    watermark TEXT,
    last_sync_started_at TEXT,
    last_sync_completed_at TEXT,
    last_status TEXT,
    last_error TEXT,
    rows_scanned INTEGER NOT NULL DEFAULT 0,
    rows_upserted INTEGER NOT NULL DEFAULT 0
  );
  CREATE INDEX IF NOT EXISTS idx_addresses_state_release_tile ON addresses(state, release_id, tile_key);
  CREATE INDEX IF NOT EXISTS idx_addresses_state_release_lat_lng ON addresses(state, release_id, lat, lng);
  CREATE INDEX IF NOT EXISTS idx_buildings_state_release_tile ON buildings(state, release_id, tile_key);
  CREATE INDEX IF NOT EXISTS idx_buildings_state_release_bbox ON buildings(state, release_id, min_lat, max_lat, min_lng, max_lng);
  CREATE INDEX IF NOT EXISTS idx_unit_ev_state_addr ON unit_address_evidence(state, postcode_norm, street_norm, house_norm, unit_norm);
  CREATE INDEX IF NOT EXISTS idx_unit_ev_state_lat_lng ON unit_address_evidence(state, lat, lng);
  CREATE INDEX IF NOT EXISTS idx_unit_ev_state_building ON unit_address_evidence(state, building_id);
  CREATE INDEX IF NOT EXISTS idx_unit_source_state_state ON unit_source_state(state);
`);

const q = {
  getState: db.prepare(`SELECT state, release_id, pinned_release, phase, phase_detail, progress_current, progress_total, strict_residential_ready, addresses_installed, buildings_installed, addresses_count, buildings_count, error, last_job_id, install_started_at, install_completed_at, backfill_completed_release, updated_at FROM dataset_state WHERE state = ?`),
  putState: db.prepare(`INSERT INTO dataset_state (state, release_id, pinned_release, phase, phase_detail, progress_current, progress_total, strict_residential_ready, addresses_installed, buildings_installed, addresses_count, buildings_count, error, last_job_id, install_started_at, install_completed_at, backfill_completed_release, updated_at) VALUES (@state,@release_id,@pinned_release,@phase,@phase_detail,@progress_current,@progress_total,@strict_residential_ready,@addresses_installed,@buildings_installed,@addresses_count,@buildings_count,@error,@last_job_id,@install_started_at,@install_completed_at,@backfill_completed_release,@updated_at) ON CONFLICT(state) DO UPDATE SET release_id=excluded.release_id,pinned_release=excluded.pinned_release,phase=excluded.phase,phase_detail=excluded.phase_detail,progress_current=excluded.progress_current,progress_total=excluded.progress_total,strict_residential_ready=excluded.strict_residential_ready,addresses_installed=excluded.addresses_installed,buildings_installed=excluded.buildings_installed,addresses_count=excluded.addresses_count,buildings_count=excluded.buildings_count,error=excluded.error,last_job_id=excluded.last_job_id,install_started_at=excluded.install_started_at,install_completed_at=excluded.install_completed_at,backfill_completed_release=excluded.backfill_completed_release,updated_at=excluded.updated_at`),
  delAddresses: db.prepare(`DELETE FROM addresses WHERE state = ?`),
  delBuildings: db.prepare(`DELETE FROM buildings WHERE state = ?`),
  delUnitEvidenceByState: db.prepare(`DELETE FROM unit_address_evidence WHERE state = ?`),
  insAddress: db.prepare(`INSERT INTO addresses (state, release_id, id, source_dataset, house_number, street, unit, city, region, postcode, country_code, full_address, lat, lng, tile_key) VALUES (@state, @release_id, @id, @source_dataset, @house_number, @street, @unit, @city, @region, @postcode, @country_code, @full_address, @lat, @lng, @tile_key) ON CONFLICT(state, release_id, id) DO UPDATE SET source_dataset=excluded.source_dataset, house_number=excluded.house_number, street=excluded.street, unit=excluded.unit, city=excluded.city, region=excluded.region, postcode=excluded.postcode, country_code=excluded.country_code, full_address=excluded.full_address, lat=excluded.lat, lng=excluded.lng, tile_key=excluded.tile_key`),
  insBuilding: db.prepare(`INSERT INTO buildings (state, release_id, id, source_dataset, building_class, levels, name, geom_wkt, min_lat, max_lat, min_lng, max_lng, tile_key) VALUES (@state, @release_id, @id, @source_dataset, @building_class, @levels, @name, @geom_wkt, @min_lat, @max_lat, @min_lng, @max_lng, @tile_key) ON CONFLICT(state, release_id, id) DO UPDATE SET source_dataset=excluded.source_dataset, building_class=excluded.building_class, levels=excluded.levels, name=excluded.name, geom_wkt=excluded.geom_wkt, min_lat=excluded.min_lat, max_lat=excluded.max_lat, min_lng=excluded.min_lng, max_lng=excluded.max_lng, tile_key=excluded.tile_key`),
  insUnitEvidence: db.prepare(`INSERT INTO unit_address_evidence (state, source_id, source_record_id, house_number, street, unit, city, region, postcode, lat, lng, geom_wkt, building_id, confidence, observed_at, updated_at, raw_json, house_norm, street_norm, postcode_norm, unit_norm) VALUES (@state, @source_id, @source_record_id, @house_number, @street, @unit, @city, @region, @postcode, @lat, @lng, @geom_wkt, @building_id, @confidence, @observed_at, @updated_at, @raw_json, @house_norm, @street_norm, @postcode_norm, @unit_norm) ON CONFLICT(state, source_id, source_record_id) DO UPDATE SET house_number=excluded.house_number, street=excluded.street, unit=excluded.unit, city=excluded.city, region=excluded.region, postcode=excluded.postcode, lat=excluded.lat, lng=excluded.lng, geom_wkt=excluded.geom_wkt, building_id=excluded.building_id, confidence=excluded.confidence, observed_at=excluded.observed_at, updated_at=excluded.updated_at, raw_json=excluded.raw_json, house_norm=excluded.house_norm, street_norm=excluded.street_norm, postcode_norm=excluded.postcode_norm, unit_norm=excluded.unit_norm`),
  getUnitSourceState: db.prepare(`SELECT source_id, state, watermark, last_sync_started_at, last_sync_completed_at, last_status, last_error, rows_scanned, rows_upserted FROM unit_source_state WHERE source_id = ?`),
  putUnitSourceState: db.prepare(`INSERT INTO unit_source_state (source_id, state, watermark, last_sync_started_at, last_sync_completed_at, last_status, last_error, rows_scanned, rows_upserted) VALUES (@source_id, @state, @watermark, @last_sync_started_at, @last_sync_completed_at, @last_status, @last_error, @rows_scanned, @rows_upserted) ON CONFLICT(source_id) DO UPDATE SET state=excluded.state, watermark=excluded.watermark, last_sync_started_at=excluded.last_sync_started_at, last_sync_completed_at=excluded.last_sync_completed_at, last_status=excluded.last_status, last_error=excluded.last_error, rows_scanned=excluded.rows_scanned, rows_upserted=excluded.rows_upserted`),
  selUnitEvidenceInBounds: db.prepare(`SELECT state, source_id, source_record_id, house_number, street, unit, city, region, postcode, lat, lng, geom_wkt, building_id, confidence, observed_at, updated_at, raw_json, house_norm, street_norm, postcode_norm, unit_norm FROM unit_address_evidence WHERE state = ? AND lat IS NOT NULL AND lng IS NOT NULL AND lat >= ? AND lat <= ? AND lng >= ? AND lng <= ?`),
  selUnitEvidenceByAddress: db.prepare(`SELECT state, source_id, source_record_id, house_number, street, unit, city, region, postcode, lat, lng, geom_wkt, building_id, confidence, observed_at, updated_at, raw_json, house_norm, street_norm, postcode_norm, unit_norm FROM unit_address_evidence WHERE state = ? AND house_norm = ? AND street_norm = ? AND postcode_norm = ?`),
  countUnitEvidenceByState: db.prepare(`SELECT COUNT(*) AS count FROM unit_address_evidence WHERE state = ?`),
  selUnitSourceStatesByState: db.prepare(`SELECT source_id, state, watermark, last_sync_started_at, last_sync_completed_at, last_status, last_error, rows_scanned, rows_upserted FROM unit_source_state WHERE state = ? ORDER BY source_id ASC`),
  selAddressCandidates: db.prepare(`SELECT id, release_id, source_dataset, house_number, street, unit, city, region, postcode, country_code, full_address, lat, lng FROM addresses WHERE state = ? AND release_id = ? AND lat >= ? AND lat <= ? AND lng >= ? AND lng <= ? LIMIT ?`),
  selBuildingCandidates: db.prepare(`SELECT id, release_id, source_dataset, building_class, levels, name, geom_wkt, min_lat, max_lat, min_lng, max_lng FROM buildings WHERE state = ? AND release_id = ? AND min_lat <= ? AND max_lat >= ? AND min_lng <= ? AND max_lng >= ? LIMIT ?`)
};

function defaultStateRow(stateCode) {
  return {
    state: stateCode,
    release_id: "",
    pinned_release: "",
    phase: PHASES.idle,
    phase_detail: "",
    progress_current: 0,
    progress_total: 0,
    strict_residential_ready: 0,
    addresses_installed: 0,
    buildings_installed: 0,
    addresses_count: 0,
    buildings_count: 0,
    error: "",
    last_job_id: "",
    install_started_at: "",
    install_completed_at: "",
    backfill_completed_release: "",
    updated_at: nowIso()
  };
}
function readStateRow(stateCode) {
  const normalizedState = normalizeStateCode(stateCode);
  if (!normalizedState) return null;
  return q.getState.get(normalizedState) || defaultStateRow(normalizedState);
}
function writeStateRow(stateCode, patch = {}) {
  const normalizedState = normalizeStateCode(stateCode);
  if (!normalizedState) throw new Error("Invalid state code.");
  const next = {
    ...defaultStateRow(normalizedState),
    ...(readStateRow(normalizedState) || {}),
    ...(patch || {}),
    state: normalizedState,
    updated_at: nowIso()
  };
  q.putState.run(next);
  return next;
}
function statusPayload(stateCode) {
  const row = readStateRow(stateCode) || defaultStateRow(normalizeStateCode(stateCode));
  const total = Math.max(0, Number(row.progress_total) || 0);
  const current = Math.min(total || Number.MAX_SAFE_INTEGER, Math.max(0, Number(row.progress_current) || 0));
  const pct = total > 0 ? Math.round((current / total) * 100) : 0;
  return {
    ok: true,
    state: row.state,
    release: asText(row.release_id, ""),
    pinnedRelease: asText(row.pinned_release, ""),
    phase: asText(row.phase, PHASES.idle) || PHASES.idle,
    phaseDetail: asText(row.phase_detail, ""),
    progress: { current, total, pct },
    datasetsInstalled: {
      addresses: !!Number(row.addresses_installed),
      buildings: !!Number(row.buildings_installed)
    },
    datasetCounts: {
      addresses: Math.max(0, Number(row.addresses_count) || 0),
      buildings: Math.max(0, Number(row.buildings_count) || 0)
    },
    strictResidentialReady: !!Number(row.strict_residential_ready),
    error: asText(row.error, ""),
    installStartedAt: asText(row.install_started_at, ""),
    installCompletedAt: asText(row.install_completed_at, ""),
    backfillCompletedRelease: asText(row.backfill_completed_release, ""),
    jobId: asText(row.last_job_id, "")
  };
}

function tryParseJson(value, fallback = null) {
  if (value == null || value === "") return fallback;
  try {
    return JSON.parse(String(value));
  } catch {
    return fallback;
  }
}
function toIsoDate(value) {
  const text = asText(value, "");
  if (!text) return "";
  const ts = Date.parse(text);
  if (!Number.isFinite(ts)) return "";
  return new Date(ts).toISOString().slice(0, 10);
}
function isIsoDateFresher(nextDate, previousDate) {
  const nextTs = Date.parse(asText(nextDate, ""));
  const prevTs = Date.parse(asText(previousDate, ""));
  if (!Number.isFinite(nextTs)) return false;
  if (!Number.isFinite(prevTs)) return true;
  return nextTs >= prevTs;
}

function polygonAreaMetersSquared(polygon) {
  const coords = normalizePolygon(polygon);
  const centerLat = coords.reduce((sum, point) => sum + Number(point[0]), 0) / coords.length;
  const mPerLng = metersPerDegreeLngAtLat(centerLat);
  let sum = 0;
  for (let i = 0; i < coords.length; i += 1) {
    const j = (i + 1) % coords.length;
    const x1 = Number(coords[i][1]) * mPerLng;
    const y1 = Number(coords[i][0]) * 111320;
    const x2 = Number(coords[j][1]) * mPerLng;
    const y2 = Number(coords[j][0]) * 111320;
    sum += (x1 * y2) - (x2 * y1);
  }
  return Math.abs(sum) * 0.5;
}
function normalizeAlignmentOptions(raw = {}) {
  return {
    shared_edge_tolerance_m: Math.max(0.2, Number(raw.shared_edge_tolerance_m ?? raw.vertexToleranceMeters ?? 2.5) || 2.5),
    edge_tolerance_m: Math.max(0.2, Number(raw.edge_tolerance_m ?? raw.edgeToleranceMeters ?? 1.25) || 1.25),
    max_vertex_move_m: Math.max(0.5, Number(raw.max_vertex_move_m ?? raw.maxVertexMoveMeters ?? 6.0) || 6.0),
    max_area_drift_pct: Math.max(0.1, Number(raw.max_area_drift_pct ?? raw.maxAreaDriftPct ?? 1.5) || 1.5),
    min_shared_edge_len_m: Math.max(1, Number(raw.min_shared_edge_len_m ?? 8.0) || 8.0)
  };
}
function normalizeAlignmentTerritories(rawTerritories = []) {
  return (Array.isArray(rawTerritories) ? rawTerritories : [])
    .map((item, index) => {
      const territoryId = asText(item && (item.id || item.territoryId), `territory_${index + 1}`);
      let polygon = null;
      try {
        polygon = normalizePolygon(item && item.polygon);
      } catch {
        polygon = null;
      }
      if (!polygon || polygon.length < 3) return null;
      return {
        id: territoryId,
        polygon: polygon.map(point => [Number(point[0]), Number(point[1])])
      };
    })
    .filter(Boolean);
}
function cleanupAlignmentPreviews() {
  const now = Date.now();
  for (const [key, value] of alignmentPreviewJobs.entries()) {
    if (!value || Number(value.expiresAt) <= now) alignmentPreviewJobs.delete(key);
  }
}
function buildAlignmentPayloadHash(territories, options) {
  const normalizedTerritories = normalizeAlignmentTerritories(territories);
  const normalizedOptions = normalizeAlignmentOptions(options);
  const payload = JSON.stringify({ territories: normalizedTerritories, options: normalizedOptions });
  return crypto.createHash("sha1").update(payload).digest("hex");
}
function buildAlignmentPreview(territories, rawOptions = {}) {
  const items = normalizeAlignmentTerritories(territories);
  const options = normalizeAlignmentOptions(rawOptions);
  if (items.length < 2) {
    return {
      patches: [],
      summary: {
        changed: false,
        impactedTerritories: 0,
        verticesAligned: 0,
        insertedVertices: 0,
        revertedTerritories: 0,
        maxShiftMeters: 0,
        overlapGapBeforeSqM: null,
        overlapGapAfterSqM: null
      }
    };
  }

  const clones = items.map(item => ({
    id: item.id,
    polygon: item.polygon.map(point => [point[0], point[1]]),
    originalPolygon: item.polygon.map(point => [point[0], point[1]])
  }));
  const tolerance = Math.max(0.2, Number(options.shared_edge_tolerance_m) || 2.5);
  const maxMove = Math.max(tolerance, Number(options.max_vertex_move_m) || 6.0);
  const maxAreaDriftPct = Math.max(0.1, Number(options.max_area_drift_pct) || 1.5);
  const vertices = [];
  clones.forEach((territory, territoryIndex) => {
    territory.polygon.forEach((point, pointIndex) => {
      vertices.push({ territoryIndex, pointIndex, point });
    });
  });
  const parent = Array.from({ length: vertices.length }, (_, index) => index);
  const rank = Array.from({ length: vertices.length }, () => 0);
  const findRoot = (index) => {
    let cursor = index;
    while (parent[cursor] !== cursor) {
      parent[cursor] = parent[parent[cursor]];
      cursor = parent[cursor];
    }
    return cursor;
  };
  const union = (a, b) => {
    const rootA = findRoot(a);
    const rootB = findRoot(b);
    if (rootA === rootB) return;
    if (rank[rootA] < rank[rootB]) parent[rootA] = rootB;
    else if (rank[rootA] > rank[rootB]) parent[rootB] = rootA;
    else {
      parent[rootB] = rootA;
      rank[rootA] += 1;
    }
  };
  const toleranceSq = tolerance * tolerance;
  for (let i = 0; i < vertices.length; i += 1) {
    for (let j = i + 1; j < vertices.length; j += 1) {
      if (vertices[i].territoryIndex === vertices[j].territoryIndex) continue;
      const dist = approxDistanceMeters(vertices[i].point, vertices[j].point);
      if (!Number.isFinite(dist)) continue;
      if ((dist * dist) <= toleranceSq) union(i, j);
    }
  }

  const groups = new Map();
  for (let i = 0; i < vertices.length; i += 1) {
    const root = findRoot(i);
    const group = groups.get(root) || [];
    group.push(i);
    groups.set(root, group);
  }

  let verticesAligned = 0;
  let maxShiftMeters = 0;
  groups.forEach(indices => {
    if (!Array.isArray(indices) || indices.length < 2) return;
    const distinctTerritories = new Set(indices.map(index => vertices[index].territoryIndex));
    if (distinctTerritories.size < 2) return;
    const avgLat = indices.reduce((sum, index) => sum + Number(vertices[index].point[0]), 0) / indices.length;
    const avgLng = indices.reduce((sum, index) => sum + Number(vertices[index].point[1]), 0) / indices.length;
    indices.forEach(index => {
      const vertex = vertices[index];
      const territory = clones[vertex.territoryIndex];
      const current = territory.polygon[vertex.pointIndex];
      const shift = approxDistanceMeters(current, [avgLat, avgLng]);
      if (!Number.isFinite(shift) || shift > maxMove || shift < 0.02) return;
      territory.polygon[vertex.pointIndex] = [avgLat, avgLng];
      verticesAligned += 1;
      if (shift > maxShiftMeters) maxShiftMeters = shift;
    });
  });

  const patches = [];
  let revertedTerritories = 0;
  for (const territory of clones) {
    const before = territory.originalPolygon;
    const after = territory.polygon.map(point => [Number(point[0].toFixed(7)), Number(point[1].toFixed(7))]);
    const changed = before.length !== after.length || before.some((point, idx) => {
      const nextPoint = after[idx];
      if (!nextPoint) return true;
      return approxDistanceMeters(point, nextPoint) > 0.02;
    });
    if (!changed) continue;
    const beforeArea = Math.max(1, polygonAreaMetersSquared(before));
    const afterArea = Math.max(0, polygonAreaMetersSquared(after));
    const driftPct = Math.abs(afterArea - beforeArea) / beforeArea * 100;
    if (!Number.isFinite(driftPct) || driftPct > maxAreaDriftPct) {
      revertedTerritories += 1;
      continue;
    }
    patches.push({
      territoryId: territory.id,
      polygon: after,
      maxShiftMeters: Number(maxShiftMeters.toFixed(2))
    });
  }

  return {
    patches,
    summary: {
      changed: patches.length > 0,
      impactedTerritories: patches.length,
      verticesAligned,
      insertedVertices: 0,
      revertedTerritories,
      maxShiftMeters: Number(maxShiftMeters.toFixed(2)),
      overlapGapBeforeSqM: null,
      overlapGapAfterSqM: null
    }
  };
}
function createAlignmentPreview(territories, options = {}) {
  cleanupAlignmentPreviews();
  const normalizedOptions = normalizeAlignmentOptions(options);
  const { patches, summary } = buildAlignmentPreview(territories, normalizedOptions);
  const previewId = `align-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
  alignmentPreviewJobs.set(previewId, {
    previewId,
    createdAt: Date.now(),
    expiresAt: Date.now() + ALIGN_PREVIEW_TTL_MS,
    hash: buildAlignmentPayloadHash(territories, normalizedOptions),
    options: normalizedOptions,
    summary,
    patches
  });
  return {
    ok: true,
    previewId,
    confirmToken: previewId,
    summary,
    patches
  };
}
function applyAlignmentPreview(confirmToken, territories, options = {}) {
  cleanupAlignmentPreviews();
  const token = asText(confirmToken, "");
  if (!token) throw new Error("Missing confirmation token.");
  const job = alignmentPreviewJobs.get(token);
  if (!job) throw new Error("Alignment preview expired. Run preview again.");
  const incomingHash = buildAlignmentPayloadHash(territories, options);
  if (incomingHash !== asText(job.hash, "")) {
    throw new Error("Alignment input changed since preview. Run preview again.");
  }
  alignmentPreviewJobs.delete(token);
  return {
    ok: true,
    applied: true,
    summary: job.summary,
    patches: job.patches
  };
}

function defaultUnitSourceState(sourceId, stateCode) {
  return {
    source_id: asText(sourceId, ""),
    state: normalizeStateCode(stateCode) || "NY",
    watermark: "",
    last_sync_started_at: "",
    last_sync_completed_at: "",
    last_status: "idle",
    last_error: "",
    rows_scanned: 0,
    rows_upserted: 0
  };
}
function readUnitSourceState(sourceId, stateCode) {
  const source = asText(sourceId, "");
  if (!source) return defaultUnitSourceState("", stateCode);
  return q.getUnitSourceState.get(source) || defaultUnitSourceState(source, stateCode);
}
function writeUnitSourceState(sourceId, stateCode, patch = {}) {
  const source = asText(sourceId, "");
  if (!source) throw new Error("Invalid unit source id.");
  const next = {
    ...defaultUnitSourceState(source, stateCode),
    ...readUnitSourceState(source, stateCode),
    ...(patch || {}),
    source_id: source,
    state: normalizeStateCode(stateCode) || "NY"
  };
  q.putUnitSourceState.run(next);
  return next;
}
function loadUnitSourceProfile(forceReload = false) {
  const cacheAgeMs = Date.now() - Number(unitSourceProfileCache.loadedAt || 0);
  if (!forceReload && unitSourceProfileCache.profile && cacheAgeMs < 60_000) return unitSourceProfileCache.profile;
  if (!fs.existsSync(UNIT_SOURCE_PROFILE_PATH)) {
    unitSourceProfileCache = { loadedAt: Date.now(), profile: null };
    return null;
  }
  try {
    const raw = fs.readFileSync(UNIT_SOURCE_PROFILE_PATH, "utf8");
    const parsed = JSON.parse(raw);
    unitSourceProfileCache = { loadedAt: Date.now(), profile: parsed };
    return parsed;
  } catch (error) {
    console.warn(`[unit-sync] Unable to read profile ${UNIT_SOURCE_PROFILE_PATH}: ${String((error && error.message) || error)}`);
    unitSourceProfileCache = { loadedAt: Date.now(), profile: null };
    return null;
  }
}
function getConfiguredUnitSourcesForState(stateCode) {
  const state = normalizeStateCode(stateCode);
  if (!state) return [];
  const profile = loadUnitSourceProfile(false);
  if (!profile || typeof profile !== "object") return [];
  const sources = Array.isArray(profile.sources) ? profile.sources : [];
  return sources
    .filter(source => {
      if (!source || typeof source !== "object") return false;
      if (source.enabled === false) return false;
      const sourceState = normalizeStateCode(source.state || profile.state || "");
      return !sourceState || sourceState === state;
    })
    .map(source => ({
      ...source,
      sourceId: asText(source.sourceId || source.source_id || "", ""),
      datasetId: asText(source.datasetId || source.dataset_id || "", ""),
      domain: asText(source.domain || profile.domain || "data.cityofnewyork.us", "data.cityofnewyork.us"),
      scheme: asText(source.scheme || profile.scheme || "https", "https"),
      confidenceBase: Math.max(0, Math.min(0.99, Number(source.confidenceBase ?? source.confidence ?? 0.7) || 0.7)),
      fieldMap: source.fieldMap && typeof source.fieldMap === "object" ? source.fieldMap : {}
    }))
    .filter(source => !!source.sourceId && !!source.datasetId);
}
function isUnitSyncStale(lastSyncCompletedAt, force = false) {
  if (force) return true;
  const parsed = Date.parse(asText(lastSyncCompletedAt, ""));
  if (!Number.isFinite(parsed)) return true;
  return (Date.now() - parsed) >= UNIT_SYNC_STALE_MS;
}
function buildUnitEvidenceRecord(source, sourceRow, stateCode, releaseId = "") {
  const map = source && source.fieldMap && typeof source.fieldMap === "object" ? source.fieldMap : {};
  const addressText = readFieldByName(sourceRow, map.address);
  const parsedAddress = parseHouseStreet(addressText);
  const houseNumber = asText(readFieldByName(sourceRow, map.houseNumber) || parsedAddress.house_number, "");
  const street = asText(readFieldByName(sourceRow, map.street) || parsedAddress.street, "");
  const unit = asText(readFieldByName(sourceRow, map.unit), "");
  const postcode = normalizeZip(readFieldByName(sourceRow, map.postcode));
  const city = asText(readFieldByName(sourceRow, map.city), "");
  const region = normalizeStateCode(readFieldByName(sourceRow, map.region) || stateCode || "NY") || "NY";
  if (!unit) return null;
  if (!houseNumber && !street) return null;

  const latField = readFieldByName(sourceRow, map.lat);
  const lngField = readFieldByName(sourceRow, map.lng);
  const geomField = readFieldByName(sourceRow, map.geom || map.geometry || map.point);
  const latText = asText(latField, "");
  const lngText = asText(lngField, "");
  let lat = latText ? Number(latText) : Number.NaN;
  let lng = lngText ? Number(lngText) : Number.NaN;
  if (!Number.isFinite(lat) || !Number.isFinite(lng)) {
    const parsedPoint = parseSocrataPoint(geomField);
    if (parsedPoint) {
      lat = parsedPoint[0];
      lng = parsedPoint[1];
    }
  }
  const hasPoint = Number.isFinite(lat) && Number.isFinite(lng) && Math.abs(lat) <= 90 && Math.abs(lng) <= 180;
  const sourceRecordIdRaw = readFieldByName(sourceRow, map.sourceRecordId || map.source_record_id || ":id");
  const sourceRecordId = asText(
    sourceRecordIdRaw,
    `rec_${crypto.createHash("sha1").update(JSON.stringify(sourceRow || {})).digest("hex").slice(0, 20)}`
  );
  const updatedAt = parseSocrataIsoTime(readFieldByName(sourceRow, map.updatedAt || ":updated_at"));
  const observedAt = updatedAt || nowIso();
  const confidenceBase = Math.max(0, Math.min(0.99, Number(source.confidenceBase) || 0.7));
  return {
    state: stateCode,
    source_id: source.sourceId,
    source_record_id: sourceRecordId,
    house_number: houseNumber,
    street,
    unit,
    city,
    region,
    postcode,
    lat: hasPoint ? lat : null,
    lng: hasPoint ? lng : null,
    geom_wkt: hasPoint ? `POINT (${Number(lng).toFixed(7)} ${Number(lat).toFixed(7)})` : "",
    building_id: "",
    confidence: confidenceBase,
    observed_at: observedAt,
    updated_at: updatedAt || observedAt,
    raw_json: JSON.stringify(sourceRow || {}, null, 0),
    house_norm: normalizeHouseForKey(houseNumber),
    street_norm: normalizeStreetForKey(street),
    postcode_norm: normalizeZip(postcode),
    unit_norm: normalizeUnitForKey(unit)
  };
}
function getBuildingGeometryCached(building) {
  const key = `${asText(building && building.id, "")}|${asText(building && building.release_id, "")}`;
  if (!key) return null;
  if (buildingGeometryCache.has(key)) return buildingGeometryCache.get(key) || null;
  const parsed = parseBuildingGeometryWkt(building && building.geom_wkt);
  buildingGeometryCache.set(key, parsed || null);
  return parsed || null;
}
function matchResidentialBuildingForEvidence(stateCode, releaseId, lat, lng) {
  if (!Number.isFinite(lat) || !Number.isFinite(lng) || !releaseId) return { buildingId: "", inside: false };
  const latPad = UNIT_POINT_MATCH_METERS / 111320;
  const lngPad = UNIT_POINT_MATCH_METERS / metersPerDegreeLngAtLat(lat);
  const candidates = q.selBuildingCandidates.all(
    stateCode,
    releaseId,
    lat + latPad,
    lat - latPad,
    lng + lngPad,
    lng - lngPad,
    2000
  );
  if (!Array.isArray(candidates) || !candidates.length) return { buildingId: "", inside: false };
  let nearest = null;
  for (const building of candidates) {
    if (!building || !isResidentialBuildingClass(building.building_class)) continue;
    const parsed = getBuildingGeometryCached(building);
    if (!parsed) continue;
    if (isPointInsideBuildingGeometry([lat, lng], parsed)) return { buildingId: asText(building.id, ""), inside: true };
    const center = [
      ((Number(building.min_lat) || lat) + (Number(building.max_lat) || lat)) / 2,
      ((Number(building.min_lng) || lng) + (Number(building.max_lng) || lng)) / 2
    ];
    const dist = approxDistanceMeters([lat, lng], center);
    if (!Number.isFinite(dist)) continue;
    if (!nearest || dist < nearest.distanceMeters) {
      nearest = { id: asText(building.id, ""), distanceMeters: dist };
    }
  }
  if (nearest && nearest.distanceMeters <= UNIT_POINT_MATCH_METERS) {
    return { buildingId: nearest.id, inside: false };
  }
  return { buildingId: "", inside: false };
}
async function fetchJsonWithRetryAdvanced(url, options = {}) {
  const retries = Math.max(1, Number(options.retries) || UNIT_SYNC_MAX_RETRIES);
  const timeoutMs = Math.max(5000, Number(options.timeoutMs) || DOWNLOAD_TIMEOUT_MS);
  let lastError = null;
  for (let attempt = 1; attempt <= retries; attempt += 1) {
    try {
      const response = await fetchWithTimeout(url, timeoutMs, options.fetchOptions || {});
      if (!response || typeof response.ok !== "boolean") throw new Error("invalid_response");
      if (!response.ok) throw new Error(`http_${response.status || "error"}`);
      return await response.json();
    } catch (error) {
      lastError = error;
      if (attempt >= retries || !isRetryableFetchError(error)) break;
      await sleep(Math.min(10_000, UNIT_SYNC_RETRY_BASE_MS * (2 ** (attempt - 1))));
    }
  }
  throw lastError || new Error("fetch_failed");
}
async function syncSingleUnitSource(stateCode, releaseId, source, options = {}) {
  const state = normalizeStateCode(stateCode);
  if (!state) throw new Error("Invalid state code.");
  const sourceId = asText(source && source.sourceId, "");
  if (!sourceId) throw new Error("Invalid unit source.");
  const previous = readUnitSourceState(sourceId, state);
  if (!isUnitSyncStale(previous.last_sync_completed_at, options.force === true)) {
    return {
      sourceId,
      status: "cached",
      skipped: true,
      rowsScanned: Math.max(0, Number(previous.rows_scanned) || 0),
      rowsUpserted: Math.max(0, Number(previous.rows_upserted) || 0),
      watermark: asText(previous.watermark, ""),
      lastSyncCompletedAt: asText(previous.last_sync_completed_at, "")
    };
  }

  writeUnitSourceState(sourceId, state, {
    last_status: "running",
    last_error: "",
    last_sync_started_at: nowIso(),
    rows_scanned: 0,
    rows_upserted: 0
  });

  let offset = 0;
  let scanned = 0;
  let upserted = 0;
  let maxWatermark = asText(previous.watermark, "");
  const startedAt = nowIso();
  const watermark = asText(previous.watermark, "");
  const sourceUpdatedAtKey = asText(source.fieldMap && source.fieldMap.updatedAt, ":updated_at");
  const headers = {};
  const appToken = asText(process.env.NYC_OPEN_DATA_APP_TOKEN || "", "");
  if (appToken) headers["X-App-Token"] = appToken;

  while (true) {
    const url = buildSocrataUrl(source, watermark, offset, UNIT_SYNC_BATCH_SIZE);
    const rows = await fetchJsonWithRetryAdvanced(url, {
      retries: UNIT_SYNC_MAX_RETRIES,
      timeoutMs: DOWNLOAD_TIMEOUT_MS,
      fetchOptions: { headers }
    });
    if (!Array.isArray(rows) || !rows.length) break;

    db.exec("BEGIN;");
    try {
      for (const row of rows) {
        scanned += 1;
        const evidence = buildUnitEvidenceRecord(source, row, state, releaseId);
        const rowUpdated = parseSocrataIsoTime(readFieldByName(row, sourceUpdatedAtKey));
        if (rowUpdated && (!maxWatermark || rowUpdated > maxWatermark)) maxWatermark = rowUpdated;
        if (!evidence) continue;
        if (Number.isFinite(Number(evidence.lat)) && Number.isFinite(Number(evidence.lng))) {
          const matched = matchResidentialBuildingForEvidence(state, releaseId, Number(evidence.lat), Number(evidence.lng));
          if (matched && matched.buildingId) {
            evidence.building_id = matched.buildingId;
            evidence.confidence = Math.min(0.99, Number(evidence.confidence || 0) + 0.10);
          }
        }
        q.insUnitEvidence.run(evidence);
        upserted += 1;
      }
      db.exec("COMMIT;");
    } catch (error) {
      db.exec("ROLLBACK;");
      throw error;
    }
    offset += rows.length;
    await sleep(0);
    if (rows.length < UNIT_SYNC_BATCH_SIZE) break;
  }

  const completedAt = nowIso();
  writeUnitSourceState(sourceId, state, {
    watermark: maxWatermark || watermark,
    last_sync_started_at: startedAt,
    last_sync_completed_at: completedAt,
    last_status: "success",
    last_error: "",
    rows_scanned: scanned,
    rows_upserted: upserted
  });
  return {
    sourceId,
    status: "success",
    skipped: false,
    rowsScanned: scanned,
    rowsUpserted: upserted,
    watermark: maxWatermark || watermark,
    lastSyncCompletedAt: completedAt
  };
}
async function runUnitSyncForState(stateCode, options = {}) {
  const state = normalizeStateCode(stateCode);
  if (!state) throw new Error("Missing or invalid state.");
  if (!SUPPORTED_STATES.has(state)) throw new Error(`State package not yet published for ${state}.`);
  const configuredSources = getConfiguredUnitSourcesForState(state);
  if (!configuredSources.length) {
    return {
      ok: true,
      state,
      startedAt: nowIso(),
      completedAt: nowIso(),
      sources: [],
      rowsScanned: 0,
      rowsUpserted: 0,
      errors: []
    };
  }
  const stateRow = readStateRow(state);
  const releaseId = asText(stateRow && stateRow.release_id, "");
  const startedAt = nowIso();
  const sourceResults = [];
  const errors = [];
  let rowsScanned = 0;
  let rowsUpserted = 0;
  for (const source of configuredSources) {
    try {
      const result = await syncSingleUnitSource(state, releaseId, source, options);
      sourceResults.push(result);
      rowsScanned += Math.max(0, Number(result.rowsScanned) || 0);
      rowsUpserted += Math.max(0, Number(result.rowsUpserted) || 0);
    } catch (error) {
      const message = String((error && error.message) || error || "unit_sync_failed");
      writeUnitSourceState(source.sourceId, state, {
        last_status: "error",
        last_error: message,
        last_sync_completed_at: nowIso()
      });
      sourceResults.push({
        sourceId: source.sourceId,
        status: "error",
        skipped: false,
        rowsScanned: 0,
        rowsUpserted: 0,
        watermark: asText(readUnitSourceState(source.sourceId, state).watermark, ""),
        error: message
      });
      errors.push({ sourceId: source.sourceId, error: message });
    }
  }
  return {
    ok: true,
    state,
    startedAt,
    completedAt: nowIso(),
    sources: sourceResults,
    rowsScanned,
    rowsUpserted,
    errors
  };
}
function ensureUnitSyncJob(stateCode, options = {}) {
  const state = normalizeStateCode(stateCode);
  if (!state) throw new Error("Invalid state code.");
  const existing = unitSyncJobs.get(state);
  if (existing) return existing;
  const jobId = `units-${state}-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 7)}`;
  const promise = (async () => {
    try {
      return await runUnitSyncForState(state, options);
    } finally {
      unitSyncJobs.delete(state);
    }
  })();
  const job = { jobId, promise, startedAt: nowIso() };
  unitSyncJobs.set(state, job);
  return job;
}
function summarizeUnitSyncStatus(stateCode) {
  const state = normalizeStateCode(stateCode);
  if (!state) throw new Error("Invalid state.");
  const configured = getConfiguredUnitSourcesForState(state);
  const currentById = new Map(
    q.selUnitSourceStatesByState.all(state).map(row => [asText(row.source_id, ""), row])
  );
  const now = Date.now();
  const sources = configured.map(source => {
    const row = currentById.get(source.sourceId) || defaultUnitSourceState(source.sourceId, state);
    const completedAt = asText(row.last_sync_completed_at, "");
    const completedTs = Date.parse(completedAt);
    const freshnessAgeMs = Number.isFinite(completedTs) ? Math.max(0, now - completedTs) : null;
    return {
      sourceId: source.sourceId,
      datasetId: source.datasetId,
      status: asText(row.last_status, "idle"),
      watermark: asText(row.watermark, ""),
      lastSyncStartedAt: asText(row.last_sync_started_at, ""),
      lastSyncCompletedAt: completedAt,
      lastError: asText(row.last_error, ""),
      rowsScanned: Math.max(0, Number(row.rows_scanned) || 0),
      rowsUpserted: Math.max(0, Number(row.rows_upserted) || 0),
      freshnessAgeMs,
      stale: freshnessAgeMs == null ? true : freshnessAgeMs >= UNIT_SYNC_STALE_MS
    };
  });
  const evidenceCountRow = q.countUnitEvidenceByState.get(state) || { count: 0 };
  const sourceCompletionTimes = sources
    .map(source => asText(source.lastSyncCompletedAt, ""))
    .filter(Boolean)
    .sort((a, b) => (a > b ? 1 : a < b ? -1 : 0));
  return {
    ok: true,
    state,
    sourceCount: sources.length,
    unitRows: Math.max(0, Number(evidenceCountRow.count) || 0),
    lastUnitSyncAt: sourceCompletionTimes.length ? sourceCompletionTimes[sourceCompletionTimes.length - 1] : "",
    stale: sources.some(source => source.stale),
    sources
  };
}
function maybeTriggerUnitSync(stateCode, options = {}) {
  if (UNIT_SYNC_DISABLED) return null;
  const state = normalizeStateCode(stateCode);
  if (!state || !SUPPORTED_STATES.has(state)) return null;
  try {
    const status = summarizeUnitSyncStatus(state);
    const shouldRun = !!options.force || !!(status.sources || []).find(source => source.stale || source.status === "error");
    if (!shouldRun) return null;
    return ensureUnitSyncJob(state, { force: !!options.force, reason: asText(options.reason, "") });
  } catch {
    return null;
  }
}

function normalizeAddressRow(raw, stateCode, releaseId, fallbackIndex = 0) {
  if (!raw || typeof raw !== "object") return null;
  const lat = Number(raw.lat);
  const lng = Number(raw.lng);
  if (!Number.isFinite(lat) || !Number.isFinite(lng)) return null;
  if (Math.abs(lat) > 90 || Math.abs(lng) > 180) return null;
  const id = asText(raw.id || `${stateCode}_${releaseId}_${fallbackIndex + 1}`, "");
  if (!id) return null;
  return {
    state: stateCode,
    release_id: asText(releaseId, ""),
    id,
    source_dataset: asText(raw.source_dataset || "", ""),
    house_number: asText(raw.house_number || raw.number || "", ""),
    street: asText(raw.street || "", ""),
    unit: asText(raw.unit || "", ""),
    city: asText(raw.city || "", ""),
    region: asText(raw.region || stateCode, stateCode),
    postcode: asText(raw.postcode || "", ""),
    country_code: asText(raw.country_code || "US", "US"),
    full_address: asText(raw.full_address || raw.full || "", ""),
    lat,
    lng,
    tile_key: computeTileKey(lat, lng)
  };
}
function normalizeBuildingRow(raw, stateCode, releaseId, fallbackIndex = 0) {
  if (!raw || typeof raw !== "object") return null;
  const id = asText(raw.id || `${stateCode}_building_${releaseId}_${fallbackIndex + 1}`, "");
  if (!id) return null;
  const geomWkt = asText(raw.geom_wkt || raw.geometry_wkt || raw.geometry || "", "");
  const parsed = parseBuildingGeometryWkt(geomWkt);
  if (!parsed || !parsed.bounds) return null;
  const centerLat = (parsed.bounds.minLat + parsed.bounds.maxLat) / 2;
  const centerLng = (parsed.bounds.minLng + parsed.bounds.maxLng) / 2;
  return {
    state: stateCode,
    release_id: asText(releaseId, ""),
    id,
    source_dataset: asText(raw.source_dataset || "", ""),
    building_class: asText(raw.building_class || raw.class || "", ""),
    levels: asText(raw.levels || raw.level || "", ""),
    name: asText(raw.name || "", ""),
    geom_wkt: geomWkt,
    min_lat: parsed.bounds.minLat,
    max_lat: parsed.bounds.maxLat,
    min_lng: parsed.bounds.minLng,
    max_lng: parsed.bounds.maxLng,
    tile_key: computeTileKey(centerLat, centerLng)
  };
}

async function installDatasetFromManifest(stateCode, options = {}) {
  const normalizedState = normalizeStateCode(stateCode);
  if (!normalizedState) throw new Error("Invalid state code.");
  if (!SUPPORTED_STATES.has(normalizedState)) throw new Error(`State package not yet published for ${normalizedState}.`);

  const force = !!options.force;
  const manifestBundle = await loadManifest();
  const statePackage = getStatePackageFromManifest(manifestBundle.manifest, normalizedState);
  if (!statePackage) throw new Error(`No package entry found for ${normalizedState}.`);

  const addressesDataset = statePackage.datasets.addresses;
  const buildingsDataset = statePackage.datasets.buildings;
  if (!Array.isArray(addressesDataset.chunks) || !addressesDataset.chunks.length) {
    throw new Error(`Missing addresses dataset chunks for ${normalizedState}.`);
  }
  if (!Array.isArray(buildingsDataset.chunks) || !buildingsDataset.chunks.length) {
    throw new Error("Buildings dataset required for strict residential filtering.");
  }

  const stateRow = readStateRow(normalizedState);
  let targetRelease = asText(options.targetRelease, "");
  if (!targetRelease) {
    if (!force && stateRow && asText(stateRow.pinned_release, "")) targetRelease = asText(stateRow.pinned_release, "");
    else targetRelease = asText(statePackage.release, "");
  }
  if (!targetRelease) throw new Error("Unable to resolve target release.");
  const pinnedRelease = targetRelease;

  const cachedReady = !!(
    stateRow
    && asText(stateRow.release_id, "") === targetRelease
    && Number(stateRow.strict_residential_ready) === 1
    && Number(stateRow.addresses_installed) === 1
    && Number(stateRow.buildings_installed) === 1
  );
  if (cachedReady && !force) {
    writeStateRow(normalizedState, {
      phase: PHASES.ready,
      phase_detail: `${normalizedState} ${targetRelease} already cached.`,
      progress_current: Math.max(0, Number(stateRow.progress_total) || 0),
      progress_total: Math.max(0, Number(stateRow.progress_total) || 0),
      error: "",
      pinned_release: asText(stateRow.pinned_release || targetRelease, targetRelease)
    });
    return { cached: true, state: normalizedState, release: targetRelease };
  }

  const jobId = asText(options.jobId, `${normalizedState}_${Date.now()}`);
  const totalChunks = addressesDataset.chunks.length + buildingsDataset.chunks.length;
  writeStateRow(normalizedState, {
    release_id: targetRelease,
    pinned_release: pinnedRelease,
    phase: PHASES.downloading,
    phase_detail: `Downloading ${normalizedState} package manifest...`,
    progress_current: 0,
    progress_total: Math.max(1, totalChunks),
    strict_residential_ready: 0,
    addresses_installed: 0,
    buildings_installed: 0,
    addresses_count: 0,
    buildings_count: 0,
    error: "",
    last_job_id: jobId,
    install_started_at: nowIso(),
    install_completed_at: ""
  });

  q.delAddresses.run(normalizedState);
  q.delBuildings.run(normalizedState);

  let addressesInserted = 0;
  let buildingsInserted = 0;
  let chunksCompleted = 0;

  const insertAddressChunk = text => {
    let inserted = 0;
    let parsedIndex = 0;
    db.exec("BEGIN;");
    try {
      forEachNdjsonLine(text, line => {
        const parsed = JSON.parse(line);
        const normalized = normalizeAddressRow(parsed, normalizedState, targetRelease, parsedIndex);
        parsedIndex += 1;
        if (!normalized) return;
        q.insAddress.run(normalized);
        inserted += 1;
      });
      db.exec("COMMIT;");
      return inserted;
    } catch (error) {
      db.exec("ROLLBACK;");
      throw error;
    }
  };

  const insertBuildingChunk = text => {
    let inserted = 0;
    let parsedIndex = 0;
    db.exec("BEGIN;");
    try {
      forEachNdjsonLine(text, line => {
        const parsed = JSON.parse(line);
        const normalized = normalizeBuildingRow(parsed, normalizedState, targetRelease, parsedIndex);
        parsedIndex += 1;
        if (!normalized) return;
        q.insBuilding.run(normalized);
        inserted += 1;
      });
      db.exec("COMMIT;");
      return inserted;
    } catch (error) {
      db.exec("ROLLBACK;");
      throw error;
    }
  };

  const installChunkGroup = async (datasetName, chunks, insertFn) => {
    for (let index = 0; index < chunks.length; index += 1) {
      const chunk = chunks[index];
      if (!chunk) continue;
      writeStateRow(normalizedState, {
        phase: PHASES.downloading,
        phase_detail: `Downloading ${normalizedState} ${datasetName} ${index + 1}/${chunks.length}...`,
        progress_current: chunksCompleted,
        progress_total: Math.max(1, totalChunks),
        error: ""
      });
      const chunkUrl = resolveChunkUrl(chunk.url, manifestBundle.manifestUrl);
      if (!chunkUrl) throw new Error(`Invalid ${datasetName} chunk URL.`);
      const text = String(await fetchTextWithRetry(chunkUrl, DOWNLOAD_TIMEOUT_MS) || "");
      writeStateRow(normalizedState, {
        phase: PHASES.indexing,
        phase_detail: `Indexing ${normalizedState} ${datasetName} ${index + 1}/${chunks.length}...`,
        progress_current: chunksCompleted,
        progress_total: Math.max(1, totalChunks),
        error: ""
      });
      const inserted = insertFn(text);
      if (datasetName === "addresses") addressesInserted += inserted;
      else buildingsInserted += inserted;
      chunksCompleted += 1;
      writeStateRow(normalizedState, {
        phase: PHASES.indexing,
        phase_detail: `Indexed ${normalizedState} ${datasetName} ${index + 1}/${chunks.length}.`,
        progress_current: chunksCompleted,
        progress_total: Math.max(1, totalChunks),
        addresses_count: addressesInserted,
        buildings_count: buildingsInserted,
        error: ""
      });
      await sleep(0);
    }
  };

  await installChunkGroup("addresses", addressesDataset.chunks, insertAddressChunk);
  await installChunkGroup("buildings", buildingsDataset.chunks, insertBuildingChunk);

  const strictReady = addressesInserted > 0 && buildingsInserted > 0;
  if (!strictReady) throw new Error("Install completed but strict residential mode is unavailable.");

  writeStateRow(normalizedState, {
    release_id: targetRelease,
    pinned_release: pinnedRelease,
    phase: PHASES.ready,
    phase_detail: `${normalizedState} dataset ready.`,
    progress_current: Math.max(1, totalChunks),
    progress_total: Math.max(1, totalChunks),
    strict_residential_ready: 1,
    addresses_installed: addressesInserted > 0 ? 1 : 0,
    buildings_installed: buildingsInserted > 0 ? 1 : 0,
    addresses_count: addressesInserted,
    buildings_count: buildingsInserted,
    error: "",
    install_completed_at: nowIso()
  });

  return { cached: false, state: normalizedState, release: targetRelease };
}

function ensureJob(stateCode, options = {}) {
  const normalizedState = normalizeStateCode(stateCode);
  if (!normalizedState) throw new Error("Invalid state code.");
  const existing = ensureJobs.get(normalizedState);
  if (existing) return existing;
  const jobId = `${normalizedState}-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 7)}`;
  const promise = (async () => {
    try {
      return await installDatasetFromManifest(normalizedState, { ...options, jobId });
    } catch (error) {
      writeStateRow(normalizedState, {
        phase: PHASES.error,
        phase_detail: "",
        error: String((error && error.message) || error || "install_failed"),
        last_job_id: jobId
      });
      throw error;
    } finally {
      ensureJobs.delete(normalizedState);
    }
  })();
  const job = { jobId, promise };
  ensureJobs.set(normalizedState, job);
  return job;
}

function buildBuildingSpatialIndex(buildings = []) {
  const tileMap = new Map();
  const fallback = [];
  for (const building of Array.isArray(buildings) ? buildings : []) {
    if (!building || typeof building !== "object") continue;
    const minLat = Number(building.min_lat);
    const maxLat = Number(building.max_lat);
    const minLng = Number(building.min_lng);
    const maxLng = Number(building.max_lng);
    if (!Number.isFinite(minLat) || !Number.isFinite(maxLat) || !Number.isFinite(minLng) || !Number.isFinite(maxLng)) {
      fallback.push(building);
      continue;
    }
    const loLat = Math.min(minLat, maxLat);
    const hiLat = Math.max(minLat, maxLat);
    const loLng = Math.min(minLng, maxLng);
    const hiLng = Math.max(minLng, maxLng);
    const minLatIdx = tileIndexLat(loLat);
    const maxLatIdx = tileIndexLat(hiLat);
    const minLngIdx = tileIndexLng(loLng);
    const maxLngIdx = tileIndexLng(hiLng);
    const latSpan = Math.abs(maxLatIdx - minLatIdx);
    const lngSpan = Math.abs(maxLngIdx - minLngIdx);
    if (latSpan > 3 || lngSpan > 3) {
      fallback.push(building);
      continue;
    }
    for (let latIdx = minLatIdx; latIdx <= maxLatIdx; latIdx += 1) {
      for (let lngIdx = minLngIdx; lngIdx <= maxLngIdx; lngIdx += 1) {
        const key = `${latIdx}:${lngIdx}`;
        const bucket = tileMap.get(key) || [];
        bucket.push(building);
        tileMap.set(key, bucket);
      }
    }
  }
  return { tileMap, fallback };
}
function getCandidateBuildingsForPoint(index, lat, lng) {
  if (!index || typeof index !== "object") return [];
  const tileMap = index.tileMap instanceof Map ? index.tileMap : new Map();
  const fallback = Array.isArray(index.fallback) ? index.fallback : [];
  const latIdx = tileIndexLat(lat);
  const lngIdx = tileIndexLng(lng);
  const dedupe = new Set();
  const candidates = [];
  const pushIfNew = (building) => {
    if (!building || typeof building !== "object") return;
    if (dedupe.has(building)) return;
    dedupe.add(building);
    candidates.push(building);
  };
  for (const building of fallback) pushIfNew(building);
  for (let dLat = -1; dLat <= 1; dLat += 1) {
    for (let dLng = -1; dLng <= 1; dLng += 1) {
      const key = `${latIdx + dLat}:${lngIdx + dLng}`;
      const bucket = tileMap.get(key) || [];
      for (const building of bucket) pushIfNew(building);
    }
  }
  return candidates;
}
async function normalizeAddressSearchRows(rows, polygon, buildingCandidates, strictResidential, options = {}) {
  const territoryToleranceMeters = Math.max(0, Number(options.territoryToleranceMeters) || 0);
  const normalized = [];
  const geometryCache = new Map();
  const buildingIndex = strictResidential ? buildBuildingSpatialIndex(buildingCandidates) : null;
  let processed = 0;
  for (const row of rows) {
    processed += 1;
    if ((processed % 250) === 0) await sleep(0);
    const lat = Number(row && row.lat);
    const lng = Number(row && row.lng);
    if (!Number.isFinite(lat) || !Number.isFinite(lng)) continue;
    if (!pointInPolygonOrNear([lat, lng], polygon, territoryToleranceMeters)) continue;

    if (strictResidential) {
      let insideResidential = false;
      const candidateBuildings = getCandidateBuildingsForPoint(buildingIndex, lat, lng);
      for (const building of candidateBuildings) {
        if (!building || !isResidentialBuildingClass(building.building_class)) continue;
        const minLat = Number(building.min_lat);
        const maxLat = Number(building.max_lat);
        const minLng = Number(building.min_lng);
        const maxLng = Number(building.max_lng);
        if (Number.isFinite(minLat) && Number.isFinite(maxLat) && Number.isFinite(minLng) && Number.isFinite(maxLng)) {
          if (lat < minLat || lat > maxLat || lng < minLng || lng > maxLng) continue;
        }
        const cacheKey = `${building.id || ""}|${building.release_id || ""}`;
        let parsed = geometryCache.get(cacheKey);
        if (!parsed) {
          parsed = parseBuildingGeometryWkt(building.geom_wkt || "");
          if (!parsed) continue;
          geometryCache.set(cacheKey, parsed);
        }
        if (isPointInsideBuildingGeometry([lat, lng], parsed)) {
          insideResidential = true;
          break;
        }
      }
      if (!insideResidential) continue;
    }

    normalized.push({
      id: asText(row.id, ""),
      release_id: asText(row.release_id, ""),
      source_dataset: asText(row.source_dataset, ""),
      house_number: asText(row.house_number, ""),
      street: asText(row.street, ""),
      unit: asText(row.unit, ""),
      city: asText(row.city, ""),
      region: asText(row.region, ""),
      postcode: asText(row.postcode, ""),
      country_code: asText(row.country_code || "US", "US"),
      full_address: asText(row.full_address, ""),
      lat,
      lng
    });
  }
  return normalized;
}

function collectUnitEvidenceCandidates(stateCode, bounds, baseRows, toleranceMeters = 0) {
  const paddedBounds = expandBoundsByMeters(bounds, toleranceMeters);
  const rows = q.selUnitEvidenceInBounds.all(
    stateCode,
    paddedBounds.minLat,
    paddedBounds.maxLat,
    paddedBounds.minLng,
    paddedBounds.maxLng
  );
  const deduped = new Map();
  for (const row of rows) {
    const key = `${asText(row.source_id, "")}|${asText(row.source_record_id, "")}`;
    if (!key || deduped.has(key)) continue;
    deduped.set(key, row);
  }

  const addressKeys = new Set();
  for (const baseRow of Array.isArray(baseRows) ? baseRows : []) {
    const key = addressKey(baseRow.house_number, baseRow.street, baseRow.postcode);
    if (!key || addressKeys.has(key)) continue;
    addressKeys.add(key);
    const parts = addressKeyParts(baseRow.house_number, baseRow.street, baseRow.postcode);
    if (!parts.house || !parts.street) continue;
    const byAddress = q.selUnitEvidenceByAddress.all(stateCode, parts.house, parts.street, parts.postcode);
    for (const unitRow of byAddress) {
      const dedupeKey = `${asText(unitRow.source_id, "")}|${asText(unitRow.source_record_id, "")}`;
      if (!dedupeKey || deduped.has(dedupeKey)) continue;
      deduped.set(dedupeKey, unitRow);
    }
  }
  return Array.from(deduped.values());
}
function distanceToNearestBaseMatchMeters(baseRows, candidatePoint, candidateStreetNorm) {
  if (!Array.isArray(baseRows) || !baseRows.length || !candidatePoint || !candidateStreetNorm) return Infinity;
  let nearest = Infinity;
  for (const row of baseRows) {
    if (!row) continue;
    const streetNorm = normalizeStreetForKey(row.street);
    if (!streetNorm || streetNorm !== candidateStreetNorm) continue;
    const dist = approxDistanceMeters(candidatePoint, [Number(row.lat), Number(row.lng)]);
    if (Number.isFinite(dist) && dist < nearest) nearest = dist;
  }
  return nearest;
}
function sortMergedAddressRows(rows, limit) {
  const sorted = [...(Array.isArray(rows) ? rows : [])].sort((a, b) => {
    const aStreet = asText(a && a.street, "").toLowerCase();
    const bStreet = asText(b && b.street, "").toLowerCase();
    if (aStreet !== bStreet) return aStreet.localeCompare(bStreet);
    const aHouse = asText(a && a.house_number, "").toLowerCase();
    const bHouse = asText(b && b.house_number, "").toLowerCase();
    if (aHouse !== bHouse) return aHouse.localeCompare(bHouse, "en", { numeric: true });
    const aUnit = asText(a && a.unit, "").toLowerCase();
    const bUnit = asText(b && b.unit, "").toLowerCase();
    return aUnit.localeCompare(bUnit, "en", { numeric: true });
  });
  return sorted.slice(0, Math.max(1, Number(limit) || DEFAULT_LIMIT));
}
function mergeBaseAndUnitEvidenceRows(stateCode, polygon, bounds, baseRows, options = {}) {
  const strictResidential = options.strictResidential !== false;
  const limit = Math.max(1, Number(options.limit) || DEFAULT_LIMIT);
  const territoryToleranceMeters = Math.max(0, Number(options.territoryToleranceMeters) || 0);
  const candidates = collectUnitEvidenceCandidates(stateCode, bounds, baseRows, territoryToleranceMeters);
  const exactBaseIndex = new Map();
  for (const baseRow of baseRows) {
    const key = addressKey(baseRow.house_number, baseRow.street, baseRow.postcode);
    if (!key) continue;
    if (!exactBaseIndex.has(key)) exactBaseIndex.set(key, []);
    exactBaseIndex.get(key).push(baseRow);
  }

  const unitRows = [];
  const exactMatchedKeys = new Set();
  const unitSources = new Set();
  for (const candidate of candidates) {
    const candidateUnit = asText(candidate && candidate.unit, "");
    if (!candidateUnit) continue;
    const baseConfidence = Math.max(0, Math.min(0.99, Number(candidate && candidate.confidence) || 0));
    if (baseConfidence < UNIT_STRICT_CONFIDENCE_THRESHOLD) continue;
    const lat = Number(candidate && candidate.lat);
    const lng = Number(candidate && candidate.lng);
    const hasPoint = Number.isFinite(lat) && Number.isFinite(lng);
    if (hasPoint && !pointInPolygonOrNear([lat, lng], polygon, territoryToleranceMeters)) continue;
    const candidateKey = addressKey(candidate.house_number, candidate.street, candidate.postcode);
    const exactMatches = exactBaseIndex.get(candidateKey) || [];
    const streetNorm = normalizeStreetForKey(candidate.street);
    let nearestDistance = Infinity;
    let nearestBase = null;
    if (!exactMatches.length && hasPoint && streetNorm) {
      nearestDistance = distanceToNearestBaseMatchMeters(baseRows, [lat, lng], streetNorm);
      if (nearestDistance <= UNIT_NEAREST_BASE_MATCH_METERS) {
        nearestBase = baseRows.find(row => normalizeStreetForKey(row.street) === streetNorm
          && approxDistanceMeters([lat, lng], [Number(row.lat), Number(row.lng)]) <= UNIT_NEAREST_BASE_MATCH_METERS) || null;
      }
    }

    const buildingId = asText(candidate && candidate.building_id, "");
    if (strictResidential && !exactMatches.length && !nearestBase && !buildingId) continue;

    let confidence = baseConfidence;
    if (exactMatches.length) confidence = Math.min(0.99, confidence + 0.05);
    if (confidence < UNIT_STRICT_CONFIDENCE_THRESHOLD) continue;

    if (exactMatches.length) exactMatchedKeys.add(candidateKey);
    const sourceId = asText(candidate && candidate.source_id, "");
    if (sourceId) unitSources.add(sourceId);
    const anchor = exactMatches[0] || nearestBase || null;
    const row = {
      id: `unit:${sourceId || "source"}:${asText(candidate && candidate.source_record_id, "") || crypto.createHash("sha1").update(JSON.stringify(candidate || {})).digest("hex").slice(0, 12)}`,
      release_id: asText(anchor && anchor.release_id, ""),
      source_dataset: sourceId ? `open-data:${sourceId}` : "open-data:unit-evidence",
      house_number: asText(candidate.house_number || (anchor && anchor.house_number), ""),
      street: asText(candidate.street || (anchor && anchor.street), ""),
      unit: candidateUnit,
      city: asText(candidate.city || (anchor && anchor.city), ""),
      region: asText(candidate.region || (anchor && anchor.region) || stateCode, stateCode),
      postcode: normalizeZip(candidate.postcode || (anchor && anchor.postcode)),
      country_code: asText((anchor && anchor.country_code) || "US", "US"),
      full_address: asText(
        `${asText(candidate.house_number || (anchor && anchor.house_number), "")} ${asText(candidate.street || (anchor && anchor.street), "")} ${candidateUnit}`.replace(/\s+/g, " "),
        ""
      ),
      lat: hasPoint ? lat : Number(anchor && anchor.lat),
      lng: hasPoint ? lng : Number(anchor && anchor.lng),
      _unitEvidence: true,
      _unitSourceId: sourceId,
      _unitConfidence: confidence
    };
    if (!Number.isFinite(row.lat) || !Number.isFinite(row.lng)) continue;
    if (!row.house_number && !row.street && !row.full_address) continue;
    unitRows.push(row);
  }

  const baseFiltered = (Array.isArray(baseRows) ? baseRows : []).filter(row => {
    const key = addressKey(row.house_number, row.street, row.postcode);
    const hasUnit = !!asText(row.unit, "");
    if (hasUnit) return true;
    if (key && exactMatchedKeys.has(key)) return false;
    return true;
  });

  const mergedDeduped = new Map();
  for (const row of [...baseFiltered, ...unitRows]) {
    const key = `${addressKey(row.house_number, row.street, row.postcode)}|${normalizeUnitForKey(row.unit)}|${Number(row.lat).toFixed(6)}|${Number(row.lng).toFixed(6)}`;
    if (!key || mergedDeduped.has(key)) continue;
    mergedDeduped.set(key, row);
  }

  const mergedRows = sortMergedAddressRows(Array.from(mergedDeduped.values()), limit);
  const evidenceRowsReturned = mergedRows.filter(row => !!row._unitEvidence);
  const unitlessRowsReturned = mergedRows.filter(row => !asText(row.unit, "")).length;
  const confidenceSummary = { high: 0, medium: 0, low: 0 };
  evidenceRowsReturned.forEach(row => {
    const confidence = Number(row._unitConfidence) || 0;
    if (confidence >= 0.8) confidenceSummary.high += 1;
    else if (confidence >= 0.6) confidenceSummary.medium += 1;
    else if (confidence >= UNIT_STRICT_CONFIDENCE_THRESHOLD) confidenceSummary.low += 1;
  });

  const syncStatus = summarizeUnitSyncStatus(stateCode);
  return {
    rows: mergedRows.map(row => {
      const cleanRow = { ...row };
      delete cleanRow._unitEvidence;
      delete cleanRow._unitSourceId;
      delete cleanRow._unitConfidence;
      return cleanRow;
    }),
    unitCoverage: {
      unitRowsReturned: evidenceRowsReturned.length,
      unitlessRowsReturned,
      sourcesUsed: Array.from(unitSources.values()).sort(),
      lastUnitSyncAt: asText(syncStatus && syncStatus.lastUnitSyncAt, "")
    },
    unitConfidenceSummary: confidenceSummary
  };
}

async function searchAddresses(body = {}) {
  const stateCode = normalizeStateCode(body.state);
  if (!stateCode) throw new Error("Missing or invalid state.");
  if (!SUPPORTED_STATES.has(stateCode)) throw new Error(`State package not yet published for ${stateCode}.`);
  const polygon = normalizePolygon(body.polygon);
  const bounds = getPolygonBounds(polygon);
  const limit = clampLimit(body.limit);
  const strictResidential = body.strictResidential !== false;
  const requestedTolerance = Number(body.territoryToleranceMeters);
  const baseToleranceMeters = Number.isFinite(requestedTolerance)
    ? Math.max(0, requestedTolerance)
    : DEFAULT_TERRITORY_EDGE_TOLERANCE_METERS;
  const queryBounds = expandBoundsByMeters(bounds, baseToleranceMeters);
  const stateRow = readStateRow(stateCode);
  const releaseId = asText(stateRow && stateRow.release_id, "");
  const ready = !!(stateRow && Number(stateRow.strict_residential_ready));
  if (!releaseId || !ready) throw new Error(`State dataset not ready for ${stateCode}.`);
  const primaryAddressScanLimit = Math.min(12000, Math.max(limit * 4, limit));
  const relaxedAddressScanLimit = Math.min(20000, Math.max(limit * 6, limit));
  const buildingScanLimit = Math.min(25000, Math.max(limit * 10, 5000));

  let addressCandidates = q.selAddressCandidates.all(
    stateCode,
    releaseId,
    queryBounds.minLat,
    queryBounds.maxLat,
    queryBounds.minLng,
    queryBounds.maxLng,
    primaryAddressScanLimit
  );
  let buildingCandidates = strictResidential
    ? q.selBuildingCandidates.all(
      stateCode,
      releaseId,
      queryBounds.maxLat,
      queryBounds.minLat,
      queryBounds.maxLng,
      queryBounds.minLng,
      buildingScanLimit
    )
    : [];

  let rows = await normalizeAddressSearchRows(
    addressCandidates,
    polygon,
    buildingCandidates,
    strictResidential,
    { territoryToleranceMeters: baseToleranceMeters }
  );

  let relaxedByProximity = false;
  let usedToleranceMeters = baseToleranceMeters;
  if (strictResidential && rows.length <= 1 && baseToleranceMeters < RELAXED_TERRITORY_EDGE_TOLERANCE_METERS) {
    const relaxedToleranceMeters = RELAXED_TERRITORY_EDGE_TOLERANCE_METERS;
    const relaxedBounds = expandBoundsByMeters(bounds, relaxedToleranceMeters);
    addressCandidates = q.selAddressCandidates.all(
      stateCode,
      releaseId,
      relaxedBounds.minLat,
      relaxedBounds.maxLat,
      relaxedBounds.minLng,
      relaxedBounds.maxLng,
      relaxedAddressScanLimit
    );
    buildingCandidates = q.selBuildingCandidates.all(
      stateCode,
      releaseId,
      relaxedBounds.maxLat,
      relaxedBounds.minLat,
      relaxedBounds.maxLng,
      relaxedBounds.minLng,
      buildingScanLimit
    );
    const relaxedRows = await normalizeAddressSearchRows(
      addressCandidates,
      polygon,
      buildingCandidates,
      strictResidential,
      { territoryToleranceMeters: relaxedToleranceMeters }
    );
    if (relaxedRows.length > rows.length) {
      rows = relaxedRows;
      relaxedByProximity = true;
      usedToleranceMeters = relaxedToleranceMeters;
    }
  }

  const merged = mergeBaseAndUnitEvidenceRows(stateCode, polygon, bounds, rows, {
    strictResidential,
    limit,
    territoryToleranceMeters: usedToleranceMeters
  });
  return {
    rows: merged.rows,
    release: releaseId,
    territoryToleranceMeters: usedToleranceMeters,
    relaxedByProximity,
    unitCoverage: merged.unitCoverage,
    unitConfidenceSummary: merged.unitConfidenceSummary
  };
}

async function searchBuildings(body = {}) {
  const stateCode = normalizeStateCode(body.state);
  if (!stateCode) throw new Error("Missing or invalid state.");
  const polygon = normalizePolygon(body.polygon);
  const bounds = getPolygonBounds(polygon);
  const limit = clampLimit(body.limit);
  const stateRow = readStateRow(stateCode);
  const releaseId = asText(stateRow && stateRow.release_id, "");
  if (!releaseId) throw new Error(`State dataset not ready for ${stateCode}.`);
  const rows = q.selBuildingCandidates
    .all(stateCode, releaseId, bounds.maxLat, bounds.minLat, bounds.maxLng, bounds.minLng, Math.max(limit * 8, limit))
    .slice(0, limit);
  return { rows, release: releaseId };
}

function getCurrentRelease(stateCode = "NY") {
  const normalizedState = normalizeStateCode(stateCode) || "NY";
  const status = statusPayload(normalizedState);
  return {
    release_id: status.release,
    imported_at: status.installCompletedAt,
    source_uri: PACKAGE_MANIFEST_URL || `file://${PACKAGE_MANIFEST_PATH.replace(/\\/g, "/")}`,
    notes: "local-open-data-cache"
  };
}

function startUnitSyncScheduler() {
  if (UNIT_SYNC_DISABLED) return;
  if (unitSyncTimer) return;
  const kick = (reason, force = false) => {
    try {
      const job = maybeTriggerUnitSync("NY", { force, reason });
      if (job && job.promise && typeof job.promise.catch === "function") {
        job.promise.catch(error => {
          console.warn(`[unit-sync] ${reason} failed: ${String((error && error.message) || error || "unknown")}`);
        });
      }
    } catch (error) {
      console.warn(`[unit-sync] ${reason} trigger failed: ${String((error && error.message) || error || "unknown")}`);
    }
  };
  setTimeout(() => kick("startup", false), 600);
  unitSyncTimer = setInterval(() => kick("interval", false), UNIT_SYNC_INTERVAL_MS);
}

async function handleRequest(req, res) {
  try {
    const url = new URL(req.url, `http://${req.headers.host || "localhost"}`);
    const pathname = url.pathname.startsWith("/api/overture/")
      ? `/api/local-data/${url.pathname.slice("/api/overture/".length)}`
      : url.pathname;

    if (req.method === "OPTIONS") {
      sendJson(res, 204, {});
      return;
    }

    if (req.method === "GET" && pathname === "/health") {
      sendJson(res, 200, {
        ok: true,
        service: "local-data-api",
        backend: "local-cache",
        cacheDbPath: CACHE_DB_PATH
      });
      return;
    }

    if (req.method === "GET" && pathname === "/api/local-data/release") {
      const stateCode = normalizeStateCode(url.searchParams.get("state") || "NY") || "NY";
      sendJson(res, 200, { ok: true, release: getCurrentRelease(stateCode) });
      return;
    }

    if (req.method === "GET" && pathname === "/api/local-data/state/status") {
      const stateCode = normalizeStateCode(url.searchParams.get("state"));
      if (!stateCode) throw new Error("Missing or invalid state.");
      if (!SUPPORTED_STATES.has(stateCode)) throw new Error(`State package not yet published for ${stateCode}.`);
      sendJson(res, 200, statusPayload(stateCode));
      return;
    }

    if (req.method === "POST" && pathname === "/api/local-data/state/ensure") {
      const body = await parseRequestBody(req);
      const stateCode = normalizeStateCode(body.state);
      if (!stateCode) throw new Error("Missing or invalid state.");
      if (!SUPPORTED_STATES.has(stateCode)) throw new Error(`State package not yet published for ${stateCode}.`);
      const status = statusPayload(stateCode);
      const cached = !!(status.strictResidentialReady && status.phase === PHASES.ready && !body.force);
      if (cached) {
        maybeTriggerUnitSync(stateCode, { force: false, reason: "state.ensure.cached" });
        sendJson(res, 200, {
          ok: true,
          state: stateCode,
          release: status.release,
          phase: status.phase,
          jobId: status.jobId || "",
          cached: true
        });
        return;
      }
      const job = ensureJob(stateCode, {
        force: !!body.force,
        targetRelease: asText(body.targetRelease, "")
      });
      const nextStatus = statusPayload(stateCode);
      maybeTriggerUnitSync(stateCode, { force: false, reason: "state.ensure" });
      sendJson(res, 202, {
        ok: true,
        state: stateCode,
        release: nextStatus.release,
        phase: nextStatus.phase,
        jobId: job.jobId,
        cached: false
      });
      return;
    }

    if (req.method === "POST" && pathname === "/api/local-data/state/upgrade") {
      const body = await parseRequestBody(req);
      const stateCode = normalizeStateCode(body.state);
      if (!stateCode) throw new Error("Missing or invalid state.");
      if (!SUPPORTED_STATES.has(stateCode)) throw new Error(`State package not yet published for ${stateCode}.`);
      const previous = statusPayload(stateCode).release;
      const job = ensureJob(stateCode, {
        force: true,
        targetRelease: asText(body.targetRelease, "")
      });
      const nextStatus = statusPayload(stateCode);
      maybeTriggerUnitSync(stateCode, { force: true, reason: "state.upgrade" });
      sendJson(res, 202, {
        ok: true,
        state: stateCode,
        previousRelease: previous,
        release: nextStatus.release,
        phase: nextStatus.phase,
        jobId: job.jobId
      });
      return;
    }

    if (req.method === "POST" && pathname === "/api/local-data/state/refresh") {
      const body = await parseRequestBody(req);
      const stateCode = normalizeStateCode(body.state);
      if (!stateCode) throw new Error("Missing or invalid state.");
      if (!SUPPORTED_STATES.has(stateCode)) throw new Error(`State package not yet published for ${stateCode}.`);
      const job = ensureJob(stateCode, {
        force: true,
        targetRelease: asText(body.targetRelease, "")
      });
      const status = statusPayload(stateCode);
      maybeTriggerUnitSync(stateCode, { force: true, reason: "state.refresh" });
      sendJson(res, 202, {
        ok: true,
        state: stateCode,
        release: status.release,
        phase: status.phase,
        jobId: job.jobId
      });
      return;
    }

    if (req.method === "POST" && pathname === "/api/local-data/addresses/search") {
      const body = await parseRequestBody(req);
      const result = await searchAddresses(body);
      sendJson(res, 200, {
        ok: true,
        count: result.rows.length,
        rows: result.rows,
        release: result.release,
        source: "local-open-data-cache",
        territoryToleranceMeters: Math.max(0, Number(result.territoryToleranceMeters) || 0),
        relaxedByProximity: !!result.relaxedByProximity,
        unitCoverage: result.unitCoverage || {
          unitRowsReturned: 0,
          unitlessRowsReturned: result.rows.length,
          sourcesUsed: [],
          lastUnitSyncAt: ""
        },
        unitConfidenceSummary: result.unitConfidenceSummary || {
          high: 0,
          medium: 0,
          low: 0
        }
      });
      return;
    }

    if (req.method === "GET" && pathname === "/api/local-data/unit-sync/status") {
      const stateCode = normalizeStateCode(url.searchParams.get("state"));
      if (!stateCode) throw new Error("Missing or invalid state.");
      if (!SUPPORTED_STATES.has(stateCode)) throw new Error(`State package not yet published for ${stateCode}.`);
      sendJson(res, 200, summarizeUnitSyncStatus(stateCode));
      return;
    }

    if (req.method === "POST" && pathname === "/api/local-data/unit-sync/run") {
      const body = await parseRequestBody(req);
      const stateCode = normalizeStateCode(body.state || "NY");
      if (!stateCode) throw new Error("Missing or invalid state.");
      if (!SUPPORTED_STATES.has(stateCode)) throw new Error(`State package not yet published for ${stateCode}.`);
      const job = ensureUnitSyncJob(stateCode, { force: !!body.force, reason: "manual" });
      sendJson(res, 202, {
        ok: true,
        state: stateCode,
        jobId: job.jobId,
        status: "running",
        force: !!body.force
      });
      return;
    }

    if (req.method === "POST" && pathname === "/api/local-data/buildings/search") {
      const body = await parseRequestBody(req);
      const result = await searchBuildings(body);
      sendJson(res, 200, {
        ok: true,
        count: result.rows.length,
        rows: result.rows,
        release: result.release
      });
      return;
    }

    if (req.method === "POST" && pathname === "/api/local-data/territories/align/preview") {
      const body = await parseRequestBody(req);
      const territories = normalizeAlignmentTerritories(body.territories);
      if (!territories.length) throw new Error("No valid territories were provided for alignment preview.");
      const payload = createAlignmentPreview(territories, body.options || {});
      sendJson(res, 200, payload);
      return;
    }

    if (req.method === "POST" && pathname === "/api/local-data/territories/align/apply") {
      const body = await parseRequestBody(req);
      const territories = normalizeAlignmentTerritories(body.territories);
      if (!territories.length) throw new Error("No valid territories were provided for alignment apply.");
      const result = applyAlignmentPreview(body.confirmToken, territories, body.options || {});
      sendJson(res, 200, result);
      return;
    }

    sendJson(res, 404, { ok: false, error: "Not found." });
  } catch (error) {
    sendJson(res, 400, {
      ok: false,
      error: String((error && error.message) || error || "unknown_error")
    });
  }
}

const server = http.createServer((req, res) => {
  handleRequest(req, res);
});

server.listen(PORT, HOST, () => {
  console.log(`Local Data API listening on http://${HOST}:${PORT} (local-cache)`);
  console.log(`Manifest source: ${PACKAGE_MANIFEST_URL || PACKAGE_MANIFEST_PATH}`);
  console.log(`Cache DB: ${CACHE_DB_PATH}`);
  console.log(`Unit source profile: ${UNIT_SOURCE_PROFILE_PATH}`);
  startUnitSyncScheduler();
});
