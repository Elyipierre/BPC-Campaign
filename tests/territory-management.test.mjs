import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { JSDOM } from "jsdom";
import { afterEach, describe, expect, it, vi } from "vitest";

const THIS_DIR = path.dirname(fileURLToPath(import.meta.url));
const APP_HTML_PATH = path.resolve(THIS_DIR, "../Territory Management.html");
const APP_HTML = fs.readFileSync(APP_HTML_PATH, "utf8");
const APP_SCRIPT = extractInlineScript(APP_HTML);

const STORAGE_KEYS = Object.freeze({
  db: "terr_final_db",
  ui: "terr_ui_prefs",
  datasetMeta: "terr_dataset_meta",
  datasetState: "terr_dataset_state",
  viewMode: "terr_view_mode_v1",
  campaignDraft: "terr_campaign_draft_v1",
  cardBlueprints: "terr_card_blueprints",
  localCampaigns: "terr_local_campaigns_v1"
});

function extractInlineScript(html) {
  const scriptRegex = /<script(?![^>]*\bsrc=)[^>]*>([\s\S]*?)<\/script>/gi;
  const matches = [...html.matchAll(scriptRegex)];
  if (!matches.length) throw new Error("Unable to locate inline app script.");
  return matches[matches.length - 1][1];
}

function makeDomTemplate() {
  return `<!doctype html>
<html>
<body>
  <div id="mainLayout" class="main-layout">
  <div class="tools-wrap">
  <select id="territorySelector"></select>
  <div id="selectionInputs">
    <input id="territoryNo" />
    <input id="locality" />
  </div>
  <button id="btnToolsMenu" aria-expanded="false"></button>
  <div id="menuTerritoryTools" hidden>
    <button id="btnViewModeSelected" data-view-mode="selected" data-tools-focusable="true" type="button">Selected Territory</button>
    <button id="btnViewModeAll" data-view-mode="all" data-tools-focusable="true" type="button">All Territories</button>
    <button id="btnViewModeCampaign" data-view-mode="campaign" data-tools-focusable="true" type="button">Campaign Mode</button>
    <div id="viewModeHint"></div>
    <button id="btnAssignmentRecords" data-tools-focusable="true"></button>
    <button id="btnPrint" data-tools-focusable="true"></button>
    <button id="btnDelete" data-tools-focusable="true"></button>
    <button id="btnExport" data-tools-focusable="true"></button>
    <button id="btnBackupExport" data-tools-focusable="true"></button>
    <button id="btnBackupImport" data-tools-focusable="true"></button>
    <button id="btnAlignTerritories" data-tools-focusable="true"></button>
    <button id="btnManualCorrection" data-tools-focusable="true"></button>
    <button id="btnClear" data-tools-focusable="true"></button>
    <button id="btnSnap" data-tools-focusable="true"></button>
  </div>
  </div>
  <div id="selectionActions"><button id="btnFetch"><span class="icon"></span><span class="btn-label">Refresh Selected</span></button></div>
  <div id="status">Ready.</div>
  <div id="territoryAssignmentNotice" hidden></div>
  <section id="addressCard">
    <div id="address-list-title">Addresses (0)</div>
    <div id="territory-stats"></div>
    <div id="addr-scroll"></div>
  </section>
  <input id="territoryBackupInput" type="file" />
  <section id="campaignCard" hidden>
    <div id="campaignSyncStatus"></div>
    <input id="campaignName" />
    <input id="campaignPublicCode" />
    <textarea id="campaignApprovedEmails"></textarea>
    <input id="campaignPublicLink" />
    <input id="campaignLocalLink" />
    <button id="btnCampaignOpenLocal"></button>
    <button id="btnCampaignPublish"></button>
    <button id="btnCampaignRefresh"></button>
    <button id="btnCampaignCopyLink"></button>
    <button id="btnCampaignCopyLocalLink"></button>
    <div id="kpiCampaignTotal"></div>
    <div id="kpiCampaignCompleted"></div>
    <div id="kpiCampaignRemaining"></div>
    <div id="kpiCampaignProgress"></div>
    <div id="campaignSelectedMeta"></div>
    <div id="campaignSelectedStatus"></div>
    <button id="btnCampaignReopenSelected"></button>
    <div id="campaignRoster"></div>
  </section>
  <section id="assignmentModal" hidden aria-hidden="true">
    <div data-assignment-close="true"></div>
    <input id="assignmentServiceYear" />
    <button id="btnAssignmentPrint"></button>
    <button id="btnAssignmentClose"></button>
    <button id="btnAssignmentNew"></button>
    <button id="btnAssignmentReturn"></button>
    <button id="btnAssignmentCancel"></button>
    <button id="btnAssignmentSave"></button>
    <div id="assignmentSummary"></div>
    <table><tbody id="assignmentTableBody"></tbody></table>
    <div id="assignmentEditorEmpty"></div>
    <div id="assignmentEditorPanel" hidden></div>
    <div id="assignmentEditorTitle"></div>
    <div id="assignmentEditorMeta"></div>
    <input id="assignmentAssigneeInput" />
    <input id="assignmentAssignedAtInput" />
    <input id="assignmentCompletedAtInput" />
    <div id="assignmentHistoryList"></div>
  </section>
  <div id="print-area"></div>
  <div id="mapLegend"></div>
  <div id="map"></div>
  </div>
</body>
</html>`;
}

function createLocalStorage({ db = [], uiPrefs = {}, datasetMeta = null, datasetState = "NY" } = {}) {
  const storage = new Map();
  storage.set(STORAGE_KEYS.db, JSON.stringify(db));
  storage.set(STORAGE_KEYS.ui, JSON.stringify(uiPrefs));
  if (datasetMeta) storage.set(STORAGE_KEYS.datasetMeta, JSON.stringify(datasetMeta));
  if (datasetState) storage.set(STORAGE_KEYS.datasetState, String(datasetState));
  const api = {
    getItem: vi.fn((key) => (storage.has(key) ? storage.get(key) : null)),
    setItem: vi.fn((key, value) => storage.set(key, String(value))),
    removeItem: vi.fn((key) => storage.delete(key)),
    clear: vi.fn(() => storage.clear())
  };
  return { storage, api };
}

function createLeafletMock() {
  function asLatLngPair(point) {
    if (Array.isArray(point)) return [Number(point[0]), Number(point[1])];
    if (point && typeof point === "object") return [Number(point.lat), Number(point.lng ?? point.lon)];
    return [NaN, NaN];
  }

  function makeBounds(latlngs) {
    const pairs = (Array.isArray(latlngs) ? latlngs : []).map(asLatLngPair);
    const lats = pairs.map(([lat]) => lat).filter(Number.isFinite);
    const lngs = pairs.map(([, lng]) => lng).filter(Number.isFinite);
    const minLat = lats.length ? Math.min(...lats) : 0;
    const maxLat = lats.length ? Math.max(...lats) : 0;
    const minLng = lngs.length ? Math.min(...lngs) : 0;
    const maxLng = lngs.length ? Math.max(...lngs) : 0;
    const spanLat = maxLat - minLat;
    const spanLng = maxLng - minLng;
    return {
      getSouthWest: () => ({ lat: minLat, lng: minLng }),
      getNorthEast: () => ({ lat: maxLat, lng: maxLng }),
      pad: (ratio) => ({
        getSouthWest: () => ({ lat: minLat - spanLat * ratio, lng: minLng - spanLng * ratio }),
        getNorthEast: () => ({ lat: maxLat + spanLat * ratio, lng: maxLng + spanLng * ratio }),
        pad: () => makeBounds(latlngs)
      })
    };
  }

  class MockFeatureGroup {
    constructor() {
      this.layers = [];
      this.map = null;
    }
    addTo(map) {
      this.map = map;
      return this;
    }
    clearLayers() {
      this.layers = [];
    }
    addLayer(layer) {
      this.layers.push(layer);
      return this;
    }
  }

  const maps = [];
  const map = vi.fn((id) => {
    const panes = {};
    const listeners = {};
    let currentZoom = 13;
    const mapObj = {
      id,
      dragging: {
        disable: vi.fn(),
        enable: vi.fn()
      },
      setView: vi.fn((_center, zoom) => {
        if (typeof zoom === "number") currentZoom = zoom;
        return mapObj;
      }),
      getMaxZoom: vi.fn(() => 19),
      getZoom: vi.fn(() => currentZoom),
      setZoom: vi.fn((zoom) => {
        currentZoom = zoom;
        if (listeners.zoomend) listeners.zoomend({ zoom });
        return mapObj;
      }),
      createPane: vi.fn((name) => {
        panes[name] = {
          style: {},
          classList: { add: vi.fn(), remove: vi.fn(), toggle: vi.fn() }
        };
        return panes[name];
      }),
      getPane: vi.fn((name) => {
        if (!panes[name]) {
          panes[name] = {
            style: {},
            classList: { add: vi.fn(), remove: vi.fn(), toggle: vi.fn() }
          };
        }
        return panes[name];
      }),
      addControl: vi.fn(),
      removeLayer: vi.fn(),
      on: vi.fn((event, cb) => {
        listeners[event] = cb;
        return mapObj;
      }),
      trigger(event, payload) {
        if (listeners[event]) listeners[event](payload);
      },
      invalidateSize: vi.fn(),
      fitBounds: vi.fn(),
      flyToBounds: vi.fn(),
      latLngToContainerPoint: vi.fn((latlng) => ({ x: Number(latlng.lng || latlng[1] || 0), y: Number(latlng.lat || latlng[0] || 0) })),
      containerPointToLatLng: vi.fn((point) => ({ lat: Number(point.y || 0), lng: Number(point.x || 0) })),
      getSize: vi.fn(() => ({ x: 800, y: 600 })),
      remove: vi.fn()
    };
    maps.push(mapObj);
    return mapObj;
  });

  const tileLayer = vi.fn((url, options) => {
    const listeners = {};
    const layer = {
      url,
      options,
      addTo: vi.fn(() => layer),
      on: vi.fn((event, cb) => {
        listeners[event] = cb;
        return layer;
      }),
      off: vi.fn((event) => {
        delete listeners[event];
        return layer;
      }),
      isLoading: vi.fn(() => false),
      trigger(event, payload) {
        if (listeners[event]) listeners[event](payload);
      }
    };
    return layer;
  });

  const polygon = vi.fn((latlngs, options) => {
    const listeners = {};
    const poly = {
      latlngs,
      options,
      addTo: vi.fn((target) => {
        if (target && typeof target.addLayer === "function") target.addLayer(poly);
        return poly;
      }),
      on: vi.fn((event, cb) => {
        listeners[event] = cb;
        return poly;
      }),
      setStyle: vi.fn(() => poly),
      getBounds: vi.fn(() => makeBounds(latlngs)),
      trigger(event, payload) {
        if (listeners[event]) listeners[event](payload);
      }
    };
    return poly;
  });

  const marker = vi.fn((latlng, options) => {
    const listeners = {};
    const mk = {
      latlng,
      options,
      addTo: vi.fn((target) => {
        if (target && typeof target.addLayer === "function") target.addLayer(mk);
        return mk;
      }),
      on: vi.fn((event, cb) => {
        listeners[event] = cb;
        return mk;
      }),
      getLatLng: vi.fn(() => ({ lat: Number(latlng[0]), lng: Number(latlng[1]) })),
      setLatLng: vi.fn(() => mk),
      trigger(event, payload) {
        if (listeners[event]) listeners[event](payload || {});
      }
    };
    return mk;
  });

  const DomUtil = {
    create: (_tagName, _className, _container) => {
      const el = {
        classList: { add: vi.fn(), remove: vi.fn(), toggle: vi.fn() },
        setAttribute: vi.fn(),
        querySelector: vi.fn(() => null)
      };
      return el;
    }
  };

  const DomEvent = {
    disableClickPropagation: vi.fn(),
    disableScrollPropagation: vi.fn(),
    on: vi.fn(),
    preventDefault: vi.fn(),
    stopPropagation: vi.fn()
  };

  const Control = {
    Draw: class {
      constructor(options) {
        this.options = options;
      }
    },
    extend(proto = {}) {
      return class {
        constructor(options = {}) {
          this.options = { ...(proto.options || {}), ...(options || {}) };
        }
        addTo(targetMap) {
          this._map = targetMap;
          if (typeof this.onAdd === "function") this._container = this.onAdd(targetMap);
          if (targetMap && typeof targetMap.addControl === "function") targetMap.addControl(this);
          return this;
        }
        onAdd(targetMap) {
          if (typeof proto.onAdd === "function") return proto.onAdd.call(this, targetMap);
          return null;
        }
        onRemove(targetMap) {
          if (typeof proto.onRemove === "function") return proto.onRemove.call(this, targetMap);
          return null;
        }
      };
    }
  };

  return {
    map,
    tileLayer,
    layerGroup: vi.fn(() => new MockFeatureGroup()),
    featureGroup: vi.fn(() => new MockFeatureGroup()),
    FeatureGroup: MockFeatureGroup,
    Control,
    Draw: { Event: { CREATED: "draw:created" } },
    polygon,
    marker,
    divIcon: vi.fn((options) => ({ ...options })),
    latLngBounds: vi.fn((coords) => makeBounds(coords)),
    DomUtil,
    DomEvent,
    __maps: maps
  };
}

function createXlsxMock() {
  const utils = {
    book_new: vi.fn(() => ({ SheetNames: [], Sheets: {} })),
    json_to_sheet: vi.fn((rows) => ({ rows })),
    book_append_sheet: vi.fn((wb, ws, name) => {
      wb.SheetNames.push(name);
      wb.Sheets[name] = ws;
    })
  };
  return {
    utils,
    writeFile: vi.fn()
  };
}

function createSupabaseCampaignMock({ campaignId = "camp-1", publicCode = "SPRING01", name = "Spring Campaign" } = {}) {
  let payload = {
    campaign: {
      id: campaignId,
      name,
      public_code: publicCode,
      approved_emails: ["worker@example.com"],
      created_at: "2026-03-05T12:00:00Z",
      updated_at: "2026-03-05T12:00:00Z"
    },
    viewer: {
      signed_in: false,
      user_id: "",
      email: "",
      display_name: "",
      avatar_url: "",
      authorized: false
    },
    territories: []
  };
  const channel = {
    on: vi.fn(() => channel),
    subscribe: vi.fn((callback) => {
      if (typeof callback === "function") callback("SUBSCRIBED");
      return channel;
    }),
    unsubscribe: vi.fn()
  };
  const client = {
    rpc: vi.fn(async (rpcName, params) => {
      if (rpcName === "campaign_publish") {
        payload = {
          campaign: {
            ...payload.campaign,
            name: String(params.p_name || name),
            public_code: String(params.p_public_code || publicCode),
            approved_emails: Array.isArray(params.p_approved_emails) ? params.p_approved_emails : ["worker@example.com"],
            updated_at: "2026-03-05T12:05:00Z"
          },
          viewer: payload.viewer,
          territories: Array.isArray(params.p_snapshot)
            ? params.p_snapshot.map((territory) => ({
              ...territory,
              completed: false,
              completed_by: "",
              completed_by_user_id: "",
              completed_by_email: "",
              completed_by_avatar_url: "",
              completed_at: "",
              updated_at: "2026-03-05T12:05:00Z"
            }))
            : []
        };
      } else if (rpcName === "campaign_set_completion") {
        payload = {
          ...payload,
          territories: payload.territories.map((territory) => territory.territory_id === params.p_territory_id
            ? {
              ...territory,
              completed: !!params.p_completed,
              completed_by: params.p_completed ? "Worker Alex" : "",
              completed_by_user_id: params.p_completed ? "user-1" : "",
              completed_by_email: params.p_completed ? "worker@example.com" : "",
              completed_by_avatar_url: params.p_completed ? "https://example.com/avatar.png" : "",
              completed_at: params.p_completed ? "2026-03-05T12:10:00Z" : "",
              updated_at: "2026-03-05T12:10:00Z"
            }
            : territory)
        };
      } else if (rpcName === "campaign_load") {
        // Return latest payload as-is.
      }
      return { data: payload, error: null };
    }),
    channel: vi.fn(() => channel),
    removeChannel: vi.fn()
  };
  return { client, channel, getPayload: () => payload };
}

function bootstrap({
  initialDb = [],
  uiPrefs = {},
  datasetMeta = null,
  datasetState = "NY",
  viewMode = "selected",
  fetchImpl = null,
  packageManifestUrl = "",
  indexedDbFactory = null,
  idbKeyRangeFactory = null,
  supabaseClient = null,
  supabaseUrl = "https://example.supabase.co",
  supabaseAnonKey = "anon-key",
  runtimeConfig = null,
  appUrl = "http://localhost/Territory%20Management.html",
  autoStateInstall = true,
  openMock = null
} = {}) {
  const dom = new JSDOM(makeDomTemplate(), { runScripts: "outside-only", url: appUrl });
  const { window } = dom;
  const { storage, api } = createLocalStorage({
    db: initialDb,
    uiPrefs,
    datasetMeta,
    datasetState
  });
  storage.set(STORAGE_KEYS.viewMode, String(viewMode));
  const L = createLeafletMock();
  const XLSX = createXlsxMock();
  const defaultFetch = vi.fn(async (url) => {
    const urlText = String(url || "");
    if (/overpass-api\.de\/api\/interpreter/i.test(urlText) || /\/api\/interpreter$/i.test(urlText)) {
      return {
        ok: true,
        json: async () => ({
          elements: [
            {
              type: "node",
              id: 101,
              lat: 40.72,
              lon: -74.01,
              tags: {
                "addr:housenumber": "10",
                "addr:street": "Main St",
                "addr:postcode": "11101",
                "addr:city": "Queens",
                building: "house"
              }
            }
          ]
        }),
        text: async () => ""
      };
    }
    if (/\/api\/local-data\/state\/status\?/i.test(urlText)) {
      return {
        ok: true,
        json: async () => ({
          ok: true,
          state: "NY",
          release: "2026-01-21.0",
          phase: "ready",
          phaseDetail: "NY dataset ready.",
          progress: { current: 1, total: 1, pct: 100 },
          datasetsInstalled: { addresses: true, buildings: true },
          datasetCounts: { addresses: 1, buildings: 1 },
          strictResidentialReady: true,
          error: "",
          jobId: "test-job"
        }),
        text: async () => ""
      };
    }
    if (/\/api\/local-data\/state\/ensure$/i.test(urlText) || /\/api\/local-data\/state\/upgrade$/i.test(urlText)) {
      return {
        ok: true,
        json: async () => ({
          ok: true,
          state: "NY",
          release: "2026-01-21.0",
          phase: "ready",
          jobId: "test-job",
          cached: true
        }),
        text: async () => ""
      };
    }
    if (/\/api\/local-data\/addresses\/search$/i.test(urlText)) {
      return {
        ok: true,
        json: async () => ({
          ok: true,
          count: 1,
          rows: [
            {
              id: "addr-1",
              house_number: "10",
              street: "Main St",
              unit: "",
              city: "Queens",
              region: "NY",
              postcode: "11101",
              country_code: "US",
              full_address: "10 Main St, Queens, NY 11101",
              lat: 40.72,
              lng: -74.01
            }
          ],
          release: "2026-01-21.0",
          source: "local-open-data-cache"
        }),
        text: async () => ""
      };
    }
    return {
      ok: true,
      json: async () => ({ ok: true }),
      text: async () => ""
    };
  });
  const fetchMock = fetchImpl || defaultFetch;

  Object.defineProperty(window, "localStorage", { value: api, configurable: true });
  window.L = L;
  window.XLSX = XLSX;
  window.fetch = fetchMock;
  window.alert = vi.fn();
  window.confirm = vi.fn(() => true);
  window.prompt = vi.fn(() => "c");
  window.print = vi.fn();
  window.matchMedia = vi.fn(() => ({ matches: false, addListener: vi.fn(), removeListener: vi.fn() }));
  Object.defineProperty(window.navigator, "clipboard", {
    value: { writeText: vi.fn(async () => {}) },
    configurable: true
  });
  window.requestAnimationFrame = (cb) => {
    cb();
    return 1;
  };
  window.TERRITORY_AUTO_STATE_INSTALL = autoStateInstall ? "true" : "false";
  if (packageManifestUrl) window.TERRITORY_PACKAGE_MANIFEST_URL = packageManifestUrl;
  if (indexedDbFactory) window.indexedDB = indexedDbFactory;
  if (idbKeyRangeFactory) window.IDBKeyRange = idbKeyRangeFactory;
  if (runtimeConfig) window.TERRITORY_APP_CONFIG = runtimeConfig;
  if (openMock) window.__TERRITORY_OPEN__ = openMock;
  if (supabaseClient) {
    window.supabase = { createClient: vi.fn(() => supabaseClient) };
    window.TERRITORY_APP_CONFIG = {
      siteBaseUrl: "https://example.github.io/territory-app/",
      githubPagesBasePath: "/territory-app/",
      campaignPagePath: "campaign.html",
      managerPagePath: "Territory%20Management.html",
      localhostBaseUrl: "http://127.0.0.1:4173/",
      supabaseUrl,
      supabaseAnonKey,
      ...(runtimeConfig || {})
    };
  }

  window.eval(APP_SCRIPT);
  if (typeof window.onload === "function") window.onload();

  return { window, document: window.document, storage, fetchMock, L };
}

function drawTerritory(env, coords = [[40.71, -74.0], [40.72, -74.01], [40.73, -74.02]]) {
  const layer = {
    getLatLngs: () => [coords.map(([lat, lng]) => ({ lat, lng }))]
  };
  env.L.__maps[0].trigger(env.window.L.Draw.Event.CREATED, { layer });
}

function selectTerritory(env, id) {
  const selector = env.document.getElementById("territorySelector");
  selector.value = id;
  selector.dispatchEvent(new env.window.Event("change", { bubbles: true }));
}

function readDb(env) {
  return JSON.parse(env.storage.get(STORAGE_KEYS.db) || "[]");
}

function readFetchBody(call) {
  const [, options] = Array.isArray(call) ? call : [];
  return String(options && options.body ? options.body : "");
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function waitFor(predicate, timeoutMs = 1200) {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    if (predicate()) return true;
    await sleep(20);
  }
  return false;
}

afterEach(() => {
  vi.restoreAllMocks();
});

describe("Territory Management manager fetch mode", () => {
  it("keeps manual install/update/import controls out of the source UI", () => {
    expect(APP_HTML).not.toMatch(/id="btnInstallStateData"/i);
    expect(APP_HTML).not.toMatch(/id="btnUpdateAddressDb"/i);
    expect(APP_HTML).not.toMatch(/id="btnImportPackageBundle"/i);
    expect(APP_HTML).not.toMatch(/id="packageFilePicker"/i);
    expect(APP_HTML).not.toMatch(/id="btnStateAdvanced"/i);
    expect(APP_HTML).not.toMatch(/id="stateAdvancedPanel"/i);
    expect(APP_HTML).not.toMatch(/id="btnStateRefresh"/i);
    expect(APP_HTML).not.toMatch(/id="btnDataEnrichment"/i);
    expect(APP_HTML).not.toMatch(/id="opsActiveJob"/i);
    expect(APP_HTML).not.toMatch(/class="results-pane/i);
  });

  it("keeps the map view selector inside Territory Tools as the only source of truth", () => {
    const dom = new JSDOM(APP_HTML);
    expect(APP_HTML).not.toMatch(/id="viewModeSelector"/g);
    expect(dom.window.document.querySelector("#menuTerritoryTools #btnViewModeSelected")).not.toBeNull();
    expect(dom.window.document.querySelector("#menuTerritoryTools #btnViewModeAll")).not.toBeNull();
    expect(dom.window.document.querySelector("#menuTerritoryTools #btnViewModeCampaign")).not.toBeNull();
    expect(dom.window.document.querySelector("#stateSelector")).toBeNull();
  });

  it("renders one Map View heading without a duplicated inner label", () => {
    const dom = new JSDOM(APP_HTML);
    const menu = dom.window.document.querySelector("#menuTerritoryTools");
    const mapViewSection = Array.from(menu?.querySelectorAll(".tools-section") || [])
      .filter((el) => String(el.textContent || "").trim() === "Map View");
    const duplicateLabel = Array.from(menu?.querySelectorAll(".tools-control-label") || [])
      .filter((el) => String(el.textContent || "").trim() === "Map View");
    expect(mapViewSection).toHaveLength(1);
    expect(duplicateLabel).toHaveLength(0);
  });

  it("includes Overpass fetch support while keeping advanced local-data API paths", () => {
    expect(APP_SCRIPT).toMatch(/api\/local-data\/state\/status/i);
    expect(APP_SCRIPT).toMatch(/api\/local-data\/territories\/align\/preview/i);
    expect(APP_SCRIPT).toMatch(/api\/local-data\/territories\/align\/apply/i);
    expect(APP_SCRIPT).not.toMatch(/api\/local-data\/enrichment/i);
    expect(APP_SCRIPT).toMatch(/overpass-api\.de\/api\/interpreter/i);
    expect(APP_SCRIPT).toMatch(/TERRITORY_OVERPASS_API_URL/);
    expect(APP_SCRIPT).not.toMatch(/zippopotam\.us/i);
  });

  it("exposes overpass browser data source mode", () => {
    const env = bootstrap();
    expect(env.window.TerritoryApp.modules.dataset.getDataSourceMode()).toBe("overpass-browser");
  });

  it("removes Address Data and dataset-state chrome from the manager selection panel", () => {
    const dom = new JSDOM(APP_HTML);
    const selectionCard = dom.window.document.querySelector("#selectionCard");
    expect(String(selectionCard?.textContent || "")).not.toMatch(/Address Data:\s*NY/i);
    expect(selectionCard?.querySelector("#datasetInfo")).toBeNull();
    expect(selectionCard?.querySelector("#datasetProgress")).toBeNull();
    expect(selectionCard?.querySelector("#stateSelector")).toBeNull();
  });

  it("auto-refreshes the selected territory on startup through Overpass when data is stale", async () => {
    const fetchImpl = vi.fn(async (url, options) => {
      const urlText = String(url || "");
      if (/overpass-api\.de\/api\/interpreter/i.test(urlText)) {
        return {
          ok: true,
          json: async () => ({
            elements: [
              {
                type: "node",
                id: "addr-1",
                lat: 40.72,
                lon: -74.01,
                tags: {
                  "addr:housenumber": "10",
                  "addr:street": "Main St",
                  "addr:postcode": "11101",
                  "addr:city": "Queens",
                  building: "house"
                }
              }
            ]
          }),
          text: async () => ""
        };
      }
      return { ok: true, json: async () => ({ ok: true }), text: async () => "" };
    });
    const env = bootstrap({
      fetchImpl,
      initialDb: [
        {
          id: "t-1",
          territoryNo: "1",
          locality: "A",
          polygon: [[40.71, -74.0], [40.72, -74.01], [40.73, -74.02]],
          addresses: [],
          city: "Queens",
          state: "NY",
          zip: ""
        }
      ]
    });
    const called = await waitFor(() => fetchImpl.mock.calls.some(([url]) => /overpass-api\.de\/api\/interpreter/i.test(String(url || ""))), 1800);
    expect(called).toBe(true);
    const updated = readDb(env);
    expect(updated[0].addresses.length).toBeGreaterThan(0);
    expect(typeof updated[0].lastFetchedAt).toBe("string");
    const overpassCall = fetchImpl.mock.calls.find(([url]) => /overpass-api\.de\/api\/interpreter/i.test(String(url || "")));
    expect(decodeURIComponent(readFetchBody(overpassCall))).toContain("out center tags qt;");
  });

  it("builds an Overpass polygon query when refreshing the selected territory", async () => {
    const freshFetchedAt = new Date().toISOString();
    const fetchImpl = vi.fn(async (url) => {
      const urlText = String(url || "");
      if (/overpass-api\.de\/api\/interpreter/i.test(urlText)) {
        return {
          ok: true,
          json: async () => ({
            elements: []
          }),
          text: async () => ""
        };
      }
      return { ok: true, json: async () => ({ ok: true }), text: async () => "" };
    });
    const env = bootstrap({
      fetchImpl,
      initialDb: [
        {
          id: "t-1",
          territoryNo: "1",
          locality: "A",
          polygon: [[40.71, -74.0], [40.72, -74.01], [40.73, -74.02]],
          addresses: [{ full: "Existing Address, Queens, NY 11101", zip: "11101" }],
          city: "Queens",
          state: "NY",
          zip: "11101",
          lastFetchedAt: freshFetchedAt
        }
      ]
    });
    selectTerritory(env, "t-1");
    env.document.getElementById("btnFetch").click();
    await waitFor(() => fetchImpl.mock.calls.some(([url]) => /overpass-api\.de\/api\/interpreter/i.test(String(url || ""))));
    const overpassCall = fetchImpl.mock.calls.find(([url]) => /overpass-api\.de\/api\/interpreter/i.test(String(url || "")));
    expect(overpassCall).toBeTruthy();
    const decodedBody = decodeURIComponent(readFetchBody(overpassCall));
    expect(decodedBody).toContain('node(poly:"40.7100000 -74.0000000');
    expect(decodedBody).toContain("way(poly:");
    expect(decodedBody).toContain("relation(poly:");
    expect(decodedBody).toContain("out center tags qt;");
  });

  it("allows overriding the Overpass endpoint from window.TERRITORY_OVERPASS_API_URL", async () => {
    const freshFetchedAt = new Date().toISOString();
    const fetchImpl = vi.fn(async (url) => {
      const urlText = String(url || "");
      if (/overpass\.example\.test\/api\/interpreter/i.test(urlText)) {
        return {
          ok: true,
          json: async () => ({ elements: [] }),
          text: async () => ""
        };
      }
      return { ok: true, json: async () => ({ ok: true }), text: async () => "" };
    });
    const env = bootstrap({
      fetchImpl,
      initialDb: [
        {
          id: "t-1",
          territoryNo: "1",
          locality: "A",
          polygon: [[40.71, -74.0], [40.72, -74.01], [40.73, -74.02]],
          addresses: [{ full: "Existing Address, Queens, NY 11101", zip: "11101" }],
          city: "Queens",
          state: "NY",
          zip: "11101",
          lastFetchedAt: freshFetchedAt
        }
      ]
    });
    env.window.TERRITORY_OVERPASS_API_URL = "https://overpass.example.test/api/interpreter";
    selectTerritory(env, "t-1");
    env.document.getElementById("btnFetch").click();
    await waitFor(() => fetchImpl.mock.calls.some(([url]) => /overpass\.example\.test\/api\/interpreter/i.test(String(url || ""))));
    expect(fetchImpl.mock.calls.some(([url]) => /overpass\.example\.test\/api\/interpreter/i.test(String(url || "")))).toBe(true);
  });

  it("filters refresh results to in-boundary residential addresses and excludes commercial rows", async () => {
    const freshFetchedAt = new Date().toISOString();
    const fetchImpl = vi.fn(async (url) => {
      const urlText = String(url || "");
      if (/overpass-api\.de\/api\/interpreter/i.test(urlText)) {
        return {
          ok: true,
          json: async () => ({
            elements: [
              {
                type: "node",
                id: 1,
                lat: 40.72,
                lon: -74.01,
                tags: {
                  "addr:housenumber": "10",
                  "addr:street": "Main St",
                  "addr:postcode": "11101",
                  "addr:city": "Queens",
                  building: "house"
                }
              },
              {
                type: "node",
                id: 2,
                lat: 40.76,
                lon: -74.04,
                tags: {
                  "addr:housenumber": "99",
                  "addr:street": "Outside Ave",
                  "addr:postcode": "11101",
                  "addr:city": "Queens",
                  building: "house"
                }
              },
              {
                type: "node",
                id: 3,
                lat: 40.721,
                lon: -74.011,
                tags: {
                  "addr:housenumber": "22",
                  "addr:street": "Shop Rd",
                  "addr:postcode": "11101",
                  "addr:city": "Queens",
                  shop: "supermarket"
                }
              },
              {
                type: "node",
                id: 4,
                lat: 40.722,
                lon: -74.012,
                tags: {
                  "addr:housenumber": "40",
                  "addr:street": "Mixed Use Ln",
                  "addr:postcode": "11101",
                  "addr:city": "Queens",
                  shop: "bakery",
                  building: "apartments"
                }
              }
            ]
          }),
          text: async () => ""
        };
      }
      return { ok: true, json: async () => ({ ok: true }), text: async () => "" };
    });
    const env = bootstrap({
      fetchImpl,
      initialDb: [
        {
          id: "t-1",
          territoryNo: "1",
          locality: "A",
          polygon: [[40.71, -74.0], [40.72, -74.02], [40.73, -74.01]],
          addresses: [{ full: "Existing Address, Queens, NY 11101", zip: "11101" }],
          city: "Queens",
          state: "NY",
          zip: "11101",
          lastFetchedAt: freshFetchedAt
        }
      ]
    });
    selectTerritory(env, "t-1");
    env.document.getElementById("btnFetch").click();
    await waitFor(() => readDb(env)[0].addresses.length === 2);
    const addresses = readDb(env)[0].addresses.map((row) => row.full);
    expect(addresses).toEqual([
      "10 Main St, Queens, NY 11101",
      "40 Mixed Use Ln, Queens, NY 11101"
    ]);
  });

  it("clears stored addresses on a successful empty Overpass result", async () => {
    const freshFetchedAt = new Date().toISOString();
    const fetchImpl = vi.fn(async (url) => {
      const urlText = String(url || "");
      if (/overpass-api\.de\/api\/interpreter/i.test(urlText)) {
        return {
          ok: true,
          json: async () => ({ elements: [] }),
          text: async () => ""
        };
      }
      return { ok: true, json: async () => ({ ok: true }), text: async () => "" };
    });
    const env = bootstrap({
      fetchImpl,
      initialDb: [
        {
          id: "t-1",
          territoryNo: "1",
          locality: "A",
          polygon: [[40.71, -74.0], [40.72, -74.02], [40.73, -74.01]],
          addresses: [{ full: "10 Main St, Queens, NY 11101", zip: "11101" }],
          city: "Queens",
          state: "NY",
          zip: "11101",
          lastFetchedAt: freshFetchedAt
        }
      ]
    });
    selectTerritory(env, "t-1");
    env.document.getElementById("btnFetch").click();
    await waitFor(() => /No residential addresses found inside this boundary\./i.test(String(env.document.getElementById("status")?.textContent || "")));
    expect(readDb(env)[0].addresses).toEqual([]);
  });

  it("preserves stored addresses when the Overpass refresh fails", async () => {
    const freshFetchedAt = new Date().toISOString();
    const fetchImpl = vi.fn(async (url) => {
      const urlText = String(url || "");
      if (/overpass-api\.de\/api\/interpreter/i.test(urlText)) {
        return {
          ok: false,
          status: 429,
          json: async () => ({ remark: "rate limit" }),
          text: async () => "rate limit"
        };
      }
      return { ok: true, json: async () => ({ ok: true }), text: async () => "" };
    });
    const env = bootstrap({
      fetchImpl,
      initialDb: [
        {
          id: "t-1",
          territoryNo: "1",
          locality: "A",
          polygon: [[40.71, -74.0], [40.72, -74.02], [40.73, -74.01]],
          addresses: [{ full: "10 Main St, Queens, NY 11101", zip: "11101" }],
          city: "Queens",
          state: "NY",
          zip: "11101",
          lastFetchedAt: freshFetchedAt
        }
      ]
    });
    selectTerritory(env, "t-1");
    env.document.getElementById("btnFetch").click();
    await waitFor(() => /Address refresh failed\./i.test(String(env.document.getElementById("status")?.textContent || "")));
    expect(readDb(env)[0].addresses).toEqual([{ full: "10 Main St, Queens, NY 11101", zip: "11101" }]);
  });

  it("allows file:// mode when local data api is reachable", async () => {
    const env = bootstrap({
      appUrl: "file:///D:/Code%20Projects/Territory%20App/Territory%20Management.html",
      autoStateInstall: true
    });
    const result = await env.window.TerritoryApp.modules.dataset.ensureStateDatasetReady("NY", { silent: true });
    expect(result.ready).toBe(true);
    expect(String(result.error || "")).toBe("");
  });

  it("runs full backfill once per installed release marker", async () => {
    const env = bootstrap({
      datasetMeta: {
        NY: {
          state: "NY",
          release: "2026-01-21.0",
          count: 100,
          strictResidentialReady: true,
          datasetsInstalled: { addresses: true, buildings: true }
        }
      },
      datasetState: "NY"
    });
    const first = await env.window.TerritoryApp.modules.dataset.ensureStateDatasetReady("NY", { silent: true });
    expect(first.ready).toBe(true);
    expect(first.backfill && first.backfill.ran).toBe(true);

    const second = await env.window.TerritoryApp.modules.dataset.ensureStateDatasetReady("NY", { silent: true });
    expect(second.ready).toBe(true);
    expect(second.backfill && second.backfill.ran).toBe(false);
    expect(second.backfill && second.backfill.reason).toBe("already_completed");
  });

  it("builds and parses territory backup payloads", () => {
    const env = bootstrap({
      initialDb: [
        {
          id: "t-1",
          territoryNo: "12",
          locality: "Baisley Park",
          polygon: [[40.701, -73.8], [40.702, -73.799], [40.703, -73.801]],
          addresses: [{ full: "12 Main St, Queens, NY 11434", zip: "11434" }],
          city: "Queens",
          state: "NY",
          zip: "11434"
        }
      ]
    });
    const modules = env.window.TerritoryApp.modules;
    const payload = modules.persistence.buildTerritoryBackupPayload();
    expect(payload.format).toBe("territory-backup-v1");
    expect(payload.schemaVersion).toBe(2);
    expect(Array.isArray(payload.territories)).toBe(true);
    expect(payload.territories.length).toBe(1);
    const parsed = modules.persistence.parseTerritoryBackupPayload(payload);
    expect(Array.isArray(parsed.territories)).toBe(true);
    expect(parsed.territories.length).toBe(1);
    expect(parsed.report.schemaVersion).toBe(2);
    expect(parsed.territories[0].territoryNo).toBe("12");
    expect(parsed.territories[0].addresses[0].full).toContain("Main St");
  });

  it("aligns neighboring territory boundaries without deleting territories", () => {
    const env = bootstrap({
      initialDb: [
        {
          id: "ta",
          territoryNo: "1",
          locality: "A",
          polygon: [[40.7, -73.01], [40.7, -73.0], [40.701, -73.0], [40.701, -73.01]],
          addresses: [],
          city: "Queens",
          state: "NY",
          zip: ""
        },
        {
          id: "tb",
          territoryNo: "2",
          locality: "B",
          polygon: [[40.7, -72.99999], [40.7, -72.99], [40.701, -72.99], [40.701, -72.99999]],
          addresses: [],
          city: "Queens",
          state: "NY",
          zip: ""
        }
      ]
    });
    const modules = env.window.TerritoryApp.modules;
    const before = readDb(env);
    const beforeGap = modules.geometry.approxDistanceMeters(before[0].polygon[1], before[1].polygon[0]);
    const result = modules.geometry.alignTerritoryBoundaries(undefined, {
      vertexToleranceMeters: 2.5,
      edgeToleranceMeters: 1.5,
      maxAreaDriftPct: 2
    });
    modules.persistence.saveDb();
    const after = readDb(env);
    const afterGap = modules.geometry.approxDistanceMeters(after[0].polygon[1], after[1].polygon[0]);
    expect(typeof result.changed).toBe("boolean");
    expect(result.territoriesChanged).toBeGreaterThanOrEqual(0);
    expect(after.length).toBe(2);
    expect(afterGap).toBeLessThanOrEqual(beforeGap);
  });

  it("sorts alphanumeric territory numbers with New-* territories last", () => {
    const env = bootstrap();
    const rows = [
      { territoryNo: "New-61" },
      { territoryNo: "12" },
      { territoryNo: "2" },
      { territoryNo: "12A" }
    ];
    const sorted = env.window.TerritoryApp.modules.territory.getSortedTerritories(rows);
    expect(sorted.map((row) => row.territoryNo)).toEqual(["2", "12", "12A", "New-61"]);
  });

  it("switches to all-territories mode, fits the full dataset, and renders every territory label", async () => {
    const env = bootstrap({
      initialDb: [
        {
          id: "t-1",
          territoryNo: "1",
          locality: "Alpha",
          polygon: [[40.70, -74.01], [40.70, -74.00], [40.71, -74.00], [40.71, -74.01]],
          addresses: []
        },
        {
          id: "t-2",
          territoryNo: "2",
          locality: "Beta",
          polygon: [[40.72, -74.03], [40.72, -74.02], [40.73, -74.02], [40.73, -74.03]],
          addresses: []
        },
        {
          id: "t-3",
          territoryNo: "New-61",
          locality: "Gamma",
          polygon: [[40.74, -74.05], [40.74, -74.04], [40.75, -74.04], [40.75, -74.05]],
          addresses: []
        }
      ],
      viewMode: "selected"
    });
    env.L.__maps[0].fitBounds.mockClear();
    env.L.__maps[0].flyToBounds.mockClear();

    const toggle = env.document.getElementById("btnViewModeAll");
    toggle.click();
    await sleep(0);

    expect(env.document.getElementById("mapLegend")?.textContent).toMatch(/All territories - 3 loaded/i);
    expect(env.document.querySelectorAll(".territory-label-node").length).toBeGreaterThanOrEqual(3);
    const fitCalls = env.L.__maps[0].fitBounds.mock.calls.length + env.L.__maps[0].flyToBounds.mock.calls.length;
    expect(fitCalls).toBeGreaterThan(0);
  });

  it("opens Territory Tools with map view focused and does not hijack select arrow keys", () => {
    const env = bootstrap();
    const trigger = env.document.getElementById("btnToolsMenu");
    const selector = env.document.getElementById("btnViewModeSelected");
    const nextButton = env.document.getElementById("btnViewModeAll");
    const territorySelector = env.document.getElementById("territorySelector");

    trigger.click();

    expect(trigger.getAttribute("aria-expanded")).toBe("true");
    expect(env.document.activeElement).toBe(selector);

    selector.dispatchEvent(new env.window.KeyboardEvent("keydown", { key: "ArrowDown", bubbles: true }));
    expect(env.document.activeElement).toBe(nextButton);

    territorySelector.focus();
    territorySelector.dispatchEvent(new env.window.KeyboardEvent("keydown", { key: "ArrowDown", bubbles: true }));
    expect(env.document.activeElement).toBe(territorySelector);
  });

  it("auto-repairs invalid manual label anchors on bootstrap", () => {
    const invalidAnchor = { lat: 40.75, lng: -74.2 };
    const env = bootstrap({
      initialDb: [
        {
          id: "t-1",
          territoryNo: "1",
          locality: "Alpha",
          polygon: [[40.70, -74.01], [40.70, -74.00], [40.71, -74.00], [40.71, -74.01]],
          labelAnchor: invalidAnchor,
          addresses: []
        }
      ]
    });
    const territory = readDb(env)[0];
    const anchor = territory.labelAnchor;

    expect(anchor).toBeTruthy();
    expect(anchor).not.toEqual(invalidAnchor);
    expect(env.window.TerritoryApp.modules.geometry.pointInPolygon([anchor.lat, anchor.lng], territory.polygon)).toBe(true);
  });

  it("preserves valid manual label anchors on bootstrap", () => {
    const validAnchor = { lat: 40.705, lng: -74.005 };
    const env = bootstrap({
      initialDb: [
        {
          id: "t-1",
          territoryNo: "1",
          locality: "Alpha",
          polygon: [[40.70, -74.01], [40.70, -74.00], [40.71, -74.00], [40.71, -74.01]],
          labelAnchor: validAnchor,
          addresses: []
        }
      ]
    });

    expect(readDb(env)[0].labelAnchor).toEqual(validAnchor);
  });

  it("stores immutable S-12 print blueprint metadata with the fixed overlay window", () => {
    const env = bootstrap({
      initialDb: [
        {
          id: "t-12",
          territoryNo: "12",
          locality: "Baisley Park",
          polygon: [[40.701, -73.801], [40.701, -73.791], [40.711, -73.791], [40.711, -73.801]],
          labelAnchor: { lat: 40.706, lng: -73.796 },
          addresses: []
        }
      ]
    });
    const modules = env.window.TerritoryApp.modules;
    const territory = readDb(env)[0];
    const printArea = env.document.getElementById("print-area");
    modules.print.applyS12LayoutVariables(printArea);

    expect(printArea.style.getPropertyValue("--s12-overlay-width")).not.toBe("");
    expect(printArea.style.getPropertyValue("--s12-overlay-height")).not.toBe("");

    const markup = modules.print.buildPrintCardMarkup(territory);
    expect(markup).toContain("s12-template-layer");
    expect(markup).toContain("s12-overlay-frame");
    expect(markup).toContain("S-12 territory overlay");

    const template = env.window.getTerritoryCardTemplate();
    expect(template.assetUrl).toContain("S-12alternate-E.pdf");
    expect(template.overlay.widthIn).toBeGreaterThan(0);
    expect(template.overlay.heightIn).toBeGreaterThan(0);

    const blueprint = modules.print.saveCardBlueprint({
      territory,
      bounds: env.window.L.latLngBounds([[40.701, -73.801], [40.711, -73.791]]),
      createdAtIso: "2026-03-05T12:00:00Z",
      selectedLabel: { territoryId: territory.id, text: territory.territoryNo, anchor: territory.labelAnchor },
      allLabels: [],
      printRenderSpec: {
        templateId: template.id,
        templateVersion: template.version,
        templateAsset: template.assetUrl,
        overlay: template.overlay
      }
    });

    expect(blueprint.template.assetUrl).toContain("S-12alternate-E.pdf");
    expect(blueprint.fit.overlay).toEqual(template.overlay);
    expect(blueprint.printRenderSpec.templateVersion).toBe(template.version);

    const stored = JSON.parse(env.storage.get(STORAGE_KEYS.cardBlueprints) || "[]");
    expect(stored[0].fit.overlay).toEqual(template.overlay);
  });

  it("removes Google Fonts in favor of local typography assets", () => {
    expect(APP_HTML).not.toMatch(/fonts\.googleapis/i);
    expect(APP_HTML).toMatch(/IBM Plex Sans/);
    expect(APP_HTML).toMatch(/assets\/fonts\/ibm-plex-sans-latin-400-normal\.woff2/);
  });

  it("keeps campaign mode in local preview when Supabase config is missing", async () => {
    const env = bootstrap({
      initialDb: [
        {
          id: "t-1",
          territoryNo: "1",
          locality: "Alpha",
          polygon: [[40.70, -74.01], [40.70, -74.00], [40.71, -74.00], [40.71, -74.01]],
          labelAnchor: { lat: 40.705, lng: -74.005 },
          addresses: []
        }
      ],
      viewMode: "campaign"
    });

    expect(env.document.getElementById("campaignSyncStatus")?.textContent).toMatch(/open local campaign/i);
    expect(env.document.getElementById("btnCampaignOpenLocal")?.disabled).toBe(false);
    expect(env.document.getElementById("btnCampaignPublish")?.disabled).toBe(true);
  });

  it("hides address and local-data workflow chrome in campaign mode so the full workspace fits", async () => {
    const env = bootstrap({
      initialDb: [
        {
          id: "t-1",
          territoryNo: "1",
          locality: "Alpha",
          polygon: [[40.70, -74.01], [40.70, -74.00], [40.71, -74.00], [40.71, -74.01]],
          labelAnchor: { lat: 40.705, lng: -74.005 },
          addresses: [{ full: "1 Main St", zip: "11111" }]
        }
      ]
    });

    selectTerritory(env, "t-1");
    env.document.getElementById("btnViewModeCampaign").click();
    await sleep(0);

    expect(env.document.getElementById("mainLayout")?.classList.contains("is-campaign")).toBe(true);
    expect(env.document.getElementById("campaignCard")?.hidden).toBe(false);
    expect(env.document.getElementById("selectionInputs")?.hidden).toBe(true);
    expect(env.document.getElementById("selectionDataTools")).toBeNull();
    expect(env.document.getElementById("selectionActions")?.hidden).toBe(true);
    expect(env.document.getElementById("status")?.hidden).toBe(true);
    expect(env.document.getElementById("addressCard")?.hidden).toBe(true);
    expect(env.document.getElementById("territory-stats")?.textContent).toMatch(/hidden in campaign mode/i);
  });

  it("shows a same-computer local campaign link without localhost requirements", async () => {
    const env = bootstrap({
      initialDb: [
        {
          id: "t-1",
          territoryNo: "1",
          locality: "Alpha",
          polygon: [[40.70, -74.01], [40.70, -74.00], [40.71, -74.00], [40.71, -74.01]],
          labelAnchor: { lat: 40.705, lng: -74.005 },
          addresses: []
        }
      ],
      viewMode: "campaign"
    });

    const codeInput = env.document.getElementById("campaignPublicCode");
    codeInput.value = "SPRING42";
    codeInput.dispatchEvent(new env.window.Event("input", { bubbles: true }));

    expect(env.document.getElementById("campaignSyncStatus")?.textContent).toMatch(/same-computer worker flow/i);
    expect(env.document.getElementById("campaignLocalLink")?.value).toBe("http://localhost/campaign.html?mode=local&campaign=SPRING42");
    expect(env.document.getElementById("btnCampaignCopyLocalLink")?.disabled).toBe(false);
  });

  it("opens a local campaign, persists its snapshot, and exposes it through the local campaign module", async () => {
    const openMock = vi.fn();
    const env = bootstrap({
      initialDb: [
        {
          id: "t-1",
          territoryNo: "1",
          locality: "Alpha",
          polygon: [[40.70, -74.01], [40.70, -74.00], [40.71, -74.00], [40.71, -74.01]],
          labelAnchor: { lat: 40.705, lng: -74.005 },
          addresses: []
        }
      ],
      viewMode: "campaign",
      openMock
    });
    const modules = env.window.TerritoryApp.modules;

    env.document.getElementById("campaignName").value = "Spring 42";
    env.document.getElementById("campaignPublicCode").value = "SPRING42";
    env.document.getElementById("campaignPublicCode").dispatchEvent(new env.window.Event("input", { bubbles: true }));

    env.document.getElementById("btnCampaignOpenLocal").click();
    await sleep(0);

    expect(openMock).toHaveBeenCalledWith("http://localhost/campaign.html?mode=local&campaign=SPRING42");
    const stored = JSON.parse(env.storage.get(STORAGE_KEYS.localCampaigns) || "{}");
    expect(stored.SPRING42.campaign.name).toBe("Spring 42");
    expect(stored.SPRING42.viewer.userId).toMatch(/^local:SPRING42:/);
    expect(Array.isArray(stored.SPRING42.territories)).toBe(true);
    expect(stored.SPRING42.territories).toHaveLength(1);
    expect(modules.localCampaign.readLocalCampaignSnapshot("SPRING42")?.campaign.mode).toBe("local");
  });

  it("preserves the local campaign viewer when the master app republishes the snapshot", async () => {
    const env = bootstrap({
      initialDb: [
        {
          id: "t-1",
          territoryNo: "1",
          locality: "Alpha",
          polygon: [[40.70, -74.01], [40.70, -74.00], [40.71, -74.00], [40.71, -74.01]],
          labelAnchor: { lat: 40.705, lng: -74.005 },
          addresses: []
        }
      ],
      viewMode: "campaign"
    });
    const modules = env.window.TerritoryApp.modules;

    await modules.localCampaign.publishLocalCampaignSnapshot({
      name: "Spring 42",
      publicCode: "SPRING42"
    });

    const firstSnapshot = modules.localCampaign.readLocalCampaignSnapshot("SPRING42");
    await modules.localCampaign.writeLocalCampaignSnapshot({
      ...firstSnapshot,
      viewer: {
        ...firstSnapshot.viewer,
        displayName: "Jordan Lee"
      }
    }, {
      render: false,
      announce: false
    });

    const storedBeforeRepublish = modules.localCampaign.readLocalCampaignSnapshot("SPRING42");

    await modules.localCampaign.publishLocalCampaignSnapshot({
      name: "Spring 42",
      publicCode: "SPRING42"
    });

    const storedAfterRepublish = modules.localCampaign.readLocalCampaignSnapshot("SPRING42");
    expect(storedAfterRepublish?.viewer.displayName).toBe("Jordan Lee");
    expect(storedAfterRepublish?.viewer.userId).toBe(storedBeforeRepublish?.viewer.userId);
  });

  it("updates local campaign completion state without Supabase", async () => {
    const env = bootstrap({
      initialDb: [
        {
          id: "t-1",
          territoryNo: "1",
          locality: "Alpha",
          polygon: [[40.70, -74.01], [40.70, -74.00], [40.71, -74.00], [40.71, -74.01]],
          labelAnchor: { lat: 40.705, lng: -74.005 },
          addresses: []
        },
        {
          id: "t-2",
          territoryNo: "2",
          locality: "Beta",
          polygon: [[40.72, -74.03], [40.72, -74.02], [40.73, -74.02], [40.73, -74.03]],
          labelAnchor: { lat: 40.725, lng: -74.025 },
          addresses: []
        }
      ],
      viewMode: "campaign"
    });
    const modules = env.window.TerritoryApp.modules;

    await modules.localCampaign.publishLocalCampaignSnapshot({
      name: "Spring 42",
      publicCode: "SPRING42"
    });
    const initialSnapshot = modules.localCampaign.readLocalCampaignSnapshot("SPRING42");
    await modules.localCampaign.writeLocalCampaignSnapshot({
      ...initialSnapshot,
      viewer: {
        ...initialSnapshot.viewer,
        displayName: "Jordan Lee"
      }
    }, {
      render: false,
      announce: false
    });
    await modules.localCampaign.setLocalCampaignTerritoryCompletion("t-1", true, { render: false });

    const storedSnapshot = modules.localCampaign.readLocalCampaignSnapshot("SPRING42");
    expect(env.document.getElementById("kpiCampaignCompleted")?.textContent).toBe("1");
    expect(env.document.getElementById("kpiCampaignRemaining")?.textContent).toBe("1");
    expect(storedSnapshot?.territories.find((territory) => territory.territoryId === "t-1")?.completedBy).toBe("Jordan Lee");
    expect(storedSnapshot?.territories.find((territory) => territory.territoryId === "t-1")?.completedByUserId).toBe(storedSnapshot?.viewer.userId);
    expect(env.document.getElementById("campaignSelectedStatus")?.textContent).toMatch(/Completed by Jordan Lee/i);
  });




  it("keeps assignment record text free of mojibake separators", () => {
    expect(APP_HTML).not.toMatch(/â€¢|â€”|â€“|â€"/);
  });

  it("publishes campaign snapshots with approved emails and reflects roster data in campaign mode", async () => {
    const supabase = createSupabaseCampaignMock({
      campaignId: "camp-42",
      publicCode: "SPRING42",
      name: "Spring 42"
    });
    const env = bootstrap({
      initialDb: [
        {
          id: "t-1",
          territoryNo: "1",
          locality: "Alpha",
          polygon: [[40.70, -74.01], [40.70, -74.00], [40.71, -74.00], [40.71, -74.01]],
          labelAnchor: { lat: 40.705, lng: -74.005 },
          addresses: []
        },
        {
          id: "t-2",
          territoryNo: "New-61",
          locality: "Beta",
          polygon: [[40.72, -74.03], [40.72, -74.02], [40.73, -74.02], [40.73, -74.03]],
          labelAnchor: { lat: 40.725, lng: -74.025 },
          addresses: []
        }
      ],
      viewMode: "campaign",
      supabaseClient: supabase.client
    });
    const modules = env.window.TerritoryApp.modules;

    env.document.getElementById("campaignName").value = "Spring 42";
    env.document.getElementById("campaignPublicCode").value = "SPRING42";
    env.document.getElementById("campaignApprovedEmails").value = "worker@example.com\nhelper@example.com";

    await modules.campaign.publishCampaignSnapshot({
      name: "Spring 42",
      publicCode: "SPRING42",
      approvedEmails: ["worker@example.com", "helper@example.com"]
    });

    expect(supabase.client.rpc.mock.calls.some(([name]) => name === "campaign_publish")).toBe(true);
    expect(supabase.client.rpc.mock.calls.find(([name]) => name === "campaign_publish")?.[1]?.p_approved_emails).toEqual(["worker@example.com", "helper@example.com"]);
    expect(supabase.client.channel).toHaveBeenCalled();
    expect(env.document.getElementById("kpiCampaignTotal")?.textContent).toBe("2");
    expect(env.document.getElementById("kpiCampaignCompleted")?.textContent).toBe("0");
    expect(env.document.getElementById("campaignPublicLink")?.value).toBe("https://example.github.io/territory-app/campaign.html?mode=live&campaign=SPRING42");

    await modules.campaign.setCampaignTerritoryCompletion("t-1", true);

    expect(supabase.client.rpc.mock.calls.some(([name]) => name === "campaign_set_completion")).toBe(true);
    expect(env.document.getElementById("kpiCampaignCompleted")?.textContent).toBe("1");
    expect(env.document.getElementById("kpiCampaignRemaining")?.textContent).toBe("1");
    expect(env.document.getElementById("kpiCampaignProgress")?.textContent).toBe("50%");
    expect(env.document.getElementById("campaignSelectedStatus")?.textContent).toMatch(/Completed by Worker Alex/i);
    expect(env.document.getElementById("campaignRoster")?.textContent).toMatch(/worker@example\.com/i);
    expect(env.document.getElementById("btnCampaignReopenSelected")?.disabled).toBe(false);
  });
});


