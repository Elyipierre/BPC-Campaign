import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { JSDOM } from "jsdom";
import { afterEach, describe, expect, it, vi } from "vitest";

const THIS_DIR = path.dirname(fileURLToPath(import.meta.url));
const CAMPAIGN_HTML_PATH = path.resolve(THIS_DIR, "../campaign.html");
const CAMPAIGN_HTML = fs.readFileSync(CAMPAIGN_HTML_PATH, "utf8");
const CAMPAIGN_SCRIPT = extractInlineScript(CAMPAIGN_HTML);
const STORAGE_KEY_DB = "terr_final_db";
const STORAGE_KEY_LOCAL_CAMPAIGNS = "terr_local_campaigns_v1";

function extractInlineScript(html) {
  const scriptRegex = /<script(?![^>]*\bsrc=)[^>]*>([\s\S]*?)<\/script>/gi;
  const matches = [...html.matchAll(scriptRegex)];
  if (!matches.length) throw new Error("Unable to locate inline campaign script.");
  return matches[matches.length - 1][1];
}

function createLocalStorage({ db = [], localCampaigns = {} } = {}) {
  const storage = new Map([[STORAGE_KEY_DB, JSON.stringify(db)]]);
  if (localCampaigns && Object.keys(localCampaigns).length) {
    storage.set(STORAGE_KEY_LOCAL_CAMPAIGNS, JSON.stringify(localCampaigns));
  }
  return {
    getItem: vi.fn((key) => (storage.has(key) ? storage.get(key) : null)),
    setItem: vi.fn((key, value) => storage.set(key, String(value))),
    removeItem: vi.fn((key) => storage.delete(key)),
    clear: vi.fn(() => storage.clear())
  };
}

function cloneJson(value) {
  return JSON.parse(JSON.stringify(value));
}

function createLeafletMock() {
  const maps = [];
  const polygons = [];
  const BASE_SCALE = 14000;

  class MockGroup {
    constructor() {
      this.layers = [];
    }
    addTo() {
      return this;
    }
    addLayer(layer) {
      this.layers.push(layer);
      return this;
    }
    clearLayers() {
      this.layers = [];
    }
  }

  function makeBounds(latlngs) {
    const points = Array.isArray(latlngs) ? latlngs : [];
    const lats = points.map((point) => Number(Array.isArray(point) ? point[0] : point?.lat)).filter(Number.isFinite);
    const lngs = points.map((point) => Number(Array.isArray(point) ? point[1] : point?.lng)).filter(Number.isFinite);
    const minLat = lats.length ? Math.min(...lats) : 0;
    const maxLat = lats.length ? Math.max(...lats) : 0;
    const minLng = lngs.length ? Math.min(...lngs) : 0;
    const maxLng = lngs.length ? Math.max(...lngs) : 0;
    return {
      getSouthWest: () => ({ lat: minLat, lng: minLng }),
      getNorthEast: () => ({ lat: maxLat, lng: maxLng }),
      getCenter: () => ({ lat: minLat + ((maxLat - minLat) / 2), lng: minLng + ((maxLng - minLng) / 2) }),
      pad: () => makeBounds(latlngs)
    };
  }

  function getScaleForZoom(zoom) {
    return BASE_SCALE * Math.pow(2, Number(zoom || 13) - 13);
  }

  const L = {
    __maps: maps,
    __polygons: polygons,
    map: vi.fn((id) => {
      const panes = {};
      const listeners = {};
      let currentZoom = 13;
      let currentCenter = { lat: 40.665, lng: -73.78 };
      const mapObj = {
        id,
        setView: vi.fn((_center, zoom) => {
          if (_center && Number.isFinite(Number(_center.lat)) && Number.isFinite(Number(_center.lng))) {
            currentCenter = { lat: Number(_center.lat), lng: Number(_center.lng) };
          } else if (Array.isArray(_center) && _center.length >= 2) {
            currentCenter = { lat: Number(_center[0]), lng: Number(_center[1]) };
          }
          if (typeof zoom === "number") currentZoom = zoom;
          return mapObj;
        }),
        getMaxZoom: vi.fn(() => 19),
        getZoom: vi.fn(() => currentZoom),
        getCenter: vi.fn(() => currentCenter),
        setZoom: vi.fn((zoom) => {
          currentZoom = zoom;
          return mapObj;
        }),
        createPane: vi.fn((name) => {
          panes[name] = { style: {}, classList: { add: vi.fn() } };
          return panes[name];
        }),
        getPane: vi.fn((name) => {
          if (!panes[name]) panes[name] = { style: {}, classList: { add: vi.fn() } };
          return panes[name];
        }),
        getBoundsZoom: vi.fn((bounds, _inside, padding) => {
          const sw = bounds && typeof bounds.getSouthWest === "function" ? bounds.getSouthWest() : { lat: 0, lng: 0 };
          const ne = bounds && typeof bounds.getNorthEast === "function" ? bounds.getNorthEast() : { lat: 0, lng: 0 };
          const size = mapObj.getSize();
          const paddingX = Number(padding && padding.x) || 0;
          const paddingY = Number(padding && padding.y) || 0;
          const availableWidth = Math.max(1, size.x - (paddingX * 2));
          const availableHeight = Math.max(1, size.y - (paddingY * 2));
          const lngSpan = Math.max(Math.abs(Number(ne.lng) - Number(sw.lng)), 1e-9);
          const latSpan = Math.max(Math.abs(Number(ne.lat) - Number(sw.lat)), 1e-9);
          const maxScale = Math.min(availableWidth / lngSpan, availableHeight / latSpan);
          return Math.max(0, Math.floor(13 + Math.log2(maxScale / BASE_SCALE)));
        }),
        fitBounds: vi.fn((bounds, options = {}) => {
          const sw = bounds && typeof bounds.getSouthWest === "function" ? bounds.getSouthWest() : { lat: 0, lng: 0 };
          const ne = bounds && typeof bounds.getNorthEast === "function" ? bounds.getNorthEast() : { lat: 0, lng: 0 };
          const padding = Array.isArray(options.padding)
            ? { x: Number(options.padding[0]) || 0, y: Number(options.padding[1]) || 0 }
            : { x: Number(options.padding) || 0, y: Number(options.padding) || 0 };
          currentCenter = {
            lat: Number(sw.lat) + ((Number(ne.lat) - Number(sw.lat)) / 2),
            lng: Number(sw.lng) + ((Number(ne.lng) - Number(sw.lng)) / 2)
          };
          currentZoom = Math.min(Number(options.maxZoom) || 19, mapObj.getBoundsZoom(bounds, false, padding));
          return mapObj;
        }),
        invalidateSize: vi.fn(),
        on: vi.fn((event, callback) => {
          listeners[event] = callback;
          return mapObj;
        }),
        getSize: vi.fn(() => ({ x: 1280, y: 820 })),
        latLngToContainerPoint: vi.fn((latlng) => {
          const lat = Number(Array.isArray(latlng) ? latlng[0] : latlng?.lat);
          const lng = Number(Array.isArray(latlng) ? latlng[1] : latlng?.lng);
          const scale = getScaleForZoom(currentZoom);
          return {
            x: Math.round((mapObj.getSize().x / 2) + ((lng - currentCenter.lng) * scale)),
            y: Math.round((mapObj.getSize().y / 2) + ((currentCenter.lat - lat) * scale))
          };
        })
      };
      maps.push(mapObj);
      return mapObj;
    }),
    tileLayer: vi.fn(() => ({
      addTo: vi.fn().mockReturnThis()
    })),
    featureGroup: vi.fn(() => new MockGroup()),
    layerGroup: vi.fn(() => new MockGroup()),
    polygon: vi.fn((latlngs, options) => {
      const listeners = {};
      const polygon = {
        latlngs,
        options,
        addTo(target) {
          if (target && typeof target.addLayer === "function") target.addLayer(polygon);
          return polygon;
        },
        on: vi.fn((event, callback) => {
          listeners[event] = callback;
          return polygon;
        }),
        trigger(event) {
          if (listeners[event]) listeners[event]();
        }
      };
      polygons.push(polygon);
      return polygon;
    }),
    latLngBounds: vi.fn((latlngs) => makeBounds(Array.isArray(latlngs) ? latlngs.flat() : [])),
    point: vi.fn((x, y) => ({ x: Number(x) || 0, y: Number(y) || 0 }))
  };

  return { L, maps, polygons };
}

function createSupabaseCampaignMock({
  signedIn = false,
  approved = false,
  userId = "user-1",
  email = "worker@example.com",
  displayName = "Worker Alex",
  avatarUrl = "https://example.com/avatar.png",
  territories: initialTerritories = null
} = {}) {
  let session = signedIn
    ? {
      user: {
        id: userId,
        email,
        user_metadata: {
          full_name: displayName,
          avatar_url: avatarUrl
        }
      }
    }
    : null;

  let territories = cloneJson(Array.isArray(initialTerritories) ? initialTerritories : [
    {
      territory_id: "t-1",
      territory_no: "1",
      locality: "Alpha",
      polygon: [[40.70, -74.01], [40.70, -74.00], [40.71, -74.00], [40.71, -74.01]],
      label_anchor: { lat: 40.705, lng: -74.005 },
      completed: false,
      completed_by: "",
      completed_by_user_id: "",
      completed_by_email: "",
      completed_by_avatar_url: "",
      completed_at: ""
    },
    {
      territory_id: "t-2",
      territory_no: "2",
      locality: "Beta",
      polygon: [[40.72, -74.03], [40.72, -74.02], [40.73, -74.02], [40.73, -74.03]],
      label_anchor: { lat: 40.725, lng: -74.025 },
      completed: false,
      completed_by: "",
      completed_by_user_id: "",
      completed_by_email: "",
      completed_by_avatar_url: "",
      completed_at: ""
    }
  ]);

  const getViewer = () => ({
    signed_in: !!session,
    user_id: session?.user?.id || "",
    email: session?.user?.email || "",
    display_name: session?.user?.user_metadata?.full_name || "",
    avatar_url: session?.user?.user_metadata?.avatar_url || "",
    authorized: !!(session && approved)
  });

  const buildPayload = () => ({
    campaign: {
      id: "camp-1",
      name: "Spring Campaign",
      public_code: "SPRING01",
      approved_emails: ["worker@example.com"]
    },
    viewer: getViewer(),
    territories
  });

  const auth = {
    _callback: null,
    getSession: vi.fn(async () => ({ data: { session }, error: null })),
    onAuthStateChange: vi.fn((callback) => {
      auth._callback = callback;
      return { data: { subscription: { unsubscribe: vi.fn() } } };
    }),
    signInWithOAuth: vi.fn(async () => ({ data: {}, error: null })),
    signOut: vi.fn(async () => {
      session = null;
      if (typeof auth._callback === "function") auth._callback("SIGNED_OUT", null);
      return { error: null };
    })
  };

  const channel = {
    on: vi.fn(() => channel),
    subscribe: vi.fn((callback) => {
      if (typeof callback === "function") callback("SUBSCRIBED");
      return channel;
    })
  };

  const client = {
    rpc: vi.fn(async (name, params) => {
      if (name === "campaign_set_completion") {
        territories = territories.map((territory) => territory.territory_id === params.p_territory_id
          ? {
            ...territory,
            completed: !!params.p_completed,
            completed_by: params.p_completed ? "Worker Alex" : "",
            completed_by_user_id: params.p_completed ? "user-1" : "",
            completed_by_email: params.p_completed ? "worker@example.com" : "",
            completed_by_avatar_url: params.p_completed ? "https://example.com/avatar.png" : "",
            completed_at: params.p_completed ? "2026-03-06T12:30:00Z" : ""
          }
          : territory);
      }
      return { data: buildPayload(), error: null };
    }),
    auth,
    channel: vi.fn(() => channel),
    removeChannel: vi.fn()
  };

  return { client, auth, channel };
}

function createLocalCampaignSnapshot({ publicCode = "SPRING01", name = "Spring Campaign", viewer, territories } = {}) {
  const snapshot = {
    campaign: {
      id: `local:${publicCode}`,
      name,
      publicCode,
      approvedEmails: [],
      mode: "local",
      createdAt: "2026-03-06T12:00:00Z",
      updatedAt: "2026-03-06T12:00:00Z"
    },
    selectedTerritoryId: "",
    viewer: viewer === false ? undefined : {
      userId: viewer && viewer.userId ? viewer.userId : `local:${publicCode}:worker-1`,
      displayName: viewer && Object.prototype.hasOwnProperty.call(viewer, "displayName") ? viewer.displayName : "Jordan Lee",
      email: viewer && viewer.email ? viewer.email : "",
      avatarUrl: viewer && viewer.avatarUrl ? viewer.avatarUrl : "",
      authorized: true,
      signedIn: false
    },
    territories: Array.isArray(territories) ? cloneJson(territories) : [
      {
        territoryId: "t-1",
        territoryNo: "1",
        locality: "Alpha",
        polygon: [[40.70, -74.01], [40.70, -74.00], [40.71, -74.00], [40.71, -74.01]],
        labelAnchor: { lat: 40.705, lng: -74.005 },
        completed: false,
        completedBy: "",
        completedByUserId: "",
        completedByEmail: "",
        completedByAvatarUrl: "",
        completedAt: ""
      },
      {
        territoryId: "t-2",
        territoryNo: "2",
        locality: "Beta",
        polygon: [[40.72, -74.03], [40.72, -74.02], [40.73, -74.02], [40.73, -74.03]],
        labelAnchor: { lat: 40.725, lng: -74.025 },
        completed: false,
        completedBy: "",
        completedByUserId: "",
        completedByEmail: "",
        completedByAvatarUrl: "",
        completedAt: ""
      }
    ]
  };
  if (viewer === false) delete snapshot.viewer;
  return { [publicCode]: snapshot };
}

async function flushAll() {
  await Promise.resolve();
  await new Promise((resolve) => setTimeout(resolve, 0));
}

async function bootstrap({
  appUrl = "http://localhost/campaign.html?campaign=SPRING01",
  signedIn = false,
  approved = false,
  supabaseOptions = {},
  configEnabled = true,
  fetchImpl = null,
  navigateMock = null,
  localCampaigns = {}
} = {}) {
  const dom = new JSDOM(CAMPAIGN_HTML, { runScripts: "outside-only", url: appUrl });
  const { window } = dom;
  const leaflet = createLeafletMock();
  const supabaseMock = createSupabaseCampaignMock({ signedIn, approved, ...supabaseOptions });
  Object.defineProperty(window, "localStorage", { value: createLocalStorage({ db: [], localCampaigns }), configurable: true });
  Object.defineProperty(window, "L", { value: leaflet.L, configurable: true });
  Object.defineProperty(window, "supabase", {
    value: { createClient: vi.fn(() => supabaseMock.client) },
    configurable: true
  });
  if (configEnabled) {
    window.TERRITORY_APP_CONFIG = {
      siteBaseUrl: "https://example.github.io/territory-app/",
      githubPagesBasePath: "/territory-app/",
      managerPagePath: "Territory%20Management.html",
      campaignPagePath: "campaign.html",
      localhostBaseUrl: "http://127.0.0.1:4173/",
      supabaseUrl: "https://example.supabase.co",
      supabaseAnonKey: "anon-key"
    };
  }
  if (fetchImpl) Object.defineProperty(window, "fetch", { value: fetchImpl, configurable: true });
  if (navigateMock) Object.defineProperty(window, "__TERRITORY_NAVIGATE__", { value: navigateMock, configurable: true });
  window.requestAnimationFrame = (callback) => {
    callback();
    return 1;
  };
  window.cancelAnimationFrame = () => {};
  window.eval(CAMPAIGN_SCRIPT);
  window.dispatchEvent(new window.Event("load"));
  await flushAll();
  return {
    window,
    document: window.document,
    leaflet,
    supabaseMock,
    cleanup: () => window.close()
  };
}

let activeEnv = null;

afterEach(() => {
  if (activeEnv && typeof activeEnv.cleanup === "function") activeEnv.cleanup();
  activeEnv = null;
  vi.restoreAllMocks();
});

describe("campaign view", () => {
  it("removes Google Fonts in favor of local typography assets", () => {
    expect(CAMPAIGN_HTML).not.toMatch(/fonts\.googleapis/i);
    expect(CAMPAIGN_HTML).toMatch(/IBM Plex Sans/);
    expect(CAMPAIGN_HTML).toMatch(/assets\/fonts\/ibm-plex-sans-latin-400-normal\.woff2/);
  });

  it("shows the Google sign-in gate for signed-out workers", async () => {
    activeEnv = await bootstrap({ signedIn: false, approved: false });
    const { document, supabaseMock } = activeEnv;

    expect(document.getElementById("authGate").hidden).toBe(false);
    expect(document.getElementById("modeChip").textContent).toMatch(/Sign In Required/i);
    expect(document.getElementById("territoryPicker").disabled).toBe(true);
    expect(document.getElementById("btnMarkCompleted").disabled).toBe(true);
    expect(document.getElementById("authGateStatus").textContent).toMatch(/Sign in with Google/i);

    document.getElementById("btnContinueWithGoogle").click();
    await flushAll();

    expect(supabaseMock.auth.signInWithOAuth).toHaveBeenCalledWith(expect.objectContaining({
      provider: "google",
      options: expect.objectContaining({
        redirectTo: "https://example.github.io/territory-app/campaign.html?mode=live&campaign=SPRING01"
      })
    }));
  });

  it("loads same-computer local campaigns with the saved worker display name", async () => {
    activeEnv = await bootstrap({
      appUrl: "file:///D:/Code%20Projects/Territory%20App/campaign.html?mode=local&campaign=SPRING01",
      configEnabled: false,
      localCampaigns: createLocalCampaignSnapshot()
    });
    const { document } = activeEnv;

    expect(document.getElementById("authGate").hidden).toBe(true);
    expect(document.getElementById("modeChip").textContent).toMatch(/Local Campaign/i);
    expect(document.getElementById("territoryPicker").disabled).toBe(false);
    expect(document.getElementById("workerDisplayName").textContent).toBe("Jordan Lee");
    expect(document.getElementById("workerDisplayMeta").textContent).toMatch(/same-computer local campaign/i);
    expect(document.getElementById("workerSessionChip").disabled).toBe(false);
    expect(document.getElementById("workerSessionChip").getAttribute("aria-label")).toMatch(/edit publisher display name/i);
    expect(document.getElementById("localWorkerEditor").hidden).toBe(true);
    expect(document.querySelector('label[for="localWorkerName"]')?.textContent).toBe("Publisher Name");
  });

  it("renders the viewer avatar image when an avatar URL is available", async () => {
    activeEnv = await bootstrap({
      appUrl: "file:///D:/Code%20Projects/Territory%20App/campaign.html?mode=local&campaign=SPRING01",
      configEnabled: false,
      localCampaigns: createLocalCampaignSnapshot({
        viewer: {
          userId: "local:SPRING01:worker-1",
          displayName: "Jordan Lee",
          avatarUrl: "https://example.com/jordan.png"
        }
      })
    });
    const { document } = activeEnv;
    const avatarImage = document.querySelector("#workerAvatar img");

    expect(avatarImage).not.toBeNull();
    expect(avatarImage?.getAttribute("src")).toBe("https://example.com/jordan.png");
  });

  it("requires saving a local worker name before local completion actions are enabled", async () => {
    activeEnv = await bootstrap({
      appUrl: "file:///D:/Code%20Projects/Territory%20App/campaign.html?mode=local&campaign=SPRING01",
      configEnabled: false,
      localCampaigns: createLocalCampaignSnapshot({
        viewer: {
          userId: "local:SPRING01:worker-1",
          displayName: ""
        }
      })
    });
    const { document, window } = activeEnv;
    const picker = document.getElementById("territoryPicker");

    picker.value = "t-1";
    picker.dispatchEvent(new window.Event("change", { bubbles: true }));
    await flushAll();

    expect(document.getElementById("workerDisplayName").textContent).toMatch(/Publisher Name Required/i);
    expect(document.getElementById("btnMarkCompleted").textContent).toBe("Save Publisher Name");
    expect(document.getElementById("btnMarkCompleted").disabled).toBe(true);
    expect(document.getElementById("workerSessionChip").getAttribute("aria-label")).toMatch(/set publisher display name/i);
    expect(document.getElementById("localWorkerEditor").hidden).toBe(true);

    const nameInput = document.getElementById("localWorkerName");
    nameInput.focus();
    nameInput.value = "Jordan Lee";
    nameInput.dispatchEvent(new window.Event("input", { bubbles: true }));
    document.getElementById("btnSaveLocalWorkerName").click();
    await flushAll();

    const stored = JSON.parse(window.localStorage.getItem(STORAGE_KEY_LOCAL_CAMPAIGNS) || "{}");
    expect(stored.SPRING01.viewer.displayName).toBe("Jordan Lee");
    expect(stored.SPRING01.viewer.userId).toMatch(/^local:SPRING01:/);
    expect(document.getElementById("workerDisplayName").textContent).toBe("Jordan Lee");
    expect(document.getElementById("localWorkerEditor").hidden).toBe(true);
    expect(document.getElementById("btnMarkCompleted").textContent).toBe("Mark Completed");
    expect(document.getElementById("btnMarkCompleted").disabled).toBe(false);
  });

  it("shows a recovery state when a same-computer local campaign snapshot is missing", async () => {
    activeEnv = await bootstrap({
      appUrl: "file:///D:/Code%20Projects/Territory%20App/campaign.html?mode=local&campaign=SPRING01",
      configEnabled: false,
      localCampaigns: {}
    });
    const { document } = activeEnv;

    expect(document.getElementById("authGate").hidden).toBe(false);
    expect(document.getElementById("authGateTitle").textContent).toMatch(/Local Campaign Not Found/i);
    expect(document.getElementById("authGateStatus").textContent).toMatch(/not found/i);
    expect(document.getElementById("btnContinueWithGoogle").hidden).toBe(true);
  });

  it("redirects file:// live campaign launches to localhost when the local app is reachable", async () => {
    const fetchImpl = vi.fn(async () => ({ ok: true }));
    const navigateMock = vi.fn();
    activeEnv = await bootstrap({
      appUrl: "file:///D:/Code%20Projects/Territory%20App/campaign.html?mode=live&campaign=SPRING01",
      signedIn: false,
      approved: false,
      fetchImpl,
      navigateMock
    });

    expect(fetchImpl).toHaveBeenCalledWith("http://127.0.0.1:4173/campaign.html?mode=live&campaign=SPRING01", expect.objectContaining({
      method: "GET",
      mode: "no-cors"
    }));
    expect(navigateMock).toHaveBeenCalledWith("http://127.0.0.1:4173/campaign.html?mode=live&campaign=SPRING01");
    expect(activeEnv.document.getElementById("authGateStatus").textContent).toMatch(/redirecting to localhost/i);
  });

  it("shows localhost setup steps instead of the sign-in gate when a live file:// campaign opens without a running local app", async () => {
    const fetchImpl = vi.fn(async () => {
      throw new Error("connect ECONNREFUSED");
    });
    activeEnv = await bootstrap({
      appUrl: "file:///D:/Code%20Projects/Territory%20App/campaign.html?mode=live&campaign=SPRING01",
      signedIn: false,
      approved: false,
      fetchImpl
    });
    const { document } = activeEnv;

    expect(document.getElementById("modeChip").textContent).toMatch(/Localhost Needed/i);
    expect(document.getElementById("authGateStatus").textContent).toMatch(/localhost is not running/i);
    expect(document.getElementById("authGateChecklist").textContent).toMatch(/start-territory-app\.cmd/i);
    expect(document.getElementById("authGateChecklist").textContent).toMatch(/assets\/app-config\.js/i);
    expect(document.getElementById("authGateChecklist").textContent).toMatch(/http:\/\/127\.0\.0\.1:4173\/campaign\.html\?mode=live&campaign=SPRING01/i);
    expect(document.getElementById("btnContinueWithGoogle").hidden).toBe(true);
    expect(document.getElementById("btnOpenLocalhostCampaign").hidden).toBe(false);
  });

  it("shows a non-live state instead of broken auth when Supabase config is missing", async () => {
    activeEnv = await bootstrap({ signedIn: false, approved: false, configEnabled: false });
    const { document } = activeEnv;

    expect(document.getElementById("modeChip").textContent).toMatch(/Live Sync Off/i);
    expect(document.getElementById("authGateStatus").textContent).toMatch(/not configured/i);
    expect(document.getElementById("authGateChecklist").textContent).toMatch(/assets\/app-config\.js/i);
    expect(document.getElementById("btnContinueWithGoogle").hidden).toBe(true);
    expect(document.getElementById("territoryPicker").disabled).toBe(true);
  });

  it("toggles local territory completion for the saved worker and returns to the all-territories overview", async () => {
    activeEnv = await bootstrap({
      appUrl: "file:///D:/Code%20Projects/Territory%20App/campaign.html?mode=local&campaign=SPRING01",
      configEnabled: false,
      localCampaigns: createLocalCampaignSnapshot()
    });
    const { document, window } = activeEnv;
    const picker = document.getElementById("territoryPicker");

    picker.value = "t-1";
    picker.dispatchEvent(new window.Event("change", { bubbles: true }));
    await flushAll();

    document.getElementById("btnMarkCompleted").click();
    await flushAll();

    let stored = JSON.parse(window.localStorage.getItem(STORAGE_KEY_LOCAL_CAMPAIGNS) || "{}");
    expect(stored.SPRING01.territories.find((territory) => territory.territoryId === "t-1")?.completed).toBe(true);
    expect(stored.SPRING01.territories.find((territory) => territory.territoryId === "t-1")?.completedBy).toBe("Jordan Lee");
    expect(stored.SPRING01.territories.find((territory) => territory.territoryId === "t-1")?.completedByUserId).toBe(stored.SPRING01.viewer.userId);
    expect(document.getElementById("kpiCompleted").textContent).toBe("1");
    expect(document.getElementById("territoryPicker").value).toBe("");
    expect(document.getElementById("mapLegend").textContent).toMatch(/1\/2 completed/i);

    picker.value = "t-1";
    picker.dispatchEvent(new window.Event("change", { bubbles: true }));
    await flushAll();

    expect(document.getElementById("btnMarkCompleted").textContent).toBe("Unmark Completed");
    expect(document.getElementById("btnMarkCompleted").disabled).toBe(false);

    await window.TerritoryCampaignView.modules.completeSelectedTerritory();
    await flushAll();

    stored = JSON.parse(window.localStorage.getItem(STORAGE_KEY_LOCAL_CAMPAIGNS) || "{}");
    expect(stored.SPRING01.territories.find((territory) => territory.territoryId === "t-1")?.completed).toBe(false);
    expect(stored.SPRING01.territories.find((territory) => territory.territoryId === "t-1")?.completedBy).toBe("");
    expect(document.getElementById("territoryPicker").value).toBe("");
    expect(document.getElementById("mapLegend").textContent).toMatch(/0\/2 completed/i);
  });

  it("locks local territories completed by another worker", async () => {
    activeEnv = await bootstrap({
      appUrl: "file:///D:/Code%20Projects/Territory%20App/campaign.html?mode=local&campaign=SPRING01",
      configEnabled: false,
      localCampaigns: createLocalCampaignSnapshot({
        territories: [
          {
            territoryId: "t-1",
            territoryNo: "1",
            locality: "Alpha",
            polygon: [[40.70, -74.01], [40.70, -74.00], [40.71, -74.00], [40.71, -74.01]],
            labelAnchor: { lat: 40.705, lng: -74.005 },
            completed: true,
            completedBy: "Blake Stone",
            completedByUserId: "local:SPRING01:worker-2",
            completedByEmail: "",
            completedByAvatarUrl: "",
            completedAt: "2026-03-06T12:30:00Z"
          },
          {
            territoryId: "t-2",
            territoryNo: "2",
            locality: "Beta",
            polygon: [[40.72, -74.03], [40.72, -74.02], [40.73, -74.02], [40.73, -74.03]],
            labelAnchor: { lat: 40.725, lng: -74.025 },
            completed: false,
            completedBy: "",
            completedByUserId: "",
            completedByEmail: "",
            completedByAvatarUrl: "",
            completedAt: ""
          }
        ]
      })
    });
    const { document, window } = activeEnv;
    const picker = document.getElementById("territoryPicker");

    picker.value = "t-1";
    picker.dispatchEvent(new window.Event("change", { bubbles: true }));
    await flushAll();

    expect(document.getElementById("btnMarkCompleted").textContent).toBe("Completed by Blake Stone");
    expect(document.getElementById("btnMarkCompleted").disabled).toBe(true);
  });

  it("lets approved workers pick territories, zoom in, and sync polygon clicks", async () => {
    activeEnv = await bootstrap({ signedIn: true, approved: true });
    const { document, leaflet } = activeEnv;
    const picker = document.getElementById("territoryPicker");

    expect(document.getElementById("authGate").hidden).toBe(true);
    expect(picker.disabled).toBe(false);

    picker.value = "t-1";
    picker.dispatchEvent(new activeEnv.window.Event("change", { bubbles: true }));
    await flushAll();

    expect(document.getElementById("selectedTerritoryMeta").textContent).toMatch(/Terr\. 1/i);

    const secondPolygon = leaflet.polygons[1];
    secondPolygon.trigger("click");
    await flushAll();

    expect(document.getElementById("territoryPicker").value).toBe("t-2");
    expect(document.getElementById("selectedTerritoryMeta").textContent).toMatch(/Terr\. 2/i);

    document.getElementById("btnShowAllTerritories").click();
    await flushAll();

    expect(document.getElementById("territoryPicker").value).toBe("");
    expect(document.getElementById("mapLegend").textContent).toMatch(/completed/i);
    expect(leaflet.maps[0].invalidateSize).toHaveBeenCalled();
  });

  it("marks territories completed and returns to the all-territories overview", async () => {
    activeEnv = await bootstrap({ signedIn: true, approved: true });
    const { document, supabaseMock } = activeEnv;
    const picker = document.getElementById("territoryPicker");

    picker.value = "t-1";
    picker.dispatchEvent(new activeEnv.window.Event("change", { bubbles: true }));
    await flushAll();

    document.getElementById("btnMarkCompleted").click();
    await flushAll();

    expect(supabaseMock.client.rpc.mock.calls.some(([name]) => name === "campaign_set_completion")).toBe(true);
    expect(supabaseMock.client.rpc.mock.calls.find(([name]) => name === "campaign_set_completion")?.[1]).toMatchObject({
      p_public_code: "SPRING01",
      p_territory_id: "t-1",
      p_completed: true
    });
    expect(document.getElementById("kpiCompleted").textContent).toBe("1");
    expect(document.getElementById("territoryPicker").value).toBe("");
    expect(document.getElementById("selectedTerritoryMeta").textContent).toMatch(/Select a territory/i);
    expect(document.getElementById("mapLegend").textContent).toMatch(/1\/2 completed/i);
  });

  it("lets approved workers unmark their own completed territories", async () => {
    activeEnv = await bootstrap({
      signedIn: true,
      approved: true,
      supabaseOptions: {
        territories: [
          {
            territory_id: "t-1",
            territory_no: "1",
            locality: "Alpha",
            polygon: [[40.70, -74.01], [40.70, -74.00], [40.71, -74.00], [40.71, -74.01]],
            label_anchor: { lat: 40.705, lng: -74.005 },
            completed: true,
            completed_by: "Worker Alex",
            completed_by_user_id: "user-1",
            completed_by_email: "worker@example.com",
            completed_by_avatar_url: "https://example.com/avatar.png",
            completed_at: "2026-03-06T12:30:00Z"
          },
          {
            territory_id: "t-2",
            territory_no: "2",
            locality: "Beta",
            polygon: [[40.72, -74.03], [40.72, -74.02], [40.73, -74.02], [40.73, -74.03]],
            label_anchor: { lat: 40.725, lng: -74.025 },
            completed: false,
            completed_by: "",
            completed_by_user_id: "",
            completed_by_email: "",
            completed_by_avatar_url: "",
            completed_at: ""
          }
        ]
      }
    });
    const { document, supabaseMock } = activeEnv;
    const picker = document.getElementById("territoryPicker");

    picker.value = "t-1";
    picker.dispatchEvent(new activeEnv.window.Event("change", { bubbles: true }));
    await flushAll();

    expect(document.getElementById("btnMarkCompleted").textContent).toBe("Unmark Completed");

    document.getElementById("btnMarkCompleted").click();
    await flushAll();

    expect(supabaseMock.client.rpc.mock.calls.find(([name, params]) => name === "campaign_set_completion" && params.p_territory_id === "t-1")?.[1]).toMatchObject({
      p_public_code: "SPRING01",
      p_territory_id: "t-1",
      p_completed: false
    });
    expect(document.getElementById("territoryPicker").value).toBe("");
    expect(document.getElementById("mapLegend").textContent).toMatch(/0\/2 completed/i);
  });

  it("locks territories completed by another live worker", async () => {
    activeEnv = await bootstrap({
      signedIn: true,
      approved: true,
      supabaseOptions: {
        territories: [
          {
            territory_id: "t-1",
            territory_no: "1",
            locality: "Alpha",
            polygon: [[40.70, -74.01], [40.70, -74.00], [40.71, -74.00], [40.71, -74.01]],
            label_anchor: { lat: 40.705, lng: -74.005 },
            completed: true,
            completed_by: "Worker Blake",
            completed_by_user_id: "user-2",
            completed_by_email: "blake@example.com",
            completed_by_avatar_url: "https://example.com/blake.png",
            completed_at: "2026-03-06T12:30:00Z"
          },
          {
            territory_id: "t-2",
            territory_no: "2",
            locality: "Beta",
            polygon: [[40.72, -74.03], [40.72, -74.02], [40.73, -74.02], [40.73, -74.03]],
            label_anchor: { lat: 40.725, lng: -74.025 },
            completed: false,
            completed_by: "",
            completed_by_user_id: "",
            completed_by_email: "",
            completed_by_avatar_url: "",
            completed_at: ""
          }
        ]
      }
    });
    const { document, supabaseMock } = activeEnv;
    const picker = document.getElementById("territoryPicker");

    picker.value = "t-1";
    picker.dispatchEvent(new activeEnv.window.Event("change", { bubbles: true }));
    await flushAll();

    expect(document.getElementById("btnMarkCompleted").textContent).toBe("Completed by Worker Blake");
    expect(document.getElementById("btnMarkCompleted").disabled).toBe(true);

    document.getElementById("btnMarkCompleted").click();
    await flushAll();

    expect(supabaseMock.client.rpc.mock.calls.some(([name, params]) => name === "campaign_set_completion" && params.p_territory_id === "t-1" && params.p_completed === false)).toBe(false);
  });
});
