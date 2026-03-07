(function initTerritoryAppConfig(globalScope) {
  const checkedInDefaults = Object.freeze({
    // Leave blank so the app auto-detects the active origin.
    // On GitHub Pages this repo resolves to:
    // https://elyipierre.github.io/BPC-Campaign/
    // On localhost it resolves to:
    // http://127.0.0.1:4173/
    siteBaseUrl: "",
    githubPagesBasePath: "",
    managerPagePath: "Territory%20Management.html",
    campaignPagePath: "campaign.html",
    localhostBaseUrl: "http://127.0.0.1:4173/",
    // Fill these with your public Supabase project values to enable
    // live multi-device campaign sync on GitHub Pages.
    supabaseUrl: "https://dlncebwzunuxouyxteir.supabase.co",
    supabaseAnonKey: "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImRsbmNlYnd6dW51eG91eXh0ZWlyIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzI4NDQ1NzgsImV4cCI6MjA4ODQyMDU3OH0.WyM45C1Co_XmG-p_g793p3mImAIHqVWRpdxGer_95qQ"
  });
  const existing = globalScope && globalScope.TERRITORY_APP_CONFIG && typeof globalScope.TERRITORY_APP_CONFIG === "object"
    ? { ...checkedInDefaults, ...globalScope.TERRITORY_APP_CONFIG }
    : {};

  const protocol = globalScope && globalScope.location ? String(globalScope.location.protocol || "") : "";
  const origin = globalScope && globalScope.location ? String(globalScope.location.origin || "") : "";
  const pathname = globalScope && globalScope.location ? String(globalScope.location.pathname || "/") : "/";
  const defaultBasePath = pathname.replace(/[^/]*$/, "");
  const configuredBasePath = String(existing.githubPagesBasePath || defaultBasePath || "/").trim() || "/";
  const normalizedBasePath = `/${configuredBasePath.replace(/^\/+|\/+$/g, "")}${configuredBasePath === "/" ? "" : "/"}`.replace(/\/{2,}/g, "/");
  const derivedSiteBaseUrl = /^https?:$/i.test(protocol) && origin
    ? `${origin}${normalizedBasePath === "/" ? "/" : normalizedBasePath}`
    : "";

  globalScope.TERRITORY_APP_CONFIG = Object.freeze({
    siteBaseUrl: String(existing.siteBaseUrl || derivedSiteBaseUrl || "").trim(),
    githubPagesBasePath: normalizedBasePath,
    managerPagePath: String(existing.managerPagePath || checkedInDefaults.managerPagePath).trim(),
    campaignPagePath: String(existing.campaignPagePath || checkedInDefaults.campaignPagePath).trim(),
    localhostBaseUrl: String(existing.localhostBaseUrl || checkedInDefaults.localhostBaseUrl).trim(),
    supabaseUrl: String(existing.supabaseUrl || checkedInDefaults.supabaseUrl).trim(),
    supabaseAnonKey: String(existing.supabaseAnonKey || checkedInDefaults.supabaseAnonKey).trim()
  });
})(window);
