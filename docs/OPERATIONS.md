## Territory Management PRO Operations

### Runtime API
The app exposes a runtime API at `window.TerritoryApp`.

Key methods:
- `start()`: idempotent startup entrypoint.
- `runSelfChecks()`: validates required DOM and database integrity.
- `getStateSnapshot()`: returns a read-only snapshot of current runtime state.
- `getDataQualityReport()`: returns data quality metrics for territories and addresses.
- `validateDatabase()`: returns `{ ok, fatalIssues, warnings, report }`.
- `getLogs(limit)`: returns the latest app operational logs.
- `clearLogs()`: clears the in-memory app logs.

### Modules
`window.TerritoryApp.modules` groups functionality by domain:
- `persistence`
- `status`
- `operations`
- `territory`
- `addresses`
- `dataset`
- `geometry`
- `labels`
- `map`
- `print`
- `ui`
- `diagnostics`

`labels` now also exposes manual placement helpers:
- `setLabelEditMode(enabled)`: toggles draggable territory labels on the live map.
- `setTerritoryLabelAnchor(territoryId, latLng)`: persists a manual label anchor.
- `clearTerritoryLabelAnchor(territoryId)`: removes manual anchor override.

UI note:
- Leaflet Draw's unused "no layers to edit" slot is repurposed into a map toolbar label-adjust toggle (pencil icon).

### Operations and stale request handling
The app now uses operation tokens (`fetch`, `snap`, `print`) to prevent stale async
responses from overwriting newer user actions.

Behavior:
- Starting a new fetch cancels the previous fetch token.
- Selection changes cancel in-flight fetch operations.
- Print and snap operations are tokenized and logged.

### Address data source mode (Local Data API)
The runtime uses a local open-data API daemon for territory address assignment:
1. App starts on localhost and the launcher starts both static hosting and API daemon.
2. State selection automatically calls `/api/local-data/state/ensure` and polls `/api/local-data/state/status`.
3. Territory fetch/recompute calls `/api/local-data/addresses/search` with strict residential mode.
4. Unit evidence sync/status uses:
   - `GET /api/local-data/unit-sync/status?state=NY`
   - `POST /api/local-data/unit-sync/run`
5. Alignment preview/apply uses:
   - `POST /api/local-data/territories/align/preview`
   - `POST /api/local-data/territories/align/apply`
6. Results are persisted in localStorage and reused until territory geometry changes.

Controls:
- `window.TERRITORY_LOCAL_DATA_API_BASE_URL`: optional API base override (default `http://127.0.0.1:8787`).
- `window.TERRITORY_OVERTURE_API_BASE_URL`: legacy alias still accepted for compatibility.
- `window.TERRITORY_AUTO_STATE_INSTALL`: enable/disable auto state ensure workflow (default enabled).

### One-click local launch
- Windows shortcut:
  - `start-territory-app.cmd`
- NPM command:
  - `npm run start:local`

Both options start:
- local static server
- local data API daemon

Then open:
- `http://127.0.0.1:4173/Territory%20Management.html`

Windows launcher behavior:
- `start-territory-app.cmd` now starts the local services in the background and opens the app URL automatically.

### Logging
The app maintains an in-memory ring buffer (`APP_EVENT_LOG_LIMIT = 300`) of operational events.
This is intended for troubleshooting in production-like usage without external telemetry.

Use:
- `window.TerritoryApp.getLogs(100)`
- `window.TerritoryApp.clearLogs()`

### Test commands
- `npm run test`
- `npm run test:api`
- `npm run test:watch`
- `npm run test:e2e:local`
- `npm run verify`
- `npm run overture:units:sync`
- `npm run overture:units:verify`
