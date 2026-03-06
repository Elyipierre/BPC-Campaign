@echo off
setlocal
cd /d "%~dp0"
set "TERRITORY_NO_AUTO_OPEN=1"
powershell -NoProfile -ExecutionPolicy Bypass -Command ^
  "$wd = (Get-Location).Path; " ^
  "$appUrl = 'http://127.0.0.1:4173/Territory%20Management.html'; " ^
  "$apiUrl = 'http://127.0.0.1:8787/health'; " ^
  "function Test-UrlReady([string]$Url, [string]$MustContain = '') { " ^
  "  try { " ^
  "    $resp = Invoke-WebRequest -Uri $Url -UseBasicParsing -TimeoutSec 3; " ^
  "    if ($resp.StatusCode -lt 200 -or $resp.StatusCode -ge 300) { return $false } " ^
  "    if ($MustContain -and [string]$resp.Content -notmatch [regex]::Escape($MustContain)) { return $false } " ^
  "    return $true " ^
  "  } catch { " ^
  "    return $false " ^
  "  } " ^
  "} " ^
  "Start-Process -FilePath 'node' -ArgumentList 'scripts/start-localhost.cjs' -WorkingDirectory $wd -WindowStyle Hidden | Out-Null; " ^
  "$deadline = (Get-Date).AddSeconds(45); " ^
  "do { " ^
  "  $appReady = Test-UrlReady $appUrl 'Territory Management PRO'; " ^
  "  $apiReady = Test-UrlReady $apiUrl 'local-data-api'; " ^
  "  if ($appReady -and $apiReady) { break } " ^
  "  Start-Sleep -Milliseconds 500; " ^
  "} while ((Get-Date) -lt $deadline); " ^
  "Start-Process $appUrl | Out-Null"
endlocal
