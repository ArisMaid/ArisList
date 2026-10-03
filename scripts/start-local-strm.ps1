param(
    [Parameter(Mandatory = $true)]
    [string]$TrustedOrigin
)

$ErrorActionPreference = 'Stop'
$origin = [Uri]$TrustedOrigin
if (-not $origin.IsAbsoluteUri -or $origin.Scheme -notin @('http', 'https') -or
    $origin.UserInfo -or $origin.AbsolutePath -ne '/' -or $origin.Query -or $origin.Fragment) {
    throw 'TrustedOrigin must be an exact HTTP(S) origin, without credentials, path, query or fragment.'
}

# Explicit local deployment choice; do not trust every Fake-IP/private network.
$savedOrigin = $env:STRM_TRUSTED_ORIGINS
$savedWorkers = $env:REMOTE_SOURCE_WORKERS
$savedInterval = $env:STRM_MIN_REQUEST_INTERVAL_MS
Push-Location (Split-Path -Parent $PSScriptRoot)
try {
    $env:STRM_TRUSTED_ORIGINS = $origin.GetLeftPart([UriPartial]::Authority)
    $env:REMOTE_SOURCE_WORKERS = '1'
    $env:STRM_MIN_REQUEST_INTERVAL_MS = '1000'
    & cargo run -p media-shelf-server
    if ($LASTEXITCODE -ne 0) { throw "Backend exited with code $LASTEXITCODE" }
} finally {
    $env:STRM_TRUSTED_ORIGINS = $savedOrigin
    $env:REMOTE_SOURCE_WORKERS = $savedWorkers
    $env:STRM_MIN_REQUEST_INTERVAL_MS = $savedInterval
    Pop-Location
}
