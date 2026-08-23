import { createHash } from "node:crypto";
import { spawnSync } from "node:child_process";

import { CURRENT_SCHEMA_VERSION } from "./schema-version.mjs";

// Keep this allowlist in one place so every performance artifact captures the
// same non-secret deployment knobs without accidentally serializing credentials
// or arbitrary environment variables.
export const SAFE_ENVIRONMENT_KEYS = Object.freeze([
  "RESOURCE_PROFILE",
  "CATALOG_V2_ENABLED",
  "FACET_BITMAP_ENABLED",
  "INVENTORY_SCANNER_ENABLED",
  "INVENTORY_SCANNER_KINDS",
  "SEARCH_OUTBOX_SHADOW_ENABLED",
  "SEARCH_SHADOW_CANARY_ENABLED",
  "SEARCH_INCREMENTAL_READER_ENABLED",
  "SEARCH_READER_PREWARM_ENABLED",
  "DERIVATIVE_CACHE_V2_ENABLED",
  "DERIVATIVE_CACHE_MAX_BYTES",
  "DERIVATIVE_CACHE_LOW_WATERMARK_BYTES",
  "JPEG_THUMBNAIL_DOWNSCALE_ENABLED",
  "ENABLE_FILE_WATCHER",
  "CLOUD_CACHE_MAX_BYTES",
  "THUMBNAIL_CACHE_MAX_BYTES_PER_DIR",
  "SQLITE_MAX_CONNECTIONS",
  "SQLITE_CACHE_KIB_PER_CONNECTION",
  "SQLITE_MMAP_SIZE_BYTES",
  "SQLITE_BUSY_TIMEOUT_MILLIS",
  "SQLITE_ACQUIRE_TIMEOUT_MILLIS",
  "SQLITE_WAL_AUTOCHECKPOINT_PAGES",
  "SQLITE_JOURNAL_SIZE_LIMIT_BYTES",
  "SQLITE_WRITER_QUEUE_MAX_DEPTH",
  "SQLITE_WRITER_QUEUE_MAX_BYTES",
  "PROCESSING_MEMORY_BUDGET_BYTES",
  "INFLIGHT_MEDIA_BUDGET_BYTES",
  "RESOURCE_WAIT_TIMEOUT_MILLIS",
  "MEMORY_SOFT_LIMIT_BYTES",
  "MEMORY_RESUME_LIMIT_BYTES",
  "ARCHIVE_MANIFEST_CACHE_BYTES",
  "THUMBNAIL_WORKERS",
  "ARCHIVE_WORKERS",
  "ARCHIVE_STREAM_WORKERS",
  "LOCAL_MEDIA_STREAM_WORKERS",
  "REMOTE_SOURCE_WORKERS",
  "SCAN_IO_CONCURRENCY",
  "CATALOG_WRITERS",
  "SEARCH_WRITERS",
  "SEARCH_WRITER_HEAP_BYTES",
  "SCAN_IO_WORKERS",
]);

function runGit(repoRoot, args, input) {
  const result = spawnSync("git", args, {
    cwd: repoRoot,
    encoding: "utf8",
    input,
    maxBuffer: 128 * 1024 * 1024,
    windowsHide: true,
  });
  return result.status === 0 ? result.stdout : null;
}

export function captureGitState(repoRoot) {
  const status = runGit(repoRoot, ["status", "--porcelain=v1", "--untracked-files=all"]) ?? "";
  const diff = runGit(repoRoot, ["diff", "--binary", "HEAD", "--"]) ?? "";
  const untracked = runGit(repoRoot, ["ls-files", "--others", "--exclude-standard"]) ?? "";
  const untrackedHashes = untracked.trim()
    ? runGit(repoRoot, ["hash-object", "--stdin-paths"], untracked) ?? ""
    : "";
  return {
    commit: runGit(repoRoot, ["rev-parse", "HEAD"])?.trim() ?? null,
    branch: runGit(repoRoot, ["branch", "--show-current"])?.trim() || null,
    dirty: Boolean(status.trim()),
    dirty_state_sha256: createHash("sha256")
      .update(status)
      .update("\0")
      .update(diff)
      .update("\0")
      .update(untrackedHashes)
      .digest("hex"),
    status: status.split(/\r?\n/u).filter(Boolean),
  };
}

export function captureFeatureFlags(environment = process.env) {
  return Object.fromEntries(
    SAFE_ENVIRONMENT_KEYS.map((key) => [key, environment[key] ?? null]),
  );
}

export function captureSourceProvenance(repoRoot, environment = process.env) {
  return {
    ...captureGitState(repoRoot),
    schema_version: CURRENT_SCHEMA_VERSION,
    feature_flags: captureFeatureFlags(environment),
  };
}
