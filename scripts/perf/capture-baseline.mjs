import {
  existsSync,
  mkdirSync,
  readFileSync,
  readdirSync,
  statfsSync,
  writeFileSync,
} from "node:fs";
import os from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { parseArgs } from "node:util";

import {
  collectDatasetManifest,
  collectMediaRootManifest,
  PERF_ARTIFACT_VERSION,
  SYSTEM_SAMPLE_COLUMNS,
  summarizeScenarioRecords,
} from "./perf-lib.mjs";
import { captureGitState, SAFE_ENVIRONMENT_KEYS } from "./provenance.mjs";

const repoRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..", "..");

const { values } = parseArgs({
  options: {
    output: { type: "string", short: "o" },
    database: { type: "string", short: "d" },
    "health-url": { type: "string" },
    "health-checkpoint": { type: "boolean" },
    "media-root": { type: "string", multiple: true },
    "media-manifest": { type: "string", multiple: true },
    "media-manifest-max-files": { type: "string" },
    "media-manifest-max-directories": { type: "string" },
    "media-manifest-inspect-limit": { type: "string" },
    "media-manifest-header-bytes": { type: "string" },
    "media-manifest-max-archive-bytes": { type: "string" },
    help: { type: "boolean", short: "h" },
  },
  strict: true,
});

if (values.help) {
  console.log(`Usage: node scripts/perf/capture-baseline.mjs [options]

Options:
  -o, --output <dir>         New, empty result directory
  -d, --database <path>      SQLite database opened read-only
      --health-url <url>     Optional /api/health/resources URL
      --health-checkpoint     Add ?checkpoint=true for one passive WAL probe
      --media-root kind=path Record filesystem capacity for a media root (repeatable)
      --media-manifest kind=path
                              Explicitly walk a media root and record bounded file facts
      --media-manifest-max-files <n>
                              Stop a media walk after n files (default: 5000000)
      --media-manifest-max-directories <n>
                              Stop a media walk after n directories (default: 1000000)
      --media-manifest-inspect-limit <n>
                              Inspect at most n image/archive headers per root (default: 256)
      --media-manifest-header-bytes <n>
                              Maximum bytes read for one image header (default: 1048576)
      --media-manifest-max-archive-bytes <n>
                              Maximum central-directory bytes read per archive (default: 67108864)
  -h, --help                 Show this help

Only allowlisted, non-secret environment variables are recorded.
--media-root never walks recursively; use --media-manifest deliberately for that.`);
  process.exit(0);
}

function normalizeDatabasePath(value) {
  if (!value) return null;
  if (value.startsWith("sqlite:")) {
    const withoutQuery = value.replace(/^sqlite:\/\//u, "").split("?", 1)[0];
    if (!withoutQuery || withoutQuery === ":memory:") return null;
    const windowsPath = withoutQuery.replace(/^\/([A-Za-z]:\/)/u, "$1");
    return resolve(windowsPath);
  }
  return resolve(value);
}

function filesystemSnapshot(path) {
  try {
    const stats = statfsSync(path);
    return {
      path,
      available: true,
      block_size: Number(stats.bsize),
      total_bytes: Number(stats.blocks) * Number(stats.bsize),
      available_bytes: Number(stats.bavail) * Number(stats.bsize),
      filesystem_type: Number(stats.type),
    };
  } catch (error) {
    return { path, available: false, error: error.message };
  }
}

function readSystemText(path) {
  try {
    return readFileSync(path, "utf8").trim();
  } catch {
    return null;
  }
}

function parseNullableUint(value) {
  if (value == null || value === "" || value.toLowerCase() === "max") return null;
  const parsed = Number(value);
  return Number.isSafeInteger(parsed) && parsed >= 0 && parsed <= 2 ** 60 ? parsed : null;
}

function cgroupSnapshot() {
  const memoryMax =
    parseNullableUint(readSystemText("/sys/fs/cgroup/memory.max")) ??
    parseNullableUint(readSystemText("/sys/fs/cgroup/memory/memory.limit_in_bytes"));
  const cpuV2 = readSystemText("/sys/fs/cgroup/cpu.max");
  if (cpuV2) {
    const [quotaText, periodText] = cpuV2.split(/\s+/u);
    const period = parseNullableUint(periodText);
    const quota = quotaText === "max" ? null : parseNullableUint(quotaText);
    return {
      version: 2,
      memory_max_bytes: memoryMax,
      cpu_quota_micros: quota,
      cpu_period_micros: period,
      cpu_limit_cores:
        quota != null && period > 0 ? quota / period : null,
    };
  }
  const quota = parseNullableUint(readSystemText("/sys/fs/cgroup/cpu/cpu.cfs_quota_us"));
  const period = parseNullableUint(readSystemText("/sys/fs/cgroup/cpu/cpu.cfs_period_us"));
  return {
    version: quota != null || period != null ? 1 : null,
    memory_max_bytes: memoryMax,
    cpu_quota_micros: quota,
    cpu_period_micros: period,
    cpu_limit_cores: quota != null && period > 0 ? quota / period : null,
  };
}

function parseMediaRoots(entries = []) {
  return entries.map((entry) => {
    const separator = entry.indexOf("=");
    if (separator <= 0 || separator === entry.length - 1) {
      throw new Error(`--media-root must use kind=path, got ${JSON.stringify(entry)}`);
    }
    const kind = entry.slice(0, separator);
    const path = resolve(entry.slice(separator + 1));
    return { kind, ...filesystemSnapshot(path) };
  });
}

function positiveIntegerOption(value, name, fallback) {
  if (value == null) return fallback;
  if (!/^\d+$/u.test(value)) {
    throw new Error(`${name} must be a positive integer`);
  }
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed < 1) {
    throw new Error(`${name} must be a positive integer`);
  }
  return parsed;
}

async function captureHealth(url, checkpoint = false) {
  if (!url) return null;
  const requestUrl = new URL(url);
  if (checkpoint) requestUrl.searchParams.set("checkpoint", "true");
  const response = await fetch(requestUrl, { signal: AbortSignal.timeout(5_000) });
  const text = await response.text();
  let body;
  try {
    body = JSON.parse(text);
  } catch {
    body = { raw: text.slice(0, 4096) };
  }
  return { url: requestUrl.toString(), status: response.status, ok: response.ok, body };
}

function writeJson(path, value) {
  writeFileSync(path, `${JSON.stringify(value, null, 2)}\n`, "utf8");
}

const timestamp = new Date().toISOString().replaceAll(":", "-");
const output = resolve(values.output ?? join(repoRoot, "perf-results", timestamp));
if (existsSync(output) && readdirSync(output).length) {
  throw new Error(`result directory must be empty: ${output}`);
}
mkdirSync(output, { recursive: true });

const databasePath = normalizeDatabasePath(values.database ?? process.env.DATABASE_URL);
const capturedAt = new Date().toISOString();
const health = await captureHealth(
  values["health-url"] ?? process.env.PERF_HEALTH_URL,
  values["health-checkpoint"] === true,
);
const mediaManifestRoots = parseMediaRoots(values["media-manifest"]);
const mediaManifestOptions = {
  maxFiles: positiveIntegerOption(
    values["media-manifest-max-files"],
    "--media-manifest-max-files",
    5_000_000,
  ),
  maxDirectories: positiveIntegerOption(
    values["media-manifest-max-directories"],
    "--media-manifest-max-directories",
    1_000_000,
  ),
  inspectionLimit: positiveIntegerOption(
    values["media-manifest-inspect-limit"],
    "--media-manifest-inspect-limit",
    256,
  ),
  headerBytes: positiveIntegerOption(
    values["media-manifest-header-bytes"],
    "--media-manifest-header-bytes",
    1024 * 1024,
  ),
  maxArchiveBytes: positiveIntegerOption(
    values["media-manifest-max-archive-bytes"],
    "--media-manifest-max-archive-bytes",
    64 * 1024 * 1024,
  ),
};
const mediaManifest = mediaManifestRoots.map((root) =>
  collectMediaRootManifest(root, mediaManifestOptions),
);
const dataset = databasePath
  ? collectDatasetManifest(databasePath)
  : {
      status: "database-not-provided",
      note: "Pass --database to capture works/assets/tags/inventory counts.",
    };
const environment = {
  artifact_version: PERF_ARTIFACT_VERSION,
  captured_at: capturedAt,
  source: captureGitState(repoRoot),
  runtime: {
    platform: process.platform,
    architecture: process.arch,
    node: process.version,
    os_type: os.type(),
    os_release: os.release(),
    hostname: os.hostname(),
    cpu_count: os.cpus().length,
    cpu_model: os.cpus()[0]?.model ?? null,
    total_memory_bytes: os.totalmem(),
    free_memory_bytes: os.freemem(),
  },
  cgroup: cgroupSnapshot(),
  feature_flags: Object.fromEntries(
    SAFE_ENVIRONMENT_KEYS.map((key) => [key, process.env[key] ?? null]),
  ),
  filesystems: {
    repository: filesystemSnapshot(repoRoot),
    database: databasePath ? filesystemSnapshot(dirname(databasePath)) : null,
    media_roots: parseMediaRoots(values["media-root"]),
  },
  health: health
    ? {
        url: health.url,
        status: health.status,
        ok: health.ok,
        schema_version: health.body?.schema_version ?? null,
      }
    : null,
};

writeJson(join(output, "environment.json"), environment);
writeJson(join(output, "dataset-manifest.json"), {
  artifact_version: PERF_ARTIFACT_VERSION,
  captured_at: capturedAt,
  media_manifest: mediaManifest,
  ...dataset,
});
if (health) writeJson(join(output, "health-snapshot.json"), health);
writeFileSync(join(output, "scenario-results.jsonl"), "", { encoding: "utf8", flag: "wx" });
writeFileSync(
  join(output, "system-samples.csv"),
  `${SYSTEM_SAMPLE_COLUMNS.join(",")}\n`,
  { encoding: "utf8", flag: "wx" },
);
writeJson(join(output, "summary.json"), summarizeScenarioRecords([], capturedAt));
writeJson(join(output, "run.json"), {
  artifact_version: PERF_ARTIFACT_VERSION,
  captured_at: capturedAt,
  status: "baseline-captured",
  artifacts: [
    "environment.json",
    "dataset-manifest.json",
    ...(health ? ["health-snapshot.json"] : []),
    "scenario-results.jsonl",
    "system-samples.csv",
    "summary.json",
  ],
});

console.log(output);
