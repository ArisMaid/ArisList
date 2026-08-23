import { createHash } from "node:crypto";
import {
  copyFileSync,
  existsSync,
  mkdirSync,
  readdirSync,
  statSync,
  writeFileSync,
  createReadStream,
} from "node:fs";
import { DatabaseSync } from "node:sqlite";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { spawn } from "node:child_process";
import { createServer } from "node:net";
import { parseArgs } from "node:util";
import { performance } from "node:perf_hooks";

import { PERF_ARTIFACT_VERSION } from "./perf-lib.mjs";
import { CURRENT_SCHEMA_VERSION } from "./schema-version.mjs";

const repoRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..", "..");
const defaultServerCandidates = [
  resolve(repoRoot, "target", "release", process.platform === "win32" ? "media-shelf-server.exe" : "media-shelf-server"),
  resolve(repoRoot, "target", "debug", process.platform === "win32" ? "media-shelf-server.exe" : "media-shelf-server"),
];

const { values } = parseArgs({
  options: {
    database: { type: "string", short: "d" },
    output: { type: "string", short: "o" },
    "server-bin": { type: "string" },
    "server-arg": { type: "string", multiple: true },
    "timeout-ms": { type: "string", default: "120000" },
    help: { type: "boolean", short: "h", default: false },
  },
  strict: true,
});

if (values.help || !values.database || !values.output) {
  console.log(`Usage: node scripts/perf/run-migration-gate.mjs --database <old.sqlite> --output <directory> [options]

Options:
  -d, --database <path>      Quiesced SQLite source/backup; opened read-only
  -o, --output <directory>   New artifact directory; non-empty directories refused
      --server-bin <path>    Server executable (release/debug binary auto-detected)
      --server-arg <value>   Extra server argument; repeatable, intended for test harnesses
      --timeout-ms <n>       Startup/readiness timeout (default: 120000)
  -h, --help                 Show this help

The source database is copied without modifying it. A non-empty WAL sidecar is
rejected because migration evidence must start from a quiesced, reproducible
snapshot. The server runs against the copied database and is stopped after the
health endpoint becomes ready.`);
  process.exit(values.help ? 0 : 2);
}

function positiveInteger(name, raw) {
  if (!/^\d+$/u.test(raw)) throw new Error(`${name} must be a positive integer`);
  const value = Number(raw);
  if (!Number.isSafeInteger(value) || value < 1) {
    throw new Error(`${name} must be a positive integer`);
  }
  return value;
}

function sizeOf(path) {
  try {
    const stats = statSync(path);
    return stats.isFile() ? stats.size : 0;
  } catch {
    return 0;
  }
}

function copyIfPresent(source, destination) {
  if (!existsSync(source)) return 0;
  copyFileSync(source, destination);
  return sizeOf(destination);
}

async function sha256File(path) {
  const hash = createHash("sha256");
  for await (const chunk of createReadStream(path)) hash.update(chunk);
  return hash.digest("hex");
}

function resolveServerPath(value) {
  if (value) return resolve(value);
  const found = defaultServerCandidates.find((candidate) => existsSync(candidate));
  if (!found) {
    throw new Error(
      `server executable not found; build media-shelf-server or pass --server-bin (checked ${defaultServerCandidates.join(", ")})`,
    );
  }
  return found;
}

async function findFreePort() {
  const server = createServer();
  await new Promise((resolvePromise, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", resolvePromise);
  });
  const address = server.address();
  const port = address && typeof address === "object" ? address.port : null;
  await new Promise((resolvePromise, reject) => {
    server.close((error) => (error ? reject(error) : resolvePromise()));
  });
  if (!Number.isInteger(port) || port < 1) throw new Error("failed to reserve a loopback port");
  return port;
}

async function waitForHealth(url, child, timeoutMs, getSpawnError) {
  const deadline = performance.now() + timeoutMs;
  let lastError = "health endpoint did not respond";
  while (performance.now() < deadline) {
    const spawnError = getSpawnError();
    if (spawnError) throw new Error(`server failed to spawn: ${spawnError}`);
    if (child.exitCode != null) {
      throw new Error(`server exited before readiness with code ${child.exitCode}`);
    }
    try {
      const response = await fetch(url, { signal: AbortSignal.timeout(1_000) });
      const body = await response.text();
      if (response.ok) return { status: response.status, body };
      lastError = `health returned ${response.status}: ${body.slice(0, 512)}`;
    } catch (error) {
      lastError = error instanceof Error ? error.message : String(error);
    }
    await new Promise((resolvePromise) => setTimeout(resolvePromise, 100));
  }
  throw new Error(`timed out waiting for ${url}: ${lastError}`);
}

async function stopChild(child) {
  if (child.exitCode != null) return;
  child.kill();
  await new Promise((resolvePromise) => {
    const timer = setTimeout(resolvePromise, 5_000);
    child.once("exit", () => {
      clearTimeout(timer);
      resolvePromise();
    });
  });
  if (child.exitCode == null) child.kill("SIGKILL");
}

function readMigrationFacts(databasePath) {
  const database = new DatabaseSync(databasePath, { readOnly: true });
  let schemaVersion = null;
  try {
    schemaVersion = database.prepare("SELECT MAX(version) AS version FROM schema_migrations").get()?.version ?? null;
  } catch {
    schemaVersion = null;
  }
  let integrity = null;
  try {
    integrity = database.prepare("PRAGMA integrity_check").get()?.integrity_check ?? null;
  } finally {
    database.close();
  }
  return { schema_version: schemaVersion == null ? null : Number(schemaVersion), integrity_check: integrity };
}

const source = resolve(values.database);
const output = resolve(values.output);
const timeoutMs = positiveInteger("--timeout-ms", values["timeout-ms"]);
if (!existsSync(source) || !statSync(source).isFile()) throw new Error(`database is not a file: ${source}`);
if (existsSync(output) && readdirSync(output).length > 0) {
  throw new Error(`result directory must be empty: ${output}`);
}
mkdirSync(output, { recursive: true });

const sourceWal = `${source}-wal`;
const sourceShm = `${source}-shm`;
const sourceWalBytes = sizeOf(sourceWal);
const sourceShmBytes = sizeOf(sourceShm);
if (sourceWalBytes > 0) {
  throw new Error(`database has a non-empty WAL sidecar (${sourceWalBytes} bytes); create a quiesced backup first`);
}
const sourceHashBefore = await sha256File(source);

const stagingData = join(output, "staging", "data");
const stagingGenerated = join(output, "staging", "generated");
const stagingCoverCache = join(output, "staging", "cover-cache");
const stagingDerivativeCache = join(output, "staging", "derivatives");
const stagingMedia = join(output, "staging", "empty-media");
for (const directory of [stagingData, stagingGenerated, stagingCoverCache, stagingDerivativeCache, stagingMedia]) {
  mkdirSync(directory, { recursive: true });
}
const snapshot = join(stagingData, "library.sqlite");
copyFileSync(source, snapshot);

const port = await findFreePort();
const serverBin = resolveServerPath(values["server-bin"]);
const childEnv = { ...process.env };
for (const key of ["OPENAI_API_KEY", "LIGHTNOVEL_ACCESS_TOKEN", "PERF_COOKIE", "PERF_AUTHORIZATION", "QMEDIASYNC_BASE_URL"]) {
  delete childEnv[key];
}
Object.assign(childEnv, {
  APP_BIND: `127.0.0.1:${port}`,
  APP_ADMIN_PASSWORD: "migration-gate-password",
  SESSION_SECRET: "migration-gate-session-secret-0123456789abcdef",
  DATABASE_URL: `sqlite://${snapshot.replaceAll("\\", "/")}`,
  DATA_DIR: stagingData,
  GENERATED_DIR: stagingGenerated,
  COVER_CACHE_DIR: stagingCoverCache,
  DERIVATIVE_CACHE_DIR: stagingDerivativeCache,
  COMICS_DIR: stagingMedia,
  NOVELS_DIR: stagingMedia,
  AUDIO_DIR: stagingMedia,
  GALLERY_DIR: stagingMedia,
  COSER_PICTURE_DIR: stagingMedia,
  STATIC_DIR: resolve(repoRoot, "frontend", "dist"),
  ENABLE_FILE_WATCHER: "false",
  CATALOG_V2_ENABLED: "false",
  FACET_BITMAP_ENABLED: "false",
  INVENTORY_SCANNER_ENABLED: "false",
  SEARCH_OUTBOX_SHADOW_ENABLED: "false",
  SEARCH_SHADOW_CANARY_ENABLED: "false",
  SEARCH_INCREMENTAL_READER_ENABLED: "false",
  DERIVATIVE_CACHE_V2_ENABLED: "false",
  JPEG_THUMBNAIL_DOWNSCALE_ENABLED: "false",
  RESOURCE_PROFILE: "nas-n100-4g",
});

const stdout = [];
const stderr = [];
const child = spawn(serverBin, values["server-arg"] ?? [], {
  cwd: repoRoot,
  env: childEnv,
  stdio: ["ignore", "pipe", "pipe"],
  windowsHide: true,
});
let spawnError = null;
child.once("error", (error) => {
  spawnError = error instanceof Error ? error.message : String(error);
});
child.stdout.setEncoding("utf8");
child.stderr.setEncoding("utf8");
child.stdout.on("data", (chunk) => stdout.push(chunk));
child.stderr.on("data", (chunk) => stderr.push(chunk));

const started = performance.now();
let readiness = null;
let failure = spawnError;
try {
  if (!failure) {
    readiness = await waitForHealth(
      `http://127.0.0.1:${port}/api/health`,
      child,
      timeoutMs,
      () => spawnError,
    );
  }
} catch (error) {
  failure = error instanceof Error ? error.message : String(error);
} finally {
  await stopChild(child);
}

writeFileSync(join(output, "server.stdout.log"), stdout.join(""), "utf8");
writeFileSync(join(output, "server.stderr.log"), stderr.join(""), "utf8");

let facts = null;
let factsError = null;
try {
  facts = readMigrationFacts(snapshot);
} catch (error) {
  factsError = error instanceof Error ? error.message : String(error);
}
let restoreFacts = null;
let restoreError = null;
let restoreMainHash = null;
let snapshotMainHash = null;
let restoreSidecars = null;
try {
  const restoreData = join(output, "restore", "data");
  mkdirSync(restoreData, { recursive: true });
  const restored = join(restoreData, "library.sqlite");
  copyFileSync(snapshot, restored);
  const restoredWalBytes = copyIfPresent(`${snapshot}-wal`, `${restored}-wal`);
  const restoredShmBytes = copyIfPresent(`${snapshot}-shm`, `${restored}-shm`);
  snapshotMainHash = await sha256File(snapshot);
  restoreMainHash = await sha256File(restored);
  restoreFacts = readMigrationFacts(restored);
  restoreSidecars = { wal_bytes: restoredWalBytes, shm_bytes: restoredShmBytes };
} catch (error) {
  restoreError = error instanceof Error ? error.message : String(error);
}
const sourceHashAfter = await sha256File(source);
const sourceUnchanged = sourceHashBefore === sourceHashAfter;
const restorePassed = restoreFacts?.schema_version >= CURRENT_SCHEMA_VERSION
  && restoreFacts.integrity_check === "ok"
  && snapshotMainHash != null
  && snapshotMainHash === restoreMainHash;
const status = readiness
  && facts?.schema_version >= CURRENT_SCHEMA_VERSION
  && facts.integrity_check === "ok"
  && restorePassed
  ? "passed"
  : "failed";
const result = {
  artifact_version: PERF_ARTIFACT_VERSION,
  captured_at: new Date().toISOString(),
  status,
  source_database: source,
  source_database_sha256_before: sourceHashBefore,
  source_database_sha256_after: sourceHashAfter,
  source_unchanged: sourceUnchanged,
  source_sidecars: { wal_bytes: sourceWalBytes, shm_bytes: sourceShmBytes },
  snapshot_database: snapshot,
  server: { executable: serverBin, args: values["server-arg"] ?? [], bind: `127.0.0.1:${port}` },
  startup_millis: Math.round(performance.now() - started),
  readiness,
  migration: facts,
  migration_error: factsError,
  restore: {
    status: restorePassed ? "passed" : "failed",
    schema_version: restoreFacts?.schema_version ?? null,
    integrity_check: restoreFacts?.integrity_check ?? null,
    database: restoreFacts ? join(output, "restore", "data", "library.sqlite") : null,
    snapshot_main_sha256: snapshotMainHash,
    restored_main_sha256: restoreMainHash,
    sidecars: restoreSidecars,
    error: restoreError,
  },
  failure,
  logs: { stdout: "server.stdout.log", stderr: "server.stderr.log" },
};
if (!sourceUnchanged) {
  result.status = "failed";
  result.failure = result.failure ?? "source database changed during migration gate";
}
writeFileSync(join(output, "migration.json"), `${JSON.stringify(result, null, 2)}\n`, "utf8");
console.log(JSON.stringify({ output, status, startup_millis: result.startup_millis, schema_version: facts?.schema_version ?? null }));
if (status !== "passed") process.exitCode = 1;
