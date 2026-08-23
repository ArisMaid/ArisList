import { createHash } from "node:crypto";
import {
  copyFileSync,
  createReadStream,
  existsSync,
  mkdirSync,
  readdirSync,
  readFileSync,
  statSync,
  writeFileSync,
} from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { spawn } from "node:child_process";
import { createServer } from "node:net";
import { parseArgs } from "node:util";
import { performance } from "node:perf_hooks";

import { PERF_ARTIFACT_VERSION } from "./perf-lib.mjs";
import { writeJsonArtifact, writeRunnerFailureArtifact } from "./runner-utils.mjs";
import { CURRENT_SCHEMA_VERSION } from "./schema-version.mjs";

const repoRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..", "..");
const serverCandidates = [
  resolve(repoRoot, "target", "release", process.platform === "win32" ? "media-shelf-server.exe" : "media-shelf-server"),
  resolve(repoRoot, "target", "debug", process.platform === "win32" ? "media-shelf-server.exe" : "media-shelf-server"),
];

const { values } = parseArgs({
  options: {
    database: { type: "string", short: "d" },
    "search-index": { type: "string", short: "i" },
    output: { type: "string", short: "o" },
    "server-bin": { type: "string" },
    requests: { type: "string", short: "n", default: "30" },
    "timeout-ms": { type: "string", default: "120000" },
    help: { type: "boolean", short: "h", default: false },
  },
  strict: true,
});

if (values.help || !values.database || !values["search-index"] || !values.output) {
  console.log(`Usage: node scripts/perf/run-r1g-prewarm-ab.mjs --database <quiesced.sqlite> --search-index <index-dir> --output <directory> [options]

Runs the same v${CURRENT_SCHEMA_VERSION} fixed corpus twice against isolated data directories. The
lazy variant starts with SEARCH_READER_PREWARM_ENABLED=false; the prewarm
variant starts with it enabled. Both variants use the same copied SQLite file
and Tantivy index. This is development evidence, not an N100 acceptance gate.`);
  process.exit(values.help ? 0 : 2);
}

function boundedInteger(name, raw, minimum, maximum) {
  const value = Number(raw);
  if (!Number.isInteger(value) || value < minimum || value > maximum) {
    throw new Error(`${name} must be an integer between ${minimum} and ${maximum}`);
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

async function sha256File(path) {
  const hash = createHash("sha256");
  for await (const chunk of createReadStream(path)) hash.update(chunk);
  return hash.digest("hex");
}

function copyTree(source, destination) {
  mkdirSync(destination, { recursive: true });
  for (const entry of readdirSync(source, { withFileTypes: true })) {
    if (entry.name === ".tantivy-meta.lock" || entry.name === ".tantivy-writer.lock") continue;
    const sourcePath = join(source, entry.name);
    const destinationPath = join(destination, entry.name);
    if (entry.isDirectory()) copyTree(sourcePath, destinationPath);
    else if (entry.isFile()) copyFileSync(sourcePath, destinationPath);
  }
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
    if (child.exitCode != null) throw new Error(`server exited before readiness with code ${child.exitCode}`);
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

function resolveServer(value) {
  if (value) return resolve(value);
  const found = serverCandidates.find((candidate) => existsSync(candidate));
  if (!found) throw new Error(`server executable not found; checked ${serverCandidates.join(", ")}`);
  return found;
}

function parseHealth(body) {
  try {
    return JSON.parse(body);
  } catch {
    throw new Error("health endpoint returned invalid JSON");
  }
}

function readJsonIfPresent(path) {
  return existsSync(path) ? JSON.parse(readFileSync(path, "utf8")) : null;
}

async function runVariant({ name, prewarm, sourceDatabase, sourceIndex, output, serverBin, requests, timeoutMs }) {
  const variantRoot = join(output, name);
  const dataDir = join(variantRoot, "data");
  const generatedDir = join(variantRoot, "generated");
  const coverCacheDir = join(variantRoot, "cover-cache");
  const derivativeCacheDir = join(variantRoot, "derivatives");
  const mediaDir = join(variantRoot, "empty-media");
  const indexDir = join(dataDir, "search-index-v2");
  const evidenceDir = join(variantRoot, "evidence");
  for (const directory of [dataDir, generatedDir, coverCacheDir, derivativeCacheDir, mediaDir, evidenceDir]) {
    mkdirSync(directory, { recursive: true });
  }
  copyFileSync(sourceDatabase, join(dataDir, "library.sqlite"));
  copyTree(sourceIndex, indexDir);

  const port = await findFreePort();
  const childEnv = { ...process.env };
  for (const key of ["OPENAI_API_KEY", "LIGHTNOVEL_ACCESS_TOKEN", "PERF_COOKIE", "PERF_AUTHORIZATION", "QMEDIASYNC_BASE_URL"]) {
    delete childEnv[key];
  }
  Object.assign(childEnv, {
    APP_BIND: `127.0.0.1:${port}`,
    APP_ADMIN_PASSWORD: "r1g-prewarm-gate-password",
    SESSION_SECRET: "r1g-prewarm-gate-session-secret-0123456789abcdef",
    DATABASE_URL: `sqlite://${join(dataDir, "library.sqlite").replaceAll("\\", "/")}`,
    DATA_DIR: dataDir,
    GENERATED_DIR: generatedDir,
    COVER_CACHE_DIR: coverCacheDir,
    DERIVATIVE_CACHE_DIR: derivativeCacheDir,
    COMICS_DIR: mediaDir,
    NOVELS_DIR: mediaDir,
    AUDIO_DIR: mediaDir,
    GALLERY_DIR: mediaDir,
    COSER_PICTURE_DIR: mediaDir,
    STATIC_DIR: resolve(repoRoot, "frontend", "dist"),
    ENABLE_FILE_WATCHER: "false",
    CATALOG_V2_ENABLED: "false",
    FACET_BITMAP_ENABLED: "false",
    INVENTORY_SCANNER_ENABLED: "false",
    SEARCH_OUTBOX_SHADOW_ENABLED: "false",
    SEARCH_SHADOW_CANARY_ENABLED: "false",
    SEARCH_INCREMENTAL_READER_ENABLED: "false",
    SEARCH_READER_PREWARM_ENABLED: prewarm ? "true" : "false",
    DERIVATIVE_CACHE_V2_ENABLED: "false",
    JPEG_THUMBNAIL_DOWNSCALE_ENABLED: "false",
    RESOURCE_PROFILE: "nas-n100-4g",
  });

  const stdout = [];
  const stderr = [];
  const child = spawn(serverBin, [], {
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
  let startupMillis = null;
  let failure = spawnError;
  let health = null;
  try {
    if (!failure) {
      readiness = await waitForHealth(`http://127.0.0.1:${port}/api/health`, child, timeoutMs, () => spawnError);
      health = parseHealth(readiness.body);
      startupMillis = Math.round(performance.now() - started);
      if (health.features?.search_reader_prewarm !== prewarm) {
        throw new Error(`health feature mismatch: expected reader prewarm=${prewarm}`);
      }
      const runner = spawn(process.execPath, [
        resolve(repoRoot, "scripts", "perf", "run-r1g-scenario.mjs"),
        "--base-url", `http://127.0.0.1:${port}`,
        "--output", join(evidenceDir, "scenario-results.jsonl"),
        "--runtime-output", join(evidenceDir, "r1g-runtime.json"),
        "--requests", String(requests),
        "--timeout", String(Math.min(timeoutMs, 60_000)),
      ], {
        cwd: repoRoot,
        env: childEnv,
        stdio: ["ignore", "pipe", "pipe"],
        windowsHide: true,
      });
      const runnerStdout = [];
      const runnerStderr = [];
      runner.stdout.setEncoding("utf8");
      runner.stderr.setEncoding("utf8");
      runner.stdout.on("data", (chunk) => runnerStdout.push(chunk));
      runner.stderr.on("data", (chunk) => runnerStderr.push(chunk));
      const runnerExit = await new Promise((resolvePromise, reject) => {
        runner.once("error", reject);
        runner.once("exit", (code, signal) => resolvePromise({ code, signal }));
      });
      writeFileSync(join(evidenceDir, "runner.stdout.log"), runnerStdout.join(""), "utf8");
      writeFileSync(join(evidenceDir, "runner.stderr.log"), runnerStderr.join(""), "utf8");
      if (runnerExit.code !== 0) throw new Error(`R1G runner exited with code ${runnerExit.code ?? "null"} (${runnerExit.signal ?? "no signal"})`);
    }
  } catch (error) {
    failure = error instanceof Error ? error.message : String(error);
  } finally {
    await stopChild(child);
    writeFileSync(join(variantRoot, "server.stdout.log"), stdout.join(""), "utf8");
    writeFileSync(join(variantRoot, "server.stderr.log"), stderr.join(""), "utf8");
  }

  const runtimePath = join(evidenceDir, "r1g-runtime.json");
  const runtime = readJsonIfPresent(runtimePath);
  const result = {
    artifact_version: PERF_ARTIFACT_VERSION,
    status: failure == null && runtime?.status === "passed" ? "passed" : "failed",
    variant: name,
    search_reader_prewarm_enabled: prewarm,
    server: { executable: serverBin, bind: `127.0.0.1:${port}` },
    startup_millis: startupMillis,
    readiness_status: readiness?.status ?? null,
    health_features: health?.features ?? null,
    runtime_artifact: runtimePath,
    scenario_artifact: join(evidenceDir, "scenario-results.jsonl"),
    source_database_sha256: await sha256File(sourceDatabase),
    index_meta_bytes: sizeOf(join(sourceIndex, "meta.json")),
    failure,
  };
  writeJsonArtifact(join(variantRoot, "variant.json"), result);
  return result;
}

const sourceDatabase = resolve(values.database);
const sourceIndex = resolve(values["search-index"]);
const output = resolve(values.output);
const requests = boundedInteger("requests", values.requests, 1, 10_000);
const timeoutMs = boundedInteger("timeout-ms", values["timeout-ms"], 1_000, 10 * 60 * 1_000);
const serverBin = resolveServer(values["server-bin"]);
if (!existsSync(sourceDatabase) || !statSync(sourceDatabase).isFile()) throw new Error(`database is not a file: ${sourceDatabase}`);
if (!existsSync(sourceIndex) || !statSync(sourceIndex).isDirectory() || !existsSync(join(sourceIndex, "meta.json"))) {
  throw new Error(`search index directory is missing meta.json: ${sourceIndex}`);
}
if (existsSync(output) && readdirSync(output).length > 0) throw new Error(`result directory must be empty: ${output}`);
mkdirSync(output, { recursive: true });

const startedAt = new Date().toISOString();
try {
  const variants = [];
  for (const variant of [
    { name: "lazy", prewarm: false },
    { name: "prewarm", prewarm: true },
  ]) {
    variants.push(await runVariant({
      ...variant,
      sourceDatabase,
      sourceIndex,
      output,
      serverBin,
      requests,
      timeoutMs,
    }));
  }
  const lazy = variants.find((variant) => variant.variant === "lazy");
  const prewarm = variants.find((variant) => variant.variant === "prewarm");
  const summary = {
    artifact_version: PERF_ARTIFACT_VERSION,
    status: variants.every((variant) => variant.status === "passed") ? "passed" : "failed",
    evidence_kind: "development-ab",
    started_at: startedAt,
    completed_at: new Date().toISOString(),
    source_database: sourceDatabase,
    source_index: sourceIndex,
    requests,
    variants,
    comparison: {
      startup_millis_delta_prewarm_minus_lazy: prewarm?.startup_millis != null && lazy?.startup_millis != null
        ? prewarm.startup_millis - lazy.startup_millis
        : null,
      lazy_runtime: lazy ? readJsonIfPresent(lazy.runtime_artifact) : null,
      prewarm_runtime: prewarm ? readJsonIfPresent(prewarm.runtime_artifact) : null,
    },
    checks: [
      {
        id: "both-variants-complete",
        status: variants.every((variant) => variant.status === "passed") ? "passed" : "failed",
        failures: variants.filter((variant) => variant.status !== "passed").map((variant) => `${variant.variant}: ${variant.failure ?? "runtime artifact failed"}`),
      },
      {
        id: "prewarm-feature-observed",
        status: prewarm?.health_features?.search_reader_prewarm === true ? "passed" : "failed",
        failures: prewarm?.health_features?.search_reader_prewarm === true ? [] : ["prewarm variant health did not report enabled"],
      },
    ],
  };
  writeJsonArtifact(join(output, "summary.json"), summary);
  console.log(JSON.stringify({ status: summary.status, output, requests }));
  if (summary.status !== "passed") process.exitCode = 1;
} catch (error) {
  const failure = writeRunnerFailureArtifact({
    runtimeOutput: join(output, "summary.json"),
    scenario: "r1g-reader-prewarm-ab",
    startedAt,
    metadata: { source_database: sourceDatabase, source_index: sourceIndex, requests },
    error,
  });
  console.error(JSON.stringify({ status: failure.status, output, error: failure.error }));
  process.exitCode = 1;
}
