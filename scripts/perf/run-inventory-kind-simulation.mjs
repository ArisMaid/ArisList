import { parseArgs } from "node:util";
import { performance } from "node:perf_hooks";
import { resolve } from "node:path";

import {
  PERF_ARTIFACT_VERSION,
} from "./perf-lib.mjs";
import { captureSourceProvenance } from "./provenance.mjs";
import {
  prepareRunnerOutputPaths,
  redactedError,
  writeJsonArtifact,
  writeRunnerFailureArtifact,
} from "./runner-utils.mjs";

const KINDS = new Set(["novel", "comic", "coser-picture", "audio", "gallery"]);
const DEFAULT_POLL_MS = 500;
const DEFAULT_TIMEOUT_MS = 120_000;

const { values } = parseArgs({
  options: {
    "base-url": { type: "string", default: process.env.INVENTORY_SIM_BASE_URL ?? "http://127.0.0.1:8788" },
    kind: { type: "string", default: "novel" },
    output: { type: "string" },
    "expected-files": { type: "string" },
    "timeout-ms": { type: "string", default: String(DEFAULT_TIMEOUT_MS) },
    "poll-ms": { type: "string", default: String(DEFAULT_POLL_MS) },
    "existing-job-id": { type: "string" },
    "password-env": { type: "string", default: "APP_ADMIN_PASSWORD" },
    "enqueue-enrichment": { type: "boolean", default: false },
    help: { type: "boolean", short: "h", default: false },
  },
  strict: true,
});

if (values.help || !values.output) {
  console.log(
    "Usage: node scripts/perf/run-inventory-kind-simulation.mjs --output <artifact.json> [--base-url <url>] [--kind novel|comic|coser-picture|audio|gallery] [--expected-files <n>] [--existing-job-id <id>]",
  );
  process.exit(values.help ? 0 : 2);
}

const repoRoot = resolve(new URL("../..", import.meta.url).pathname.replace(/^\/(\w:)/u, "$1"));
const output = resolve(values.output);
const kind = String(values.kind).trim().toLowerCase();
const baseUrl = String(values["base-url"]).replace(/\/+$/u, "");
const timeoutMs = Number(values["timeout-ms"]);
const pollMs = Number(values["poll-ms"]);
const expectedFiles = values["expected-files"] == null ? null : Number(values["expected-files"]);
const existingJobId = values["existing-job-id"] == null ? null : Number(values["existing-job-id"]);
const passwordEnv = String(values["password-env"]);
const startedAt = new Date().toISOString();

function assertPositiveInteger(value, label, { allowNull = false } = {}) {
  if (allowNull && value == null) return;
  if (!Number.isInteger(value) || value < 0) throw new Error(`${label} must be a non-negative integer`);
}

if (!KINDS.has(kind)) throw new Error(`unsupported kind ${kind}`);
assertPositiveInteger(timeoutMs, "timeout-ms");
assertPositiveInteger(pollMs, "poll-ms");
assertPositiveInteger(expectedFiles, "expected-files", { allowNull: true });
assertPositiveInteger(existingJobId, "existing-job-id", { allowNull: true });
if (expectedFiles === 0) throw new Error("expected-files must be greater than zero when provided");
if (existingJobId === 0) throw new Error("existing-job-id must be greater than zero when provided");

prepareRunnerOutputPaths(output);

function sleep(durationMs) {
  return new Promise((resolveSleep) => setTimeout(resolveSleep, durationMs));
}

function parseCookie(response) {
  const valuesFromHeader = typeof response.headers.getSetCookie === "function"
    ? response.headers.getSetCookie()
    : [];
  const raw = valuesFromHeader[0] ?? response.headers.get("set-cookie") ?? "";
  return raw.split(";", 1)[0];
}

async function requestJson(path, { method = "GET", body, cookie, csrf } = {}) {
  const headers = { accept: "application/json" };
  if (body !== undefined) headers["content-type"] = "application/json";
  if (cookie) headers.cookie = cookie;
  if (csrf) headers["x-csrf-token"] = csrf;
  const response = await fetch(`${baseUrl}${path}`, {
    method,
    headers,
    body: body === undefined ? undefined : JSON.stringify(body),
    signal: AbortSignal.timeout(15_000),
  });
  const text = await response.text();
  let parsed;
  try {
    parsed = text ? JSON.parse(text) : null;
  } catch {
    throw new Error(`${method} ${path} returned non-JSON HTTP ${response.status}`);
  }
  if (!response.ok) {
    const detail = typeof parsed === "string" ? parsed : JSON.stringify(parsed);
    throw new Error(`${method} ${path} returned HTTP ${response.status}: ${detail.slice(0, 512)}`);
  }
  return { body: parsed, cookie: parseCookie(response) };
}

function pickHealth(health) {
  const sqlite = health?.sqlite ?? {};
  const resources = health?.resources ?? {};
  const jobs = health?.jobs ?? {};
  return {
    status: health?.status ?? null,
    schema_version: health?.schema_version ?? null,
    cgroup_cpu: health?.cgroup_cpu ?? null,
    cgroup_memory: health?.cgroup_memory ?? null,
    jobs: {
      active_jobs: jobs.active_jobs ?? null,
      completed_jobs: jobs.completed_jobs ?? null,
      failed_jobs: jobs.failed_jobs ?? null,
      claim_errors: jobs.claim_errors ?? null,
    },
    resources: {
      background_paused: resources.background_paused ?? null,
      resource_wait_timeouts: resources.resource_wait_timeouts ?? null,
      resource_wait_total_micros: resources.resource_wait_total_micros ?? null,
      resource_wait_max_micros: resources.resource_wait_max_micros ?? null,
    },
    sqlite: {
      database_bytes: sqlite.database_bytes ?? null,
      wal_bytes: sqlite.wal_bytes ?? null,
      busy_errors: sqlite.sqlite_busy_errors ?? null,
      pool_acquire_timeouts: sqlite.pool_acquire_timeouts ?? null,
      pool_acquire_errors: sqlite.pool_acquire_errors ?? null,
      read_snapshot: sqlite.read_snapshot ?? null,
      write_gate: sqlite.write_gate ?? null,
    },
  };
}

function findJob(jobs, jobId) {
  return Array.isArray(jobs) ? jobs.find((job) => Number(job.id) === Number(jobId)) ?? null : null;
}

function findRoot(inventory) {
  return inventory?.roots?.find((root) => root.kind === kind) ?? null;
}

function jobDurationMs(job) {
  if (!job?.created_at || !job?.updated_at) return null;
  const created = Date.parse(job.created_at);
  const updated = Date.parse(job.updated_at);
  return Number.isFinite(created) && Number.isFinite(updated) && updated >= created
    ? updated - created
    : null;
}

function evaluateChecks({ job, inventory, health }) {
  const root = findRoot(inventory);
  const checks = [
    {
      id: "scan-job",
      status: job?.status === "done" ? "passed" : "failed",
      details: job?.status ?? "missing",
    },
    {
      id: "inventory-kind-enabled",
      status: inventory?.enabled === true && inventory.enabled_kinds?.includes(kind) ? "passed" : "failed",
      details: inventory?.enabled_kinds ?? [],
    },
    {
      id: "root-terminal-state",
      status: root?.status === "idle" && !root?.last_error ? "passed" : "failed",
      details: root ? { status: root.status, last_error: root.last_error } : "missing",
    },
    {
      id: "runtime-health",
      status: health?.status === "ok" && (health.jobs?.failed_jobs ?? 0) === 0
        && (health.jobs?.claim_errors ?? 0) === 0
        && (health.sqlite?.busy_errors ?? 0) === 0
        && (health.sqlite?.pool_acquire_timeouts ?? 0) === 0
        && (health.sqlite?.pool_acquire_errors ?? 0) === 0
        ? "passed"
        : "failed",
      details: health,
    },
  ];
  if (expectedFiles != null) {
    checks.push({
      id: "expected-file-count",
      status: root?.present_files === expectedFiles && root?.last_discovered === expectedFiles
        ? "passed"
        : "failed",
      details: {
        expected: expectedFiles,
        present: root?.present_files ?? null,
        discovered: root?.last_discovered ?? null,
      },
    });
  }
  return checks;
}

let runtimeOutput;
try {
  let jobId = existingJobId;
  let scanRequested = false;
  if (jobId == null) {
    const password = process.env[passwordEnv];
    if (!password) throw new Error(`missing admin password in ${passwordEnv}`);

    const login = await requestJson("/api/auth/login", {
      method: "POST",
      body: { password },
    });
    const cookie = login.cookie;
    const csrf = login.body?.csrf;
    if (!cookie || !csrf || login.body?.authenticated !== true) {
      throw new Error("login did not return an authenticated session and CSRF token");
    }

    const scan = await requestJson("/api/scan", {
      method: "POST",
      cookie,
      csrf,
      body: { kind, enqueue_enrichment: Boolean(values["enqueue-enrichment"]) },
    });
    jobId = scan.body?.job_id;
    scanRequested = true;
  }
  if (!Number.isInteger(Number(jobId))) throw new Error("scan response did not contain a job id");

  const startedMono = performance.now();
  const deadline = startedMono + timeoutMs;
  const samples = [];
  let final = null;
  while (performance.now() <= deadline) {
    const [jobsResponse, inventoryResponse, healthResponse] = await Promise.all([
      requestJson("/api/jobs"),
      requestJson("/api/inventory/status"),
      requestJson("/api/health/resources"),
    ]);
    const job = findJob(jobsResponse.body, jobId);
    const sample = {
      captured_at: new Date().toISOString(),
      elapsed_ms: Math.round(performance.now() - startedMono),
      job,
      inventory: inventoryResponse.body,
      health: pickHealth(healthResponse.body),
    };
    samples.push(sample);
    final = sample;
    if (["done", "failed"].includes(job?.status)) break;
    await sleep(pollMs);
  }

  const job = final?.job ?? null;
  const inventory = final?.inventory ?? null;
  const health = final?.health ?? null;
  const checks = evaluateChecks({ job, inventory, health });
  const passed = checks.every((check) => check.status === "passed");
  const artifact = {
    artifact_version: PERF_ARTIFACT_VERSION,
    scenario: "inventory-kind-simulation",
    status: passed ? "passed" : "failed",
    acceptance_role: expectedFiles == null ? "kind-smoke-evidence" : "kind-scale-evidence",
    started_at: startedAt,
    completed_at: new Date().toISOString(),
    source: {
      ...captureSourceProvenance(repoRoot),
      platform: process.platform,
      arch: process.arch,
      node: process.version,
    },
    runtime: {
      base_url: baseUrl,
      kind,
      expected_files: expectedFiles,
      timeout_ms: timeoutMs,
      poll_ms: pollMs,
      elapsed_ms: Math.round(performance.now() - startedMono),
      job_duration_ms: jobDurationMs(job),
    },
    simulation: {
      image: process.env.ARISLIST_SIM_IMAGE ?? null,
      container: process.env.SIM_CONTAINER_NAME ?? null,
      cpu_limit_cores: process.env.SIM_CPUS ? Number(process.env.SIM_CPUS) : null,
      memory_limit: process.env.SIM_MEMORY_LIMIT ?? null,
      memory_swap_limit: process.env.SIM_MEMORY_SWAP_LIMIT ?? null,
      pids_limit: process.env.SIM_PIDS_LIMIT ? Number(process.env.SIM_PIDS_LIMIT) : null,
    },
    request: {
      job_id: jobId,
      scan_requested: scanRequested,
      enqueue_enrichment: Boolean(values["enqueue-enrichment"]),
    },
    final: {
      job,
      inventory,
      health,
    },
    samples,
    checks,
    limitations: [
      "The runner measures the mounted root and Inventory coordinator path, not physical NAS/HDD latency.",
      ...(expectedFiles == null ? ["No target file count was supplied; this artifact is smoke evidence only."] : []),
    ],
  };
  writeJsonArtifact(output, artifact);
  runtimeOutput = artifact;
  console.log(JSON.stringify({ output, status: artifact.status, acceptance_role: artifact.acceptance_role, checks }));
  if (!passed) process.exitCode = 1;
} catch (error) {
  const failure = writeRunnerFailureArtifact({
    runtimeOutput: output,
    scenario: "inventory-kind-simulation",
    startedAt,
    metadata: {
      acceptance_role: expectedFiles == null ? "kind-smoke-evidence" : "kind-scale-evidence",
      runtime: { base_url: baseUrl, kind, expected_files: expectedFiles },
    },
    error,
  });
  runtimeOutput = failure;
  console.error(redactedError(error));
  process.exitCode = 1;
}

void runtimeOutput;
