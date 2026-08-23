import { parseArgs } from "node:util";
import { performance } from "node:perf_hooks";
import { existsSync } from "node:fs";
import { resolve } from "node:path";

import { PERF_ARTIFACT_VERSION } from "./perf-lib.mjs";
import { captureSourceProvenance } from "./provenance.mjs";
import {
  prepareRunnerOutputPaths,
  redactedError,
  writeJsonArtifact,
} from "./runner-utils.mjs";

const KINDS = new Set(["novel", "comic", "coser-picture", "audio", "gallery"]);
const RECONCILIATION_KINDS = new Set(KINDS);
const DEFAULT_TIMEOUT_MS = 120_000;
const DEFAULT_POLL_MS = 500;

const { values } = parseArgs({
  options: {
    "base-url": {
      type: "string",
      default: process.env.INVENTORY_SIM_BASE_URL ?? "http://127.0.0.1:8788",
    },
    kind: { type: "string", default: "novel" },
    output: { type: "string" },
    "expected-files": { type: "string" },
    "timeout-ms": { type: "string", default: String(DEFAULT_TIMEOUT_MS) },
    "poll-ms": { type: "string", default: String(DEFAULT_POLL_MS) },
    "password-env": { type: "string", default: "APP_ADMIN_PASSWORD" },
    reason: { type: "string", default: "inventory promotion/rollback rehearsal" },
    "enqueue-reconciliation": { type: "boolean", default: false },
    "allow-ownership-mutation": { type: "boolean", default: false },
    help: { type: "boolean", short: "h", default: false },
  },
  strict: true,
});

if (values.help || !values.output) {
  console.log(`Usage: node scripts/perf/run-inventory-promotion-rollback.mjs \\
  --output <artifact.json> --kind <novel|comic|coser-picture|audio|gallery> \\
  --allow-ownership-mutation [--base-url <url>] [--expected-files <n>] \\
  [--enqueue-reconciliation] [--timeout-ms <n>] [--poll-ms <n>]

The runner is fail-closed. It refuses to mutate Catalog ownership unless
--allow-ownership-mutation is explicit, requires a current passing reconciliation
before promotion, records bounded before/after evidence, and rolls back to the
legacy writer after a successful promotion. Use only against a disposable or
explicitly approved deployment database.`);
  process.exit(values.help ? 0 : 2);
}

const repoRoot = resolve(new URL("../..", import.meta.url).pathname.replace(/^\/(\w:)/u, "$1"));
const output = resolve(values.output);
const kind = String(values.kind).trim().toLowerCase();
const baseUrl = new URL(String(values["base-url"]));
const timeoutMs = Number(values["timeout-ms"]);
const pollMs = Number(values["poll-ms"]);
const expectedFiles = values["expected-files"] == null ? null : Number(values["expected-files"]);
const passwordEnv = String(values["password-env"]);
const reason = String(values.reason).trim();
const startedAt = new Date().toISOString();

function assertInteger(value, label, minimum, maximum = Number.MAX_SAFE_INTEGER) {
  if (!Number.isInteger(value) || value < minimum || value > maximum) {
    throw new Error(`${label} must be an integer between ${minimum} and ${maximum}`);
  }
}

if (!KINDS.has(kind)) throw new Error(`unsupported kind ${kind}`);
if (!RECONCILIATION_KINDS.has(kind)) throw new Error(`unsupported reconciliation kind ${kind}`);
if (!new Set(["http:", "https:"]).has(baseUrl.protocol)) {
  throw new Error("--base-url must use http or https");
}
if (baseUrl.username || baseUrl.password) throw new Error("--base-url must not contain URL credentials");
assertInteger(timeoutMs, "timeout-ms", 1_000, 30 * 60 * 1_000);
assertInteger(pollMs, "poll-ms", 10, 10_000);
if (expectedFiles != null) assertInteger(expectedFiles, "expected-files", 1, Number.MAX_SAFE_INTEGER);
if (!reason || reason.length > 512) throw new Error("reason must contain 1..512 characters");
prepareRunnerOutputPaths(output);
if (existsSync(output)) throw new Error(`refusing to overwrite existing artifact: ${output}`);

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
  const response = await fetch(new URL(path, baseUrl), {
    method,
    headers,
    body: body === undefined ? undefined : JSON.stringify(body),
    signal: AbortSignal.timeout(15_000),
  });
  const text = await response.text();
  let parsed = null;
  try {
    parsed = text ? JSON.parse(text) : null;
  } catch {
    throw new Error(`${method} ${path} returned non-JSON HTTP ${response.status}`);
  }
  if (!response.ok) {
    const detail = typeof parsed === "string" ? parsed : JSON.stringify(parsed);
    throw new Error(`${method} ${path} returned HTTP ${response.status}: ${detail.slice(0, 512)}`);
  }
  return { body: parsed, cookie: parseCookie(response), status: response.status };
}

function ownershipFor(body) {
  return Array.isArray(body) ? body.find((item) => item?.kind === kind) ?? null : null;
}

function inventoryRootFor(body) {
  return body?.roots?.find((root) => root?.kind === kind) ?? null;
}

function reconciliationFor(body) {
  return body?.items?.find((item) => item?.kind === kind) ?? null;
}

function jobFor(body, id) {
  return Array.isArray(body) ? body.find((job) => Number(job?.id) === Number(id)) ?? null : null;
}

function runtimeHealth(body) {
  const sqlite = body?.sqlite ?? {};
  const jobs = body?.jobs ?? {};
  const resources = body?.resources ?? {};
  return {
    status: body?.status ?? null,
    failed_jobs: jobs.failed_jobs ?? null,
    claim_errors: jobs.claim_errors ?? null,
    busy_errors: sqlite.sqlite_busy_errors ?? null,
    pool_acquire_timeouts: sqlite.pool_acquire_timeouts ?? null,
    pool_acquire_errors: sqlite.pool_acquire_errors ?? null,
    resource_wait_timeouts: resources.resource_wait_timeouts ?? null,
    search_features: body?.search_features ?? null,
    search_outbox: body?.search_outbox ?? null,
    search_reconciliation: body?.search_reconciliation ?? null,
  };
}

function rootReady(root) {
  return root?.status === "idle"
    && !root?.last_error
    && (root?.active_token == null)
    && root?.generation === root?.completed_generation;
}

function reconciliationReady(status) {
  return status?.status === "passed"
    && status?.current === true
    && status?.expected_works === status?.matched_works
    && status?.missing_works === 0
    && status?.unexpected_works === 0
    && status?.mismatch_works === 0
    && status?.error_works === 0;
}

function ownershipReady(ownership, writer) {
  return ownership?.authoritative_writer === writer
    && ownership?.pending_events === 0
    && ownership?.failed_events === 0;
}

function healthReady(health) {
  return health?.status === "ok"
    && (health.failed_jobs ?? 0) === 0
    && (health.claim_errors ?? 0) === 0
    && (health.busy_errors ?? 0) === 0
    && (health.pool_acquire_timeouts ?? 0) === 0
    && (health.pool_acquire_errors ?? 0) === 0
    && (health.resource_wait_timeouts ?? 0) === 0;
}

function searchPromotionEvidenceReady(snapshotValue) {
  const health = snapshotValue?.health ?? {};
  const outbox = health.search_outbox ?? {};
  const reconciliation = health.search_reconciliation ?? {};
  return reconciliation.status === "passed"
    && (outbox.pending ?? 0) === 0
    && (outbox.revision_lag ?? 0) === 0
    && (outbox.search_revision_lag ?? 0) === 0;
}

async function login() {
  const password = process.env[passwordEnv];
  if (!password) throw new Error(`missing admin password in ${passwordEnv}`);
  const response = await requestJson("/api/auth/login", {
    method: "POST",
    body: { password },
  });
  const cookie = response.cookie;
  const csrf = response.body?.csrf;
  if (!cookie || !csrf || response.body?.authenticated !== true) {
    throw new Error("login did not return an authenticated session and CSRF token");
  }
  return { cookie, csrf };
}

async function snapshot(session) {
  const [healthResponse, ownershipResponse, inventoryResponse, reconciliationResponse, jobsResponse] =
    await Promise.all([
      requestJson("/api/health/resources", session),
      requestJson("/api/catalog/ownership", session),
      requestJson("/api/inventory/status", session),
      requestJson("/api/catalog/reconciliation", session),
      requestJson("/api/jobs", session),
    ]);
  return {
    captured_at: new Date().toISOString(),
    health: runtimeHealth(healthResponse.body),
    ownership: ownershipFor(ownershipResponse.body),
    inventory: inventoryRootFor(inventoryResponse.body),
    inventory_enabled: inventoryResponse.body?.enabled === true,
    inventory_enabled_kinds: inventoryResponse.body?.enabled_kinds ?? [],
    reconciliation: reconciliationFor(reconciliationResponse.body),
    jobs: jobsResponse.body,
  };
}

function snapshotReady(snapshotValue, writer) {
  return ownershipReady(snapshotValue.ownership, writer)
    && rootReady(snapshotValue.inventory)
    && healthReady(snapshotValue.health);
}

function compactSnapshot(snapshotValue) {
  return {
    captured_at: snapshotValue.captured_at,
    health: snapshotValue.health,
    ownership: snapshotValue.ownership,
    inventory: snapshotValue.inventory,
    inventory_enabled: snapshotValue.inventory_enabled,
    inventory_enabled_kinds: snapshotValue.inventory_enabled_kinds,
    reconciliation: snapshotValue.reconciliation,
  };
}

async function awaitState(session, predicate, label, jobId, samples, startedMono) {
  const deadline = performance.now() + timeoutMs;
  let latest = null;
  while (performance.now() <= deadline) {
    latest = await snapshot(session);
    samples.push({
      phase: label,
      elapsed_ms: Math.round(performance.now() - startedMono),
      ...compactSnapshot(latest),
      job: jobFor(latest.jobs, jobId),
    });
    if (predicate(latest, jobId)) return latest;
    await sleep(pollMs);
  }
  throw new Error(`${label} did not reach a terminal ready state within ${timeoutMs}ms`);
}

async function enqueueReconciliation(session, samples, startedMono) {
  const response = await requestJson(`/api/catalog/reconciliation/${kind}`, {
    ...session,
    method: "POST",
    body: {},
  });
  const jobId = response.body?.job_id;
  if (!Number.isInteger(Number(jobId))) throw new Error("reconciliation response did not contain a job id");
  const completed = await awaitState(
    session,
    (state, currentJobId) => {
      const job = jobFor(state.jobs, currentJobId);
      return job?.status === "done" && reconciliationReady(state.reconciliation);
    },
    "reconciliation",
    jobId,
    samples,
    startedMono,
  );
  return completed.reconciliation;
}

async function ensureSearchPromotionEvidence(session, samples, startedMono) {
  let latest = await snapshot(session);
  if (searchPromotionEvidenceReady(latest)) return latest;

  const deadline = performance.now() + timeoutMs;
  while (performance.now() <= deadline) {
    try {
      const response = await requestJson("/api/search/shadow/reconcile", session);
      latest = await snapshot(session);
      samples.push({
        phase: "search-reconciliation",
        elapsed_ms: Math.round(performance.now() - startedMono),
        report: response.body,
        ...compactSnapshot(latest),
      });
      if (searchPromotionEvidenceReady(latest)) return latest;
    } catch (error) {
      samples.push({
        phase: "search-reconciliation",
        elapsed_ms: Math.round(performance.now() - startedMono),
        error: redactedError(error),
      });
    }
    await sleep(pollMs);
  }
  throw new Error("Search shadow reconciliation did not reach a current passing evidence row");
}

async function changeOwnership(session, action) {
  return requestJson(`/api/catalog/ownership/${kind}`, {
    ...session,
    method: "POST",
    body: { action, reason },
  });
}

function promotionComplete(state, jobId) {
  const job = jobFor(state.jobs, jobId);
  return job?.status === "done"
    && snapshotReady(state, "catalog-v2");
}

function rollbackComplete(state, jobId) {
  const job = jobFor(state.jobs, jobId);
  return job?.status === "done"
    && snapshotReady(state, "legacy");
}

function checksFor({ initial, reconciliation, promoted, rolledBack, mutationConsent, error }) {
  const checks = [
    {
      id: "explicit-mutation-consent",
      status: mutationConsent ? "passed" : "failed",
      details: mutationConsent ? "--allow-ownership-mutation was supplied" : "ownership mutation was refused",
    },
    {
      id: "initial-legacy-ownership",
      status: initial?.ownership?.authoritative_writer === "legacy" ? "passed" : "failed",
      details: initial?.ownership?.authoritative_writer ?? null,
    },
    {
      id: "inventory-root-ready",
      status: initial && initial.inventory_enabled && initial.inventory_enabled_kinds.includes(kind)
        && rootReady(initial.inventory) ? "passed" : "failed",
      details: {
        enabled: initial?.inventory_enabled ?? null,
        enabled_kinds: initial?.inventory_enabled_kinds ?? [],
        root: initial?.inventory ?? null,
      },
    },
    {
      id: "reconciliation-current-pass",
      status: reconciliationReady(reconciliation) ? "passed" : "failed",
      details: reconciliation ?? null,
    },
    {
      id: "promotion-complete",
      status: promoted && promotionComplete(promoted, promoted.promotion_job_id) ? "passed" : "failed",
      details: promoted ? compactSnapshot(promoted) : null,
    },
    {
      id: "rollback-complete",
      status: rolledBack && rollbackComplete(rolledBack, rolledBack.rollback_job_id) ? "passed" : "failed",
      details: rolledBack ? compactSnapshot(rolledBack) : null,
    },
  ];
  if (error) checks.push({ id: "runner-execution", status: "failed", details: error });
  return checks;
}

let artifact;
let session;
let initial = null;
let promoted = null;
let rolledBack = null;
let reconciliation = null;
let promotionResponse = null;
let rollbackResponse = null;
let promotionJobId = null;
let rollbackJobId = null;
let rollbackAttempted = false;
const samples = [];
const startedMono = performance.now();

try {
  if (!values["allow-ownership-mutation"]) {
    throw new Error("refusing ownership mutation without explicit --allow-ownership-mutation");
  }
  session = await login();
  initial = await snapshot(session);
  reconciliation = initial.reconciliation;
  if (!reconciliationReady(reconciliation) && values["enqueue-reconciliation"]) {
    reconciliation = await enqueueReconciliation(session, samples, startedMono);
  }
  if (initial.ownership?.authoritative_writer !== "legacy") {
    throw new Error(`expected initial ${kind} ownership to be legacy`);
  }
  if (!initial.inventory_enabled || !initial.inventory_enabled_kinds.includes(kind)) {
    throw new Error(`Inventory is not enabled for ${kind}`);
  }
  if (!rootReady(initial.inventory)) throw new Error(`${kind} Inventory root is not ready`);
  if (expectedFiles != null
      && (initial.inventory?.present_files !== expectedFiles
        || initial.inventory?.last_discovered !== expectedFiles)) {
    throw new Error(`expected ${expectedFiles} files but Inventory reports present=${initial.inventory?.present_files} discovered=${initial.inventory?.last_discovered}`);
  }
  if (!reconciliationReady(reconciliation)) {
    throw new Error(`${kind} reconciliation is not a current passing evidence row`);
  }

  await ensureSearchPromotionEvidence(session, samples, startedMono);

  promotionResponse = await changeOwnership(session, "promote");
  promotionJobId = Number(promotionResponse.body?.scan_job_id);
  if (!Number.isInteger(promotionJobId) || promotionJobId < 1) {
    throw new Error("promotion response did not contain a scan job id");
  }
  // The ownership API checks the current legacy reconciliation inside the
  // promotion transaction. Once ownership is catalog-v2, the reconciliation
  // endpoint intentionally refuses to produce a new legacy comparison, so
  // promotion completion is fenced by the scan/root/owner state here while
  // the pre-promotion row remains the recorded evidence.
  promoted = await awaitState(session, (state, jobId) => promotionComplete(state, jobId), "promotion", promotionJobId, samples, startedMono);
  promoted.promotion_job_id = promotionJobId;

  rollbackAttempted = true;
  rollbackResponse = await changeOwnership(session, "rollback");
  rollbackJobId = Number(rollbackResponse.body?.scan_job_id);
  if (!Number.isInteger(rollbackJobId) || rollbackJobId < 1) {
    throw new Error("rollback response did not contain a scan job id");
  }
  rolledBack = await awaitState(session, (state, jobId) => rollbackComplete(state, jobId), "rollback", rollbackJobId, samples, startedMono);
  rolledBack.rollback_job_id = rollbackJobId;

  const checks = checksFor({
    initial,
    reconciliation,
    promoted,
    rolledBack,
    mutationConsent: true,
  });
  artifact = {
    artifact_version: PERF_ARTIFACT_VERSION,
    scenario: "inventory-promotion-rollback",
    status: checks.every((check) => check.status === "passed") ? "passed" : "failed",
    acceptance_role: "deployment-promotion-rollback-precheck",
    started_at: startedAt,
    completed_at: new Date().toISOString(),
    source: {
      ...captureSourceProvenance(repoRoot),
      platform: process.platform,
      arch: process.arch,
      node: process.version,
    },
    runtime: {
      base_url: `${baseUrl.protocol}//${baseUrl.host}`,
      kind,
      expected_files: expectedFiles,
      timeout_ms: timeoutMs,
      poll_ms: pollMs,
      elapsed_ms: Math.round(performance.now() - startedMono),
      reconciliation_enqueued: Boolean(values["enqueue-reconciliation"]),
      promotion_job_id: promotionJobId,
      rollback_job_id: rollbackJobId,
      rollback_attempted: rollbackAttempted,
    },
    simulation: {
      image: process.env.ARISLIST_SIM_IMAGE ?? null,
      container: process.env.SIM_CONTAINER_NAME ?? null,
      cpu_limit_cores: process.env.SIM_CPUS ? Number(process.env.SIM_CPUS) : null,
      memory_limit: process.env.SIM_MEMORY_LIMIT ?? null,
      memory_swap_limit: process.env.SIM_MEMORY_SWAP_LIMIT ?? null,
      pids_limit: process.env.SIM_PIDS_LIMIT ? Number(process.env.SIM_PIDS_LIMIT) : null,
    },
    before: compactSnapshot(initial),
    promotion: {
      response: promotionResponse.body,
      after: compactSnapshot(promoted),
    },
    rollback: {
      response: rollbackResponse.body,
      after: compactSnapshot(rolledBack),
    },
    samples: samples.slice(0, 5).concat(samples.length > 5 ? samples.slice(-5) : []),
    checks,
    limitations: [
      "This runner mutates Catalog ownership and must only target an explicitly approved disposable or deployment database.",
      "The runner validates mounted roots and application control-plane state; it does not prove physical NAS/HDD throughput.",
    ],
  };
  writeJsonArtifact(output, artifact);
  console.log(JSON.stringify({ output, status: artifact.status, kind, promotion_job_id: promotionJobId, rollback_job_id: rollbackJobId }));
  if (artifact.status !== "passed") process.exitCode = 1;
} catch (error) {
  const message = redactedError(error);
  // If promotion succeeded but the normal rollback path failed, make one
  // bounded best-effort rollback attempt. The failure artifact records that
  // the attempt happened, so an operator never mistakes it for a clean run.
  if (session && promotionJobId != null && !rollbackAttempted) {
    rollbackAttempted = true;
    try {
      rollbackResponse = await changeOwnership(session, "rollback");
      rollbackJobId = Number(rollbackResponse.body?.scan_job_id);
      if (Number.isInteger(rollbackJobId) && rollbackJobId > 0) {
        rolledBack = await awaitState(session, (state, jobId) => rollbackComplete(state, jobId), "rollback-after-error", rollbackJobId, samples, startedMono);
        rolledBack.rollback_job_id = rollbackJobId;
      }
    } catch (rollbackError) {
      samples.push({ phase: "rollback-after-error", error: redactedError(rollbackError) });
    }
  }
  artifact = {
    artifact_version: PERF_ARTIFACT_VERSION,
    scenario: "inventory-promotion-rollback",
    status: "failed",
    acceptance_role: "deployment-promotion-rollback-precheck",
    started_at: startedAt,
    completed_at: new Date().toISOString(),
    source: captureSourceProvenance(repoRoot),
    runtime: {
      base_url: `${baseUrl.protocol}//${baseUrl.host}`,
      kind,
      expected_files: expectedFiles,
      timeout_ms: timeoutMs,
      poll_ms: pollMs,
      elapsed_ms: Math.round(performance.now() - startedMono),
      promotion_job_id: promotionJobId,
      rollback_job_id: rollbackJobId,
      rollback_attempted: rollbackAttempted,
    },
    before: initial ? compactSnapshot(initial) : null,
    promotion: promotionResponse ? { response: promotionResponse.body, after: promoted ? compactSnapshot(promoted) : null } : null,
    rollback: rollbackResponse ? { response: rollbackResponse.body, after: rolledBack ? compactSnapshot(rolledBack) : null } : null,
    samples: samples.slice(0, 5).concat(samples.length > 5 ? samples.slice(-5) : []),
    checks: checksFor({
      initial,
      reconciliation,
      promoted,
      rolledBack,
      mutationConsent: Boolean(values["allow-ownership-mutation"]),
      error: message,
    }),
    error: message,
  };
  // The normal failure helper is intentionally not used here because this
  // artifact carries bounded before/after rollback evidence.
  try {
    writeJsonArtifact(output, artifact);
  } catch {
    // Preserve an earlier artifact if a late write failed; the process status
    // still reports the runner failure to the caller.
  }
  console.error(message);
  process.exitCode = 1;
}
