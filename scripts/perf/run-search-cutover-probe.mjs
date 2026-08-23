import { existsSync, mkdirSync, writeFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { performance } from "node:perf_hooks";
import { parseArgs } from "node:util";
import { createHash } from "node:crypto";

const { values } = parseArgs({
  options: {
    "base-url": { type: "string" },
    output: { type: "string", short: "o" },
    timeout: { type: "string", default: "30000" },
    "poll-millis": { type: "string", default: "100" },
    help: { type: "boolean", short: "h", default: false },
  },
  strict: true,
});

if (values.help || !values["base-url"] || !values.output) {
  console.log(`Usage: node scripts/perf/run-search-cutover-probe.mjs --base-url <url> --output <artifact.json> [options]

The probe records only status, timing, and health gate state. Query text and
credentials are never written to the artifact. It expects an initially
unarmed incremental reader to return a bounded 503, then a 200 after arm.`);
  process.exit(values.help ? 0 : 2);
}

function integerOption(name, raw, minimum, maximum) {
  const value = Number(raw);
  if (!Number.isInteger(value) || value < minimum || value > maximum) {
    throw new Error(`${name} must be an integer between ${minimum} and ${maximum}`);
  }
  return value;
}

const timeoutMillis = integerOption("timeout", values.timeout, 100, 10 * 60 * 1000);
const pollMillis = integerOption("poll-millis", values["poll-millis"], 1, 10_000);
const baseUrl = new URL(values["base-url"]);
if (!new Set(["http:", "https:"]).has(baseUrl.protocol)) {
  throw new Error("--base-url must use http or https");
}
if (baseUrl.username || baseUrl.password) {
  throw new Error("--base-url must not contain URL credentials");
}

const output = resolve(values.output);
if (existsSync(output)) throw new Error(`refusing to overwrite existing artifact: ${output}`);
mkdirSync(dirname(output), { recursive: true });

const query = "author:perf";
const querySha256 = createHash("sha256").update(query).digest("hex");
const headers = {};
if (process.env.PERF_COOKIE) headers.cookie = process.env.PERF_COOKIE;
if (process.env.PERF_AUTHORIZATION) headers.authorization = process.env.PERF_AUTHORIZATION;

async function fetchHealth() {
  const response = await fetch(new URL("/api/health/resources", baseUrl), {
    headers,
    signal: AbortSignal.timeout(Math.min(timeoutMillis, 5_000)),
  });
  if (!response.ok) throw new Error(`health request failed with HTTP ${response.status}`);
  const body = await response.json();
  const outbox = body.search_outbox ?? {};
  const shadow = body.search_shadow ?? {};
  const reconciliation = body.search_reconciliation ?? {};
  return {
    armed: reconciliation.cutover_armed === true,
    shadowReady: shadow.ready === true,
    shadowStatus: shadow.status ?? null,
    shadowAppliedRevision: shadow.applied_revision ?? null,
    outboxPending: outbox.pending ?? null,
    outboxRevisionLag: outbox.revision_lag ?? null,
    outboxSearchRevisionLag: outbox.search_revision_lag ?? null,
    outboxCatalogRevision: outbox.catalog_revision ?? null,
    outboxShadowAppliedRevision: outbox.shadow_applied_revision ?? null,
    reconciliationStatus: reconciliation.status ?? null,
    reconciliationCatalogRevision: reconciliation.catalog_revision ?? null,
    reconciliationCatalogRevisionAfter: reconciliation.catalog_revision_after ?? null,
    reconciliationAppliedRevision: reconciliation.applied_revision ?? null,
    reconciliationAppliedRevisionAfter: reconciliation.applied_revision_after ?? null,
    reconciliationSearchRevision: reconciliation.search_revision ?? null,
    reconciliationSearchRevisionAfter: reconciliation.search_revision_after ?? null,
    searchFeatures: body.search_features ?? null,
  };
}

async function fetchSearch() {
  const target = new URL("/api/search", baseUrl);
  target.searchParams.set("q", query);
  target.searchParams.set("limit", "50");
  const started = performance.now();
  try {
    const response = await fetch(target, {
      headers,
      signal: AbortSignal.timeout(Math.min(timeoutMillis, 15_000)),
    });
    let reader = null;
    let rebuilt = null;
    if (response.ok) {
      const body = await response.json();
      reader = body?.reader ?? null;
      rebuilt = body?.rebuilt ?? null;
    }
    return {
      status: response.status,
      latencyMs: performance.now() - started,
      reader,
      rebuilt,
      error: null,
    };
  } catch (error) {
    return {
      status: null,
      latencyMs: performance.now() - started,
      reader: null,
      rebuilt: null,
      error: error instanceof Error ? error.name : "request-error",
    };
  }
}

function percentile(valuesToRank, percentile) {
  if (!valuesToRank.length) return null;
  const sorted = [...valuesToRank].sort((a, b) => a - b);
  const index = Math.min(sorted.length - 1, Math.ceil(percentile * sorted.length) - 1);
  return sorted[index];
}

const startedAt = new Date().toISOString();
const deadline = performance.now() + timeoutMillis;
const rows = [];
let firstUnarmed = null;
let finalArmed = null;
let healthFailures = 0;

while (performance.now() < deadline) {
  let health;
  try {
    health = await fetchHealth();
  } catch {
    healthFailures += 1;
    await new Promise((resolveDelay) => setTimeout(resolveDelay, pollMillis));
    continue;
  }
  const search = await fetchSearch();
  const row = {
    status: search.status,
    latency_ms: Number(search.latencyMs.toFixed(3)),
    armed: health.armed,
    shadow_ready: health.shadowReady,
    shadow_status: health.shadowStatus,
    shadow_applied_revision: health.shadowAppliedRevision,
    outbox_pending: health.outboxPending,
    outbox_revision_lag: health.outboxRevisionLag,
    outbox_search_revision_lag: health.outboxSearchRevisionLag,
    outbox_catalog_revision: health.outboxCatalogRevision,
    outbox_shadow_applied_revision: health.outboxShadowAppliedRevision,
    reconciliation_status: health.reconciliationStatus,
    reconciliation_catalog_revision: health.reconciliationCatalogRevision,
    reconciliation_catalog_revision_after: health.reconciliationCatalogRevisionAfter,
    reconciliation_applied_revision: health.reconciliationAppliedRevision,
    reconciliation_applied_revision_after: health.reconciliationAppliedRevisionAfter,
    reconciliation_search_revision: health.reconciliationSearchRevision,
    reconciliation_search_revision_after: health.reconciliationSearchRevisionAfter,
    reader: search.reader,
    rebuilt: search.rebuilt,
    error: search.error,
  };
  rows.push(row);
  if (!health.armed && firstUnarmed === null) firstUnarmed = row;
  if (health.armed) {
    finalArmed = row;
    break;
  }
  await new Promise((resolveDelay) => setTimeout(resolveDelay, pollMillis));
}

const unarmedRows = rows.filter((row) => row.armed === false);
const unarmedLatencies = unarmedRows
  .map((row) => row.latency_ms)
  .filter((value) => Number.isFinite(value));
const passed =
  firstUnarmed?.status === 503 &&
  firstUnarmed.latency_ms <= timeoutMillis &&
  finalArmed?.status === 200 &&
  finalArmed.latency_ms <= timeoutMillis &&
  finalArmed.reader === "production" &&
  finalArmed.rebuilt === false &&
  finalArmed.shadow_ready === true &&
  finalArmed.shadow_status === "ready" &&
  finalArmed.reconciliation_status === "passed" &&
  finalArmed.armed === true &&
  finalArmed.outbox_pending === 0 &&
  finalArmed.outbox_revision_lag === 0 &&
  finalArmed.outbox_search_revision_lag === 0 &&
  finalArmed.outbox_catalog_revision === finalArmed.outbox_shadow_applied_revision &&
  finalArmed.reconciliation_catalog_revision === finalArmed.reconciliation_catalog_revision_after &&
  finalArmed.reconciliation_applied_revision === finalArmed.reconciliation_applied_revision_after &&
  finalArmed.reconciliation_catalog_revision === finalArmed.outbox_catalog_revision;
const artifact = {
  artifact_version: 1,
  scenario: "search-cutover-unarmed-probe",
  approximation: true,
  status: passed ? "passed" : "failed",
  started_at: startedAt,
  completed_at: new Date().toISOString(),
  base_url: `${baseUrl.protocol}//${baseUrl.host}`,
  query_sha256: querySha256,
  timeout_millis: timeoutMillis,
  poll_millis: pollMillis,
  health_failures: healthFailures,
  attempts: rows.length,
  first_unarmed: firstUnarmed,
  final_armed: finalArmed,
  unarmed_summary: {
    samples: unarmedRows.length,
    status_503: unarmedRows.filter((row) => row.status === 503).length,
    latency_p50_ms: percentile(unarmedLatencies, 0.5),
    latency_p95_ms: percentile(unarmedLatencies, 0.95),
    latency_max_ms: unarmedLatencies.length ? Math.max(...unarmedLatencies) : null,
  },
  sample_rows: rows.slice(0, 5).concat(rows.length > 5 ? rows.slice(-5) : []),
};
writeFileSync(output, `${JSON.stringify(artifact, null, 2)}\n`, { encoding: "utf8", flag: "wx" });
console.log(JSON.stringify({ output, status: artifact.status, attempts: artifact.attempts }));
if (artifact.status !== "passed") process.exitCode = 1;
