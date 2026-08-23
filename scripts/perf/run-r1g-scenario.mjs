import { existsSync } from "node:fs";
import { resolve } from "node:path";
import { performance } from "node:perf_hooks";
import { parseArgs } from "node:util";

import {
  evaluateR1gRuntimeEvidence,
  percentile,
  PERF_ARTIFACT_VERSION,
} from "./perf-lib.mjs";
import {
  appendJsonLine,
  prepareRunnerOutputPaths,
  writeJsonArtifact,
  writeRunnerFailureArtifact,
} from "./runner-utils.mjs";

const { values } = parseArgs({
  options: {
    "base-url": { type: "string" },
    query: { type: "string", short: "q", default: "R1GCommon" },
    output: { type: "string", short: "o" },
    "runtime-output": { type: "string" },
    requests: { type: "string", short: "n", default: "30" },
    timeout: { type: "string", default: "30000" },
    help: { type: "boolean", short: "h" },
  },
  strict: true,
});

if (values.help || !values["base-url"] || !values.output || !values["runtime-output"]) {
  console.log(`Usage: node scripts/perf/run-r1g-scenario.mjs --base-url <url> --output <scenario-results.jsonl> --runtime-output <r1g-runtime.json> [options]

Runs works/counts/facets as one concurrent triplet. The first triplet is used
only for single-flight evidence; then --requests warm triplets are recorded.
Credentials may be supplied through PERF_COOKIE or PERF_AUTHORIZATION and are
never written to artifacts.`);
  process.exit(values.help ? 0 : 2);
}

function boundedInteger(name, raw, minimum, maximum) {
  const value = Number(raw);
  if (!Number.isInteger(value) || value < minimum || value > maximum) {
    throw new Error(`${name} must be an integer between ${minimum} and ${maximum}`);
  }
  return value;
}

const requests = boundedInteger("requests", values.requests, 1, 10_000);
const timeout = boundedInteger("timeout", values.timeout, 100, 10 * 60 * 1000);
const baseUrl = new URL(values["base-url"]);
if (!new Set(["http:", "https:"]).has(baseUrl.protocol)) {
  throw new Error("--base-url must use http or https");
}
if (baseUrl.username || baseUrl.password) {
  throw new Error("--base-url must not contain URL credentials; use PERF_COOKIE or PERF_AUTHORIZATION");
}
const output = resolve(values.output);
const runtimeOutput = resolve(values["runtime-output"]);
if (existsSync(runtimeOutput)) throw new Error(`runtime artifact already exists: ${runtimeOutput}`);
if (existsSync(output)) throw new Error(`scenario artifact already exists: ${output}`);
if (output === runtimeOutput) throw new Error("scenario and runtime artifacts must use different paths");
prepareRunnerOutputPaths(output, runtimeOutput);
const headers = {};
if (process.env.PERF_COOKIE) headers.cookie = process.env.PERF_COOKIE;
if (process.env.PERF_AUTHORIZATION) headers.authorization = process.env.PERF_AUTHORIZATION;

const targets = [
  {
    scenario: "r1g-catalog-works",
    path: "/api/catalog/works",
    parameters: { kind: "gallery", q: values.query, limit: "60" },
  },
  {
    scenario: "r1g-catalog-counts",
    path: "/api/catalog/counts",
    parameters: { q: values.query },
  },
  {
    scenario: "r1g-catalog-facets",
    path: "/api/catalog/facets/tags",
    parameters: { kind: "gallery", q: values.query, limit: "100" },
  },
];

function targetUrl(target) {
  const url = new URL(target.path, baseUrl);
  for (const [key, value] of Object.entries(target.parameters)) url.searchParams.set(key, value);
  return url;
}

async function health() {
  const url = new URL("/api/health/resources", baseUrl);
  const response = await fetch(url, { headers, signal: AbortSignal.timeout(timeout) });
  if (!response.ok) throw new Error(`health request failed with HTTP ${response.status}`);
  const body = await response.json();
  if (!body.search_runtime) throw new Error("health response is missing search_runtime");
  return body.search_runtime;
}

async function request(target, ordinal, record) {
  const url = targetUrl(target);
  const startedAt = new Date().toISOString();
  const started = performance.now();
  let status = 0;
  let bytes = 0;
  let ttfb = null;
  let error = null;
  try {
    const response = await fetch(url, { headers, signal: AbortSignal.timeout(timeout) });
    ttfb = performance.now() - started;
    status = response.status;
    bytes = (await response.arrayBuffer()).byteLength;
  } catch (requestError) {
    error = requestError instanceof Error ? requestError.message : String(requestError);
  }
  if (!record) {
    return {
      status,
      error,
      ttfb_ms: ttfb,
      total_ms: performance.now() - started,
      bytes,
    };
  }
  return {
    artifact_version: PERF_ARTIFACT_VERSION,
    started_at: startedAt,
    scenario: target.scenario,
    target: url.pathname,
    warm: "warm",
    concurrency: 3,
    ordinal,
    status,
    ttfb_ms: ttfb,
    total_ms: performance.now() - started,
    bytes,
    queue_wait_ms: null,
    execution_ms: null,
    cache_state: "warm",
    error,
  };
}

async function triplet(ordinal, record) {
  return Promise.all(targets.map((target) => request(target, ordinal, record)));
}

function primeMetricSummary(records, field) {
  const values = records
    .map((record) => record[field])
    .filter((value) => Number.isFinite(value))
    .map(Number);
  return {
    p50: percentile(values, 0.5),
    p95: percentile(values, 0.95),
    p99: percentile(values, 0.99),
    max: values.length ? Math.max(...values) : null,
  };
}

const startedAt = new Date().toISOString();
try {
  const before = await health();
  const prime = await triplet(-1, false);
  const primeFailed = prime.filter(
    (record) => record.error != null || record.status < 200 || record.status >= 400,
  ).length;
  const afterFirstTriplet = await health();
  const records = [];
  for (let ordinal = 0; ordinal < requests; ordinal += 1) {
    records.push(...(await triplet(ordinal, true)));
  }
  const afterAll = await health();
  for (const record of records) appendJsonLine(output, record);
  const evaluation = evaluateR1gRuntimeEvidence(before, afterFirstTriplet, afterAll);
  const runtimeArtifact = {
    artifact_version: PERF_ARTIFACT_VERSION,
    status: evaluation.status,
    started_at: startedAt,
    completed_at: new Date().toISOString(),
    query_bytes: Buffer.byteLength(values.query, "utf8"),
    prime_http: prime,
    prime_summary: {
      requests: prime.length,
      failed: primeFailed,
      ttfb_ms: primeMetricSummary(prime, "ttfb_ms"),
      total_ms: primeMetricSummary(prime, "total_ms"),
    },
    before,
    after_first_triplet: afterFirstTriplet,
    after_all: afterAll,
    ...evaluation,
  };
  writeJsonArtifact(runtimeOutput, runtimeArtifact);
  const failedRequests = records.filter(
    (record) => record.error != null || record.status < 200 || record.status >= 400,
  ).length;
  console.log(
    JSON.stringify({
      status: evaluation.status,
      triplets: requests,
      records: records.length,
      failed_requests: failedRequests,
      output,
      runtime_output: runtimeOutput,
    }),
  );
  if (evaluation.status !== "passed" || failedRequests) process.exitCode = 1;
} catch (error) {
  const failure = writeRunnerFailureArtifact({
    runtimeOutput,
    scenario: "r1g-catalog-runtime",
    startedAt,
    metadata: {
      query_bytes: Buffer.byteLength(values.query, "utf8"),
      triplets: requests,
    },
    error,
  });
  console.error(JSON.stringify({ status: failure.status, error: failure.error, runtime_output: runtimeOutput }));
  process.exitCode = 1;
}
