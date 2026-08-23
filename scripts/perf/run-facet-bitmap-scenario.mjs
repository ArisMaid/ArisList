import { createHash } from "node:crypto";
import { appendFileSync, existsSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";
import { performance } from "node:perf_hooks";
import { parseArgs } from "node:util";

import {
  evaluateFacetBitmapRuntimeEvidence,
  PERF_ARTIFACT_VERSION,
} from "./perf-lib.mjs";

const { values } = parseArgs({
  options: {
    "base-url": { type: "string" },
    "include-tag": { type: "string" },
    kind: { type: "string" },
    output: { type: "string", short: "o" },
    "runtime-output": { type: "string" },
    requests: { type: "string", short: "n", default: "30" },
    timeout: { type: "string", default: "30000" },
    "build-timeout": { type: "string", default: "120000" },
    help: { type: "boolean", short: "h" },
  },
  strict: true,
});

if (
  values.help ||
  !values["base-url"] ||
  !values["include-tag"] ||
  !values.kind ||
  !values.output ||
  !values["runtime-output"]
) {
  console.log(`Usage: node scripts/perf/run-facet-bitmap-scenario.mjs --base-url <url> --include-tag <namespace:key> --kind <kind> --output <scenario-results.jsonl> --runtime-output <facet-bitmap-runtime.json> [options]

Requires FACET_BITMAP_ENABLED=true and catalog-v2 ownership for the selected
kind. The runner triggers a background bitmap build, waits for ready, then
waits past the advertised response-cache TTL before every measured request.
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

const requests = boundedInteger("requests", values.requests, 1, 100);
const timeout = boundedInteger("timeout", values.timeout, 100, 10 * 60 * 1000);
const buildTimeout = boundedInteger(
  "build-timeout",
  values["build-timeout"],
  100,
  30 * 60 * 1000,
);
const baseUrl = new URL(values["base-url"]);
if (!new Set(["http:", "https:"]).has(baseUrl.protocol)) {
  throw new Error("--base-url must use http or https");
}
const output = resolve(values.output);
const runtimeOutput = resolve(values["runtime-output"]);
if (existsSync(output)) throw new Error(`scenario artifact already exists: ${output}`);
if (existsSync(runtimeOutput)) throw new Error(`runtime artifact already exists: ${runtimeOutput}`);
const headers = {};
if (process.env.PERF_COOKIE) headers.cookie = process.env.PERF_COOKIE;
if (process.env.PERF_AUTHORIZATION) headers.authorization = process.env.PERF_AUTHORIZATION;

const target = new URL("/api/catalog/facets/tags", baseUrl);
target.searchParams.set("include_tag", values["include-tag"]);
target.searchParams.set("kind", values.kind);
target.searchParams.set("limit", "100");
const warmupTarget = new URL(target);
warmupTarget.searchParams.set("limit", "199");

function sleep(milliseconds) {
  return new Promise((resolveSleep) => setTimeout(resolveSleep, milliseconds));
}

async function health() {
  const url = new URL("/api/health/resources", baseUrl);
  const response = await fetch(url, { headers, signal: AbortSignal.timeout(timeout) });
  if (!response.ok) throw new Error(`health request failed with HTTP ${response.status}`);
  const body = await response.json();
  if (!body.catalog_runtime?.facet_bitmap) {
    throw new Error("health response is missing catalog_runtime.facet_bitmap");
  }
  return {
    facetCacheTtlMillis: Number(body.catalog_runtime.facet_cache_ttl_millis),
    bitmap: body.catalog_runtime.facet_bitmap,
  };
}

async function fetchAndConsume(url) {
  const response = await fetch(url, { headers, signal: AbortSignal.timeout(timeout) });
  await response.arrayBuffer();
  if (!response.ok) throw new Error(`Facet request failed with HTTP ${response.status}`);
}

async function request(ordinal) {
  const startedAt = new Date().toISOString();
  const started = performance.now();
  let status = 0;
  let bytes = 0;
  let ttfb = null;
  let error = null;
  try {
    const response = await fetch(target, { headers, signal: AbortSignal.timeout(timeout) });
    ttfb = performance.now() - started;
    status = response.status;
    bytes = (await response.arrayBuffer()).byteLength;
  } catch (requestError) {
    error = requestError instanceof Error ? requestError.message : String(requestError);
  }
  return {
    artifact_version: PERF_ARTIFACT_VERSION,
    started_at: startedAt,
    scenario: "catalog-facets-bitmap-cold",
    target: target.pathname,
    warm: "cold",
    concurrency: 1,
    ordinal,
    status,
    ttfb_ms: ttfb,
    total_ms: performance.now() - started,
    bytes,
    queue_wait_ms: null,
    execution_ms: null,
    cache_state: "response-cache-expired",
    error,
  };
}

const startedAt = new Date().toISOString();
await fetchAndConsume(warmupTarget);
const buildDeadline = performance.now() + buildTimeout;
let ready = await health();
while (ready.bitmap.state !== "ready" && performance.now() < buildDeadline) {
  if (ready.bitmap.state === "failed") {
    throw new Error("Facet bitmap build entered failed state");
  }
  await sleep(100);
  ready = await health();
}
if (ready.bitmap.state !== "ready") {
  throw new Error(`Facet bitmap did not become ready within ${buildTimeout} ms`);
}
if (!Number.isFinite(ready.facetCacheTtlMillis) || ready.facetCacheTtlMillis < 0) {
  throw new Error("health response has an invalid Facet cache TTL");
}
const waitMillis = ready.facetCacheTtlMillis + 100;
const before = ready.bitmap;
const records = [];
for (let ordinal = 0; ordinal < requests; ordinal += 1) {
  await sleep(waitMillis);
  records.push(await request(ordinal));
}
const after = (await health()).bitmap;
for (const record of records) appendFileSync(output, `${JSON.stringify(record)}\n`, "utf8");

const evaluation = evaluateFacetBitmapRuntimeEvidence(before, after, requests);
const runtimeArtifact = {
  artifact_version: PERF_ARTIFACT_VERSION,
  status: evaluation.status,
  started_at: startedAt,
  completed_at: new Date().toISOString(),
  include_tag_sha256: createHash("sha256").update(values["include-tag"]).digest("hex"),
  kind: values.kind,
  requests,
  response_cache_wait_millis: waitMillis,
  before,
  after,
  ...evaluation,
};
writeFileSync(runtimeOutput, `${JSON.stringify(runtimeArtifact, null, 2)}\n`, {
  encoding: "utf8",
  flag: "wx",
});

const failedRequests = records.filter(
  (record) => record.error != null || record.status < 200 || record.status >= 400,
).length;
console.log(
  JSON.stringify({
    status: evaluation.status,
    records: records.length,
    failed_requests: failedRequests,
    output,
    runtime_output: runtimeOutput,
  }),
);
if (evaluation.status !== "passed" || failedRequests) process.exitCode = 1;
