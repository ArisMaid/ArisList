import { createHash } from "node:crypto";
import { existsSync } from "node:fs";
import { resolve } from "node:path";
import { performance } from "node:perf_hooks";
import { parseArgs } from "node:util";

import {
  evaluateFacetCacheRuntimeEvidence,
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
    "include-tag": { type: "string" },
    kind: { type: "string" },
    output: { type: "string", short: "o" },
    "runtime-output": { type: "string" },
    requests: { type: "string", short: "n", default: "30" },
    concurrency: { type: "string", short: "c", default: "3" },
    timeout: { type: "string", default: "30000" },
    help: { type: "boolean", short: "h" },
  },
  strict: true,
});

if (
  values.help ||
  !values["base-url"] ||
  !values["include-tag"] ||
  !values.output ||
  !values["runtime-output"]
) {
  console.log(`Usage: node scripts/perf/run-facet-cache-scenario.mjs --base-url <url> --include-tag <namespace:key> --output <scenario-results.jsonl> --runtime-output <facet-runtime.json> [options]

Runs one concurrent cold request group to prove single-flight, followed by
sequential warm requests against the same revision-bound Facet key. Start from
a fresh server or wait longer than the advertised Facet TTL before invoking.
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
const concurrency = boundedInteger("concurrency", values.concurrency, 2, 16);
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
if (existsSync(output)) throw new Error(`scenario artifact already exists: ${output}`);
if (existsSync(runtimeOutput)) throw new Error(`runtime artifact already exists: ${runtimeOutput}`);
if (output === runtimeOutput) throw new Error("scenario and runtime artifacts must use different paths");
prepareRunnerOutputPaths(output, runtimeOutput);
const headers = {};
if (process.env.PERF_COOKIE) headers.cookie = process.env.PERF_COOKIE;
if (process.env.PERF_AUTHORIZATION) headers.authorization = process.env.PERF_AUTHORIZATION;

const target = new URL("/api/catalog/facets/tags", baseUrl);
target.searchParams.set("include_tag", values["include-tag"]);
target.searchParams.set("limit", "100");
if (values.kind) target.searchParams.set("kind", values.kind);

async function health() {
  const url = new URL("/api/health/resources", baseUrl);
  const response = await fetch(url, { headers, signal: AbortSignal.timeout(timeout) });
  if (!response.ok) throw new Error(`health request failed with HTTP ${response.status}`);
  const body = await response.json();
  if (!body.catalog_runtime) throw new Error("health response is missing catalog_runtime");
  return body.catalog_runtime;
}

async function request(scenario, ordinal, warm, activeConcurrency) {
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
    scenario,
    target: target.pathname,
    warm,
    concurrency: activeConcurrency,
    ordinal,
    status,
    ttfb_ms: ttfb,
    total_ms: performance.now() - started,
    bytes,
    queue_wait_ms: null,
    execution_ms: null,
    cache_state: warm,
    error,
  };
}

const startedAt = new Date().toISOString();
const includeTagSha256 = createHash("sha256").update(values["include-tag"]).digest("hex");
try {
  const before = await health();
  const concurrent = await Promise.all(
    Array.from({ length: concurrency }, (_, ordinal) =>
      request("catalog-facets-singleflight", ordinal, "cold", concurrency),
    ),
  );
  const afterConcurrent = await health();
  const warm = [];
  for (let ordinal = 0; ordinal < requests; ordinal += 1) {
    warm.push(await request("catalog-facets-cached", ordinal, "warm", 1));
  }
  const afterWarm = await health();
  const records = [...concurrent, ...warm];
  for (const record of records) appendJsonLine(output, record);

  const evaluation = evaluateFacetCacheRuntimeEvidence(
    before,
    afterConcurrent,
    afterWarm,
    concurrency,
    requests,
  );
  const runtimeArtifact = {
    artifact_version: PERF_ARTIFACT_VERSION,
    status: evaluation.status,
    started_at: startedAt,
    completed_at: new Date().toISOString(),
    include_tag_sha256: includeTagSha256,
    kind: values.kind ?? null,
    concurrency,
    warm_requests: requests,
    before,
    after_concurrent: afterConcurrent,
    after_warm: afterWarm,
    ...evaluation,
  };
  writeJsonArtifact(runtimeOutput, runtimeArtifact);

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
} catch (error) {
  const failure = writeRunnerFailureArtifact({
    runtimeOutput,
    scenario: "catalog-facets-cache",
    startedAt,
    metadata: {
      include_tag_sha256: includeTagSha256,
      kind: values.kind ?? null,
      concurrency,
      warm_requests: requests,
    },
    error,
  });
  console.error(JSON.stringify({ status: failure.status, error: failure.error, runtime_output: runtimeOutput }));
  process.exitCode = 1;
}
