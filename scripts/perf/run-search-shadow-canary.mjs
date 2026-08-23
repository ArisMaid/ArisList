import { createHash } from "node:crypto";
import { existsSync, readFileSync } from "node:fs";
import { resolve } from "node:path";
import { performance } from "node:perf_hooks";
import { parseArgs } from "node:util";

import {
  evaluateSearchShadowCanaryEvidence,
  PERF_ARTIFACT_VERSION,
} from "./perf-lib.mjs";
import {
  appendJsonLine,
  prepareRunnerOutputPaths,
  writeJsonArtifact,
  writeRunnerFailureArtifact,
} from "./runner-utils.mjs";

const MAX_QUERY_BYTES = 512;

const { values } = parseArgs({
  options: {
    "base-url": { type: "string" },
    corpus: { type: "string" },
    output: { type: "string", short: "o" },
    "runtime-output": { type: "string" },
    timeout: { type: "string", default: "30000" },
    help: { type: "boolean", short: "h" },
  },
  strict: true,
});

if (
  values.help ||
  !values["base-url"] ||
  !values.corpus ||
  !values.output ||
  !values["runtime-output"]
) {
  console.log(`Usage: node scripts/perf/run-search-shadow-canary.mjs --base-url <url> --corpus <queries.json> --output <scenario-results.jsonl> --runtime-output <search-canary-runtime.json> [options]

The corpus must be a JSON array containing strings or {"query": "...",
"limit": 1..200} objects. Both SEARCH_OUTBOX_SHADOW_ENABLED and
SEARCH_SHADOW_CANARY_ENABLED must be true, and the shadow index must already be
ready. Query text and credentials are never written to evidence artifacts.`);
  process.exit(values.help ? 0 : 2);
}

function boundedInteger(name, raw, minimum, maximum) {
  const value = Number(raw);
  if (!Number.isInteger(value) || value < minimum || value > maximum) {
    throw new Error(`${name} must be an integer between ${minimum} and ${maximum}`);
  }
  return value;
}

function loadCorpus(path) {
  const parsed = JSON.parse(readFileSync(path, "utf8"));
  if (!Array.isArray(parsed) || parsed.length < 1 || parsed.length > 256) {
    throw new Error("corpus must contain between 1 and 256 queries");
  }
  const seen = new Set();
  return parsed.map((entry, index) => {
    const query = (typeof entry === "string" ? entry : entry?.query)?.trim();
    const limit = boundedInteger(`corpus[${index}].limit`, entry?.limit ?? 200, 1, 200);
    if (!query) throw new Error(`corpus[${index}] query must not be empty`);
    const queryBytes = Buffer.byteLength(query, "utf8");
    if (queryBytes > MAX_QUERY_BYTES) {
      throw new Error(`corpus[${index}] query exceeds ${MAX_QUERY_BYTES} bytes`);
    }
    const key = JSON.stringify([query, limit]);
    if (seen.has(key)) throw new Error(`corpus[${index}] duplicates an earlier query and limit`);
    seen.add(key);
    return { query, limit, queryBytes };
  });
}

const timeout = boundedInteger("timeout", values.timeout, 100, 10 * 60 * 1000);
const baseUrl = new URL(values["base-url"]);
if (!new Set(["http:", "https:"]).has(baseUrl.protocol)) {
  throw new Error("--base-url must use http or https");
}
if (baseUrl.username || baseUrl.password) {
  throw new Error("--base-url must not contain URL credentials; use PERF_COOKIE or PERF_AUTHORIZATION");
}
const corpusPath = resolve(values.corpus);
const corpus = loadCorpus(corpusPath);
const corpusSha256 = createHash("sha256")
  .update(JSON.stringify(corpus.map(({ query, limit }) => ({ query, limit }))))
  .digest("hex");
const output = resolve(values.output);
const runtimeOutput = resolve(values["runtime-output"]);
if (existsSync(output)) throw new Error(`scenario artifact already exists: ${output}`);
if (existsSync(runtimeOutput)) throw new Error(`runtime artifact already exists: ${runtimeOutput}`);
if (output === runtimeOutput) throw new Error("scenario and runtime artifacts must use different paths");
prepareRunnerOutputPaths(output, runtimeOutput);

const headers = {};
if (process.env.PERF_COOKIE) headers.cookie = process.env.PERF_COOKIE;
if (process.env.PERF_AUTHORIZATION) headers.authorization = process.env.PERF_AUTHORIZATION;

async function health() {
  const url = new URL("/api/health/resources", baseUrl);
  const response = await fetch(url, { headers, signal: AbortSignal.timeout(timeout) });
  if (!response.ok) throw new Error(`health request failed with HTTP ${response.status}`);
  const body = await response.json();
  if (!body.search_runtime || !body.search_shadow) {
    throw new Error("health response is missing search_runtime or search_shadow");
  }
  return { runtime: body.search_runtime, shadow: body.search_shadow };
}

async function facts() {
  const url = new URL("/api/search/shadow/reconcile", baseUrl);
  const deadline = performance.now() + timeout;
  let lastStatus = null;
  while (performance.now() < deadline) {
    const remaining = Math.max(100, Math.floor(deadline - performance.now()));
    const response = await fetch(url, {
      headers,
      signal: AbortSignal.timeout(Math.min(timeout, remaining)),
    });
    if (response.ok) {
      const body = await response.json();
      for (const field of [
        "catalog_revision",
        "shadow_applied_revision",
        "sqlite_work_count",
        "shadow_document_count",
        "shadow_unique_work_count",
        "missing_work_ids",
        "unexpected_work_ids",
        "duplicate_documents",
        "invalid_documents",
      ]) {
        if (!Number.isFinite(Number(body[field]))) {
          throw new Error(`shadow fact response is missing numeric ${field}`);
        }
      }
      return body;
    }
    lastStatus = response.status;
    if (response.status !== 503) {
      throw new Error(`shadow fact reconciliation failed with HTTP ${response.status}`);
    }
    const delay = Math.min(250, Math.max(1, Math.floor(deadline - performance.now())));
    await new Promise((resolveDelay) => setTimeout(resolveDelay, delay));
  }
  throw new Error(
    `shadow fact reconciliation did not become ready within ${timeout}ms (last HTTP ${lastStatus ?? "timeout"})`,
  );
}

function shadowReady(snapshot) {
  return snapshot?.shadow?.ready === true && snapshot.shadow.status === "ready";
}

async function waitForShadowReady() {
  const deadline = performance.now() + timeout;
  let snapshot = await health();
  while (!shadowReady(snapshot)) {
    if (snapshot.shadow?.status === "degraded") {
      throw new Error(
        `shadow index became degraded before canary start: ${snapshot.shadow.last_error ?? "unknown error"}`,
      );
    }
    await facts();
    snapshot = await health();
    if (shadowReady(snapshot)) return snapshot;
    if (performance.now() >= deadline) {
      throw new Error(
        `shadow index did not become ready within ${timeout}ms (status=${snapshot.shadow?.status ?? "unknown"})`,
      );
    }
    const delay = Math.min(250, Math.max(1, Math.floor(deadline - performance.now())));
    await new Promise((resolveDelay) => setTimeout(resolveDelay, delay));
  }
  return snapshot;
}

async function request(entry, ordinal) {
  const target = new URL("/api/search", baseUrl);
  target.searchParams.set("q", entry.query);
  target.searchParams.set("limit", String(entry.limit));
  target.searchParams.set("reader", "shadow");
  const startedAt = new Date().toISOString();
  const started = performance.now();
  let status = 0;
  let bytes = 0;
  let ttfb = null;
  let reader = null;
  let canary = null;
  let error = null;
  try {
    const response = await fetch(target, { headers, signal: AbortSignal.timeout(timeout) });
    ttfb = performance.now() - started;
    status = response.status;
    const payload = Buffer.from(await response.arrayBuffer());
    bytes = payload.byteLength;
    if (response.ok) {
      const body = JSON.parse(payload.toString("utf8"));
      reader = body.reader ?? null;
      canary = body.canary
        ? {
            production_count: Number(body.canary.production_count),
            shadow_count: Number(body.canary.shadow_count),
            id_match: body.canary.id_match === true,
            order_match: body.canary.order_match === true,
          }
        : null;
    } else {
      error = `http-${response.status}`;
    }
  } catch (requestError) {
    error = requestError instanceof Error ? requestError.name : "request-failed";
  }
  return {
    artifact_version: PERF_ARTIFACT_VERSION,
    started_at: startedAt,
    scenario: "search-shadow-fixed-corpus",
    target: target.pathname,
    warm: "canary",
    concurrency: 1,
    ordinal,
    query_sha256: createHash("sha256").update(entry.query).digest("hex"),
    query_bytes: entry.queryBytes,
    limit: entry.limit,
    status,
    ttfb_ms: ttfb,
    total_ms: performance.now() - started,
    bytes,
    queue_wait_ms: null,
    execution_ms: null,
    cache_state: "shadow-canary",
    reader,
    canary,
    error,
  };
}

const startedAt = new Date().toISOString();
try {
  const before = await waitForShadowReady();
  const beforeFacts = await facts();
  const records = [];
  for (let ordinal = 0; ordinal < corpus.length; ordinal += 1) {
    records.push(await request(corpus[ordinal], ordinal));
  }
  const afterFacts = await facts();
  const after = await health();
  for (const record of records) appendJsonLine(output, record);

  const evaluation = evaluateSearchShadowCanaryEvidence(
    before.runtime,
    after.runtime,
    before.shadow,
    after.shadow,
    records,
    beforeFacts,
    afterFacts,
  );
  const runtimeArtifact = {
    artifact_version: PERF_ARTIFACT_VERSION,
    status: evaluation.status,
    started_at: startedAt,
    completed_at: new Date().toISOString(),
    corpus_sha256: corpusSha256,
    corpus_queries: corpus.length,
    before,
    after,
    before_facts: beforeFacts,
    after_facts: afterFacts,
    ...evaluation,
  };
  writeJsonArtifact(runtimeOutput, runtimeArtifact);

  const failedRequests = records.filter(
    (record) => record.error != null || record.status < 200 || record.status >= 400,
  ).length;
  console.log(
    JSON.stringify({
      status: evaluation.status,
      corpus_queries: corpus.length,
      failed_requests: failedRequests,
      output,
      runtime_output: runtimeOutput,
    }),
  );
  if (evaluation.status !== "passed" || failedRequests) process.exitCode = 1;
} catch (error) {
  const failure = writeRunnerFailureArtifact({
    runtimeOutput,
    scenario: "search-shadow-fixed-corpus",
    startedAt,
    metadata: { corpus_sha256: corpusSha256, corpus_queries: corpus.length },
    error,
  });
  console.error(JSON.stringify({ status: failure.status, error: failure.error, runtime_output: runtimeOutput }));
  process.exitCode = 1;
}
