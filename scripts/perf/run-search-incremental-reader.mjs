import { createHash } from "node:crypto";
import { existsSync, readFileSync } from "node:fs";
import { resolve } from "node:path";
import { performance } from "node:perf_hooks";
import { parseArgs } from "node:util";

import {
  evaluateSearchIncrementalReaderEvidence,
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
  console.log(`Usage: node scripts/perf/run-search-incremental-reader.mjs --base-url <url> --corpus <queries.json> --output <scenario-results.jsonl> --runtime-output <runtime.json> [options]

The corpus must be a JSON array containing strings or {"query": "...",
"limit": 1..200} objects. The server must have the incremental reader flag
enabled and its persisted cutover gate armed. Query text and credentials are
never written to evidence artifacts.`);
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
const corpus = loadCorpus(resolve(values.corpus));
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
  if (!body.search_outbox || !body.search_shadow || !body.search_reconciliation) {
    throw new Error("health response is missing incremental reader sections");
  }
  return body;
}

async function request(entry, ordinal) {
  const target = new URL("/api/search", baseUrl);
  target.searchParams.set("q", entry.query);
  target.searchParams.set("limit", String(entry.limit));
  const startedAt = new Date().toISOString();
  const started = performance.now();
  let status = 0;
  let bytes = 0;
  let ttfb = null;
  let reader = null;
  let rebuilt = null;
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
      rebuilt = body.rebuilt ?? null;
    } else {
      error = `http-${response.status}`;
    }
  } catch (requestError) {
    error = requestError instanceof Error ? requestError.name : "request-failed";
  }
  return {
    artifact_version: PERF_ARTIFACT_VERSION,
    started_at: startedAt,
    scenario: "search-incremental-reader-fixed-corpus",
    target: target.pathname,
    warm: "incremental-reader",
    concurrency: 1,
    ordinal,
    query_sha256: createHash("sha256").update(entry.query).digest("hex"),
    query_bytes: entry.queryBytes,
    limit: entry.limit,
    status,
    ttfb_ms: ttfb,
    total_ms: performance.now() - started,
    bytes,
    reader,
    rebuilt,
    error,
  };
}

const startedAt = new Date().toISOString();
try {
  const before = await health();
  const records = [];
  for (let ordinal = 0; ordinal < corpus.length; ordinal += 1) {
    records.push(await request(corpus[ordinal], ordinal));
  }
  const after = await health();
  for (const record of records) appendJsonLine(output, record);

  const evaluation = evaluateSearchIncrementalReaderEvidence(before, after, records);
  const runtimeArtifact = {
    artifact_version: PERF_ARTIFACT_VERSION,
    status: evaluation.status,
    started_at: startedAt,
    completed_at: new Date().toISOString(),
    corpus_sha256: corpusSha256,
    corpus_queries: corpus.length,
    before: {
      search_outbox: before.search_outbox,
      search_shadow: before.search_shadow,
      search_reconciliation: before.search_reconciliation,
      search_features: before.search_features,
    },
    after: {
      search_outbox: after.search_outbox,
      search_shadow: after.search_shadow,
      search_reconciliation: after.search_reconciliation,
      search_features: after.search_features,
    },
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
    scenario: "search-incremental-reader-fixed-corpus",
    startedAt,
    metadata: { corpus_sha256: corpusSha256, corpus_queries: corpus.length },
    error,
  });
  console.error(JSON.stringify({ status: failure.status, error: failure.error, runtime_output: runtimeOutput }));
  process.exitCode = 1;
}
