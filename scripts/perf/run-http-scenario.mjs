import { appendFileSync, mkdirSync, writeFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { parseArgs } from "node:util";
import { performance } from "node:perf_hooks";

import { evaluateQmsRuntimeEvidence, PERF_ARTIFACT_VERSION } from "./perf-lib.mjs";

const { values } = parseArgs({
  options: {
    url: { type: "string", short: "u" },
    scenario: { type: "string", short: "s" },
    output: { type: "string", short: "o" },
    requests: { type: "string", short: "n", default: "30" },
    concurrency: { type: "string", short: "c", default: "1" },
    warm: { type: "string", default: "warm" },
    timeout: { type: "string", default: "30000" },
    range: { type: "string" },
    "abort-after-bytes": { type: "string" },
    "max-body-bytes": { type: "string", default: "0" },
    "client-count": { type: "string", default: "1" },
    "health-url": { type: "string" },
    "runtime-output": { type: "string" },
    "require-partial": { type: "boolean", default: false },
    help: { type: "boolean", short: "h" },
  },
  strict: true,
});

if (values.help || !values.url || !values.scenario || !values.output) {
  console.log(`Usage: node scripts/perf/run-http-scenario.mjs --url <url> --scenario <name> --output <scenario-results.jsonl> [options]

Options:
  -n, --requests <count>       Number of GET requests (default 30)
  -c, --concurrency <count>    Concurrent requests (default 1)
      --warm cold|warm         Cache state label
      --timeout <ms>           Per-request timeout (default 30000)
      --range <bytes=...>      Optional HTTP Range header for media startup tests
      --abort-after-bytes <n>  Abort the response after reading n body bytes
      --max-body-bytes <n>     Stop after n body bytes (0 means read the full body)
      --client-count <n>       Logical clients represented by this scenario (default 1)
      --health-url <url>       Optional /api/health/resources URL for qms counters
      --runtime-output <path>  Save before/after qms runtime evidence (requires health URL)
      --require-partial        Require 206 responses when --range is supplied

PERF_COOKIE and PERF_AUTHORIZATION may provide credentials. Their values are
used for requests but never written to the result artifact.`);
  process.exit(values.help ? 0 : 2);
}

function boundedInteger(name, raw, minimum, maximum) {
  const value = Number(raw);
  if (!Number.isInteger(value) || value < minimum || value > maximum) {
    throw new Error(`${name} must be an integer between ${minimum} and ${maximum}`);
  }
  return value;
}

const requests = boundedInteger("requests", values.requests, 1, 100_000);
const concurrency = boundedInteger("concurrency", values.concurrency, 1, 128);
const clientCount = boundedInteger("client-count", values["client-count"], 1, 128);
const timeout = boundedInteger("timeout", values.timeout, 100, 10 * 60 * 1000);
function nonNegativeInteger(name, raw) {
  const value = Number(raw);
  if (!Number.isSafeInteger(value) || value < 0) {
    throw new Error(`${name} must be a non-negative safe integer`);
  }
  return value;
}
const abortAfterBytes = values["abort-after-bytes"]
  ? nonNegativeInteger("abort-after-bytes", values["abort-after-bytes"])
  : null;
const maxBodyBytes = nonNegativeInteger("max-body-bytes", values["max-body-bytes"]);
if (!new Set(["cold", "warm"]).has(values.warm)) {
  throw new Error("--warm must be cold or warm");
}
const target = new URL(values.url);
if (!new Set(["http:", "https:"]).has(target.protocol)) {
  throw new Error("--url must use http or https");
}
const output = resolve(values.output);
mkdirSync(dirname(output), { recursive: true });
const runtimeOutput = values["runtime-output"] ? resolve(values["runtime-output"]) : null;
const healthUrl = values["health-url"] ? new URL(values["health-url"]) : null;
if (runtimeOutput && !healthUrl) {
  throw new Error("--runtime-output requires --health-url");
}
if (healthUrl && !new Set(["http:", "https:"]).has(healthUrl.protocol)) {
  throw new Error("--health-url must use http or https");
}
const headers = {};
if (process.env.PERF_COOKIE) headers.cookie = process.env.PERF_COOKIE;
if (process.env.PERF_AUTHORIZATION) headers.authorization = process.env.PERF_AUTHORIZATION;
if (values.range) headers.range = values.range;

async function readQmsHealth() {
  if (!healthUrl) return null;
  const response = await fetch(healthUrl, {
    signal: AbortSignal.timeout(Math.min(timeout, 5_000)),
  });
  if (!response.ok) throw new Error(`health endpoint returned ${response.status}`);
  const body = await response.json();
  return body?.qmediasync ?? null;
}

const qmsBefore = runtimeOutput ? await readQmsHealth() : null;

let next = 0;
const records = [];
async function worker() {
  while (true) {
    const ordinal = next++;
    if (ordinal >= requests) return;
    const startedAt = new Date().toISOString();
    const started = performance.now();
    let status = 0;
    let bytes = 0;
    let ttfb = null;
    let error = null;
    let bodyReadBytes = 0;
    let intentionallyAborted = false;
    let responseContentRange = null;
    let responseContentLength = null;
    let responseAcceptRanges = null;
    let responseEtag = null;
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), timeout);
    try {
      const response = await fetch(target, {
        method: "GET",
        headers,
        signal: controller.signal,
      });
      ttfb = performance.now() - started;
      status = response.status;
      responseContentRange = response.headers.get("content-range");
      responseContentLength = response.headers.get("content-length");
      responseAcceptRanges = response.headers.get("accept-ranges");
      responseEtag = response.headers.get("etag");
      if (values["require-partial"] && values.range && status !== 206) {
        throw new Error(`expected HTTP 206 for ranged request, got ${status}`);
      }
      if (values["require-partial"] && values.range && !responseContentRange) {
        throw new Error("expected Content-Range for ranged request");
      }
      const boundedRead = abortAfterBytes != null || maxBodyBytes > 0 || values.range;
      if (!boundedRead) {
        bytes = (await response.arrayBuffer()).byteLength;
        bodyReadBytes = bytes;
      } else if (response.body) {
        const reader = response.body.getReader();
        try {
          while (true) {
            const { done, value } = await reader.read();
            if (done) break;
            bodyReadBytes += value.byteLength;
            const reachedAbort = abortAfterBytes != null && bodyReadBytes >= abortAfterBytes;
            const reachedLimit = maxBodyBytes > 0 && bodyReadBytes >= maxBodyBytes;
            if (reachedAbort || reachedLimit) {
              intentionallyAborted = reachedAbort;
              await reader.cancel();
              controller.abort();
              break;
            }
          }
        } catch (streamError) {
          if (!intentionallyAborted) throw streamError;
        }
        bytes = bodyReadBytes;
      }
    } catch (requestError) {
      if (!intentionallyAborted) {
        error = requestError instanceof Error ? requestError.message : String(requestError);
      }
    } finally {
      clearTimeout(timeoutId);
    }
    const record = {
      artifact_version: PERF_ARTIFACT_VERSION,
      started_at: startedAt,
      scenario: values.scenario,
      target: target.pathname,
      warm: values.warm,
      concurrency,
      ordinal,
      status,
      ttfb_ms: ttfb,
      total_ms: performance.now() - started,
      bytes,
      body_read_bytes: bodyReadBytes,
      request_range: values.range ?? null,
      response_content_range: responseContentRange,
      response_content_length: responseContentLength,
      response_accept_ranges: responseAcceptRanges,
      response_etag: responseEtag,
      intentionally_aborted: intentionallyAborted,
      ...(clientCount > 1
        ? {
            client_id: (ordinal % clientCount) + 1,
            client_count: clientCount,
          }
        : {}),
      queue_wait_ms: null,
      execution_ms: null,
      cache_state: values.warm,
      error,
    };
    appendFileSync(output, `${JSON.stringify(record)}\n`, "utf8");
    records.push(record);
  }
}

await Promise.all(Array.from({ length: Math.min(concurrency, requests) }, () => worker()));
let runtimeEvidence = null;
if (runtimeOutput) {
  const qmsAfter = await readQmsHealth();
  runtimeEvidence = evaluateQmsRuntimeEvidence(qmsBefore, qmsAfter, {
    expectedRequests: requests,
    expectedRangeRequests: values.range ? requests : 0,
    requirePartial: values["require-partial"],
  });
  mkdirSync(dirname(runtimeOutput), { recursive: true });
  writeFileSync(
    runtimeOutput,
    `${JSON.stringify({
      artifact_version: PERF_ARTIFACT_VERSION,
      captured_at: new Date().toISOString(),
      scenario: values.scenario,
      expected_requests: requests,
      expected_range_requests: values.range ? requests : 0,
      require_partial: values["require-partial"],
      before: qmsBefore,
      after: qmsAfter,
      evidence: runtimeEvidence,
    }, null, 2)}\n`,
    { encoding: "utf8", flag: "wx" },
  );
}
const failed = records.filter(
  (record) => record.error != null || record.status < 200 || record.status >= 400,
).length;
console.log(JSON.stringify({ scenario: values.scenario, requests, concurrency, failed, output, runtime_output: runtimeOutput }));
if (failed || (runtimeEvidence != null && runtimeEvidence.status !== "passed")) {
  process.exitCode = 1;
}
