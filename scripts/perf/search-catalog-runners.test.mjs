import { execFile } from "node:child_process";
import { existsSync, mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { createServer } from "node:http";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { promisify } from "node:util";
import { fileURLToPath } from "node:url";
import test from "node:test";
import assert from "node:assert/strict";

import { redactedError } from "./runner-utils.mjs";

const execFileAsync = promisify(execFile);
const repoRoot = fileURLToPath(new URL("../..", import.meta.url));

test("runner error redaction removes URL credentials and query parameters", () => {
  const message = redactedError(
    new Error("request failed at https://user:password@example.test/api/search?q=secret&token=abc"),
  );
  assert.equal(message, "request failed at https://example.test/api/search");
});

test("search shadow runner waits through a startup 503 before capturing stable evidence", async () => {
  let reconcileCalls = 0;
  let searchCalls = 0;
  const hash = "a".repeat(64);
  const server = createServer((request, response) => {
    if (request.url === "/api/health/resources") {
      const ready = reconcileCalls >= 2;
      response.writeHead(200, { "content-type": "application/json" });
      response.end(JSON.stringify({
        search_runtime: {
          canary_queries: searchCalls,
          canary_id_mismatches: 0,
          canary_order_mismatches: 0,
          canary_failures: 0,
          canary_rejections: 0,
        },
        search_shadow: {
          applied_revision: 1,
          baseline_revision: 1,
          indexed_documents: 1,
          ready,
          status: ready ? "ready" : "building",
          last_error: null,
        },
      }));
      return;
    }
    if (request.url === "/api/search/shadow/reconcile") {
      reconcileCalls += 1;
      if (reconcileCalls === 1) {
        response.writeHead(503, { "content-type": "application/json" });
        response.end(JSON.stringify({ error: "shadow baseline is still building" }));
        return;
      }
      response.writeHead(200, { "content-type": "application/json" });
      response.end(JSON.stringify({
        status: "passed",
        catalog_revision: 1,
        catalog_revision_after: 1,
        shadow_applied_revision: 1,
        shadow_applied_revision_after: 1,
        sqlite_work_count: 1,
        shadow_document_count: 1,
        shadow_unique_work_count: 1,
        missing_work_ids: 0,
        unexpected_work_ids: 0,
        duplicate_documents: 0,
        invalid_documents: 0,
        sqlite_ids_sha256: hash,
        shadow_ids_sha256: hash,
      }));
      return;
    }
    if (request.url?.startsWith("/api/search?")) {
      searchCalls += 1;
      response.writeHead(200, { "content-type": "application/json" });
      response.end(JSON.stringify({
        reader: "shadow",
        hits: [{ work_id: 1, score: 1, title: "fixture", kind: "novel" }],
        canary: {
          production_count: 1,
          shadow_count: 1,
          id_match: true,
          order_match: true,
        },
      }));
      return;
    }
    response.writeHead(404);
    response.end();
  });
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  const address = server.address();
  assert.ok(address && typeof address === "object");
  const temp = mkdtempSync(join(tmpdir(), "search-shadow-retry-"));
  const corpusPath = join(temp, "corpus.json");
  const output = join(temp, "nested", "scenario-results.jsonl");
  const runtimeOutput = join(temp, "nested", "runtime.json");
  writeFileSync(corpusPath, `${JSON.stringify(["startup-retry"])}\n`);
  try {
    await execFileAsync(
      process.execPath,
      [
        join(repoRoot, "scripts/perf/run-search-shadow-canary.mjs"),
        "--base-url",
        `http://127.0.0.1:${address.port}`,
        "--corpus",
        corpusPath,
        "--output",
        output,
        "--runtime-output",
        runtimeOutput,
      ],
      { cwd: temp, maxBuffer: 4 * 1024 * 1024, windowsHide: true },
    );
    const artifact = JSON.parse(readFileSync(runtimeOutput, "utf8"));
    assert.equal(artifact.status, "passed");
    assert.ok(reconcileCalls >= 3);
    assert.equal(searchCalls, 1);
  } finally {
    await new Promise((resolve) => server.close(resolve));
  }
});

test("search cutover probe requires a caught-up production reader after the unarmed 503", async () => {
  let healthCalls = 0;
  let armed = false;
  const server = createServer((request, response) => {
    if (request.url === "/api/health/resources") {
      healthCalls += 1;
      armed = healthCalls >= 2;
      const revision = 7;
      response.writeHead(200, { "content-type": "application/json" });
      response.end(JSON.stringify({
        search_outbox: {
          pending: 0,
          revision_lag: 0,
          search_revision_lag: 0,
          catalog_revision: revision,
          shadow_applied_revision: revision,
        },
        search_shadow: {
          ready: armed,
          status: armed ? "ready" : "building",
          applied_revision: revision,
        },
        search_reconciliation: {
          cutover_armed: armed,
          status: armed ? "passed" : "unknown",
          catalog_revision: revision,
          catalog_revision_after: revision,
          applied_revision: revision,
          applied_revision_after: revision,
          search_revision: revision,
          search_revision_after: revision,
        },
        search_features: {
          outbox_shadow: true,
          shadow_canary: true,
          incremental_reader: true,
        },
      }));
      return;
    }
    if (request.url?.startsWith("/api/search?")) {
      response.writeHead(armed ? 200 : 503, { "content-type": "application/json" });
      response.end(JSON.stringify(armed
        ? { reader: "production", rebuilt: false, hits: [] }
        : { error: "incremental reader is not armed" }));
      return;
    }
    response.writeHead(404);
    response.end();
  });
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  const address = server.address();
  assert.ok(address && typeof address === "object");
  const temp = mkdtempSync(join(tmpdir(), "search-cutover-probe-"));
  const output = join(temp, "nested", "cutover.json");
  try {
    await execFileAsync(
      process.execPath,
      [
        join(repoRoot, "scripts/perf/run-search-cutover-probe.mjs"),
        "--base-url",
        `http://127.0.0.1:${address.port}`,
        "--output",
        output,
        "--timeout",
        "2000",
        "--poll-millis",
        "10",
      ],
      { cwd: temp, maxBuffer: 4 * 1024 * 1024, windowsHide: true },
    );
    const artifact = JSON.parse(readFileSync(output, "utf8"));
    assert.equal(artifact.status, "passed");
    assert.equal(artifact.first_unarmed.status, 503);
    assert.equal(artifact.final_armed.reader, "production");
    assert.equal(artifact.final_armed.rebuilt, false);
    assert.equal(artifact.final_armed.outbox_revision_lag, 0);
  } finally {
    await new Promise((resolve) => server.close(resolve));
  }
});

test("search cutover probe rejects an armed response that bypasses production routing", async () => {
  let healthCalls = 0;
  const server = createServer((request, response) => {
    if (request.url === "/api/health/resources") {
      healthCalls += 1;
      const armed = healthCalls >= 2;
      const revision = 3;
      response.writeHead(200, { "content-type": "application/json" });
      response.end(JSON.stringify({
        search_outbox: {
          pending: 0,
          revision_lag: 0,
          search_revision_lag: 0,
          catalog_revision: revision,
          shadow_applied_revision: revision,
        },
        search_shadow: { ready: armed, status: armed ? "ready" : "building", applied_revision: revision },
        search_reconciliation: {
          cutover_armed: armed,
          status: armed ? "passed" : "unknown",
          catalog_revision: revision,
          catalog_revision_after: revision,
          applied_revision: revision,
          applied_revision_after: revision,
          search_revision: revision,
          search_revision_after: revision,
        },
      }));
      return;
    }
    if (request.url?.startsWith("/api/search?")) {
      const armed = healthCalls >= 2;
      response.writeHead(armed ? 200 : 503, { "content-type": "application/json" });
      response.end(JSON.stringify(armed
        ? { reader: "legacy", rebuilt: false, hits: [] }
        : { error: "incremental reader is not armed" }));
      return;
    }
    response.writeHead(404);
    response.end();
  });
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  const address = server.address();
  assert.ok(address && typeof address === "object");
  const temp = mkdtempSync(join(tmpdir(), "search-cutover-bypass-"));
  const output = join(temp, "nested", "cutover.json");
  try {
    await assert.rejects(
      execFileAsync(
        process.execPath,
        [
          join(repoRoot, "scripts/perf/run-search-cutover-probe.mjs"),
          "--base-url",
          `http://127.0.0.1:${address.port}`,
          "--output",
          output,
          "--timeout",
          "2000",
          "--poll-millis",
          "10",
        ],
        { cwd: temp, maxBuffer: 4 * 1024 * 1024, windowsHide: true },
      ),
    );
    const artifact = JSON.parse(readFileSync(output, "utf8"));
    assert.equal(artifact.status, "failed");
    assert.equal(artifact.final_armed.reader, "legacy");
  } finally {
    await new Promise((resolve) => server.close(resolve));
  }
});

test("R1G runtime artifact records the cold prime triplet separately", async () => {
  let healthCalls = 0;
  let targetCalls = 0;
  const runtime = (overrides = {}) => ({
    readers: 1,
    reader_opens: 1,
    candidate_entries: 0,
    candidate_ids: 0,
    candidate_hits: 0,
    candidate_misses: 0,
    candidate_coalesced: 0,
    candidate_evictions: 0,
    candidate_cache_max_entries: 16,
    candidate_cache_max_ids: 500000,
    ...overrides,
  });
  const server = createServer((request, response) => {
    if (request.url === "/api/health/resources") {
      healthCalls += 1;
      const snapshot = healthCalls === 1
        ? runtime()
        : healthCalls === 2
          ? runtime({ candidate_entries: 1, candidate_ids: 1, candidate_misses: 1, candidate_coalesced: 2 })
          : runtime({ candidate_entries: 1, candidate_ids: 1, candidate_misses: 1, candidate_coalesced: 2, candidate_hits: 3 });
      response.writeHead(200, { "content-type": "application/json" });
      response.end(JSON.stringify({ search_runtime: snapshot }));
      return;
    }
    targetCalls += 1;
    const delay = targetCalls <= 3 ? 25 : 0;
    setTimeout(() => {
      response.writeHead(200, { "content-type": "application/json" });
      response.end("{}");
    }, delay);
  });
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  const address = server.address();
  assert.ok(address && typeof address === "object");
  const temp = mkdtempSync(join(tmpdir(), "r1g-prime-"));
  const output = join(temp, "nested", "scenario.jsonl");
  const runtimeOutput = join(temp, "nested", "runtime.json");
  try {
    await execFileAsync(
      process.execPath,
      [
        join(repoRoot, "scripts/perf/run-r1g-scenario.mjs"),
        "--base-url",
        `http://127.0.0.1:${address.port}`,
        "--requests",
        "1",
        "--output",
        output,
        "--runtime-output",
        runtimeOutput,
      ],
      { cwd: temp, maxBuffer: 4 * 1024 * 1024, windowsHide: true },
    );
    const artifact = JSON.parse(readFileSync(runtimeOutput, "utf8"));
    assert.equal(artifact.status, "passed");
    assert.equal(artifact.prime_summary.requests, 3);
    assert.equal(artifact.prime_summary.failed, 0);
    assert.ok(artifact.prime_summary.total_ms.p95 >= 20);
  } finally {
    await new Promise((resolve) => server.close(resolve));
  }
});

test("Search and Catalog runners leave redacted failure runtime artifacts", async () => {
  const temp = mkdtempSync(join(tmpdir(), "search-catalog-runners-"));
  const corpusPath = join(temp, "corpus.json");
  writeFileSync(corpusPath, `${JSON.stringify(["failure-probe"])}\n`);
  const baseUrl = "http://127.0.0.1:1/unreachable?token=must-not-be-written";
  const cases = [
    {
      id: "search-shadow",
      script: "run-search-shadow-canary.mjs",
      extra: ["--corpus", corpusPath],
    },
    {
      id: "search-incremental",
      script: "run-search-incremental-reader.mjs",
      extra: ["--corpus", corpusPath],
    },
    {
      id: "r1g",
      script: "run-r1g-scenario.mjs",
      extra: ["--requests", "1"],
    },
    {
      id: "facet-cache",
      script: "run-facet-cache-scenario.mjs",
      extra: ["--include-tag", "namespace:failure-probe", "--requests", "1", "--concurrency", "2"],
    },
  ];

  for (const item of cases) {
    const output = join(temp, item.id, "nested", "scenario-results.jsonl");
    const runtimeOutput = join(temp, item.id, "nested", "runtime.json");
    await assert.rejects(
      execFileAsync(
        process.execPath,
        [
          join(repoRoot, "scripts/perf", item.script),
          "--base-url",
          baseUrl,
          "--output",
          output,
          "--runtime-output",
          runtimeOutput,
          ...item.extra,
        ],
        { cwd: temp, maxBuffer: 4 * 1024 * 1024, windowsHide: true },
      ),
    );
    assert.equal(existsSync(runtimeOutput), true, `${item.id} should write failure evidence`);
    const artifact = JSON.parse(readFileSync(runtimeOutput, "utf8"));
    assert.equal(artifact.status, "failed");
    assert.equal(artifact.checks[0].id, "runner-execution");
    assert.equal(artifact.error.includes("token=must-not-be-written"), false);
    assert.equal(existsSync(join(temp, item.id, "nested")), true);
  }
});
