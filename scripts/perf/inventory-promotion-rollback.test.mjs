import { execFile } from "node:child_process";
import { existsSync, mkdtempSync, readFileSync } from "node:fs";
import { createServer } from "node:http";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { promisify } from "node:util";
import test from "node:test";
import assert from "node:assert/strict";

const execFileAsync = promisify(execFile);
const repoRoot = new URL("../..", import.meta.url).pathname.replace(/^\/(\w:)/u, "$1");
const runner = join(repoRoot, "scripts/perf/run-inventory-promotion-rollback.mjs");

function fixtureServer({ reconciliationCurrent = true } = {}) {
  const state = {
    owner: "legacy",
    nextJob: 7,
    jobs: [],
    promotionCalls: 0,
    rollbackCalls: 0,
    jobPolls: new Map(),
  };
  const root = {
    kind: "novel",
    status: "idle",
    last_error: null,
    active_token: null,
    generation: 4,
    completed_generation: 4,
    present_files: 10,
    last_discovered: 10,
    missing_files: 0,
  };
  const reconciliation = {
    kind: "novel",
    status: reconciliationCurrent ? "passed" : "stale",
    current: reconciliationCurrent,
    expected_works: 1,
    matched_works: 1,
    missing_works: 0,
    unexpected_works: 0,
    mismatch_works: 0,
    error_works: 0,
  };
  const server = createServer((request, response) => {
    const url = new URL(request.url ?? "/", "http://127.0.0.1");
    const send = (status, body, headers = {}) => {
      response.writeHead(status, { "content-type": "application/json", ...headers });
      response.end(JSON.stringify(body));
    };
    if (url.pathname === "/api/auth/login" && request.method === "POST") {
      send(200, { authenticated: true, csrf: "fixture-csrf" }, { "set-cookie": "session=fixture; Path=/" });
      return;
    }
    if (url.pathname === "/api/health/resources" && request.method === "GET") {
      send(200, {
        status: "ok",
        jobs: { failed_jobs: 0, claim_errors: 0 },
        sqlite: { sqlite_busy_errors: 0, pool_acquire_timeouts: 0, pool_acquire_errors: 0 },
        resources: { resource_wait_timeouts: 0 },
        search_features: { outbox_shadow: true, shadow_canary: true, incremental_reader: false },
        search_outbox: { pending: 0, revision_lag: 0 },
        search_reconciliation: { status: "passed" },
      });
      return;
    }
    if (url.pathname === "/api/search/shadow/reconcile" && request.method === "GET") {
      send(200, {
        status: "passed",
        catalog_revision: 1,
        catalog_revision_after: 1,
        shadow_applied_revision: 1,
        shadow_applied_revision_after: 1,
        search_revision: 1,
        search_revision_after: 1,
        shadow_applied_search_revision: 1,
        shadow_applied_search_revision_after: 1,
        sqlite_work_count: 1,
        shadow_document_count: 1,
        shadow_unique_work_count: 1,
        missing_work_ids: 0,
        unexpected_work_ids: 0,
        duplicate_documents: 0,
        invalid_documents: 0,
      });
      return;
    }
    if (url.pathname === "/api/catalog/ownership" && request.method === "GET") {
      send(200, [{
        kind: "novel",
        authoritative_writer: state.owner,
        roots: 1,
        enabled_roots: 1,
        ready_roots: 1,
        pending_events: 0,
        failed_events: 0,
      }]);
      return;
    }
    if (url.pathname === "/api/inventory/status" && request.method === "GET") {
      send(200, {
        enabled: true,
        enabled_kinds: ["novel"],
        roots: [root],
      });
      return;
    }
    if (url.pathname === "/api/catalog/reconciliation" && request.method === "GET") {
      send(200, { items: [reconciliation], diffs: [], max_recorded_diffs_per_kind: 256 });
      return;
    }
    if (url.pathname === "/api/catalog/reconciliation/novel" && request.method === "POST") {
      const id = state.nextJob;
      state.nextJob += 1;
      state.jobs.push({ id, type: "reconcile-catalog-novel", status: "queued" });
      reconciliation.status = "passed";
      reconciliation.current = true;
      send(200, { created: true, job_id: id, status: "queued" });
      return;
    }
    if (url.pathname === "/api/jobs" && request.method === "GET") {
      for (const job of state.jobs) {
        const polls = (state.jobPolls.get(job.id) ?? 0) + 1;
        state.jobPolls.set(job.id, polls);
        if (polls >= 2 && job.status === "queued") job.status = "done";
      }
      send(200, state.jobs);
      return;
    }
    if (url.pathname === "/api/catalog/ownership/novel" && request.method === "POST") {
      let body = "";
      request.on("data", (chunk) => { body += chunk; });
      request.on("end", () => {
        const parsed = JSON.parse(body || "{}");
        const action = parsed.action;
        const target = action === "promote" ? "catalog-v2" : "legacy";
        state.owner = target;
        const id = state.nextJob;
        state.nextJob += 1;
        state.jobs.push({ id, type: "scan-library", status: "queued" });
        if (target === "catalog-v2") state.promotionCalls += 1;
        else state.rollbackCalls += 1;
        send(200, {
          status: "changed",
          change: { kind: "novel", authoritative_writer: target, previous_writer: target === "catalog-v2" ? "legacy" : "catalog-v2", changed: true },
          scan_job_id: id,
          scan_job_created: true,
        });
      });
      return;
    }
    send(404, { error: "not found" });
  });
  return { server, state };
}

async function listen(server) {
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  const address = server.address();
  assert.ok(address && typeof address === "object");
  return `http://127.0.0.1:${address.port}`;
}

test("Inventory promotion runner refuses ownership mutation without explicit consent", async () => {
  const { server, state } = fixtureServer();
  const baseUrl = await listen(server);
  const temp = mkdtempSync(join(tmpdir(), "inventory-promotion-consent-"));
  const output = join(temp, "nested", "artifact.json");
  try {
    await assert.rejects(execFileAsync(process.execPath, [
      runner,
      "--base-url", baseUrl,
      "--kind", "novel",
      "--output", output,
    ], {
      cwd: temp,
      env: { ...process.env, APP_ADMIN_PASSWORD: "fixture-password" },
      windowsHide: true,
      maxBuffer: 4 * 1024 * 1024,
    }));
    const artifact = JSON.parse(readFileSync(output, "utf8"));
    assert.equal(artifact.status, "failed");
    assert.equal(artifact.runtime.rollback_attempted, false);
    assert.equal(state.promotionCalls, 0);
    assert.equal(state.rollbackCalls, 0);
  } finally {
    await new Promise((resolve) => server.close(resolve));
  }
});

test("Inventory promotion runner completes a bounded promotion and rollback rehearsal", async () => {
  const { server, state } = fixtureServer({ reconciliationCurrent: false });
  const baseUrl = await listen(server);
  const temp = mkdtempSync(join(tmpdir(), "inventory-promotion-success-"));
  const output = join(temp, "nested", "artifact.json");
  try {
    await execFileAsync(process.execPath, [
      runner,
      "--base-url", baseUrl,
      "--kind", "novel",
      "--output", output,
      "--expected-files", "10",
      "--allow-ownership-mutation",
      "--enqueue-reconciliation",
      "--timeout-ms", "3000",
      "--poll-ms", "10",
    ], {
      cwd: temp,
      env: { ...process.env, APP_ADMIN_PASSWORD: "fixture-password" },
      windowsHide: true,
      maxBuffer: 4 * 1024 * 1024,
    });
    const artifact = JSON.parse(readFileSync(output, "utf8"));
    assert.equal(artifact.status, "passed");
    assert.equal(artifact.acceptance_role, "deployment-promotion-rollback-precheck");
    assert.equal(artifact.before.ownership.authoritative_writer, "legacy");
    assert.equal(artifact.promotion.after.ownership.authoritative_writer, "catalog-v2");
    assert.equal(artifact.rollback.after.ownership.authoritative_writer, "legacy");
    assert.equal(state.promotionCalls, 1);
    assert.equal(state.rollbackCalls, 1);
    assert.ok(artifact.samples.length <= 10);
  } finally {
    await new Promise((resolve) => server.close(resolve));
  }
});

test("Inventory promotion runner fails closed on stale reconciliation before mutation", async () => {
  const { server, state } = fixtureServer({ reconciliationCurrent: false });
  const baseUrl = await listen(server);
  const temp = mkdtempSync(join(tmpdir(), "inventory-promotion-stale-"));
  const output = join(temp, "artifact.json");
  try {
    await assert.rejects(execFileAsync(process.execPath, [
      runner,
      "--base-url", baseUrl,
      "--kind", "novel",
      "--output", output,
      "--allow-ownership-mutation",
      "--timeout-ms", "3000",
      "--poll-ms", "10",
    ], {
      cwd: temp,
      env: { ...process.env, APP_ADMIN_PASSWORD: "fixture-password" },
      windowsHide: true,
      maxBuffer: 4 * 1024 * 1024,
    }));
    const artifact = JSON.parse(readFileSync(output, "utf8"));
    assert.equal(artifact.status, "failed");
    assert.equal(artifact.checks.find((check) => check.id === "reconciliation-current-pass").status, "failed");
    assert.equal(state.promotionCalls, 0);
    assert.equal(state.rollbackCalls, 0);
    assert.equal(existsSync(output), true);
  } finally {
    await new Promise((resolve) => server.close(resolve));
  }
});
