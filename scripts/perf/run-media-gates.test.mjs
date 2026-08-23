import { execFile } from "node:child_process";
import { mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { promisify } from "node:util";
import { fileURLToPath } from "node:url";
import { createServer } from "node:http";
import test from "node:test";
import assert from "node:assert/strict";

const execFileAsync = promisify(execFile);
const repoRoot = fileURLToPath(new URL("../..", import.meta.url));

test("media gate runner preserves redaction and evaluates a complete matrix", async () => {
  const server = createServer((_request, response) => {
    response.writeHead(200, { "content-type": "image/png", "content-length": "4" });
    response.end("test");
  });
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  const address = server.address();
  assert.ok(address && typeof address === "object");

  const temp = mkdtempSync(join(tmpdir(), "media-gates-"));
  const targetsPath = join(temp, "targets.json");
  const gatesPath = join(temp, "gates.json");
  const outputPath = join(temp, "output");
  const baseUrl = `http://127.0.0.1:${address.port}`;
  writeFileSync(
    targetsPath,
    `${JSON.stringify({
      profile: "smoke",
      targets: [{
        id: "smoke-thumbnail",
        scenario: "smoke-thumbnail",
        warm: "warm",
        url: `${baseUrl}/api/assets/1/thumb?secret=must-not-be-written`,
        requests: 3,
        concurrency: 1,
      }],
    })}\n`,
  );
  writeFileSync(
    gatesPath,
    `${JSON.stringify({
      profile: "smoke",
      scenarios: [{
        id: "smoke-thumbnail",
        scenario: "smoke-thumbnail",
        warm: "warm",
        concurrency: 1,
        minimum: { requests: 3, succeeded: 3 },
        maximum: { failed: 0, status_503: 0, "total_ms.p95": 2_000 },
      }],
    })}\n`,
  );

  try {
    await execFileAsync(
      process.execPath,
      [
        join(repoRoot, "scripts/perf/run-media-gates.mjs"),
        "--targets",
        targetsPath,
        "--output",
        outputPath,
        "--gates",
        gatesPath,
      ],
      { cwd: temp, maxBuffer: 4 * 1024 * 1024, windowsHide: true },
    );
    const summary = JSON.parse(readFileSync(join(outputPath, "summary.json"), "utf8"));
    const matrix = readFileSync(join(outputPath, "matrix.json"), "utf8");
    const records = readFileSync(join(outputPath, "scenario-results.jsonl"), "utf8");
    assert.equal(summary.gate.status, "passed");
    assert.equal(summary.record_count, 3);
    assert.equal(matrix.includes("must-not-be-written"), false);
    assert.equal(records.includes("must-not-be-written"), false);
  } finally {
    await new Promise((resolve) => server.close(resolve));
  }
});

test("media gate runner resolves its default gate profile outside the repository", async () => {
  const server = createServer((_request, response) => {
    response.writeHead(200, { "content-type": "image/png", "content-length": "4" });
    response.end("test");
  });
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  const address = server.address();
  assert.ok(address && typeof address === "object");

  const temp = mkdtempSync(join(tmpdir(), "media-gates-default-"));
  const targetsPath = join(temp, "targets.json");
  const outputPath = join(temp, "output");
  const baseUrl = `http://127.0.0.1:${address.port}`;
  const scenarios = [
    ["catalog-first-page", "catalog-first-page", "warm"],
    ["tag-filter", "tag-filter", "warm"],
    ["gallery-thumbnail-warm", "gallery-thumbnail", "warm"],
    ["gallery-thumbnail-cold", "gallery-thumbnail", "cold"],
    ["comic-page-cold", "comic-page", "cold"],
    ["coser-picture-page-cold", "coser-picture-page", "cold"],
    ["audio-startup-range", "audio-startup-range", "cold"],
    ["novel-summary-detail", "novel-summary-detail", "warm"],
  ];
  writeFileSync(
    targetsPath,
    `${JSON.stringify({
      profile: "nas-n100-4g",
      scope: "media-preview-filter-and-startup",
      targets: scenarios.map(([id, scenario, warm]) => ({
        id,
        scenario,
        warm,
        url: `${baseUrl}/health?token=must-not-be-written`,
        requests: 30,
        concurrency: 1,
      })),
    })}\n`,
  );

  try {
    await execFileAsync(
      process.execPath,
      [join(repoRoot, "scripts/perf/run-media-gates.mjs"), "--targets", targetsPath, "--output", outputPath],
      { cwd: temp, maxBuffer: 4 * 1024 * 1024, windowsHide: true },
    );
    const summary = JSON.parse(readFileSync(join(outputPath, "summary.json"), "utf8"));
    assert.equal(summary.gate.status, "passed");
    assert.equal(summary.record_count, 240);
  } finally {
    await new Promise((resolve) => server.close(resolve));
  }
});

test("media gate runner records child failures as a failed artifact gate", async () => {
  const temp = mkdtempSync(join(tmpdir(), "media-gates-failure-"));
  const targetsPath = join(temp, "targets.json");
  const gatesPath = join(temp, "gates.json");
  const outputPath = join(temp, "output");
  writeFileSync(
    targetsPath,
    `${JSON.stringify({
      targets: [{
        id: "unreachable",
        scenario: "smoke-thumbnail",
        warm: "warm",
        url: "http://127.0.0.1:1/unreachable",
        requests: 1,
        concurrency: 1,
      }],
    })}\n`,
  );
  writeFileSync(
    gatesPath,
    `${JSON.stringify({
      scenarios: [{
        id: "unreachable",
        scenario: "smoke-thumbnail",
        warm: "warm",
        concurrency: 1,
        minimum: { requests: 1 },
        maximum: { failed: 0 },
      }],
    })}\n`,
  );

  await assert.rejects(
    execFileAsync(
      process.execPath,
      [
        join(repoRoot, "scripts/perf/run-media-gates.mjs"),
        "--targets",
        targetsPath,
        "--output",
        outputPath,
        "--gates",
        gatesPath,
      ],
      { cwd: temp, maxBuffer: 4 * 1024 * 1024, windowsHide: true },
    ),
  );
  const summary = JSON.parse(readFileSync(join(outputPath, "summary.json"), "utf8"));
  assert.equal(summary.gate.status, "failed");
  assert.equal(summary.gate.checks.at(-1).id, "target-runs");
});
