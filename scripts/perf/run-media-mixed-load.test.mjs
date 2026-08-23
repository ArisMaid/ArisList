import { createServer } from "node:http";
import { execFile } from "node:child_process";
import { mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { promisify } from "node:util";
import { fileURLToPath } from "node:url";
import test from "node:test";
import assert from "node:assert/strict";

const execFileAsync = promisify(execFile);
const repoRoot = fileURLToPath(new URL("../..", import.meta.url));
const mixedRunner = join(repoRoot, "scripts/perf/run-media-mixed-load.mjs");
const httpRunner = join(repoRoot, "scripts/perf/run-http-scenario.mjs");

test("mixed media runner overlaps targets, preserves coverage, and redacts URLs", async () => {
  let active = 0;
  let maxActive = 0;
  const server = createServer((_request, response) => {
    active += 1;
    maxActive = Math.max(maxActive, active);
    setTimeout(() => {
      active -= 1;
      response.writeHead(200, { "content-type": "image/png", "content-length": "4" });
      response.end("test");
    }, 80);
  });
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  const address = server.address();
  assert.ok(address && typeof address === "object");

  const temp = mkdtempSync(join(tmpdir(), "media-mixed-load-"));
  const targetsPath = join(temp, "targets.json");
  const gatesPath = join(temp, "gates.json");
  const outputPath = join(temp, "output");
  const baseUrl = `http://127.0.0.1:${address.port}`;
  writeFileSync(
    targetsPath,
    `${JSON.stringify({
      profile: "smoke",
      scope: "mixed-smoke",
      targets: [
        {
          id: "target-a",
          scenario: "mixed-a",
          warm: "warm",
          url: `${baseUrl}/a?token=must-not-be-written`,
          requests: 2,
          concurrency: 1,
        },
        {
          id: "target-b",
          scenario: "mixed-b",
          warm: "cold",
          url: `${baseUrl}/b?secret=must-not-be-written`,
          requests: 2,
          concurrency: 1,
        },
      ],
    })}\n`,
  );
  writeFileSync(
    gatesPath,
    `${JSON.stringify({
      profile: "smoke",
      scenarios: [
        "mixed-a",
        "mixed-b",
      ].map((scenario) => ({
        id: scenario,
        scenario,
        warm: scenario === "mixed-a" ? "warm" : "cold",
        concurrency: 1,
        minimum: { requests: 1, succeeded: 1 },
        maximum: { failed: 0, status_503: 0, "total_ms.p95": 2_000 },
      })),
    })}\n`,
  );

  try {
    await execFileAsync(
      process.execPath,
      [
        mixedRunner,
        "--targets",
        targetsPath,
        "--output",
        outputPath,
        "--gates",
        gatesPath,
        "--clients",
        "2",
        "--rounds",
        "3",
      ],
      { cwd: temp, maxBuffer: 4 * 1024 * 1024, windowsHide: true },
    );
    const summary = JSON.parse(readFileSync(join(outputPath, "summary.json"), "utf8"));
    const matrixText = readFileSync(join(outputPath, "matrix.json"), "utf8");
    const matrix = JSON.parse(matrixText);
    const records = readFileSync(join(outputPath, "scenario-results.jsonl"), "utf8");
    assert.equal(summary.gate.status, "passed");
    assert.equal(summary.record_count, 24);
    assert.equal(matrix.mode.rounds_requested, 3);
    assert.equal(matrix.mode.rounds_completed, 3);
    assert.equal(summary.gate.checks.filter((check) => check.id.startsWith("coverage:")).length, 2);
    assert.equal(matrixText.includes("must-not-be-written"), false);
    assert.equal(records.includes("must-not-be-written"), false);
    assert.ok(maxActive >= 2, `expected target overlap, observed max active ${maxActive}`);
  } finally {
    await new Promise((resolve) => server.close(resolve));
  }
});

test("HTTP scenario fails a required Range target when the server returns 200", async () => {
  const server = createServer((_request, response) => {
    response.writeHead(200, { "content-type": "audio/mpeg", "content-length": "4" });
    response.end("test");
  });
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  const address = server.address();
  assert.ok(address && typeof address === "object");
  const temp = mkdtempSync(join(tmpdir(), "http-range-contract-"));
  const output = join(temp, "range.jsonl");
  try {
    await assert.rejects(
      execFileAsync(
        process.execPath,
        [
          httpRunner,
          "--url",
          `http://127.0.0.1:${address.port}/audio`,
          "--scenario",
          "range-contract",
          "--output",
          output,
          "--requests",
          "1",
          "--range",
          "bytes=0-3",
          "--max-body-bytes",
          "4",
          "--require-partial",
        ],
        { cwd: temp, maxBuffer: 4 * 1024 * 1024, windowsHide: true },
      ),
    );
    const record = JSON.parse(readFileSync(output, "utf8").trim());
    assert.match(record.error, /expected HTTP 206/u);
  } finally {
    await new Promise((resolve) => server.close(resolve));
  }
});
