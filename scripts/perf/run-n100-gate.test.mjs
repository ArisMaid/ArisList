import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { mkdtempSync, readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { promisify } from "node:util";
import { fileURLToPath } from "node:url";
import test from "node:test";
import { sourceProvenanceMatches } from "./n100-gate-provenance.mjs";

const execFileAsync = promisify(execFile);
const repoRoot = fileURLToPath(new URL("../..", import.meta.url));
const runner = join(repoRoot, "scripts", "perf", "run-n100-gate.mjs");

test("Gate provenance accepts identical flat source records", () => {
  const source = { commit: "abc", dirty: true, dirty_state_sha256: "hash" };
  assert.equal(sourceProvenanceMatches(source, { ...source }), true);
});

test("Gate provenance rejects a changed dirty tree or commit", () => {
  const source = { commit: "abc", dirty: true, dirty_state_sha256: "hash" };
  assert.equal(sourceProvenanceMatches(source, { ...source, dirty_state_sha256: "other" }), false);
  assert.equal(sourceProvenanceMatches(source, { ...source, commit: "def" }), false);
  assert.equal(sourceProvenanceMatches(source, { ...source, dirty: false }), false);
});

test("unified N100 runner refuses to start workload after failed preflight", async () => {
  const output = join(mkdtempSync(join(tmpdir(), "n100-gate-preflight-")), "run");
  await assert.rejects(
    execFileAsync(
      process.execPath,
      [
        runner,
        "--output",
        output,
        "--health-url",
        "http://127.0.0.1:1/api/health/resources",
      ],
      { cwd: repoRoot, maxBuffer: 4 * 1024 * 1024, windowsHide: true },
    ),
  );
  const run = JSON.parse(readFileSync(join(output, "run.json"), "utf8"));
  assert.equal(run.status, "failed");
  assert.equal(run.target_gate_status, "incomplete");
  assert.deepEqual(run.steps.map((step) => step.id), ["preflight"]);
  assert.equal(run.gate.reason.includes("workload steps were not started"), true);
});

test("unified N100 runner help documents approximation classification", async () => {
  const result = await execFileAsync(process.execPath, [runner, "--help"], {
    cwd: repoRoot,
    maxBuffer: 4 * 1024 * 1024,
    windowsHide: true,
  });
  assert.match(result.stdout, /target\|approximation/u);
  assert.match(result.stdout, /formal Docker simulation/u);
});

test("formal Docker mixed window rejects durations shorter than five minutes", async () => {
  const output = join(mkdtempSync(join(tmpdir(), "n100-gate-short-window-")), "run");
  await assert.rejects(
    execFileAsync(
      process.execPath,
      [
        runner,
        "--output",
        output,
        "--health-url",
        "http://127.0.0.1:1/api/health/resources",
        "--mixed-duration-seconds",
        "299",
      ],
      { cwd: repoRoot, maxBuffer: 4 * 1024 * 1024, windowsHide: true },
    ),
    /mixed-duration-seconds must be an integer between 300 and 300/u,
  );
});

test("formal Docker mixed window rejects durations longer than five minutes", async () => {
  const output = join(mkdtempSync(join(tmpdir(), "n100-gate-long-window-")), "run");
  await assert.rejects(
    execFileAsync(
      process.execPath,
      [
        runner,
        "--output",
        output,
        "--health-url",
        "http://127.0.0.1:1/api/health/resources",
        "--mixed-duration-seconds",
        "301",
      ],
      { cwd: repoRoot, maxBuffer: 4 * 1024 * 1024, windowsHide: true },
    ),
    /mixed-duration-seconds must be an integer between 300 and 300/u,
  );
});

test("formal runner rejects ambiguous rounds and duration settings", async () => {
  const output = join(mkdtempSync(join(tmpdir(), "n100-gate-mixed-mode-")), "run");
  await assert.rejects(
    execFileAsync(
      process.execPath,
      [
        runner,
        "--output",
        output,
        "--health-url",
        "http://127.0.0.1:1/api/health/resources",
        "--mixed-rounds",
        "2",
        "--mixed-duration-seconds",
        "300",
      ],
      { cwd: repoRoot, maxBuffer: 4 * 1024 * 1024, windowsHide: true },
    ),
    /--mixed-rounds and --mixed-duration-seconds are mutually exclusive/u,
  );
});
