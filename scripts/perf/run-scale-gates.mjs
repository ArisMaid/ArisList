import { createHash } from "node:crypto";
import { existsSync, mkdirSync, writeFileSync } from "node:fs";
import { dirname, relative, resolve } from "node:path";
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";
import { parseArgs } from "node:util";

import {
  evaluateLargeScaleGateRuns,
  parseLargeScaleGateOutput,
  PERF_ARTIFACT_VERSION,
} from "./perf-lib.mjs";
import { captureSourceProvenance } from "./provenance.mjs";

const { values } = parseArgs({
  options: {
    output: { type: "string", short: "o" },
    "skip-inventory": { type: "boolean", default: false },
    "skip-derivative": { type: "boolean", default: false },
    "derivative-rows": { type: "string", multiple: true },
    "cargo-bin": { type: "string", default: process.env.CARGO_BIN ?? "cargo" },
    help: { type: "boolean", short: "h", default: false },
  },
  strict: true,
});

if (values.help || !values.output) {
  console.log(
    "Usage: node scripts/perf/run-scale-gates.mjs --output <scale-gates.json> [--skip-inventory] [--skip-derivative] [--derivative-rows 700000]",
  );
  process.exit(values.help ? 0 : 2);
}

const repoRoot = resolve(fileURLToPath(new URL("../..", import.meta.url)));
const output = resolve(values.output);
const rawLog = `${output}.log`;
if (existsSync(output) || existsSync(rawLog)) {
  throw new Error(`refusing to overwrite existing scale-gate artifact or log: ${output}`);
}
if (values["skip-inventory"] && values["skip-derivative"]) {
  throw new Error("at least one scale gate must be selected");
}

const derivativeRows = (values["derivative-rows"]?.length
  ? values["derivative-rows"]
  : ["700000", "1400000"]
).map((value) => Number(value));
if (
  derivativeRows.some((value) => ![700_000, 1_400_000].includes(value)) ||
  new Set(derivativeRows).size !== derivativeRows.length
) {
  throw new Error("--derivative-rows may contain each of 700000 and 1400000 at most once");
}

function sha256(text) {
  return createHash("sha256").update(text).digest("hex");
}

function runCargo(id, args, env = {}) {
  return new Promise((resolveRun) => {
    const started = performance.now();
    const child = spawn(values["cargo-bin"], args, {
      cwd: repoRoot,
      env: { ...process.env, ...env },
      stdio: ["ignore", "pipe", "pipe"],
      windowsHide: true,
    });
    let stdout = "";
    let stderr = "";
    child.stdout.setEncoding("utf8");
    child.stderr.setEncoding("utf8");
    child.stdout.on("data", (chunk) => {
      stdout += chunk;
    });
    child.stderr.on("data", (chunk) => {
      stderr += chunk;
    });
    child.on("error", (error) => {
      resolveRun({
        id,
        command: [values["cargo-bin"], ...args],
        status: "failed",
        exit_code: null,
        signal: null,
        duration_ms: Math.round(performance.now() - started),
        stdout_bytes: Buffer.byteLength(stdout),
        stderr_bytes: Buffer.byteLength(stderr),
        stdout_sha256: sha256(stdout),
        stderr_sha256: sha256(`${stderr}\n${error.message}`),
        parsed: parseLargeScaleGateOutput(`${stdout}\n${stderr}`),
        error: error.message.slice(0, 512),
        raw: `${stdout}${stderr}`,
      });
    });
    child.on("close", (code, signal) => {
      const combined = `${stdout}\n${stderr}`;
      const parsed = parseLargeScaleGateOutput(combined);
      const expectedRows = Number(id.split("-")[1]);
      const hasExpectedOutput = id === "inventory-700000"
        ? parsed.inventory != null
        : parsed.derivatives.some((entry) => Number(entry.rows) === expectedRows);
      resolveRun({
        id,
        command: [values["cargo-bin"], ...args],
        status: code === 0 && hasExpectedOutput ? "passed" : code === 0 ? "incomplete" : "failed",
        exit_code: code,
        signal,
        duration_ms: Math.round(performance.now() - started),
        stdout_bytes: Buffer.byteLength(stdout),
        stderr_bytes: Buffer.byteLength(stderr),
        stdout_sha256: sha256(stdout),
        stderr_sha256: sha256(stderr),
        parsed,
        raw: combined,
      });
    });
  });
}

const runs = [];
if (!values["skip-inventory"]) {
  console.error("[scale-gates] running 700k Inventory fixed-batch gate");
  runs.push(await runCargo("inventory-700000", [
    "test",
    "-p",
    "media-shelf-server",
    "synthetic_700k_inventory_uses_fixed_batches",
    "--",
    "--ignored",
    "--nocapture",
  ]));
}
if (!values["skip-derivative"]) {
  for (const rows of derivativeRows) {
    console.error(`[scale-gates] running ${rows} Derivative ledger gate`);
    runs.push(await runCargo(`derivative-${rows}`, [
      "test",
      "-p",
      "media-shelf-server",
      "synthetic_derivative_ledger_scale_gate",
      "--",
      "--ignored",
      "--nocapture",
    ], { DERIVATIVE_LEDGER_GATE_ROWS: String(rows) }));
  }
}

const normalizedRuns = runs.map((run) => {
  const expectedRows = Number(run.id.split("-")[1]);
  return {
    ...run,
    parsed: {
      inventory: run.parsed.inventory,
      derivative: run.parsed.derivatives.find((entry) => Number(entry.rows) === expectedRows) ?? null,
      parse_errors: run.parsed.parse_errors,
    },
    raw: undefined,
  };
});
const gate = evaluateLargeScaleGateRuns(normalizedRuns, {
  requireInventory: !values["skip-inventory"],
  derivativeRows: values["skip-derivative"] ? [] : derivativeRows,
});
const raw = runs.map((run) => `=== ${run.id} ===\n${run.raw ?? ""}`).join("\n");
mkdirSync(dirname(output), { recursive: true });
writeFileSync(rawLog, raw, "utf8");
const artifact = {
  artifact_version: PERF_ARTIFACT_VERSION,
  scenario: "large-scale-inventory-derivative-gates",
  generated_at: new Date().toISOString(),
  source: {
    repo_root: repoRoot,
    ...captureSourceProvenance(repoRoot),
    platform: process.platform,
    arch: process.arch,
    node: process.version,
    cargo_bin: values["cargo-bin"],
  },
  raw_log: relative(dirname(output), rawLog).replaceAll("\\", "/"),
  runs: normalizedRuns,
  gate,
};
writeFileSync(output, `${JSON.stringify(artifact, null, 2)}\n`, "utf8");
console.log(JSON.stringify({ output, raw_log: rawLog, status: gate.status, checks: gate.checks }));
if (gate.status !== "passed") process.exitCode = 1;
