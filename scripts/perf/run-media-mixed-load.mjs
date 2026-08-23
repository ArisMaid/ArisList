import {
  existsSync,
  mkdirSync,
  readdirSync,
  readFileSync,
  writeFileSync,
} from "node:fs";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { resolve } from "node:path";
import { parseArgs } from "node:util";
import { fileURLToPath } from "node:url";

import {
  evaluateScenarioGates,
  PERF_ARTIFACT_VERSION,
  readJsonLines,
  summarizeScenarioRecords,
} from "./perf-lib.mjs";
import { redactedError } from "./runner-utils.mjs";

const execFileAsync = promisify(execFile);

const { values } = parseArgs({
  options: {
    targets: { type: "string", short: "t" },
    output: { type: "string", short: "o" },
    gates: { type: "string", short: "g" },
    "base-url": { type: "string" },
    "health-url": { type: "string" },
    clients: { type: "string" },
    rounds: { type: "string" },
    "duration-seconds": { type: "string" },
    "max-rounds": { type: "string", default: "1000" },
    "node-bin": { type: "string", default: process.execPath },
    help: { type: "boolean", short: "h" },
  },
  strict: true,
});

const repoRoot = resolve(fileURLToPath(new URL("../..", import.meta.url)));

if (values.help || !values.targets || !values.output) {
  console.log(`Usage: node scripts/perf/run-media-mixed-load.mjs --targets <targets.json> --output <directory> [options]

Options:
  -t, --targets <path>          Matrix with explicit media URLs and request counts
  -o, --output <directory>      New artifact directory; non-empty directories are refused
  -g, --gates <path>            Declarative latency/error thresholds
      --base-url <url>          Base URL for relative target URLs
      --health-url <url>        Optional /api/health/resources URL for before/after evidence
      --clients <count>         Logical clients sharing each target (default 2)
      --rounds <count>          Number of complete mixed rounds (default 1)
      --duration-seconds <n>    Repeat rounds until this duration; mutually exclusive with --rounds
      --max-rounds <count>     Safety cap for duration mode (default 1000)
      --node-bin <path>        Node executable used for child HTTP runners

Each round runs every target concurrently. A target's requests and concurrency are
multiplied by the client count, while merged records retain the target's original
concurrency for declarative Gate matching. Credentials are read only by child
runners from PERF_COOKIE or PERF_AUTHORIZATION and are never written to artifacts.`);
  process.exit(values.help ? 0 : 2);
}

function boundedInteger(name, raw, minimum, maximum) {
  const value = Number(raw);
  if (!Number.isSafeInteger(value) || value < minimum || value > maximum) {
    throw new TypeError(`${name} must be an integer between ${minimum} and ${maximum}`);
  }
  return value;
}

function resolveHttpUrl(raw, baseUrl, label) {
  if (typeof raw !== "string" || !raw.trim()) {
    throw new TypeError(`${label} must be a non-empty URL`);
  }
  const url = new URL(raw, baseUrl ?? undefined);
  if (!(url.protocol === "http:" || url.protocol === "https:")) {
    throw new TypeError(`${label} must use http or https`);
  }
  if (url.username || url.password) {
    throw new TypeError(`${label} must not contain URL credentials`);
  }
  return url;
}

function safeId(value, label) {
  if (typeof value !== "string" || !/^[A-Za-z0-9][A-Za-z0-9._-]*$/u.test(value)) {
    throw new TypeError(`${label} must contain only letters, numbers, dot, underscore or dash`);
  }
  return value;
}

function optionalString(value, label) {
  if (value == null) return null;
  if (typeof value !== "string" || !value.trim()) throw new TypeError(`${label} must be a string`);
  return value;
}

function requiredString(value, label) {
  const result = optionalString(value, label);
  if (result == null) throw new TypeError(`${label} is required`);
  return result;
}

function readObject(path, label) {
  let parsed;
  try {
    parsed = JSON.parse(readFileSync(path, "utf8"));
  } catch (error) {
    throw new Error(`${label} is not valid JSON: ${error.message}`);
  }
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
    throw new TypeError(`${label} must contain a JSON object`);
  }
  return parsed;
}

function normalizeTargets(spec, baseUrl) {
  if (!Array.isArray(spec.targets) || spec.targets.length === 0) {
    throw new TypeError("targets JSON must contain a non-empty targets array");
  }
  const ids = new Set();
  return spec.targets.map((target, index) => {
    if (!target || typeof target !== "object" || Array.isArray(target)) {
      throw new TypeError(`targets[${index}] must be an object`);
    }
    const id = safeId(target.id, `targets[${index}].id`);
    if (!ids.add(id)) throw new TypeError(`duplicate target id ${id}`);
    const scenario = requiredString(target.scenario, `targets[${index}].scenario`);
    const warm = target.warm ?? "warm";
    if (!new Set(["cold", "warm"]).has(warm)) {
      throw new TypeError(`targets[${index}].warm must be cold or warm`);
    }
    const requests = boundedInteger(`targets[${index}].requests`, target.requests ?? 30, 1, 100_000);
    const concurrency = boundedInteger(
      `targets[${index}].concurrency`,
      target.concurrency ?? 1,
      1,
      128,
    );
    const timeout = boundedInteger(
      `targets[${index}].timeout`,
      target.timeout ?? 30_000,
      100,
      10 * 60 * 1000,
    );
    const range = optionalString(target.range, `targets[${index}].range`);
    const abortAfterBytes = target.abort_after_bytes == null
      ? null
      : boundedInteger(`targets[${index}].abort_after_bytes`, target.abort_after_bytes, 0, 2 ** 31);
    const maxBodyBytes = target.max_body_bytes == null
      ? 0
      : boundedInteger(`targets[${index}].max_body_bytes`, target.max_body_bytes, 0, 2 ** 31);
    const base = target.base_url == null
      ? baseUrl
      : resolveHttpUrl(target.base_url, baseUrl, `targets[${index}].base_url`);
    const url = resolveHttpUrl(target.url, base, `targets[${index}].url`);
    const requirePartial = target.require_partial === true;
    if (requirePartial && !range) {
      throw new TypeError(`targets[${index}].require_partial requires range`);
    }
    return {
      id,
      scenario,
      warm,
      requests,
      concurrency,
      timeout,
      range,
      abortAfterBytes,
      maxBodyBytes,
      requirePartial,
      url,
    };
  });
}

function childArgs(target, output, clients) {
  const script = resolve(repoRoot, "scripts/perf/run-http-scenario.mjs");
  const requests = target.requests * clients;
  const concurrency = target.concurrency * clients;
  if (requests > 100_000) {
    throw new RangeError(`${target.id} requests multiplied by clients exceed 100000`);
  }
  if (concurrency > 128) {
    throw new RangeError(`${target.id} concurrency multiplied by clients exceeds 128`);
  }
  const args = [
    script,
    "--url",
    target.url.toString(),
    "--scenario",
    target.scenario,
    "--output",
    output,
    "--requests",
    String(requests),
    "--concurrency",
    String(concurrency),
    "--client-count",
    String(clients),
    "--warm",
    target.warm,
    "--timeout",
    String(target.timeout),
  ];
  if (target.range) args.push("--range", target.range);
  if (target.abortAfterBytes != null) args.push("--abort-after-bytes", String(target.abortAfterBytes));
  if (target.maxBodyBytes > 0) args.push("--max-body-bytes", String(target.maxBodyBytes));
  if (target.requirePartial) args.push("--require-partial");
  return args;
}

function targetDescriptor(target) {
  return {
    id: target.id,
    scenario: target.scenario,
    warm: target.warm,
    requests: target.requests,
    concurrency: target.concurrency,
    range: target.range,
    require_partial: target.requirePartial,
    target_path: target.url.pathname,
  };
}

async function readHealthSnapshot(healthUrl) {
  if (!healthUrl) return null;
  const response = await fetch(healthUrl, { signal: AbortSignal.timeout(5_000) });
  if (!response.ok) throw new Error(`health endpoint returned ${response.status}`);
  return response.json();
}

function relativeName(path, outputDir) {
  const prefix = `${outputDir.replaceAll("\\", "/")}/`;
  return path.replaceAll("\\", "/").startsWith(prefix)
    ? path.replaceAll("\\", "/").slice(prefix.length)
    : path;
}

async function runTarget(target, round, outputDir, clients, nodeBin) {
  const rawOutput = resolve(outputDir, `round-${String(round).padStart(4, "0")}-${target.id}.jsonl`);
  const args = childArgs(target, rawOutput, clients);
  let status = "failed";
  let exitCode = null;
  let stdout = "";
  let stderr = "";
  try {
    const result = await execFileAsync(nodeBin, args, {
      cwd: repoRoot,
      env: process.env,
      windowsHide: true,
      maxBuffer: 4 * 1024 * 1024,
    });
    stdout = result.stdout ?? "";
    stderr = result.stderr ?? "";
    status = "passed";
    exitCode = 0;
  } catch (error) {
    stdout = error.stdout ?? "";
    stderr = error.stderr ?? error.message ?? "";
    exitCode = Number.isInteger(error.code) ? error.code : null;
  }
  const childRecords = existsSync(rawOutput) ? readJsonLines(rawOutput) : [];
  const records = childRecords.map((record, ordinal) => ({
    ...record,
    target_id: target.id,
    round,
    client_id: record.client_id ?? ((ordinal % clients) + 1),
    client_count: clients,
    effective_concurrency: record.concurrency,
    concurrency: target.concurrency,
  }));
  return {
    target: targetDescriptor(target),
    round,
    clients,
    status: status === "passed" && exitCode === 0 ? "passed" : "failed",
    exit_code: exitCode,
    records,
    raw_output: relativeName(rawOutput, outputDir),
    stdout_bytes: Buffer.byteLength(stdout),
    stderr_bytes: Buffer.byteLength(stderr),
    error: status === "passed" ? null : redactedError(stderr),
  };
}

function coverageChecks(targets, records, completedRounds, clients) {
  return targets.map((target) => {
    const expected = target.requests * clients * completedRounds;
    const observed = records.filter((record) => record.target_id === target.id).length;
    const successful = records.filter(
      (record) => record.target_id === target.id && record.error == null && Number(record.status) >= 200 && Number(record.status) < 400,
    ).length;
    const failures = [];
    if (completedRounds < 1) failures.push("no complete mixed round finished");
    if (observed < expected) failures.push(`records=${observed} is below expected ${expected}`);
    if (successful < expected) failures.push(`succeeded=${successful} is below expected ${expected}`);
    return {
      id: `coverage:${target.id}`,
      status: failures.length ? "failed" : "passed",
      expected,
      observed,
      succeeded: successful,
      failures,
    };
  });
}

const targetsPath = resolve(values.targets);
const outputDir = resolve(values.output);
const gatesPath = resolve(values.gates ?? resolve(repoRoot, "scripts/perf/gates/media-n100-4g.json"));
const baseUrl = values["base-url"] ? resolveHttpUrl(values["base-url"], undefined, "--base-url") : null;
const healthUrl = values["health-url"] ? resolveHttpUrl(values["health-url"], undefined, "--health-url") : null;
const clients = boundedInteger("clients", values.clients ?? "2", 2, 16);
const rounds = values.rounds == null ? null : boundedInteger("rounds", values.rounds, 1, 10_000);
const durationSeconds = values["duration-seconds"] == null
  ? null
  : boundedInteger("duration-seconds", values["duration-seconds"], 1, 24 * 60 * 60);
const maxRounds = boundedInteger("max-rounds", values["max-rounds"], 1, 10_000);
if (rounds != null && durationSeconds != null) {
  throw new TypeError("--rounds and --duration-seconds are mutually exclusive");
}
if (existsSync(outputDir) && readdirSync(outputDir).length > 0) {
  throw new Error(`refusing to write into a non-empty output directory: ${outputDir}`);
}
mkdirSync(outputDir, { recursive: true });

const targetSpec = readObject(targetsPath, "targets");
const gateSpec = readObject(gatesPath, "gates");
const targets = normalizeTargets(targetSpec, baseUrl);
const healthBefore = await readHealthSnapshot(healthUrl);
const startedAt = new Date().toISOString();
const started = Date.now();
const runs = [];
let completedRounds = 0;

while (true) {
  const round = completedRounds + 1;
  const roundRuns = await Promise.all(
    targets.map((target) => runTarget(target, round, outputDir, clients, values["node-bin"])),
  );
  runs.push(...roundRuns);
  completedRounds = round;
  const failedRound = roundRuns.some((run) => run.status !== "passed");
  if (failedRound) break;
  if (rounds != null && completedRounds >= rounds) break;
  if (durationSeconds == null && rounds == null) break;
  if (completedRounds >= maxRounds) break;
  if (durationSeconds != null && Date.now() - started >= durationSeconds * 1000) break;
}

const healthAfter = await readHealthSnapshot(healthUrl);
const records = runs.flatMap((run) => run.records);
const resultsPath = resolve(outputDir, "scenario-results.jsonl");
writeFileSync(
  resultsPath,
  records.map((record) => JSON.stringify(record)).join("\n") + (records.length ? "\n" : ""),
  { flag: "wx" },
);
const summary = summarizeScenarioRecords(records);
const gate = evaluateScenarioGates(summary, gateSpec);
const coverage = coverageChecks(targets, records, completedRounds, clients);
const failedRuns = runs.filter((run) => run.status !== "passed");
summary.gate = {
  ...gate,
  status: gate.status === "passed" && coverage.every((check) => check.status === "passed") && failedRuns.length === 0
    ? "passed"
    : "failed",
  checks: [
    ...gate.checks,
    ...coverage,
    ...(failedRuns.length
      ? [{
          id: "target-runs",
          status: "failed",
          failures: failedRuns.map((run) => `${run.target.id} round ${run.round} exited with ${run.exit_code ?? "unknown error"}: ${run.error}`),
        }]
      : []),
  ],
};
writeFileSync(resolve(outputDir, "summary.json"), `${JSON.stringify(summary, null, 2)}\n`, { flag: "wx" });
if (healthUrl) {
  writeFileSync(
    resolve(outputDir, "health-runtime.json"),
    `${JSON.stringify({
      artifact_version: PERF_ARTIFACT_VERSION,
      captured_at: new Date().toISOString(),
      health_path: healthUrl.pathname,
      before: healthBefore,
      after: healthAfter,
    }, null, 2)}\n`,
    { flag: "wx" },
  );
}
const artifact = {
  artifact_version: PERF_ARTIFACT_VERSION,
  scenario: "media-preview-filter-mixed-load",
  generated_at: new Date().toISOString(),
  source: {
    targets_file: targetsPath,
    gates_file: gatesPath,
    node: process.version,
    platform: process.platform,
    arch: process.arch,
  },
  profile: targetSpec.profile ?? gateSpec.profile ?? "unnamed",
  scope: targetSpec.scope ?? gateSpec.scope ?? "media-preview-filter-mixed-load",
  mode: {
    clients,
    rounds_requested: rounds,
    duration_seconds: durationSeconds,
    max_rounds: durationSeconds == null ? null : maxRounds,
    rounds_completed: completedRounds,
    elapsed_ms: Date.now() - started,
    started_at: startedAt,
  },
  targets: targets.map(targetDescriptor),
  runs: runs.map(({ records: _records, ...run }) => run),
  records: records.length,
  summary: "summary.json",
  scenario_results: "scenario-results.jsonl",
  health_runtime: healthUrl ? "health-runtime.json" : null,
  gate: summary.gate,
};
writeFileSync(resolve(outputDir, "matrix.json"), `${JSON.stringify(artifact, null, 2)}\n`, { flag: "wx" });
console.log(JSON.stringify({
  output: outputDir,
  records: records.length,
  clients,
  rounds_completed: completedRounds,
  status: summary.gate.status,
}));
if (summary.gate.status !== "passed") process.exitCode = 1;
