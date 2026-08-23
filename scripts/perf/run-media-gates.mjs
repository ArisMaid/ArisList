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

const execFileAsync = promisify(execFile);

const { values } = parseArgs({
  options: {
    targets: { type: "string", short: "t" },
    output: { type: "string", short: "o" },
    gates: {
      type: "string",
      short: "g",
    },
    "base-url": { type: "string" },
    "node-bin": { type: "string", default: process.execPath },
    help: { type: "boolean", short: "h", default: false },
  },
  strict: true,
});

const repoRoot = resolve(fileURLToPath(new URL("../..", import.meta.url)));

if (values.help || !values.targets || !values.output) {
  console.log(`Usage: node scripts/perf/run-media-gates.mjs --targets <targets.json> --output <directory> [options]

Options:
  -t, --targets <path>       Matrix with explicit media URLs and request counts
  -o, --output <directory>   New artifact directory; non-empty directories are refused
  -g, --gates <path>         Declarative latency/error thresholds (defaults to repository media-n100-4g.json)
      --base-url <url>       Base URL for relative target and health URLs
      --node-bin <path>      Node executable used for child HTTP runners

Credentials are read only by the child runner from PERF_COOKIE or
PERF_AUTHORIZATION. URLs, query strings and credentials are not written to the
matrix artifact.`);
  process.exit(values.help ? 0 : 2);
}

function boundedInteger(name, value, minimum, maximum) {
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed < minimum || parsed > maximum) {
    throw new Error(`${name} must be an integer between ${minimum} and ${maximum}`);
  }
  return parsed;
}

function optionalNonNegativeInteger(name, value) {
  if (value == null) return null;
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed < 0) {
    throw new Error(`${name} must be a non-negative safe integer`);
  }
  return parsed;
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
    const abortAfterBytes = optionalNonNegativeInteger(
      `targets[${index}].abort_after_bytes`,
      target.abort_after_bytes,
    );
    const maxBodyBytes = optionalNonNegativeInteger(
      `targets[${index}].max_body_bytes`,
      target.max_body_bytes,
    );
    const base = target.base_url == null ? baseUrl : resolveHttpUrl(target.base_url, baseUrl, `targets[${index}].base_url`);
    const url = resolveHttpUrl(target.url, base, `targets[${index}].url`);
    const healthUrl = target.health_url == null
      ? null
      : resolveHttpUrl(target.health_url, base, `targets[${index}].health_url`);
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
      maxBodyBytes: maxBodyBytes ?? 0,
      healthUrl,
      requirePartial,
      url,
    };
  });
}

function childArgs(target, output, runtimeOutput) {
  const script = resolve(repoRoot, "scripts/perf/run-http-scenario.mjs");
  const args = [
    script,
    "--url",
    target.url.toString(),
    "--scenario",
    target.scenario,
    "--output",
    output,
    "--requests",
    String(target.requests),
    "--concurrency",
    String(target.concurrency),
    "--warm",
    target.warm,
    "--timeout",
    String(target.timeout),
  ];
  if (target.range) args.push("--range", target.range);
  if (target.abortAfterBytes != null) args.push("--abort-after-bytes", String(target.abortAfterBytes));
  if (target.maxBodyBytes > 0) args.push("--max-body-bytes", String(target.maxBodyBytes));
  if (target.healthUrl) args.push("--health-url", target.healthUrl.toString());
  if (runtimeOutput) args.push("--runtime-output", runtimeOutput);
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
    health_path: target.healthUrl?.pathname ?? null,
  };
}

async function runTarget(target, outputDir) {
  const rawOutput = resolve(outputDir, `${target.id}.jsonl`);
  const runtimeOutput = target.healthUrl ? resolve(outputDir, `${target.id}.runtime.json`) : null;
  const args = childArgs(target, rawOutput, runtimeOutput);
  let stdout = "";
  let stderr = "";
  let status = "failed";
  let exitCode = null;
  try {
    const result = await execFileAsync(values["node-bin"], args, {
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
  const records = existsSync(rawOutput) ? readJsonLines(rawOutput) : [];
  return {
    target: targetDescriptor(target),
    status: status === "passed" && exitCode === 0 ? "passed" : "failed",
    exit_code: exitCode,
    records,
    raw_output: `${target.id}.jsonl`,
    runtime_output: runtimeOutput ? `${target.id}.runtime.json` : null,
    stdout_bytes: Buffer.byteLength(stdout),
    stderr_bytes: Buffer.byteLength(stderr),
  };
}

const targetsPath = resolve(values.targets);
const outputDir = resolve(values.output);
const gatesPath = resolve(
  values.gates ?? resolve(repoRoot, "scripts/perf/gates/media-n100-4g.json"),
);
const baseUrl = values["base-url"]
  ? resolveHttpUrl(values["base-url"], undefined, "--base-url")
  : null;
if (existsSync(outputDir) && readdirSync(outputDir).length > 0) {
  throw new Error(`refusing to write into a non-empty output directory: ${outputDir}`);
}
mkdirSync(outputDir, { recursive: true });

const targetSpec = readObject(targetsPath, "targets");
const gateSpec = readObject(gatesPath, "gates");
const targets = normalizeTargets(targetSpec, baseUrl);
const runs = [];
for (const target of targets) {
  console.error(`[media-gates] running ${target.id}`);
  runs.push(await runTarget(target, outputDir));
}

const records = runs.flatMap((run) => run.records);
const resultsPath = resolve(outputDir, "scenario-results.jsonl");
writeFileSync(resultsPath, records.map((record) => JSON.stringify(record)).join("\n") + (records.length ? "\n" : ""), { flag: "wx" });
const summary = summarizeScenarioRecords(records);
summary.gate = evaluateScenarioGates(summary, gateSpec);
const failedRuns = runs.filter((run) => run.status !== "passed");
if (failedRuns.length) {
  summary.gate = {
    ...summary.gate,
    status: "failed",
    checks: [
      ...summary.gate.checks,
      {
        id: "target-runs",
        status: "failed",
        failures: failedRuns.map(
          (run) => `${run.target.id} exited with ${run.exit_code ?? "unknown error"}`,
        ),
      },
    ],
  };
}
writeFileSync(resolve(outputDir, "summary.json"), `${JSON.stringify(summary, null, 2)}\n`, { flag: "wx" });

const artifact = {
  artifact_version: PERF_ARTIFACT_VERSION,
  scenario: "media-preview-and-filter-matrix",
  generated_at: new Date().toISOString(),
  source: {
    targets_file: targetsPath,
    gates_file: gatesPath,
    node: process.version,
    platform: process.platform,
    arch: process.arch,
  },
  profile: targetSpec.profile ?? gateSpec.profile ?? "unnamed",
  scope: targetSpec.scope ?? gateSpec.scope ?? "media-preview-and-filter",
  targets: runs,
  records: records.length,
  summary: "summary.json",
  scenario_results: "scenario-results.jsonl",
  gate: summary.gate,
};
writeFileSync(resolve(outputDir, "matrix.json"), `${JSON.stringify(artifact, null, 2)}\n`, { flag: "wx" });
console.log(JSON.stringify({ output: outputDir, records: records.length, status: summary.gate.status }));
if (summary.gate.status !== "passed" || runs.some((run) => run.status !== "passed")) process.exitCode = 1;
