import {
  existsSync,
  mkdirSync,
  readdirSync,
  readFileSync,
  writeFileSync,
} from "node:fs";
import { createHash } from "node:crypto";
import { spawn } from "node:child_process";
import { relative, resolve } from "node:path";
import { parseArgs } from "node:util";
import { fileURLToPath } from "node:url";

import { PERF_ARTIFACT_VERSION } from "./perf-lib.mjs";
import { captureSourceProvenance } from "./provenance.mjs";
import { sourceProvenanceMatches } from "./n100-gate-provenance.mjs";
import { DOCKER_SIM_CPU_LIMIT_CORES } from "./n100-environment-lib.mjs";

const repoRoot = resolve(fileURLToPath(new URL("../..", import.meta.url)));
const FORMAL_STABLE_WINDOW_SECONDS = 300;

const { values } = parseArgs({
  options: {
    output: { type: "string", short: "o" },
    database: { type: "string", short: "d" },
    "health-url": { type: "string" },
    "base-url": { type: "string" },
    "block-device": { type: "string" },
    container: { type: "string" },
    mode: { type: "string", default: "target" },
    targets: { type: "string" },
    gates: { type: "string" },
    "media-root": { type: "string", multiple: true },
    "media-manifest": { type: "string", multiple: true },
    "media-manifest-max-files": { type: "string" },
    "media-manifest-max-directories": { type: "string" },
    "media-manifest-inspect-limit": { type: "string" },
    "media-manifest-header-bytes": { type: "string" },
    "media-manifest-max-archive-bytes": { type: "string" },
    "mixed-rounds": { type: "string" },
    "mixed-duration-seconds": { type: "string" },
    clients: { type: "string", default: "2" },
    "system-duration-seconds": { type: "string", default: "3600" },
    "system-interval": { type: "string", default: "1000" },
    "skip-baseline": { type: "boolean", default: false },
    "node-bin": { type: "string", default: process.execPath },
    help: { type: "boolean", short: "h", default: false },
  },
  strict: true,
});

function usage() {
  console.log(`Usage: node scripts/perf/run-n100-gate.mjs --output <directory> --health-url <url> [options]

Runs the fail-closed N100/NAS evidence sequence:
  preflight -> read-only baseline -> media target preparation -> media matrix
  -> concurrent mixed load while system metrics are sampled.

Required:
  -o, --output <directory>   New, empty artifact directory
      --health-url <url>     /api/health/resources URL without credentials

Deployment:
  -d, --database <path>      SQLite database for baseline/target preparation
      --base-url <url>       Media API base URL (default 127.0.0.1:8787)
      --container <name>     Read preflight evidence through docker exec
      --block-device <name>  Linux /proc/diskstats device to require
      --mode <target|approximation>
                              target is hardware validation; approximation is
                              the current formal Docker simulation (default target)

Inputs and workload:
      --targets <path>       Existing target matrix; otherwise prepare it
      --gates <path>         Declarative media thresholds
      --media-root kind=path Repeatable baseline capacity input
      --media-manifest kind=path
                              Repeatable bounded read-only media manifest
      --skip-baseline        Skip baseline (final status is not a completed Gate)
      --mixed-rounds <n>     Explicit non-stable fixed-round compatibility mode
      --mixed-duration-seconds <n>
                              Formal stable window; must be exactly 300s
      --clients <n>          Logical clients (default 2)
      --system-duration-seconds <n>
                              Sampler safety duration; stopped after workload
      --system-interval <ms> Sampler interval (default 1000)
      --node-bin <path>      Node executable for child runners

Credentials are inherited only by child HTTP runners from PERF_COOKIE or
PERF_AUTHORIZATION and are never written to this artifact.`);
}

if (values.help) {
  usage();
  process.exit(0);
}

function boundedInteger(name, raw, minimum, maximum) {
  const value = Number(raw);
  if (!Number.isSafeInteger(value) || value < minimum || value > maximum) {
    throw new TypeError(`${name} must be an integer between ${minimum} and ${maximum}`);
  }
  return value;
}

function safeUrl(raw, label) {
  if (typeof raw !== "string" || !raw.trim()) {
    throw new TypeError(`${label} must be a non-empty URL`);
  }
  const url = new URL(raw);
  if (!(url.protocol === "http:" || url.protocol === "https:")) {
    throw new TypeError(`${label} must use http or https`);
  }
  if (url.username || url.password) {
    throw new TypeError(`${label} must not contain URL credentials`);
  }
  return url;
}

function readJson(path, label) {
  try {
    return JSON.parse(readFileSync(path, "utf8"));
  } catch (error) {
    throw new Error(`${label} is not valid JSON: ${error.message}`);
  }
}

function relativeArtifact(outputDir, path) {
  return relative(outputDir, path).replaceAll("\\", "/");
}

function captureTailBytes(counter, chunk) {
  counter.value += Buffer.byteLength(chunk);
}

/**
 * Spawn a child runner without buffering its output.  The child artifacts are
 * the source of truth; this wrapper stores only byte counts and exit facts so
 * a URL, query string, cookie or authorization header cannot leak into the
 * orchestration artifact through captured stdout/stderr.
 */
function startStep(id, nodeBin, args) {
  const startedAt = new Date().toISOString();
  const stdout = { value: 0 };
  const stderr = { value: 0 };
  let settled = false;
  let intentionalStop = false;
  let resolveCompletion;
  const completion = new Promise((resolvePromise) => {
    resolveCompletion = resolvePromise;
  });
  let child;
  try {
    child = spawn(nodeBin, args, {
      cwd: repoRoot,
      env: process.env,
      stdio: ["ignore", "pipe", "pipe"],
      windowsHide: true,
    });
  } catch (_error) {
    settled = true;
    resolveCompletion({
      id,
      status: "failed",
      exit_code: null,
      signal: null,
      expected_stop: false,
      stdout_bytes: 0,
      stderr_bytes: 0,
      spawn_error: true,
      started_at: startedAt,
    });
    return { child: null, completion };
  }
  child.stdout?.on("data", (chunk) => captureTailBytes(stdout, chunk));
  child.stderr?.on("data", (chunk) => captureTailBytes(stderr, chunk));
  child.once("error", () => {
    if (settled) return;
    settled = true;
    resolveCompletion({
      id,
      status: "failed",
      exit_code: null,
      signal: null,
      expected_stop: false,
      stdout_bytes: stdout.value,
      stderr_bytes: stderr.value,
      spawn_error: true,
      started_at: startedAt,
    });
  });
  child.once("close", (exitCode, signal) => {
    if (settled) return;
    settled = true;
    resolveCompletion({
      id,
      status: intentionalStop ? "stopped" : exitCode === 0 ? "passed" : "failed",
      exit_code: exitCode,
      signal,
      expected_stop: intentionalStop,
      stdout_bytes: stdout.value,
      stderr_bytes: stderr.value,
      spawn_error: false,
      started_at: startedAt,
    });
  });
  return {
    child,
    completion,
    stop() {
      if (settled || !child) return false;
      intentionalStop = true;
      return child.kill();
    },
  };
}

async function runStep(id, nodeBin, args) {
  const started = startStep(id, nodeBin, args);
  return started.completion;
}

function scriptPath(name) {
  return resolve(repoRoot, "scripts", "perf", name);
}

function addOption(args, flag, value) {
  if (value != null && String(value).trim()) args.push(flag, String(value));
}

function addRepeatedOption(args, flag, valuesToAdd) {
  for (const value of valuesToAdd ?? []) addOption(args, flag, value);
}

function artifactExists(path) {
  return existsSync(path);
}

function sha256File(path) {
  try {
    return createHash("sha256").update(readFileSync(path)).digest("hex");
  } catch {
    return null;
  }
}

function provenanceCheck(id, status, details = {}) {
  return { id, status, ...details };
}

function resolveTargetUrl(target, baseUrl) {
  const targetBase = target?.base_url == null
    ? baseUrl
    : new URL(target.base_url, baseUrl);
  return new URL(target?.url, targetBase);
}

/**
 * Verify that all child artifacts describe the same source tree, schema,
 * server origin and target matrix.  The media runners intentionally keep
 * their artifacts redacted and small; this sidecar supplies the missing
 * cross-artifact identity without storing query strings or credentials.
 */
function buildProvenance({ current, baselineDir, targetsPath, baseUrl, healthUrl }) {
  const checks = [];
  const baselineEnvironmentPath = resolve(baselineDir, "environment.json");
  const baselineManifestPath = resolve(baselineDir, "dataset-manifest.json");
  const baselineHealthPath = resolve(baselineDir, "health-snapshot.json");
  const baselineEnvironment = artifactExists(baselineEnvironmentPath)
    ? readJson(baselineEnvironmentPath, "baseline environment")
    : null;
  const baselineManifest = artifactExists(baselineManifestPath)
    ? readJson(baselineManifestPath, "baseline dataset manifest")
    : null;
  const baselineHealth = artifactExists(baselineHealthPath)
    ? readJson(baselineHealthPath, "baseline health snapshot")
    : null;
  const baselineSource = baselineEnvironment?.source;
  const sourceMatches = sourceProvenanceMatches(baselineSource, current);
  checks.push(sourceMatches
    ? provenanceCheck("source-state", "passed", { commit: current.commit, dirty: current.dirty })
    : provenanceCheck("source-state", baselineSource ? "failed" : "incomplete", {
        failure: baselineSource ? "baseline source state differs from the runner source" : "baseline source provenance is missing",
      }));

  const baselineSchema = baselineManifest?.schema?.version;
  checks.push(baselineSchema === current?.schema_version
    ? provenanceCheck("schema", "passed", { version: current.schema_version })
    : provenanceCheck("schema", baselineSchema == null ? "incomplete" : "failed", {
        baseline: baselineSchema ?? null,
        current: current?.schema_version ?? null,
        failure: baselineSchema == null ? "baseline schema version is missing" : "schema version differs from the runner",
      }));

  let targetSpec = null;
  let targetReadError = null;
  if (artifactExists(targetsPath)) {
    try {
      targetSpec = readJson(targetsPath, "target matrix");
    } catch (error) {
      targetReadError = error.message;
    }
  }
  const targets = Array.isArray(targetSpec?.targets) ? targetSpec.targets : [];
  const targetIds = targets.map((target) => target?.id).filter((id) => typeof id === "string");
  const uniqueIds = new Set(targetIds).size === targetIds.length;
  const targetShapeValid = targets.length > 0
    && targetSpec?.profile === "nas-n100-4g"
    && uniqueIds
    && !targetReadError;
  checks.push(targetShapeValid
    ? provenanceCheck("target-matrix", "passed", {
        profile: targetSpec.profile,
        targets: targets.length,
        ids_sha256: createHash("sha256").update(targetIds.join("\n")).digest("hex"),
      })
    : provenanceCheck("target-matrix", targetReadError || !artifactExists(targetsPath) ? "incomplete" : "failed", {
        profile: targetSpec?.profile ?? null,
        targets: targets.length,
        failure: targetReadError ?? (targetSpec?.profile !== "nas-n100-4g"
          ? "target matrix profile is not nas-n100-4g"
          : "target matrix is empty or contains duplicate IDs"),
      }));

  const endpointOrigins = new Set();
  let targetOriginError = null;
  for (const target of targets) {
    try {
      endpointOrigins.add(resolveTargetUrl(target, baseUrl).origin);
    } catch (error) {
      targetOriginError = error.message;
      break;
    }
  }
  const targetOriginMatches = !targetOriginError
    && endpointOrigins.size === 1
    && endpointOrigins.has(baseUrl.origin);
  checks.push(targetOriginMatches
    ? provenanceCheck("target-origin", "passed", { origin: baseUrl.origin })
    : provenanceCheck("target-origin", targetOriginError ? "incomplete" : "failed", {
        expected: baseUrl.origin,
        observed: [...endpointOrigins],
        failure: targetOriginError ?? "one or more target URLs point to a different server origin",
      }));

  const healthBody = baselineHealth?.body;
  const baselineHealthUrl = typeof baselineHealth?.url === "string" ? baselineHealth.url : null;
  let healthOriginMatches = false;
  try {
    healthOriginMatches = baselineHealth?.ok === true
      && healthBody?.status === "ok"
      && healthBody?.schema_version === current?.schema_version
      && healthBody?.sqlite?.config?.profile === "nas-n100-4g"
      && baselineHealthUrl != null
      && new URL(baselineHealthUrl).origin === healthUrl.origin;
  } catch {
    healthOriginMatches = false;
  }
  checks.push(healthOriginMatches
    ? provenanceCheck("baseline-health", "passed", { origin: healthUrl.origin, schema_version: current.schema_version })
    : provenanceCheck("baseline-health", baselineHealth ? "failed" : "incomplete", {
        expected_origin: healthUrl.origin,
        failure: baselineHealth ? "baseline health is not coherent with the target profile/schema/origin" : "baseline health snapshot is missing",
      }));

  const failed = checks.filter((check) => check.status === "failed");
  const incomplete = checks.filter((check) => check.status === "incomplete");
  return {
    artifact_version: PERF_ARTIFACT_VERSION,
    scenario: "n100-gate-provenance",
    captured_at: new Date().toISOString(),
    source: current,
    inputs: {
      baseline_environment: relativeArtifact(resolve(baselineDir, ".."), baselineEnvironmentPath),
      baseline_manifest: relativeArtifact(resolve(baselineDir, ".."), baselineManifestPath),
      baseline_health: relativeArtifact(resolve(baselineDir, ".."), baselineHealthPath),
      target_file_sha256: sha256File(targetsPath),
      target_count: targets.length,
      base_origin: baseUrl.origin,
      health_origin: healthUrl.origin,
    },
    checks,
    status: failed.length ? "failed" : incomplete.length ? "incomplete" : "passed",
    failed: failed.map((check) => check.id),
    incomplete: incomplete.map((check) => check.id),
  };
}

function hasSystemSamples(path) {
  if (!existsSync(path)) return false;
  try {
    return readFileSync(path, "utf8").trim().split(/\r?\n/u).length >= 2;
  } catch {
    return false;
  }
}

async function main() {
  if (!values.output || !values["health-url"]) {
    usage();
    process.exitCode = 2;
    return;
  }
  const outputDir = resolve(values.output);
  if (existsSync(outputDir) && readdirSync(outputDir).length > 0) {
    throw new Error(`refusing to write into a non-empty output directory: ${outputDir}`);
  }
  mkdirSync(outputDir, { recursive: true });

  const mode = values.mode;
  if (mode !== "target" && mode !== "approximation") {
    throw new TypeError("--mode must be target or approximation");
  }
  const healthUrl = safeUrl(values["health-url"], "--health-url");
  const baseUrl = safeUrl(
    values["base-url"] ?? "http://127.0.0.1:8787",
    "--base-url",
  );
  const nodeBin = resolve(values["node-bin"]);
  const clients = boundedInteger("clients", values.clients, 2, 16);
  const hasRounds = values["mixed-rounds"] != null;
  const hasDuration = values["mixed-duration-seconds"] != null;
  if (hasRounds && hasDuration) {
    throw new TypeError("--mixed-rounds and --mixed-duration-seconds are mutually exclusive");
  }
  const rounds = hasRounds
    ? boundedInteger("mixed-rounds", values["mixed-rounds"], 1, 10_000)
    : null;
  // The formal acceptance window is fixed at five minutes. Longer diagnostic
  // runs must use the explicitly non-stable fixed-round compatibility mode.
  const durationSeconds = hasRounds
    ? null
    : boundedInteger(
        "mixed-duration-seconds",
        values["mixed-duration-seconds"] ?? "300",
        FORMAL_STABLE_WINDOW_SECONDS,
        FORMAL_STABLE_WINDOW_SECONDS,
      );
  const systemDurationSeconds = boundedInteger(
    "system-duration-seconds",
    values["system-duration-seconds"],
    1,
    24 * 60 * 60,
  );
  const systemInterval = boundedInteger("system-interval", values["system-interval"], 100, 60_000);
  const database = values.database ? resolve(values.database) : null;
  const targetsPath = values.targets ? resolve(values.targets) : resolve(outputDir, "media-targets.json");
  const gatesPath = resolve(
    values.gates ?? resolve(repoRoot, "scripts", "perf", "gates", "media-n100-4g.json"),
  );
  const baselineDir = resolve(outputDir, "baseline");
  const mediaDir = resolve(outputDir, "media-gates");
  const mixedDir = resolve(outputDir, "media-mixed-5m");
  const environmentPath = resolve(outputDir, "environment-gate.json");
  const provenancePath = resolve(outputDir, "provenance.json");
  const systemPath = resolve(outputDir, "system-samples.csv");
  const currentProvenance = captureSourceProvenance(repoRoot, process.env);

  const steps = [];
  const startedAt = new Date().toISOString();

  const preflightArgs = [
    scriptPath("check-n100-environment.mjs"),
    "--output",
    environmentPath,
    "--health-url",
    healthUrl.toString(),
    "--expected-profile",
    "nas-n100-4g",
  ];
  if (values.container) addOption(preflightArgs, "--container", values.container);
  if (values["block-device"]) addOption(preflightArgs, "--block-device", values["block-device"]);
  // A cgroup CPU quota is evidence for Docker/approximation runs. Bare-metal
  // N100 deployments are intentionally allowed to omit that optional quota.
  if (values.container || mode === "approximation") {
    addOption(preflightArgs, "--max-cpu-cores", String(DOCKER_SIM_CPU_LIMIT_CORES));
  }
  console.error("[n100-gate] preflight");
  const preflight = await runStep("preflight", nodeBin, preflightArgs);
  steps.push(preflight);
  const preflightArtifact = artifactExists(environmentPath)
    ? readJson(environmentPath, "environment-gate artifact")
    : null;
  const preflightGateStatus = preflightArtifact?.gate?.status ?? "incomplete";
  const targetPreflightPassed = preflightGateStatus === "passed";
  const approximationCpuOnlyFailure = preflightArtifact?.gate?.status === "failed"
    && Array.isArray(preflightArtifact.gate.failed)
    && preflightArtifact.gate.failed.length === 1
    && preflightArtifact.gate.failed[0] === "n100-model"
    && (!preflightArtifact.gate.incomplete || preflightArtifact.gate.incomplete.length === 0);
  const approximationAllowed = mode === "approximation"
    && (targetPreflightPassed || approximationCpuOnlyFailure);
  if (!targetPreflightPassed && !approximationAllowed) {
    const provenance = buildProvenance({
      current: currentProvenance,
      baselineDir,
      targetsPath,
      baseUrl,
      healthUrl,
    });
    writeFileSync(provenancePath, `${JSON.stringify(provenance, null, 2)}\n`, { flag: "wx" });
    const run = {
      artifact_version: PERF_ARTIFACT_VERSION,
      scenario: "n100-nas-gate",
      generated_at: new Date().toISOString(),
      execution_mode: mode,
      status: "failed",
      target_gate_status: preflightGateStatus,
      started_at: startedAt,
      requirements: {
        profile: "nas-n100-4g",
        memory_limit_bytes: 4 * 1024 * 1024 * 1024,
        cpu_quota_cores: values.container || mode === "approximation" ? DOCKER_SIM_CPU_LIMIT_CORES : null,
      },
      inputs: {
        health_path: healthUrl.pathname,
        base_path: baseUrl.pathname,
        database_provided: database != null,
        media_root_kinds: (values["media-root"] ?? []).map((entry) => entry.split("=", 1)[0]),
        media_manifest_kinds: (values["media-manifest"] ?? []).map((entry) => entry.split("=", 1)[0]),
      },
      steps,
      artifacts: {
        environment_gate: relativeArtifact(outputDir, environmentPath),
        provenance: relativeArtifact(outputDir, provenancePath),
      },
      gate: {
        status: "failed",
        reason: "target preflight did not pass; workload steps were not started",
      },
    };
    writeFileSync(resolve(outputDir, "run.json"), `${JSON.stringify(run, null, 2)}\n`, { flag: "wx" });
    process.exitCode = 1;
    return;
  }

  if (!values["skip-baseline"]) {
    if (!database) throw new Error("--database is required unless --skip-baseline is used");
    const baselineArgs = [scriptPath("capture-baseline.mjs"), "--output", baselineDir, "--database", database, "--health-url", healthUrl.toString()];
    addRepeatedOption(baselineArgs, "--media-root", values["media-root"]);
    addRepeatedOption(baselineArgs, "--media-manifest", values["media-manifest"]);
    for (const [flag, key] of [
      ["--media-manifest-max-files", "media-manifest-max-files"],
      ["--media-manifest-max-directories", "media-manifest-max-directories"],
      ["--media-manifest-inspect-limit", "media-manifest-inspect-limit"],
      ["--media-manifest-header-bytes", "media-manifest-header-bytes"],
      ["--media-manifest-max-archive-bytes", "media-manifest-max-archive-bytes"],
    ]) addOption(baselineArgs, flag, values[key]);
    console.error("[n100-gate] baseline");
    steps.push(await runStep("baseline", nodeBin, baselineArgs));
  } else {
    steps.push({ id: "baseline", status: "skipped", reason: "explicit --skip-baseline" });
  }

  if (!values.targets) {
    if (!database) throw new Error("--database is required when --targets is not supplied");
    const prepareArgs = [
      scriptPath("prepare-media-targets.mjs"),
      "--database",
      database,
      "--output",
      targetsPath,
      "--base-url",
      baseUrl.toString().replace(/\/$/u, ""),
    ];
    console.error("[n100-gate] media target preparation");
    steps.push(await runStep("target-preparation", nodeBin, prepareArgs));
  } else {
    steps.push({ id: "target-preparation", status: "provided", target_path: relativeArtifact(outputDir, targetsPath) });
  }

  const mediaArgs = [
    scriptPath("run-media-gates.mjs"),
    "--targets",
    targetsPath,
    "--output",
    mediaDir,
    "--gates",
    gatesPath,
    "--base-url",
    baseUrl.toString(),
  ];
  console.error("[n100-gate] media matrix");
  steps.push(await runStep("media-matrix", nodeBin, mediaArgs));

  const samplerArgs = [
    scriptPath("sample-system.mjs"),
    "--output",
    systemPath,
    "--health-url",
    healthUrl.toString(),
    "--duration",
    String(systemDurationSeconds),
    "--interval",
    String(systemInterval),
  ];
  if (values["block-device"]) addOption(samplerArgs, "--block-device", values["block-device"]);
  console.error("[n100-gate] mixed load + system sampler");
  const sampler = startStep("system-sampler", nodeBin, samplerArgs);
  const mixedArgs = [
    scriptPath("run-media-mixed-load.mjs"),
    "--targets",
    targetsPath,
    "--output",
    mixedDir,
    "--gates",
    gatesPath,
    "--base-url",
    baseUrl.toString(),
    "--health-url",
    healthUrl.toString(),
    "--clients",
    String(clients),
  ];
  if (durationSeconds == null) addOption(mixedArgs, "--rounds", rounds);
  else addOption(mixedArgs, "--duration-seconds", durationSeconds);
  const mixed = await runStep("mixed-load", nodeBin, mixedArgs);
  steps.push(mixed);
  sampler.stop?.();
  steps.push(await sampler.completion);

  const mediaSummaryPath = resolve(mediaDir, "summary.json");
  const mixedSummaryPath = resolve(mixedDir, "summary.json");
  const mediaSummary = artifactExists(mediaSummaryPath) ? readJson(mediaSummaryPath, "media summary") : null;
  const mixedSummary = artifactExists(mixedSummaryPath) ? readJson(mixedSummaryPath, "mixed summary") : null;
  const provenance = buildProvenance({
    current: currentProvenance,
    baselineDir,
    targetsPath,
    baseUrl,
    healthUrl,
  });
  writeFileSync(provenancePath, `${JSON.stringify(provenance, null, 2)}\n`, { flag: "wx" });
  const samplerStep = steps.find((step) => step.id === "system-sampler");
  const samplerEvidence = hasSystemSamples(systemPath)
    && ["passed", "stopped"].includes(samplerStep?.status);
  const requiredStepIds = ["preflight", "baseline", "target-preparation", "media-matrix", "mixed-load", "system-sampler"];
  const stepFailures = steps.filter((step) => ["failed", "incomplete"].includes(step.status));
  const skipped = steps.filter((step) => step.status === "skipped");
  const workloadPassed = mediaSummary?.gate?.status === "passed"
    && mixedSummary?.gate?.status === "passed"
    && samplerEvidence
    && provenance.status === "passed"
    && !stepFailures.some((step) => ["baseline", "target-preparation", "media-matrix", "mixed-load", "system-sampler"].includes(step.id));
  const finalStatus = mode === "approximation"
    ? workloadPassed && approximationAllowed && skipped.length === 0
      ? "passed"
      : skipped.length > 0 ? "incomplete" : "failed"
    : workloadPassed && targetPreflightPassed && skipped.length === 0
      ? "passed"
      : skipped.length > 0 ? "incomplete" : "failed";
  const run = {
    artifact_version: PERF_ARTIFACT_VERSION,
    scenario: "n100-nas-gate",
    generated_at: new Date().toISOString(),
    execution_mode: mode,
    status: finalStatus,
    target_gate_status: preflightGateStatus,
    started_at: startedAt,
    requirements: {
      profile: "nas-n100-4g",
      memory_limit_bytes: 4 * 1024 * 1024 * 1024,
      cpu_quota_cores: values.container || mode === "approximation" ? DOCKER_SIM_CPU_LIMIT_CORES : null,
      clients,
      stable_window_seconds: durationSeconds,
    },
    inputs: {
      health_path: healthUrl.pathname,
      base_path: baseUrl.pathname,
      database_provided: database != null,
      media_root_kinds: (values["media-root"] ?? []).map((entry) => entry.split("=", 1)[0]),
      media_manifest_kinds: (values["media-manifest"] ?? []).map((entry) => entry.split("=", 1)[0]),
      targets_source: values.targets ? "provided" : "prepared-read-only",
    },
    steps,
    artifacts: {
      environment_gate: relativeArtifact(outputDir, environmentPath),
      provenance: relativeArtifact(outputDir, provenancePath),
      baseline: values["skip-baseline"] ? null : relativeArtifact(outputDir, baselineDir),
      targets: relativeArtifact(outputDir, targetsPath),
      media_gates: relativeArtifact(outputDir, mediaDir),
      mixed_load: relativeArtifact(outputDir, mixedDir),
      system_samples: relativeArtifact(outputDir, systemPath),
    },
    summaries: {
      media_gate_status: mediaSummary?.gate?.status ?? null,
      mixed_gate_status: mixedSummary?.gate?.status ?? null,
      provenance_status: provenance.status,
      stable_window_seconds: durationSeconds,
    },
    gate: {
      status: finalStatus,
      required_steps: requiredStepIds,
      failed_steps: stepFailures.map((step) => step.id),
      skipped_steps: skipped.map((step) => step.id),
      provenance_failed: provenance.failed,
      provenance_incomplete: provenance.incomplete,
      note: mode === "approximation"
        ? "Docker simulation is the formal project acceptance environment; CPU/model provenance remains recorded"
        : null,
    },
  };
  writeFileSync(resolve(outputDir, "run.json"), `${JSON.stringify(run, null, 2)}\n`, { flag: "wx" });
  if (finalStatus !== "passed") process.exitCode = 1;
}

main().catch((error) => {
  console.error(`[n100-gate] ${error.message}`);
  process.exitCode = 1;
});
