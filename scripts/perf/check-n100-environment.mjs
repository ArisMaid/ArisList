import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { execFile } from "node:child_process";
import os from "node:os";
import { dirname, resolve } from "node:path";
import { parseArgs } from "node:util";
import { promisify } from "node:util";

import {
  evaluateN100Environment,
  N100_MEMORY_LIMIT_BYTES,
  N100_CPU_LIMIT_CORES,
  N100_PROFILE,
} from "./n100-environment-lib.mjs";
import { PERF_ARTIFACT_VERSION } from "./perf-lib.mjs";

const execFileAsync = promisify(execFile);

const { values } = parseArgs({
  options: {
    output: { type: "string", short: "o" },
    "health-url": { type: "string" },
    "block-device": { type: "string" },
    "expected-profile": { type: "string", default: N100_PROFILE },
    "max-memory-bytes": { type: "string", default: String(N100_MEMORY_LIMIT_BYTES) },
    "max-cpu-cores": { type: "string" },
    container: { type: "string" },
    help: { type: "boolean", short: "h", default: false },
  },
  strict: true,
});

if (values.help || !values.output) {
  console.log(`Usage: node scripts/perf/check-n100-environment.mjs --output <artifact.json> [options]

Options:
  -o, --output <path>          New JSON artifact; existing files are refused
      --health-url <url>       /api/health/resources URL (required for a pass)
      --block-device <name>    Linux /proc/diskstats device to require
      --expected-profile <id>  Expected server resource profile
      --max-memory-bytes <n>   Maximum cgroup memory limit (default 4GiB)
      --max-cpu-cores <n>      Optional maximum cgroup CPU quota (use 1 for project simulation)
      --container <name>      Read /proc and cgroup evidence through docker exec
  -h, --help                  Show this help

The command is fail-closed: missing Linux, N100, cgroup, health or requested
block-device evidence produces an incomplete/failed Gate, never a pass.`);
  process.exit(values.help ? 0 : 2);
}

function readText(path) {
  try {
    return readFileSync(path, "utf8").trim();
  } catch {
    return null;
  }
}

function parseMemoryLimit(readValue) {
  const candidates = ["/sys/fs/cgroup/memory.max", "/sys/fs/cgroup/memory/memory.limit_in_bytes"];
  for (const path of candidates) {
    const value = readValue(path);
    if (value == null || value === "max") continue;
    const parsed = Number(value);
    if (Number.isSafeInteger(parsed) && parsed > 0) return parsed;
  }
  return null;
}

function parseCpuLimitCores(readValue) {
  const v2 = readValue("/sys/fs/cgroup/cpu.max");
  if (v2) {
    const [quotaText, periodText] = v2.split(/\s+/u);
    const period = Number(periodText);
    if (Number.isFinite(period) && period > 0 && quotaText !== "max") {
      const quota = Number(quotaText);
      if (Number.isFinite(quota) && quota > 0) return quota / period;
    }
  }
  const quota = Number(readValue("/sys/fs/cgroup/cpu/cpu.cfs_quota_us"));
  const period = Number(readValue("/sys/fs/cgroup/cpu/cpu.cfs_period_us"));
  return Number.isFinite(quota) && quota > 0 && Number.isFinite(period) && period > 0
    ? quota / period
    : null;
}

function hasBlockDevice(device, diskstats) {
  if (!device) return null;
  const lines = diskstats?.split(/\r?\n/u) ?? [];
  return lines.some((line) => line.trim().split(/\s+/u)[2] === device) ? device : null;
}

function parseCpuModel(cpuInfo) {
  const line = cpuInfo
    ?.split(/\r?\n/u)
    .find((entry) => /^(?:model name|Hardware|Processor)\s*:/u.test(entry));
  return line?.split(":").slice(1).join(":").trim() || null;
}

function parsePlatform(uname) {
  const value = uname?.trim().toLowerCase();
  return value === "linux" ? "linux" : value || null;
}

const targetContainer = values.container?.trim() || null;
const targetReadErrors = [];

async function readTargetFile(path) {
  if (!targetContainer) return readText(path);
  try {
    const result = await execFileAsync("docker", ["exec", targetContainer, "cat", path], {
      windowsHide: true,
      maxBuffer: 2 * 1024 * 1024,
    });
    return result.stdout.trim();
  } catch (error) {
    // cgroup v1/v2 fallback files are intentionally probed in turn; a missing
    // alternate hierarchy is not itself a read failure.  A missing required
    // value still becomes an explicit incomplete Gate below.
    if (!/No such file or directory/u.test(error.message)) {
      targetReadErrors.push({ path, error: error.message });
    }
    return null;
  }
}

async function readTargetCommand(args, label) {
  if (!targetContainer) return null;
  try {
    const result = await execFileAsync("docker", ["exec", targetContainer, ...args], {
      windowsHide: true,
      maxBuffer: 64 * 1024,
    });
    return result.stdout.trim();
  } catch (error) {
    targetReadErrors.push({ path: label, error: error.message });
    return null;
  }
}

async function readHealth(rawUrl) {
  if (!rawUrl) return { status: null, profile: null, path: null };
  const url = new URL(rawUrl);
  if (url.username || url.password) throw new Error("--health-url must not contain credentials");
  const response = await fetch(url, { signal: AbortSignal.timeout(5_000) });
  let body = null;
  try {
    body = await response.json();
  } catch {
    body = null;
  }
  return {
    status: response.ok ? body?.status ?? null : `http-${response.status}`,
    profile: body?.sqlite?.config?.profile ?? null,
    path: url.pathname,
  };
}

const maxMemoryBytes = Number(values["max-memory-bytes"]);
if (!Number.isSafeInteger(maxMemoryBytes) || maxMemoryBytes <= 0) {
  throw new Error("--max-memory-bytes must be a positive safe integer");
}
const maxCpuCores = values["max-cpu-cores"] == null
  ? null
  : Number(values["max-cpu-cores"]);
if (maxCpuCores != null && (!Number.isFinite(maxCpuCores) || maxCpuCores <= 0 || maxCpuCores > 256)) {
  throw new Error(`--max-cpu-cores must be a number between 0 and 256 (suggested N100 value: ${N100_CPU_LIMIT_CORES})`);
}
const targetFiles = new Map();
for (const path of [
  "/proc/cpuinfo",
  "/sys/fs/cgroup/memory.max",
  "/sys/fs/cgroup/memory/memory.limit_in_bytes",
  "/sys/fs/cgroup/cpu.max",
  "/sys/fs/cgroup/cpu/cpu.cfs_quota_us",
  "/sys/fs/cgroup/cpu/cpu.cfs_period_us",
  "/proc/diskstats",
]) {
  targetFiles.set(path, await readTargetFile(path));
}
const targetPlatform = targetContainer
  ? parsePlatform(await readTargetCommand(["uname", "-s"], "uname -s"))
  : null;
const readValue = (path) => targetFiles.get(path) ?? null;
const health = await readHealth(values["health-url"]);
const evidence = {
  platform: targetContainer
    ? targetPlatform
    : process.platform === "linux"
      ? "linux"
      : process.platform,
  cpu_model: targetContainer ? parseCpuModel(targetFiles.get("/proc/cpuinfo")) : os.cpus()[0]?.model ?? null,
  cgroup_memory_max_bytes: parseMemoryLimit(readValue),
  cgroup_cpu_limit_cores: parseCpuLimitCores(readValue),
  resource_profile: health.profile,
  health_status: health.status,
  health_path: health.path,
  block_device: hasBlockDevice(values["block-device"], targetFiles.get("/proc/diskstats")),
  target_container: targetContainer,
  target_read_errors: targetReadErrors,
};
const gate = evaluateN100Environment(evidence, {
  expectedProfile: values["expected-profile"],
  maxMemoryBytes,
  maxCpuCores,
  blockDevice: values["block-device"] ?? null,
});
const artifact = {
  artifact_version: PERF_ARTIFACT_VERSION,
  scenario: "n100-environment-preflight",
  captured_at: new Date().toISOString(),
  requirements: {
    expected_profile: values["expected-profile"],
    max_memory_bytes: maxMemoryBytes,
    max_cpu_cores: maxCpuCores,
    block_device: values["block-device"] ?? null,
  },
  evidence,
  gate,
};
const output = resolve(values.output);
if (existsSync(output)) throw new Error(`refusing to overwrite existing artifact: ${output}`);
mkdirSync(dirname(output), { recursive: true });
writeFileSync(output, `${JSON.stringify(artifact, null, 2)}\n`, { encoding: "utf8", flag: "wx" });
console.log(JSON.stringify({ output, status: gate.status }));
if (gate.status !== "passed") process.exitCode = 1;
