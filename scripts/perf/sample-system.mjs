import {
  appendFileSync,
  existsSync,
  readFileSync,
  readdirSync,
  writeFileSync,
} from "node:fs";
import os from "node:os";
import { join, resolve } from "node:path";
import { parseArgs } from "node:util";

import { SYSTEM_SAMPLE_COLUMNS } from "./perf-lib.mjs";

const header = SYSTEM_SAMPLE_COLUMNS;

const { values } = parseArgs({
  options: {
    output: { type: "string", short: "o" },
    "health-url": { type: "string" },
    duration: { type: "string", default: "60" },
    interval: { type: "string", default: "1000" },
    samples: { type: "string" },
    "block-device": { type: "string" },
    help: { type: "boolean", short: "h" },
  },
  strict: true,
});

if (values.help || !values.output) {
  console.log(`Usage: node scripts/perf/sample-system.mjs --output <system-samples.csv> [options]

Options:
      --health-url <url>      Optional /api/health/resources URL
      --duration <seconds>    Sampling duration (default 60)
      --interval <ms>         Sampling interval (default 1000)
      --samples <count>       Exact sample count; overrides duration
      --block-device <name>   Linux /proc/diskstats device, for example sda

The sampler appends to an existing G0 CSV and never truncates it.`);
  process.exit(values.help ? 0 : 2);
}

function boundedInteger(name, raw, minimum, maximum) {
  const value = Number(raw);
  if (!Number.isInteger(value) || value < minimum || value > maximum) {
    throw new Error(`${name} must be an integer between ${minimum} and ${maximum}`);
  }
  return value;
}

const intervalMs = boundedInteger("interval", values.interval, 100, 60_000);
const durationSeconds = boundedInteger("duration", values.duration, 1, 24 * 60 * 60);
const sampleCount = values.samples
  ? boundedInteger("samples", values.samples, 1, 1_000_000)
  : Math.max(1, Math.ceil((durationSeconds * 1000) / intervalMs));
const output = resolve(values.output);
const healthUrl = values["health-url"] ?? process.env.PERF_HEALTH_URL ?? null;
const blockDevice = values["block-device"] ?? process.env.PERF_BLOCK_DEVICE ?? null;

function readText(path) {
  try {
    return readFileSync(path, "utf8").trim();
  } catch {
    return null;
  }
}

function readNumber(path) {
  const text = readText(path);
  if (text == null || text === "max") return null;
  const value = Number(text);
  return Number.isFinite(value) ? value : null;
}

function readCgroupLimit(path) {
  const text = readText(path);
  if (text == null || text === "max") return null;
  const value = Number(text);
  return Number.isFinite(value) && value >= 0 && value <= 2 ** 60 ? value : null;
}

function memoryEvents() {
  const text = readText("/sys/fs/cgroup/memory.events");
  if (!text) return null;
  const events = Object.fromEntries(
    text.split(/\r?\n/u).map((line) => {
      const [key, value] = line.trim().split(/\s+/u);
      return [key, Number(value)];
    }),
  );
  return Number(events.oom ?? 0) + Number(events.oom_kill ?? 0);
}

function cgroupMemoryLimit() {
  return (
    readCgroupLimit("/sys/fs/cgroup/memory.max") ??
    readCgroupLimit("/sys/fs/cgroup/memory/memory.limit_in_bytes")
  );
}

function cgroupCpu() {
  const v2 = readText("/sys/fs/cgroup/cpu.max");
  if (v2) {
    const [quotaText, periodText] = v2.split(/\s+/u);
    const period = Number(periodText);
    if (Number.isFinite(period) && period > 0) {
      const quota = quotaText === "max" ? null : Number(quotaText);
      return {
        quota_micros: Number.isFinite(quota) && quota >= 0 ? quota : null,
        period_micros: period,
        limit_cores:
          Number.isFinite(quota) && quota >= 0 ? quota / period : null,
      };
    }
  }
  const quotaText = readText("/sys/fs/cgroup/cpu/cpu.cfs_quota_us");
  const period = Number(readText("/sys/fs/cgroup/cpu/cpu.cfs_period_us"));
  if (!Number.isFinite(period) || period <= 0) return null;
  const quota = quotaText == null ? Number.NaN : Number(quotaText);
  return {
    quota_micros: Number.isFinite(quota) && quota >= 0 ? quota : null,
    period_micros: period,
    limit_cores: Number.isFinite(quota) && quota >= 0 ? quota / period : null,
  };
}

function cpuStat() {
  const text = readText("/proc/stat");
  const line = text?.split(/\r?\n/u).find((entry) => entry.startsWith("cpu "));
  if (!line) return null;
  const fields = line.trim().split(/\s+/u).slice(1).map(Number);
  if (fields.some((value) => !Number.isFinite(value))) return null;
  return {
    total: fields.reduce((sum, value) => sum + value, 0),
    idle: Number(fields[3] ?? 0),
    iowait: Number(fields[4] ?? 0),
  };
}

function cpuDelta(previous, current) {
  if (!previous || !current) return { cpu: null, iowait: null };
  const total = current.total - previous.total;
  if (total <= 0) return { cpu: null, iowait: null };
  const idle = current.idle - previous.idle;
  const iowait = current.iowait - previous.iowait;
  return {
    cpu: ((total - idle - iowait) / total) * 100,
    iowait: (iowait / total) * 100,
  };
}

function diskStat(device) {
  if (!device) return null;
  const text = readText("/proc/diskstats");
  const line = text
    ?.split(/\r?\n/u)
    .map((entry) => entry.trim().split(/\s+/u))
    .find((fields) => fields[2] === device);
  if (!line) return null;
  const fields = line.slice(3).map(Number);
  return {
    operations: Number(fields[0] ?? 0) + Number(fields[4] ?? 0),
    operation_ms: Number(fields[3] ?? 0) + Number(fields[7] ?? 0),
    weighted_ms: Number(fields[10] ?? 0),
  };
}

function diskDelta(previous, current, elapsedMs) {
  if (!previous || !current || elapsedMs <= 0) return { awaitMs: null, queue: null };
  const operations = current.operations - previous.operations;
  return {
    awaitMs: operations > 0 ? (current.operation_ms - previous.operation_ms) / operations : 0,
    queue: Math.max(0, current.weighted_ms - previous.weighted_ms) / elapsedMs,
  };
}

function temperature() {
  const candidates = [];
  try {
    for (const name of readdirSync("/sys/class/thermal")) {
      if (name.startsWith("thermal_zone")) {
        candidates.push(join("/sys/class/thermal", name, "temp"));
      }
    }
  } catch {
    // Non-Linux host.
  }
  try {
    for (const hwmon of readdirSync("/sys/class/hwmon")) {
      const root = join("/sys/class/hwmon", hwmon);
      for (const name of readdirSync(root)) {
        if (/^temp\d+_input$/u.test(name)) candidates.push(join(root, name));
      }
    }
  } catch {
    // No readable hwmon tree.
  }
  const values = candidates
    .map(readNumber)
    .filter(Number.isFinite)
    .map((value) => (value > 1000 ? value / 1000 : value))
    .filter((value) => value > -50 && value < 200);
  return values.length ? Math.max(...values) : null;
}

async function healthSnapshot() {
  if (!healthUrl) return null;
  const response = await fetch(healthUrl, {
    signal: AbortSignal.timeout(Math.min(5_000, intervalMs)),
  });
  if (!response.ok) throw new Error(`health endpoint returned ${response.status}`);
  return response.json();
}

function csvValue(value) {
  if (value == null || !Number.isFinite(Number(value))) return "";
  return String(Number(value));
}

if (!existsSync(output)) {
  writeFileSync(output, `${header.join(",")}\n`, { encoding: "utf8", flag: "wx" });
} else {
  const firstLine = readFileSync(output, "utf8").split(/\r?\n/u, 1)[0];
  if (firstLine !== header.join(",")) {
    throw new Error(`unexpected CSV header in ${output}`);
  }
}

let previousCpu = cpuStat();
let previousDisk = diskStat(blockDevice);
let previousAt = Date.now();
let healthFailures = 0;
for (let index = 0; index < sampleCount; index += 1) {
  if (index > 0) await new Promise((resolveDelay) => setTimeout(resolveDelay, intervalMs));
  const now = Date.now();
  const currentCpu = cpuStat();
  const currentDisk = diskStat(blockDevice);
  const cpu = cpuDelta(previousCpu, currentCpu);
  const disk = diskDelta(previousDisk, currentDisk, now - previousAt);
  let health = null;
  try {
    health = await healthSnapshot();
  } catch (error) {
    healthFailures += 1;
    console.warn(`health sample failed: ${error.message}`);
  }
  const speeds = os.cpus().map((entry) => entry.speed).filter((value) => value > 0);
  const pools = health?.resources?.pools ?? {};
  const sqlite = health?.sqlite ?? {};
  const readSnapshot = sqlite.read_snapshot ?? {};
  const writeGate = sqlite.write_gate ?? {};
  const walCheckpoint = sqlite.wal_checkpoint ?? {};
  const jobs = health?.jobs ?? {};
  const cgroupCpuSnapshot = health?.cgroup_cpu ?? cgroupCpu();
  const cgroupMemoryLimitBytes =
    health?.cgroup_memory?.max_bytes ?? cgroupMemoryLimit();
  const catalogRevision = health?.search_outbox?.catalog_revision;
  const appliedRevision = health?.search_shadow?.applied_revision;
  const qmediasync = health?.qmediasync ?? {};
  const row = [
    new Date(now).toISOString(),
    health?.cgroup_memory?.current_bytes ?? readNumber("/sys/fs/cgroup/memory.current"),
    readNumber("/sys/fs/cgroup/memory.peak"),
    memoryEvents(),
    cgroupMemoryLimitBytes,
    cgroupCpuSnapshot?.quota_micros ?? null,
    cgroupCpuSnapshot?.period_micros ?? null,
    cgroupCpuSnapshot?.limit_cores ?? null,
    cpu.cpu,
    speeds.length ? speeds.reduce((sum, value) => sum + value, 0) / speeds.length : null,
    temperature(),
    cpu.iowait,
    disk.awaitMs,
    disk.queue,
    sqlite.wal_bytes ?? null,
    sqlite.pool_size ?? null,
    sqlite.idle_connections ?? null,
    sqlite.active_connections ?? null,
    sqlite.pool_saturated ?? null,
    sqlite.pool_checkout_samples ?? null,
    sqlite.pool_checkout_idle_total_micros ?? null,
    sqlite.pool_checkout_idle_max_micros ?? null,
    sqlite.pool_connections_opened ?? null,
    sqlite.pool_tracked_acquire_samples ?? null,
    sqlite.pool_tracked_acquire_wait_total_micros ?? null,
    sqlite.pool_tracked_acquire_wait_max_micros ?? null,
    sqlite.pool_acquire_timeouts ?? null,
    sqlite.pool_acquire_errors ?? null,
    sqlite.sqlite_busy_errors ?? null,
    readSnapshot.active ?? null,
    readSnapshot.samples ?? null,
    readSnapshot.completed ?? null,
    readSnapshot.hold_total_micros ?? null,
    readSnapshot.hold_max_micros ?? null,
    readSnapshot.oldest_active_micros ?? null,
    readSnapshot.implicit_rollbacks ?? null,
    writeGate.queue_depth ?? null,
    writeGate.queue_bytes ?? null,
    writeGate.active ?? null,
    writeGate.active_bytes ?? null,
    writeGate.acquire_samples ?? null,
    writeGate.acquire_wait_total_micros ?? null,
    writeGate.acquire_wait_max_micros ?? null,
    writeGate.completed ?? null,
    writeGate.hold_total_micros ?? null,
    writeGate.hold_max_micros ?? null,
    walCheckpoint.attempts ?? null,
    walCheckpoint.successes ?? null,
    walCheckpoint.failures ?? null,
    walCheckpoint.last_busy_pages ?? null,
    walCheckpoint.last_log_pages ?? null,
    walCheckpoint.last_checkpointed_pages ?? null,
    walCheckpoint.last_duration_micros ?? null,
    jobs.worker_limit ?? null,
    jobs.active_jobs ?? null,
    jobs.maintenance_active ?? null,
    jobs.maintenance_wait_samples ?? null,
    jobs.maintenance_wait_total_micros ?? null,
    jobs.maintenance_wait_max_micros ?? null,
    jobs.completed_jobs ?? null,
    jobs.failed_jobs ?? null,
    jobs.claim_errors ?? null,
    health?.search_outbox?.pending ?? null,
    Number.isFinite(catalogRevision) && Number.isFinite(appliedRevision)
      ? Math.max(0, catalogRevision - appliedRevision)
      : null,
    health?.resources?.interactive_waiters ?? null,
    health?.resources?.background_waiters ?? null,
    health?.resources?.resource_wait_samples ?? null,
    health?.resources?.resource_wait_total_micros ?? null,
    health?.resources?.resource_wait_max_micros ?? null,
    health?.resources?.resource_wait_timeouts ?? null,
    pools.processing_memory?.used_bytes ?? null,
    pools.inflight_media?.used_bytes ?? null,
    pools.thumbnail_decode?.used ?? null,
    pools.archive_stream?.used ?? null,
    pools.scan_io?.used ?? null,
    pools.search_writer?.used ?? null,
    qmediasync.stream_requests ?? null,
    qmediasync.stream_range_requests ?? null,
    qmediasync.stream_successes ?? null,
    qmediasync.stream_failures ?? null,
    qmediasync.stream_response_bytes ?? null,
    qmediasync.cache_hits ?? null,
    qmediasync.cache_misses ?? null,
    qmediasync.cache_downloads ?? null,
    qmediasync.cache_download_bytes ?? null,
    qmediasync.cache_quota_rejections ?? null,
    qmediasync.cache_usage_rescans ?? null,
  ];
  appendFileSync(output, `${row.map(csvValue).join(",")}\n`, "utf8");
  previousCpu = currentCpu;
  previousDisk = currentDisk;
  previousAt = now;
}

console.log(
  JSON.stringify({
    output,
    samples: sampleCount,
    health_failures: healthFailures,
    block_device: blockDevice,
  }),
);
if (healthUrl && healthFailures === sampleCount) process.exitCode = 1;
