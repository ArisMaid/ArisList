import { closeSync, openSync, readFileSync, readdirSync, readSync, statSync } from "node:fs";
import { DatabaseSync } from "node:sqlite";
import { extname, join, relative } from "node:path";

// Version 5 adds tracked read-snapshot lifetime columns to the SQLite/runtime
// system sample. Keep this explicit so a runner never appends an older row
// to an older artifact with a different column layout.
export const PERF_ARTIFACT_VERSION = 5;

// One schema is shared by the baseline initializer and the live sampler.
// Keeping the order here prevents a later metric addition from silently
// shifting values into the wrong CSV column.
export const SYSTEM_SAMPLE_COLUMNS = Object.freeze([
  "timestamp",
  "memory_current_bytes",
  "memory_peak_bytes",
  "memory_oom_events",
  "cgroup_memory_limit_bytes",
  "cgroup_cpu_quota_micros",
  "cgroup_cpu_period_micros",
  "cgroup_cpu_limit_cores",
  "cpu_percent",
  "cpu_frequency_mhz",
  "cpu_temperature_c",
  "iowait_percent",
  "device_await_ms",
  "device_queue",
  "sqlite_wal_bytes",
  "sqlite_pool_size",
  "sqlite_idle_connections",
  "sqlite_active_connections",
  "sqlite_pool_saturated",
  "sqlite_pool_checkout_samples",
  "sqlite_pool_checkout_idle_total_micros",
  "sqlite_pool_checkout_idle_max_micros",
  "sqlite_pool_connections_opened",
  "sqlite_pool_tracked_acquire_samples",
  "sqlite_pool_tracked_acquire_wait_total_micros",
  "sqlite_pool_tracked_acquire_wait_max_micros",
  "sqlite_pool_acquire_timeouts",
  "sqlite_pool_acquire_errors",
  "sqlite_busy_errors",
  "sqlite_read_snapshot_active",
  "sqlite_read_snapshot_samples",
  "sqlite_read_snapshot_completed",
  "sqlite_read_snapshot_hold_total_micros",
  "sqlite_read_snapshot_hold_max_micros",
  "sqlite_read_snapshot_oldest_active_micros",
  "sqlite_read_snapshot_implicit_rollbacks",
  "sqlite_writer_queue_depth",
  "sqlite_writer_queue_bytes",
  "sqlite_writer_active",
  "sqlite_writer_active_bytes",
  "sqlite_writer_acquire_samples",
  "sqlite_writer_acquire_wait_total_micros",
  "sqlite_writer_acquire_wait_max_micros",
  "sqlite_writer_completed",
  "sqlite_writer_hold_total_micros",
  "sqlite_writer_hold_max_micros",
  "sqlite_wal_checkpoint_attempts",
  "sqlite_wal_checkpoint_successes",
  "sqlite_wal_checkpoint_failures",
  "sqlite_wal_checkpoint_busy_pages",
  "sqlite_wal_checkpoint_log_pages",
  "sqlite_wal_checkpoint_checkpointed_pages",
  "sqlite_wal_checkpoint_duration_micros",
  "jobs_worker_limit",
  "jobs_active",
  "jobs_maintenance_active",
  "jobs_maintenance_wait_samples",
  "jobs_maintenance_wait_total_micros",
  "jobs_maintenance_wait_max_micros",
  "jobs_completed",
  "jobs_failed",
  "jobs_claim_errors",
  "outbox_pending",
  "outbox_lag_revisions",
  "interactive_waiters",
  "background_waiters",
  "resource_wait_samples",
  "resource_wait_total_micros",
  "resource_wait_max_micros",
  "resource_wait_timeouts",
  "processing_memory_used_bytes",
  "inflight_media_used_bytes",
  "thumbnail_workers_used",
  "archive_workers_used",
  "scan_workers_used",
  "search_writer_used",
  "qms_stream_requests",
  "qms_stream_range_requests",
  "qms_stream_successes",
  "qms_stream_failures",
  "qms_stream_response_bytes",
  "qms_cache_hits",
  "qms_cache_misses",
  "qms_cache_downloads",
  "qms_cache_download_bytes",
  "qms_cache_quota_rejections",
  "qms_cache_usage_rescans",
]);

/**
 * Parse the bounded, machine-readable lines emitted by the large-scale Rust
 * acceptance tests. Cargo's surrounding output is intentionally ignored;
 * callers can retain a hash and a separate raw log without putting compiler
 * noise into the structured artifact.
 */
export function parseLargeScaleGateOutput(text) {
  if (typeof text !== "string") throw new TypeError("large-scale gate output must be text");
  const inventoryMatch = text.match(
    /synthetic inventory rows=(\d+) elapsed_ms=(\d+) max_batch_rows=(\d+) max_batch_bytes=(\d+)/u,
  );
  const inventory = inventoryMatch
    ? {
        rows: Number(inventoryMatch[1]),
        elapsed_ms: Number(inventoryMatch[2]),
        max_batch_rows: Number(inventoryMatch[3]),
        max_batch_bytes: Number(inventoryMatch[4]),
      }
    : null;

  const derivatives = [];
  const parseErrors = [];
  for (const line of text.split(/\r?\n/u)) {
    const marker = "DERIVATIVE_LEDGER_GATE=";
    const markerIndex = line.indexOf(marker);
    if (markerIndex < 0) continue;
    const encoded = line.slice(markerIndex + marker.length).trim();
    try {
      const value = JSON.parse(encoded);
      if (!value || typeof value !== "object" || Array.isArray(value)) {
        throw new TypeError("derivative gate payload must be an object");
      }
      derivatives.push(value);
    } catch (error) {
      parseErrors.push(String(error?.message ?? error).slice(0, 512));
    }
  }
  return { inventory, derivatives, parse_errors: parseErrors };
}

/**
 * Evaluate scale-test process results without treating missing output as a
 * pass. These tests prove bounded batch/query shapes and database integrity,
 * not N100 latency.
 */
export function evaluateLargeScaleGateRuns(
  runs,
  { requireInventory = true, derivativeRows = [700_000, 1_400_000] } = {},
) {
  if (!Array.isArray(runs)) throw new TypeError("large-scale runs must be an array");
  const checks = [];
  const passedRun = (run) => run && run.status === "passed" && Number(run.exit_code) === 0;

  if (requireInventory) {
    const run = runs.find((candidate) => candidate.id === "inventory-700000");
    const parsed = run?.parsed?.inventory;
    const failures = [];
    if (!run) failures.push("inventory-700000 run is missing");
    else if (!passedRun(run)) failures.push("inventory-700000 process did not pass");
    if (!parsed) failures.push("inventory metrics are missing");
    else {
      if (parsed.rows !== 700_000) failures.push(`inventory rows=${parsed.rows} != 700000`);
      if (!(parsed.max_batch_rows > 0 && parsed.max_batch_rows <= 1024)) {
        failures.push(`inventory max_batch_rows=${parsed.max_batch_rows} is outside 1..1024`);
      }
      if (!(parsed.max_batch_bytes > 0 && parsed.max_batch_bytes <= 8 * 1024 * 1024)) {
        failures.push(`inventory max_batch_bytes=${parsed.max_batch_bytes} is outside bounded range`);
      }
    }
    checks.push({
      id: "inventory-700000-fixed-batches",
      status: failures.length ? (run && parsed ? "failed" : "incomplete") : "passed",
      failures,
    });
  }

  for (const expectedRows of derivativeRows) {
    const id = `derivative-${expectedRows}`;
    const run = runs.find((candidate) => candidate.id === id);
    const parsed = run?.parsed?.derivative;
    const failures = [];
    if (!run) failures.push(`${id} run is missing`);
    else if (!passedRun(run)) failures.push(`${id} process did not pass`);
    if (!parsed) failures.push("derivative metrics are missing");
    else {
      if (Number(parsed.rows) !== expectedRows) {
        failures.push(`derivative rows=${parsed.rows} != ${expectedRows}`);
      }
      if (parsed.integrity_check !== "ok") failures.push("integrity_check is not ok");
      if (
        !Array.isArray(parsed.query_plan) ||
        !parsed.query_plan.some((entry) => String(entry).includes("idx_derivatives_eviction_lru"))
      ) {
        failures.push("eviction query plan does not use idx_derivatives_eviction_lru");
      }
      const after = Number(parsed.capacity_after?.bytes);
      const lowWatermark = Number(parsed.low_watermark_bytes);
      if (!(Number.isFinite(after) && Number.isFinite(lowWatermark))) {
        failures.push("capacity bytes are missing or non-finite");
      } else if (after > lowWatermark) {
        failures.push(`capacity_after.bytes=${after} exceeds low watermark ${lowWatermark}`);
      }
    }
    checks.push({
      id,
      status: failures.length ? (run && parsed ? "failed" : "incomplete") : "passed",
      failures,
    });
  }

  const status = checks.some((check) => check.status === "incomplete")
    ? "incomplete"
    : checks.some((check) => check.status === "failed")
      ? "failed"
      : "passed";
  return {
    version: 1,
    profile: "development-scale",
    scope: "inventory-derivative",
    status,
    checks,
  };
}

function asNumber(value) {
  if (typeof value === "bigint") return Number(value);
  return value == null ? 0 : Number(value);
}

export function percentile(values, quantile) {
  if (!values.length) return null;
  if (!(quantile > 0 && quantile <= 1)) {
    throw new RangeError("quantile must be in (0, 1]");
  }
  const sorted = values.map(Number).sort((left, right) => left - right);
  return sorted[Math.max(0, Math.ceil(sorted.length * quantile) - 1)];
}

function metricSummary(records, field) {
  const values = records
    .map((record) => record[field])
    .filter((value) => Number.isFinite(value))
    .map(Number);
  return {
    p50: percentile(values, 0.5),
    p95: percentile(values, 0.95),
    p99: percentile(values, 0.99),
    max: values.length ? Math.max(...values) : null,
  };
}

export function summarizeScenarioRecords(records, generatedAt = new Date().toISOString()) {
  const groups = new Map();
  for (const record of records) {
    if (!record || typeof record.scenario !== "string" || !record.scenario.trim()) {
      throw new TypeError("every scenario record must have a non-empty scenario");
    }
    const key = JSON.stringify([
      record.scenario,
      record.warm ?? "unspecified",
      Number(record.concurrency ?? 1),
    ]);
    const group = groups.get(key) ?? [];
    group.push(record);
    groups.set(key, group);
  }

  const summaries = [...groups.entries()]
    .map(([key, group]) => {
      const [scenario, warm, concurrency] = JSON.parse(key);
      const succeeded = group.filter(
        (record) => record.error == null && Number(record.status) >= 200 && Number(record.status) < 400,
      ).length;
      return {
        scenario,
        warm,
        concurrency,
        requests: group.length,
        succeeded,
        failed: group.length - succeeded,
        status_503: group.filter((record) => Number(record.status) === 503).length,
        bytes: group.reduce((sum, record) => sum + asNumber(record.bytes), 0),
        ttfb_ms: metricSummary(group, "ttfb_ms"),
        total_ms: metricSummary(group, "total_ms"),
      };
    })
    .sort(
      (left, right) =>
        left.scenario.localeCompare(right.scenario) ||
        String(left.warm).localeCompare(String(right.warm)) ||
        left.concurrency - right.concurrency,
    );

  return {
    artifact_version: PERF_ARTIFACT_VERSION,
    status: records.length ? "complete" : "not-run",
    generated_at: generatedAt,
    record_count: records.length,
    groups: summaries,
  };
}

function nestedValue(value, path) {
  return path.split(".").reduce((current, key) => current?.[key], value);
}

function finiteThresholds(value, label) {
  if (value == null) return {};
  if (typeof value !== "object" || Array.isArray(value)) {
    throw new TypeError(`${label} must be an object`);
  }
  for (const [path, threshold] of Object.entries(value)) {
    if (!path || !Number.isFinite(threshold)) {
      throw new TypeError(`${label}.${path || "<empty>"} must be a finite number`);
    }
  }
  return value;
}

function scenarioGateKey(gate) {
  return [gate.scenario, gate.warm ?? "unspecified", Number(gate.concurrency ?? 1)];
}

function sameScenarioGroup(group, gate) {
  const [scenario, warm, concurrency] = scenarioGateKey(gate);
  return (
    group.scenario === scenario &&
    group.warm === warm &&
    Number(group.concurrency) === concurrency
  );
}

/**
 * Evaluate declarative latency/error thresholds against a scenario summary.
 * Missing groups or too few samples are "incomplete", while observed values
 * outside a threshold are "failed". This distinction prevents prepared or
 * partial artifacts from being reported as a passed hardware Gate.
 */
export function evaluateScenarioGates(summary, configuration) {
  if (!configuration || typeof configuration !== "object" || Array.isArray(configuration)) {
    throw new TypeError("gate configuration must be an object");
  }
  if (!Array.isArray(configuration.scenarios) || !configuration.scenarios.length) {
    throw new TypeError("gate configuration must contain at least one scenario");
  }
  const checks = configuration.scenarios.map((gate, index) => {
    if (!gate || typeof gate !== "object" || typeof gate.scenario !== "string") {
      throw new TypeError(`scenarios[${index}] must have a scenario string`);
    }
    const id = gate.id ?? `${gate.scenario}:${gate.warm ?? "unspecified"}:${gate.concurrency ?? 1}`;
    const minimum = finiteThresholds(gate.minimum, `scenarios[${index}].minimum`);
    const maximum = finiteThresholds(gate.maximum, `scenarios[${index}].maximum`);
    const group = summary.groups.find((candidate) => sameScenarioGroup(candidate, gate));
    if (!group) {
      return {
        id,
        status: "incomplete",
        selector: {
          scenario: gate.scenario,
          warm: gate.warm ?? "unspecified",
          concurrency: Number(gate.concurrency ?? 1),
        },
        failures: ["matching scenario group is missing"],
      };
    }

    const incomplete = [];
    const failures = [];
    for (const [path, threshold] of Object.entries(minimum)) {
      const observed = nestedValue(group, path);
      if (!Number.isFinite(observed)) {
        incomplete.push(`${path} is missing or non-finite`);
      } else if (observed < threshold) {
        incomplete.push(`${path}=${observed} is below required ${threshold}`);
      }
    }
    for (const [path, threshold] of Object.entries(maximum)) {
      const observed = nestedValue(group, path);
      if (!Number.isFinite(observed)) {
        incomplete.push(`${path} is missing or non-finite`);
      } else if (observed > threshold) {
        failures.push(`${path}=${observed} exceeds ${threshold}`);
      }
    }
    return {
      id,
      status: incomplete.length ? "incomplete" : failures.length ? "failed" : "passed",
      selector: {
        scenario: group.scenario,
        warm: group.warm,
        concurrency: group.concurrency,
      },
      observed: group,
      failures: [...incomplete, ...failures],
    };
  });
  const status = checks.some((check) => check.status === "incomplete")
    ? "incomplete"
    : checks.some((check) => check.status === "failed")
      ? "failed"
      : "passed";
  return {
    version: 1,
    profile: configuration.profile ?? "unnamed",
    scope: configuration.scope ?? "http-scenarios",
    status,
    checks,
  };
}

function runtimeDelta(before, after, field) {
  const left = Number(before?.[field]);
  const right = Number(after?.[field]);
  return Number.isFinite(left) && Number.isFinite(right) ? right - left : null;
}

/**
 * Check the process-local qmediasync counters around a bounded media-stream
 * scenario.  This is intentionally independent of HTTP latency thresholds:
 * a fast 200 response that ignored a requested Range header is not a passing
 * audio-startup result, and a slow request with an unbounded cache scan should
 * remain diagnosable in the same artifact.
 */
export function evaluateQmsRuntimeEvidence(
  before,
  after,
  { expectedRequests, expectedRangeRequests = 0, requirePartial = false, maxFailures = 0 } = {},
) {
  const fields = [
    "stream_requests",
    "stream_range_requests",
    "stream_full_requests",
    "stream_successes",
    "stream_partial_responses",
    "stream_unsatisfiable_responses",
    "stream_failures",
    "stream_response_bytes",
    "cache_hits",
    "cache_misses",
    "cache_not_modified",
    "cache_downloads",
    "cache_download_bytes",
    "cache_quota_rejections",
    "cache_usage_rescans",
  ];
  const missing = [before, after].flatMap((snapshot) =>
    fields.filter((field) => !Number.isFinite(Number(snapshot?.[field]))),
  );
  const delta = Object.fromEntries(fields.map((field) => [field, runtimeDelta(before, after, field)]));
  const checks = [
    {
      id: "qms-runtime-metrics-present",
      status: missing.length ? "incomplete" : "passed",
      failures: missing.length
        ? [`missing or non-finite metrics: ${[...new Set(missing)].join(", ")}`]
        : [],
    },
  ];

  const requestFailures = [];
  if (Number.isSafeInteger(expectedRequests) && delta.stream_requests !== expectedRequests) {
    requestFailures.push(
      `stream_requests delta must be ${expectedRequests}, got ${delta.stream_requests}`,
    );
  }
  if (delta.stream_range_requests !== expectedRangeRequests) {
    requestFailures.push(
      `stream_range_requests delta must be ${expectedRangeRequests}, got ${delta.stream_range_requests}`,
    );
  }
  if (delta.stream_failures > maxFailures) {
    requestFailures.push(
      `stream_failures delta ${delta.stream_failures} exceeds ${maxFailures}`,
    );
  }
  checks.push({
    id: "qms-stream-request-contract",
    status: requestFailures.length ? "failed" : "passed",
    failures: requestFailures,
  });

  const responseFailures = [];
  if (delta.stream_successes + delta.stream_unsatisfiable_responses + delta.stream_failures < 0) {
    responseFailures.push("stream response counters moved backwards");
  }
  if (requirePartial && delta.stream_partial_responses < expectedRangeRequests) {
    responseFailures.push(
      `partial responses ${delta.stream_partial_responses} are below ${expectedRangeRequests}`,
    );
  }
  if (delta.stream_successes > 0 && delta.stream_response_bytes <= 0) {
    responseFailures.push("successful stream requests produced no response bytes");
  }
  checks.push({
    id: "qms-stream-response-contract",
    status: responseFailures.length ? "failed" : "passed",
    failures: responseFailures,
  });

  const cacheFailures = [];
  if (delta.cache_downloads > delta.cache_misses) {
    cacheFailures.push(
      `cache_downloads ${delta.cache_downloads} exceeds cache_misses ${delta.cache_misses}`,
    );
  }
  if (delta.cache_downloads > 0 && delta.cache_download_bytes <= 0) {
    cacheFailures.push("cache downloads were recorded without downloaded bytes");
  }
  if (delta.cache_quota_rejections > 0) {
    cacheFailures.push(`cache quota rejections observed: ${delta.cache_quota_rejections}`);
  }
  checks.push({
    id: "qms-cache-contract",
    status: cacheFailures.length ? "failed" : "passed",
    failures: cacheFailures,
  });

  const status = checks.some((check) => check.status === "incomplete")
    ? "incomplete"
    : checks.some((check) => check.status === "failed")
      ? "failed"
      : "passed";
  return { status, delta, checks };
}

export function evaluateR1gRuntimeEvidence(before, afterFirstTriplet, afterAll) {
  const fields = [
    "reader_opens",
    "candidate_hits",
    "candidate_misses",
    "candidate_coalesced",
    "candidate_evictions",
  ];
  const firstTripletDelta = Object.fromEntries(
    fields.map((field) => [field, runtimeDelta(before, afterFirstTriplet, field)]),
  );
  const totalDelta = Object.fromEntries(
    fields.map((field) => [field, runtimeDelta(before, afterAll, field)]),
  );
  const checks = [];
  const metricValues = [before, afterFirstTriplet, afterAll].flatMap((snapshot) =>
    [
      "readers",
      "reader_opens",
      "candidate_entries",
      "candidate_ids",
      "candidate_hits",
      "candidate_misses",
      "candidate_coalesced",
      "candidate_evictions",
      "candidate_cache_max_entries",
      "candidate_cache_max_ids",
    ].map((field) => snapshot?.[field]),
  );
  if (metricValues.some((value) => !Number.isFinite(Number(value)))) {
    checks.push({
      id: "runtime-metrics-present",
      status: "incomplete",
      failures: ["one or more search_runtime metrics are missing or non-finite"],
    });
  } else {
    checks.push({ id: "runtime-metrics-present", status: "passed", failures: [] });
  }

  const shared = [];
  if (firstTripletDelta.candidate_misses !== 1) {
    shared.push(
      `first triplet candidate_misses delta must be 1, got ${firstTripletDelta.candidate_misses}`,
    );
  }
  const reused =
    Number(firstTripletDelta.candidate_hits ?? 0) +
    Number(firstTripletDelta.candidate_coalesced ?? 0);
  if (reused < 2) shared.push(`first triplet hit+coalesced delta must be at least 2, got ${reused}`);
  checks.push({
    id: "candidate-singleflight",
    status: shared.length ? "failed" : "passed",
    failures: shared,
  });

  const readerFailures = [];
  if (Number(totalDelta.reader_opens) > 1) {
    readerFailures.push(`reader_opens delta exceeds 1: ${totalDelta.reader_opens}`);
  }
  if (Number(afterAll?.readers) !== 1) {
    readerFailures.push(`expected exactly one resident reader, got ${afterAll?.readers}`);
  }
  checks.push({
    id: "reader-reuse",
    status: readerFailures.length ? "failed" : "passed",
    failures: readerFailures,
  });

  const capacityFailures = [];
  if (Number(afterAll?.candidate_entries) > Number(afterAll?.candidate_cache_max_entries)) {
    capacityFailures.push("candidate entry capacity exceeded");
  }
  if (Number(afterAll?.candidate_ids) > Number(afterAll?.candidate_cache_max_ids)) {
    capacityFailures.push("candidate ID capacity exceeded");
  }
  checks.push({
    id: "candidate-capacity",
    status: capacityFailures.length ? "failed" : "passed",
    failures: capacityFailures,
  });

  const status = checks.some((check) => check.status === "incomplete")
    ? "incomplete"
    : checks.some((check) => check.status === "failed")
      ? "failed"
      : "passed";
  return { status, first_triplet_delta: firstTripletDelta, total_delta: totalDelta, checks };
}

export function evaluateFacetCacheRuntimeEvidence(
  before,
  afterConcurrent,
  afterWarm,
  concurrency,
  warmRequests,
) {
  const fields = ["facet_hits", "facet_misses", "facet_coalesced", "facet_evictions"];
  const concurrentDelta = Object.fromEntries(
    fields.map((field) => [field, runtimeDelta(before, afterConcurrent, field)]),
  );
  const warmDelta = Object.fromEntries(
    fields.map((field) => [field, runtimeDelta(afterConcurrent, afterWarm, field)]),
  );
  const checks = [];
  const required = [
    "facet_entries",
    "facet_items",
    "facet_hits",
    "facet_misses",
    "facet_coalesced",
    "facet_evictions",
    "facet_cache_max_entries",
    "facet_cache_max_items",
    "facet_cache_ttl_millis",
  ];
  const missing = [before, afterConcurrent, afterWarm].flatMap((snapshot) =>
    required.filter((field) => !Number.isFinite(Number(snapshot?.[field]))),
  );
  checks.push({
    id: "facet-runtime-metrics-present",
    status: missing.length ? "incomplete" : "passed",
    failures: missing.length ? [`missing or non-finite metrics: ${[...new Set(missing)].join(", ")}`] : [],
  });

  const singleflightFailures = [];
  if (concurrentDelta.facet_misses !== 1) {
    singleflightFailures.push(
      `concurrent facet_misses delta must be 1, got ${concurrentDelta.facet_misses}`,
    );
  }
  if (Number(concurrentDelta.facet_coalesced) < concurrency - 1) {
    singleflightFailures.push(
      `concurrent facet_coalesced delta must be at least ${concurrency - 1}, got ${concurrentDelta.facet_coalesced}`,
    );
  }
  checks.push({
    id: "facet-singleflight",
    status: singleflightFailures.length ? "failed" : "passed",
    failures: singleflightFailures,
  });

  const warmFailures = [];
  if (Number(warmDelta.facet_hits) < warmRequests) {
    warmFailures.push(
      `warm facet_hits delta must be at least ${warmRequests}, got ${warmDelta.facet_hits}`,
    );
  }
  if (Number(warmDelta.facet_misses) !== 0) {
    warmFailures.push(`warm facet_misses delta must be 0, got ${warmDelta.facet_misses}`);
  }
  checks.push({
    id: "facet-warm-hits",
    status: warmFailures.length ? "failed" : "passed",
    failures: warmFailures,
  });

  const capacityFailures = [];
  if (Number(afterWarm?.facet_entries) > Number(afterWarm?.facet_cache_max_entries)) {
    capacityFailures.push("facet entry capacity exceeded");
  }
  if (Number(afterWarm?.facet_items) > Number(afterWarm?.facet_cache_max_items)) {
    capacityFailures.push("facet item capacity exceeded");
  }
  checks.push({
    id: "facet-cache-capacity",
    status: capacityFailures.length ? "failed" : "passed",
    failures: capacityFailures,
  });

  const status = checks.some((check) => check.status === "incomplete")
    ? "incomplete"
    : checks.some((check) => check.status === "failed")
      ? "failed"
      : "passed";
  return {
    status,
    concurrent_delta: concurrentDelta,
    warm_delta: warmDelta,
    checks,
  };
}

export function evaluateFacetBitmapRuntimeEvidence(before, after, requests) {
  const numericFields = [
    "catalog_revision",
    "works",
    "tags",
    "associations",
    "estimated_bytes",
    "builds",
    "build_failures",
    "queries",
    "fallbacks",
    "scope_rejections",
    "delta_refreshes",
    "delta_work_ids",
    "max_works",
    "max_tags",
    "max_associations",
    "max_estimated_bytes",
  ];
  const missing = [before, after].flatMap((snapshot) =>
    numericFields.filter((field) => !Number.isFinite(Number(snapshot?.[field]))),
  );
  const checks = [
    {
      id: "facet-bitmap-runtime-metrics-present",
      status: missing.length ? "incomplete" : "passed",
      failures: missing.length
        ? [`missing or non-finite metrics: ${[...new Set(missing)].join(", ")}`]
        : [],
    },
  ];
  const delta = Object.fromEntries(
    [
      "builds",
      "build_failures",
      "queries",
      "fallbacks",
      "scope_rejections",
      "delta_refreshes",
      "delta_work_ids",
    ].map((field) => [field, runtimeDelta(before, after, field)]),
  );

  const readinessFailures = [];
  if (before?.state !== "ready" || after?.state !== "ready") {
    readinessFailures.push(`bitmap state must remain ready, got ${before?.state} -> ${after?.state}`);
  }
  if (Number(before?.catalog_revision) !== Number(after?.catalog_revision)) {
    readinessFailures.push(
      `catalog revision changed during the Gate: ${before?.catalog_revision} -> ${after?.catalog_revision}`,
    );
  }
  checks.push({
    id: "facet-bitmap-stable-ready",
    status: readinessFailures.length ? "failed" : "passed",
    failures: readinessFailures,
  });

  const routingFailures = [];
  if (Number(delta.queries) < requests) {
    routingFailures.push(`bitmap query delta must be at least ${requests}, got ${delta.queries}`);
  }
  if (Number(delta.fallbacks) !== 0) {
    routingFailures.push(`bitmap fallback delta must be 0, got ${delta.fallbacks}`);
  }
  if (Number(delta.scope_rejections) !== 0) {
    routingFailures.push(`bitmap scope rejection delta must be 0, got ${delta.scope_rejections}`);
  }
  if (Number(delta.build_failures) !== 0) {
    routingFailures.push(`bitmap build failure delta must be 0, got ${delta.build_failures}`);
  }
  checks.push({
    id: "facet-bitmap-authoritative-routing",
    status: routingFailures.length ? "failed" : "passed",
    failures: routingFailures,
  });

  const capacityFailures = [];
  for (const [value, maximum, label] of [
    [after?.works, after?.max_works, "works"],
    [after?.tags, after?.max_tags, "tags"],
    [after?.associations, after?.max_associations, "associations"],
    [after?.estimated_bytes, after?.max_estimated_bytes, "estimated bytes"],
  ]) {
    if (Number(value) > Number(maximum)) {
      capacityFailures.push(`bitmap ${label} capacity exceeded: ${value} > ${maximum}`);
    }
  }
  checks.push({
    id: "facet-bitmap-capacity",
    status: capacityFailures.length ? "failed" : "passed",
    failures: capacityFailures,
  });

  const status = checks.some((check) => check.status === "incomplete")
    ? "incomplete"
    : checks.some((check) => check.status === "failed")
      ? "failed"
      : "passed";
  return { status, delta, checks };
}

export function evaluateSearchShadowCanaryEvidence(
  beforeRuntime,
  afterRuntime,
  beforeShadow,
  afterShadow,
  records,
  beforeFacts,
  afterFacts,
) {
  const metricFields = [
    "canary_queries",
    "canary_id_mismatches",
    "canary_order_mismatches",
    "canary_failures",
    "canary_rejections",
  ];
  const missing = [beforeRuntime, afterRuntime].flatMap((snapshot) =>
    metricFields.filter((field) => !Number.isFinite(Number(snapshot?.[field]))),
  );
  const checks = [
    {
      id: "search-canary-runtime-metrics-present",
      status: missing.length ? "incomplete" : "passed",
      failures: missing.length
        ? [`missing or non-finite metrics: ${[...new Set(missing)].join(", ")}`]
        : [],
    },
  ];
  const delta = Object.fromEntries(
    metricFields.map((field) => [field, runtimeDelta(beforeRuntime, afterRuntime, field)]),
  );

  const readinessFailures = [];
  if (
    beforeShadow?.ready !== true ||
    afterShadow?.ready !== true ||
    beforeShadow?.status !== "ready" ||
    afterShadow?.status !== "ready"
  ) {
    readinessFailures.push(
      `shadow index must remain ready, got ${beforeShadow?.status}/${beforeShadow?.ready} -> ${afterShadow?.status}/${afterShadow?.ready}`,
    );
  }
  if (Number(beforeShadow?.applied_revision) !== Number(afterShadow?.applied_revision)) {
    readinessFailures.push(
      `shadow applied revision changed during corpus: ${beforeShadow?.applied_revision} -> ${afterShadow?.applied_revision}`,
    );
  }
  checks.push({
    id: "search-canary-stable-ready",
    status: readinessFailures.length ? "failed" : "passed",
    failures: readinessFailures,
  });

  const routingFailures = [];
  if (!Array.isArray(records) || !records.length) {
    routingFailures.push("fixed query corpus produced no records");
  }
  const failedRecords = Array.isArray(records)
    ? records.filter(
        (record) =>
          record?.error != null ||
          Number(record?.status) < 200 ||
          Number(record?.status) >= 400 ||
          record?.reader !== "shadow" ||
          record?.canary == null,
      )
    : [];
  if (failedRecords.length) {
    routingFailures.push(`${failedRecords.length} canary requests failed or bypassed shadow`);
  }
  if (Number(delta.canary_queries) !== Number(records?.length ?? 0)) {
    routingFailures.push(
      `canary query delta must equal corpus size ${records?.length ?? 0}, got ${delta.canary_queries}`,
    );
  }
  for (const field of ["canary_failures", "canary_rejections"]) {
    if (Number(delta[field]) !== 0) {
      routingFailures.push(`${field} delta must be 0, got ${delta[field]}`);
    }
  }
  checks.push({
    id: "search-canary-authoritative-routing",
    status: routingFailures.length ? "failed" : "passed",
    failures: routingFailures,
  });

  const comparisonFailures = [];
  const recordMismatches = Array.isArray(records)
    ? records.filter(
        (record) => record?.canary?.id_match !== true || record?.canary?.order_match !== true,
      ).length
    : 0;
  if (recordMismatches) comparisonFailures.push(`${recordMismatches} corpus queries mismatched`);
  for (const field of ["canary_id_mismatches", "canary_order_mismatches"]) {
    if (Number(delta[field]) !== 0) {
      comparisonFailures.push(`${field} delta must be 0, got ${delta[field]}`);
    }
  }
  checks.push({
    id: "search-canary-zero-diff",
    status: comparisonFailures.length ? "failed" : "passed",
    failures: comparisonFailures,
  });

  const factFields = [
    "catalog_revision",
    "shadow_applied_revision",
    "sqlite_work_count",
    "shadow_document_count",
    "shadow_unique_work_count",
    "missing_work_ids",
    "unexpected_work_ids",
    "duplicate_documents",
    "invalid_documents",
  ];
  const factMissing = [beforeFacts, afterFacts].flatMap((facts, index) => {
    if (!facts || typeof facts !== "object") return [`facts[${index}] is missing`];
    return [
      ...factFields
        .filter((field) => !Number.isFinite(Number(facts[field])))
        .map((field) => `facts[${index}].${field} is missing or non-finite`),
      ...(typeof facts.sqlite_ids_sha256 === "string" && facts.sqlite_ids_sha256.length === 64
        ? []
        : [`facts[${index}].sqlite_ids_sha256 is missing`]),
      ...(typeof facts.shadow_ids_sha256 === "string" && facts.shadow_ids_sha256.length === 64
        ? []
        : [`facts[${index}].shadow_ids_sha256 is missing`]),
    ];
  });
  const factFailures = [];
  if (!factMissing.length) {
    for (const [label, facts] of [
      ["before", beforeFacts],
      ["after", afterFacts],
    ]) {
      if (facts.status !== "passed") factFailures.push(`${label} fact status is ${facts.status}`);
      if (Number(facts.catalog_revision) !== Number(facts.shadow_applied_revision)) {
        factFailures.push(`${label} SQLite/shadow revisions differ`);
      }
      if (facts.sqlite_ids_sha256 !== facts.shadow_ids_sha256) {
        factFailures.push(`${label} SQLite/shadow ID hashes differ`);
      }
      for (const field of [
        "missing_work_ids",
        "unexpected_work_ids",
        "duplicate_documents",
        "invalid_documents",
      ]) {
        if (Number(facts[field]) !== 0) {
          factFailures.push(`${label} ${field} must be 0, got ${facts[field]}`);
        }
      }
    }
    if (Number(beforeFacts.catalog_revision) !== Number(afterFacts.catalog_revision)) {
      factFailures.push("SQLite catalog revision changed during corpus");
    }
    if (beforeFacts.sqlite_ids_sha256 !== afterFacts.sqlite_ids_sha256) {
      factFailures.push("SQLite work ID hash changed during corpus");
    }
    if (beforeFacts.shadow_ids_sha256 !== afterFacts.shadow_ids_sha256) {
      factFailures.push("shadow work ID hash changed during corpus");
    }
  }
  checks.push({
    id: "search-canary-sqlite-facts",
    status: factMissing.length ? "incomplete" : factFailures.length ? "failed" : "passed",
    failures: [...factMissing, ...factFailures],
  });

  const status = checks.some((check) => check.status === "incomplete")
    ? "incomplete"
    : checks.some((check) => check.status === "failed")
      ? "failed"
      : "passed";
  return { status, delta, checks };
}

export function evaluateSearchIncrementalReaderEvidence(beforeHealth, afterHealth, records) {
  const snapshots = [beforeHealth, afterHealth];
  const requiredSections = [
    "search_outbox",
    "search_shadow",
    "search_reconciliation",
    "search_features",
  ];
  const missing = snapshots.flatMap((health, index) =>
    requiredSections
      .filter((section) => !health || typeof health[section] !== "object")
      .map((section) => `health[${index}].${section} is missing`),
  );
  const checks = [
    {
      id: "search-incremental-health-present",
      status: missing.length ? "incomplete" : "passed",
      failures: missing,
    },
  ];

  const progressFailures = [];
  if (!missing.length) {
    for (const [label, health] of [
      ["before", beforeHealth],
      ["after", afterHealth],
    ]) {
      const outbox = health.search_outbox;
      const shadow = health.search_shadow;
      const reconciliation = health.search_reconciliation;
      const features = health.search_features;
      if (
        features.outbox_shadow !== true ||
        features.shadow_canary !== true ||
        features.incremental_reader !== true
      ) {
        progressFailures.push(
          `${label} search flags must all be enabled for this evidence run`,
        );
      }
      if (shadow.ready !== true || shadow.status !== "ready") {
        progressFailures.push(
          `${label} shadow index must be ready, got ${shadow.status}/${shadow.ready}`,
        );
      }
      if (reconciliation.cutover_armed !== true) {
        progressFailures.push(`${label} incremental reader cutover is not armed`);
      }
      if (reconciliation.status !== "passed") {
        progressFailures.push(
          `${label} persisted search reconciliation must be passed, got ${reconciliation.status}`,
        );
      }
      if (Number(outbox.pending) !== 0 || Number(outbox.revision_lag) !== 0) {
        progressFailures.push(
          `${label} outbox must be caught up, pending=${outbox.pending}, revision_lag=${outbox.revision_lag}`,
        );
      }
      if (Number(shadow.applied_revision) !== Number(outbox.catalog_revision)) {
        progressFailures.push(
          `${label} shadow/catalog revisions differ: ${shadow.applied_revision} != ${outbox.catalog_revision}`,
        );
      }
    }
    if (
      Number(beforeHealth.search_outbox.catalog_revision) !==
      Number(afterHealth.search_outbox.catalog_revision)
    ) {
      progressFailures.push("catalog revision changed during the production-reader corpus");
    }
  }
  checks.push({
    id: "search-incremental-progress-gate",
    status: missing.length ? "incomplete" : progressFailures.length ? "failed" : "passed",
    failures: progressFailures,
  });

  const routingFailures = [];
  if (!Array.isArray(records) || !records.length) {
    routingFailures.push("fixed query corpus produced no records");
  }
  const failedRecords = Array.isArray(records)
    ? records.filter(
        (record) =>
          record?.error != null ||
          Number(record?.status) < 200 ||
          Number(record?.status) >= 400 ||
          record?.reader !== "production" ||
          record?.rebuilt !== false,
      )
    : [];
  if (failedRecords.length) {
    routingFailures.push(
      `${failedRecords.length} requests failed, bypassed production routing, or rebuilt the index`,
    );
  }
  checks.push({
    id: "search-incremental-production-routing",
    status: routingFailures.length ? "failed" : "passed",
    failures: routingFailures,
  });

  const status = checks.some((check) => check.status === "incomplete")
    ? "incomplete"
    : checks.some((check) => check.status === "failed")
      ? "failed"
      : "passed";
  return { status, checks };
}

export function readJsonLines(path) {
  const text = readFileSync(path, "utf8");
  return text
    .split(/\r?\n/u)
    .map((line) => line.trim())
    .filter(Boolean)
    .map((line, index) => {
      try {
        return JSON.parse(line);
      } catch (error) {
        throw new Error(`${path}:${index + 1}: invalid JSON: ${error.message}`);
      }
    });
}

function tableExists(database, name) {
  return Boolean(
    database
      .prepare("SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = ?1")
      .get(name),
  );
}

function tableColumns(database, name) {
  if (!tableExists(database, name)) return new Set();
  return new Set(
    database
      .prepare(`PRAGMA table_info(${name})`)
      .all()
      .map((row) => String(row.name)),
  );
}

function distribution(values) {
  const numbers = values
    .map((value) => asNumber(value))
    .filter((value) => Number.isFinite(value) && value >= 0)
    .sort((left, right) => left - right);
  if (!numbers.length) {
    return {
      count: 0,
      total: 0,
      zero: 0,
      p50: 0,
      p95: 0,
      p99: 0,
      max: 0,
    };
  }
  return {
    count: numbers.length,
    total: numbers.reduce((sum, value) => sum + value, 0),
    zero: numbers.filter((value) => value === 0).length,
    p50: percentile(numbers, 0.5),
    p95: percentile(numbers, 0.95),
    p99: percentile(numbers, 0.99),
    max: numbers[numbers.length - 1],
  };
}

function groupDistribution(rows, keyName, valueName) {
  const grouped = new Map();
  for (const row of rows) {
    const key = String(row[keyName]);
    const values = grouped.get(key) ?? [];
    values.push(row[valueName]);
    grouped.set(key, values);
  }
  return [...grouped.entries()]
    .sort(([left], [right]) => left.localeCompare(right))
    .map(([kind, values]) => ({ kind, ...distribution(values) }));
}

function namedWorkDistribution(rows, totalName, zeroName) {
  return rows.map(({ kind, count, total, zero, ...rest }) => ({
    kind,
    works: count,
    [totalName]: total,
    [zeroName]: zero,
    ...rest,
  }));
}

function queryScalarOrNull(database, sql) {
  try {
    const row = database.prepare(sql).get();
    if (!row) return null;
    const value = Object.values(row)[0];
    return value == null ? null : asNumber(value);
  } catch {
    // Old database copies can legitimately predate an optional diagnostic
    // table/column.  A missing optional fact must remain explicit in the
    // artifact rather than turning a read-only baseline into a failure.
    return null;
  }
}

function collectLongTailFacts(database) {
  const worksColumns = tableColumns(database, "works");
  const activeWorkClause = worksColumns.has("deleted_at") ? "WHERE work.deleted_at IS NULL" : "";
  const assetsPerWorkByKind = tableExists(database, "works") && tableExists(database, "assets")
    ? groupDistribution(
        database
          .prepare(
            `SELECT work.kind AS kind, COUNT(asset.id) AS value
             FROM works AS work
             LEFT JOIN assets AS asset ON asset.work_id = work.id
             ${activeWorkClause}
             GROUP BY work.kind, work.id
             ORDER BY work.kind, work.id`,
          )
          .all(),
        "kind",
        "value",
      )
    : [];
  const tagsPerWorkByKind = tableExists(database, "works") && tableExists(database, "work_tags")
    ? groupDistribution(
        database
          .prepare(
            `SELECT work.kind AS kind, COUNT(work_tag.tag_id) AS value
             FROM works AS work
             LEFT JOIN work_tags AS work_tag ON work_tag.work_id = work.id
             ${activeWorkClause}
             GROUP BY work.kind, work.id
             ORDER BY work.kind, work.id`,
          )
          .all(),
        "kind",
        "value",
      )
    : [];

  const inventoryByKind = [];
  if (tableExists(database, "library_roots") && tableExists(database, "file_inventory")) {
    const inventoryColumns = tableColumns(database, "file_inventory");
    const sizeExpression = inventoryColumns.has("size")
      ? "COALESCE(SUM(COALESCE(inventory.size, 0)), 0)"
      : "0";
    const perRoot = database
      .prepare(
        `SELECT root.kind AS kind,
                root.id AS root_id,
                COUNT(inventory.relative_path) AS files,
                ${sizeExpression} AS bytes
         FROM library_roots AS root
         LEFT JOIN file_inventory AS inventory ON inventory.root_id = root.id
         GROUP BY root.kind, root.id
         ORDER BY root.kind, root.id`,
      )
      .all();
    const statusRows = inventoryColumns.has("status")
      ? database
          .prepare(
            `SELECT root.kind AS kind,
                    inventory.status AS status,
                    COUNT(*) AS files
             FROM library_roots AS root
             JOIN file_inventory AS inventory ON inventory.root_id = root.id
             GROUP BY root.kind, inventory.status
             ORDER BY root.kind, inventory.status`,
          )
          .all()
      : [];
    const workMaxRows = inventoryColumns.has("work_key")
      ? database
          .prepare(
            `SELECT kind, MAX(files) AS max_files_per_work
             FROM (
               SELECT root.kind AS kind,
                      inventory.root_id AS root_id,
                      inventory.work_key AS work_key,
                      COUNT(*) AS files
               FROM library_roots AS root
               JOIN file_inventory AS inventory ON inventory.root_id = root.id
               WHERE inventory.work_key IS NOT NULL AND inventory.work_key <> ''
               GROUP BY root.kind, inventory.root_id, inventory.work_key
             )
             GROUP BY kind
             ORDER BY kind`,
          )
          .all()
      : [];
    const byKind = new Map();
    for (const row of perRoot) {
      const kind = String(row.kind);
      const entry = byKind.get(kind) ?? {
        kind,
        roots: 0,
        files: 0,
        present_files: 0,
        missing_files: 0,
        total_bytes: 0,
        max_files_per_root: 0,
        max_files_per_work: 0,
      };
      entry.roots += 1;
      entry.files += asNumber(row.files);
      entry.total_bytes += asNumber(row.bytes);
      entry.max_files_per_root = Math.max(entry.max_files_per_root, asNumber(row.files));
      byKind.set(kind, entry);
    }
    for (const row of statusRows) {
      const entry = byKind.get(String(row.kind));
      if (!entry) continue;
      if (row.status === "present") entry.present_files += asNumber(row.files);
      if (row.status === "missing") entry.missing_files += asNumber(row.files);
    }
    for (const row of workMaxRows) {
      const entry = byKind.get(String(row.kind));
      if (entry) entry.max_files_per_work = asNumber(row.max_files_per_work);
    }
    inventoryByKind.push(...[...byKind.values()].sort((left, right) => left.kind.localeCompare(right.kind)));
  }

  const searchIndexes = tableExists(database, "search_index_state")
    ? database
        .prepare(
          `SELECT index_name, schema_version, baseline_revision, applied_revision,
                  indexed_documents, ready, status
           FROM search_index_state ORDER BY index_name`,
        )
        .all()
        .map((row) => ({
          index_name: String(row.index_name),
          schema_version: asNumber(row.schema_version),
          baseline_revision: asNumber(row.baseline_revision),
          applied_revision: asNumber(row.applied_revision),
          indexed_documents: asNumber(row.indexed_documents),
          ready: asNumber(row.ready) !== 0,
          status: String(row.status),
        }))
    : [];

  return {
    assets_per_work_by_kind: namedWorkDistribution(
      assetsPerWorkByKind,
      "total_assets",
      "zero_asset_works",
    ),
    tags_per_work_by_kind: namedWorkDistribution(
      tagsPerWorkByKind,
      "total_tags",
      "zero_tag_works",
    ),
    inventory_by_kind: inventoryByKind,
    deleted_works: worksColumns.has("deleted_at")
      ? queryScalarOrNull(database, "SELECT COUNT(*) FROM works WHERE deleted_at IS NOT NULL")
      : null,
    catalog_revision: tableExists(database, "catalog_state")
      ? queryScalarOrNull(database, "SELECT revision FROM catalog_state WHERE singleton = 1")
      : null,
    activity_revision: tableColumns(database, "catalog_state").has("activity_revision")
      ? queryScalarOrNull(database, "SELECT activity_revision FROM catalog_state WHERE singleton = 1")
      : null,
    search_indexes: searchIndexes,
  };
}

function count(database, table) {
  if (!tableExists(database, table)) return null;
  return asNumber(database.prepare(`SELECT COUNT(*) AS count FROM ${table}`).get().count);
}

function sidecarSize(path) {
  try {
    return statSync(path).size;
  } catch {
    return null;
  }
}

export function collectDatasetManifest(databasePath) {
  const database = new DatabaseSync(databasePath, { readOnly: true });
  try {
    database.exec("PRAGMA query_only = ON");
    const worksByKind = tableExists(database, "works")
      ? database
          .prepare("SELECT kind, COUNT(*) AS count FROM works GROUP BY kind ORDER BY kind")
          .all()
          .map((row) => ({ kind: row.kind, count: asNumber(row.count) }))
      : [];
    const assetsByRole = tableExists(database, "assets")
      ? database
          .prepare(
            "SELECT role, COUNT(*) AS count, COALESCE(SUM(size), 0) AS bytes FROM assets GROUP BY role ORDER BY role",
          )
          .all()
          .map((row) => ({ role: row.role, count: asNumber(row.count), bytes: asNumber(row.bytes) }))
      : [];
    const maxAssetsPerWork = tableExists(database, "assets")
      ? asNumber(
          database
            .prepare(
              "SELECT COALESCE(MAX(asset_count), 0) AS count FROM (SELECT COUNT(*) AS asset_count FROM assets GROUP BY work_id)",
            )
            .get().count,
        )
      : null;
    const schema = tableExists(database, "schema_migrations")
      ? database
          .prepare(
            "SELECT COALESCE(MAX(version), 0) AS version, COUNT(*) AS applied FROM schema_migrations",
          )
          .get()
      : { version: 0, applied: 0 };
    const outbox = tableExists(database, "search_outbox")
      ? database
          .prepare(
            `SELECT
               COUNT(*) AS total,
               SUM(CASE WHEN committed_at IS NULL THEN 1 ELSE 0 END) AS pending,
               SUM(CASE WHEN claimed_at IS NOT NULL AND committed_at IS NULL THEN 1 ELSE 0 END) AS claimed
             FROM search_outbox`,
          )
          .get()
      : null;
    const longTail = collectLongTailFacts(database);

    return {
      status: "captured",
      database_path: databasePath,
      database_files: {
        database_bytes: sidecarSize(databasePath),
        wal_bytes: sidecarSize(`${databasePath}-wal`),
        shm_bytes: sidecarSize(`${databasePath}-shm`),
      },
      schema: {
        version: asNumber(schema.version),
        applied_migrations: asNumber(schema.applied),
      },
      counts: {
        works: count(database, "works"),
        assets: count(database, "assets"),
        tags: count(database, "tags"),
        work_tags: count(database, "work_tags"),
        file_inventory: count(database, "file_inventory"),
        derivatives: count(database, "derivatives"),
      },
      works_by_kind: worksByKind,
      assets_by_role: assetsByRole,
      max_assets_per_work: maxAssetsPerWork,
      long_tail: longTail,
      search_outbox: outbox
        ? {
            total: asNumber(outbox.total),
            pending: asNumber(outbox.pending),
            claimed: asNumber(outbox.claimed),
          }
        : null,
    };
  } finally {
    database.close();
  }
}

const MEDIA_MANIFEST_ERROR_LIMIT = 32;
const DEFAULT_MEDIA_INSPECTION_LIMIT = 256;
const DEFAULT_MEDIA_HEADER_BYTES = 1024 * 1024;
const DEFAULT_MEDIA_ARCHIVE_BYTES = 64 * 1024 * 1024;
const MAX_MEDIA_INSPECTION_LIMIT = 10_000;
const MAX_MEDIA_HEADER_BYTES = 16 * 1024 * 1024;
const MAX_MEDIA_ARCHIVE_BYTES = 256 * 1024 * 1024;
const MEDIA_IMAGE_EXTENSIONS = new Set([".jpg", ".jpeg", ".png", ".gif", ".webp"]);
const MEDIA_ARCHIVE_EXTENSIONS = new Set([".zip", ".cbz", ".epub"]);

function normalizedExtension(name) {
  const extension = extname(name).toLowerCase();
  return extension || "<none>";
}

function portableRelativePath(rootPath, absolutePath) {
  return relative(rootPath, absolutePath).replaceAll("\\", "/");
}

function readFileSlice(path, offset, length) {
  if (length <= 0) return Buffer.alloc(0);
  const descriptor = openSync(path, "r");
  try {
    const buffer = Buffer.alloc(length);
    const bytesRead = readSync(descriptor, buffer, 0, length, offset);
    return bytesRead === length ? buffer : buffer.subarray(0, bytesRead);
  } finally {
    closeSync(descriptor);
  }
}

function isJpegSofMarker(marker) {
  return (
    (marker >= 0xc0 && marker <= 0xc3) ||
    (marker >= 0xc5 && marker <= 0xc7) ||
    (marker >= 0xc9 && marker <= 0xcb) ||
    (marker >= 0xcd && marker <= 0xcf)
  );
}

function parseJpegDimensions(bytes) {
  if (bytes.length < 4 || bytes[0] !== 0xff || bytes[1] !== 0xd8) return null;
  let offset = 2;
  while (offset + 3 < bytes.length) {
    while (offset < bytes.length && bytes[offset] === 0xff) offset += 1;
    if (offset >= bytes.length) return null;
    const marker = bytes[offset++];
    if (marker === 0xd9 || marker === 0xda) return null;
    if (marker >= 0xd0 && marker <= 0xd7) continue;
    if (offset + 2 > bytes.length) return null;
    const segmentLength = bytes.readUInt16BE(offset);
    if (segmentLength < 2 || offset + segmentLength > bytes.length) return null;
    if (isJpegSofMarker(marker) && segmentLength >= 7) {
      return {
        width: bytes.readUInt16BE(offset + 3),
        height: bytes.readUInt16BE(offset + 5),
      };
    }
    offset += segmentLength;
  }
  return null;
}

function parseImageDimensions(path, extension, headerBytes) {
  const stats = statSync(path);
  const bytes = readFileSlice(path, 0, Math.min(Number(stats.size), headerBytes));
  if (extension === ".png" && bytes.length >= 24) {
    if (bytes.subarray(0, 8).equals(Buffer.from([137, 80, 78, 71, 13, 10, 26, 10]))) {
      return { width: bytes.readUInt32BE(16), height: bytes.readUInt32BE(20) };
    }
  }
  if (extension === ".gif" && bytes.length >= 10) {
    if (bytes.subarray(0, 3).toString("ascii") === "GIF") {
      return { width: bytes.readUInt16LE(6), height: bytes.readUInt16LE(8) };
    }
  }
  if (extension === ".jpg" || extension === ".jpeg") return parseJpegDimensions(bytes);
  if (extension === ".webp" && bytes.length >= 30) {
    if (
      bytes.subarray(0, 4).toString("ascii") === "RIFF" &&
      bytes.subarray(8, 12).toString("ascii") === "WEBP" &&
      bytes.subarray(12, 16).toString("ascii") === "VP8X"
    ) {
      return {
        width: 1 + bytes[24] + (bytes[25] << 8) + (bytes[26] << 16),
        height: 1 + bytes[27] + (bytes[28] << 8) + (bytes[29] << 16),
      };
    }
  }
  return null;
}

function parseZipCentralDirectory(path, fileSize, maxArchiveBytes) {
  const tailLength = Math.min(fileSize, 128 * 1024);
  const tailOffset = fileSize - tailLength;
  const tail = readFileSlice(path, tailOffset, tailLength);
  let eocdOffset = -1;
  for (let index = tail.length - 22; index >= 0; index -= 1) {
    if (tail.readUInt32LE(index) === 0x06054b50) {
      eocdOffset = index;
      break;
    }
  }
  if (eocdOffset < 0) return { parsed: false, reason: "eocd_not_found" };
  const entryCount = tail.readUInt16LE(eocdOffset + 10);
  const centralDirectoryBytes = tail.readUInt32LE(eocdOffset + 12);
  const centralDirectoryOffset = tail.readUInt32LE(eocdOffset + 16);
  if (
    entryCount === 0xffff ||
    centralDirectoryBytes === 0xffffffff ||
    centralDirectoryOffset === 0xffffffff
  ) {
    return { parsed: false, reason: "zip64_requires_sample_tool" };
  }
  if (centralDirectoryBytes > maxArchiveBytes) {
    return { parsed: false, reason: "central_directory_over_limit" };
  }
  const eocdAbsolute = tailOffset + eocdOffset;
  const prefixBytes = eocdAbsolute - centralDirectoryBytes - centralDirectoryOffset;
  if (prefixBytes < 0) return { parsed: false, reason: "central_directory_bounds" };
  const central = readFileSlice(
    path,
    prefixBytes + centralDirectoryOffset,
    centralDirectoryBytes,
  );
  let offset = 0;
  let parsedEntries = 0;
  let uncompressedBytes = 0;
  let maxEntryBytes = 0;
  while (offset + 46 <= central.length && parsedEntries < entryCount) {
    if (central.readUInt32LE(offset) !== 0x02014b50) {
      return { parsed: false, reason: "central_directory_record_invalid" };
    }
    const compressedSize = central.readUInt32LE(offset + 20);
    const uncompressedSize = central.readUInt32LE(offset + 24);
    const nameBytes = central.readUInt16LE(offset + 28);
    const extraBytes = central.readUInt16LE(offset + 30);
    const commentBytes = central.readUInt16LE(offset + 32);
    const recordBytes = 46 + nameBytes + extraBytes + commentBytes;
    if (offset + recordBytes > central.length) {
      return { parsed: false, reason: "central_directory_record_truncated" };
    }
    if (compressedSize === 0xffffffff || uncompressedSize === 0xffffffff) {
      return { parsed: false, reason: "zip64_entry_requires_sample_tool" };
    }
    parsedEntries += 1;
    uncompressedBytes += uncompressedSize;
    maxEntryBytes = Math.max(maxEntryBytes, uncompressedSize);
    offset += recordBytes;
  }
  if (parsedEntries !== entryCount) {
    return { parsed: false, reason: "central_directory_entry_count_mismatch" };
  }
  return {
    parsed: true,
    entries: parsedEntries,
    uncompressed_bytes: uncompressedBytes,
    max_entry_bytes: maxEntryBytes,
    central_directory_bytes: centralDirectoryBytes,
  };
}

function emptyImageInspection(limit) {
  return {
    sample_limit: limit,
    inspected: 0,
    parsed: 0,
    unparsed: 0,
    errors: [],
    dimensions: {
      min_width: null,
      max_width: null,
      min_height: null,
      max_height: null,
      min_pixels: null,
      max_pixels: null,
      pixel_buckets: {
        "<1MP": 0,
        "1-5MP": 0,
        "5-15MP": 0,
        "15-24MP": 0,
        "24-50MP": 0,
        ">=50MP": 0,
      },
    },
  };
}

function emptyArchiveInspection(limit) {
  return {
    sample_limit: limit,
    inspected: 0,
    parsed: 0,
    unparsed: 0,
    errors: [],
    entries: { min: null, max: null, total: 0 },
    uncompressed_bytes: { total: 0, max_entry: 0 },
    central_directory_bytes: { total: 0, max: 0 },
    unparsed_reasons: {},
  };
}

function recordImageInspection(inspection, dimensions) {
  inspection.parsed += 1;
  const width = dimensions.width;
  const height = dimensions.height;
  const pixels = width * height;
  const summary = inspection.dimensions;
  summary.min_width = summary.min_width == null ? width : Math.min(summary.min_width, width);
  summary.max_width = summary.max_width == null ? width : Math.max(summary.max_width, width);
  summary.min_height = summary.min_height == null ? height : Math.min(summary.min_height, height);
  summary.max_height = summary.max_height == null ? height : Math.max(summary.max_height, height);
  summary.min_pixels = summary.min_pixels == null ? pixels : Math.min(summary.min_pixels, pixels);
  summary.max_pixels = summary.max_pixels == null ? pixels : Math.max(summary.max_pixels, pixels);
  const bucket =
    pixels < 1_000_000
      ? "<1MP"
      : pixels < 5_000_000
        ? "1-5MP"
        : pixels < 15_000_000
          ? "5-15MP"
          : pixels < 24_000_000
            ? "15-24MP"
            : pixels < 50_000_000
              ? "24-50MP"
              : ">=50MP";
  summary.pixel_buckets[bucket] += 1;
}

function recordArchiveInspection(inspection, summary) {
  inspection.parsed += 1;
  inspection.entries.min =
    inspection.entries.min == null ? summary.entries : Math.min(inspection.entries.min, summary.entries);
  inspection.entries.max =
    inspection.entries.max == null ? summary.entries : Math.max(inspection.entries.max, summary.entries);
  inspection.entries.total += summary.entries;
  inspection.uncompressed_bytes.total += summary.uncompressed_bytes;
  inspection.uncompressed_bytes.max_entry = Math.max(
    inspection.uncompressed_bytes.max_entry,
    summary.max_entry_bytes,
  );
  inspection.central_directory_bytes.total += summary.central_directory_bytes;
  inspection.central_directory_bytes.max = Math.max(
    inspection.central_directory_bytes.max,
    summary.central_directory_bytes,
  );
}

/**
 * Collect bounded, read-only facts about one media root.
 *
 * This is deliberately opt-in.  The ordinary baseline capture must not walk
 * a multi-terabyte media tree just because --media-root was supplied for a
 * filesystem-capacity snapshot.  The walk keeps only aggregate counters and
 * a bounded error sample, so its memory usage is proportional to directory
 * depth rather than file count.
 */
export function collectMediaRootManifest(
  { kind, path: rootPath },
  {
    maxFiles = 5_000_000,
    maxDirectories = 1_000_000,
    inspectionLimit = DEFAULT_MEDIA_INSPECTION_LIMIT,
    headerBytes = DEFAULT_MEDIA_HEADER_BYTES,
    maxArchiveBytes = DEFAULT_MEDIA_ARCHIVE_BYTES,
  } = {},
) {
  if (!Number.isInteger(maxFiles) || maxFiles < 1) {
    throw new RangeError("maxFiles must be a positive integer");
  }
  if (!Number.isInteger(maxDirectories) || maxDirectories < 1) {
    throw new RangeError("maxDirectories must be a positive integer");
  }
  if (
    !Number.isInteger(inspectionLimit) ||
    inspectionLimit < 0 ||
    inspectionLimit > MAX_MEDIA_INSPECTION_LIMIT
  ) {
    throw new RangeError(
      `inspectionLimit must be an integer between 0 and ${MAX_MEDIA_INSPECTION_LIMIT}`,
    );
  }
  if (!Number.isInteger(headerBytes) || headerBytes < 64 || headerBytes > MAX_MEDIA_HEADER_BYTES) {
    throw new RangeError(
      `headerBytes must be between 64 and ${MAX_MEDIA_HEADER_BYTES} bytes`,
    );
  }
  if (
    !Number.isInteger(maxArchiveBytes) ||
    maxArchiveBytes < 1 ||
    maxArchiveBytes > MAX_MEDIA_ARCHIVE_BYTES
  ) {
    throw new RangeError(
      `maxArchiveBytes must be between 1 and ${MAX_MEDIA_ARCHIVE_BYTES} bytes`,
    );
  }

  const result = {
    kind,
    path: rootPath,
    status: "captured",
    complete: true,
    stop_reason: null,
    files: 0,
    directories: 0,
    skipped_symlinks: 0,
    total_bytes: 0,
    path_bytes: 0,
    max_path_bytes: 0,
    extensions: [],
    image_inspection: emptyImageInspection(inspectionLimit),
    archive_inspection: emptyArchiveInspection(inspectionLimit),
    errors: [],
  };

  try {
    const rootStats = statSync(rootPath);
    if (!rootStats.isDirectory()) {
      result.status = "unavailable";
      result.complete = false;
      result.stop_reason = "root_not_directory";
      return result;
    }
  } catch (error) {
    result.status = "unavailable";
    result.complete = false;
    result.stop_reason = "root_unreadable";
    result.errors.push(String(error?.message ?? error).slice(0, 512));
    return result;
  }

  const extensionStats = new Map();
  const stack = [rootPath];
  let stop = false;

  const recordError = (error) => {
    if (result.errors.length < MEDIA_MANIFEST_ERROR_LIMIT) {
      result.errors.push(String(error?.message ?? error).slice(0, 512));
    }
  };

  while (stack.length && !stop) {
    const current = stack.pop();
    let entries;
    try {
      entries = readdirSync(current, { withFileTypes: true });
    } catch (error) {
      result.complete = false;
      recordError(error);
      continue;
    }

    for (const entry of entries) {
      if (entry.isSymbolicLink()) {
        result.skipped_symlinks += 1;
        continue;
      }
      const absolutePath = join(current, entry.name);
      if (entry.isDirectory()) {
        result.directories += 1;
        if (result.directories >= maxDirectories) {
          result.complete = false;
          result.stop_reason = "max_directories";
          stop = true;
          break;
        }
        stack.push(absolutePath);
        continue;
      }
      if (!entry.isFile()) continue;
      if (result.files >= maxFiles) {
        result.complete = false;
        result.stop_reason = "max_files";
        stop = true;
        break;
      }

      let stats;
      try {
        stats = statSync(absolutePath);
      } catch (error) {
        result.complete = false;
        recordError(error);
        continue;
      }

      const relativePath = portableRelativePath(rootPath, absolutePath);
      const pathBytes = Buffer.byteLength(relativePath, "utf8");
      const bytes = Number(stats.size);
      result.files += 1;
      result.total_bytes += Number.isSafeInteger(bytes) && bytes >= 0 ? bytes : 0;
      result.path_bytes += pathBytes;
      result.max_path_bytes = Math.max(result.max_path_bytes, pathBytes);

      const extension = normalizedExtension(entry.name);
      const currentStats = extensionStats.get(extension) ?? { extension, files: 0, bytes: 0 };
      currentStats.files += 1;
      currentStats.bytes += Number.isSafeInteger(bytes) && bytes >= 0 ? bytes : 0;
      extensionStats.set(extension, currentStats);

      const imageInspection = result.image_inspection;
      const isImage = MEDIA_IMAGE_EXTENSIONS.has(extension);
      if (isImage && imageInspection.inspected < inspectionLimit) {
        imageInspection.inspected += 1;
        try {
          const dimensions = parseImageDimensions(absolutePath, extension, headerBytes);
          if (dimensions && dimensions.width > 0 && dimensions.height > 0) {
            recordImageInspection(imageInspection, dimensions);
          } else {
            imageInspection.unparsed += 1;
          }
        } catch (error) {
          imageInspection.unparsed += 1;
          if (imageInspection.errors.length < MEDIA_MANIFEST_ERROR_LIMIT) {
            imageInspection.errors.push(String(error?.message ?? error).slice(0, 512));
          }
        }
      }

      const isArchive = MEDIA_ARCHIVE_EXTENSIONS.has(extension);
      const archiveInspection = result.archive_inspection;
      if (isArchive && archiveInspection.inspected < inspectionLimit) {
        archiveInspection.inspected += 1;
        try {
          const summary = parseZipCentralDirectory(absolutePath, bytes, maxArchiveBytes);
          if (summary.parsed) {
            recordArchiveInspection(archiveInspection, summary);
          } else {
            archiveInspection.unparsed += 1;
            archiveInspection.unparsed_reasons[summary.reason] =
              (archiveInspection.unparsed_reasons[summary.reason] ?? 0) + 1;
          }
        } catch (error) {
          archiveInspection.unparsed += 1;
          if (archiveInspection.errors.length < MEDIA_MANIFEST_ERROR_LIMIT) {
            archiveInspection.errors.push(String(error?.message ?? error).slice(0, 512));
          }
        }
      }
    }
  }

  result.extensions = [...extensionStats.values()].sort(
    (left, right) => right.files - left.files || left.extension.localeCompare(right.extension),
  );
  return result;
}
