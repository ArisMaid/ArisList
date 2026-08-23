import assert from "node:assert/strict";
import { mkdtempSync, mkdirSync, writeFileSync } from "node:fs";
import os from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import test from "node:test";

import {
  collectDatasetManifest,
  collectMediaRootManifest,
  evaluateFacetBitmapRuntimeEvidence,
  evaluateFacetCacheRuntimeEvidence,
  evaluateLargeScaleGateRuns,
  evaluateQmsRuntimeEvidence,
  evaluateScenarioGates,
  evaluateR1gRuntimeEvidence,
  evaluateSearchIncrementalReaderEvidence,
  evaluateSearchShadowCanaryEvidence,
  parseLargeScaleGateOutput,
  percentile,
  summarizeScenarioRecords,
  SYSTEM_SAMPLE_COLUMNS,
} from "./perf-lib.mjs";

test("system sampler schema has stable SQLite and resource columns", () => {
  assert.equal(SYSTEM_SAMPLE_COLUMNS.length, 87);
  assert.equal(SYSTEM_SAMPLE_COLUMNS[0], "timestamp");
  assert.equal(SYSTEM_SAMPLE_COLUMNS.at(-1), "qms_cache_usage_rescans");
  for (const column of [
    "sqlite_pool_size",
    "sqlite_active_connections",
    "sqlite_pool_checkout_idle_max_micros",
    "sqlite_pool_tracked_acquire_wait_max_micros",
    "sqlite_pool_acquire_timeouts",
    "sqlite_busy_errors",
    "sqlite_read_snapshot_oldest_active_micros",
    "sqlite_read_snapshot_hold_max_micros",
    "sqlite_writer_queue_depth",
    "sqlite_writer_acquire_wait_max_micros",
    "sqlite_wal_checkpoint_busy_pages",
    "jobs_maintenance_wait_max_micros",
    "processing_memory_used_bytes",
    "cgroup_memory_limit_bytes",
    "cgroup_cpu_quota_micros",
    "cgroup_cpu_period_micros",
    "cgroup_cpu_limit_cores",
  ]) {
    assert.ok(SYSTEM_SAMPLE_COLUMNS.includes(column), `missing ${column}`);
  }
});

test("large-scale gate parser and evaluator stay fail-closed", () => {
  const derivative = {
    rows: 700000,
    low_watermark_bytes: 100,
    capacity_after: { bytes: 90 },
    integrity_check: "ok",
    query_plan: ["SEARCH derivatives USING INDEX idx_derivatives_eviction_lru (status=?)"],
  };
  const parsed = parseLargeScaleGateOutput(
    `synthetic inventory rows=700000 elapsed_ms=17 max_batch_rows=1024 max_batch_bytes=169985\nDERIVATIVE_LEDGER_GATE=${JSON.stringify(derivative)}`,
  );
  assert.equal(parsed.inventory.max_batch_rows, 1024);
  assert.equal(parsed.derivatives[0].rows, 700000);
  const passed = evaluateLargeScaleGateRuns([
    {
      id: "inventory-700000",
      status: "passed",
      exit_code: 0,
      parsed: { inventory: parsed.inventory, derivative: null },
    },
    {
      id: "derivative-700000",
      status: "passed",
      exit_code: 0,
      parsed: { inventory: null, derivative: parsed.derivatives[0] },
    },
  ], { derivativeRows: [700000] });
  assert.equal(passed.status, "passed");
  const incomplete = evaluateLargeScaleGateRuns([], { derivativeRows: [700000] });
  assert.equal(incomplete.status, "incomplete");
});

test("percentile uses the conservative nearest-rank definition", () => {
  assert.equal(percentile([40, 10, 30, 20], 0.5), 20);
  assert.equal(percentile([40, 10, 30, 20], 0.95), 40);
  assert.equal(percentile([], 0.95), null);
});

test("scenario summaries remain separated by cache state and concurrency", () => {
  const records = [
    { scenario: "catalog", warm: "warm", concurrency: 1, status: 200, ttfb_ms: 10, total_ms: 20, bytes: 100 },
    { scenario: "catalog", warm: "warm", concurrency: 1, status: 503, ttfb_ms: 30, total_ms: 40, bytes: 10 },
    { scenario: "catalog", warm: "cold", concurrency: 2, status: 200, ttfb_ms: 50, total_ms: 60, bytes: 200 },
  ];
  const summary = summarizeScenarioRecords(records, "2026-07-31T00:00:00.000Z");
  assert.equal(summary.status, "complete");
  assert.equal(summary.record_count, 3);
  assert.equal(summary.groups.length, 2);
  const warm = summary.groups.find((group) => group.warm === "warm");
  assert.deepEqual(
    {
      requests: warm.requests,
      succeeded: warm.succeeded,
      failed: warm.failed,
      status503: warm.status_503,
      p95: warm.total_ms.p95,
      bytes: warm.bytes,
    },
    { requests: 2, succeeded: 1, failed: 1, status503: 1, p95: 40, bytes: 110 },
  );
});

test("scenario gates distinguish missing evidence from breached thresholds", () => {
  const summary = summarizeScenarioRecords(
    Array.from({ length: 30 }, (_, index) => ({
      scenario: "catalog-works",
      warm: "warm",
      concurrency: 1,
      status: 200,
      ttfb_ms: 20 + index,
      total_ms: 40 + index,
      bytes: 100,
    })),
    "2026-07-31T00:00:00.000Z",
  );
  const configuration = {
    profile: "test",
    scenarios: [
      {
        id: "works",
        scenario: "catalog-works",
        warm: "warm",
        concurrency: 1,
        minimum: { requests: 30, succeeded: 30 },
        maximum: { failed: 0, "total_ms.p95": 100 },
      },
      {
        id: "facets",
        scenario: "catalog-facets",
        warm: "warm",
        concurrency: 1,
        minimum: { requests: 30 },
        maximum: { "total_ms.p95": 100 },
      },
    ],
  };
  const incomplete = evaluateScenarioGates(summary, configuration);
  assert.equal(incomplete.status, "incomplete");
  assert.equal(incomplete.checks[0].status, "passed");
  assert.equal(incomplete.checks[1].status, "incomplete");

  configuration.scenarios = [
    {
      id: "works",
      scenario: "catalog-works",
      warm: "warm",
      concurrency: 1,
      minimum: { requests: 30 },
      maximum: { "total_ms.p95": 50 },
    },
  ];
  const failed = evaluateScenarioGates(summary, configuration);
  assert.equal(failed.status, "failed");
  assert.match(failed.checks[0].failures[0], /exceeds/u);
});

test("R1G runtime evidence requires one miss and two shared candidate lookups", () => {
  const before = {
    readers: 1,
    reader_opens: 1,
    candidate_entries: 0,
    candidate_ids: 0,
    candidate_hits: 0,
    candidate_misses: 0,
    candidate_coalesced: 0,
    candidate_evictions: 0,
    candidate_cache_max_entries: 16,
    candidate_cache_max_ids: 500_000,
  };
  const afterFirst = {
    ...before,
    candidate_entries: 1,
    candidate_ids: 40_000,
    candidate_misses: 1,
    candidate_coalesced: 2,
  };
  const afterAll = {
    ...afterFirst,
    candidate_hits: 90,
  };
  const passed = evaluateR1gRuntimeEvidence(before, afterFirst, afterAll);
  assert.equal(passed.status, "passed");
  assert.equal(passed.first_triplet_delta.candidate_misses, 1);

  const failed = evaluateR1gRuntimeEvidence(before, { ...afterFirst, candidate_misses: 3 }, afterAll);
  assert.equal(failed.status, "failed");
  assert.equal(failed.checks.find((check) => check.id === "candidate-singleflight").status, "failed");
});


test("facet runtime evidence requires one miss, coalescing, warm hits and bounded state", () => {
  const before = {
    facet_entries: 0,
    facet_items: 0,
    facet_hits: 0,
    facet_misses: 0,
    facet_coalesced: 0,
    facet_evictions: 0,
    facet_cache_max_entries: 32,
    facet_cache_max_items: 4096,
    facet_cache_ttl_millis: 5000,
  };
  const afterConcurrent = {
    ...before,
    facet_entries: 1,
    facet_items: 100,
    facet_misses: 1,
    facet_coalesced: 2,
  };
  const afterWarm = { ...afterConcurrent, facet_hits: 30 };
  const passed = evaluateFacetCacheRuntimeEvidence(before, afterConcurrent, afterWarm, 3, 30);
  assert.equal(passed.status, "passed");
  assert.equal(passed.concurrent_delta.facet_misses, 1);
  assert.equal(passed.warm_delta.facet_hits, 30);

  const failed = evaluateFacetCacheRuntimeEvidence(
    before,
    { ...afterConcurrent, facet_misses: 3, facet_coalesced: 0 },
    afterWarm,
    3,
    30,
  );
  assert.equal(failed.status, "failed");
  assert.equal(failed.checks.find((check) => check.id === "facet-singleflight").status, "failed");
});

test("facet bitmap evidence requires ready routing without fallbacks and bounded memory", () => {
  const before = {
    state: "ready",
    catalog_revision: 10,
    works: 40_000,
    tags: 2_048,
    associations: 800_000,
    estimated_bytes: 12 * 1024 * 1024,
    builds: 1,
    build_failures: 0,
    queries: 0,
    fallbacks: 0,
    scope_rejections: 0,
    delta_refreshes: 0,
    delta_work_ids: 0,
    max_works: 50_000,
    max_tags: 65_536,
    max_associations: 1_000_000,
    max_estimated_bytes: 64 * 1024 * 1024,
  };
  const passed = evaluateFacetBitmapRuntimeEvidence(
    before,
    { ...before, queries: 30 },
    30,
  );
  assert.equal(passed.status, "passed");
  assert.equal(passed.delta.queries, 30);

  const failed = evaluateFacetBitmapRuntimeEvidence(
    before,
    { ...before, queries: 29, fallbacks: 1 },
    30,
  );
  assert.equal(failed.status, "failed");
  assert.equal(
    failed.checks.find((check) => check.id === "facet-bitmap-authoritative-routing").status,
    "failed",
  );
});

test("qmediasync runtime evidence requires bounded Range responses and clean cache accounting", () => {
  const before = {
    stream_requests: 4,
    stream_range_requests: 0,
    stream_full_requests: 4,
    stream_successes: 4,
    stream_partial_responses: 0,
    stream_unsatisfiable_responses: 0,
    stream_failures: 0,
    stream_response_bytes: 0,
    cache_hits: 2,
    cache_misses: 0,
    cache_not_modified: 0,
    cache_downloads: 0,
    cache_download_bytes: 0,
    cache_quota_rejections: 0,
    cache_usage_rescans: 1,
  };
  const after = {
    ...before,
    stream_requests: 34,
    stream_range_requests: 30,
    stream_full_requests: 4,
    stream_successes: 34,
    stream_partial_responses: 30,
    stream_response_bytes: 1_536_000,
    cache_hits: 12,
    cache_misses: 2,
    cache_downloads: 2,
    cache_download_bytes: 40_000_000,
    cache_usage_rescans: 2,
  };
  const passed = evaluateQmsRuntimeEvidence(before, after, {
    expectedRequests: 30,
    expectedRangeRequests: 30,
    requirePartial: true,
  });
  assert.equal(passed.status, "passed");
  assert.equal(passed.delta.stream_response_bytes, 1_536_000);

  const failed = evaluateQmsRuntimeEvidence(
    before,
    { ...after, stream_partial_responses: 28, stream_failures: 1, cache_quota_rejections: 1 },
    { expectedRequests: 30, expectedRangeRequests: 30, requirePartial: true },
  );
  assert.equal(failed.status, "failed");
  assert.equal(
    failed.checks.find((check) => check.id === "qms-stream-response-contract").status,
    "failed",
  );
  assert.equal(failed.checks.find((check) => check.id === "qms-cache-contract").status, "failed");
});

test("search shadow canary evidence requires stable readiness and zero ID/order drift", () => {
  const beforeRuntime = {
    canary_queries: 5,
    canary_id_mismatches: 0,
    canary_order_mismatches: 0,
    canary_failures: 0,
    canary_rejections: 0,
  };
  const afterRuntime = { ...beforeRuntime, canary_queries: 7 };
  const shadow = { ready: true, status: "ready", applied_revision: 42 };
  const records = [1, 2].map((ordinal) => ({
    ordinal,
    status: 200,
    error: null,
    reader: "shadow",
    canary: { id_match: true, order_match: true },
  }));
  const facts = {
    status: "passed",
    catalog_revision: 42,
    shadow_applied_revision: 42,
    sqlite_work_count: 2,
    shadow_document_count: 2,
    shadow_unique_work_count: 2,
    missing_work_ids: 0,
    unexpected_work_ids: 0,
    duplicate_documents: 0,
    invalid_documents: 0,
    sqlite_ids_sha256: "a".repeat(64),
    shadow_ids_sha256: "a".repeat(64),
  };
  const passed = evaluateSearchShadowCanaryEvidence(
    beforeRuntime,
    afterRuntime,
    shadow,
    shadow,
    records,
    facts,
    facts,
  );
  assert.equal(passed.status, "passed");
  assert.equal(passed.delta.canary_queries, 2);

  const failed = evaluateSearchShadowCanaryEvidence(
    beforeRuntime,
    {
      ...afterRuntime,
      canary_id_mismatches: 1,
      canary_order_mismatches: 1,
    },
    shadow,
    { ...shadow, applied_revision: 43 },
    [{ ...records[0], canary: { id_match: false, order_match: false } }, records[1]],
    facts,
    {
      ...facts,
      status: "failed",
      catalog_revision: 43,
      shadow_applied_revision: 43,
      missing_work_ids: 1,
      shadow_ids_sha256: "b".repeat(64),
    },
  );
  assert.equal(failed.status, "failed");
  assert.equal(failed.checks.find((check) => check.id === "search-canary-zero-diff").status, "failed");
});

test("incremental reader evidence requires an armed, caught-up production path", () => {
  const health = {
    search_outbox: { pending: 0, revision_lag: 0, catalog_revision: 42 },
    search_shadow: { ready: true, status: "ready", applied_revision: 42 },
    search_reconciliation: { status: "passed", cutover_armed: true },
    search_features: { outbox_shadow: true, shadow_canary: true, incremental_reader: true },
  };
  const records = [
    { status: 200, error: null, reader: "production", rebuilt: false },
    { status: 200, error: null, reader: "production", rebuilt: false },
  ];
  assert.equal(
    evaluateSearchIncrementalReaderEvidence(health, health, records).status,
    "passed",
  );

  const failed = evaluateSearchIncrementalReaderEvidence(
    health,
    {
      ...health,
      search_shadow: { ...health.search_shadow, status: "degraded", ready: false },
      search_reconciliation: { status: "failed", cutover_armed: false },
      search_features: { ...health.search_features, incremental_reader: false },
    },
    [{ ...records[0], rebuilt: true }],
  );
  assert.equal(failed.status, "failed");
  assert.equal(
    failed.checks.find((check) => check.id === "search-incremental-progress-gate").status,
    "failed",
  );
  assert.equal(
    failed.checks.find((check) => check.id === "search-incremental-production-routing").status,
    "failed",
  );
});

test("dataset capture reads catalog facts without mutating SQLite", () => {
  const directory = mkdtempSync(join(os.tmpdir(), "arislist-perf-test-"));
  const path = join(directory, "catalog.sqlite");
  const database = new DatabaseSync(path);
  database.exec(`
    CREATE TABLE schema_migrations (version INTEGER PRIMARY KEY);
    INSERT INTO schema_migrations VALUES (8), (9);
    CREATE TABLE works (id INTEGER PRIMARY KEY, kind TEXT NOT NULL, deleted_at TEXT);
    INSERT INTO works VALUES (1, 'gallery', NULL), (2, 'novel', NULL);
    CREATE TABLE assets (id INTEGER PRIMARY KEY, work_id INTEGER, role TEXT, size INTEGER);
    INSERT INTO assets VALUES (1, 1, 'image', 10), (2, 1, 'image', 20), (3, 2, 'book', 30);
    CREATE TABLE tags (id INTEGER PRIMARY KEY);
    INSERT INTO tags VALUES (1);
    CREATE TABLE work_tags (work_id INTEGER, tag_id INTEGER);
    INSERT INTO work_tags VALUES (1, 1), (2, 1);
    CREATE TABLE library_roots (id INTEGER PRIMARY KEY, kind TEXT NOT NULL);
    INSERT INTO library_roots VALUES (1, 'gallery'), (2, 'novel');
    CREATE TABLE file_inventory (
      root_id INTEGER NOT NULL,
      relative_path TEXT NOT NULL,
      work_key TEXT,
      status TEXT NOT NULL,
      size INTEGER
    );
    INSERT INTO file_inventory VALUES
      (1, 'set/cover.jpg', 'set', 'present', 100),
      (1, 'set/page.jpg', 'set', 'missing', 200),
      (2, 'book.epub', 'book', 'present', 300);
    CREATE TABLE catalog_state (
      singleton INTEGER PRIMARY KEY,
      revision INTEGER NOT NULL,
      activity_revision INTEGER NOT NULL
    );
    INSERT INTO catalog_state VALUES (1, 12, 3);
    CREATE TABLE search_index_state (
      index_name TEXT PRIMARY KEY,
      schema_version INTEGER,
      baseline_revision INTEGER,
      applied_revision INTEGER,
      indexed_documents INTEGER,
      ready INTEGER,
      status TEXT
    );
    INSERT INTO search_index_state VALUES ('shadow-v3', 1, 10, 12, 2, 1, 'ready');
  `);
  database.close();

  const manifest = collectDatasetManifest(path);
  assert.equal(manifest.schema.version, 9);
  assert.equal(manifest.counts.works, 2);
  assert.equal(manifest.counts.assets, 3);
  assert.equal(manifest.counts.work_tags, 2);
  assert.equal(manifest.max_assets_per_work, 2);
  assert.deepEqual(manifest.works_by_kind, [
    { kind: "gallery", count: 1 },
    { kind: "novel", count: 1 },
  ]);
  assert.deepEqual(manifest.assets_by_role, [
    { role: "book", count: 1, bytes: 30 },
    { role: "image", count: 2, bytes: 30 },
  ]);
  assert.deepEqual(manifest.long_tail.assets_per_work_by_kind, [
    {
      kind: "gallery",
      works: 1,
      total_assets: 2,
      zero_asset_works: 0,
      p50: 2,
      p95: 2,
      p99: 2,
      max: 2,
    },
    {
      kind: "novel",
      works: 1,
      total_assets: 1,
      zero_asset_works: 0,
      p50: 1,
      p95: 1,
      p99: 1,
      max: 1,
    },
  ]);
  assert.equal(manifest.long_tail.tags_per_work_by_kind[0].total_tags, 1);
  assert.deepEqual(manifest.long_tail.inventory_by_kind, [
    {
      kind: "gallery",
      roots: 1,
      files: 2,
      present_files: 1,
      missing_files: 1,
      total_bytes: 300,
      max_files_per_root: 2,
      max_files_per_work: 2,
    },
    {
      kind: "novel",
      roots: 1,
      files: 1,
      present_files: 1,
      missing_files: 0,
      total_bytes: 300,
      max_files_per_root: 1,
      max_files_per_work: 1,
    },
  ]);
  assert.equal(manifest.long_tail.deleted_works, 0);
  assert.equal(manifest.long_tail.catalog_revision, 12);
  assert.equal(manifest.long_tail.activity_revision, 3);
  assert.deepEqual(manifest.long_tail.search_indexes[0], {
    index_name: "shadow-v3",
    schema_version: 1,
    baseline_revision: 10,
    applied_revision: 12,
    indexed_documents: 2,
    ready: true,
    status: "ready",
  });
});

test("dataset capture keeps optional long-tail facts explicit on an old schema", () => {
  const directory = mkdtempSync(join(os.tmpdir(), "arislist-perf-legacy-manifest-"));
  const path = join(directory, "legacy.sqlite");
  const database = new DatabaseSync(path);
  database.exec(`
    CREATE TABLE works (id INTEGER PRIMARY KEY, kind TEXT NOT NULL);
    INSERT INTO works VALUES (1, 'novel');
    CREATE TABLE assets (id INTEGER PRIMARY KEY, work_id INTEGER, role TEXT, size INTEGER);
    CREATE TABLE work_tags (work_id INTEGER, tag_id INTEGER);
  `);
  database.close();

  const manifest = collectDatasetManifest(path);
  assert.deepEqual(manifest.long_tail.inventory_by_kind, []);
  assert.equal(manifest.long_tail.deleted_works, null);
  assert.equal(manifest.long_tail.catalog_revision, null);
  assert.equal(manifest.long_tail.activity_revision, null);
  assert.deepEqual(manifest.long_tail.search_indexes, []);
  assert.equal(manifest.long_tail.assets_per_work_by_kind[0].total_assets, 0);
  assert.equal(manifest.long_tail.tags_per_work_by_kind[0].total_tags, 0);
});

test("media manifest walks an explicit root with bounded aggregate memory", () => {
  const root = mkdtempSync(join(os.tmpdir(), "arislist-media-manifest-"));
  mkdirSync(join(root, "author", "set"), { recursive: true });
  writeFileSync(join(root, "author", "cover.jpg"), Buffer.alloc(7));
  writeFileSync(join(root, "author", "set", "page.PNG"), Buffer.alloc(11));

  const manifest = collectMediaRootManifest({ kind: "gallery", path: root });
  assert.equal(manifest.status, "captured");
  assert.equal(manifest.complete, true);
  assert.equal(manifest.files, 2);
  assert.equal(manifest.directories, 2);
  assert.equal(manifest.total_bytes, 18);
  assert.equal(manifest.max_path_bytes, Buffer.byteLength("author/set/page.PNG"));
  assert.deepEqual(manifest.extensions, [
    { extension: ".jpg", files: 1, bytes: 7 },
    { extension: ".png", files: 1, bytes: 11 },
  ]);
});

test("media manifest samples image dimensions and ZIP central-directory facts", () => {
  const root = mkdtempSync(join(os.tmpdir(), "arislist-media-manifest-inspect-"));
  const png = Buffer.alloc(24);
  Buffer.from([137, 80, 78, 71, 13, 10, 26, 10]).copy(png, 0);
  png.writeUInt32BE(3200, 16);
  png.writeUInt32BE(1800, 20);
  writeFileSync(join(root, "cover.png"), png);

  const name = Buffer.from("chapter.xhtml");
  const body = Buffer.from("hello");
  const local = Buffer.alloc(30 + name.length + body.length);
  local.writeUInt32LE(0x04034b50, 0);
  local.writeUInt16LE(20, 4);
  local.writeUInt16LE(0, 6);
  local.writeUInt16LE(0, 8);
  local.writeUInt16LE(0, 10);
  local.writeUInt16LE(0, 12);
  local.writeUInt32LE(0, 14);
  local.writeUInt32LE(body.length, 18);
  local.writeUInt32LE(body.length, 22);
  local.writeUInt16LE(name.length, 26);
  name.copy(local, 30);
  body.copy(local, 30 + name.length);
  const central = Buffer.alloc(46 + name.length);
  central.writeUInt32LE(0x02014b50, 0);
  central.writeUInt16LE(20, 4);
  central.writeUInt16LE(20, 6);
  central.writeUInt16LE(0, 8);
  central.writeUInt16LE(0, 10);
  central.writeUInt16LE(0, 12);
  central.writeUInt16LE(0, 14);
  central.writeUInt32LE(0, 16);
  central.writeUInt32LE(body.length, 20);
  central.writeUInt32LE(body.length, 24);
  central.writeUInt16LE(name.length, 28);
  central.writeUInt16LE(0, 30);
  central.writeUInt16LE(0, 32);
  central.writeUInt16LE(0, 34);
  central.writeUInt16LE(0, 36);
  central.writeUInt32LE(0, 38);
  central.writeUInt32LE(0, 42);
  name.copy(central, 46);
  const eocd = Buffer.alloc(22);
  eocd.writeUInt32LE(0x06054b50, 0);
  eocd.writeUInt16LE(0, 4);
  eocd.writeUInt16LE(0, 6);
  eocd.writeUInt16LE(1, 8);
  eocd.writeUInt16LE(1, 10);
  eocd.writeUInt32LE(central.length, 12);
  eocd.writeUInt32LE(local.length, 16);
  eocd.writeUInt16LE(0, 20);
  writeFileSync(join(root, "book.epub"), Buffer.concat([local, central, eocd]));

  const manifest = collectMediaRootManifest(
    { kind: "novel", path: root },
    { inspectionLimit: 8 },
  );
  assert.equal(manifest.image_inspection.inspected, 1);
  assert.equal(manifest.image_inspection.parsed, 1);
  assert.equal(manifest.image_inspection.dimensions.min_width, 3200);
  assert.equal(manifest.image_inspection.dimensions.min_height, 1800);
  assert.equal(manifest.image_inspection.dimensions.pixel_buckets["5-15MP"], 1);
  assert.equal(manifest.archive_inspection.inspected, 1);
  assert.equal(manifest.archive_inspection.parsed, 1);
  assert.equal(manifest.archive_inspection.entries.min, 1);
  assert.equal(manifest.archive_inspection.entries.max, 1);
  assert.equal(manifest.archive_inspection.uncompressed_bytes.total, body.length);
  assert.equal(manifest.archive_inspection.uncompressed_bytes.max_entry, body.length);
});

test("media manifest stops at explicit file and directory bounds", () => {
  const root = mkdtempSync(join(os.tmpdir(), "arislist-media-manifest-limit-"));
  mkdirSync(join(root, "nested"));
  writeFileSync(join(root, "one.bin"), Buffer.alloc(1));
  writeFileSync(join(root, "nested", "two.bin"), Buffer.alloc(2));

  const fileLimited = collectMediaRootManifest(
    { kind: "comic", path: root },
    { maxFiles: 1 },
  );
  assert.equal(fileLimited.complete, false);
  assert.equal(fileLimited.stop_reason, "max_files");
  assert.equal(fileLimited.files, 1);

  const directoryLimited = collectMediaRootManifest(
    { kind: "comic", path: root },
    { maxDirectories: 1 },
  );
  assert.equal(directoryLimited.complete, false);
  assert.equal(directoryLimited.stop_reason, "max_directories");
});
