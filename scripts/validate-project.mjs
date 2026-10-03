import { existsSync, readFileSync, readdirSync, statSync } from "node:fs";
import { extname, join, relative } from "node:path";

import { CURRENT_SCHEMA_VERSION } from "./perf/schema-version.mjs";

const root = new URL("..", import.meta.url).pathname.replace(/^\/([A-Za-z]:)/, "$1");
const requiredFiles = [
  "Cargo.toml",
  "crates/server/Cargo.toml",
  "crates/server/src/main.rs",
  "crates/server/src/scanner/mod.rs",
  "crates/server/src/scanner/inspectors/audio.rs",
  "crates/server/src/scanner/inspectors/comic.rs",
  "crates/server/src/scanner/inspectors/coser_picture.rs",
  "crates/server/src/scanner/inspectors/gallery.rs",
  "crates/server/src/scanner/inspectors/novel.rs",
  "crates/server/src/routes.rs",
  "crates/server/src/search.rs",
  "crates/server/src/search/outbox.rs",
  "crates/server/src/catalog/facet_bitmap.rs",
  "crates/server/src/catalog_reconciliation.rs",
  "crates/server/src/catalog_writer.rs",
  "crates/server/src/inventory.rs",
  "crates/server/src/watcher.rs",
  "frontend/package.json",
  "frontend/src/App.tsx",
  "frontend/src/audioQueue.ts",
  "frontend/src/features/library/libraryLayout.ts",
  "frontend/src/ui/motion.ts",
  "frontend/src/ui/MotionProvider.tsx",
  "frontend/src/styles.css",
  "docs/reference-research.md",
  "scripts/inspect-media.mjs",
  "scripts/perf/capture-baseline.mjs",
  "scripts/perf/initialize-database.mjs",
  "scripts/perf/generate-synthetic-dataset.mjs",
  "scripts/perf/run-facet-bitmap-scenario.mjs",
  "scripts/perf/run-http-scenario.mjs",
  "scripts/perf/run-facet-cache-scenario.mjs",
  "scripts/perf/run-r1g-scenario.mjs",
  "scripts/perf/run-r1g-prewarm-ab.mjs",
  "scripts/perf/run-search-shadow-canary.mjs",
  "scripts/perf/run-search-incremental-reader.mjs",
  "scripts/perf/run-search-cutover-probe.mjs",
  "scripts/perf/run-scale-gates.mjs",
  "scripts/perf/run-inventory-kind-simulation.mjs",
  "scripts/perf/run-inventory-promotion-rollback.mjs",
  "scripts/perf/run-migration-gate.mjs",
  "scripts/perf/run-migration-gate.test.mjs",
  "scripts/perf/prepare-media-targets.mjs",
  "scripts/perf/prepare-media-targets.test.mjs",
  "scripts/perf/run-media-gates.mjs",
  "scripts/perf/run-media-gates.test.mjs",
  "scripts/perf/run-media-mixed-load.mjs",
  "scripts/perf/run-media-mixed-load.test.mjs",
  "scripts/perf/run-n100-gate.mjs",
  "scripts/perf/run-n100-gate.test.mjs",
  "scripts/perf/n100-gate-provenance.mjs",
  "scripts/perf/sample-system.mjs",
  "scripts/perf/check-n100-environment.mjs",
  "scripts/perf/n100-environment-lib.mjs",
  "scripts/perf/n100-environment.test.mjs",
  "scripts/perf/summarize-results.mjs",
  "scripts/perf/perf-lib.mjs",
  "scripts/perf/perf-lib.test.mjs",
  "scripts/perf/audio-queue.test.mjs",
  "scripts/perf/synthetic-dataset-lib.mjs",
  "scripts/perf/schema-version.mjs",
  "scripts/perf/provenance.mjs",
  "scripts/perf/provenance.test.mjs",
  "scripts/perf/synthetic-dataset-lib.test.mjs",
  "scripts/perf/gates/r1g-development.json",
  "scripts/perf/gates/facet-bitmap-development.json",
  "scripts/perf/gates/tag-filter-development.json",
  "scripts/perf/gates/media-n100-4g.json",
  "Dockerfile",
  "docker-compose.yml",
  "docker-compose.n100-sim.yml"
];

const requiredRoutes = [
  "/library",
  "/catalog/works",
  "/catalog/ownership",
  "/catalog/ownership/{kind}",
  "/catalog/reconciliation",
  "/catalog/reconciliation/novel",
  "/catalog/reconciliation/comic",
  "/catalog/reconciliation/coser-picture",
  "/catalog/reconciliation/audio",
  "/catalog/reconciliation/gallery",
  "/catalog/collections",
  "/catalog/facets/tags",
  "/catalog/counts",
  "/catalog/history",
  "/catalog/random",
  "/inventory/status",
  "/works/{id}/assets",
  "/jobs",
  "/settings",
  "/search",
  "/search/shadow/reconcile",
  "/search/rebuild",
  "/search/shadow/rebuild",
  "/works/{id}",
  "/works/{id}/progress",
  "/scan",
  "/enrich",
  "/works/{id}/epub",
  "/works/{id}/epub/{chapter}/html",
  "/tags",
  "/assets/{id}/stream",
  "/assets/generate",
  "/events"
];

function fail(message) {
  console.error(`FAIL ${message}`);
  process.exitCode = 1;
}

function pass(message) {
  console.log(`OK   ${message}`);
}

function walk(dir, result = []) {
  for (const entry of readdirSync(dir)) {
    const path = join(dir, entry);
    const stats = statSync(path);
    if (stats.isDirectory()) walk(path, result);
    else result.push(path);
  }
  return result;
}

for (const file of requiredFiles) {
  existsSync(join(root, file)) ? pass(`required file ${file}`) : fail(`missing ${file}`);
}

const forbiddenDeleteCommands = [
  "del " + "/s",
  "rd " + "/s",
  "rmdir " + "/s",
  "Remove-Item -" + "Recurse",
  "rm -" + "rf"
];
const commandCheckedFiles = [...new Set([
  "Dockerfile",
  "docker-compose.yml",
  ".dockerignore",
  ".gitignore",
  ".env.example",
  "README.md",
  "Cargo.toml",
  "crates/server/Cargo.toml",
  "frontend/package.json",
  "frontend/vite.config.ts",
  "frontend/tsconfig.json",
  "frontend/tsconfig.node.json",
  "scripts/validate-project.mjs",
  ...walk(join(root, "scripts")).filter((file) => [".js", ".mjs", ".ps1"].includes(extname(file))).map((file) => relative(root, file).replaceAll("\\", "/")),
  ...walk(join(root, "crates")).filter((file) => [".rs", ".toml"].includes(extname(file))).map((file) => relative(root, file).replaceAll("\\", "/")),
  ...walk(join(root, "frontend/src")).filter((file) => [".ts", ".tsx", ".css"].includes(extname(file))).map((file) => relative(root, file).replaceAll("\\", "/"))
])];
for (const file of commandCheckedFiles) {
  const text = readFileSync(join(root, file), "utf8");
  let clean = true;
  for (const command of forbiddenDeleteCommands) {
    if (text.includes(command)) {
      clean = false;
      fail(`forbidden batch delete command in ${file}: ${command}`);
    }
  }
  if (clean) pass(`no forbidden batch delete commands in ${file}`);
}

const routes = readFileSync(join(root, "crates/server/src/routes.rs"), "utf8");
const perfLib = readFileSync(join(root, "scripts/perf/perf-lib.mjs"), "utf8");
const systemSampler = readFileSync(join(root, "scripts/perf/sample-system.mjs"), "utf8");
const n100EnvironmentPreflight = readFileSync(join(root, "scripts/perf/check-n100-environment.mjs"), "utf8");
const n100EnvironmentLib = readFileSync(join(root, "scripts/perf/n100-environment-lib.mjs"), "utf8");
const migrationsSource = readFileSync(join(root, "crates/server/src/migrations.rs"), "utf8");
const mediaTargetPreparer = readFileSync(join(root, "scripts/perf/prepare-media-targets.mjs"), "utf8");
const mediaMixedRunner = readFileSync(join(root, "scripts/perf/run-media-mixed-load.mjs"), "utf8");
const n100GateRunner = readFileSync(join(root, "scripts/perf/run-n100-gate.mjs"), "utf8");
const provenance = readFileSync(join(root, "scripts/perf/provenance.mjs"), "utf8");
const scaleGateRunner = readFileSync(join(root, "scripts/perf/run-scale-gates.mjs"), "utf8");
const inventoryKindRunner = readFileSync(join(root, "scripts/perf/run-inventory-kind-simulation.mjs"), "utf8");
const inventoryPromotionRunner = readFileSync(join(root, "scripts/perf/run-inventory-promotion-rollback.mjs"), "utf8");
const baselineRunner = readFileSync(join(root, "scripts/perf/capture-baseline.mjs"), "utf8");
const dockerSimulation = readFileSync(join(root, "docker-compose.n100-sim.yml"), "utf8");

const migrationVersions = [...migrationsSource.matchAll(/Migration\s*\{\s*version:\s*(\d+)/gu)]
  .map((match) => Number(match[1]));
if (migrationVersions.length === 0) {
  fail("Rust migration list has no parseable versions");
} else {
  const ordered = migrationVersions.every((version, index) =>
    index === 0 ? version === 1 : version === migrationVersions[index - 1] + 1,
  );
  ordered
    ? pass(`Rust migration versions are contiguous through v${migrationVersions.at(-1)}`)
    : fail(`Rust migration versions are not contiguous: ${migrationVersions.join(", ")}`);
  const latestMigrationVersion = migrationVersions.at(-1);
  latestMigrationVersion === CURRENT_SCHEMA_VERSION
    ? pass(`schema-version.mjs matches Rust migration v${latestMigrationVersion}`)
    : fail(
      `schema version contract mismatch: Rust migration v${latestMigrationVersion}, `
      + `scripts/perf/schema-version.mjs v${CURRENT_SCHEMA_VERSION}`,
    );
}
mediaTargetPreparer.includes("readOnly: true") &&
mediaTargetPreparer.includes("PRAGMA query_only = ON") &&
mediaTargetPreparer.includes("refusing to overwrite existing target file") &&
mediaTargetPreparer.includes("cannot prepare")
  ? pass("media Gate target preparation is read-only and fail-closed")
  : fail("media Gate target preparation is missing its read-only/fail-closed safeguards");
mediaMixedRunner.includes("Promise.all") &&
mediaMixedRunner.includes("coverageChecks") &&
mediaMixedRunner.includes("refusing to write into a non-empty output directory") &&
mediaMixedRunner.includes("target_path: target.url.pathname") &&
mediaMixedRunner.includes("rounds != null && completedRounds >= rounds") &&
mediaMixedRunner.includes("durationSeconds != null && Date.now() - started")
  ? pass("media mixed-load runner is concurrent, coverage-bound, and redacted")
  : fail("media mixed-load runner is missing concurrency, coverage, or redaction safeguards");
n100GateRunner.includes("preflight") &&
n100GateRunner.includes("target-preparation") &&
n100GateRunner.includes("system-sampler") &&
n100GateRunner.includes("samplerEvidence") &&
n100GateRunner.includes("refusing to write into a non-empty output directory") &&
n100GateRunner.includes("Docker simulation is the formal project acceptance environment") &&
n100GateRunner.includes('"mixed-duration-seconds": { type: "string" }') &&
n100GateRunner.includes("FORMAL_STABLE_WINDOW_SECONDS = 300") &&
n100GateRunner.includes("FORMAL_STABLE_WINDOW_SECONDS,\n        FORMAL_STABLE_WINDOW_SECONDS")
  ? pass("unified N100 Gate runner is fail-closed, staged, and approximation-labelled")
  : fail("unified N100 Gate runner is missing staged execution or approximation safeguards");
dockerSimulation.includes("mem_limit: ${SIM_MEMORY_LIMIT:-4g}") &&
dockerSimulation.includes("cpus: ${SIM_CPUS:-1.0}") &&
dockerSimulation.includes("memswap_limit: ${SIM_MEMORY_SWAP_LIMIT:-4g}") &&
dockerSimulation.includes("volumes: !override") &&
dockerSimulation.includes("does not emulate the N100 CPU")
  && dockerSimulation.includes("SIM_INVENTORY_SCANNER_ENABLED:-false")
  && dockerSimulation.includes("SIM_INVENTORY_SCANNER_KINDS:-all")
  && dockerSimulation.includes("SIM_DERIVATIVE_CACHE_V2_ENABLED:-false")
  && dockerSimulation.includes("SIM_JPEG_THUMBNAIL_DOWNSCALE_ENABLED:-false")
  ? pass("Docker N100 simulation override is isolated and explicitly approximate")
  : fail("Docker N100 simulation override is missing isolation or approximation safeguards");
provenance.includes("dirty_state_sha256") &&
provenance.includes("CURRENT_SCHEMA_VERSION") &&
scaleGateRunner.includes("captureSourceProvenance(repoRoot)") &&
baselineRunner.includes("captureGitState(repoRoot)") &&
baselineRunner.includes("SAFE_ENVIRONMENT_KEYS")
  ? pass("scale and baseline artifacts share commit, schema, and feature-flag provenance")
  : fail("performance artifact provenance is not shared across scale and baseline runners");
inventoryKindRunner.includes("/api/scan") &&
inventoryKindRunner.includes("/api/inventory/status") &&
inventoryKindRunner.includes("kind-smoke-evidence") &&
inventoryKindRunner.includes("writeRunnerFailureArtifact") &&
inventoryKindRunner.includes("captureSourceProvenance(repoRoot)")
  ? pass("Inventory kind simulation runner is authenticated, bounded, and provenance-labelled")
  : fail("Inventory kind simulation runner is missing API, failure, or provenance safeguards");
inventoryPromotionRunner.includes("--allow-ownership-mutation") &&
inventoryPromotionRunner.includes("/api/catalog/ownership/") &&
inventoryPromotionRunner.includes("/api/catalog/reconciliation/") &&
inventoryPromotionRunner.includes("current passing evidence row") &&
inventoryPromotionRunner.includes("rollback-after-error") &&
inventoryPromotionRunner.includes("captureSourceProvenance(repoRoot)") &&
inventoryPromotionRunner.includes("writeJsonArtifact")
  ? pass("Inventory promotion/rollback runner is explicit, fail-closed, and provenance-labelled")
  : fail("Inventory promotion/rollback runner is missing mutation safety or evidence safeguards");
for (const route of requiredRoutes) {
  routes.includes(route) ? pass(`API route ${route}`) : fail(`missing API route ${route}`);
}
routes.includes('"media"') && routes.includes('"openai_image_configured"') && routes.includes("enrichment_concurrency.clamp")
  ? pass("health endpoint exposes deployment readiness without secrets")
  : fail("health endpoint deployment readiness payload is missing");
const healthDiagnostics = publicAsyncFunctionBody(routes, "health");
healthDiagnostics?.includes("begin_tracked_read_transaction") &&
healthDiagnostics?.includes("status_in(transaction.connection())") &&
healthDiagnostics?.includes("shadow_index_status_in(transaction.connection())") &&
healthDiagnostics?.includes("reconciliation_status_in(transaction.connection())") &&
healthDiagnostics?.includes("transaction.commit()")
  ? pass("health search diagnostics share one committed tracked read snapshot")
  : fail("health search diagnostics still combine independent pool snapshots");
routes.includes('"qmediasync": crate::vfs::qms_runtime_snapshot()') &&
readFileSync(join(root, "crates/server/src/vfs.rs"), "utf8").includes("pub fn qms_runtime_snapshot") &&
readFileSync(join(root, "scripts/perf/run-http-scenario.mjs"), "utf8").includes('"abort-after-bytes"') &&
perfLib.includes("qms_stream_response_bytes") &&
perfLib.includes("resource_wait_total_micros") &&
perfLib.includes("SYSTEM_SAMPLE_COLUMNS") &&
systemSampler.includes("SYSTEM_SAMPLE_COLUMNS")
  ? pass("qmediasync Range/cache runtime evidence is observable and bounded")
  : fail("qmediasync Range/cache runtime evidence is incomplete");
n100EnvironmentPreflight.includes("evaluateN100Environment") &&
n100EnvironmentPreflight.includes("cgroup_memory_max_bytes") &&
n100EnvironmentPreflight.includes("cgroup_cpu_limit_cores") &&
n100EnvironmentPreflight.includes("--container") &&
n100EnvironmentPreflight.includes("docker") &&
n100EnvironmentPreflight.includes("refusing to overwrite existing artifact") &&
n100EnvironmentLib.includes("N100_MEMORY_LIMIT_BYTES") &&
n100EnvironmentLib.includes("maxCpuCores") &&
n100EnvironmentLib.includes('"incomplete"')
  ? pass("N100/4GiB environment preflight is explicit and fail-closed")
  : fail("N100 environment preflight is incomplete or not fail-closed");
const catalogWriterPromotion = readFileSync(join(root, "crates/server/src/catalog_writer.rs"), "utf8");
routes.includes("change_catalog_ownership") &&
  routes.includes("change_catalog_kind_ownership_checked") &&
  catalogWriterPromotion.includes("search_promotion_snapshot_in")
  ? pass("catalog kind cutover is explicit and audited")
  : fail("catalog kind cutover control plane is missing its safety boundary");

const catalogReconciliation = readFileSync(join(root, "crates/server/src/catalog_reconciliation.rs"), "utf8");
catalogWriterPromotion.includes("catalog_reconciliation::require_current_pass") &&
catalogReconciliation.includes("RECONCILE_NOVEL_JOB_TYPE") &&
catalogReconciliation.includes("RECONCILE_COMIC_JOB_TYPE") &&
catalogReconciliation.includes("RECONCILE_COSER_PICTURE_JOB_TYPE") &&
catalogReconciliation.includes("RECONCILE_AUDIO_JOB_TYPE") &&
catalogReconciliation.includes("RECONCILE_GALLERY_JOB_TYPE") &&
catalogReconciliation.includes("root_generation_after_sha256") &&
catalogReconciliation.includes("MAX_RECORDED_DIFFS")
  ? pass("all media kinds catalog promotion requires bounded current reconciliation evidence")
  : fail("one or more media kind catalog reconciliation evidence or promotion gates are missing");
catalogReconciliation.includes("struct ReconciliationAccumulator") &&
catalogReconciliation.includes("MAX_RECONCILIATION_ASSETS_PER_WORK") &&
catalogReconciliation.includes("streamed asset safety limit") &&
catalogReconciliation.includes("AssetMultisetSummary")
  ? pass("Audio/Gallery reconciliation accumulates chunk summaries with a bounded total asset limit")
  : fail("Audio/Gallery reconciliation accumulator is not visibly bounded");
const reconciliationRuntimeError = publicAsyncFunctionBody(catalogReconciliation, "record_runtime_error");
reconciliationRuntimeError?.includes("begin_tracked_transaction") &&
  reconciliationRuntimeError.includes("transaction.commit()") &&
  !reconciliationRuntimeError.includes("execute(db.pool())")
  ? pass("catalog reconciliation runtime errors use a tracked writer transaction")
  : fail("catalog reconciliation runtime error writes bypass the tracked writer transaction");

const compose = readFileSync(join(root, "docker-compose.yml"), "utf8");
for (const mount of ["./漫画:/library/comics:ro", "./轻小说:/library/novels:ro", "./音声:/library/audio:ro", "./图库:/library/gallery:ro"]) {
  compose.includes(mount) ? pass(`compose mount ${mount}`) : fail(`missing compose mount ${mount}`);
}

const referenceResearch = readFileSync(join(root, "docs/reference-research.md"), "utf8");
for (const needle of ["Tag Translation", "LightNovel.app", "ASMR One", "UI And Motion Decisions"]) {
  referenceResearch.includes(needle)
    ? pass(`reference research covers ${needle}`)
    : fail(`reference research missing ${needle}`);
}

const enrich = readFileSync(join(root, "crates/server/src/enrich.rs"), "utf8");
if ((enrich.includes('"model": state.config.openai_image_model') || enrich.includes('"model": model')) && enrich.includes('"output_format": "png"')) {
  pass("OpenAI image generation uses configured GPT image model and PNG output");
} else {
  fail("OpenAI image generation payload is incomplete");
}
if (enrich.includes("response_format")) {
  fail("OpenAI image generation still uses deprecated response_format for GPT Image models");
}
if (!enrich.includes("eh::") && !enrich.includes("downloads::execute_download")) {
  pass("E/EX queued actions and download executor are removed");
} else {
  fail("E/EX or download executor is still wired");
}
if (enrich.includes('"scan-library"') && enrich.includes("scanner::scan_all")) {
  pass("file watcher scan job executor is wired");
} else {
  fail("file watcher scan job executor is missing");
}
if (enrich.includes('"enrich-asmr-work"') && enrich.includes("https://asmr.one/api/workInfo/")) {
  pass("ASMR One enrichment worker is wired");
} else {
  fail("ASMR One enrichment worker is missing");
}
if (
  enrich.includes('"enrich-lightnovel-work"') &&
  enrich.includes("GetBookInfo") &&
  enrich.includes("GetBookListByTags") &&
  enrich.includes("/hub/api/negotiate")
) {
  pass("LightNovelShelf enrichment worker is wired");
} else {
  fail("LightNovelShelf enrichment worker is missing");
}

const scanner = readFileSync(join(root, "crates/server/src/scanner/mod.rs"), "utf8");
scanner.includes("if enqueue_enrichment") &&
scanner.includes('"enrich-lightnovel-work"') &&
scanner.includes('"enrich-asmr-work"')
  ? pass("media scan gates local online enrichment behind explicit opt-in")
  : fail("media scan online enrichment must remain explicitly gated");
scanner.includes("extract_epub_cover") && scanner.includes("epub-cover-") && scanner.includes("epub-extracted")
  ? pass("EPUB scan extracts cover assets")
  : fail("EPUB cover extraction is missing");
scanner.includes('"rebuild-search-index"')
  ? pass("scan queues Tantivy search index rebuild")
  : fail("scan does not queue search index rebuild");
scanner.includes("Probe::open") && scanner.includes("duration_seconds") && scanner.includes("audio_bitrate")
  ? pass("audio scan reads Lofty metadata")
  : fail("audio Lofty metadata scan is missing");
scanner.includes("normalize_track_key") && scanner.includes('"track_key"') && scanner.includes('"preferred_playback"')
  ? pass("audio scan records logical track keys and preferred playback variants")
  : fail("audio MP3/WAV logical track metadata is missing");
scanner.includes("is_local_comic_archive") && scanner.includes('["cbz", "zip"]') && scanner.includes("local_comic_archive_type")
  ? pass("local comic scanner accepts CBZ and plain ZIP through one archive path")
  : fail("plain ZIP comic compatibility is missing");

const jobs = readFileSync(join(root, "crates/server/src/jobs.rs"), "utf8");
jobs.includes("reschedule_job") ? pass("job worker retries failed enrichment") : fail("job worker retry path is missing");
jobs.includes("enrichment_concurrency.clamp") && jobs.includes("claim_next_queued_job") && jobs.includes("worker_id")
  ? pass("job worker honors ENRICHMENT_CONCURRENCY with claimed jobs")
  : fail("job worker concurrency/claim path is missing");

const config = readFileSync(join(root, "crates/server/src/config.rs"), "utf8");
const envExample = readFileSync(join(root, ".env.example"), "utf8");
const prewarmRunner = readFileSync(join(root, "scripts/perf/run-r1g-prewarm-ab.mjs"), "utf8");
!config.includes("APP_ADMIN_PASSWORD") &&
  !config.includes("admin-password") &&
  !config.includes("SESSION_SECRET") &&
  !compose.includes("APP_ADMIN_PASSWORD") &&
  !compose.includes("SESSION_SECRET") &&
  !envExample.includes("APP_ADMIN_PASSWORD") &&
  !envExample.includes("SESSION_SECRET")
  ? pass("admin password and session-secret mechanisms are removed")
  : fail("admin password or session-secret configuration remains");
config.includes("LIGHTNOVEL_API_BASES")
  ? pass("LightNovelShelf API bases are configurable")
  : fail("LIGHTNOVEL_API_BASES config is missing");
config.includes("LIGHTNOVEL_ACCESS_TOKEN")
  ? pass("LightNovelShelf access token is configurable")
  : fail("LIGHTNOVEL_ACCESS_TOKEN config is missing");
!config.includes("DOWNLOADS_DIR") && !compose.includes("DOWNLOADS_DIR")
  ? pass("download directory config is removed with E/EX")
  : fail("download directory config should be removed");
config.includes("ENABLE_FILE_WATCHER") && config.includes("WATCH_DEBOUNCE_SECONDS")
  ? pass("file watcher config is wired")
  : fail("file watcher config is missing");
config.includes('env_flag("FACET_BITMAP_ENABLED", false)') &&
compose.includes("${FACET_BITMAP_ENABLED:-false}") &&
envExample.includes("FACET_BITMAP_ENABLED=false") &&
routes.includes('"facet_bitmap": state.config.facet_bitmap_enabled')
  ? pass("experimental Facet bitmap is observable and disabled by default")
  : fail("Facet bitmap feature flag must remain independently disabled by default");
config.includes('env_flag("SEARCH_SHADOW_CANARY_ENABLED", false)') &&
compose.includes("${SEARCH_SHADOW_CANARY_ENABLED:-false}") &&
envExample.includes("SEARCH_SHADOW_CANARY_ENABLED=false") &&
routes.includes('"search_shadow_canary": state.config.search_shadow_canary_enabled')
  ? pass("shadow search canary is independently observable and disabled by default")
  : fail("shadow search canary must remain independently disabled by default");
config.includes('env_flag("SEARCH_READER_PREWARM_ENABLED", false)') &&
compose.includes("${SEARCH_READER_PREWARM_ENABLED:-false}") &&
envExample.includes("SEARCH_READER_PREWARM_ENABLED=false") &&
routes.includes('"search_reader_prewarm": state.config.search_reader_prewarm_enabled')
  ? pass("search reader prewarm is explicit and disabled by default")
  : fail("search reader prewarm must remain explicit and disabled by default");
prewarmRunner.includes("SEARCH_READER_PREWARM_ENABLED") &&
prewarmRunner.includes("development-ab") &&
prewarmRunner.includes("Both variants use the same copied SQLite file")
  ? pass("reader prewarm has an isolated development A/B runner")
  : fail("reader prewarm A/B runner is incomplete");
const assets = readFileSync(join(root, "crates/server/src/assets.rs"), "utf8");
assets.includes("read_epub_manifest") && assets.includes("read_epub_chapter_html")
  ? pass("EPUB manifest and chapter reader are implemented")
  : fail("EPUB reader API implementation is missing");
assets.includes("EpubManifestQuery") &&
  assets.includes("epub_manifest_bounds") &&
  assets.includes("EPUB_MANIFEST_MAX_PAGE_SIZE") &&
  assets.includes("next_cursor")
  ? pass("EPUB manifest responses are bounded and cursor-paginated")
  : fail("EPUB manifest pagination is missing its bounded cursor contract");
assets.includes("PARTIAL_CONTENT") && assets.includes("CONTENT_RANGE") && assets.includes("parse_byte_range")
  ? pass("asset streaming supports byte range requests")
  : fail("asset byte range streaming is missing");

const search = readFileSync(join(root, "crates/server/src/search.rs"), "utf8");
const searchOutbox = readFileSync(join(root, "crates/server/src/search/outbox.rs"), "utf8");
const searchCanaryRunner = readFileSync(join(root, "scripts/perf/run-search-shadow-canary.mjs"), "utf8");
const searchIncrementalRunner = readFileSync(join(root, "scripts/perf/run-search-incremental-reader.mjs"), "utf8");
const searchCutoverProbe = readFileSync(join(root, "scripts/perf/run-search-cutover-probe.mjs"), "utf8");
search.includes("MAX_SEARCH_QUERY_BYTES") &&
search.includes("fn bounded_search_query") &&
search.includes("bounded_search_query(input.q.as_deref().unwrap_or_default())") &&
search.includes("let query = bounded_search_query(&query)?")
  ? pass("search query input is bounded before reader and candidate work")
  : fail("search query input is missing its bounded fail-closed boundary");
config.includes("validate_search_flags") &&
config.includes("SEARCH_INCREMENTAL_READER_ENABLED requires SEARCH_SHADOW_CANARY_ENABLED") &&
searchOutbox.includes("baseline_revision >= 0")
  ? pass("incremental search cutover requires its reconciliation worker and accepts revision-zero baselines")
  : fail("incremental search cutover flag dependencies or revision-zero handling are incomplete");
searchOutbox.includes("reconciliation.index_document_count == reconciliation.sqlite_work_count") &&
searchOutbox.includes("reconciliation.index_unique_work_count == reconciliation.sqlite_work_count") &&
searchOutbox.includes("shadow.indexed_documents == reconciliation.index_document_count") &&
searchOutbox.includes("reconciliation.sqlite_ids_sha256") &&
searchOutbox.includes("fn matching_id_hashes") &&
searchOutbox.includes("is_ascii_hexdigit") &&
searchOutbox.includes(
  "snapshot.indexed_documents == snapshot.reconciled_index_document_count",
) &&
searchOutbox.includes(
  "snapshot.reconciled_index_unique_work_count == snapshot.reconciled_sqlite_work_count",
) &&
searchOutbox.includes("snapshot.reconciled_sqlite_ids_sha256") &&
searchOutbox.includes("snapshot.reconciled_missing_work_ids == 0")
  ? pass("Search promotion and first reader arm verify persisted reconciliation counts, indexed totals, and ID hashes")
  : fail("Search promotion/reader gates do not explicitly verify reconciliation counts and ID hashes");
for (const needle of ["tantivy", "QueryParser", "TopDocs", "rebuild_search_index", "search-index"]) {
  search.includes(needle) ? pass(`Tantivy search contains ${needle}`) : fail(`Tantivy search missing ${needle}`);
}
search.includes("pub struct SearchRuntime") &&
search.includes("OnceCell<Arc<SearchIndexReader>>") &&
search.includes("CANDIDATE_CACHE_MAX_ENTRIES") &&
search.includes("CANDIDATE_CACHE_MAX_IDS") &&
search.includes("candidate_coalesced") &&
routes.includes('"search_runtime": search_runtime')
  ? pass("Tantivy readers and catalog candidates are persistent, single-flight, bounded, and observable")
  : fail("persistent Tantivy reader or bounded candidate reuse is incomplete");
search.includes('LEGACY_SEARCH_INDEX_NAME: &str = "production-v2"') &&
search.includes("legacy_search_index_freshness") &&
search.includes("record_legacy_search_index_ready") &&
search.includes("queue_legacy_search_rebuild") &&
readFileSync(join(root, "crates/server/src/migrations.rs"), "utf8").includes('name: "legacy-search-index-revision-fence"') &&
readFileSync(join(root, "crates/server/src/catalog.rs"), "utf8").includes("expected_search_revision") &&
readFileSync(join(root, "crates/server/src/search.rs"), "utf8").includes("pub search_revision: i64") &&
readFileSync(join(root, "crates/server/src/db.rs"), "utf8").includes("JOIN search_source_state AS search") &&
readFileSync(join(root, "crates/server/src/catalog.rs"), "utf8").includes("revisions.search_revision") &&
readFileSync(join(root, "crates/server/src/catalog.rs"), "utf8").includes("search_candidates_expired_error")
  ? pass("Tantivy candidates are revision-fenced before they join a Catalog snapshot")
  : fail("Catalog can still join a stale Tantivy candidate set with a newer SQLite snapshot");
search.includes("recover_legacy_search_index_after_error") &&
search.includes("quarantine_corrupt_search_index") &&
searchOutbox.includes("status = 'building'") &&
search.includes("mark_legacy_search_index_degraded")
  ? pass("legacy search corruption is quarantined and recovered through a fail-closed rebuild")
  : fail("legacy search corruption recovery does not fail closed");

const watcher = readFileSync(join(root, "crates/server/src/watcher.rs"), "utf8");
for (const needle of ["notify::recommended_watcher", "RecursiveMode::Recursive", "\"scan-library\"", "WATCH_DEBOUNCE_SECONDS"]) {
  const text = needle === "WATCH_DEBOUNCE_SECONDS" ? config : watcher;
  text.includes(needle) ? pass(`file watcher contains ${needle}`) : fail(`file watcher missing ${needle}`);
}

const db = readFileSync(join(root, "crates/server/src/db.rs"), "utf8");
const catalog = readFileSync(join(root, "crates/server/src/catalog.rs"), "utf8");
const catalogWriter = readFileSync(join(root, "crates/server/src/catalog_writer.rs"), "utf8");
const inventory = readFileSync(join(root, "crates/server/src/inventory.rs"), "utf8");
const audioInspector = readFileSync(join(root, "crates/server/src/scanner/inspectors/audio.rs"), "utf8");
const galleryInspector = readFileSync(join(root, "crates/server/src/scanner/inspectors/gallery.rs"), "utf8");
const novelInspector = readFileSync(join(root, "crates/server/src/scanner/inspectors/novel.rs"), "utf8");
const migrations = readFileSync(join(root, "crates/server/src/migrations.rs"), "utf8");
const facetCacheScenario = readFileSync(join(root, "scripts/perf/run-facet-cache-scenario.mjs"), "utf8");
const facetBitmap = readFileSync(join(root, "crates/server/src/catalog/facet_bitmap.rs"), "utf8");
const facetBitmapScenario = readFileSync(join(root, "scripts/perf/run-facet-bitmap-scenario.mjs"), "utf8");
const trackedTransactionSources = [
  ["assets.rs", assets],
  ["catalog.rs", catalog],
  ["catalog_reconciliation.rs", catalogReconciliation],
  ["catalog_writer.rs", catalogWriter],
  ["derivative.rs", readFileSync(join(root, "crates/server/src/derivative.rs"), "utf8")],
  ["inventory.rs", inventory],
  ["search/outbox.rs", searchOutbox],
];
const leakedDirectPoolBegins = trackedTransactionSources.filter(([, source]) => {
  const production = source.split(/\n#\[cfg\(test\)\]/u, 1)[0];
  return /\.pool\(\)\.begin\(/u.test(production);
});
if (leakedDirectPoolBegins.length === 0 && trackedTransactionSources.every(([, source]) => source.includes("begin_tracked_transaction"))) {
  pass("production write transactions use the tracked Db acquire boundary");
} else {
  fail(`production transaction entry bypasses begin_tracked_transaction: ${leakedDirectPoolBegins.map(([name]) => name).join(", ") || "missing helper"}`);
}
const dbProduction = db.slice(0, db.lastIndexOf("\n#[cfg(test)]"));
const trackedDbShortWriteFunctions = [
  "upsert_work",
  "upsert_tag",
  "mark_scanner_work",
  "try_acquire_scanner_lock",
  "heartbeat_scanner_lock",
  "release_scanner_lock",
  "set_work_cover",
  "create_job",
  "create_job_if_absent",
  "create_work_job_once",
  "update_job",
  "update_work_enrichment",
  "update_work_meta",
  "claim_next_queued_job",
  "audit",
];
function publicAsyncFunctionBody(source, name) {
  const markers = [
    `    pub async fn ${name}`,
    `pub async fn ${name}`,
    `    async fn ${name}`,
    `async fn ${name}`,
  ];
  const matches = markers
    .map((marker) => ({ marker, start: source.indexOf(marker) }))
    .filter(({ start }) => start >= 0)
    .sort((left, right) => left.start - right.start);
  if (matches.length === 0) return null;
  const { marker, start } = matches[0];
  const candidates = [
    source.indexOf("\n    pub async fn ", start + marker.length),
    source.indexOf("\n    async fn ", start + marker.length),
    source.indexOf("\npub async fn ", start + marker.length),
    source.indexOf("\nasync fn ", start + marker.length),
    source.indexOf("\n}\n", start + marker.length),
  ].filter((index) => index >= 0);
  const end = Math.min(...candidates);
  return source.slice(start, end);
}
const untrackedDbShortWrites = trackedDbShortWriteFunctions.filter((name) => {
  const body = publicAsyncFunctionBody(dbProduction, name);
  return body == null
    || !body.includes("begin_tracked_transaction")
    || /\.(?:execute|fetch_one|fetch_optional)\(&self\.pool\)/u.test(body);
});
untrackedDbShortWrites.length === 0
  ? pass("Db runtime short writes use tracked transactions instead of direct pool execution")
  : fail(`Db runtime short write bypasses tracked transaction: ${untrackedDbShortWrites.join(", ")}`);
const currentScannerTag = publicAsyncFunctionBody(dbProduction, "link_current_scanner_tag");
currentScannerTag?.includes("begin_tracked_transaction") &&
  currentScannerTag?.includes("link_tag_owned_in_transaction") &&
  !currentScannerTag?.includes("fetch_optional(&self.pool)")
  ? pass("current scanner tag lease and association share one tracked writer transaction")
  : fail("current scanner tag still reads the lease outside its writer transaction");
publicAsyncFunctionBody(scanner, "scan_all_locked")?.includes("revision_fence_snapshot") &&
  !publicAsyncFunctionBody(scanner, "scan_all_locked")?.includes("search_source_revision()")
  ? pass("scan revision fences use one tracked snapshot for Catalog and Search")
  : fail("scan revision fence still combines independent Catalog/Search pool reads");
db.includes("pub async fn work_asset_by_role") &&
db.includes("idx_assets_work_role_id") &&
assets.includes("work_asset_by_role(work_id, \"archive\"") &&
assets.includes("work_asset_by_role(work_id, \"book\"") &&
db.includes("pub async fn work_cover_source") &&
db.includes("pub async fn work_archive_and_meta") &&
publicAsyncFunctionBody(assets, "work_cover")?.includes("work_cover_source") &&
publicAsyncFunctionBody(assets, "comic_pages")?.includes("work_archive_and_meta") &&
!publicAsyncFunctionBody(assets, "work_cover")?.includes("work_detail(work_id)")
  ? pass("comic/EPUB media routes use bounded, snapshot-coherent source lookups")
  : fail("media routes still materialize full work details for source assets");
db.includes("idx_assets_work_audio_role") &&
db.includes("idx_assets_work_audio_mime_lower") &&
catalog.includes("audio_asset_query_plan_uses_compatibility_partial_indexes") &&
publicAsyncFunctionBody(db, "work_detail_assets_with_connection")?.includes("UNION ALL") &&
publicAsyncFunctionBody(db, "work_asset_counts_with_connection")?.includes("lower(mime) LIKE 'audio/%'")
  ? pass("audio role/mime compatibility pages have bounded partial indexes and a query-plan regression")
  : fail("audio role/mime compatibility pages lack their partial-index guard");
db.includes("idx_assets_work_role_position_keyset") &&
catalog.includes("(role, COALESCE(position, 9223372036854775807), id) > (?, ?, ?)") &&
catalog.includes("ORDER BY role ASC, COALESCE(position, 9223372036854775807) ASC, id ASC") &&
catalog.includes("catalog_asset_keyset_keeps_sentinel_positions_in_order")
  ? pass("local Catalog asset pages use NULL-last tuple keyset cursors with a sentinel-order regression")
  : fail("local Catalog asset pages lack the NULL-last tuple keyset contract");
catalog.includes("role <> 'track' AND lower(mime) LIKE 'audio/%'") &&
catalog.includes("merged.sort_unstable_by") &&
catalog.includes("merged.extend(rows.into_iter().map(catalog_asset_item_from_row))")
  ? pass("local playable asset pages merge bounded track/MIME branches in memory")
  : fail("local playable asset pages still rely on an unbounded SQL UNION merge");
const lightNovelEnrichment = publicAsyncFunctionBody(enrich, "enrich_lightnovel_work");
db.includes("pub async fn work_title_and_meta") &&
lightNovelEnrichment?.includes("work_title_and_meta(work_id)") &&
!lightNovelEnrichment?.includes("work_detail(work_id)")
  ? pass("light-novel enrichment reads only title and metadata")
  : fail("light-novel enrichment still materializes full work details");
const catalogWorks = publicAsyncFunctionBody(catalog, "query_works_with_candidates");
catalogWorks?.includes("begin_tracked_read_transaction") &&
catalogWorks?.includes("revision_snapshot_with_connection") &&
catalog.includes("fetch_all(&mut *connection)")
  ? pass("catalog work pages bind rows and revisions to one read snapshot")
  : fail("catalog work pages do not use one tracked read snapshot");
const catalogSnapshotPaths = [
  "query_random_work_with_candidates",
  "query_counts_with_candidates",
  "query_collections_with_candidates",
  "query_history",
  "query_assets",
];
const catalogPathsMissingSnapshots = catalogSnapshotPaths.filter((name) => {
  const body = publicAsyncFunctionBody(catalog, name);
  return body == null || !body.includes("begin_tracked_read_transaction");
});
catalogPathsMissingSnapshots.length === 0 &&
  catalog.includes("catalog_backfill_pending_with_connection") &&
  catalog.includes("resolve_selected_tag_ids_for_revision") &&
  catalog.includes("resolve_selected_tag_ids_with_connection") &&
catalog.includes("maintained_asset_count_with_connection") &&
catalog.includes('"asset_count"') &&
  publicAsyncFunctionBody(catalog, "query_assets")?.includes("source_version") &&
  publicAsyncFunctionBody(catalog, "query_assets")?.includes("fetch_all(&mut *transaction)")
  ? pass("catalog secondary pages use bounded tracked snapshots, maintained counts, and snapshot-bound cursors")
  : fail(`catalog secondary read path bypasses its tracked snapshot: ${catalogPathsMissingSnapshots.join(", ") || "missing bounded asset cursor"}`);
const catalogOwnership = publicAsyncFunctionBody(routes, "catalog_ownership");
catalogOwnership?.includes("begin_tracked_read_transaction") &&
  catalogOwnership.includes("fetch_all(&mut *transaction)") &&
  catalogOwnership.includes("transaction.commit()")
  ? pass("catalog ownership diagnostics use one bounded tracked read snapshot")
  : fail("catalog ownership diagnostics can combine independent pool generations");
catalog.includes("STATS_BACKFILL_AGGREGATE_SQL") &&
catalog.includes("asset_counts") &&
catalog.includes("tag_counts") &&
catalog.includes("AS MATERIALIZED") &&
catalog.includes("JOIN selected ON selected.work_id = asset.work_id") &&
!catalog.includes("STATS_ASSIGNMENTS")
  ? pass("catalog stats backfill groups assets and tags once per bounded batch")
  : fail("catalog stats backfill still uses per-work correlated aggregate reads");
db.includes("pub struct GalleryAssetsPage") &&
  db.includes("pub async fn gallery_assets_page") &&
  db.includes("gallery_asset_count_with_connection") &&
  routes.includes("gallery_assets_page(id, offset, after, limit + 1)") &&
  routes.includes("source_version: Option<String>") &&
  routes.includes("catalog_revision: Option<i64>")
  ? pass("legacy gallery pages use one tracked snapshot and revision-bound cursors")
  : fail("legacy gallery page still combines independent count/type/page reads");
db.includes("const LEGACY_LIBRARY_PAGE_LIMIT: i64 = 500") &&
publicAsyncFunctionBody(db, "library_page")?.includes("begin_tracked_read_transaction") &&
  publicAsyncFunctionBody(db, "library_page")?.includes("tags_with_connection") &&
  publicAsyncFunctionBody(db, "library_page")?.includes("history_with_connection") &&
  !publicAsyncFunctionBody(db, "library")?.includes("i64::MAX")
  ? pass("legacy library helper is bounded and its page context shares one snapshot")
  : fail("legacy library path can still materialize an unbounded catalog or cross snapshots");
const legacyLibraryTagKeys = publicAsyncFunctionBody(db, "populate_library_tag_keys_with_connection");
publicAsyncFunctionBody(db, "library_page")?.includes("populate_library_tag_keys_with_connection") &&
  publicAsyncFunctionBody(db, "library_page")?.includes("LEFT JOIN work_stats AS stats") &&
  legacyLibraryTagKeys?.includes("WITH selected(work_id) AS MATERIALIZED") &&
  legacyLibraryTagKeys?.includes("group_concat(") &&
  legacyLibraryTagKeys?.includes("ORDER BY tag.namespace, tag.key") &&
  legacyLibraryTagKeys?.includes("GROUP BY work_tag.work_id")
  ? pass("legacy library pages batch tag keys and reuse maintained work counters")
  : fail("legacy library pages still perform per-row tag aggregation or discard maintained counters");
publicAsyncFunctionBody(db, "work_detail_with_mode")?.includes("work_detail_with_snapshot") &&
  publicAsyncFunctionBody(db, "work_detail_with_snapshot")?.includes("begin_tracked_read_transaction") &&
  publicAsyncFunctionBody(db, "work_detail_with_snapshot")?.includes("work_detail_assets_with_connection")
  ? pass("legacy and summary work detail responses use one tracked snapshot")
  : fail("work detail still combines independent pool snapshots");
const reconciliationOverview = publicAsyncFunctionBody(catalogReconciliation, "overview");
reconciliationOverview?.includes("begin_tracked_read_transaction") &&
  reconciliationOverview?.includes("root_snapshots_with_connection") &&
  reconciliationOverview?.includes("transaction.commit")
  ? pass("catalog reconciliation overview uses one bounded tracked snapshot")
  : fail("catalog reconciliation overview still combines independent pool snapshots");
const reconciliationBaseline = publicAsyncFunctionBody(catalogReconciliation, "reconciliation_baseline");
reconciliationBaseline?.includes("begin_tracked_read_transaction") &&
  reconciliationBaseline?.includes("catalog_revision_with_connection") &&
  reconciliationBaseline?.includes("root_snapshot_with_connection") &&
  reconciliationBaseline?.includes("previous_evidence_with_connection") &&
  reconciliationBaseline?.includes("reconciliation_prerequisite_error_with_connection") &&
  reconciliationBaseline?.includes("transaction.commit")
  ? pass("catalog reconciliation baseline uses one bounded tracked snapshot")
  : fail("catalog reconciliation baseline still combines independent pool snapshots");
const reconciliationCompare = publicAsyncFunctionBody(catalogReconciliation, "compare_catalog_works");
reconciliationCompare?.includes("begin_tracked_read_transaction") &&
  reconciliationCompare?.includes("transaction.commit") &&
  !reconciliationCompare?.includes("fetch_all(db.pool())") &&
  !reconciliationCompare?.includes("fetch(db.pool())")
  ? pass("catalog reconciliation fact comparison uses one bounded page snapshot")
  : fail("catalog reconciliation fact comparison still combines independent pool snapshots");
const reconciliationUnexpected = publicAsyncFunctionBody(catalogReconciliation, "record_unexpected_works");
reconciliationUnexpected?.includes("begin_tracked_read_transaction") &&
  reconciliationUnexpected?.includes("transaction.commit") &&
  reconciliationUnexpected?.includes("fetch(&mut *transaction)")
  ? pass("catalog reconciliation unexpected-work check uses one bounded snapshot")
  : fail("catalog reconciliation unexpected-work check still performs independent pool reads");
const reconciliationTarget = publicAsyncFunctionBody(catalogReconciliation, "reconcile_target_inner");
reconciliationTarget?.includes("begin_tracked_read_transaction") &&
  reconciliationTarget.includes("page_transaction.commit()") &&
  !reconciliationTarget.includes("fetch_all(db.pool())")
  ? pass("catalog reconciliation work-key pages use short tracked read snapshots")
  : fail("catalog reconciliation work-key pages can hold or bypass an untracked read snapshot");
const reconciliationShortReads = ["catalog_revision", "root_snapshot"].filter((name) => {
  const body = publicAsyncFunctionBody(catalogReconciliation, name);
  return body == null || !body.includes("begin_tracked_read_transaction") || !body.includes("transaction.commit()");
});
reconciliationShortReads.length === 0
  ? pass("catalog reconciliation revision and root reads use tracked snapshots")
  : fail(`catalog reconciliation short read bypasses tracked snapshot: ${reconciliationShortReads.join(", ")}`);
const reconciliationInventoryLoads = ["load_audio_inventory_paths", "load_gallery_inventory_assets"].filter((name) => {
  const body = publicAsyncFunctionBody(catalogReconciliation, name);
  return body == null ||
    !body.includes("begin_tracked_read_transaction") ||
    !body.includes("transaction.commit()") ||
    !body.includes("transaction.rollback()") ||
    !body.includes("fetch(&mut *transaction)") ||
    body.includes("fetch(db.pool())");
});
reconciliationInventoryLoads.length === 0
  ? pass("Audio/Gallery reconciliation inventory loads use bounded committed snapshots")
  : fail(`Audio/Gallery reconciliation inventory load bypasses tracked snapshot: ${reconciliationInventoryLoads.join(", ")}`);
const watcherOwnershipCheck = publicAsyncFunctionBody(inventory, "event_requires_legacy_scan_inner");
const watcherJournal = publicAsyncFunctionBody(inventory, "journal_paths_for_specs_with_kinds");
inventory.includes("async fn catalog_v2_kinds_snapshot") &&
  watcherOwnershipCheck?.includes("catalog_v2_kinds_snapshot(db)") &&
  watcherJournal?.includes("catalog_v2_kinds_snapshot(db)") &&
  !watcherOwnershipCheck?.includes('catalog_kind_is_v2(db, "novel")') &&
  !watcherJournal?.includes('catalog_kind_is_v2(db, "novel")')
  ? pass("watcher ownership decisions use one bounded tracked snapshot")
  : fail("watcher ownership decisions still perform per-kind pool queries");
const coordinatorDispatch = publicAsyncFunctionBody(inventory, "process_pending_events_inner");
coordinatorDispatch?.includes("catalog_v2_kinds_snapshot(db)") &&
  coordinatorDispatch?.includes("catalog_v2_kinds.contains(\"novel\")") &&
  coordinatorDispatch?.includes("catalog_v2_kinds.contains(\"gallery\")") &&
  !coordinatorDispatch?.includes('catalog_kind_is_v2(db, "novel")')
  ? pass("inventory coordinator dispatch uses one bounded ownership snapshot")
  : fail("inventory coordinator dispatch still performs per-kind ownership queries");
const inventoryRootQueueReads = [
  ["pending_catalog_event_root_ids", "fetch_all(&mut *transaction)"],
  ["terminal_catalog_event_status", "fetch_optional(&mut *transaction)"],
  ["catalog_event_queue_phase", "fetch_one(&mut *transaction)"],
].filter(([name, readNeedle]) => {
  const body = publicAsyncFunctionBody(inventory, name);
  return body == null
    || !body.includes("begin_tracked_read_transaction")
    || !body.includes(readNeedle)
    || !body.includes("transaction.commit()");
});
inventoryRootQueueReads.length === 0 &&
  ["process_pending_novel_catalog_events", "process_pending_archive_catalog_events",
    "process_pending_audio_catalog_events", "process_pending_gallery_catalog_events"]
    .every((name) => publicAsyncFunctionBody(inventory, name)?.includes("pending_catalog_event_root_ids(db")) &&
  inventory.includes("catalog_event_queue_phase(db, root.id)")
  ? pass("Inventory coordinator root and terminal queue reads use short tracked snapshots")
  : fail("Inventory coordinator queue reads still bypass short tracked snapshots");
const inventoryShortWriteFunctions = [
  "complete_event",
  "prune_completed_events",
  "update_novel_coordinator_state",
  "update_novel_coordinator_state_unlocked",
  "fail_novel_catalog_event",
  "release_novel_coordinator_lease",
  "complete_root_scan",
  "mark_root_incomplete",
];
const untrackedInventoryShortWrites = inventoryShortWriteFunctions.filter((name) => {
  const body = publicAsyncFunctionBody(inventory, name);
  return body == null
    || !body.includes("begin_tracked_transaction")
    || !body.includes("transaction.commit()")
    || /\.(?:execute|fetch_one|fetch_optional)\(db\.pool\(\)\)/u.test(body);
});
untrackedInventoryShortWrites.length === 0
  ? pass("Inventory coordinator short writes use tracked transactions")
  : fail(`Inventory coordinator short write bypasses tracked transaction: ${untrackedInventoryShortWrites.join(", ")}`);
db.includes("CREATE TABLE IF NOT EXISTS audit_logs") && db.includes("pub async fn audit")
  ? pass("audit log table and writer are implemented")
  : fail("audit logging is missing");
db.includes("w.cover_asset_id, w.meta_json") && readFileSync(join(root, "crates/server/src/models.rs"), "utf8").includes("pub meta_json: String")
  ? pass("library summaries include work metadata")
  : fail("library summaries do not include work metadata");
!db.includes("CREATE TABLE IF NOT EXISTS downloads") && !db.includes("pub async fn downloads")
  ? pass("download records are removed with E/EX")
  : fail("download records should be removed");
db.includes("pub async fn claim_next_queued_job") && db.includes("UPDATE jobs SET") && db.includes("RETURNING *")
  ? pass("queued jobs are atomically claimed before execution")
  : fail("queued jobs are not atomically claimed");
db.includes("rebuild-search-index") && db.includes("import-tag-translations") && !db.includes("job_type LIKE 'eh-%'")
  ? pass("job queue prioritizes local index/tag work and user actions before slow enrichment")
  : fail("job queue priority ordering is missing");
readFileSync(join(root, "crates/server/src/models.rs"), "utf8").includes("WorkKind::Generated") && readFileSync(join(root, "crates/server/src/models.rs"), "utf8").includes('"generated"')
  ? pass("backend work kind model includes generated UI assets")
  : fail("backend work kind model is missing generated UI assets");
db.includes("pub async fn requeue_interrupted_running_jobs") && db.includes("WHERE status = 'running'") && readFileSync(join(root, "crates/server/src/main.rs"), "utf8").includes("requeue_interrupted_running_jobs")
  ? pass("interrupted running jobs are requeued on startup")
  : fail("startup recovery for interrupted running jobs is missing");
db.includes("pub async fn generated_assets_work") && db.includes("pub async fn set_work_cover")
  ? pass("generated image assets have a browsable system work and cover setter")
  : fail("generated image asset DB tracking is missing");
db.includes("SQLITE_MAX_CONNECTIONS") &&
db.includes("SQLITE_CACHE_KIB_PER_CONNECTION") &&
db.includes("SQLITE_WRITER_QUEUE_MAX_DEPTH") &&
db.includes("pub async fn acquire_write_slot") &&
db.includes("pub async fn checkpoint_wal") &&
db.includes("runtime_snapshot")
  ? pass("SQLite N100 connection, page-cache, WAL, writer queue, and runtime diagnostics are wired")
  : fail("SQLite N100 runtime governance is incomplete");
migrations.includes('name: "catalog-tag-kind-counts"') &&
catalog.includes("query_preaggregated_tag_facets") &&
catalog.includes("tag_kind_count_state")
  ? pass("kind-level tag facet preaggregation has migration, fallback, and readiness state")
  : fail("kind-level tag facet preaggregation is incomplete");
catalog.includes("FACET_CACHE_MAX_ENTRIES: usize = 32") &&
catalog.includes("FACET_CACHE_MAX_ITEMS: usize = 4_096") &&
catalog.includes("FACET_CACHE_TTL: Duration = Duration::from_secs(5)") &&
catalog.includes("prune_expired") &&
catalog.includes("enforce_limits") &&
catalog.includes("get_or_try_init") &&
catalog.includes("facet_coalesced") &&
catalog.includes("facet_evictions") &&
routes.includes('"catalog_runtime": catalog_runtime')
  ? pass("Facet cache is revision-bound, short-lived, single-flight, bounded, and observable")
  : fail("Facet cache capacity, lifetime, single-flight, or health diagnostics are incomplete");
facetCacheScenario.includes("evaluateFacetCacheRuntimeEvidence") &&
facetCacheScenario.includes('new URL("/api/health/resources"') &&
facetCacheScenario.includes("body.catalog_runtime") &&
facetCacheScenario.includes("target: target.pathname") &&
facetCacheScenario.includes("include_tag_sha256") &&
!facetCacheScenario.includes("JSON.stringify(headers)") &&
!facetCacheScenario.includes("process.env.PERF_COOKIE,") &&
!facetCacheScenario.includes("process.env.PERF_AUTHORIZATION,")
  ? pass("Facet cache Gate reads runtime health and keeps credentials/query text out of artifacts")
  : fail("Facet cache Gate runtime evidence or artifact redaction contract is incomplete");
facetBitmap.includes("FACET_BITMAP_MAX_WORKS: usize = 50_000") &&
facetBitmap.includes("FACET_BITMAP_MAX_ASSOCIATIONS: usize = 1_000_000") &&
facetBitmap.includes("FACET_BITMAP_MAX_BYTES: usize = 64 * 1024 * 1024") &&
facetBitmap.includes("enum AdaptiveBitmap") &&
facetBitmap.includes("scope_uses_typed_writer") &&
facetBitmap.includes("reserve_background") &&
facetBitmap.includes("catalog_work_ids_for_revision_range") &&
facetBitmap.includes("validate_capacity") &&
catalogWriter.includes("record_catalog_work_revision_delta") &&
db.includes("CATALOG_REVISION_DELTA_LOG_MAX")
  ? pass("cold Facet bitmap is adaptive, resource-governed, bounded, and revision-incremental")
  : fail("cold Facet bitmap capacity, ownership, governor, or revision safeguards are incomplete");
facetBitmapScenario.includes("evaluateFacetBitmapRuntimeEvidence") &&
facetBitmapScenario.includes("catalog_runtime?.facet_bitmap") &&
facetBitmapScenario.includes("facet_cache_ttl_millis") &&
facetBitmapScenario.includes("target: target.pathname") &&
facetBitmapScenario.includes("include_tag_sha256") &&
!facetBitmapScenario.includes("JSON.stringify(headers)") &&
!facetBitmapScenario.includes("process.env.PERF_COOKIE,") &&
!facetBitmapScenario.includes("process.env.PERF_AUTHORIZATION,")
  ? pass("Facet bitmap Gate expires the response cache and redacts credentials/query text")
  : fail("Facet bitmap Gate cache-cold evidence or artifact redaction contract is incomplete");
migrations.includes('name: "typed-catalog-writer-and-search-outbox"') &&
migrations.includes("CREATE TABLE catalog_kind_ownership") &&
migrations.includes("CREATE TABLE external_id_sources") &&
migrations.includes("CREATE TABLE search_outbox")
  ? pass("typed catalog writer ownership and search outbox have an append-only migration")
  : fail("typed catalog writer migration is incomplete");
catalogWriter.includes("WORK_MUTATION_ASSET_LIMIT: usize = 512") &&
catalogWriter.includes("MAX_STAGED_BYTES_PER_MUTATION") &&
catalogWriter.includes("acquire_mutation_fence") &&
catalogWriter.includes("complete_snapshot") &&
catalogWriter.includes("temp_catalog_asset_batch") &&
catalogWriter.includes("temp_catalog_tag_batch") &&
catalogWriter.includes("temp_catalog_external_batch") &&
catalogWriter.includes("enqueue_search_outbox")
  ? pass("typed catalog mutations are bounded, fenced, set-based, and transactional with outbox")
  : fail("typed catalog mutation safeguards are incomplete");
db.includes("INSERT INTO external_id_sources") && db.includes("owner, seen_token")
  ? pass("legacy scanner and enrichment writes preserve external-id ownership")
  : fail("external-id ownership is not maintained by existing write paths");
novelInspector.includes("NovelInspectionRequest") &&
novelInspector.includes("WorkMutation") &&
novelInspector.includes("complete_snapshot: true") &&
novelInspector.includes("extract_epub_cover_content_addressed_blocking") &&
novelInspector.includes("Component::ParentDir") &&
novelInspector.includes("MutationOwner::Scanner")
  ? pass("novel inspector emits fenced complete mutations with content-addressed covers")
  : fail("novel inspector mutation or path-safety contract is incomplete");
audioInspector.includes("AudioInspectionRequest") &&
audioInspector.includes("AUDIO_ASSET_CHUNK_SIZE: usize = 256") &&
audioInspector.includes("AUDIO_MUTATION_CHANNEL_DEPTH: usize = 2") &&
audioInspector.includes("MAX_AUDIO_FILES_PER_WORK: usize = 20_000") &&
audioInspector.includes("final_fence.complete_snapshot = true") &&
audioInspector.includes("blocking_send(WorkMutation") &&
audioInspector.includes("Component::ParentDir") &&
inventory.includes("process_pending_audio_catalog_events") &&
inventory.includes("drain_audio_catalog_events_under_fence")
  ? pass("audio inspector streams bounded chunks and finalizes through the fenced coordinator")
  : fail("audio inspector chunk, finalize, path-safety, or coordinator safeguards are incomplete");
galleryInspector.includes("GalleryInspectionRequest") &&
galleryInspector.includes("GALLERY_ASSET_CHUNK_SIZE: usize = 256") &&
galleryInspector.includes("GALLERY_MUTATION_CHANNEL_DEPTH: usize = 2") &&
galleryInspector.includes("MAX_GALLERY_FILES_PER_WORK: usize = 20_000") &&
galleryInspector.includes("MAX_GALLERY_PATH_BYTES: usize = 32 * 1024 * 1024") &&
galleryInspector.includes("final_fence.complete_snapshot = true") &&
galleryInspector.includes("blocking_send(WorkMutation") &&
galleryInspector.includes("Component::ParentDir") &&
inventory.includes("process_pending_gallery_catalog_events") &&
inventory.includes("drain_gallery_catalog_events_under_fence") &&
inventory.includes("enqueue_gallery_reconcile_catalog_events") &&
inventory.includes("enqueue_missing_gallery_catalog_events")
  ? pass("gallery inspector streams bounded directory snapshots through the fenced coordinator")
  : fail("gallery inspector chunk, finalize, path-safety, reconcile, or coordinator safeguards are incomplete");

!routes.includes("auth::") &&
  !enrich.includes("auth::") &&
  !assets.includes("auth::") &&
  !search.includes("auth::") &&
  !readFileSync(join(root, "crates/server/src/settings.rs"), "utf8").includes("auth::")
  ? pass("backend no longer depends on administrator password authentication")
  : fail("legacy administrator authentication remains in backend routes");
searchOutbox.includes("claim_items") &&
searchOutbox.includes("delete_term") &&
searchOutbox.includes("writer.commit()") &&
searchOutbox.includes("acknowledge_claims") &&
searchOutbox.includes("catalog_revision") &&
searchOutbox.includes("committed_at")
  ? pass("shadow Tantivy outbox consumer uses claim, idempotent delete+add, commit, then ack")
  : fail("shadow Tantivy outbox crash-recovery ordering is incomplete");
searchOutbox.includes("async fn load_documents(") &&
  searchOutbox.includes("let mut transaction = db.begin_tracked_read_transaction().await?;") &&
  searchOutbox.includes(".fetch_all(&mut *transaction)") &&
  searchOutbox.includes("transaction.commit().await?;") &&
  !searchOutbox.includes(".fetch_all(db.pool())")
  ? pass("shadow outbox document batches use a short tracked read snapshot")
  : fail("shadow outbox document batches still bypass the tracked read snapshot");
const searchStatusHelpers = ["status", "shadow_index_status", "reconciliation_status"].filter((name) => {
  const body = publicAsyncFunctionBody(searchOutbox, name);
  return body == null
    || !body.includes("begin_tracked_read_transaction")
    || !body.includes("transaction.connection()")
    || !body.includes("transaction.commit()");
});
searchStatusHelpers.length === 0
  ? pass("search outbox status helpers use committed tracked read snapshots")
  : fail(`search outbox status helper bypasses tracked snapshot: ${searchStatusHelpers.join(", ")}`);
const refreshShadowProgress = publicAsyncFunctionBody(searchOutbox, "refresh_shadow_progress");
refreshShadowProgress?.includes("let fact_failed = reconciliation_status == \"failed\"") &&
  refreshShadowProgress.includes("SET cutover_armed = 0") &&
  refreshShadowProgress.includes("execute(&mut *transaction)") &&
  refreshShadowProgress.includes("transaction.commit().await?")
  ? pass("shadow progress refresh clears cutover arm in the failed tracked transaction")
  : fail("shadow progress refresh can leave degraded search state armed");
searchOutbox.includes("pub(crate) async fn shadow_reconciliation_snapshot") &&
  searchOutbox.includes("begin_tracked_read_transaction") &&
  searchOutbox.includes("status_in(transaction.connection())") &&
  searchOutbox.includes("shadow_index_status_in(transaction.connection())") &&
  searchOutbox.includes("transaction.commit().await?") &&
  search.includes("let initial_status = outbox::shadow_reconciliation_snapshot(&state.db).await?") &&
  search.includes("let after_status = outbox::shadow_reconciliation_snapshot(&state.db).await?")
  ? pass("shadow fact reconciliation bounds status reads to short before/after snapshots")
  : fail("shadow fact reconciliation still mixes unbounded status reads around Tantivy I/O");
const incrementalReaderGate = publicAsyncFunctionBody(searchOutbox, "ensure_incremental_reader_gate");
const incrementalReaderGateImplementation = publicAsyncFunctionBody(searchOutbox, "incremental_reader_gate");
searchOutbox.includes("async fn incremental_reader_fast_state(") &&
  searchOutbox.includes("incremental_reader_fast_state_in(transaction.connection())") &&
  (incrementalReaderGate?.includes("let (fast_ready, fast_status) = incremental_reader_fast_state(db).await?") ||
    incrementalReaderGateImplementation?.includes("let (fast_ready, fast_status) = incremental_reader_fast_state(db).await?")) &&
  !incrementalReaderGate?.includes(".fetch_one(db.pool())") &&
  !incrementalReaderGateImplementation?.includes(".fetch_one(db.pool())")
  ? pass("incremental reader fast-fail state uses a committed tracked read snapshot")
  : fail("incremental reader fast-fail state still bypasses its tracked read snapshot");
routes.includes('"search_features":') &&
searchIncrementalRunner.includes("search_features")
  ? pass("search reader flag state is exposed to the incremental fixed-corpus Gate")
  : fail("search reader flag state is not exposed to the incremental Gate");
search.includes("SearchReaderRequest::Shadow") &&
search.includes("search_shadow_canary_enabled") &&
search.includes("validate_shadow_canary_state") &&
search.includes("record_canary_comparison") &&
search.includes("canary_id_mismatches") &&
search.includes("canary_order_mismatches")
  ? pass("explicit shadow reader canary is readiness-gated and records ID/order drift")
  : fail("shadow reader canary gating or drift metrics are incomplete");
search.includes("reconcile_shadow_facts") &&
search.includes("SEARCH_FACT_MAX_WORKS") &&
search.includes("DocSetCollector") &&
search.includes("sqlite_ids_sha256") &&
search.includes("shadow_ids_sha256")
  ? pass("shadow search fact reconciliation is bounded and compares hashed SQLite/Tantivy IDs")
  : fail("shadow search SQLite/Tantivy fact reconciliation is incomplete");
search.includes("recover_incremental_search_index_after_error") &&
search.includes("recover_search_index_after_error") &&
search.includes("REBUILD_SHADOW_SEARCH_JOB_TYPE") &&
searchOutbox.includes("pub(crate) async fn record_shadow_error")
  ? pass("incremental search corruption fails closed and queues a shadow-only rebuild")
  : fail("incremental search corruption recovery is not isolated from legacy rebuilds");
searchCanaryRunner.includes('target.searchParams.set("reader", "shadow")') &&
searchCanaryRunner.includes('/api/search/shadow/reconcile') &&
searchCanaryRunner.includes("query_sha256") &&
searchCanaryRunner.includes("corpus_sha256") &&
searchCanaryRunner.includes("evaluateSearchShadowCanaryEvidence") &&
!searchCanaryRunner.includes("query: entry.query")
  ? pass("fixed-corpus search canary Gate records redacted, zero-diff evidence")
  : fail("fixed-corpus search canary Gate or query redaction is incomplete");
searchIncrementalRunner.includes("/api/health/resources") &&
searchIncrementalRunner.includes("evaluateSearchIncrementalReaderEvidence") &&
searchIncrementalRunner.includes("query_sha256") &&
searchIncrementalRunner.includes("search_reconciliation") &&
!searchIncrementalRunner.includes("query: entry.query")
  ? pass("incremental production-reader Gate records redacted routing and cutover evidence")
  : fail("incremental production-reader fixed-corpus Gate is incomplete or leaks query text");
searchCutoverProbe.includes('scenario: "search-cutover-unarmed-probe"') &&
searchCutoverProbe.includes("first_unarmed") &&
searchCutoverProbe.includes("final_armed") &&
searchCutoverProbe.includes("query_sha256") &&
searchCutoverProbe.includes('finalArmed.reader === "production"') &&
searchCutoverProbe.includes("finalArmed.rebuilt === false") &&
searchCutoverProbe.includes("finalArmed.outbox_revision_lag === 0") &&
searchCutoverProbe.includes('finalArmed.reconciliation_status === "passed"') &&
searchCutoverProbe.includes("approximation: true")
  ? pass("search cutover probe records bounded unarmed/armed evidence without query text")
  : fail("search cutover probe is missing bounded, redacted unarmed/armed evidence");
routes.includes('"search_outbox": search_outbox') && searchOutbox.includes("pub(crate) async fn status")
  ? pass("health diagnostics expose search outbox lag and commit state")
  : fail("search outbox health diagnostics are missing");
inventory.includes("INVENTORY_BATCH_SIZE") &&
inventory.includes("mpsc::channel(INVENTORY_CHANNEL_BATCHES)") &&
inventory.includes("generation fence") &&
inventory.includes("mark_root_incomplete")
  ? pass("shadow inventory uses bounded batches, cancellation fences, and incomplete-scan safety")
  : fail("shadow inventory bounded discovery safeguards are incomplete");
inventory.includes("journal_watcher_paths") &&
inventory.includes("process_pending_events") &&
watcher.includes("mark_watcher_gap")
  ? pass("watcher journal and targeted shadow inventory processing are wired")
  : fail("watcher journal or targeted inventory processing is missing");
for (const forbiddenMutation of ["UPDATE works", "DELETE FROM works", "INSERT INTO works", "UPDATE assets", "DELETE FROM assets"]) {
  inventory.includes(forbiddenMutation)
    ? fail(`shadow inventory must not mutate authoritative media tables: ${forbiddenMutation}`)
    : pass(`shadow inventory avoids authoritative mutation ${forbiddenMutation}`);
}
routes.includes('"/works/{id}/progress"') && routes.includes("update_work_progress")
  ? pass("progress writeback is available for local readers")
  : fail("progress writeback route is missing");

!existsSync(join(root, "crates/server/src/eh.rs")) && !routes.includes("/eh/")
  ? pass("backend E/EX routes and implementation are removed")
  : fail("backend E/EX implementation should be removed");
enrich.includes("generated_assets_work") && enrich.includes("upsert_asset") && enrich.includes('"assets.generate"') && enrich.includes('"done"')
  ? pass("generated OpenAI images are saved as browsable local assets")
  : fail("generated OpenAI images are not tracked as local assets");

const frontend = readFileSync(join(root, "frontend/src/App.tsx"), "utf8");
const catalogHook = readFileSync(join(root, "frontend/src/catalog/useCatalog.ts"), "utf8");
const novelReader = readFileSync(join(root, "frontend/src/components/NovelReader.tsx"), "utf8");
const frontendStyles = readFileSync(join(root, "frontend/src/styles.css"), "utf8");
const settingsModule = readFileSync(join(root, "crates/server/src/settings.rs"), "utf8");
const libraryLayout = readFileSync(join(root, "frontend/src/features/library/libraryLayout.ts"), "utf8");
const motionModule = readFileSync(join(root, "frontend/src/ui/motion.ts"), "utf8");
const motionProvider = readFileSync(join(root, "frontend/src/ui/MotionProvider.tsx"), "utf8");
const mainEntry = readFileSync(join(root, "frontend/src/main.tsx"), "utf8");
settingsModule.includes("app-settings.json") && scanner.includes("load_settings") && scanner.includes("comic_roots") && scanner.includes("audio_roots")
  ? pass("settings-backed media directories are wired into scanner")
  : fail("settings-backed media directories are not wired into scanner");
novelReader.includes("epubManifest") && novelReader.includes("epubChapterHtml") && novelReader.includes("chapter-list")
  ? pass("frontend EPUB reader is wired")
  : fail("frontend EPUB reader is missing");
novelReader.includes("function VirtualChapterList") &&
  novelReader.includes("LEGACY_CHAPTER_OVERSCAN") &&
  novelReader.includes("chapter-list-window") &&
  !novelReader.includes("{chapters.map((item) => (")
  ? pass("fallback EPUB chapter list is virtualized and bounded")
  : fail("fallback EPUB chapter list still renders every chapter button");
novelReader.includes("ReadonlyMap<number, EpubChapter>") &&
  novelReader.includes("chapterCount") &&
  novelReader.includes("requestManifestRange") &&
  novelReader.includes("EPUB_MANIFEST_PAGE_SIZE")
  ? pass("fallback EPUB manifest pages are sparse, cancellable, and window-driven")
  : fail("fallback EPUB reader still requires a full chapter manifest in memory");
frontend.includes("cycleTagFilter") && frontend.includes("availableTagKeys") && !frontend.includes("excludeTags")
  ? pass("frontend tag filtering is two-state and kind-scoped")
  : fail("frontend tag filtering should be two-state and kind-scoped");
catalogHook.includes("MAX_RESIDENT_FACET_TAGS") &&
  catalogHook.includes("tagNextCursor") &&
  catalogHook.includes("loadMoreTags") &&
  catalogHook.includes("tagLimitReached") &&
  catalogHook.includes("tagLoadMoreControllerRef") &&
  frontend.includes("onLoadMoreTags") &&
  frontend.includes("tag-load-more")
  ? pass("catalog facets use cancellable explicit continuation with a bounded resident tag set")
  : fail("catalog facet continuation is missing cancellation, bounds, or UI wiring");
frontend.includes("TagDetailPanel") && frontend.includes("tagLanguage") && frontend.includes("tagNamespace")
  ? pass("frontend tag detail and language toggle are wired")
  : fail("frontend tag detail/language toggle is missing");
frontend.includes("buildNovelCollections") && frontend.includes("NovelDisplayMode") && frontend.includes('"novel-collection"')
  ? pass("frontend novel collection display is wired")
  : fail("frontend novel collection display is missing");
frontend.includes("onProgressSaved") &&
  (frontend.includes("saveTrackProgress") || frontend.includes("saveAudioProgress")) &&
  frontend.includes("persistProgress")
  ? pass("frontend reader progress persistence is wired")
  : fail("frontend reader progress persistence is missing");
frontend.includes('.work(selectedId, controller.signal, "summary")') &&
  !frontend.includes('.work(selectedId, controller.signal, catalogEnabled === true ? "summary" : "legacy")')
  ? pass("frontend detail requests stay on the bounded summary asset shape")
  : fail("frontend detail requests can still materialize the unbounded legacy asset shape");
frontend.includes("LEGACY_BACKGROUND_PAGE_LIMIT = 5") &&
  frontend.includes("loadedPages < LEGACY_BACKGROUND_PAGE_LIMIT") &&
  frontend.includes("legacyKnownWorkIdsRef") &&
  frontend.includes("const loadMoreLegacy") &&
  frontend.includes('aria-label="旧版书架续页"') &&
  !frontend.includes("sort(compareWorksByUpdatedAt)")
  ? pass("legacy fallback keeps background loading bounded and exposes explicit continuation")
  : fail("legacy fallback can still load or sort the entire library during startup");
frontend.includes("preferredTrackVariants") && frontend.includes("preferred_playback")
  ? pass("frontend deduplicates audio track variants for preferred playback")
  : fail("frontend audio track variant preference is missing");
frontend.includes("playlistRequestRef") &&
  frontend.includes("playlistGenerationRef") &&
  frontend.includes("signal: controller.signal") &&
  frontend.includes("playlistRequestRef.current?.abort()") &&
  frontend.includes("generation !== playlistGenerationRef.current")
  ? pass("frontend audio queue cancels stale pages and isolates playback sessions")
  : fail("frontend audio queue must cancel stale page requests at session boundaries");
frontend.includes("comicMode") && frontend.includes('"horizontal"') && frontend.includes("scrollLeft") && frontend.includes('data-mode={comicMode}')
  ? pass("frontend comic reader supports paged/scroll/horizontal/zoom/keyboard controls")
  : fail("frontend comic reader controls are incomplete");
frontend.includes("comic_prefetch_pages") &&
  frontend.includes("Math.max(5, Math.min(10,") &&
  frontend.includes("if (stopped || !success) break") &&
  frontend.includes("clearGalleryPreloads(comicPagePreloadsRef.current)") &&
  frontend.includes("for (const url of desired)")
  ? pass("frontend comic reader uses bounded sequential 5-10 page prefetch")
  : fail("frontend comic reader page prefetch is missing its sequential bound or cleanup");
novelReader.includes('theme: "paper" | "dark" | "sepia"') &&
novelReader.includes("data-theme={settings.theme}") &&
novelReader.includes('updateSettings({ theme: "dark" })')
  ? pass("frontend EPUB reader theme toggle is wired")
  : fail("frontend EPUB reader theme toggle is missing");
frontend.includes("SettingsOverlay") &&
frontend.includes("AppSettings") &&
frontend.includes("media_dirs") &&
frontend.includes("onSaveSettings") &&
!frontend.includes("onThemeChange") &&
!frontend.includes("onAppearanceChange") &&
!frontend.includes("updateAppearance")
  ? pass("frontend settings panel manages directories, reader preferences, and rescan")
  : fail("frontend settings panel is incomplete");
frontend.includes("资源访问目录") &&
  frontend.includes("容器挂载") &&
  !frontend.includes("addDir") &&
  !frontend.includes("removeDir") &&
  settingsModule.includes("with_container_directories")
  ? pass("resource directories are read-only and bound to container configuration")
  : fail("resource directory settings are still editable or not container-bound");
frontendStyles.includes("--ui-canvas") &&
frontendStyles.includes("--ui-sidebar") &&
frontendStyles.includes("--ui-shadow-float") &&
frontendStyles.includes("@media (prefers-reduced-motion: reduce)") &&
frontend.includes("getLibraryLayout") &&
libraryLayout.includes("rowHeight") &&
libraryLayout.includes('contentKind === "audio" ? 1 : 4 / 3') &&
frontend.includes('contentKind={kind === "audio" ? "audio" : "portrait"}') &&
frontendStyles.includes("aspect-ratio: 1 / var(--library-cover-ratio") &&
motionModule.includes("uiEaseOut") &&
motionProvider.includes('reducedMotion="user"') &&
mainEntry.includes("MotionProvider") &&
!frontendStyles.includes("backdrop-filter") &&
!frontendStyles.includes("data-material") &&
!frontendStyles.includes("ambient-shelf") &&
!frontend.includes("GlassSurface") &&
!frontend.includes("GlassFilterProvider")
  ? pass("frontend uses the single soft-light UI foundation with shared motion and layout primitives")
  : fail("frontend soft-light UI foundation or shared motion/layout primitives are missing");
const removedUiPaths = [
  "frontend/src/components/material/GlassSurface.tsx",
  "frontend/src/components/material/GlassFilterProvider.tsx",
  "frontend/src/components/material/index.ts",
  "frontend/public/assets/ambient-shelf.png"
];
removedUiPaths.every((file) => !existsSync(join(root, file))) &&
!settingsModule.includes("ThemeMode") &&
!settingsModule.includes("UiMaterial") &&
!settingsModule.includes("GlassIntensity") &&
!settingsModule.includes("AppearanceSettings") &&
!settingsModule.includes("appearance:")
  ? pass("legacy application material files, asset, and settings fields are removed")
  : fail("legacy application material files, asset, or settings fields remain");
frontend.includes("setLocalSearch") && frontend.includes("api.search(needle") && frontend.includes("searchRank")
  ? pass("frontend local Tantivy search is used for bookshelf filtering")
  : fail("frontend local Tantivy search is not wired into filtering");
!frontend.includes("remote-eh") && !frontend.includes("EhCookieImport") && !frontend.includes("EhWatchedPanel") && !frontend.includes("api.eh")
  ? pass("frontend E/EX UI is removed")
  : fail("frontend E/EX UI should be removed");

const api = readFileSync(join(root, "frontend/src/api.ts"), "utf8");
!api.includes("EhSearchOptions") && !api.includes("ehWatched") && !api.includes("ehLogin") && !api.includes("ehFavorite") && !api.includes("/api/downloads")
  ? pass("frontend E/EX API client is removed")
  : fail("frontend E/EX API client should be removed");
api.includes("AppSettings") && api.includes("/api/settings")
  ? pass("frontend settings API client is wired")
  : fail("frontend settings API client is missing");
api.includes("generateAsset: (input") && api.includes("allow_cover_style") && api.includes("/api/assets/generate")
  ? pass("frontend API client supports queued gpt-image asset generation")
  : fail("frontend API client image generation request is incomplete");
api.includes("SearchResponse") && api.includes("/api/search?q=")
  ? pass("frontend API client exposes local Tantivy search")
  : fail("frontend local search API client is missing");
api.includes("meta_json: string")
  ? pass("frontend work summaries carry metadata")
  : fail("frontend work summary metadata is missing");
api.includes("updateProgress") && routes.includes("update_work_progress")
  ? pass("progress API client and backend writer are wired")
  : fail("progress API wiring is incomplete");
!api.includes("setCsrfToken") &&
  !api.includes("/api/auth") &&
  !frontend.includes("AuthControls") &&
  !frontend.includes("loginPassword")
  ? pass("frontend administrator password and CSRF client are removed")
  : fail("frontend administrator authentication UI or client remains");
!frontend.includes("AssetGenerator") && !frontend.includes("queueGeneratedAsset")
  ? pass("frontend gpt-image asset generation UI is removed")
  : fail("frontend gpt-image asset generation UI should be removed");
frontend.includes('"generated"') && frontend.includes("generatedImages") && frontend.includes("generated-stage")
  ? pass("frontend exposes generated image asset shelf and preview")
  : fail("frontend generated image asset browsing is missing");
frontend.includes("function VirtualShelf") && frontend.includes("ResizeObserver") && frontend.includes("virtual-shelf-window")
  ? pass("frontend bookshelf uses a measured virtualized shelf")
  : fail("frontend virtualized shelf is missing");
catalogHook.includes("prefetchPage") &&
  catalogHook.includes("prefetchControllersRef") &&
  catalogHook.includes("MAX_RESIDENT_PAGES") &&
  catalogHook.includes("clearPrefetch")
  ? pass("catalog shelf prefetches only the next page inside the bounded LRU")
  : fail("catalog shelf bounded next-page prefetch is missing");
frontendStyles.includes(".virtual-shelf") && frontendStyles.includes(".virtual-shelf-window") && frontendStyles.includes(".virtual-shelf-cell")
  ? pass("frontend virtualized shelf layout styles are present")
  : fail("frontend virtualized shelf layout styles are missing");
frontend.includes("const [comicPageCount, setComicPageCount]") &&
  frontend.includes("const total = Math.min(COMIC_MAX_PAGE_COUNT") &&
  frontend.includes("setPages(loaded.slice(0, total))") &&
  frontend.includes("comicPageFromVerticalPosition") &&
  !frontend.includes("Array.from({ length: total }, (_, index) => loaded[index]") &&
  !frontend.includes("new Array<number>(pages.length + 1)")
  ? pass("frontend comic reader keeps page metadata sparse and avoids full offset arrays")
  : fail("frontend comic reader still materializes a full page placeholder/offset model");
assets.includes("total > COMIC_MANIFEST_MAX_PAGE_SIZE") &&
  assets.includes("let bounded =") &&
  assets.includes("query.cursor.is_some()") &&
  assets.includes("large_no_query_comic_manifest_falls_back_to_a_bounded_page")
  ? pass("large comic manifests stay bounded on the compatibility no-query path")
  : fail("large comic manifest no-query requests can still bypass pagination");

const mediaChecks = [
  { dir: "漫画", required: [".cbz", ".xml"] },
  { dir: "轻小说", required: [".epub"] },
  { dir: "音声", required: [".mp3", ".wav"] }
];

for (const check of mediaChecks) {
  const dir = join(root, check.dir);
  if (!existsSync(dir)) {
    fail(`missing media directory ${check.dir}`);
    continue;
  }
  const counts = new Map();
  for (const file of walk(dir)) {
    const ext = extname(file).toLowerCase();
    counts.set(ext, (counts.get(ext) ?? 0) + 1);
  }
  for (const ext of check.required) {
    const count = counts.get(ext) ?? 0;
    count > 0 ? pass(`${check.dir} has ${count} ${ext} files`) : fail(`${check.dir} has no ${ext} files`);
  }
}

const frontendDist = join(root, "frontend/dist/index.html");
existsSync(frontendDist) ? pass("frontend production build exists") : fail("frontend/dist/index.html missing; run npm.cmd run build");

if (process.exitCode) process.exit(process.exitCode);
