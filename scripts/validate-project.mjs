import { existsSync, readFileSync, readdirSync, statSync } from "node:fs";
import { extname, join, relative } from "node:path";

const root = new URL("..", import.meta.url).pathname.replace(/^\/([A-Za-z]:)/, "$1");
const requiredFiles = [
  "Cargo.toml",
  "crates/server/Cargo.toml",
  "crates/server/src/main.rs",
  "crates/server/src/scanner/mod.rs",
  "crates/server/src/routes.rs",
  "crates/server/src/search.rs",
  "crates/server/src/watcher.rs",
  "frontend/package.json",
  "frontend/src/App.tsx",
  "frontend/src/styles.css",
  "frontend/public/assets/ambient-shelf.png",
  "docs/reference-research.md",
  "scripts/inspect-media.mjs",
  "Dockerfile",
  "docker-compose.yml"
];

const requiredRoutes = [
  "/auth/session",
  "/auth/login",
  "/auth/logout",
  "/library",
  "/settings",
  "/search",
  "/search/rebuild",
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
for (const route of requiredRoutes) {
  routes.includes(route) ? pass(`API route ${route}`) : fail(`missing API route ${route}`);
}
routes.includes('"media"') && routes.includes('"openai_image_configured"') && routes.includes("enrichment_concurrency.clamp")
  ? pass("health endpoint exposes deployment readiness without secrets")
  : fail("health endpoint deployment readiness payload is missing");

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
!scanner.includes('"enrich-lightnovel-work"') && !scanner.includes('"enrich-asmr-work"')
  ? pass("media scan does not queue local online enrichment by default")
  : fail("media scan should not queue local online enrichment");
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

const jobs = readFileSync(join(root, "crates/server/src/jobs.rs"), "utf8");
jobs.includes("reschedule_job") ? pass("job worker retries failed enrichment") : fail("job worker retry path is missing");
jobs.includes("enrichment_concurrency.clamp") && jobs.includes("claim_next_queued_job") && jobs.includes("worker_id")
  ? pass("job worker honors ENRICHMENT_CONCURRENCY with claimed jobs")
  : fail("job worker concurrency/claim path is missing");

const config = readFileSync(join(root, "crates/server/src/config.rs"), "utf8");
config.includes("APP_ADMIN_PASSWORD")
  ? pass("admin password config is wired")
  : fail("APP_ADMIN_PASSWORD config is missing");
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

const assets = readFileSync(join(root, "crates/server/src/assets.rs"), "utf8");
assets.includes("read_epub_manifest") && assets.includes("read_epub_chapter_html")
  ? pass("EPUB manifest and chapter reader are implemented")
  : fail("EPUB reader API implementation is missing");
assets.includes("PARTIAL_CONTENT") && assets.includes("CONTENT_RANGE") && assets.includes("parse_byte_range")
  ? pass("asset streaming supports byte range requests")
  : fail("asset byte range streaming is missing");

const search = readFileSync(join(root, "crates/server/src/search.rs"), "utf8");
for (const needle of ["tantivy", "QueryParser", "TopDocs", "rebuild_search_index", "search-index"]) {
  search.includes(needle) ? pass(`Tantivy search contains ${needle}`) : fail(`Tantivy search missing ${needle}`);
}

const watcher = readFileSync(join(root, "crates/server/src/watcher.rs"), "utf8");
for (const needle of ["notify::recommended_watcher", "RecursiveMode::Recursive", "\"scan-library\"", "WATCH_DEBOUNCE_SECONDS"]) {
  const text = needle === "WATCH_DEBOUNCE_SECONDS" ? config : watcher;
  text.includes(needle) ? pass(`file watcher contains ${needle}`) : fail(`file watcher missing ${needle}`);
}

const auth = readFileSync(join(root, "crates/server/src/auth.rs"), "utf8");
auth.includes("x-csrf-token") && auth.includes("media_shelf_session") && auth.includes("require_csrf")
  ? pass("single-user session and CSRF guard are implemented")
  : fail("session/CSRF guard is missing");

const db = readFileSync(join(root, "crates/server/src/db.rs"), "utf8");
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

const protectedBackend = [
  ["settings.rs", readFileSync(join(root, "crates/server/src/settings.rs"), "utf8"), "settings update", 'auth::require_csrf(&state, &headers, "settings.update")'],
  ["routes.rs", routes, "scan", 'auth::require_csrf(&state, &headers, "scan")'],
  ["enrich.rs", enrich, "enrich", 'auth::require_csrf(&state, &headers, "enrich")'],
  ["assets.rs", assets, "asset generation", 'auth::require_csrf(&state, &headers, "assets.generate")']
];
for (const [file, text, label, needle] of protectedBackend) {
  text.includes(needle) ? pass(`${label} is CSRF protected in ${file}`) : fail(`${label} is not CSRF protected in ${file}`);
}
routes.includes("update_work_progress") && routes.includes('"works.progress"')
  ? pass("progress writeback is available for local readers")
  : fail("progress writeback route is missing");

!existsSync(join(root, "crates/server/src/eh.rs")) && !routes.includes("/eh/")
  ? pass("backend E/EX routes and implementation are removed")
  : fail("backend E/EX implementation should be removed");
enrich.includes("generated_assets_work") && enrich.includes("upsert_asset") && enrich.includes('"assets.generate"') && enrich.includes('"done"')
  ? pass("generated OpenAI images are saved as browsable local assets")
  : fail("generated OpenAI images are not tracked as local assets");

const frontend = readFileSync(join(root, "frontend/src/App.tsx"), "utf8");
const frontendStyles = readFileSync(join(root, "frontend/src/styles.css"), "utf8");
const settingsModule = readFileSync(join(root, "crates/server/src/settings.rs"), "utf8");
settingsModule.includes("app-settings.json") && scanner.includes("load_settings") && scanner.includes("comic_roots") && scanner.includes("audio_roots")
  ? pass("settings-backed media directories are wired into scanner")
  : fail("settings-backed media directories are not wired into scanner");
frontend.includes("epubManifest") && frontend.includes("epubChapterHtml") && frontend.includes("chapter-list")
  ? pass("frontend EPUB reader is wired")
  : fail("frontend EPUB reader is missing");
frontend.includes("cycleTagFilter") && frontend.includes("availableTagKeys") && !frontend.includes("excludeTags")
  ? pass("frontend tag filtering is two-state and kind-scoped")
  : fail("frontend tag filtering should be two-state and kind-scoped");
frontend.includes("TagDetailPanel") && frontend.includes("tagLanguage") && frontend.includes("tagNamespace")
  ? pass("frontend tag detail and language toggle are wired")
  : fail("frontend tag detail/language toggle is missing");
frontend.includes("buildNovelCollections") && frontend.includes("NovelDisplayMode") && frontend.includes('"novel-collection"')
  ? pass("frontend novel collection display is wired")
  : fail("frontend novel collection display is missing");
frontend.includes("onProgressSaved") && frontend.includes("saveTrackProgress") && frontend.includes("persistProgress")
  ? pass("frontend reader progress persistence is wired")
  : fail("frontend reader progress persistence is missing");
frontend.includes("preferredTrackVariants") && frontend.includes("preferred_playback")
  ? pass("frontend deduplicates audio track variants for preferred playback")
  : fail("frontend audio track variant preference is missing");
frontend.includes("comicMode") && frontend.includes('"horizontal"') && frontend.includes("scrollLeft") && frontend.includes('data-mode={comicMode}')
  ? pass("frontend comic reader supports paged/scroll/horizontal/zoom/keyboard controls")
  : fail("frontend comic reader controls are incomplete");
frontend.includes("novelTheme") && frontend.includes("applyNovelTheme") && frontend.includes("setNovelTheme")
  ? pass("frontend EPUB reader theme toggle is wired")
  : fail("frontend EPUB reader theme toggle is missing");
frontend.includes("SettingsOverlay") && frontend.includes("AppSettings") && frontend.includes("media_dirs") && frontend.includes("onSaveSettings")
  ? pass("frontend settings panel manages theme, directories, and rescan")
  : fail("frontend settings panel is incomplete");
frontendStyles.includes(':root[data-theme="dark"]') && frontend.includes("onThemeChange")
  ? pass("frontend light/dark theme toggle is wired")
  : fail("frontend theme toggle is missing");
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
api.includes("setCsrfToken") && api.includes('"x-csrf-token"') && frontend.includes("AuthControls")
  ? pass("frontend admin login and CSRF client are wired")
  : fail("frontend admin login/CSRF wiring is missing");
!frontend.includes("AssetGenerator") && !frontend.includes("queueGeneratedAsset")
  ? pass("frontend gpt-image asset generation UI is removed")
  : fail("frontend gpt-image asset generation UI should be removed");
frontend.includes('"generated"') && frontend.includes("generatedImages") && frontend.includes("generated-stage")
  ? pass("frontend exposes generated image asset shelf and preview")
  : fail("frontend generated image asset browsing is missing");
frontend.includes("function VirtualShelf") && frontend.includes("ResizeObserver") && frontend.includes("virtual-shelf-window")
  ? pass("frontend bookshelf uses a measured virtualized shelf")
  : fail("frontend virtualized shelf is missing");
frontendStyles.includes(".virtual-shelf") && frontendStyles.includes(".virtual-shelf-window") && frontendStyles.includes(".virtual-shelf-cell")
  ? pass("frontend virtualized shelf layout styles are present")
  : fail("frontend virtualized shelf layout styles are missing");

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
