import { DatabaseSync } from "node:sqlite";
import {
  existsSync,
  mkdirSync,
  statSync,
  writeFileSync,
} from "node:fs";
import { dirname, resolve } from "node:path";
import { parseArgs } from "node:util";

const { values } = parseArgs({
  options: {
    database: { type: "string", short: "d" },
    output: { type: "string", short: "o" },
    "base-url": { type: "string", default: "http://127.0.0.1:8787" },
    requests: { type: "string", default: "30" },
    concurrency: { type: "string", default: "1" },
    "range-bytes": { type: "string", default: "262144" },
    help: { type: "boolean", short: "h", default: false },
  },
  strict: true,
});

if (values.help || !values.database || !values.output) {
  console.log(`Usage: node scripts/perf/prepare-media-targets.mjs --database <path> --output <targets.json> [options]

Options:
  -d, --database <path>       SQLite database opened read-only
  -o, --output <path>         New target matrix; existing files are refused
      --base-url <url>        Server base URL (default http://127.0.0.1:8787)
      --requests <n>          Requests per target (default 30)
      --concurrency <n>       Concurrency per target (default 1)
      --range-bytes <n>       Audio startup range size (default 262144)

The database is never modified. Selection is deterministic and fails closed when
an active representative for any required media scenario is unavailable.`);
  process.exit(values.help ? 0 : 2);
}

function positiveInteger(name, raw, maximum = 100_000) {
  const value = Number(raw);
  if (!Number.isSafeInteger(value) || value < 1 || value > maximum) {
    throw new TypeError(`${name} must be an integer between 1 and ${maximum}`);
  }
  return value;
}

function safeUrl(raw) {
  const url = new URL(raw);
  if (!new Set(["http:", "https:"]).has(url.protocol)) {
    throw new TypeError("--base-url must use http or https");
  }
  if (url.username || url.password || url.search || url.hash) {
    throw new TypeError("--base-url must not contain credentials, query text or a fragment");
  }
  return url.toString().replace(/\/$/u, "");
}

function tableColumns(database, table) {
  return new Set(database.prepare(`PRAGMA table_info(${table})`).all().map((row) => row.name));
}

function requireSchema(database) {
  const tables = new Set(
    database
      .prepare("SELECT name FROM sqlite_schema WHERE type = 'table'")
      .all()
      .map((row) => row.name),
  );
  for (const table of ["works", "assets", "tags", "work_tags"]) {
    if (!tables.has(table)) throw new Error(`database is missing required table ${table}`);
  }
  const workColumns = tableColumns(database, "works");
  const assetColumns = tableColumns(database, "assets");
  const requiredWorkColumns = ["id", "kind", "deleted_at"];
  const requiredAssetColumns = ["id", "work_id", "mime", "role", "size", "meta_json"];
  for (const column of requiredWorkColumns) {
    if (!workColumns.has(column)) throw new Error(`works table is missing required column ${column}`);
  }
  for (const column of requiredAssetColumns) {
    if (!assetColumns.has(column)) throw new Error(`assets table is missing required column ${column}`);
  }
}

function requireRow(row, scenario, detail) {
  if (!row || row.work_id == null) {
    throw new Error(`cannot prepare ${scenario}: ${detail}`);
  }
  return row;
}

function path(baseUrl, suffix) {
  return `${baseUrl}${suffix}`;
}

function target({ id, scenario, warm, url, requests, concurrency, range, maxBodyBytes, requirePartial }) {
  return {
    id,
    scenario,
    warm,
    url,
    requests,
    concurrency,
    ...(range ? { range } : {}),
    ...(maxBodyBytes ? { max_body_bytes: maxBodyBytes } : {}),
    ...(requirePartial ? { require_partial: true } : {}),
  };
}

const databasePath = resolve(values.database);
const outputPath = resolve(values.output);
if (!existsSync(databasePath) || !statSync(databasePath).isFile()) {
  throw new Error(`database does not exist or is not a file: ${databasePath}`);
}
if (existsSync(outputPath)) {
  throw new Error(`refusing to overwrite existing target file: ${outputPath}`);
}

const baseUrl = safeUrl(values["base-url"]);
const requests = positiveInteger("--requests", values.requests);
const concurrency = positiveInteger("--concurrency", values.concurrency, 128);
const rangeBytes = positiveInteger("--range-bytes", values["range-bytes"], 16 * 1024 * 1024);
const database = new DatabaseSync(databasePath, { readOnly: true });

try {
  database.exec("PRAGMA query_only = ON; PRAGMA busy_timeout = 5000;");
  requireSchema(database);

  const gallery = requireRow(
    database.prepare(`
      SELECT work.id AS work_id, asset.id AS asset_id
      FROM works AS work
      JOIN assets AS asset ON asset.work_id = work.id
      WHERE work.kind = 'gallery'
        AND work.deleted_at IS NULL
        AND asset.role = 'image'
        AND asset.mime LIKE 'image/%'
      ORDER BY COALESCE(asset.size, 0) DESC, asset.id ASC
      LIMIT 1
    `).get(),
    "gallery-thumbnail",
    "no active gallery image asset exists",
  );

  const comic = requireRow(
    database.prepare(`
      SELECT work.id AS work_id, asset.id AS asset_id
      FROM works AS work
      JOIN assets AS asset ON asset.work_id = work.id
      WHERE work.kind = 'comic'
        AND work.deleted_at IS NULL
        AND asset.role = 'archive'
        AND CAST(COALESCE(json_extract(asset.meta_json, '$.page_count'), 0) AS INTEGER) > 0
      ORDER BY COALESCE(asset.size, 0) DESC, asset.id ASC
      LIMIT 1
    `).get(),
    "comic-page",
    "no active comic archive with a positive page_count exists",
  );

  const coserPicture = requireRow(
    database.prepare(`
      SELECT work.id AS work_id, asset.id AS asset_id
      FROM works AS work
      JOIN assets AS asset ON asset.work_id = work.id
      WHERE work.kind = 'coser-picture'
        AND work.deleted_at IS NULL
        AND asset.role = 'archive'
        AND CAST(COALESCE(json_extract(asset.meta_json, '$.page_count'), 0) AS INTEGER) > 0
      ORDER BY COALESCE(asset.size, 0) DESC, asset.id ASC
      LIMIT 1
    `).get(),
    "coser-picture-page",
    "no active CoserPicture archive with a positive page_count exists",
  );

  const audio = requireRow(
    database.prepare(`
      SELECT work.id AS work_id, asset.id AS asset_id, COALESCE(asset.size, 0) AS size
      FROM works AS work
      JOIN assets AS asset ON asset.work_id = work.id
      WHERE work.kind = 'audio'
        AND work.deleted_at IS NULL
        AND (asset.role = 'track' OR asset.mime LIKE 'audio/%')
        AND COALESCE(asset.size, 0) >= ?1
      ORDER BY COALESCE(asset.size, 0) DESC, asset.id ASC
      LIMIT 1
    `).get(rangeBytes),
    "audio-startup-range",
    `no active audio track is at least ${rangeBytes} bytes`,
  );

  const novel = requireRow(
    database.prepare(`
      SELECT work.id AS work_id, asset.id AS asset_id
      FROM works AS work
      JOIN assets AS asset ON asset.work_id = work.id
      WHERE work.kind = 'novel'
        AND work.deleted_at IS NULL
        AND asset.role = 'book'
      ORDER BY COALESCE(asset.size, 0) DESC, asset.id ASC
      LIMIT 1
    `).get(),
    "novel-summary-detail",
    "no active novel book asset exists",
  );

  const tag = database.prepare(`
    SELECT tag.namespace, tag.key, COUNT(DISTINCT work_tag.work_id) AS work_count
    FROM tags AS tag
    JOIN work_tags AS work_tag ON work_tag.tag_id = tag.id
    JOIN works AS work ON work.id = work_tag.work_id
    WHERE work.deleted_at IS NULL
    GROUP BY tag.id, tag.namespace, tag.key
    ORDER BY work_count DESC, tag.id ASC
    LIMIT 1
  `).get();
  if (!tag) throw new Error("cannot prepare tag-filter: no tag is attached to an active work");

  const includeTag = `${tag.namespace}:${tag.key}`;
  const targets = [
    target({
      id: "catalog-first-page",
      scenario: "catalog-first-page",
      warm: "warm",
      url: path(baseUrl, "/api/catalog/works?limit=100"),
      requests,
      concurrency,
    }),
    target({
      id: "tag-filter",
      scenario: "tag-filter",
      warm: "warm",
      url: path(baseUrl, `/api/catalog/works?include_tag=${encodeURIComponent(includeTag)}&limit=100`),
      requests,
      concurrency,
    }),
    target({
      id: "gallery-thumbnail-warm",
      scenario: "gallery-thumbnail",
      warm: "warm",
      url: path(baseUrl, `/api/assets/${gallery.asset_id}/thumb?size=256`),
      requests,
      concurrency,
    }),
    target({
      id: "gallery-thumbnail-cold",
      scenario: "gallery-thumbnail",
      warm: "cold",
      url: path(baseUrl, `/api/assets/${gallery.asset_id}/thumb?size=256`),
      requests,
      concurrency,
    }),
    target({
      id: "comic-page-cold",
      scenario: "comic-page",
      warm: "cold",
      url: path(baseUrl, `/api/works/${comic.work_id}/pages/0/stream?size=1280`),
      requests,
      concurrency,
    }),
    target({
      id: "coser-picture-page-cold",
      scenario: "coser-picture-page",
      warm: "cold",
      url: path(baseUrl, `/api/works/${coserPicture.work_id}/pages/0/stream?size=1280`),
      requests,
      concurrency,
    }),
    target({
      id: "audio-startup-range",
      scenario: "audio-startup-range",
      warm: "cold",
      url: path(baseUrl, `/api/assets/${audio.asset_id}/stream`),
      range: `bytes=0-${rangeBytes - 1}`,
      maxBodyBytes: rangeBytes,
      requirePartial: true,
      requests,
      concurrency,
    }),
    target({
      id: "novel-summary-detail",
      scenario: "novel-summary-detail",
      warm: "warm",
      url: path(baseUrl, `/api/works/${novel.work_id}?asset_mode=summary`),
      requests,
      concurrency,
    }),
  ];

  mkdirSync(dirname(outputPath), { recursive: true });
  writeFileSync(
    outputPath,
    `${JSON.stringify({
      profile: "nas-n100-4g",
      scope: "media-preview-filter-and-startup",
      prepared_by: "prepare-media-targets",
      selection: {
        gallery_asset_id: gallery.asset_id,
        comic_work_id: comic.work_id,
        coser_picture_work_id: coserPicture.work_id,
        audio_asset_id: audio.asset_id,
        audio_size_bytes: Number(audio.size),
        novel_work_id: novel.work_id,
        tag: includeTag,
        tag_work_count: Number(tag.work_count),
      },
      targets,
    }, null, 2)}\n`,
    { flag: "wx" },
  );
  console.log(JSON.stringify({
    status: "prepared",
    output: outputPath,
    target_count: targets.length,
    audio_size_bytes: Number(audio.size),
    tag: includeTag,
  }));
} finally {
  database.close();
}
