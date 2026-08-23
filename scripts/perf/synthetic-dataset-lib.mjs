import { statSync } from "node:fs";
import { DatabaseSync } from "node:sqlite";
import { performance } from "node:perf_hooks";
import { CURRENT_SCHEMA_VERSION } from "./schema-version.mjs";

// The default fixture is intentionally exact-versioned. A future migration
// must update the profile deliberately instead of silently generating a
// corpus against an unreviewed schema shape.
const REQUIRED_SCHEMA_VERSION = CURRENT_SCHEMA_VERSION;
const NUMBER_BATCH_SIZE = 10_000;

export const R1G_40K_PROFILE = Object.freeze({
  name: "r1g-40k-740k-800k-v6",
  seed: 20260731,
  requiredSchemaVersion: REQUIRED_SCHEMA_VERSION,
  worksByKind: [
    { kind: "gallery", count: 2_000 },
    { kind: "novel", count: 10_000 },
    { kind: "comic", count: 10_000 },
    { kind: "coser-picture", count: 8_000 },
    { kind: "audio", count: 10_000 },
  ],
  assetGroups: [
    {
      name: "gallery-images",
      kind: "gallery",
      count: 700_000,
      role: "image",
      mime: "image/jpeg",
      suffix: "jpg",
      size: 10_000_000,
      meta: {},
    },
    {
      name: "novel-books",
      kind: "novel",
      count: 10_000,
      role: "book",
      mime: "application/epub+zip",
      suffix: "epub",
      size: 10_000_000,
      meta: {},
    },
    {
      name: "comic-archives",
      kind: "comic",
      count: 10_000,
      role: "archive",
      mime: "application/vnd.comicbook+zip",
      suffix: "cbz",
      size: 1_000_000_000,
      meta: { page_count: 250 },
    },
    {
      name: "cos-archives",
      kind: "coser-picture",
      count: 8_000,
      role: "archive",
      mime: "application/zip",
      suffix: "zip",
      size: 750_000_000,
      meta: { page_count: 300 },
    },
    {
      name: "audio-tracks",
      kind: "audio",
      count: 10_000,
      role: "track",
      mime: "audio/flac",
      suffix: "flac",
      size: 600_000_000,
      meta: { duration_seconds: 1_800 },
    },
    {
      name: "gallery-covers",
      kind: "gallery",
      count: 2_000,
      role: "cover",
      mime: "image/jpeg",
      suffix: "jpg",
      size: 500_000,
      meta: {},
    },
  ],
  tagCount: 2_048,
  tagTiers: [
    { tags: 5, works: 8_000 },
    { tags: 20, works: 30_500 },
    { tags: 100, works: 1_500 },
  ],
  tierPermutation: 7_919,
  hotTagOrdinal: 1,
  warmTagOrdinal: 2,
  warmTagModulo: 2,
});

function asNumber(value) {
  return typeof value === "bigint" ? Number(value) : Number(value ?? 0);
}

function assertPositiveInteger(value, label) {
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new TypeError(`${label} must be a positive safe integer`);
  }
}

function validateProfile(profile) {
  if (!profile || typeof profile.name !== "string" || !profile.name.trim()) {
    throw new TypeError("synthetic profile must have a name");
  }
  if (!Array.isArray(profile.worksByKind) || !profile.worksByKind.length) {
    throw new TypeError("synthetic profile must define worksByKind");
  }
  const kinds = new Map();
  for (const [index, group] of profile.worksByKind.entries()) {
    if (!group || typeof group.kind !== "string" || !group.kind.trim()) {
      throw new TypeError(`worksByKind[${index}].kind must be non-empty`);
    }
    assertPositiveInteger(group.count, `worksByKind[${index}].count`);
    if (kinds.has(group.kind)) throw new TypeError(`duplicate work kind ${group.kind}`);
    kinds.set(group.kind, group.count);
  }
  if (!Array.isArray(profile.assetGroups) || !profile.assetGroups.length) {
    throw new TypeError("synthetic profile must define assetGroups");
  }
  for (const [index, group] of profile.assetGroups.entries()) {
    for (const field of ["name", "kind", "role", "mime", "suffix"]) {
      if (typeof group?.[field] !== "string" || !group[field].trim()) {
        throw new TypeError(`assetGroups[${index}].${field} must be non-empty`);
      }
    }
    if (!kinds.has(group.kind)) {
      throw new TypeError(`assetGroups[${index}] references unknown kind ${group.kind}`);
    }
    assertPositiveInteger(group.count, `assetGroups[${index}].count`);
    assertPositiveInteger(group.size, `assetGroups[${index}].size`);
  }
  assertPositiveInteger(profile.tagCount, "tagCount");
  const totalWorks = [...kinds.values()].reduce((sum, count) => sum + count, 0);
  if (profile.tagTiers != null) {
    if (!Array.isArray(profile.tagTiers) || !profile.tagTiers.length) {
      throw new TypeError("tagTiers must be a non-empty array");
    }
    let tierWorks = 0;
    for (const [index, tier] of profile.tagTiers.entries()) {
      assertPositiveInteger(tier?.tags, `tagTiers[${index}].tags`);
      assertPositiveInteger(tier?.works, `tagTiers[${index}].works`);
      if (tier.tags > profile.tagCount) {
        throw new TypeError(`tagTiers[${index}].tags cannot exceed tagCount`);
      }
      tierWorks += tier.works;
    }
    if (tierWorks !== totalWorks) {
      throw new TypeError(`tagTiers covers ${tierWorks} works, expected ${totalWorks}`);
    }
    assertPositiveInteger(profile.tierPermutation, "tierPermutation");
    const gcd = (left, right) => (right === 0 ? left : gcd(right, left % right));
    if (gcd(profile.tierPermutation, totalWorks) !== 1) {
      throw new TypeError("tierPermutation must be coprime with the total work count");
    }
  } else {
    assertPositiveInteger(profile.tagsPerWork, "tagsPerWork");
    if (profile.tagsPerWork > profile.tagCount) {
      throw new TypeError("tagsPerWork cannot exceed tagCount");
    }
  }
  assertPositiveInteger(profile.hotTagOrdinal ?? 1, "hotTagOrdinal");
  if ((profile.hotTagOrdinal ?? 1) > profile.tagCount) {
    throw new TypeError("hotTagOrdinal cannot exceed tagCount");
  }
  if (profile.warmTagOrdinal != null) {
    assertPositiveInteger(profile.warmTagOrdinal, "warmTagOrdinal");
    assertPositiveInteger(profile.warmTagModulo, "warmTagModulo");
    if (profile.hotTagOrdinal !== 1 || profile.warmTagOrdinal !== 2) {
      throw new TypeError("the reserved hot/warm tag ordinals must be 1 and 2");
    }
    if (profile.tagCount <= 2) throw new TypeError("warm tag distribution needs at least 3 tags");
    const minimumTags = profile.tagTiers
      ? Math.min(...profile.tagTiers.map((tier) => tier.tags))
      : profile.tagsPerWork;
    if (minimumTags < 2) {
      throw new TypeError("warm tag distribution requires at least 2 tags on every work");
    }
  }
  assertPositiveInteger(profile.requiredSchemaVersion, "requiredSchemaVersion");
  return kinds;
}

function tableExists(database, name) {
  return Boolean(
    database
      .prepare("SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = ?")
      .get(name),
  );
}

function count(database, table) {
  return asNumber(database.prepare(`SELECT COUNT(*) AS count FROM ${table}`).get().count);
}

function validateDatabase(database, profile) {
  const requiredTables = [
    "schema_migrations",
    "works",
    "assets",
    "tags",
    "work_tags",
    "work_stats",
    "catalog_state",
    "tag_kind_counts",
    "tag_kind_count_state",
    "scanner_works",
    "scanner_assets",
    "work_tag_sources",
  ];
  const missing = requiredTables.filter((table) => !tableExists(database, table));
  if (missing.length) {
    throw new Error(
      `database is not initialized by the server migrations; missing: ${missing.join(", ")}`,
    );
  }
  const version = asNumber(
    database
      .prepare("SELECT COALESCE(MAX(version), 0) AS version FROM schema_migrations")
      .get().version,
  );
  if (version !== profile.requiredSchemaVersion) {
    throw new Error(
      `synthetic profile requires schema v${profile.requiredSchemaVersion}, found v${version}`,
    );
  }
  const nonEmpty = ["works", "assets", "tags", "work_tags"].filter(
    (table) => count(database, table) !== 0,
  );
  if (nonEmpty.length) {
    throw new Error(
      `synthetic generation only accepts an empty migrated database; non-empty: ${nonEmpty.join(", ")}`,
    );
  }
}

function quoteIdentifier(value) {
  return `"${String(value).replaceAll('"', '""')}"`;
}

function triggersForTables(database, tables) {
  const placeholders = tables.map(() => "?").join(",");
  return database
    .prepare(
      `SELECT name, sql FROM sqlite_schema
       WHERE type = 'trigger' AND tbl_name IN (${placeholders})
       ORDER BY name`,
    )
    .all(...tables)
    .filter((row) => typeof row.sql === "string" && row.sql.trim());
}

function checkpoint(database) {
  const walPath = database.__perfDatabasePath ? `${database.__perfDatabasePath}-wal` : null;
  let walBytes = null;
  if (walPath) {
    try {
      walBytes = statSync(walPath).size;
    } catch {
      walBytes = null;
    }
  }
  const row = database.prepare("PRAGMA wal_checkpoint(TRUNCATE)").get();
  return {
    wal_bytes_before: walBytes,
    busy: asNumber(row.busy),
    log_frames: asNumber(row.log),
    checkpointed_frames: asNumber(row.checkpointed),
  };
}

function runPhase(database, timings, name, triggerTables, callback) {
  const triggers = triggersForTables(database, triggerTables);
  const started = performance.now();
  database.exec("BEGIN IMMEDIATE");
  try {
    for (const trigger of triggers) {
      database.exec(`DROP TRIGGER ${quoteIdentifier(trigger.name)}`);
    }
    callback();
    for (const trigger of triggers) database.exec(trigger.sql);
    database.exec("COMMIT");
  } catch (error) {
    database.exec("ROLLBACK");
    throw error;
  }
  const checkpointResult = checkpoint(database);
  timings.push({
    phase: name,
    elapsed_ms: performance.now() - started,
    triggers_restored: triggers.length,
    checkpoint: checkpointResult,
  });
}

function createTemporarySequences(database, maximum) {
  const digits = Array.from({ length: 10 }, (_, value) => `(${value})`).join(",");
  database.exec(`
    CREATE TEMP TABLE perf_digits(value INTEGER PRIMARY KEY) WITHOUT ROWID;
    INSERT INTO perf_digits(value) VALUES ${digits};
    CREATE TEMP TABLE perf_numbers(value INTEGER PRIMARY KEY) WITHOUT ROWID;
    INSERT INTO perf_numbers(value)
    SELECT ones.value
         + tens.value * 10
         + hundreds.value * 100
         + thousands.value * 1000
         + ten_thousands.value * 10000
         + 1
    FROM perf_digits AS ones
    CROSS JOIN perf_digits AS tens
    CROSS JOIN perf_digits AS hundreds
    CROSS JOIN perf_digits AS thousands
    CROSS JOIN perf_digits AS ten_thousands
    WHERE ones.value
        + tens.value * 10
        + hundreds.value * 100
        + thousands.value * 1000
        + ten_thousands.value * 10000 < ${maximum};
  `);
}

function insertWorks(database, profile) {
  const statement = database.prepare(`
    INSERT INTO works (
      kind, title, category, description, source_path, meta_json, created_at, updated_at
    )
    SELECT
      ?,
      'R1GCommon ' || ? || ' work ' || printf('%08d', ? + number.value),
      'synthetic-' || ?,
      'deterministic R1G performance fixture seed ${profile.seed}',
      '/perf/r1g/' || printf('%08d', ? + number.value),
      json_object('synthetic_profile', ?, 'ordinal', ? + number.value),
      printf(
        '2026-01-%02dT%02d:%02d:%02d.000Z',
        1 + ((? + number.value) % 28),
        ((? + number.value) % 24),
        ((? + number.value) % 60),
        ((? + number.value * 7) % 60)
      ),
      printf(
        '2026-01-%02dT%02d:%02d:%02d.000Z',
        1 + ((? + number.value) % 28),
        ((? + number.value) % 24),
        ((? + number.value) % 60),
        ((? + number.value * 7) % 60)
      )
    FROM perf_numbers AS number
    WHERE number.value <= ?
  `);
  let offset = 0;
  for (const group of profile.worksByKind) {
    statement.run(
      group.kind,
      group.kind,
      offset,
      group.kind,
      offset,
      profile.name,
      offset,
      offset,
      offset,
      offset,
      offset,
      offset,
      offset,
      offset,
      offset,
      group.count,
    );
    offset += group.count;
  }
  database.exec(`
    CREATE TEMP TABLE perf_work_map (
      ordinal INTEGER PRIMARY KEY,
      work_id INTEGER NOT NULL UNIQUE,
      kind TEXT NOT NULL,
      kind_ordinal INTEGER NOT NULL
    ) WITHOUT ROWID;
    INSERT INTO perf_work_map(ordinal, work_id, kind, kind_ordinal)
    SELECT
      CAST(substr(source_path, length('/perf/r1g/') + 1) AS INTEGER),
      id,
      kind,
      ROW_NUMBER() OVER (PARTITION BY kind ORDER BY id)
    FROM works
    WHERE source_path LIKE '/perf/r1g/%';
    CREATE INDEX perf_work_map_kind ON perf_work_map(kind, kind_ordinal);

    DELETE FROM work_stats;
    INSERT INTO work_stats (
      work_id, asset_count, tag_count, image_count, track_count, page_count,
      catalog_revision, computed_at, collection_key, collection_title
    )
    SELECT
      work_id, 0, 0, 0, 0, 0,
      (SELECT revision FROM catalog_state WHERE singleton = 1),
      strftime('%Y-%m-%dT%H:%M:%fZ','now'),
      'perf:' || kind || ':' || printf('%04d', ((kind_ordinal - 1) / 100) + 1),
      'Synthetic ' || kind || ' collection ' || printf('%04d', ((kind_ordinal - 1) / 100) + 1)
    FROM perf_work_map;
    UPDATE catalog_state
    SET revision = revision + 1,
        updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
    WHERE singleton = 1;
  `);
}

function insertAssetGroup(database, group, kindWorkCount) {
  const statement = database.prepare(`
    INSERT INTO assets (
      work_id, path, mime, role, variant, position, size, meta_json
    )
    SELECT
      work.work_id,
      '/perf/media/' || ? || '/' || printf('%08d', work.kind_ordinal) ||
        '/' || ? || '-' || printf('%08d', ? + number.value) || '.' || ?,
      ?,
      ?,
      '',
      CAST((? + number.value - 1) / ? AS INTEGER),
      ?,
      ?
    FROM perf_numbers AS number
    JOIN perf_work_map AS work
      ON work.kind = ?
     AND work.kind_ordinal = ((? + number.value - 1) % ?) + 1
    WHERE number.value <= ?
  `);
  const meta = JSON.stringify(group.meta ?? {});
  for (let offset = 0; offset < group.count; offset += NUMBER_BATCH_SIZE) {
    const batch = Math.min(NUMBER_BATCH_SIZE, group.count - offset);
    statement.run(
      group.name,
      group.role,
      offset,
      group.suffix,
      group.mime,
      group.role,
      offset,
      kindWorkCount,
      group.size,
      meta,
      group.kind,
      offset,
      kindWorkCount,
      batch,
    );
  }
}

function rebuildAssetStats(database) {
  database.exec(`
    WITH asset_counts AS (
      SELECT
        work_id,
        COUNT(*) AS asset_count,
        SUM(CASE WHEN mime LIKE 'image/%' THEN 1 ELSE 0 END) AS image_count,
        SUM(CASE WHEN role = 'track' OR mime LIKE 'audio/%' THEN 1 ELSE 0 END) AS track_count,
        SUM(CASE
          WHEN role = 'page' THEN 1
          WHEN role = 'archive' THEN CAST(COALESCE(json_extract(meta_json, '$.page_count'), 0) AS INTEGER)
          ELSE 0
        END) AS page_count
      FROM assets
      GROUP BY work_id
    )
    UPDATE work_stats
    SET asset_count = COALESCE((
          SELECT asset_count FROM asset_counts WHERE asset_counts.work_id = work_stats.work_id
        ), 0),
        image_count = COALESCE((
          SELECT image_count FROM asset_counts WHERE asset_counts.work_id = work_stats.work_id
        ), 0),
        track_count = COALESCE((
          SELECT track_count FROM asset_counts WHERE asset_counts.work_id = work_stats.work_id
        ), 0),
        page_count = COALESCE((
          SELECT page_count FROM asset_counts WHERE asset_counts.work_id = work_stats.work_id
        ), 0),
        catalog_revision = (SELECT revision FROM catalog_state WHERE singleton = 1),
        computed_at = strftime('%Y-%m-%dT%H:%M:%fZ','now');
  `);
}

function insertTagsAndLinks(database, profile) {
  database
    .prepare(`
      INSERT INTO tags (
        namespace, key, label, translated_label, translated_namespace, source, intro, links, count
      )
      SELECT
        CASE WHEN value % 8 = 0 THEN 'artist' ELSE 'genre' END,
        'perf-tag-' || printf('%04d', value),
        'Synthetic tag ' || printf('%04d', value),
        '合成标签 ' || printf('%04d', value),
        CASE WHEN value % 8 = 0 THEN '作者' ELSE '类型' END,
        'perf-r1g',
        'deterministic performance fixture tag',
        NULL,
        0
      FROM perf_numbers
      WHERE value <= ?
    `)
    .run(profile.tagCount);
  database.exec(`
    CREATE TEMP TABLE perf_tag_map (
      ordinal INTEGER PRIMARY KEY,
      tag_id INTEGER NOT NULL UNIQUE
    ) WITHOUT ROWID;
    INSERT INTO perf_tag_map(ordinal, tag_id)
    SELECT
      CAST(substr(key, length('perf-tag-') + 1) AS INTEGER),
      id
    FROM tags
    WHERE source = 'perf-r1g';
  `);
  const totalWorks = profile.worksByKind.reduce((sum, group) => sum + group.count, 0);
  let tagLimitExpression;
  if (profile.tagTiers) {
    let upper = 0;
    const branches = profile.tagTiers.map((tier, index) => {
      upper += tier.works;
      return index === profile.tagTiers.length - 1
        ? `ELSE ${tier.tags}`
        : `WHEN tier_bucket <= ${upper} THEN ${tier.tags}`;
    });
    tagLimitExpression = `CASE ${branches.join(" ")} END`;
  } else {
    tagLimitExpression = String(profile.tagsPerWork);
  }
  const tierBucket = profile.tagTiers
    ? `1 + (((ordinal - 1) * ${profile.tierPermutation}) % ${totalWorks})`
    : "ordinal";
  database.exec(`
    CREATE TEMP TABLE perf_work_tag_limits (
      work_ordinal INTEGER PRIMARY KEY,
      tag_limit INTEGER NOT NULL
    ) WITHOUT ROWID;
    INSERT INTO perf_work_tag_limits(work_ordinal, tag_limit)
    SELECT ordinal, ${tagLimitExpression}
    FROM (
      SELECT ordinal, ${tierBucket} AS tier_bucket
      FROM perf_work_map
    );
  `);
  const statement = database.prepare(`
    INSERT INTO work_tags(work_id, tag_id)
    SELECT work.work_id, tag.tag_id
    FROM perf_work_map AS work
    JOIN perf_work_tag_limits AS limits ON limits.work_ordinal = work.ordinal
    CROSS JOIN perf_numbers AS slot
    JOIN perf_tag_map AS tag
      ON tag.ordinal = CASE
        WHEN slot.value = 1 OR ? = 1 THEN ?
        WHEN ? IS NOT NULL AND slot.value = 2 AND work.ordinal % ? = 0 THEN ?
        WHEN ? IS NOT NULL
          THEN 3 + ((work.ordinal * 37 + (slot.value - 2) * 97) % (? - 2))
        ELSE 2 + ((work.ordinal * 37 + (slot.value - 2) * 97) % (? - 1))
      END
    WHERE work.ordinal > ?
      AND work.ordinal <= ?
      AND slot.value <= limits.tag_limit
  `);
  for (let offset = 0; offset < totalWorks; offset += 500) {
    const end = Math.min(totalWorks, offset + 500);
    statement.run(
      profile.tagCount,
      profile.hotTagOrdinal ?? 1,
      profile.warmTagOrdinal ?? null,
      profile.warmTagModulo ?? 1,
      profile.warmTagOrdinal ?? null,
      profile.warmTagOrdinal ?? null,
      profile.tagCount,
      profile.tagCount,
      offset,
      end,
    );
  }
  database.exec(`
    WITH tag_counts AS (
      SELECT tag_id, COUNT(*) AS link_count
      FROM work_tags
      GROUP BY tag_id
    )
    UPDATE tags
    SET count = COALESCE((
      SELECT link_count FROM tag_counts WHERE tag_counts.tag_id = tags.id
    ), 0);

    WITH work_tag_counts AS (
      SELECT work_id, COUNT(*) AS link_count
      FROM work_tags
      GROUP BY work_id
    )
    UPDATE work_stats
    SET tag_count = COALESCE((
          SELECT link_count FROM work_tag_counts
          WHERE work_tag_counts.work_id = work_stats.work_id
        ), 0),
        catalog_revision = (SELECT revision FROM catalog_state WHERE singleton = 1),
        computed_at = strftime('%Y-%m-%dT%H:%M:%fZ','now');

    UPDATE catalog_state
    SET revision = revision + 1,
        updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
    WHERE singleton = 1;
    DELETE FROM tag_kind_counts;
    INSERT INTO tag_kind_counts(kind, tag_id, work_count, catalog_revision)
    SELECT work.kind, work_tag.tag_id, COUNT(*), state.revision
    FROM work_tags AS work_tag
    JOIN works AS work ON work.id = work_tag.work_id
    CROSS JOIN catalog_state AS state
    WHERE state.singleton = 1
    GROUP BY work.kind, work_tag.tag_id;
    UPDATE tag_kind_count_state
    SET ready = 1,
        catalog_revision = (SELECT revision FROM catalog_state WHERE singleton = 1),
        updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now'),
        last_error = NULL
    WHERE singleton = 1;
  `);
}

function insertOwnership(database) {
  database.exec(`
    INSERT INTO scanner_works(work_id, scope, seen_token, fingerprint)
    SELECT id, 'legacy|' || kind, 'perf-r1g', 'synthetic'
    FROM works;
    INSERT INTO scanner_assets(asset_id, work_id, seen_token)
    SELECT id, work_id, 'perf-r1g'
    FROM assets;
    INSERT INTO work_tag_sources(work_id, tag_id, owner, seen_token)
    SELECT work_id, tag_id, 'external', NULL
    FROM work_tags;
  `);
}

function actualCounts(database) {
  const scalar = (sql) => asNumber(database.prepare(sql).get().value);
  return {
    works: scalar("SELECT COUNT(*) AS value FROM works"),
    assets: scalar("SELECT COUNT(*) AS value FROM assets"),
    tags: scalar("SELECT COUNT(*) AS value FROM tags"),
    work_tags: scalar("SELECT COUNT(*) AS value FROM work_tags"),
    scanner_works: scalar("SELECT COUNT(*) AS value FROM scanner_works"),
    scanner_assets: scalar("SELECT COUNT(*) AS value FROM scanner_assets"),
    work_tag_sources: scalar("SELECT COUNT(*) AS value FROM work_tag_sources"),
    tag_kind_counts: scalar("SELECT COUNT(*) AS value FROM tag_kind_counts"),
    logical_asset_bytes: scalar("SELECT COALESCE(SUM(size), 0) AS value FROM assets"),
  };
}

function expectedCounts(profile) {
  const works = profile.worksByKind.reduce((sum, group) => sum + group.count, 0);
  const assets = profile.assetGroups.reduce((sum, group) => sum + group.count, 0);
  const workTags = profile.tagTiers
    ? profile.tagTiers.reduce((sum, tier) => sum + tier.works * tier.tags, 0)
    : works * profile.tagsPerWork;
  return {
    works,
    assets,
    tags: profile.tagCount,
    work_tags: workTags,
    scanner_works: works,
    scanner_assets: assets,
    work_tag_sources: workTags,
    logical_asset_bytes: profile.assetGroups.reduce(
      (sum, group) => sum + group.count * group.size,
      0,
    ),
  };
}

function verifyFacts(database, profile) {
  const expected = expectedCounts(profile);
  const actual = actualCounts(database);
  for (const field of [
    "works",
    "assets",
    "tags",
    "work_tags",
    "scanner_works",
    "scanner_assets",
    "work_tag_sources",
    "logical_asset_bytes",
  ]) {
    if (actual[field] !== expected[field]) {
      throw new Error(`synthetic ${field} mismatch: expected ${expected[field]}, got ${actual[field]}`);
    }
  }
  const mismatchedStats = asNumber(
    database
      .prepare(`
        SELECT COUNT(*) AS count
        FROM work_stats AS stats
        WHERE stats.tag_count != (
          SELECT COUNT(*) FROM work_tags WHERE work_id = stats.work_id
        ) OR stats.asset_count != (
          SELECT COUNT(*) FROM assets WHERE work_id = stats.work_id
        )
      `)
      .get().count,
  );
  if (mismatchedStats) throw new Error(`${mismatchedStats} work_stats rows do not match facts`);
  const actualTagTiers = database
    .prepare(
      `SELECT tag_count AS tags, COUNT(*) AS works
       FROM work_stats
       GROUP BY tag_count
       ORDER BY tag_count`,
    )
    .all()
    .map((row) => ({ tags: asNumber(row.tags), works: asNumber(row.works) }));
  const expectedTagTiers = profile.tagTiers
    ? [...profile.tagTiers].sort((left, right) => left.tags - right.tags)
    : [{ tags: profile.tagsPerWork, works: expected.works }];
  if (JSON.stringify(actualTagTiers) !== JSON.stringify(expectedTagTiers)) {
    throw new Error(
      `synthetic tag tier mismatch: expected ${JSON.stringify(expectedTagTiers)}, got ${JSON.stringify(actualTagTiers)}`,
    );
  }
  const hotTagLinks = asNumber(
    database
      .prepare(
        `SELECT COUNT(*) AS count
         FROM work_tags AS work_tag
         JOIN perf_tag_map AS tag ON tag.tag_id = work_tag.tag_id
         WHERE tag.ordinal = ?`,
      )
      .get(profile.hotTagOrdinal ?? 1).count,
  );
  if (hotTagLinks !== expected.works) {
    throw new Error(`hot tag links mismatch: expected ${expected.works}, got ${hotTagLinks}`);
  }
  let warmTagLinks = null;
  if (profile.warmTagOrdinal != null) {
    warmTagLinks = asNumber(
      database
        .prepare(
          `SELECT COUNT(*) AS count
           FROM work_tags AS work_tag
           JOIN perf_tag_map AS tag ON tag.tag_id = work_tag.tag_id
           WHERE tag.ordinal = ?`,
        )
        .get(profile.warmTagOrdinal).count,
    );
    const expectedWarmTagLinks = Math.floor(expected.works / profile.warmTagModulo);
    if (warmTagLinks !== expectedWarmTagLinks) {
      throw new Error(
        `warm tag links mismatch: expected ${expectedWarmTagLinks}, got ${warmTagLinks}`,
      );
    }
  }
  const facetReady = database
    .prepare("SELECT ready, catalog_revision FROM tag_kind_count_state WHERE singleton = 1")
    .get();
  const catalogRevision = asNumber(
    database.prepare("SELECT revision FROM catalog_state WHERE singleton = 1").get().revision,
  );
  if (asNumber(facetReady.ready) !== 1 || asNumber(facetReady.catalog_revision) !== catalogRevision) {
    throw new Error("tag_kind_counts is not ready at the current catalog revision");
  }
  const foreignKeyViolations = database.prepare("PRAGMA foreign_key_check").all();
  if (foreignKeyViolations.length) {
    throw new Error(`synthetic database has ${foreignKeyViolations.length} foreign-key violations`);
  }
  const integrity = database.prepare("PRAGMA integrity_check").get().integrity_check;
  if (integrity !== "ok") throw new Error(`SQLite integrity_check returned ${integrity}`);
  return {
    expected,
    actual,
    tag_tiers: actualTagTiers,
    hot_tag_links: hotTagLinks,
    warm_tag_links: warmTagLinks,
    mismatched_stats: mismatchedStats,
    integrity_check: integrity,
  };
}

export function populateSyntheticDataset(databasePath, profile = R1G_40K_PROFILE) {
  const kinds = validateProfile(profile);
  const database = new DatabaseSync(databasePath);
  Object.defineProperty(database, "__perfDatabasePath", { value: databasePath });
  const timings = [];
  const startedAt = new Date().toISOString();
  const started = performance.now();
  try {
    database.exec("PRAGMA foreign_keys = ON; PRAGMA busy_timeout = 30000;");
    validateDatabase(database, profile);
    createTemporarySequences(database, NUMBER_BATCH_SIZE);

    runPhase(database, timings, "works", ["works"], () => insertWorks(database, profile));
    runPhase(database, timings, "assets", ["assets"], () => {
      for (const group of profile.assetGroups) {
        insertAssetGroup(database, group, kinds.get(group.kind));
      }
      rebuildAssetStats(database);
    });
    runPhase(database, timings, "tags-and-links", ["tags", "work_tags"], () => {
      insertTagsAndLinks(database, profile);
    });
    runPhase(database, timings, "ownership", [], () => insertOwnership(database));

    const analyzeStarted = performance.now();
    database.exec("ANALYZE; PRAGMA optimize;");
    timings.push({
      phase: "analyze-optimize",
      elapsed_ms: performance.now() - analyzeStarted,
      triggers_restored: 0,
      checkpoint: checkpoint(database),
    });
    const verification = verifyFacts(database, profile);
    return {
      artifact_version: 1,
      status: "generated-and-verified",
      profile: profile.name,
      seed: profile.seed,
      schema_version: profile.requiredSchemaVersion,
      started_at: startedAt,
      completed_at: new Date().toISOString(),
      elapsed_ms: performance.now() - started,
      distribution: {
        works_by_kind: profile.worksByKind,
        asset_groups: profile.assetGroups.map(({ meta, ...group }) => ({ ...group, meta })),
        tag_tiers: profile.tagTiers ?? [{ tags: profile.tagsPerWork, works: verification.expected.works }],
        hot_tag_ordinal: profile.hotTagOrdinal ?? 1,
        warm_tag_ordinal: profile.warmTagOrdinal ?? null,
        warm_tag_modulo: profile.warmTagModulo ?? null,
      },
      ...verification,
      phases: timings,
      database_bytes: statSync(databasePath).size,
    };
  } finally {
    database.close();
  }
}
