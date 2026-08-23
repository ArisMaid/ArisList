import assert from "node:assert/strict";
import { mkdtempSync } from "node:fs";
import os from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import test from "node:test";

import { populateSyntheticDataset, R1G_40K_PROFILE } from "./synthetic-dataset-lib.mjs";
import { CURRENT_SCHEMA_VERSION } from "./schema-version.mjs";

function createSchema(path) {
  const database = new DatabaseSync(path);
  database.exec(`
    PRAGMA foreign_keys = ON;
    CREATE TABLE schema_migrations(version INTEGER PRIMARY KEY);
    INSERT INTO schema_migrations(version) VALUES (9);
    CREATE TABLE works (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      kind TEXT NOT NULL,
      title TEXT NOT NULL,
      subtitle TEXT,
      category TEXT,
      description TEXT,
      rating REAL,
      progress REAL NOT NULL DEFAULT 0,
      source_path TEXT,
      cover_asset_id INTEGER,
      meta_json TEXT NOT NULL DEFAULT '{}',
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      UNIQUE(kind, source_path)
    );
    CREATE TABLE assets (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      work_id INTEGER NOT NULL REFERENCES works(id) ON DELETE CASCADE,
      path TEXT NOT NULL,
      mime TEXT NOT NULL,
      role TEXT NOT NULL,
      variant TEXT NOT NULL DEFAULT '',
      position INTEGER NOT NULL DEFAULT -1,
      size INTEGER,
      meta_json TEXT NOT NULL DEFAULT '{}',
      created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
      UNIQUE(work_id, path, role, variant)
    );
    CREATE TABLE tags (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      namespace TEXT NOT NULL,
      key TEXT NOT NULL,
      label TEXT NOT NULL,
      translated_label TEXT,
      translated_namespace TEXT,
      source TEXT NOT NULL,
      intro TEXT,
      links TEXT,
      count INTEGER NOT NULL DEFAULT 0,
      UNIQUE(namespace, key)
    );
    CREATE TABLE work_tags (
      work_id INTEGER NOT NULL REFERENCES works(id) ON DELETE CASCADE,
      tag_id INTEGER NOT NULL REFERENCES tags(id) ON DELETE CASCADE,
      PRIMARY KEY(work_id, tag_id)
    );
    CREATE TABLE catalog_state (
      singleton INTEGER PRIMARY KEY,
      revision INTEGER NOT NULL,
      updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
    );
    INSERT INTO catalog_state(singleton, revision) VALUES (1, 1);
    CREATE TABLE work_stats (
      work_id INTEGER PRIMARY KEY REFERENCES works(id) ON DELETE CASCADE,
      asset_count INTEGER NOT NULL DEFAULT 0,
      tag_count INTEGER NOT NULL DEFAULT 0,
      image_count INTEGER NOT NULL DEFAULT 0,
      track_count INTEGER NOT NULL DEFAULT 0,
      page_count INTEGER NOT NULL DEFAULT 0,
      catalog_revision INTEGER NOT NULL DEFAULT 0,
      computed_at TEXT,
      collection_key TEXT,
      collection_title TEXT
    );
    CREATE TABLE tag_kind_counts (
      kind TEXT NOT NULL,
      tag_id INTEGER NOT NULL REFERENCES tags(id) ON DELETE CASCADE,
      work_count INTEGER NOT NULL,
      catalog_revision INTEGER NOT NULL,
      PRIMARY KEY(kind, tag_id)
    ) WITHOUT ROWID;
    CREATE TABLE tag_kind_count_state (
      singleton INTEGER PRIMARY KEY,
      ready INTEGER NOT NULL,
      catalog_revision INTEGER NOT NULL,
      updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
      last_error TEXT
    );
    INSERT INTO tag_kind_count_state(singleton, ready, catalog_revision) VALUES (1, 0, 0);
    CREATE TABLE scanner_works (
      work_id INTEGER PRIMARY KEY REFERENCES works(id) ON DELETE CASCADE,
      scope TEXT NOT NULL,
      seen_token TEXT NOT NULL,
      fingerprint TEXT
    );
    CREATE TABLE scanner_assets (
      asset_id INTEGER PRIMARY KEY REFERENCES assets(id) ON DELETE CASCADE,
      work_id INTEGER NOT NULL REFERENCES works(id) ON DELETE CASCADE,
      seen_token TEXT NOT NULL
    );
    CREATE TABLE work_tag_sources (
      work_id INTEGER NOT NULL REFERENCES works(id) ON DELETE CASCADE,
      tag_id INTEGER NOT NULL REFERENCES tags(id) ON DELETE CASCADE,
      owner TEXT NOT NULL,
      seen_token TEXT,
      PRIMARY KEY(work_id, tag_id, owner)
    );
    CREATE TRIGGER fixture_work_insert AFTER INSERT ON works BEGIN
      INSERT INTO work_stats(work_id) VALUES (NEW.id);
    END;
  `);
  database.close();
}

test("default R1G profile is pinned to the current migrated schema", () => {
  assert.equal(R1G_40K_PROFILE.requiredSchemaVersion, CURRENT_SCHEMA_VERSION);
  assert.equal(R1G_40K_PROFILE.name, "r1g-40k-740k-800k-v6");
});

test("synthetic generator creates exact deterministic facts and restores triggers", () => {
  const directory = mkdtempSync(join(os.tmpdir(), "arislist-synthetic-test-"));
  const path = join(directory, "library.sqlite");
  createSchema(path);
  const profile = {
    name: "tiny-r1g",
    seed: 7,
    requiredSchemaVersion: 9,
    worksByKind: [
      { kind: "gallery", count: 2 },
      { kind: "novel", count: 1 },
    ],
    assetGroups: [
      {
        name: "images",
        kind: "gallery",
        count: 4,
        role: "image",
        mime: "image/jpeg",
        suffix: "jpg",
        size: 10,
        meta: {},
      },
      {
        name: "books",
        kind: "novel",
        count: 1,
        role: "book",
        mime: "application/epub+zip",
        suffix: "epub",
        size: 20,
        meta: {},
      },
    ],
    tagCount: 7,
    tagTiers: [
      { tags: 2, works: 1 },
      { tags: 3, works: 2 },
    ],
    tierPermutation: 1,
    hotTagOrdinal: 1,
    warmTagOrdinal: 2,
    warmTagModulo: 2,
  };
  const result = populateSyntheticDataset(path, profile);
  assert.equal(result.status, "generated-and-verified");
  assert.deepEqual(
    {
      works: result.actual.works,
      assets: result.actual.assets,
      workTags: result.actual.work_tags,
      bytes: result.actual.logical_asset_bytes,
    },
    { works: 3, assets: 5, workTags: 8, bytes: 60 },
  );
  assert.deepEqual(result.tag_tiers, [
    { tags: 2, works: 1 },
    { tags: 3, works: 2 },
  ]);
  assert.equal(result.hot_tag_links, 3);
  assert.equal(result.warm_tag_links, 1);

  const database = new DatabaseSync(path, { readOnly: true });
  assert.equal(
    database
      .prepare("SELECT COUNT(*) AS count FROM sqlite_schema WHERE type='trigger' AND name='fixture_work_insert'")
      .get().count,
    1,
  );
  assert.equal(database.prepare("SELECT COUNT(*) AS count FROM work_stats WHERE tag_count = 3").get().count, 2);
  database.close();
});

test("synthetic generator refuses a non-empty catalog", () => {
  const directory = mkdtempSync(join(os.tmpdir(), "arislist-synthetic-test-"));
  const path = join(directory, "library.sqlite");
  createSchema(path);
  const database = new DatabaseSync(path);
  database
    .prepare(
      "INSERT INTO works(kind, title, source_path, created_at, updated_at) VALUES ('gallery', 'existing', '/existing', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
    )
    .run();
  database.close();
  assert.throws(
    () =>
      populateSyntheticDataset(path, {
        name: "tiny",
        seed: 1,
        requiredSchemaVersion: 9,
        worksByKind: [{ kind: "gallery", count: 1 }],
        assetGroups: [
          {
            name: "images",
            kind: "gallery",
            count: 1,
            role: "image",
            mime: "image/jpeg",
            suffix: "jpg",
            size: 1,
            meta: {},
          },
        ],
        tagCount: 1,
        tagsPerWork: 1,
      }),
    /non-empty/u,
  );
});
