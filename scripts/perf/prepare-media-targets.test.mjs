import { DatabaseSync } from "node:sqlite";
import { execFile } from "node:child_process";
import { existsSync, mkdtempSync, readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { promisify } from "node:util";
import { fileURLToPath } from "node:url";
import test from "node:test";
import assert from "node:assert/strict";

const execFileAsync = promisify(execFile);
const repoRoot = fileURLToPath(new URL("../..", import.meta.url));
const script = join(repoRoot, "scripts/perf/prepare-media-targets.mjs");

function createFixture(includeCoser = true) {
  const directory = mkdtempSync(join(tmpdir(), "prepare-media-targets-"));
  const databasePath = join(directory, "library.sqlite");
  const database = new DatabaseSync(databasePath);
  database.exec(`
    CREATE TABLE works (
      id INTEGER PRIMARY KEY,
      kind TEXT NOT NULL,
      deleted_at TEXT
    );
    CREATE TABLE assets (
      id INTEGER PRIMARY KEY,
      work_id INTEGER NOT NULL,
      mime TEXT NOT NULL,
      role TEXT NOT NULL,
      size INTEGER,
      meta_json TEXT NOT NULL DEFAULT '{}'
    );
    CREATE TABLE tags (id INTEGER PRIMARY KEY, namespace TEXT NOT NULL, key TEXT NOT NULL);
    CREATE TABLE work_tags (work_id INTEGER NOT NULL, tag_id INTEGER NOT NULL);
  `);
  const insertWork = database.prepare("INSERT INTO works (id, kind, deleted_at) VALUES (?, ?, NULL)");
  const insertAsset = database.prepare(
    "INSERT INTO assets (id, work_id, mime, role, size, meta_json) VALUES (?, ?, ?, ?, ?, ?)",
  );
  insertWork.run(1, "gallery");
  insertAsset.run(11, 1, "image/jpeg", "image", 5_000_000, "{}");
  insertWork.run(2, "comic");
  insertAsset.run(21, 2, "application/zip", "archive", 1_000_000_000, '{"page_count":120}');
  if (includeCoser) {
    insertWork.run(3, "coser-picture");
    insertAsset.run(31, 3, "application/zip", "archive", 900_000_000, '{"page_count":80}');
  }
  insertWork.run(4, "audio");
  insertAsset.run(41, 4, "audio/mpeg", "track", 300_000, "{}");
  insertWork.run(5, "novel");
  insertAsset.run(51, 5, "application/epub+zip", "book", 2_000_000, "{}");
  database.exec("INSERT INTO tags (id, namespace, key) VALUES (1, 'genre', 'all'); INSERT INTO work_tags (work_id, tag_id) VALUES (1, 1), (2, 1);");
  database.close();
  return { directory, databasePath };
}

test("media target preparation selects all bounded scenarios without leaking source paths", async () => {
  const { directory, databasePath } = createFixture();
  const output = join(directory, "targets.json");
  await execFileAsync(process.execPath, [
    script,
    "--database",
    databasePath,
    "--output",
    output,
    "--base-url",
    "http://127.0.0.1:8787",
  ], { cwd: repoRoot, windowsHide: true });

  const matrix = JSON.parse(readFileSync(output, "utf8"));
  assert.equal(matrix.profile, "nas-n100-4g");
  assert.deepEqual(
    matrix.targets.map((entry) => entry.id),
    [
      "catalog-first-page",
      "tag-filter",
      "gallery-thumbnail-warm",
      "gallery-thumbnail-cold",
      "comic-page-cold",
      "coser-picture-page-cold",
      "audio-startup-range",
      "novel-summary-detail",
    ],
  );
  assert.equal(matrix.targets.find((entry) => entry.id === "audio-startup-range").require_partial, true);
  assert.equal(matrix.targets.find((entry) => entry.id === "tag-filter").url.includes("genre%3Aall"), true);
  assert.equal(
    matrix.targets.find((entry) => entry.id === "comic-page-cold").url.endsWith("/pages/0/stream?size=1280"),
    true,
  );
  assert.equal(
    matrix.targets.find((entry) => entry.id === "coser-picture-page-cold").url.endsWith("/pages/0/stream?size=1280"),
    true,
  );
  assert.equal(JSON.stringify(matrix).includes("source_path"), false);
});

test("media target preparation fails closed when a required kind is absent", async () => {
  const { directory, databasePath } = createFixture(false);
  const output = join(directory, "targets.json");
  await assert.rejects(
    execFileAsync(process.execPath, [script, "--database", databasePath, "--output", output], {
      cwd: repoRoot,
      windowsHide: true,
    }),
    /cannot prepare coser-picture-page/u,
  );
  assert.equal(existsSync(output), false);
});
