import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { existsSync, mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { DatabaseSync } from "node:sqlite";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";
import { tmpdir } from "node:os";
import test from "node:test";
import { CURRENT_SCHEMA_VERSION } from "./schema-version.mjs";

const execFileAsync = promisify(execFile);
const repoRoot = fileURLToPath(new URL("../..", import.meta.url));

function createLegacyDatabase(path) {
  const database = new DatabaseSync(path);
  database.exec(`
    CREATE TABLE schema_migrations (version INTEGER PRIMARY KEY);
    CREATE TABLE works (id INTEGER PRIMARY KEY, kind TEXT NOT NULL, title TEXT NOT NULL);
  `);
  database
    .prepare("INSERT INTO schema_migrations(version) VALUES (?)")
    .run(CURRENT_SCHEMA_VERSION);
  database.close();
}

function createFakeServer(path) {
  writeFileSync(
    path,
    `import { createServer } from "node:http";
const port = Number(process.env.APP_BIND.split(":").at(-1));
createServer((request, response) => {
  if (request.url === "/api/health") {
    response.writeHead(200, { "content-type": "application/json" });
    response.end(JSON.stringify({ status: "ok" }));
    return;
  }
  response.writeHead(404);
  response.end();
}).listen(port, "127.0.0.1");
`,
    "utf8",
  );
}

test("migration gate snapshots a quiesced database and records schema/integrity", async () => {
  const temp = mkdtempSync(join(tmpdir(), "migration-gate-test-"));
  const databasePath = join(temp, "library.sqlite");
  const fakeServer = join(temp, "fake-server.mjs");
  const output = join(temp, "artifact");
  createLegacyDatabase(databasePath);
  createFakeServer(fakeServer);

  await execFileAsync(
    process.execPath,
    [
      join(repoRoot, "scripts/perf/run-migration-gate.mjs"),
      "--database",
      databasePath,
      "--output",
      output,
      "--server-bin",
      process.execPath,
      "--server-arg",
      fakeServer,
    ],
    { cwd: temp, maxBuffer: 4 * 1024 * 1024, windowsHide: true },
  );

  const result = JSON.parse(readFileSync(join(output, "migration.json"), "utf8"));
  assert.equal(result.status, "passed");
  assert.equal(result.source_unchanged, true);
  assert.equal(result.source_database_sha256_before, result.source_database_sha256_after);
  assert.equal(result.migration.schema_version, CURRENT_SCHEMA_VERSION);
  assert.equal(result.migration.integrity_check, "ok");
  assert.equal(result.restore.status, "passed");
  assert.equal(result.restore.schema_version, CURRENT_SCHEMA_VERSION);
  assert.equal(result.restore.integrity_check, "ok");
  assert.equal(existsSync(join(output, "staging", "data", "library.sqlite")), true);
  assert.equal(existsSync(join(output, "server.stderr.log")), true);
});

test("migration gate refuses a source with a non-empty WAL sidecar", async () => {
  const temp = mkdtempSync(join(tmpdir(), "migration-gate-wal-"));
  const databasePath = join(temp, "library.sqlite");
  const output = join(temp, "artifact");
  createLegacyDatabase(databasePath);
  writeFileSync(`${databasePath}-wal`, "active", "utf8");

  await assert.rejects(
    execFileAsync(
      process.execPath,
      [
        join(repoRoot, "scripts/perf/run-migration-gate.mjs"),
        "--database",
        databasePath,
        "--output",
        output,
        "--server-bin",
        process.execPath,
      ],
      { cwd: temp, maxBuffer: 4 * 1024 * 1024, windowsHide: true },
    ),
    /non-empty WAL sidecar/u,
  );
});
