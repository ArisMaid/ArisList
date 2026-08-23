import { spawnSync } from "node:child_process";
import { existsSync, statSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { parseArgs } from "node:util";

const repoRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..", "..");
const { values } = parseArgs({
  options: {
    database: { type: "string", short: "d" },
    help: { type: "boolean", short: "h" },
  },
  strict: true,
});

if (values.help || !values.database) {
  console.log(`Usage: node scripts/perf/initialize-database.mjs --database <new.sqlite>

Runs the server's real append-only migrations through an explicitly ignored
Rust fixture test. An existing non-empty path is always rejected.`);
  process.exit(values.help ? 0 : 2);
}

const database = resolve(values.database);
if (existsSync(database) && (!statSync(database).isFile() || statSync(database).size !== 0)) {
  throw new Error(`refusing to initialize existing non-empty path: ${database}`);
}

const result = spawnSync(
  "cargo",
  [
    "test",
    "-p",
    "media-shelf-server",
    "db::tests::initialize_empty_perf_database_schema",
    "--",
    "--ignored",
    "--exact",
    "--nocapture",
    "--test-threads=1",
  ],
  {
    cwd: repoRoot,
    env: { ...process.env, PERF_DATABASE_PATH: database },
    encoding: "utf8",
    windowsHide: true,
    maxBuffer: 128 * 1024 * 1024,
  },
);
if (result.stdout) process.stdout.write(result.stdout);
if (result.stderr) process.stderr.write(result.stderr);
if (result.status !== 0) {
  throw new Error(`database initialization test exited with status ${result.status}`);
}
if (!existsSync(database) || statSync(database).size === 0) {
  throw new Error(`database initialization did not create a non-empty SQLite file: ${database}`);
}
console.log(JSON.stringify({ status: "initialized", database, database_bytes: statSync(database).size }));
