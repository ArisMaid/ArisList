import { existsSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";
import { parseArgs } from "node:util";

import { populateSyntheticDataset, R1G_40K_PROFILE } from "./synthetic-dataset-lib.mjs";
import { CURRENT_SCHEMA_VERSION } from "./schema-version.mjs";

const { values } = parseArgs({
  options: {
    database: { type: "string", short: "d" },
    output: { type: "string", short: "o" },
    help: { type: "boolean", short: "h" },
  },
  strict: true,
});

if (values.help || !values.database || !values.output) {
  console.log(`Usage: node scripts/perf/generate-synthetic-dataset.mjs --database <empty-v${CURRENT_SCHEMA_VERSION}.sqlite> --output <manifest.json>

Populates an empty database already initialized by the current server migrations
with the deterministic ${R1G_40K_PROFILE.name} profile. Existing catalog data and
existing output artifacts are never overwritten.`);
  process.exit(values.help ? 0 : 2);
}

const database = resolve(values.database);
const output = resolve(values.output);
if (!existsSync(database)) {
  throw new Error(
    `database does not exist: ${database}; initialize it with the current server migrations first`,
  );
}
if (existsSync(output)) throw new Error(`output artifact already exists: ${output}`);

const result = populateSyntheticDataset(database);
writeFileSync(output, `${JSON.stringify(result, null, 2)}\n`, { encoding: "utf8", flag: "wx" });
console.log(
  JSON.stringify({
    status: result.status,
    profile: result.profile,
    elapsed_ms: result.elapsed_ms,
    database_bytes: result.database_bytes,
    output,
  }),
);
