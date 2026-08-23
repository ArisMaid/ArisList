import { readFileSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";
import { parseArgs } from "node:util";

import {
  evaluateScenarioGates,
  readJsonLines,
  summarizeScenarioRecords,
} from "./perf-lib.mjs";

const { values } = parseArgs({
  options: {
    input: { type: "string", short: "i", multiple: true },
    output: { type: "string", short: "o" },
    gates: { type: "string", short: "g" },
    help: { type: "boolean", short: "h" },
  },
  strict: true,
});

if (values.help || !values.input || !values.output) {
  console.log("Usage: node scripts/perf/summarize-results.mjs --input <scenario-results.jsonl> [--input <more.jsonl>] --output <summary.json> [--gates <gates.json>]");
  process.exit(values.help ? 0 : 2);
}

const inputs = values.input.map((input) => resolve(input));
const output = resolve(values.output);
const summary = summarizeScenarioRecords(inputs.flatMap((input) => readJsonLines(input)));
if (values.gates) {
  const gatesPath = resolve(values.gates);
  const configuration = JSON.parse(readFileSync(gatesPath, "utf8"));
  summary.gate = evaluateScenarioGates(summary, configuration);
}
writeFileSync(output, `${JSON.stringify(summary, null, 2)}\n`, "utf8");
console.log(
  JSON.stringify({
    inputs,
    output,
    records: summary.record_count,
    groups: summary.groups.length,
    gate: summary.gate?.status ?? "not-evaluated",
  }),
);
if (summary.gate && summary.gate.status !== "passed") process.exitCode = 1;
