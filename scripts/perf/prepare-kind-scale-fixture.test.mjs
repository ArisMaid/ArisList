import { execFile } from "node:child_process";
import { existsSync, mkdirSync, mkdtempSync, readFileSync, readdirSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { promisify } from "node:util";
import { fileURLToPath } from "node:url";
import test from "node:test";
import assert from "node:assert/strict";

const execFileAsync = promisify(execFile);
const repoRoot = fileURLToPath(new URL("../..", import.meta.url));
const script = join(repoRoot, "scripts/perf/prepare-kind-scale-fixture.mjs");

function countFiles(root) {
  let count = 0;
  const stack = [root];
  while (stack.length > 0) {
    const current = stack.pop();
    for (const entry of readdirSync(current, { withFileTypes: true })) {
      const child = join(current, entry.name);
      if (entry.isDirectory()) stack.push(child);
      else if (entry.isFile()) count += 1;
    }
  }
  return count;
}

test("kind fixture keeps the manifest outside a gallery root and groups files per work", async () => {
  const directory = mkdtempSync(join(tmpdir(), "prepare-kind-scale-fixture-"));
  const source = join(directory, "source");
  const output = join(directory, "gallery");
  const manifestOutput = join(directory, "gallery-manifest.json");
  mkdirSync(source, { recursive: true });
  const sourceFile = join(source, "sample.png");
  writeFileSync(sourceFile, Buffer.from("fixture"));

  await execFileAsync(process.execPath, [
    script,
    "--kind", "gallery",
    "--source", source,
    "--output", output,
    "--count", "12",
    "--authors", "3",
    "--files-per-work", "4",
    "--source-file", sourceFile,
    "--manifest-output", manifestOutput,
  ], { cwd: repoRoot, windowsHide: true });

  const manifest = JSON.parse(readFileSync(manifestOutput, "utf8"));
  assert.equal(manifest.fixture_kind, "gallery");
  assert.equal(manifest.file_count, 12);
  assert.equal(manifest.author_directory_count, 3);
  assert.equal(manifest.files_per_work, 4);
  assert.equal(countFiles(output), 12);
  assert.equal(readdirSync(output, { withFileTypes: true }).filter((entry) => entry.isDirectory()).length, 3);
  assert.equal(existsSync(join(output, "fixture-manifest.json")), false);
});

test("audio fixture groups tracks without changing the requested file count", async () => {
  const directory = mkdtempSync(join(tmpdir(), "prepare-kind-scale-fixture-"));
  const source = join(directory, "source");
  const output = join(directory, "audio");
  mkdirSync(source, { recursive: true });
  const sourceFile = join(source, "sample.mp3");
  writeFileSync(sourceFile, Buffer.from("fixture"));

  await execFileAsync(process.execPath, [
    script,
    "--kind", "audio",
    "--source", source,
    "--output", output,
    "--count", "6",
    "--authors", "2",
    "--files-per-work", "3",
    "--source-file", sourceFile,
  ], { cwd: repoRoot, windowsHide: true });

  assert.equal(countFiles(output), 6);
  assert.equal(readdirSync(join(output, "author-000", "work-0000")).length, 3);
  assert.equal(readdirSync(join(output, "author-001", "work-0000")).length, 3);
});
