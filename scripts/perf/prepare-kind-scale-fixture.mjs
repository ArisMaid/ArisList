#!/usr/bin/env node

import fs from "node:fs";
import path from "node:path";
import process from "node:process";
import { parseArgs } from "node:util";

const AUDIO_EXTENSIONS = new Set([".aac", ".flac", ".m4a", ".mp3", ".ogg", ".opus", ".wav", ".wma"]);
const ARCHIVE_EXTENSIONS = new Set([".cbz", ".zip"]);
const IMAGE_EXTENSIONS = new Set([".jpg", ".jpeg", ".png", ".webp", ".gif", ".avif", ".bmp"]);

function usage() {
  console.log(`Usage: node scripts/perf/prepare-kind-scale-fixture.mjs \
  --kind coser-picture|audio|gallery --source <directory> --output <directory> \
  --count <n> --authors <n> [--files-per-work <n>] [--source-file <path>] \
  [--manifest-output <file>]`);
}

function fail(message) {
  console.error(`fixture error: ${message}`);
  process.exitCode = 1;
}

function parseCli() {
  const { values } = parseArgs({
    options: {
      kind: { type: "string" },
      source: { type: "string" },
      output: { type: "string" },
      count: { type: "string" },
      authors: { type: "string" },
      "files-per-work": { type: "string", default: "1" },
      "source-file": { type: "string" },
      "manifest-output": { type: "string" },
      help: { type: "boolean", short: "h" },
    },
    strict: true,
  });
  if (values.help) {
    usage();
    process.exit(0);
  }
  const kind = values.kind?.trim();
  const source = values.source && path.resolve(values.source);
  const output = values.output && path.resolve(values.output);
  const count = Number.parseInt(values.count ?? "", 10);
  const authors = Number.parseInt(values.authors ?? "", 10);
  if (!kind || !["coser-picture", "audio", "gallery"].includes(kind)) {
    throw new Error("--kind must be coser-picture, audio, or gallery");
  }
  if (!source || !fs.statSync(source, { throwIfNoEntry: false })?.isDirectory()) {
    throw new Error("--source must be an existing directory");
  }
  if (!output) throw new Error("--output is required");
  if (!Number.isSafeInteger(count) || count < 1) throw new Error("--count must be a positive integer");
  if (!Number.isSafeInteger(authors) || authors < 1 || authors > count) {
    throw new Error("--authors must be between 1 and --count");
  }
  const filesPerWork = Number.parseInt(values["files-per-work"] ?? "", 10);
  if (!Number.isSafeInteger(filesPerWork) || filesPerWork < 1) {
    throw new Error("--files-per-work must be a positive integer");
  }
  const sourceFile = values["source-file"] ? path.resolve(values["source-file"]) : null;
  const manifestOutput = values["manifest-output"]
    ? path.resolve(values["manifest-output"])
    : path.join(path.dirname(output), `${path.basename(output)}-fixture-manifest.json`);
  const relativeManifest = path.relative(output, manifestOutput);
  if (relativeManifest === "" || (!relativeManifest.startsWith("..") && !path.isAbsolute(relativeManifest))) {
    throw new Error("--manifest-output must be outside --output so it is not scanned as media");
  }
  return { kind, source, output, count, authors, filesPerWork, sourceFile, manifestOutput };
}

function walkFiles(root) {
  const result = [];
  const stack = [root];
  while (stack.length > 0) {
    const current = stack.pop();
    for (const entry of fs.readdirSync(current, { withFileTypes: true })) {
      const child = path.join(current, entry.name);
      if (entry.isDirectory()) stack.push(child);
      else if (entry.isFile()) result.push(child);
    }
  }
  return result.sort((left, right) => left.localeCompare(right));
}

function extensionAllowed(kind, filePath) {
  const extension = path.extname(filePath).toLowerCase();
  if (kind === "audio") return AUDIO_EXTENSIONS.has(extension);
  if (kind === "gallery") return IMAGE_EXTENSIONS.has(extension);
  return ARCHIVE_EXTENSIONS.has(extension);
}

function ensureNewOutput(output) {
  if (fs.existsSync(output)) {
    const entries = fs.readdirSync(output);
    if (entries.length > 0) throw new Error(`refusing to populate non-empty output: ${output}`);
  } else {
    fs.mkdirSync(output, { recursive: true });
  }
}

function authorPlacement(index, count, authors) {
  const ordinal = Math.min(authors - 1, Math.floor((index * authors) / count));
  const start = Math.floor((ordinal * count) / authors);
  return { ordinal, localIndex: index - start };
}

function destinationFor(kind, output, index, count, authors, filesPerWork, sourcePath) {
  const { ordinal, localIndex } = authorPlacement(index, count, authors);
  const author = `author-${String(ordinal).padStart(3, "0")}`;
  const serial = String(index).padStart(5, "0");
  const workOrdinal = Math.floor(localIndex / filesPerWork);
  const workSerial = String(workOrdinal).padStart(4, "0");
  const trackSerial = String(localIndex % filesPerWork + 1).padStart(4, "0");
  if (kind === "audio") {
    return path.join(output, author, `work-${workSerial}`, `track-${trackSerial}${path.extname(sourcePath).toLowerCase()}`);
  }
  if (kind === "gallery") {
    return path.join(output, author, `set-${workSerial}`, `image-${serial}${path.extname(sourcePath).toLowerCase()}`);
  }
  return path.join(output, author, `set-${serial}${path.extname(sourcePath).toLowerCase()}`);
}

function main() {
  const cli = parseCli();
  ensureNewOutput(cli.output);
  if (fs.existsSync(cli.manifestOutput)) {
    throw new Error(`refusing to overwrite existing manifest: ${cli.manifestOutput}`);
  }
  fs.mkdirSync(path.dirname(cli.manifestOutput), { recursive: true });
  const allSources = cli.sourceFile
    ? [cli.sourceFile]
    : walkFiles(cli.source).filter((filePath) => extensionAllowed(cli.kind, filePath));
  if (allSources.length === 0) throw new Error(`no supported ${cli.kind} source files found`);
  for (const sourcePath of allSources) {
    if (!fs.statSync(sourcePath, { throwIfNoEntry: false })?.isFile()) {
      throw new Error(`source file is missing: ${sourcePath}`);
    }
    if (!extensionAllowed(cli.kind, sourcePath)) {
      throw new Error(`source extension is unsupported for ${cli.kind}: ${sourcePath}`);
    }
  }

  let logicalBytes = 0;
  const sourceStats = new Map();
  for (let index = 0; index < cli.count; index += 1) {
    const sourcePath = allSources[index % allSources.length];
    const stat = fs.statSync(sourcePath);
    const destination = destinationFor(
      cli.kind,
      cli.output,
      index,
      cli.count,
      cli.authors,
      cli.filesPerWork,
      sourcePath,
    );
    fs.mkdirSync(path.dirname(destination), { recursive: true });
    fs.linkSync(sourcePath, destination);
    logicalBytes += stat.size;
    sourceStats.set(sourcePath, stat.size);
  }

  const manifest = {
    schema_version: 1,
    fixture_kind: cli.kind,
    file_count: cli.count,
    author_directory_count: cli.authors,
    files_per_work: cli.filesPerWork,
    source_file_count: sourceStats.size,
    logical_bytes: logicalBytes,
    physical_source_bytes: [...sourceStats.values()].reduce((sum, value) => sum + value, 0),
    hardlink_reuse: true,
    source_root: cli.source,
    source_files: [...sourceStats.entries()].map(([file, bytes]) => ({ file, bytes })),
  };
  fs.writeFileSync(cli.manifestOutput, `${JSON.stringify(manifest, null, 2)}\n`, { encoding: "utf8", flag: "wx" });
  console.log(JSON.stringify({ output: cli.output, ...manifest }));
}

try {
  main();
} catch (error) {
  fail(error instanceof Error ? error.message : String(error));
}
