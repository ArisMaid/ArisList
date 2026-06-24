import { existsSync, readFileSync, readdirSync, statSync } from "node:fs";
import { extname, join, relative } from "node:path";
import { inflateRawSync } from "node:zlib";

const root = new URL("..", import.meta.url).pathname.replace(/^\/([A-Za-z]:)/, "$1");
const imageExts = new Set([".jpg", ".jpeg", ".png", ".webp", ".gif", ".avif"]);
const audioExts = new Set([".mp3", ".wav", ".flac", ".ogg", ".m4a"]);

let failed = false;

function fail(message) {
  failed = true;
  console.error(`FAIL ${message}`);
}

function pass(message) {
  console.log(`OK   ${message}`);
}

function walk(dir, result = []) {
  for (const entry of readdirSync(dir)) {
    const path = join(dir, entry);
    const stats = statSync(path);
    if (stats.isDirectory()) walk(path, result);
    else result.push(path);
  }
  return result;
}

function readUInt16(buffer, offset) {
  return buffer.readUInt16LE(offset);
}

function readUInt32(buffer, offset) {
  return buffer.readUInt32LE(offset);
}

function zipEntries(path) {
  const buffer = readFileSync(path);
  let eocd = -1;
  const min = Math.max(0, buffer.length - 0xffff - 22);
  for (let offset = buffer.length - 22; offset >= min; offset -= 1) {
    if (readUInt32(buffer, offset) === 0x06054b50) {
      eocd = offset;
      break;
    }
  }
  if (eocd < 0) throw new Error("ZIP EOCD not found");
  const count = readUInt16(buffer, eocd + 10);
  const centralOffset = readUInt32(buffer, eocd + 16);
  const entries = [];
  let cursor = centralOffset;
  for (let index = 0; index < count; index += 1) {
    if (readUInt32(buffer, cursor) !== 0x02014b50) throw new Error("invalid ZIP central directory");
    const method = readUInt16(buffer, cursor + 10);
    const compressedSize = readUInt32(buffer, cursor + 20);
    const uncompressedSize = readUInt32(buffer, cursor + 24);
    const nameLength = readUInt16(buffer, cursor + 28);
    const extraLength = readUInt16(buffer, cursor + 30);
    const commentLength = readUInt16(buffer, cursor + 32);
    const localHeaderOffset = readUInt32(buffer, cursor + 42);
    const name = buffer.subarray(cursor + 46, cursor + 46 + nameLength).toString("utf8");
    entries.push({ name, method, compressedSize, uncompressedSize, localHeaderOffset });
    cursor += 46 + nameLength + extraLength + commentLength;
  }
  return { buffer, entries };
}

function readZipEntry(zip, name) {
  const entry = zip.entries.find((item) => item.name === name);
  if (!entry) return null;
  const offset = entry.localHeaderOffset;
  if (readUInt32(zip.buffer, offset) !== 0x04034b50) throw new Error(`invalid ZIP local header for ${name}`);
  const nameLength = readUInt16(zip.buffer, offset + 26);
  const extraLength = readUInt16(zip.buffer, offset + 28);
  const dataStart = offset + 30 + nameLength + extraLength;
  const compressed = zip.buffer.subarray(dataStart, dataStart + entry.compressedSize);
  if (entry.method === 0) return compressed;
  if (entry.method === 8) return inflateRawSync(compressed);
  throw new Error(`unsupported ZIP compression method ${entry.method} in ${name}`);
}

function tagText(xml, tag) {
  const match = xml.match(new RegExp(`<${tag}[^>]*>([\\s\\S]*?)<\\/${tag}>`, "i"));
  return match?.[1]?.replace(/<[^>]+>/g, "").trim() ?? "";
}

function attrValue(tag, attr) {
  const match = tag.match(new RegExp(`${attr}\\s*=\\s*["']([^"']+)["']`, "i"));
  return match?.[1] ?? "";
}

function inspectComics() {
  const dir = join(root, "漫画");
  if (!existsSync(dir)) return fail("漫画 directory is missing");
  const cbzFiles = walk(dir).filter((file) => extname(file).toLowerCase() === ".cbz");
  if (cbzFiles.length === 0) return fail("no CBZ comic files found");

  let totalPages = 0;
  let taggedComics = 0;
  let linkedComics = 0;
  for (const file of cbzFiles) {
    const zip = zipEntries(file);
    const pages = zip.entries.filter((entry) => imageExts.has(extname(entry.name).toLowerCase()));
    if (pages.length === 0) fail(`CBZ has no image pages: ${relative(root, file)}`);
    totalPages += pages.length;

    const comicInfoPath = join(file, "..", "ComicInfo.xml");
    if (!existsSync(comicInfoPath)) {
      fail(`ComicInfo.xml missing for ${relative(root, file)}`);
      continue;
    }
    const xml = readFileSync(comicInfoPath, "utf8");
    const genre = tagText(xml, "Genre");
    const web = tagText(xml, "Web");
    if (/(^|,\s*)(f|m|x):/i.test(genre)) taggedComics += 1;
    if (/\/g\/\d+\/[0-9a-f]+/i.test(web)) linkedComics += 1;
  }

  taggedComics > 0 ? pass(`CBZ ComicInfo contains namespaced genre tags in ${taggedComics} comics`) : fail("no ComicInfo f:/m:/x: tags found");
  if (linkedComics > 0) pass(`ComicInfo Web links are present in ${linkedComics} comics and ignored by the local-only app`);
  pass(`validated ${cbzFiles.length} CBZ files with ${totalPages} readable image entries`);
}

function inspectNovels() {
  const dir = join(root, "轻小说");
  if (!existsSync(dir)) return fail("轻小说 directory is missing");
  const epubFiles = walk(dir).filter((file) => extname(file).toLowerCase() === ".epub");
  if (epubFiles.length === 0) return fail("no EPUB files found");

  let withSubjects = 0;
  let withSpine = 0;
  let withCoverCandidate = 0;
  for (const file of epubFiles) {
    const zip = zipEntries(file);
    const container = readZipEntry(zip, "META-INF/container.xml")?.toString("utf8") ?? "";
    const rootfileTag = container.match(/<rootfile\s+[^>]+>/i)?.[0] ?? "";
    const opfName = attrValue(rootfileTag, "full-path") || zip.entries.find((entry) => entry.name.endsWith(".opf"))?.name;
    if (!opfName) {
      fail(`EPUB OPF missing: ${relative(root, file)}`);
      continue;
    }
    const opf = readZipEntry(zip, opfName)?.toString("utf8") ?? "";
    if (!tagText(opf, "dc:title")) fail(`EPUB title missing: ${relative(root, file)}`);
    if ((opf.match(/<dc:subject\b/gi) ?? []).length > 0) withSubjects += 1;
    if ((opf.match(/<itemref\b/gi) ?? []).length > 0) withSpine += 1;
    if (/properties\s*=\s*["'][^"']*cover-image/i.test(opf) || /name\s*=\s*["']cover/i.test(opf)) withCoverCandidate += 1;
  }

  withSpine === epubFiles.length ? pass(`all ${epubFiles.length} EPUB files expose spine chapters`) : fail(`${epubFiles.length - withSpine} EPUB files have no spine`);
  withSubjects > 0 ? pass(`EPUB dc:subject tags found in ${withSubjects} files`) : fail("no EPUB dc:subject tags found");
  withCoverCandidate > 0 ? pass(`EPUB cover candidates found in ${withCoverCandidate} files`) : fail("no EPUB cover candidates found");
}

function inspectAudio() {
  const dir = join(root, "音声");
  if (!existsSync(dir)) return fail("音声 directory is missing");
  const files = walk(dir);
  const groups = new Map();
  for (const file of files) {
    const ext = extname(file).toLowerCase();
    if (!audioExts.has(ext) && !imageExts.has(ext) && ext !== ".txt") continue;
    const match = file.match(/RJ\d{6,9}/i);
    if (!match) continue;
    const key = match[0].toUpperCase();
    if (!groups.has(key)) groups.set(key, []);
    groups.get(key).push(file);
  }
  if (groups.size === 0) return fail("no RJ audio groups found");

  let groupsWithAudio = 0;
  let groupsWithMp3 = 0;
  let groupsWithWav = 0;
  let groupsWithCover = 0;
  for (const [rj, groupFiles] of groups) {
    const exts = new Set(groupFiles.map((file) => extname(file).toLowerCase()));
    const audioCount = groupFiles.filter((file) => audioExts.has(extname(file).toLowerCase())).length;
    if (audioCount === 0) fail(`${rj} has no playable audio tracks`);
    else groupsWithAudio += 1;
    if (exts.has(".mp3")) groupsWithMp3 += 1;
    if (exts.has(".wav")) groupsWithWav += 1;
    if (groupFiles.some((file) => imageExts.has(extname(file).toLowerCase()) && /cover|jacket|ジャケット/i.test(file))) groupsWithCover += 1;
  }

  pass(`validated ${groups.size} RJ groups; ${groupsWithAudio} contain playable tracks`);
  groupsWithMp3 > 0 ? pass(`MP3 playback variants found in ${groupsWithMp3} RJ groups`) : fail("no MP3 playback variants found");
  groupsWithWav > 0 ? pass(`WAV lossless variants found in ${groupsWithWav} RJ groups`) : fail("no WAV lossless variants found");
  groupsWithCover > 0 ? pass(`audio cover images found in ${groupsWithCover} RJ groups`) : fail("no audio cover images found");
}

inspectComics();
inspectNovels();
inspectAudio();

if (failed) process.exit(1);
