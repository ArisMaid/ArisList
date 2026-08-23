import {
  appendFileSync,
  existsSync,
  mkdirSync,
  writeFileSync,
} from "node:fs";
import { dirname, resolve } from "node:path";

import { PERF_ARTIFACT_VERSION } from "./perf-lib.mjs";

/**
 * Prepare output paths before a runner performs network or health work.
 * Keeping this in one place makes a missing parent directory an invocation
 * error instead of a late, un-auditable append failure.
 */
export function prepareRunnerOutputPaths(...paths) {
  const resolved = paths.filter((path) => path != null).map((path) => resolve(path));
  for (const path of resolved) mkdirSync(dirname(path), { recursive: true });
  return resolved;
}

export function appendJsonLine(path, value) {
  mkdirSync(dirname(path), { recursive: true });
  appendFileSync(path, `${JSON.stringify(value)}\n`, "utf8");
}

export function writeJsonArtifact(path, value) {
  mkdirSync(dirname(path), { recursive: true });
  writeFileSync(path, `${JSON.stringify(value, null, 2)}\n`, {
    encoding: "utf8",
    flag: "wx",
  });
}

function redactUrlLikeText(text) {
  return text.replace(/https?:\/\/[^\s"'<>]+/giu, (raw) => {
    try {
      const url = new URL(raw);
      return `${url.origin}${url.pathname}`;
    } catch {
      return raw.replace(/[?#].*$/u, "");
    }
  });
}

/**
 * Error text is diagnostic evidence, not a place to leak query parameters or
 * credentials.  Keep it short and deterministic so failure artifacts remain
 * safe to archive with the rest of a Gate run.
 */
export function redactedError(error) {
  let message = error instanceof Error ? error.message : String(error);
  message = redactUrlLikeText(message);
  message = message.replace(
    /((?:authorization|cookie|token|password|secret|api[_-]?key)\s*[=:]\s*)[^\s,;]+/giu,
    "$1[REDACTED]",
  );
  return message.slice(0, 512);
}

export function writeRunnerFailureArtifact({
  runtimeOutput,
  scenario,
  startedAt,
  metadata = {},
  error,
}) {
  const message = redactedError(error);
  const artifact = {
    artifact_version: PERF_ARTIFACT_VERSION,
    status: "failed",
    started_at: startedAt,
    completed_at: new Date().toISOString(),
    scenario,
    ...metadata,
    error: message,
    checks: [
      {
        id: "runner-execution",
        status: "failed",
        failures: [message],
      },
    ],
  };
  if (!existsSync(runtimeOutput)) writeJsonArtifact(runtimeOutput, artifact);
  return artifact;
}
