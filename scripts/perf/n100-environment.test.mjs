import assert from "node:assert/strict";
import test from "node:test";

import { evaluateN100Environment, N100_MEMORY_LIMIT_BYTES, N100_PROFILE } from "./n100-environment-lib.mjs";

const base = {
  platform: "linux",
  cpu_model: "Intel(R) N100",
  cgroup_memory_max_bytes: N100_MEMORY_LIMIT_BYTES,
  cgroup_cpu_limit_cores: 4,
  resource_profile: N100_PROFILE,
  health_status: "ok",
  block_device: "sda",
};

test("N100 environment evaluator passes only with complete target evidence", () => {
  const result = evaluateN100Environment(base, { blockDevice: "sda" });
  assert.equal(result.status, "passed");
  assert.deepEqual(result.failed, []);
  assert.deepEqual(result.incomplete, []);
});

test("N100 environment evaluator stays incomplete when cgroup or health evidence is missing", () => {
  const result = evaluateN100Environment(
    { ...base, cgroup_memory_max_bytes: null, resource_profile: null, health_status: null },
    { blockDevice: "sda" },
  );
  assert.equal(result.status, "incomplete");
  assert.ok(result.incomplete.includes("memory-limit"));
  assert.ok(result.incomplete.includes("resource-profile"));
  assert.ok(result.incomplete.includes("health"));
});

test("N100 environment evaluator rejects a non-N100 CPU or oversized cgroup", () => {
  const result = evaluateN100Environment(
    { ...base, cpu_model: "Intel(R) Core(TM) i7", cgroup_memory_max_bytes: N100_MEMORY_LIMIT_BYTES + 1 },
    { blockDevice: "sda" },
  );
  assert.equal(result.status, "failed");
  assert.ok(result.failed.includes("n100-model"));
  assert.ok(result.failed.includes("memory-limit"));
});

test("N100 environment evaluator can enforce an optional Docker CPU quota", () => {
  const passed = evaluateN100Environment(base, { maxCpuCores: 4 });
  assert.equal(passed.status, "passed");
  assert.ok(passed.checks.some((item) => item.id === "cpu-limit" && item.status === "passed"));

  const failed = evaluateN100Environment(
    { ...base, cgroup_cpu_limit_cores: 4.5 },
    { maxCpuCores: 4 },
  );
  assert.equal(failed.status, "failed");
  assert.ok(failed.failed.includes("cpu-limit"));

  const incomplete = evaluateN100Environment(
    { ...base, cgroup_cpu_limit_cores: null },
    { maxCpuCores: 4 },
  );
  assert.equal(incomplete.status, "incomplete");
  assert.ok(incomplete.incomplete.includes("cpu-limit"));
});
