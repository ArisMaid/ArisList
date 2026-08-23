export const N100_MEMORY_LIMIT_BYTES = 4 * 1024 * 1024 * 1024;
export const N100_CPU_LIMIT_CORES = 4;
export const DOCKER_SIM_CPU_LIMIT_CORES = 1;
export const N100_PROFILE = "nas-n100-4g";

function check(id, status, details) {
  return { id, status, ...details };
}

/**
 * Evaluate the deployment facts required before a media latency artifact can
 * be called an N100/4GiB Gate.  Missing evidence is intentionally incomplete,
 * never silently converted to a pass.
 */
export function evaluateN100Environment(snapshot, options = {}) {
  const expectedProfile = options.expectedProfile ?? N100_PROFILE;
  const maxMemoryBytes = options.maxMemoryBytes ?? N100_MEMORY_LIMIT_BYTES;
  const checks = [];
  const platform = snapshot?.platform ?? null;
  checks.push(
    platform === "linux"
      ? check("linux", "passed", { value: platform })
      : check("linux", platform == null ? "incomplete" : "failed", {
          value: platform,
          failure: platform == null ? "platform was not captured" : "target Gate requires Linux",
        }),
  );

  const model = typeof snapshot?.cpu_model === "string" ? snapshot.cpu_model : null;
  if (!model) {
    checks.push(check("n100-model", "incomplete", { failure: "CPU model was not captured" }));
  } else if (/\bN100\b/iu.test(model)) {
    checks.push(check("n100-model", "passed", { value: model }));
  } else {
    checks.push(check("n100-model", "failed", { value: model, failure: "CPU model is not Intel N100" }));
  }

  const memory = snapshot?.cgroup_memory_max_bytes;
  if (!Number.isSafeInteger(memory) || memory <= 0) {
    checks.push(check("memory-limit", "incomplete", {
      value: memory ?? null,
      failure: "finite cgroup memory.max was not captured",
    }));
  } else if (memory <= maxMemoryBytes) {
    checks.push(check("memory-limit", "passed", {
      value: memory,
      maximum: maxMemoryBytes,
    }));
  } else {
    checks.push(check("memory-limit", "failed", {
      value: memory,
      maximum: maxMemoryBytes,
      failure: "cgroup memory limit exceeds the 4GiB budget",
    }));
  }

  const maxCpuCores = options.maxCpuCores;
  if (maxCpuCores != null) {
    const cpuLimit = snapshot?.cgroup_cpu_limit_cores;
    if (!Number.isFinite(cpuLimit) || cpuLimit <= 0) {
      checks.push(check("cpu-limit", "incomplete", {
        value: cpuLimit ?? null,
        maximum: maxCpuCores,
        failure: "finite cgroup CPU quota was not captured",
      }));
    } else if (cpuLimit <= maxCpuCores) {
      checks.push(check("cpu-limit", "passed", {
        value: cpuLimit,
        maximum: maxCpuCores,
      }));
    } else {
      checks.push(check("cpu-limit", "failed", {
        value: cpuLimit,
        maximum: maxCpuCores,
        failure: "cgroup CPU quota exceeds the requested simulation limit",
      }));
    }
  }

  const profile = snapshot?.resource_profile;
  if (profile == null) {
    checks.push(check("resource-profile", "incomplete", {
      failure: "health snapshot did not expose sqlite.config.profile",
    }));
  } else if (profile === expectedProfile) {
    checks.push(check("resource-profile", "passed", { value: profile }));
  } else {
    checks.push(check("resource-profile", "failed", {
      value: profile,
      expected: expectedProfile,
      failure: "server resource profile does not match the N100 budget profile",
    }));
  }

  const healthStatus = snapshot?.health_status;
  if (healthStatus === "ok") {
    checks.push(check("health", "passed", { value: healthStatus }));
  } else {
    checks.push(check("health", healthStatus == null ? "incomplete" : "failed", {
      value: healthStatus ?? null,
      failure: healthStatus == null ? "health snapshot was not captured" : "health endpoint was not ok",
    }));
  }

  if (options.blockDevice) {
    if (snapshot?.block_device === options.blockDevice) {
      checks.push(check("block-device", "passed", { value: options.blockDevice }));
    } else if (snapshot?.block_device == null) {
      checks.push(check("block-device", "incomplete", {
        expected: options.blockDevice,
        failure: "requested Linux block device was not found in diskstats",
      }));
    } else {
      checks.push(check("block-device", "failed", {
        value: snapshot.block_device,
        expected: options.blockDevice,
        failure: "captured block device does not match the requested NAS device",
      }));
    }
  }

  const failed = checks.filter((item) => item.status === "failed");
  const incomplete = checks.filter((item) => item.status === "incomplete");
  return {
    status: failed.length ? "failed" : incomplete.length ? "incomplete" : "passed",
    checks,
    failed: failed.map((item) => item.id),
    incomplete: incomplete.map((item) => item.id),
  };
}
