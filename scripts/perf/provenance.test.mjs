import test from "node:test";
import assert from "node:assert/strict";

import {
  captureFeatureFlags,
  SAFE_ENVIRONMENT_KEYS,
} from "./provenance.mjs";

test("performance provenance records only the shared non-secret environment allowlist", () => {
  const environment = {
    RESOURCE_PROFILE: "nas-n100-4g",
    CATALOG_V2_ENABLED: "true",
    SESSION_SECRET: "must-not-be-captured",
  };
  const flags = captureFeatureFlags(environment);
  assert.deepEqual(Object.keys(flags), [...SAFE_ENVIRONMENT_KEYS]);
  assert.equal(flags.RESOURCE_PROFILE, "nas-n100-4g");
  assert.equal(flags.CATALOG_V2_ENABLED, "true");
  assert.equal("SESSION_SECRET" in flags, false);
  assert.equal(flags.FACET_BITMAP_ENABLED, null);
});
