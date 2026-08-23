import assert from "node:assert/strict";
import test from "node:test";

import {
  AUDIO_TRACK_MAX_CACHED,
  AUDIO_TRACK_PAGE_SIZE,
  mergeAudioTrackPage,
} from "../../frontend/src/audioQueue.ts";

const asset = (id) => ({
  id,
  work_id: 1,
  path: `track-${id}.mp3`,
  mime: "audio/mpeg",
  role: "track",
  variant: null,
  position: id,
  size: 1,
  meta_json: "{}",
  created_at: "2026-01-01T00:00:00Z",
});

const page = (start, length = AUDIO_TRACK_PAGE_SIZE) =>
  Array.from({ length }, (_, offset) => asset(start + offset));

const initialState = (total = 2048) => ({
  items: page(0),
  nextCursor: null,
  hasMore: true,
  startIndex: 0,
  nextPageStartIndex: AUDIO_TRACK_PAGE_SIZE,
  pageCursors: [{ startIndex: 0, cursor: null }],
  total,
});

function merge(state, start, { direction = "append", replace = false, currentAssetId } = {}) {
  return mergeAudioTrackPage({
    current: state,
    fetched: page(start),
    pageCursor: start === 0 ? null : `c${start}`,
    responseNextCursor: `c${start + AUDIO_TRACK_PAGE_SIZE}`,
    responseTotal: state.total,
    currentAssetId,
    direction,
    replace,
    requestedStartIndex: direction === "prepend" ? start : undefined,
  });
}

test("audio queue reloads an evicted tail page instead of skipping it", () => {
  let state = merge(initialState(), 0, { replace: true, currentAssetId: 127 });
  for (let start = 128; start <= 640; start += AUDIO_TRACK_PAGE_SIZE) {
    state = merge(state, start, { currentAssetId: state.items.at(-1).id });
  }

  assert.equal(state.startIndex, 128);
  assert.equal(state.nextPageStartIndex, 768);
  assert.equal(state.nextCursor, "c768");
  assert.equal(state.items.length, AUDIO_TRACK_MAX_CACHED);

  // Move back across the five-page window.  The prepend evicts the tail,
  // so the next cursor must point at 640, not at the old 768 boundary.
  state = merge(state, 0, { direction: "prepend", currentAssetId: 128 });
  assert.equal(state.startIndex, 0);
  assert.equal(state.nextPageStartIndex, 640);
  assert.equal(state.nextCursor, "c640");
  assert.deepEqual(state.items.map((item) => item.id), page(0).map((item) => item.id).concat(page(128, 512).map((item) => item.id)));

  // Advancing again must load page 640, then continue at page 768 without a
  // duplicate or a gap.
  state = merge(state, 640, { currentAssetId: 639 });
  assert.equal(state.startIndex, 128);
  assert.equal(state.nextPageStartIndex, 768);
  assert.equal(state.nextCursor, "c768");
  assert.deepEqual(state.items.map((item) => item.id), Array.from({ length: 640 }, (_, index) => index + 128));
  state = merge(state, 768, { currentAssetId: 767 });
  assert.equal(state.nextCursor, "c896");
  assert.deepEqual(state.items.map((item) => item.id), Array.from({ length: 640 }, (_, index) => index + 256));
});

test("audio queue keeps five pages while repeatedly crossing backwards", () => {
  let state = merge(initialState(), 0, { replace: true, currentAssetId: 127 });
  for (let start = 128; start <= 1152; start += AUDIO_TRACK_PAGE_SIZE) {
    state = merge(state, start, { currentAssetId: state.items.at(-1).id });
  }

  assert.equal(state.items.length, AUDIO_TRACK_MAX_CACHED);
  assert.equal(state.startIndex, 640);
  assert.equal(state.nextPageStartIndex, 1280);
  assert.equal(state.nextCursor, "c1280");

  for (let start = 512; start >= 0; start -= AUDIO_TRACK_PAGE_SIZE) {
    const currentFirst = state.items[0].id;
    state = merge(state, start, { direction: "prepend", currentAssetId: currentFirst });
    assert.ok(state.items.length <= AUDIO_TRACK_MAX_CACHED);
    assert.equal(state.startIndex, start);
    assert.equal(state.nextPageStartIndex, start + AUDIO_TRACK_MAX_CACHED);
    assert.equal(state.nextCursor, `c${start + AUDIO_TRACK_MAX_CACHED}`);
  }
  assert.deepEqual(state.items.map((item) => item.id), Array.from({ length: 640 }, (_, index) => index));
});
