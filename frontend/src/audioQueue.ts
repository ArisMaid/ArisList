import type { Asset } from "./api";

export const AUDIO_TRACK_PAGE_SIZE = 128;
export const AUDIO_TRACK_MAX_CACHED = AUDIO_TRACK_PAGE_SIZE * 5;
export const AUDIO_QUEUE_WINDOW = 24;

export type AudioPlaylistState = {
  items: Asset[];
  nextCursor: string | null;
  hasMore: boolean;
  startIndex: number;
  nextPageStartIndex: number;
  pageCursors: Array<{ startIndex: number; cursor: string | null }>;
  total: number;
};

export type MergeAudioTrackPageInput = {
  current: AudioPlaylistState;
  fetched: Asset[];
  pageCursor: string | null;
  responseNextCursor: string | null;
  responseTotal: number;
  activeAsset?: Asset | null;
  currentAssetId?: number | null;
  replace?: boolean;
  direction?: "append" | "prepend";
  requestedStartIndex?: number;
  maxCached?: number;
};

/**
 * Merge one keyset page into the bounded audio queue.
 *
 * The cursor map is deliberately retained outside the visible window.  When
 * a page is prepended and the tail is evicted, the next cursor must be the
 * cursor for the new absolute tail, rather than the cursor returned by the
 * page that was just fetched.  Keeping this transition pure makes the
 * eviction/reload invariant testable without mounting React or an <audio>
 * element.
 */
export function mergeAudioTrackPage({
  current,
  fetched,
  pageCursor,
  responseNextCursor,
  responseTotal,
  activeAsset = null,
  currentAssetId = null,
  replace = false,
  direction = "append",
  requestedStartIndex,
  maxCached = AUDIO_TRACK_MAX_CACHED
}: MergeAudioTrackPageInput): AudioPlaylistState {
  const pageStartIndex = requestedStartIndex ?? (replace ? 0 : current.nextPageStartIndex);
  const knownIds = new Set(current.items.map((asset) => asset.id));
  const unseen = fetched.filter((asset) => !knownIds.has(asset.id));
  let items = replace
    ? [...fetched]
    : direction === "prepend"
      ? [...unseen, ...current.items]
      : [...current.items, ...unseen];
  let startIndex = replace
    ? 0
    : direction === "prepend"
      ? Math.min(current.startIndex, pageStartIndex)
      : current.startIndex;

  if (activeAsset && !items.some((asset) => asset.id === activeAsset.id)) {
    items = [activeAsset, ...items];
    startIndex = Math.max(0, startIndex - 1);
  }

  // Keep the currently playing item in the bounded window. Appending evicts
  // from the front; reloading an older page evicts from the tail.
  if (items.length > maxCached) {
    const excess = items.length - maxCached;
    const playingIndex = Math.max(0, items.findIndex((asset) => asset.id === currentAssetId));
    if (direction === "prepend" && !replace) {
      const removable = Math.min(excess, Math.max(0, items.length - playingIndex - 1));
      if (removable > 0) items = items.slice(0, items.length - removable);
    } else {
      const removable = Math.min(excess, playingIndex);
      if (removable > 0) {
        items = items.slice(removable);
        startIndex += removable;
      }
    }
  }

  const cursorByStart = new Map<number, string | null>();
  for (const page of replace ? [] : current.pageCursors) {
    cursorByStart.set(page.startIndex, page.cursor);
  }
  cursorByStart.set(pageStartIndex, pageCursor);
  // This cursor begins immediately after the fetched page.  Recording the
  // end marker lets a later prepend/reload recover the exact page at the new
  // cached tail after eviction.
  cursorByStart.set(pageStartIndex + fetched.length, responseNextCursor);
  const pageCursors = Array.from(cursorByStart, ([cursorStartIndex, cursor]) => ({
    startIndex: cursorStartIndex,
    cursor
  })).sort((left, right) => left.startIndex - right.startIndex);

  const cachedEndIndex = startIndex + items.length;
  const nextCursor = cursorByStart.has(cachedEndIndex)
    ? cursorByStart.get(cachedEndIndex) ?? null
    : direction === "prepend" && !replace
      ? current.nextCursor
      : responseNextCursor;

  return {
    items,
    nextCursor,
    hasMore: nextCursor !== null,
    startIndex,
    nextPageStartIndex: cachedEndIndex,
    pageCursors,
    total: Math.max(responseTotal, current.total, cachedEndIndex)
  };
}
