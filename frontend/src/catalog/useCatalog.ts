import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import type { HistoryRecord, Tag, WorkSummary } from "../api";
import {
  loadCatalogCounts,
  loadCatalogFacets,
  loadCatalogJobs,
  loadCatalogPage,
  type CatalogShelfQuery
} from "./api";

const MAX_RESIDENT_PAGES = 5;
// Facets are loaded only when the user asks for another page.  Keep a
// generous but finite resident bound so an unusually large tag table cannot
// turn the sidebar into an unbounded browser-side catalog.
const MAX_RESIDENT_FACET_TAGS = 4096;
const COLLECTION_BACKFILL_RETRY_MS = 750;

type CachedPage = {
  items: WorkSummary[];
  history: HistoryRecord[];
  cursor: string | null;
  nextCursor: string | null;
  revision: number;
  lastUsed: number;
};

export type CatalogShelfState = {
  items: WorkSummary[];
  history: HistoryRecord[];
  pageIndex: number;
  canPrevious: boolean;
  canNext: boolean;
  loading: boolean;
  backfillPending: boolean;
  error: string | null;
  tookMs?: number;
  next: () => void;
  previous: () => void;
  refresh: () => void;
};

function stableQueryKey(query: CatalogShelfQuery) {
  return JSON.stringify({
    ...query,
    includeTags: [...query.includeTags].sort(),
    query: query.query.trim()
  });
}

export function useCatalogShelf(enabled: boolean, query: CatalogShelfQuery): CatalogShelfState {
  const queryKey = useMemo(() => stableQueryKey(query), [query]);
  const pagesRef = useRef(new Map<number, CachedPage>());
  const cursorsRef = useRef(new Map<number, string | null>([[0, null]]));
  const abortRef = useRef<AbortController | null>(null);
  const prefetchControllersRef = useRef(new Map<number, AbortController>());
  const retryRef = useRef<number | null>(null);
  const generationRef = useRef(0);
  const accessRef = useRef(0);
  const [state, setState] = useState({
    items: [] as WorkSummary[],
    history: [] as HistoryRecord[],
    pageIndex: 0,
    canPrevious: false,
    canNext: false,
    loading: false,
    backfillPending: false,
    error: null as string | null,
    tookMs: undefined as number | undefined
  });

  const clearRetry = useCallback(() => {
    if (retryRef.current !== null) {
      window.clearTimeout(retryRef.current);
      retryRef.current = null;
    }
  }, []);

  const enforcePageLimit = useCallback((protectedIndex: number) => {
    while (pagesRef.current.size > MAX_RESIDENT_PAGES) {
      const victim = [...pagesRef.current.entries()]
        .filter(([index]) => index !== protectedIndex)
        .sort((left, right) => left[1].lastUsed - right[1].lastUsed)[0];
      if (!victim) break;
      pagesRef.current.delete(victim[0]);
    }
  }, []);

  const clearPrefetch = useCallback(() => {
    for (const controller of prefetchControllersRef.current.values()) controller.abort();
    prefetchControllersRef.current.clear();
  }, []);

  const prefetchPage = useCallback(
    async (pageIndex: number, cursor: string | null, generation: number, protectedIndex: number) => {
      if (!enabled || generation !== generationRef.current || !cursor) return;
      if (pagesRef.current.has(pageIndex) || prefetchControllersRef.current.has(pageIndex)) return;
      const controller = new AbortController();
      prefetchControllersRef.current.set(pageIndex, controller);
      try {
        const page = await loadCatalogPage(query, cursor, controller.signal);
        if (
          controller.signal.aborted ||
          generation !== generationRef.current ||
          page.backfillPending
        ) return;
        pagesRef.current.set(pageIndex, {
          items: page.items,
          history: page.history,
          cursor,
          nextCursor: page.nextCursor,
          revision: page.revision,
          lastUsed: ++accessRef.current
        });
        if (page.nextCursor) cursorsRef.current.set(pageIndex + 1, page.nextCursor);
        else cursorsRef.current.delete(pageIndex + 1);
        // Keep the visible page protected while the background page enters
        // the same bounded LRU as foreground navigation.
        enforcePageLimit(protectedIndex);
      } catch {
        // Prefetch is opportunistic. A foreground navigation can retry the
        // same cursor and surface a real error if the request is needed.
      } finally {
        if (prefetchControllersRef.current.get(pageIndex) === controller) {
          prefetchControllersRef.current.delete(pageIndex);
        }
      }
    },
    [enabled, enforcePageLimit, query]
  );

  const loadPage = useCallback(
    async (pageIndex: number, cursor: string | null, generation: number, force = false) => {
      if (!enabled || generation !== generationRef.current) return;
      clearRetry();
      const cached = !force ? pagesRef.current.get(pageIndex) : undefined;
      if (cached && cached.cursor === cursor) {
        cached.lastUsed = ++accessRef.current;
        setState({
          items: cached.items,
          history: cached.history,
          pageIndex,
          canPrevious: pageIndex > 0,
          canNext: Boolean(cached.nextCursor),
          loading: false,
          backfillPending: false,
          error: null,
          tookMs: 0
        });
        void prefetchPage(pageIndex + 1, cached.nextCursor, generation, pageIndex);
        return;
      }
      // A user click can overtake the opportunistic request. Cancel that
      // request before starting the foreground load so a slow page does not
      // consume two HTTP/database slots for the same cursor.
      prefetchControllersRef.current.get(pageIndex)?.abort();
      prefetchControllersRef.current.delete(pageIndex);
      abortRef.current?.abort();
      const controller = new AbortController();
      abortRef.current = controller;
      setState((current) => ({ ...current, loading: true, error: null, pageIndex }));
      const started = performance.now();
      try {
        const page = await loadCatalogPage(query, cursor, controller.signal);
        if (controller.signal.aborted || generation !== generationRef.current) return;
        const tookMs = Math.max(0, Math.round(performance.now() - started));
        if (page.backfillPending) {
          setState({
            items: [],
            history: [],
            pageIndex: 0,
            canPrevious: false,
            canNext: false,
            loading: false,
            backfillPending: true,
            error: null,
            tookMs
          });
          retryRef.current = window.setTimeout(() => {
            retryRef.current = null;
            void loadPage(0, null, generation, true);
          }, COLLECTION_BACKFILL_RETRY_MS);
          return;
        }
        const cachedPage: CachedPage = {
          items: page.items,
          history: page.history,
          cursor,
          nextCursor: page.nextCursor,
          revision: page.revision,
          lastUsed: ++accessRef.current
        };
        pagesRef.current.set(pageIndex, cachedPage);
        if (page.nextCursor) cursorsRef.current.set(pageIndex + 1, page.nextCursor);
        else cursorsRef.current.delete(pageIndex + 1);
        enforcePageLimit(pageIndex);
        setState({
          items: page.items,
          history: page.history,
          pageIndex,
          canPrevious: pageIndex > 0,
          canNext: Boolean(page.nextCursor),
          loading: false,
          backfillPending: false,
          error: null,
          tookMs
        });
        void prefetchPage(pageIndex + 1, page.nextCursor, generation, pageIndex);
      } catch (error) {
        if (controller.signal.aborted || generation !== generationRef.current) return;
        setState((current) => ({
          ...current,
          loading: false,
          backfillPending: false,
          error: error instanceof Error ? error.message : String(error)
        }));
      } finally {
        if (abortRef.current === controller) abortRef.current = null;
      }
    },
    [clearRetry, enabled, enforcePageLimit, prefetchPage, query, queryKey]
  );

  const resetAndLoad = useCallback(() => {
    abortRef.current?.abort();
    clearPrefetch();
    clearRetry();
    const generation = ++generationRef.current;
    pagesRef.current.clear();
    cursorsRef.current = new Map([[0, null]]);
    setState((current) => ({
      ...current,
      items: [],
      history: [],
      pageIndex: 0,
      canPrevious: false,
      canNext: false,
      backfillPending: false,
      error: null
    }));
    if (enabled) void loadPage(0, null, generation, true);
  }, [clearPrefetch, clearRetry, enabled, loadPage]);

  useEffect(() => {
    resetAndLoad();
    return () => {
      generationRef.current += 1;
      abortRef.current?.abort();
      clearPrefetch();
      clearRetry();
    };
  }, [clearPrefetch, clearRetry, queryKey, enabled]); // queryKey intentionally owns query invalidation.

  const next = useCallback(() => {
    if (state.loading || !state.canNext) return;
    const nextIndex = state.pageIndex + 1;
    const cursor = cursorsRef.current.get(nextIndex);
    if (cursor === undefined) return;
    void loadPage(nextIndex, cursor, generationRef.current);
  }, [loadPage, state.canNext, state.loading, state.pageIndex]);

  const previous = useCallback(() => {
    if (state.loading || state.pageIndex <= 0) return;
    const previousIndex = state.pageIndex - 1;
    const cursor = cursorsRef.current.get(previousIndex);
    if (cursor === undefined) return;
    void loadPage(previousIndex, cursor, generationRef.current);
  }, [loadPage, state.loading, state.pageIndex]);

  return { ...state, next, previous, refresh: resetAndLoad };
}

export function useCatalogContext(
  enabled: boolean,
  query: CatalogShelfQuery,
  tagQuery: string
) {
  const queryKey = useMemo(() => stableQueryKey(query), [query]);
  const [counts, setCounts] = useState<Record<string, number>>({});
  const [tags, setTags] = useState<Tag[]>([]);
  const [tagNextCursor, setTagNextCursor] = useState<string | null>(null);
  const [tagLoadingMore, setTagLoadingMore] = useState(false);
  const [tagLimitReached, setTagLimitReached] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const tagGenerationRef = useRef(0);
  const tagNextCursorRef = useRef<string | null>(null);
  const tagLoadingMoreRef = useRef(false);
  const tagLoadMoreControllerRef = useRef<AbortController | null>(null);

  useEffect(() => {
    const generation = ++tagGenerationRef.current;
    tagLoadMoreControllerRef.current?.abort();
    tagLoadMoreControllerRef.current = null;
    tagNextCursorRef.current = null;
    tagLoadingMoreRef.current = false;
    setTagNextCursor(null);
    setTagLoadingMore(false);
    setTagLimitReached(false);
    if (!enabled) {
      setCounts({});
      setTags([]);
      return;
    }
    const controller = new AbortController();
    const timer = window.setTimeout(async () => {
      setLoading(true);
      setError(null);
      try {
        const countsPromise = loadCatalogCounts(query.includeTags, query.query, controller.signal);
        const facetsPromise = query.kind === "history"
          ? Promise.resolve({ items: [] as Tag[], nextCursor: null, revision: 0 })
          : loadCatalogFacets(query, tagQuery, null, controller.signal);
        const [nextCounts, nextFacets] = await Promise.all([countsPromise, facetsPromise]);
        if (controller.signal.aborted || generation !== tagGenerationRef.current) return;
        setCounts(nextCounts.kinds);
        setTags(nextFacets.items);
        tagNextCursorRef.current = nextFacets.nextCursor;
        setTagNextCursor(nextFacets.nextCursor);
      } catch (error) {
        if (!controller.signal.aborted) {
          setError(error instanceof Error ? error.message : String(error));
        }
      } finally {
        if (!controller.signal.aborted && generation === tagGenerationRef.current) setLoading(false);
      }
    }, 150);
    return () => {
      window.clearTimeout(timer);
      controller.abort();
      tagLoadMoreControllerRef.current?.abort();
    };
  }, [enabled, queryKey, tagQuery]);

  const loadMoreTags = useCallback(async () => {
    const cursor = tagNextCursorRef.current;
    if (!enabled || !cursor || tagLoadingMoreRef.current) return;
    const generation = tagGenerationRef.current;
    const controller = new AbortController();
    tagLoadMoreControllerRef.current = controller;
    tagLoadingMoreRef.current = true;
    setTagLoadingMore(true);
    try {
      const page = await loadCatalogFacets(query, tagQuery, cursor, controller.signal);
      if (controller.signal.aborted || generation !== tagGenerationRef.current) return;
      const known = new Set(tags.map((tag) => `${tag.namespace}:${tag.key}`));
      const appended = page.items.filter((tag) => {
        const key = `${tag.namespace}:${tag.key}`;
        if (known.has(key)) return false;
        known.add(key);
        return true;
      });
      const merged = [...tags, ...appended];
      const limitReached = merged.length > MAX_RESIDENT_FACET_TAGS;
      setTags(merged.slice(0, MAX_RESIDENT_FACET_TAGS));
      setTagLimitReached(limitReached);
      const nextCursor = limitReached ? null : page.nextCursor;
      tagNextCursorRef.current = nextCursor;
      setTagNextCursor(nextCursor);
    } catch (loadError) {
      if (!controller.signal.aborted && generation === tagGenerationRef.current) {
        setError(loadError instanceof Error ? loadError.message : String(loadError));
      }
    } finally {
      if (tagLoadMoreControllerRef.current === controller) {
        tagLoadMoreControllerRef.current = null;
        tagLoadingMoreRef.current = false;
        setTagLoadingMore(false);
      }
    }
  }, [enabled, query, queryKey, tagQuery, tags]);

  return { counts, tags, error, loading, tagNextCursor, tagLoadingMore, tagLimitReached, loadMoreTags };
}

export function useCatalogJobs(enabled: boolean) {
  const [jobs, setJobs] = useState<import("../api").Job[]>([]);
  useEffect(() => {
    if (!enabled) return;
    const controller = new AbortController();
    loadCatalogJobs(controller.signal).then(setJobs).catch(() => undefined);
    return () => controller.abort();
  }, [enabled]);
  return { jobs, setJobs };
}
