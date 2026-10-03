import { useCallback, useEffect, useLayoutEffect, useMemo, useRef, useState, type CSSProperties, type ReactNode, type UIEvent } from "react";
import { lazy, Suspense } from "react";
import { AnimatePresence, motion } from "motion/react";
import type { WheelEvent } from "react";
import { createPortal } from "react-dom";
import {
  AudioLines,
  BookOpen,
  BookCopy,
  Bookmark,
  ChevronLeft,
  ChevronRight,
  Cloud,
  ExternalLink,
  Folders,
  Gauge,
  GalleryHorizontal,
  GalleryThumbnails,
  Headphones,
  History as HistoryIcon,
  Image,
  Info,
  Library,
  ListMusic,
  LayoutGrid,
  LayoutList,
  ListFilter,
  Loader2,
  Menu,
  Pause,
  Play,
  RefreshCw,
  Repeat,
  Repeat1,
  Shuffle,
  Search,
  Settings,
  SkipBack,
  SkipForward,
  Sparkles,
  Tags,
  Volume2,
  X,
  ZoomIn,
  ZoomOut
} from "lucide-react";
import { api, assetUrl, assetVersion, catalogAssetToAsset, comicPageUrl, coverUrl, parseMeta, thumbUrl, type AppSettings, type Asset, type AssetRouteInfo, type CloudStatus, type ComicPageInfo, type HistoryRecord, type Job, type LibraryResponse, type StrmTask, type Tag, type WorkDetail, type WorkSummary } from "./api";
import { loadRandomCatalogWork, type CatalogShelfQuery } from "./catalog/api";
import { useCatalogContext, useCatalogJobs, useCatalogShelf } from "./catalog/useCatalog";
import { useProgressQueue } from "./hooks/useProgressQueue";
import { getLibraryLayout, type LibraryContentKind, type LibraryViewMode } from "./features/library/libraryLayout";
import {
  AUDIO_QUEUE_WINDOW,
  AUDIO_TRACK_MAX_CACHED,
  AUDIO_TRACK_PAGE_SIZE,
  mergeAudioTrackPage,
  type AudioPlaylistState
} from "./audioQueue";
import { uiDuration, uiEaseOut } from "./ui/motion";
const NovelReader = lazy(() => import("./components/NovelReader").then((module) => ({ default: module.NovelReader })));

type KindFilter = "history" | "comic" | "novel" | "audio" | "gallery" | "coser-picture";
type ViewMode = LibraryViewMode;
type TagFilterMode = "include";
type TagLanguage = "translated" | "raw";
type ComicReaderMode = "paged" | "scroll" | "horizontal";
type ShelfDisplayMode = "collections" | "single";
type DetailMode = "modal" | "docked";
type LocalSearchState = {
  query: string;
  ids: number[];
  status: "idle" | "loading" | "ready" | "fallback";
  tookMs?: number;
};
type ActiveAudioState = {
  work: WorkDetail["work"];
  asset: Asset;
  playlist: Asset[];
  playlistTotal: number;
  resumePosition: string | null;
  sessionId: number;
};
type OpenCollectionDescriptor = {
  collectionKey: string;
  kind: WorkSummary["kind"];
  title?: string;
};
type AudioRepeatMode = "none" | "all" | "one";

const COMIC_DEFAULT_ASPECT = 0.72;
const COMIC_HORIZONTAL_OVERSCAN = 4;
const COMIC_VERTICAL_OVERSCAN = 4;
const COMIC_MAX_PAGE_COUNT = 100_000;
// The legacy fallback is kept bounded so a disabled Catalog v2 cannot pull
// the entire library into the browser during startup.  Further pages remain
// available through the explicit continuation control below.
const LEGACY_BACKGROUND_PAGE_LIMIT = 5;

const kindLabels: Record<string, string> = {
  comic: "漫画",
  "comic-collection": "合集",
  novel: "轻小说",
  "novel-collection": "合集",
  audio: "音声",
  gallery: "图库",
  "coser-picture": "CoserPicture",
  "coser-picture-collection": "合集",
  generated: "图库",
  history: "浏览历史"
};

const kindIcon: Record<string, ReactNode> = {
  comic: <Image size={16} />,
  novel: <BookOpen size={16} />,
  audio: <Headphones size={16} />,
  gallery: <GalleryThumbnails size={16} />,
  "coser-picture": <GalleryHorizontal size={16} />,
  history: <HistoryIcon size={16} />,
  generated: <GalleryThumbnails size={16} />
};

Object.assign(kindIcon, {
  "comic-collection": <Folders size={16} />,
  "novel-collection": <Folders size={16} />,
  "coser-picture-collection": <Folders size={16} />
});

function isArchiveWorkKind(kind: string) {
  return kind === "comic" || kind === "coser-picture";
}

const defaultReaderSettings = {
  comic_prefetch_pages: 5,
  comic_auto_read_interval_ms: 4000
};

function clampComicAutoReadIntervalMs(value: unknown) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) return defaultReaderSettings.comic_auto_read_interval_ms;
  return Math.round(Math.min(Math.max(numeric, 500), 120000));
}

export function App() {
  const [library, setLibrary] = useState<LibraryResponse>({ works: [], tags: [], jobs: [], history: [] });
  const [legacyLoading, setLegacyLoading] = useState(false);
  const [catalogEnabled, setCatalogEnabled] = useState<boolean | null>(null);
  const [catalogRefreshToken, setCatalogRefreshToken] = useState(0);
  const [activeCollection, setActiveCollection] = useState<OpenCollectionDescriptor | null>(null);
  const [selectedId, setSelectedId] = useState<number | null>(null);
  const [detail, setDetail] = useState<WorkDetail | null>(null);
  const [kind, setKind] = useState<KindFilter>("comic");
  const [query, setQuery] = useState("");
  const [localSearch, setLocalSearch] = useState<LocalSearchState>({ query: "", ids: [], status: "idle" });
  const [tagQuery, setTagQuery] = useState("");
  const [tagFilters, setTagFilters] = useState<Record<string, TagFilterMode>>({});
  const includeTags = useMemo(
    () => Object.entries(tagFilters).filter(([, mode]) => mode === "include").map(([key]) => key),
    [tagFilters]
  );
  const [tagLanguage, setTagLanguage] = useState<TagLanguage>("translated");
  const [selectedTag, setSelectedTag] = useState<Tag | null>(null);
  const [viewMode, setViewMode] = useState<ViewMode>("cover");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [readerOpen, setReaderOpen] = useState(false);
  const [readerDerivativesEnabled, setReaderDerivativesEnabled] = useState(false);
  const [pendingReaderId, setPendingReaderId] = useState<number | null>(null);
  const [readerResume, setReaderResume] = useState(true);
  const [readerPositionOverride, setReaderPositionOverride] = useState<string | null | undefined>(undefined);
  const [settings, setSettings] = useState<AppSettings | null>(null);
  const [settingsOpen, setSettingsOpen] = useState(false);
  const [mobileRailOpen, setMobileRailOpen] = useState(false);
  const [detailMode, setDetailMode] = useState<DetailMode>("modal");
  const [detailModalOpen, setDetailModalOpen] = useState(false);
  const [collectionStack, setCollectionStack] = useState<WorkSummary[] | null>(null);
  const [comicDisplayMode, setComicDisplayMode] = useState<ShelfDisplayMode>("collections");
  const [novelDisplayMode, setNovelDisplayMode] = useState<ShelfDisplayMode>("collections");
  const [coserPictureDisplayMode, setCoserPictureDisplayMode] = useState<ShelfDisplayMode>("collections");
  const [activeAudio, setActiveAudio] = useState<ActiveAudioState | null>(null);
  const audioSessionIdRef = useRef(0);
  const selectedIdRef = useRef<number | null>(null);
  const libraryRequestRef = useRef<AbortController | null>(null);
  const libraryGenerationRef = useRef(0);
  const legacyKnownWorkIdsRef = useRef(new Set<number>());
  const jobsSnapshotRef = useRef<Job[]>([]);
  const seenLibraryTerminalJobIdsRef = useRef(new Set<number>());
  const detailRequestRef = useRef<AbortController | null>(null);
  const detailGenerationRef = useRef(0);
  const detailRef = useRef<WorkDetail | null>(null);
  const openCollectionRef = useRef<OpenCollectionDescriptor | null>(null);
  const historyRequestRef = useRef<AbortController | null>(null);
  const historyGenerationRef = useRef(0);
  const catalogEnabledRef = useRef(false);
  const catalogRandomRequestRef = useRef<AbortController | null>(null);

  const catalogShelfMode = useMemo<CatalogShelfQuery["mode"]>(() => {
    if (kind === "history") return "history";
    if (activeCollection) return "works";
    if (kind === "comic" && comicDisplayMode === "collections") return "collections";
    if (kind === "novel" && novelDisplayMode === "collections") return "collections";
    if (kind === "coser-picture" && coserPictureDisplayMode === "collections") return "collections";
    return "works";
  }, [activeCollection, comicDisplayMode, coserPictureDisplayMode, kind, novelDisplayMode]);
  const catalogQuery = useMemo<CatalogShelfQuery>(() => ({
    kind,
    mode: catalogShelfMode,
    collection: activeCollection?.collectionKey ?? null,
    includeTags,
    query,
    limit: 60,
    refreshToken: catalogRefreshToken
  }), [activeCollection?.collectionKey, catalogRefreshToken, catalogShelfMode, includeTags, kind, query]);
  const catalogShelf = useCatalogShelf(catalogEnabled === true, catalogQuery);
  const catalogContext = useCatalogContext(catalogEnabled === true, catalogQuery, tagQuery);
  const { jobs: catalogJobs, setJobs: setCatalogJobs } = useCatalogJobs(catalogEnabled === true);

  useEffect(() => {
    catalogEnabledRef.current = catalogEnabled === true;
  }, [catalogEnabled]);

  useEffect(() => {
    selectedIdRef.current = selectedId;
  }, [selectedId]);

  useEffect(() => {
    detailRef.current = detail;
    if (detail && pendingReaderId === detail.work.id) {
      setPendingReaderId(null);
      setReaderOpen(true);
    }
  }, [detail, pendingReaderId]);

  const appendLegacyPage = useCallback((page: LibraryResponse, controller: AbortController, generation: number) => {
    if (controller.signal.aborted || generation !== libraryGenerationRef.current) return false;
    const appended = page.works.filter((work) => {
      if (legacyKnownWorkIdsRef.current.has(work.id)) return false;
      legacyKnownWorkIdsRef.current.add(work.id);
      return true;
    });
    setLibrary((current) => {
      if (controller.signal.aborted || generation !== libraryGenerationRef.current) return current;
      return {
        ...current,
        // The API cursor already preserves updated_at order.  Appending the
        // bounded page avoids sorting and re-indexing the entire shelf per page.
        works: appended.length > 0 ? [...current.works, ...appended] : current.works,
        next_cursor: page.next_cursor ?? null
      };
    });
    return true;
  }, []);

  const refreshLegacy = useCallback(async () => {
    libraryRequestRef.current?.abort();
    const controller = new AbortController();
    const generation = ++libraryGenerationRef.current;
    libraryRequestRef.current = controller;
    legacyKnownWorkIdsRef.current = new Set();
    setLegacyLoading(true);
    setError(null);
    let firstPage: LibraryResponse;
    try {
      firstPage = await api.library({ limit: 100, includeContext: true, signal: controller.signal });
    } catch (error) {
      if (libraryRequestRef.current === controller && generation === libraryGenerationRef.current) {
        libraryRequestRef.current = null;
        setLegacyLoading(false);
      }
      if (controller.signal.aborted || generation !== libraryGenerationRef.current) return;
      throw error;
    }
    if (controller.signal.aborted || generation !== libraryGenerationRef.current) return;
    legacyKnownWorkIdsRef.current = new Set(firstPage.works.map((work) => work.id));
    markLibraryTerminalJobsSeen(firstPage.jobs, seenLibraryTerminalJobIdsRef.current);
    jobsSnapshotRef.current = firstPage.jobs;
    setLibrary((current) => (
      !controller.signal.aborted && generation === libraryGenerationRef.current ? firstPage : current
    ));
    if (firstPage.works[0]) setSelectedId((current) => current ?? firstPage.works[0].id);

    const loadRemainingPages = async () => {
      let cursor = firstPage.next_cursor ?? null;
      const seenCursors = new Set<string>();
      let loadedPages = 0;
      try {
        while (cursor && !seenCursors.has(cursor) && loadedPages < LEGACY_BACKGROUND_PAGE_LIMIT) {
          if (controller.signal.aborted || generation !== libraryGenerationRef.current) return;
          seenCursors.add(cursor);
          const page = await api.library({
            cursor,
            limit: 500,
            includeContext: false,
            signal: controller.signal
          });
          if (controller.signal.aborted || generation !== libraryGenerationRef.current) return;
          if (!appendLegacyPage(page, controller, generation)) return;
          cursor = page.next_cursor ?? null;
          loadedPages += 1;
        }
        if (!controller.signal.aborted && generation === libraryGenerationRef.current) {
          setLibrary((current) => ({ ...current, next_cursor: cursor }));
        }
      } catch (error) {
        if (controller.signal.aborted || generation !== libraryGenerationRef.current) return;
        setError(`后台加载作品列表失败：${error instanceof Error ? error.message : String(error)}`);
      } finally {
        if (libraryRequestRef.current === controller && generation === libraryGenerationRef.current) {
          libraryRequestRef.current = null;
          setLegacyLoading(false);
        }
      }
    };

    if (firstPage.next_cursor) {
      void loadRemainingPages();
    } else if (libraryRequestRef.current === controller) {
      libraryRequestRef.current = null;
      setLegacyLoading(false);
    }
  }, [appendLegacyPage]);

  const loadMoreLegacy = useCallback(async () => {
    if (catalogEnabledRef.current || legacyLoading || libraryRequestRef.current) return;
    const cursor = library.next_cursor ?? null;
    if (!cursor) return;
    const controller = new AbortController();
    const generation = ++libraryGenerationRef.current;
    libraryRequestRef.current = controller;
    setLegacyLoading(true);
    try {
      const page = await api.library({ cursor, limit: 500, includeContext: false, signal: controller.signal });
      if (controller.signal.aborted || generation !== libraryGenerationRef.current) return;
      appendLegacyPage(page, controller, generation);
    } catch (error) {
      if (controller.signal.aborted || generation !== libraryGenerationRef.current) return;
      setError(`加载更多作品失败：${error instanceof Error ? error.message : String(error)}`);
    } finally {
      if (libraryRequestRef.current === controller && generation === libraryGenerationRef.current) {
        libraryRequestRef.current = null;
        setLegacyLoading(false);
      }
    }
  }, [appendLegacyPage, legacyLoading, library.next_cursor]);

  const comicAutoReadIntervalMs = clampComicAutoReadIntervalMs(settings?.reader?.comic_auto_read_interval_ms);

  useEffect(() => {
    api
      .settings()
      .then((value) => {
        setSettings(value);
        setDetailMode(value.detail_mode ?? "modal");
      })
      .catch((err) => setError(err.message));
    api
      .health()
      .then((health) => {
        const enabled = health.features?.catalog_v2 ?? false;
        setReaderDerivativesEnabled(health.features?.derivative_cache_v2 ?? false);
        catalogEnabledRef.current = enabled;
        setCatalogEnabled(enabled);
        if (enabled) {
          return api.history().then((history) => {
            setLibrary((current) => ({ ...current, history }));
          });
        }
        return refreshLegacy();
      })
      .catch((err) => {
        setCatalogEnabled(null);
        setError(err instanceof Error ? err.message : String(err));
      });
    let disposed = false;
    let events: EventSource | null = null;
    let retryTimer: number | null = null;
    let retryMs = 1000;
    const connectEvents = () => {
      if (disposed) return;
      events = new EventSource("/api/events");
      events.addEventListener("open", () => {
        retryMs = 1000;
      });
      events.addEventListener("jobs", (event) => {
        try {
          const payload = JSON.parse((event as MessageEvent).data) as { jobs?: Job[] };
          if (!payload.jobs) return;
          const previousJobs = jobsSnapshotRef.current;
          const nextJobs = payload.jobs;
          const refreshAfterTerminalJob = markLibraryTerminalJobsSeen(nextJobs, seenLibraryTerminalJobIdsRef.current);
          const snapshotChanged = !jobsEqual(previousJobs, nextJobs);
          jobsSnapshotRef.current = nextJobs;
          if (snapshotChanged) {
            setCatalogJobs(nextJobs);
            setLibrary((prev) => jobsEqual(prev.jobs, nextJobs) ? prev : { ...prev, jobs: nextJobs });
          }
          if (refreshAfterTerminalJob) {
            if (catalogEnabledRef.current) {
              setCatalogRefreshToken((value) => value + 1);
            } else {
              void refreshLegacy().catch((err) => setError(err instanceof Error ? err.message : String(err)));
            }
          }
        } catch {
          // The active catalog or legacy refresh path remains the source of truth.
        }
      });
      events.onerror = () => {
        events?.close();
        events = null;
        if (disposed || retryTimer !== null) return;
        const delay = retryMs;
        retryMs = Math.min(retryMs * 2, 30000);
        retryTimer = window.setTimeout(() => {
          retryTimer = null;
          connectEvents();
        }, delay);
      };
    };
    connectEvents();
    return () => {
      disposed = true;
      libraryGenerationRef.current += 1;
      libraryRequestRef.current?.abort();
      libraryRequestRef.current = null;
      historyGenerationRef.current += 1;
      historyRequestRef.current?.abort();
      historyRequestRef.current = null;
      catalogRandomRequestRef.current?.abort();
      catalogRandomRequestRef.current = null;
      events?.close();
      if (retryTimer !== null) window.clearTimeout(retryTimer);
    };
  }, [refreshLegacy, setCatalogJobs]);

  useEffect(() => {
    if (catalogEnabled !== true) return;
    setLibrary((current) => ({
      ...current,
      works: catalogShelf.items,
      history: catalogShelf.history.length > 0
        ? [
            ...catalogShelf.history,
            ...current.history.filter((record) => !catalogShelf.history.some((item) => item.work_id === record.work_id))
          ].slice(0, 200)
        : current.history,
      next_cursor: null
    }));
    const firstReadable = catalogShelf.items.find((work) => !work.kind.endsWith("-collection"));
    if (firstReadable) {
      setSelectedId((current) => (
        current && catalogShelf.items.some((work) => work.id === current && !work.kind.endsWith("-collection"))
          ? current
          : firstReadable.id
      ));
    }
  }, [catalogEnabled, catalogShelf.history, catalogShelf.items]);

  useEffect(() => {
    if (catalogEnabled !== true) return;
    setLibrary((current) => ({ ...current, tags: catalogContext.tags }));
  }, [catalogContext.tags, catalogEnabled]);

  useEffect(() => {
    if (catalogEnabled !== true) return;
    jobsSnapshotRef.current = catalogJobs;
    setLibrary((current) => jobsEqual(current.jobs, catalogJobs) ? current : { ...current, jobs: catalogJobs });
  }, [catalogEnabled, catalogJobs]);

  useEffect(() => {
    const catalogError = catalogShelf.error ?? catalogContext.error;
    if (catalogError) setError(catalogError);
  }, [catalogContext.error, catalogShelf.error]);

  useEffect(() => {
    detailRequestRef.current?.abort();
    if (!selectedId) {
      setDetail(null);
      return;
    }
    const controller = new AbortController();
    const generation = ++detailGenerationRef.current;
    detailRequestRef.current = controller;
    if (detailRef.current?.work.id !== selectedId) setDetail(null);
    // The React client always uses the bounded detail shape.  The legacy
    // server mode remains available to older external clients, while this
    // client obtains tracks/pages/images through their dedicated cursors.
    api
      .work(selectedId, controller.signal, "summary")
      .then((next) => {
        if (generation === detailGenerationRef.current && selectedIdRef.current === selectedId) setDetail(next);
      })
      .catch((err) => {
        if (err instanceof DOMException && err.name === "AbortError") return;
        if (generation === detailGenerationRef.current) {
          setPendingReaderId((current) => (current === selectedId ? null : current));
          setError(err instanceof Error ? err.message : String(err));
        }
      });
    return () => controller.abort();
  }, [selectedId]);

  useEffect(() => {
    if (catalogEnabled === true) {
      setLocalSearch({ query: query.trim(), ids: [], status: query.trim() ? "ready" : "idle", tookMs: catalogShelf.tookMs });
      return;
    }
    const needle = query.trim();
    if (needle.length < 2) {
      setLocalSearch({ query: "", ids: [], status: "idle" });
      return;
    }

    let cancelled = false;
    const controller = new AbortController();
    const handle = window.setTimeout(async () => {
      setLocalSearch((prev) => ({ ...prev, query: needle, status: "loading" }));
      try {
        const result = await api.search(needle, 200, controller.signal);
        if (!cancelled) {
          setLocalSearch({
            query: result.query,
            ids: result.hits.map((hit) => hit.work_id),
            status: "ready",
            tookMs: result.took_ms
          });
        }
      } catch {
        if (!cancelled) {
          setLocalSearch({ query: needle, ids: [], status: "fallback" });
        }
      }
    }, 220);

    return () => {
      cancelled = true;
      controller.abort();
      window.clearTimeout(handle);
    };
  }, [catalogEnabled, catalogShelf.tookMs, query, kind]);

  const baseWorks = useMemo(() => library.works.filter((work) => work.kind !== "generated"), [library.works]);

  const scopedWorks = useMemo(() => {
    if (catalogEnabled === true) return baseWorks;
    if (kind !== "history") return baseWorks;
    const byId = new Map(baseWorks.map((work) => [work.id, work]));
    return library.history
      .map((record) => byId.get(record.work_id))
      .filter((work): work is WorkSummary => Boolean(work));
  }, [baseWorks, catalogEnabled, kind, library.history]);

  const availableTagKeys = useMemo(() => {
    if (catalogEnabled === true) return new Set(library.tags.map(tagKey));
    const keys = new Set<string>();
    for (const work of scopedWorks) {
      if (kind !== "history" && work.kind !== kind) continue;
      for (const key of (work.tag_keys ?? "").split(",").map((value) => value.trim()).filter(Boolean)) {
        keys.add(key);
      }
    }
    return keys;
  }, [catalogEnabled, library.tags, scopedWorks, kind]);

  const visibleTags = useMemo(() => {
    if (catalogEnabled === true) return library.tags;
    if (availableTagKeys.size === 0) return [];
    const needle = tagQuery.trim().toLowerCase();
    return library.tags
      .filter((tag) => {
        if (availableTagKeys.size > 0 && !availableTagKeys.has(tagKey(tag))) return false;
        if (!needle) return true;
        return `${tag.namespace}:${tag.key} ${tag.label} ${tag.translated_label ?? ""}`.toLowerCase().includes(needle);
      })
      .slice(0, 120);
  }, [availableTagKeys, catalogEnabled, library.tags, tagQuery]);

  const filteredWorks = useMemo(() => {
    if (catalogEnabled === true) return scopedWorks;
    const needle = query.trim().toLowerCase();
    const searchReady = needle.length >= 2 && localSearch.status === "ready" && localSearch.query.toLowerCase() === needle;
    const searchRank = searchReady ? new Map(localSearch.ids.map((id, index) => [id, index])) : null;
    return scopedWorks.filter((work) => {
      if (kind !== "history" && work.kind !== kind) return false;
      if (needle && searchRank && !searchRank.has(work.id)) return false;
      if (needle && !searchRank && !`${work.title} ${work.category ?? ""} ${work.source_path ?? ""}`.toLowerCase().includes(needle)) return false;
      if (includeTags.length === 0) return true;
      const workTags = work.tag_keys ? work.tag_keys.split(",") : detail?.work.id === work.id ? detail.tags.map(tagKey) : [];
      return includeTags.every((tag) => workTags.includes(tag));
    }).sort((a, b) => (searchRank ? (searchRank.get(a.id) ?? 0) - (searchRank.get(b.id) ?? 0) : 0));
  }, [catalogEnabled, scopedWorks, kind, query, localSearch, includeTags, detail]);

  const counts = useMemo(() => {
    if (catalogEnabled === true) {
      return {
        history: 0,
        comic: 0,
        novel: 0,
        audio: 0,
        gallery: 0,
        "coser-picture": 0,
        ...catalogContext.counts
      };
    }
    return baseWorks.reduce<Record<string, number>>(
      (acc, work) => {
        acc[work.kind] = (acc[work.kind] ?? 0) + 1;
        return acc;
      },
      { history: library.history.length, comic: 0, novel: 0, audio: 0, gallery: 0, "coser-picture": 0 }
    );
  }, [baseWorks, catalogContext.counts, catalogEnabled, library.history.length]);

  const historyByWorkId = useMemo(() => new Map(library.history.map((record) => [record.work_id, record])), [library.history]);

  const cancelHistoryLookup = () => {
    historyGenerationRef.current += 1;
    historyRequestRef.current?.abort();
    historyRequestRef.current = null;
  };

  const resolveExactHistoryPosition = async (workId: number, fallback: string | null) => {
    historyRequestRef.current?.abort();
    const controller = new AbortController();
    const generation = ++historyGenerationRef.current;
    historyRequestRef.current = controller;
    try {
      const record = await api.workHistory(workId, controller.signal);
      if (controller.signal.aborted || generation !== historyGenerationRef.current) {
        return { current: false, position: fallback };
      }
      setLibrary((prev) => ({
        ...prev,
        history: record
          ? [record, ...prev.history.filter((item) => item.work_id !== workId)]
          : prev.history.filter((item) => item.work_id !== workId)
      }));
      return { current: true, position: record?.position ?? null };
    } catch {
      return {
        current: !controller.signal.aborted && generation === historyGenerationRef.current,
        position: fallback
      };
    } finally {
      if (historyRequestRef.current === controller) historyRequestRef.current = null;
    }
  };

  const openReader = (resume = true) => {
    setPendingReaderId(null);
    setReaderResume(resume);
    const workId = detailRef.current?.work.id;
    if (!resume || !workId) {
      cancelHistoryLookup();
      setReaderPositionOverride(resume ? undefined : "start");
      setReaderOpen(true);
      return;
    }
    const fallback = historyByWorkId.get(workId)?.position ?? null;
    void resolveExactHistoryPosition(workId, fallback).then((result) => {
      if (!result.current || detailRef.current?.work.id !== workId) return;
      setReaderPositionOverride(result.position);
      setReaderOpen(true);
    });
  };

  const runScan = async () => {
    setBusy(true);
    setError(null);
    try {
      await api.scan(false);
      if (catalogEnabledRef.current) setCatalogRefreshToken((value) => value + 1);
      else await refreshLegacy();
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setBusy(false);
    }
  };

  const runTagImport = async () => {
    setBusy(true);
    setError(null);
    try {
      await api.enrich("import-tag-translations");
      if (catalogEnabledRef.current) setCatalogRefreshToken((value) => value + 1);
      else await refreshLegacy();
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setBusy(false);
    }
  };

  const saveSettings = async (next: AppSettings) => {
    setBusy(true);
    setError(null);
    try {
      const saved = await api.updateSettings(next);
      setSettings(saved);
      setDetailMode(saved.detail_mode ?? "modal");
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setBusy(false);
    }
  };

  const closeCollection = useCallback(() => {
    openCollectionRef.current = null;
    setActiveCollection(null);
    setCollectionStack(null);
  }, []);

  useEffect(() => {
    closeCollection();
  }, [closeCollection, comicDisplayMode, coserPictureDisplayMode, kind, novelDisplayMode, query, tagFilters]);

  const syncProgress = (id: number, progress: number, position?: string | null) => {
    setLibrary((prev) => ({
      ...prev,
      works: prev.works.map((work) => (work.id === id ? { ...work, progress } : work)),
      history: upsertLocalHistory(prev.history, prev.works.find((work) => work.id === id), progress, position)
    }));
    setDetail((prev) => (prev?.work.id === id ? { ...prev, work: { ...prev.work, progress } } : prev));
  };

  const playTrackInDock = (work: WorkDetail["work"], asset: Asset, playlist?: Asset[], playlistTotal?: number) => {
    const fallback = historyByWorkId.get(work.id)?.position ?? null;
    void resolveExactHistoryPosition(work.id, fallback).then((result) => {
      if (!result.current) return;
      setActiveAudio({
        work,
        asset,
        playlist: playlist && playlist.length > 0 ? playlist : [asset],
        playlistTotal: Math.max(playlistTotal ?? playlist?.length ?? 1, playlist?.length ?? 1),
        resumePosition: result.position,
        sessionId: ++audioSessionIdRef.current
      });
    });
  };

  const collectionShelfWorks = useMemo(() => {
    if (catalogEnabled === true) return filteredWorks;
    if (kind === "comic" && comicDisplayMode === "collections") return buildComicCollections(filteredWorks);
    if (kind === "novel" && novelDisplayMode === "collections") return buildNovelCollections(filteredWorks);
    if (kind === "coser-picture" && coserPictureDisplayMode === "collections") return buildCoserPictureCollections(filteredWorks);
    return filteredWorks;
  }, [catalogEnabled, comicDisplayMode, coserPictureDisplayMode, filteredWorks, kind, novelDisplayMode]);

  const displayedWorks = useMemo(
    () => catalogEnabled === true ? collectionShelfWorks : collectionStack ?? collectionShelfWorks,
    [catalogEnabled, collectionShelfWorks, collectionStack]
  );
  const collectionNavigationStack = catalogEnabled === true && activeCollection
    ? displayedWorks
    : collectionStack;

  useEffect(() => {
    if (catalogEnabled === true) return;
    const descriptor = openCollectionRef.current;
    if (!descriptor) return;
    const collection = collectionShelfWorks.find((work) => {
      if (work.kind !== descriptor.kind) return false;
      return parseMeta<{ collection_key?: string }>(work.meta_json).collection_key === descriptor.collectionKey;
    });
    if (!collection) return;
    const meta = parseMeta<{ volume_ids?: number[] }>(collection.meta_json);
    const workById = new Map(filteredWorks.map((work) => [work.id, work]));
    const volumes = (meta.volume_ids ?? [])
      .map((id) => workById.get(id))
      .filter((work): work is WorkSummary => Boolean(work));
    if (volumes.length === 0) return;
    setCollectionStack((current) => {
      if (
        current?.length === volumes.length &&
        current.every((work, index) => work === volumes[index])
      ) {
        return current;
      }
      return volumes;
    });
  }, [catalogEnabled, collectionShelfWorks, filteredWorks]);

  const openWorkPreview = (work: WorkSummary) => {
    cancelHistoryLookup();
    setPendingReaderId(null);
    setSelectedId(work.id);
    if (detailMode === "modal") setDetailModalOpen(true);
  };

  const openGalleryReader = (work: WorkSummary) => {
    const fallback = historyByWorkId.get(work.id)?.position ?? null;
    void resolveExactHistoryPosition(work.id, fallback).then((result) => {
      if (!result.current) return;
      setSelectedId(work.id);
      setDetailModalOpen(false);
      setReaderResume(true);
      setReaderPositionOverride(result.position);
      if (detailRef.current?.work.id === work.id) {
        setPendingReaderId(null);
        setReaderOpen(true);
      } else {
        setPendingReaderId(work.id);
      }
    });
  };

  const openComicReader = async (work: WorkSummary, resume = true, position?: string | null) => {
    let resolvedPosition = position;
    if (resume && position === undefined) {
      const result = await resolveExactHistoryPosition(work.id, historyByWorkId.get(work.id)?.position ?? null);
      if (!result.current) return;
      resolvedPosition = result.position;
    } else {
      cancelHistoryLookup();
    }
    setSelectedId(work.id);
    setDetailModalOpen(false);
    setReaderResume(resume);
    setReaderPositionOverride(resume ? resolvedPosition : "start");
    if (detailRef.current?.work.id === work.id) {
      setPendingReaderId(null);
      setReaderOpen(true);
    } else {
      setPendingReaderId(work.id);
    }
  };

  const openCollection = (work: WorkSummary) => {
    cancelHistoryLookup();
    setPendingReaderId(null);
    const meta = parseMeta<{ collection_key?: string; first_work_id?: number; volume_ids?: number[] }>(work.meta_json);
    if (catalogEnabled === true && meta.collection_key) {
      const descriptor = { collectionKey: meta.collection_key, kind: work.kind, title: work.title };
      openCollectionRef.current = descriptor;
      setActiveCollection(descriptor);
      setCollectionStack([]);
      setDetailModalOpen(false);
      return;
    }
    const volumes = (meta.volume_ids ?? [])
      .map((id) => filteredWorks.find((item) => item.id === id))
      .filter((item): item is WorkSummary => Boolean(item));
    if (volumes.length === 0) {
      openWorkPreview(filteredWorks.find((item) => item.id === meta.first_work_id) ?? work);
      return;
    }
    if (meta.collection_key) {
      openCollectionRef.current = { collectionKey: meta.collection_key, kind: work.kind };
    }
    setCollectionStack(volumes);
    setSelectedId(volumes[0].id);
    setDetailModalOpen(false);
  };

  const openRandomComic = () => {
    if (catalogEnabled === true) {
      catalogRandomRequestRef.current?.abort();
      const controller = new AbortController();
      catalogRandomRequestRef.current = controller;
      void loadRandomCatalogWork({ ...catalogQuery, kind: "comic", mode: "works" }, controller.signal)
        .then((work) => {
          if (!work || controller.signal.aborted) return;
          void openComicReader(work, false, "start");
        })
        .catch((err) => {
          if (err instanceof DOMException && err.name === "AbortError") return;
          setError(err instanceof Error ? err.message : String(err));
        })
        .finally(() => {
          if (catalogRandomRequestRef.current === controller) catalogRandomRequestRef.current = null;
        });
      return;
    }
    const candidates = (collectionStack ?? filteredWorks).filter((work) => work.kind === "comic");
    if (candidates.length === 0) return;
    const work = candidates[Math.floor(Math.random() * candidates.length)];
    void openComicReader(work, false, "start");
  };

  return (
    <div className={detailMode === "docked" ? "app-shell has-detail-pane" : "app-shell modal-detail"}>
      <button
        className={mobileRailOpen ? "rail-mobile-backdrop open" : "rail-mobile-backdrop"}
        type="button"
        onClick={() => setMobileRailOpen(false)}
        aria-label="关闭导航"
      />
      <aside className={mobileRailOpen ? "rail mobile-open" : "rail"}>
        <RailContent
          counts={counts}
          includeTags={includeTags}
          kind={kind}
          selectedTag={selectedTag}
          tagFilters={tagFilters}
          tagLanguage={tagLanguage}
          tagQuery={tagQuery}
          visibleTags={visibleTags}
          hasMoreTags={catalogEnabled === true && Boolean(catalogContext.tagNextCursor)}
          tagLoadingMore={catalogContext.tagLoadingMore}
          tagLimitReached={catalogContext.tagLimitReached}
          onKindChange={(next) => {
            setKind(next);
            setMobileRailOpen(false);
          }}
          onSelectedTagChange={setSelectedTag}
          onTagFiltersChange={setTagFilters}
          onTagLanguageChange={setTagLanguage}
          onTagQueryChange={setTagQuery}
          onLoadMoreTags={catalogContext.loadMoreTags}
        />
      </aside>

      <main className="workspace">
        <header className="toolbar">
          <ToolbarContent
            collectionStack={collectionNavigationStack}
            comicDisplayMode={comicDisplayMode}
            comicCount={catalogEnabled === true ? counts.comic ?? 0 : (collectionNavigationStack ?? filteredWorks).filter((work) => work.kind === "comic").length}
            coserPictureDisplayMode={coserPictureDisplayMode}
            kind={kind}
            localSearch={localSearch}
            novelDisplayMode={novelDisplayMode}
            query={query}
            viewMode={viewMode}
            onMenuOpen={() => setMobileRailOpen(true)}
            onCollectionBack={closeCollection}
            onComicDisplayModeChange={setComicDisplayMode}
            onCoserPictureDisplayModeChange={setCoserPictureDisplayMode}
            onOpenRandomComic={openRandomComic}
            onNovelDisplayModeChange={setNovelDisplayMode}
            onQueryChange={setQuery}
            onSettingsOpen={() => setSettingsOpen(true)}
            onViewModeChange={setViewMode}
          />
        </header>

        {error && (
          <motion.div className="error-strip" initial={{ opacity: 0, y: -8 }} animate={{ opacity: 1, y: 0 }}>
            {error}
          </motion.div>
        )}

        {catalogEnabled === null && (
          <div className="catalog-status" role="status">
            <Loader2 className="spin" size={16} />
            <span>正在确认书架服务状态…</span>
          </div>
        )}
        {catalogEnabled === true && catalogShelf.backfillPending && (
          <div className="catalog-status" role="status">
            <Loader2 className="spin" size={16} />
            <span>正在生成合集摘要，完成后会自动显示…</span>
          </div>
        )}
        {catalogEnabled === true && catalogShelf.loading && !catalogShelf.backfillPending && (
          <div className="catalog-status" role="status">
            <Loader2 className="spin" size={16} />
            <span>正在加载第 {catalogShelf.pageIndex + 1} 页…</span>
          </div>
        )}

        <VirtualShelf
          items={displayedWorks}
          itemKey={(work) => work.id}
          contentKind={kind === "audio" ? "audio" : "portrait"}
          viewMode={viewMode}
          renderItem={(work) => (
            <WorkCard
              key={work.id}
              selected={work.id === selectedId}
              viewMode={viewMode}
              work={work}
              onClick={() => {
                if (work.kind === "novel-collection" || work.kind === "comic-collection" || work.kind === "coser-picture-collection") {
                  openCollection(work);
                } else if (work.kind === "gallery") {
                  openGalleryReader(work);
                } else if (work.kind === "coser-picture") {
                  void openComicReader(work);
                } else {
                  openWorkPreview(work);
                }
              }}
            />
          )}
        />
        {catalogEnabled === true && displayedWorks.length > 0 && !catalogShelf.backfillPending && (
          <nav className="catalog-pager" aria-label="书架分页">
            <button onClick={catalogShelf.previous} disabled={!catalogShelf.canPrevious || catalogShelf.loading}>
              <ChevronLeft size={16} />
              <span>上一页</span>
            </button>
            <span>第 {catalogShelf.pageIndex + 1} 页</span>
            <button onClick={catalogShelf.next} disabled={!catalogShelf.canNext || catalogShelf.loading}>
              <span>下一页</span>
              <ChevronRight size={16} />
            </button>
          </nav>
        )}
        {catalogEnabled === false && library.next_cursor && (
          <nav className="catalog-pager legacy-pager" aria-label="旧版书架续页">
            <button onClick={() => void loadMoreLegacy()} disabled={legacyLoading}>
              {legacyLoading ? <Loader2 className="spin" size={16} /> : <ChevronRight size={16} />}
              <span>{legacyLoading ? "正在加载…" : "加载更多"}</span>
            </button>
            <span>已加载 {baseWorks.length} 项</span>
          </nav>
        )}
      </main>

      {detailMode === "docked" && (
        <DetailPane
          detail={detail}
          jobs={library.jobs}
          tagLanguage={tagLanguage}
          variant="docked"
          onClose={() => setDetailModalOpen(false)}
          onOpenReader={openReader}
          onPlayTrack={playTrackInDock}
          onTagPick={(key) => setTagFilters((prev) => cycleTagFilter(prev, key))}
        />
      )}
      <AudioDock
        active={activeAudio}
        canPersistProgress={true}
        onClose={() => setActiveAudio(null)}
        onProgressSaved={syncProgress}
        resumePosition={activeAudio?.resumePosition ?? null}
      />

      <AnimatePresence>
        {detailMode === "modal" && detailModalOpen && detail && (
          <motion.div className="detail-modal-backdrop" initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }} onClick={() => setDetailModalOpen(false)}>
            <DetailPane
              detail={detail}
              jobs={library.jobs}
              tagLanguage={tagLanguage}
              variant="modal"
              onClose={() => setDetailModalOpen(false)}
              onOpenReader={openReader}
              onPlayTrack={playTrackInDock}
              onTagPick={(key) => setTagFilters((prev) => cycleTagFilter(prev, key))}
            />
          </motion.div>
        )}
        {settingsOpen && (
          <SettingsOverlay
            busy={busy}
            jobs={library.jobs}
            settings={settings}
            onClose={() => setSettingsOpen(false)}
            onRescan={runScan}
            onSaveSettings={saveSettings}
            onTagImport={runTagImport}
          />
        )}
        {readerOpen && detail && (
          <ReaderOverlay
            key={detail.work.id}
            canPersistProgress={true}
            detail={detail}
            onClose={() => {
              setPendingReaderId(null);
              setReaderOpen(false);
            }}
            onPlayTrack={playTrackInDock}
            onProgressSaved={syncProgress}
            readerDerivativesEnabled={readerDerivativesEnabled}
            resumePosition={readerResume ? readerPositionOverride ?? historyByWorkId.get(detail.work.id)?.position ?? null : "start"}
            comicPrefetchPages={settings?.reader?.comic_prefetch_pages ?? 5}
            comicAutoReadIntervalMs={comicAutoReadIntervalMs}
          />
        )}
      </AnimatePresence>
    </div>
  );
}

function RailContent({
  counts,
  includeTags,
  kind,
  selectedTag,
  tagFilters,
  tagLanguage,
  tagQuery,
  visibleTags,
  hasMoreTags,
  tagLoadingMore,
  tagLimitReached,
  onKindChange,
  onSelectedTagChange,
  onTagFiltersChange,
  onTagLanguageChange,
  onTagQueryChange,
  onLoadMoreTags
}: {
  counts: Record<string, number>;
  includeTags: string[];
  kind: KindFilter;
  selectedTag: Tag | null;
  tagFilters: Record<string, TagFilterMode>;
  tagLanguage: TagLanguage;
  tagQuery: string;
  visibleTags: Tag[];
  hasMoreTags: boolean;
  tagLoadingMore: boolean;
  tagLimitReached: boolean;
  onKindChange: (kind: KindFilter) => void;
  onSelectedTagChange: (tag: Tag | null) => void;
  onTagFiltersChange: (next: Record<string, TagFilterMode> | ((prev: Record<string, TagFilterMode>) => Record<string, TagFilterMode>)) => void;
  onTagLanguageChange: (next: TagLanguage | ((prev: TagLanguage) => TagLanguage)) => void;
  onTagQueryChange: (value: string) => void;
  onLoadMoreTags: () => void;
}) {
  return (
    <>
      <div className="brand">
        <Library />
        <span>Aris的仓库</span>
      </div>
      <nav className="kind-nav">
        {(["gallery", "coser-picture", "comic", "novel", "audio", "history"] as KindFilter[]).map((item) => (
          <button className={kind === item ? "active" : ""} key={item} onClick={() => onKindChange(item)}>
            {kindIcon[item]}
            <span>{kindLabels[item]}</span>
            <strong>{counts[item] ?? 0}</strong>
          </button>
        ))}
      </nav>
      <div className="tag-search">
        <ListFilter size={16} />
        <input value={tagQuery} onChange={(event) => onTagQueryChange(event.target.value)} placeholder="标签" />
        <button
          className="tag-language-toggle"
          onClick={() => onTagLanguageChange((value) => (value === "translated" ? "raw" : "translated"))}
          aria-label="切换标签语言"
        >
          {tagLanguage === "translated" ? "ZH" : "RAW"}
        </button>
      </div>
      {includeTags.length > 0 && (
        <div className="tag-filter-summary">
          {includeTags.map((key) => (
            <button key={`include-${key}`} onClick={() => onTagFiltersChange((prev) => cycleTagFilter(prev, key))}>
              + {shortTag(key)}
            </button>
          ))}
        </div>
      )}
      {selectedTag && (
        <TagDetailPanel
          language={tagLanguage}
          tag={selectedTag}
          onClose={() => onSelectedTagChange(null)}
        />
      )}
      <div className="tag-list">
        {visibleTags.map((tag) => {
          const key = tagKey(tag);
          const mode = tagFilters[key];
          return (
            <div className={mode ? `tag-row ${mode}` : "tag-row"} key={key}>
              <button className="tag-pick" onClick={() => onTagFiltersChange((prev) => cycleTagFilter(prev, key))}>
                <span>{tagNamespace(tag, tagLanguage)}</span>
                <b>{tagLabel(tag, tagLanguage)}</b>
                <em>{tag.count}</em>
              </button>
              <button className="tag-info" onClick={() => onSelectedTagChange(tag)} aria-label="标签详情">
                <Info size={14} />
              </button>
            </div>
          );
        })}
        {hasMoreTags && (
          <button
            className="tag-load-more"
            type="button"
            onClick={onLoadMoreTags}
            disabled={tagLoadingMore}
            aria-label="加载更多标签"
          >
            {tagLoadingMore ? <Loader2 className="spin" size={14} /> : <ChevronRight size={14} />}
            <span>{tagLoadingMore ? "加载中" : "加载更多标签"}</span>
          </button>
        )}
        {tagLimitReached && <span className="tag-limit-note">已达到标签显示上限</span>}
      </div>
    </>
  );
}

function ToolbarContent({
  collectionStack,
  comicDisplayMode,
  comicCount,
  coserPictureDisplayMode,
  kind,
  localSearch,
  novelDisplayMode,
  query,
  viewMode,
  onCollectionBack,
  onComicDisplayModeChange,
  onCoserPictureDisplayModeChange,
  onOpenRandomComic,
  onNovelDisplayModeChange,
  onQueryChange,
  onMenuOpen,
  onSettingsOpen,
  onViewModeChange
}: {
  collectionStack: WorkSummary[] | null;
  comicDisplayMode: ShelfDisplayMode;
  comicCount: number;
  coserPictureDisplayMode: ShelfDisplayMode;
  kind: KindFilter;
  localSearch: LocalSearchState;
  novelDisplayMode: ShelfDisplayMode;
  query: string;
  viewMode: ViewMode;
  onCollectionBack: () => void;
  onComicDisplayModeChange: (mode: ShelfDisplayMode) => void;
  onCoserPictureDisplayModeChange: (mode: ShelfDisplayMode) => void;
  onOpenRandomComic: () => void;
  onNovelDisplayModeChange: (mode: ShelfDisplayMode) => void;
  onQueryChange: (value: string) => void;
  onMenuOpen: () => void;
  onSettingsOpen: () => void;
  onViewModeChange: (value: ViewMode) => void;
}) {
  return (
    <>
      <div className="toolbar-left">
        <button className="icon-btn mobile-menu-btn" type="button" onClick={onMenuOpen} aria-label="打开导航">
          <Menu size={18} />
        </button>
        {collectionStack && (
          <button className="primary-action subtle-action collection-back-action" onClick={onCollectionBack}>
            <ChevronLeft size={16} />
            <span>返回合集</span>
          </button>
        )}
      </div>
      <div className="toolbar-center">
        <div className="searchbar">
          <Search size={18} />
          <input value={query} onChange={(event) => onQueryChange(event.target.value)} placeholder="搜索书架" />
          {localSearch.status === "loading" && <Loader2 className="spin search-state" size={15} />}
          {localSearch.status === "ready" && localSearch.query === query.trim() && <span className="search-state">{localSearch.tookMs ?? 0}ms</span>}
        </div>
      </div>
      <div className="toolbar-actions">
        {kind === "comic" && (
          <>
            <div className="segmented compact-segmented" aria-label="漫画显示方式">
              <button className={comicDisplayMode === "collections" ? "active" : ""} onClick={() => onComicDisplayModeChange("collections")}>
                <Folders size={16} />
                <span>合集</span>
              </button>
              <button className={comicDisplayMode === "single" ? "active" : ""} onClick={() => onComicDisplayModeChange("single")}>
                <BookCopy size={16} />
                <span>单本</span>
              </button>
            </div>
            <button className="primary-action subtle-action random-action" onClick={onOpenRandomComic} disabled={comicCount <= 0}>
              <Shuffle size={16} />
              <span>随机阅读</span>
            </button>
          </>
        )}
        {kind === "novel" && (
          <div className="segmented compact-segmented" aria-label="小说显示方式">
            <button className={novelDisplayMode === "collections" ? "active" : ""} onClick={() => onNovelDisplayModeChange("collections")}>
              <Folders size={16} />
              <span>合集</span>
            </button>
            <button className={novelDisplayMode === "single" ? "active" : ""} onClick={() => onNovelDisplayModeChange("single")}>
              <BookCopy size={16} />
              <span>单本</span>
            </button>
          </div>
        )}
        {kind === "coser-picture" && (
          <div className="segmented compact-segmented" aria-label="CoserPicture显示方式">
            <button className={coserPictureDisplayMode === "collections" ? "active" : ""} onClick={() => onCoserPictureDisplayModeChange("collections")}>
              <Folders size={16} />
              <span>合集</span>
            </button>
            <button className={coserPictureDisplayMode === "single" ? "active" : ""} onClick={() => onCoserPictureDisplayModeChange("single")}>
              <BookCopy size={16} />
              <span>单套</span>
            </button>
          </div>
        )}
        <ViewModePicker value={viewMode} onChange={onViewModeChange} />
        <button className="icon-btn" onClick={onSettingsOpen} aria-label="设置">
          <Settings size={18} />
        </button>
      </div>
    </>
  );
}

function ViewModePicker({ value, onChange }: { value: ViewMode; onChange: (value: ViewMode) => void }) {
  const modes: Array<[ViewMode, ReactNode, string]> = [
    ["cover", <GalleryHorizontal size={16} />, "封面"],
    ["grid", <LayoutGrid size={16} />, "网格"],
    ["compact", <GalleryThumbnails size={16} />, "紧凑"],
    ["list", <LayoutList size={16} />, "列表"]
  ];
  const currentIndex = Math.max(0, modes.findIndex(([mode]) => mode === value));
  const [, icon, label] = modes[currentIndex];
  const nextMode = modes[(currentIndex + 1) % modes.length][0];
  return (
    <button className="icon-btn view-cycle-btn" onClick={() => onChange(nextMode)} title={`当前：${label}`} aria-label="切换视图">
      {icon}
      <span>{label}</span>
    </button>
  );
}

function SettingsOverlay({
  busy,
  jobs,
  settings,
  onClose,
  onRescan,
  onSaveSettings,
  onTagImport
}: {
  busy: boolean;
  jobs: Job[];
  settings: AppSettings | null;
  onClose: () => void;
  onRescan: () => void;
  onSaveSettings: (settings: AppSettings) => void;
  onTagImport: () => void;
}) {
  const [draft, setDraft] = useState<AppSettings | null>(settings);
  const [cloudStatus, setCloudStatus] = useState<CloudStatus | null>(null);
  const latestScanResult = [...jobs]
    .filter((job) => job.job_type === "scan-library")
    .sort((left, right) => right.id - left.id)
    .map((job) => parseMeta<{
      scan_result?: {
        status: string;
        source_results: Array<{
          kind: string;
          root: string;
          status: string;
          discovered: number;
          imported: number;
          skipped: number;
          failed: number;
          message?: string | null;
        }>;
      };
    }>(job.payload_json).scan_result)
    .find(Boolean);

  useEffect(() => {
    setDraft(settings ? normalizeSettingsDraft(settings) : null);
  }, [settings]);

  useEffect(() => {
    api
      .cloudStatus()
      .then((status) => setCloudStatus(status))
      .catch(() => setCloudStatus(null));
  }, [settings]);

  const updateDraft = (updater: (value: AppSettings) => AppSettings) => {
    setDraft((prev) => (prev ? updater(prev) : prev));
  };

  type MediaDirectoryKey = "comics" | "novels" | "audio" | "gallery" | "coser_picture";

  const mediaLabels: Record<MediaDirectoryKey, string> = {
    comics: "漫画目录",
    novels: "轻小说目录",
    audio: "音声目录",
    gallery: "图库目录",
    coser_picture: "CoserPicture目录"
  };
  const coverCacheLabels: Record<keyof AppSettings["cover_cache_dirs"], string> = {
    comic: "漫画封面缓存",
    novel: "轻小说封面缓存",
    audio: "音声封面缓存",
    gallery: "图库封面缓存",
    coser_picture: "CoserPicture封面缓存"
  };
  const cloudKindLabels: Record<AppSettings["media_sources"][number]["kind"], string> = {
    comic: "漫画",
    novel: "轻小说",
    audio: "音声",
    gallery: "图库",
    "coser-picture": "CoserPicture"
  };
  const updateQMediaSync = (patch: Partial<AppSettings["qmediasync"]>) => {
    updateDraft((prev) => ({
      ...prev,
      qmediasync: {
        ...prev.qmediasync,
        ...patch
      }
    }));
  };

  return (
      <motion.div className="settings-backdrop" initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }} transition={{ duration: uiDuration.fade, ease: uiEaseOut }}>
      <motion.article className="settings-panel" initial={{ opacity: 0, y: 18, scale: 0.98 }} animate={{ opacity: 1, y: 0, scale: 1 }} exit={{ opacity: 0, y: 18, scale: 0.98 }} transition={{ duration: uiDuration.modal, ease: uiEaseOut }}>
        <header className="settings-header">
          <div>
            <span>Settings</span>
            <h2>设置</h2>
          </div>
          <button className="icon-btn" onClick={onClose} aria-label="关闭设置">
            <X size={18} />
          </button>
        </header>

        <div className="settings-body">
          <section className="settings-section">
            <h3>本地服务</h3>
            <p className="settings-hint settings-access-note">
              当前服务由本机或容器访问控制保护，不再使用管理员密码。资源目录由容器挂载决定，设置页仅展示当前访问路径。
            </p>
          </section>

          <>
              {draft && (
                <section className="settings-section">
                  <h3>预览栏</h3>
                  <div className="segmented">
                    <button className={draft.detail_mode !== "docked" ? "active" : ""} onClick={() => updateDraft((prev) => ({ ...prev, detail_mode: "modal" }))}>
                      <GalleryHorizontal size={16} />
                      <span>中间弹出</span>
                    </button>
                    <button className={draft.detail_mode === "docked" ? "active" : ""} onClick={() => updateDraft((prev) => ({ ...prev, detail_mode: "docked" }))}>
                      <LayoutList size={16} />
                      <span>固定右侧</span>
                    </button>
                  </div>
                </section>
              )}

              {draft && (
                <section className="settings-section">
                  <h3>阅读</h3>
                  <label className="setting-field">
                    <span>图片归档自动阅读间隔（秒）</span>
                    <input
                      min={0.5}
                      max={120}
                      step={0.5}
                      type="number"
                      value={Number(((draft.reader?.comic_auto_read_interval_ms ?? defaultReaderSettings.comic_auto_read_interval_ms) / 1000).toFixed(1))}
                      onChange={(event) => {
                        const seconds = Number(event.currentTarget.value);
                        const milliseconds = clampComicAutoReadIntervalMs(Number.isFinite(seconds) ? seconds * 1000 : defaultReaderSettings.comic_auto_read_interval_ms);
                        updateDraft((prev) => ({
                          ...prev,
                          reader: {
                            ...(prev.reader ?? defaultReaderSettings),
                            comic_auto_read_interval_ms: milliseconds
                          }
                        }));
                      }}
                    />
                  </label>
                  <label className="setting-field">
                    <span>漫画向前预加载页数</span>
                    <select value={draft.reader?.comic_prefetch_pages ?? 5}
                      onChange={(event) => updateDraft((prev) => ({ ...prev, reader: {
                        ...(prev.reader ?? defaultReaderSettings), comic_prefetch_pages: Number(event.target.value)
                      } }))}>
                      {[5, 6, 7, 8, 9, 10].map(count => <option key={count} value={count}>{count} 页</option>)}
                    </select>
                  </label>
                  <p className="settings-hint">当前页完成后逐页预加载；慢速或省流网络暂停预加载，读取失败即停止。</p>
                  <p className="settings-hint">用于漫画和 CoserPicture 阅读器的自动翻页按钮，支持 0.5 到 120 秒。</p>
                </section>
              )}

              {draft && (
                <section className="settings-section">
                  <h3>资源访问目录</h3>
                  <p className="settings-hint">
                    目录为只读信息。需要调整资源位置时，请修改容器的 volumes 映射和对应的 *_DIR 环境变量，然后重启服务。
                  </p>
                  {(["comics", "novels", "audio", "gallery", "coser_picture"] as MediaDirectoryKey[]).map((dirKind) => (
                    <div className="directory-editor readonly-directory" key={dirKind}>
                      <b>{mediaLabels[dirKind]}</b>
                      <div className="directory-list">
                        {draft.media_dirs[dirKind].map((path) => (
                          <span key={path}>
                            <em>{path}</em>
                            <small>容器挂载</small>
                          </span>
                        ))}
                      </div>
                    </div>
                  ))}
                  <label className="setting-field">
                    <span>本地漫画扫描深度</span>
                    <input
                      min={1}
                      max={64}
                      step={1}
                      type="number"
                      value={draft.media_dirs.comic_scan_depth}
                      onChange={(event) => {
                        const value = Number.parseInt(event.currentTarget.value, 10);
                        updateDraft((prev) => ({
                          ...prev,
                          media_dirs: {
                            ...prev.media_dirs,
                            comic_scan_depth: Math.min(Math.max(Number.isFinite(value) ? value : 3, 1), 64)
                          }
                        }));
                      }}
                    />
                  </label>
                  <p className="settings-hint">从漫画根目录开始计数；默认 3 可覆盖“作者/系列/文件.cbz”，最大 64。</p>
                  <label className="setting-field">
                    <span>音声分组策略</span>
                    <select
                      value={draft.audio_grouping ?? "auto"}
                      onChange={(event) => {
                        const value = event.currentTarget.value as AppSettings["audio_grouping"];
                        updateDraft((prev) => ({ ...prev, audio_grouping: value }));
                      }}
                    >
                      <option value="auto">自动（RJ 优先，普通目录回退）</option>
                      <option value="rj">RJ 编号</option>
                      <option value="folder">文件夹</option>
                    </select>
                  </label>
                  <p className="settings-hint">音声目录会将该策略写入 Inventory root，监听、重命名和删除事件保持同一 work identity。</p>
                </section>
              )}

              {draft && (
                <section className="settings-section">
                  <h3>封面缓存目录</h3>
                  <p className="settings-hint">缓存目录同样由容器配置，应用内不提供修改入口。</p>
                  {(["comic", "novel", "audio", "gallery", "coser_picture"] as Array<keyof AppSettings["cover_cache_dirs"]>).map((cacheKind) => (
                    <div className="directory-list readonly-directory" key={cacheKind}>
                      <span>
                        <b>{coverCacheLabels[cacheKind]}</b>
                        <em>{draft.cover_cache_dirs[cacheKind]}</em>
                        <small>容器目录</small>
                      </span>
                    </div>
                  ))}
                </section>
              )}

              {draft && (
                <section className="settings-section">
                  <h3>云盘源</h3>
                  <div className="cloud-settings">
                    <label className="toggle-row">
                      <input
                        checked={draft.qmediasync.enabled}
                        onChange={(event) => updateQMediaSync({ enabled: event.currentTarget.checked })}
                        type="checkbox"
                      />
                      <span>启用 qmediasync</span>
                    </label>
                    <div className="directory-add cloud-endpoint">
                      <input
                        value={draft.qmediasync.base_url}
                        onChange={(event) => updateQMediaSync({ base_url: event.currentTarget.value })}
                        placeholder="qmediasync 服务地址（可选）"
                      />
                      <span className="cloud-route-static">
                        <Cloud size={15} />
                        {"115 -> qmediasync -> STRM -> 本项目缓存/浏览器"}
                      </span>
                    </div>
                    {cloudStatus && (
                      <p className="settings-hint">
                        云缓存 {formatBytes(cloudStatus.cache.bytes)} / {cloudStatus.cache.files} 文件
                      </p>
                    )}
                    <p className="settings-hint">STRM 根目录和挂载名为只读信息；修改请调整容器的 qmediasync 访问目录及 volumes 映射。</p>
                    {draft.qmediasync.strm_roots.length > 0 && (
                      <div className="directory-list readonly-directory cloud-source-list">
                        {draft.qmediasync.strm_roots.map((root) => (
                          <span key={root}>
                            <em>STRM 根目录 · {root}</em>
                            <small>容器挂载</small>
                          </span>
                        ))}
                      </div>
                    )}
                    <div className="directory-list readonly-directory cloud-source-list">
                      {draft.media_sources.map((source) => (
                        <span key={`${source.provider}-${source.kind}-${source.mount_name}-${source.root}`}>
                          <em>{cloudKindLabels[source.kind]} · {source.mount_name}:{source.root} · 深度 {source.scan_depth}</em>
                          <small>容器源</small>
                        </span>
                      ))}
                    </div>
                    {cloudStatus?.qmediasync.source_details.length ? (
                      <div className="source-health-list">
                        {cloudStatus.qmediasync.source_details.map((source) => (
                          <div className="source-health-row" key={`${source.kind}-${source.mount_name}-${source.root}`}>
                            <span className={`source-health-dot ${source.readable ? "ready" : "failed"}`} aria-hidden="true" />
                            <div>
                              <b>{cloudKindLabels[source.kind as keyof typeof cloudKindLabels] ?? source.kind} · {source.mount_name ?? "qmediasync"}</b>
                              <small>{source.root} · {source.source} · 深度 {source.scan_depth} · {source.readable ? `发现 ${source.discovered} 个候选` : "不可读"}</small>
                            </div>
                            <em>{source.readable ? "可读" : "失败"}</em>
                          </div>
                        ))}
                      </div>
                    ) : null}
                    {latestScanResult ? (
                      <div className="scan-source-results">
                        <p className="settings-hint">最近一次扫描：{latestScanResult.status}</p>
                        {latestScanResult.source_results.map((source) => (
                          <div className="scan-source-result" key={`${source.kind}-${source.root}`}>
                            <b>{cloudKindLabels[source.kind as keyof typeof cloudKindLabels] ?? source.kind}</b>
                            <span>{source.root}</span>
                            <em>{source.status} · 发现 {source.discovered} · 导入 {source.imported} · 跳过 {source.skipped} · 失败 {source.failed}</em>
                            {source.message ? <small>{source.message}</small> : null}
                          </div>
                        ))}
                      </div>
                    ) : null}
                  </div>
                </section>
              )}

              {draft && (
                <section className="settings-section">
                  <h3>扫描与索引</h3>
                  <div className="settings-actions">
                    <button className="primary-action" disabled={busy} onClick={() => onSaveSettings({ ...draft, scan: { ...draft.scan, enqueue_enrichment: false } })}>
                      <Settings size={16} />
                      <span>保存设置</span>
                    </button>
                    <button className="primary-action" disabled={busy} onClick={onRescan}>
                      {busy ? <Loader2 className="spin" size={16} /> : <RefreshCw size={16} />}
                      <span>重新扫描并重建索引</span>
                    </button>
                    <button className="icon-btn" disabled={busy} onClick={onTagImport} aria-label="导入标签翻译">
                      <Tags size={17} />
                    </button>
                  </div>
                </section>
              )}
          </>
        </div>
      </motion.article>
    </motion.div>
  );
}

function TagDetailPanel({
  language,
  tag,
  onClose
}: {
  language: TagLanguage;
  tag: Tag;
  onClose: () => void;
}) {
  return (
    <motion.section className="tag-detail-panel" initial={{ opacity: 0, y: -6 }} animate={{ opacity: 1, y: 0 }} exit={{ opacity: 0, y: -6 }}>
      <header>
        <span>{tagNamespace(tag, language)}</span>
        <button className="icon-btn compact" onClick={onClose} aria-label="关闭标签详情">
          <X size={14} />
        </button>
      </header>
      <h3>{tagLabel(tag, language)}</h3>
      <div className="tag-detail-grid">
        <span>raw</span>
        <b>{tag.namespace}:{tag.key}</b>
        {tag.translated_label && (
          <>
            <span>zh</span>
            <b>{tag.translated_namespace ?? tag.namespace}:{tag.translated_label}</b>
          </>
        )}
        <span>source</span>
        <b>{tag.source}</b>
        <span>count</span>
        <b>{tag.count}</b>
      </div>
      {tag.intro && <p>{tag.intro}</p>}
      {tag.links && <p>{tag.links}</p>}
    </motion.section>
  );
}

type VirtualShelfProps<T> = {
  items: T[];
  itemKey: (item: T) => string | number;
  renderItem: (item: T, index: number) => ReactNode;
  contentKind?: LibraryContentKind;
  viewMode: ViewMode;
};

function VirtualShelf<T>({ items, itemKey, renderItem, contentKind = "portrait", viewMode }: VirtualShelfProps<T>) {
  const ref = useRef<HTMLElement | null>(null);
  const [scrollTop, setScrollTop] = useState(0);
  const [viewport, setViewport] = useState({ width: 0, height: 0 });
  const layout = getLibraryLayout(viewMode, viewport.width, contentKind);
  const { columns, gap, rowHeight } = layout;
  const rowCount = Math.ceil(items.length / columns);
  const overscanRows = 4;
  const startRow = Math.max(0, Math.floor(scrollTop / rowHeight) - overscanRows);
  const endRow = Math.min(rowCount, Math.ceil((scrollTop + viewport.height) / rowHeight) + overscanRows);
  const startIndex = startRow * columns;
  const endIndex = Math.min(items.length, endRow * columns);
  const visibleItems = items.slice(startIndex, endIndex);
  const totalHeight = Math.max(0, rowCount * rowHeight - gap);
  const offsetY = startRow * rowHeight;
  const previousLayoutRef = useRef<typeof layout | null>(null);

  useLayoutEffect(() => {
    const node = ref.current;
    const previous = previousLayoutRef.current;
    previousLayoutRef.current = layout;
    if (!node || !previous || items.length === 0) return;
    if (previous.columns === layout.columns && previous.rowHeight === layout.rowHeight) return;
    const anchorIndex = Math.min(
      Math.max(0, items.length - 1),
      Math.floor(node.scrollTop / previous.rowHeight) * previous.columns
    );
    const rowOffset = node.scrollTop % previous.rowHeight;
    const nextScrollTop = Math.max(
      0,
      Math.min(
        Math.max(0, totalHeight - viewport.height),
        Math.floor(anchorIndex / layout.columns) * layout.rowHeight + Math.min(rowOffset, layout.rowHeight - 1)
      )
    );
    if (Math.abs(node.scrollTop - nextScrollTop) > 1) {
      node.scrollTop = nextScrollTop;
      setScrollTop(nextScrollTop);
    }
  }, [items.length, layout, totalHeight, viewport.height]);

  useEffect(() => {
    const node = ref.current;
    if (!node) return;
    const update = () => {
      const rect = node.getBoundingClientRect();
      const parentRect = node.parentElement?.getBoundingClientRect();
      const availableHeight = parentRect ? Math.max(120, parentRect.bottom - rect.top) : node.clientHeight;
      const next = {
        width: node.clientWidth || Math.round(rect.width),
        height: Math.round(availableHeight || Math.min(window.innerHeight * 0.72, 720))
      };
      setViewport((current) => current.width === next.width && current.height === next.height ? current : next);
    };
    return observeElementResize(node, update);
  }, []);

  useEffect(() => {
    const node = ref.current;
    if (!node || totalHeight === 0) return;
    if (node.scrollTop > totalHeight) {
      node.scrollTop = 0;
      setScrollTop(0);
    }
  }, [items.length, totalHeight, viewMode]);

  const shelfStyle = {
    "--library-columns": columns,
    "--library-gap": `${gap}px`,
    "--library-item-height": `${Math.max(80, layout.cardHeight)}px`,
    "--library-column-width": `${layout.columnWidth}px`,
    "--library-cover-ratio": layout.coverRatio,
    "--library-text-reserve": `${layout.textReserve}px`,
    height: items.length > 0 && totalHeight > 0 && viewport.height > 0 ? `${Math.min(totalHeight, viewport.height)}px` : undefined,
  } as CSSProperties;

  return (
    <section
      className="virtual-shelf"
      data-view={viewMode}
      ref={ref}
      style={shelfStyle}
      onScroll={(event) => setScrollTop(event.currentTarget.scrollTop)}
    >
      {items.length === 0 ? (
        <motion.div className="empty-shelf" initial={{ opacity: 0 }} animate={{ opacity: 1 }}>
          <Sparkles size={24} />
        </motion.div>
      ) : (
        <div className="virtual-shelf-spacer" style={{ height: totalHeight }}>
          <div className="virtual-shelf-window" style={{ transform: `translateY(${offsetY}px)` }}>
            {visibleItems.map((item, localIndex) => (
              <div className="virtual-shelf-cell" key={itemKey(item)}>
                {renderItem(item, startIndex + localIndex)}
              </div>
            ))}
          </div>
        </div>
      )}
    </section>
  );
}

function CoverImage({ kind, loading = "lazy", src }: { kind: string; loading?: "eager" | "lazy"; src: string }) {
  const [failed, setFailed] = useState(false);
  const [loadedSrc, setLoadedSrc] = useState<string | null>(null);
  const [attempt, setAttempt] = useState(0);
  const imageRef = useRef<HTMLImageElement | null>(null);

  useEffect(() => {
    setFailed(false);
    setLoadedSrc(null);
    setAttempt(0);
  }, [src]);

  if (!src) return <FallbackCover kind={kind} />;
  if (failed) {
    return (
      <div
        className="cover-load-error"
        data-state="error"
        role="button"
        tabIndex={0}
        aria-label="封面加载失败，点击重试"
        onClick={(event) => {
          event.stopPropagation();
          setFailed(false);
          setLoadedSrc(null);
          setAttempt((value) => value + 1);
        }}
        onKeyDown={(event) => {
          if (event.key === "Enter" || event.key === " ") {
            event.preventDefault();
            event.stopPropagation();
            setFailed(false);
            setLoadedSrc(null);
            setAttempt((value) => value + 1);
          }
        }}
      >
        <span>封面暂时无法加载</span>
        <small>点击重试</small>
      </div>
    );
  }
  const requestSrc = attempt > 0
    ? `${src}${src.includes("?") ? "&" : "?"}retry=${attempt}`
    : src;
  return (
    <img
      ref={(node) => {
        imageRef.current = node;
        if (node?.complete && node.naturalWidth > 0) setLoadedSrc(src);
      }}
      src={requestSrc}
      alt=""
      loading={loading}
      data-loaded={loadedSrc === src}
      onLoad={() => setLoadedSrc(src)}
      onError={() => setFailed(true)}
    />
  );
}

function WorkCard({ work, selected, viewMode, onClick }: { work: WorkSummary; selected: boolean; viewMode: ViewMode; onClick: () => void }) {
  const meta = parseMeta<{ series?: string; page_count?: number; volume_count?: number; first_work_id?: number; image_count?: number }>(work.meta_json);
  const cover = workCoverUrl(work);
  const badge = isArchiveWorkKind(work.kind) && meta.page_count
    ? `${meta.page_count}p`
    : work.kind === "comic-collection"
      ? `${meta.volume_count ?? work.asset_count}本`
      : work.kind === "novel-collection"
        ? `${meta.volume_count ?? work.asset_count}卷`
        : work.kind === "coser-picture-collection"
          ? `${meta.volume_count ?? work.asset_count}套`
          : work.kind === "gallery" && meta.image_count ? `${meta.image_count}图` : null;
  return (
    <button
      type="button"
      className={selected ? "work-card selected" : "work-card"}
      data-view={viewMode}
      onClick={onClick}
    >
      <div className="cover">
        {cover ? <CoverImage kind={work.kind} src={cover} /> : <FallbackCover kind={work.kind} />}
      </div>
      <div className="work-copy">
        <div className="work-kicker">
          {kindIcon[work.kind] ?? <Bookmark size={14} />}
          <span>{kindLabels[work.kind] ?? work.kind}</span>
          {badge ? <b>{badge}</b> : null}
        </div>
        <h3>{work.title}</h3>
        <p>{String(meta.series ?? work.category ?? work.source_path ?? "")}</p>
        <div className="meter">
          <span style={{ width: `${Math.min(100, Math.max(0, work.progress * 100))}%` }} />
        </div>
      </div>
    </button>
  );
}

function DetailPane({
  detail,
  jobs,
  tagLanguage,
  variant,
  onClose,
  onTagPick,
  onPlayTrack,
  onOpenReader
}: {
  detail: WorkDetail | null;
  jobs: Job[];
  tagLanguage: TagLanguage;
  variant: "modal" | "docked";
  onClose: () => void;
  onTagPick: (key: string) => void;
  onPlayTrack: (work: WorkDetail["work"], asset: Asset, playlist?: Asset[], playlistTotal?: number) => void;
  onOpenReader: (resume?: boolean) => void;
}) {
  const [jobOpen, setJobOpen] = useState(false);
  const tracks = detail?.assets.filter((asset) => asset.role === "track" || asset.mime.startsWith("audio/")) ?? [];
  const displayTracks = preferredTrackVariants(tracks);
  const generatedImages = detail?.assets.filter((asset) => ["generated", "image"].includes(asset.role) && asset.mime.startsWith("image/")) ?? [];
  const routeAsset = detail?.assets.find((asset) => asset.path.startsWith("qms-strm://")) ?? null;
  const [routeInfo, setRouteInfo] = useState<AssetRouteInfo | null>(null);
  const [routeError, setRouteError] = useState<string | null>(null);
  const meta = parseMeta<Record<string, unknown>>(detail?.work.meta_json);
  const detailCover = detail ? workCoverUrl(detail.work) : "";
  const canOpenReader = detail ? ["comic", "coser-picture", "novel", "audio", "generated", "gallery"].includes(detail.work.kind) : false;
  const hasReadableProgress = Boolean(detail && detail.work.progress > 0.01 && detail.work.progress < 0.995 && canOpenReader);
  const openLabel = detail?.work.kind === "audio" ? "文件" : detail?.work.kind === "gallery" || detail?.work.kind === "generated" ? "预览" : "阅读";
  const groupedTags = useMemo(() => groupDetailTags(detail?.tags ?? [], tagLanguage), [detail?.tags, tagLanguage]);

  useEffect(() => {
    setRouteInfo(null);
    setRouteError(null);
    if (!routeAsset) return;
    const controller = new AbortController();
    api
      .assetRoute(routeAsset.id, controller.signal)
      .then((info) => {
        if (!controller.signal.aborted) setRouteInfo(info);
      })
      .catch((err) => {
        if (!controller.signal.aborted) setRouteError(err instanceof Error ? err.message : String(err));
      });
    return () => controller.abort();
  }, [routeAsset?.id]);

  const detailClassName = variant === "modal" ? "detail-pane detail-pane-modal" : "detail-pane";
  const detailContent = (
      <AnimatePresence>
        {detail ? (
          <motion.div key={detail.work.id} initial={{ opacity: 0, x: 24 }} animate={{ opacity: 1, x: 0 }} exit={{ opacity: 0, x: 12 }} className="detail-content">
            {variant === "modal" && (
              <button className="icon-btn detail-close" type="button" onClick={onClose} aria-label="关闭预览">
                <X size={18} />
              </button>
            )}
            <div className="detail-hero">
              <div className="detail-cover">
                {detailCover ? <CoverImage kind={detail.work.kind} loading="eager" src={detailCover} /> : <FallbackCover kind={detail.work.kind} />}
              </div>
              <div className="detail-summary">
                <div className="detail-title">
                  <span>{kindLabels[detail.work.kind] ?? detail.work.kind}</span>
                  <h2>{detail.work.title}</h2>
                  <p>{String(meta.series ?? meta.creator ?? detail.work.category ?? "")}</p>
                </div>
                <div className="detail-progress">
                  <span>阅读进度</span>
                  <b>{Math.round((detail.work.progress || 0) * 100)}%</b>
                  <i style={{ width: `${Math.round((detail.work.progress || 0) * 100)}%` }} />
                </div>
                <div className="quick-actions">
                  {hasReadableProgress && (
                    <button className="continue-action" onClick={() => onOpenReader(true)}>
                      <BookOpen size={16} />
                      <span>继续阅读</span>
                    </button>
                  )}
                  <button onClick={canOpenReader ? () => onOpenReader(false) : undefined} disabled={!canOpenReader}>
                    <BookOpen size={16} />
                    <span>{hasReadableProgress && detail.work.kind !== "gallery" && detail.work.kind !== "generated" ? "从头阅读" : openLabel}</span>
                  </button>
                </div>
                {routeAsset && (
                  <div className="route-card">
                    <div>
                      <Cloud size={15} />
                      <span>链路</span>
                      {routeInfo && <b>{routePolicyLabel(routeInfo.policy)}</b>}
                    </div>
                    {routeInfo ? (
                      <>
                        <strong>{routeLabel(routeInfo.route_label)}</strong>
                        <small>{routeInfo.target_host ? `目标 ${routeInfo.target_host}` : routeTransferLabel(routeInfo.transfer)}</small>
                        {routeInfo.note && <small>{routeNoteLabel(routeInfo.note)}</small>}
                      </>
                    ) : (
                      <strong>{routeError ? "链路不可用" : "正在确认链路"}</strong>
                    )}
                  </div>
                )}
              </div>
            </div>
            <div className="tag-cloud tag-group-list">
              <div className="tag-group-names">
                {groupedTags.map((group) => (
                  <span className="tag-group-name" key={group.namespace}>{group.namespace}</span>
                ))}
              </div>
              <div className="tag-group-values">
                {groupedTags.flatMap((group) => group.tags).map((tag) => (
                  <button key={tagKey(tag)} onClick={() => onTagPick(tagKey(tag))}>
                    {tagLabel(tag, tagLanguage)}
                  </button>
                ))}
              </div>
            </div>
            {detail.work.description && <p className="description">{detail.work.description}</p>}
            {displayTracks.length > 0 && (
              <div className="track-stack">
                {displayTracks.slice(0, 10).map((track) => {
                  const trackMeta = parseMeta<{ title?: string; quality?: string }>(track.meta_json);
                  return (
                    <div className="track-line" key={track.id}>
                      <button
                        className="inline-play"
                        onClick={() => detail && onPlayTrack(detail.work, track, displayTracks, detail.track_count ?? displayTracks.length)}
                        aria-label="播放"
                      >
                        <Play size={14} />
                      </button>
                      <span>{trackMeta.title ?? shortName(track.path)}</span>
                      <b>{trackMeta.quality ?? track.variant}</b>
                    </div>
                  );
                })}
                {(detail.track_count ?? displayTracks.length) > displayTracks.length && (
                  <small className="track-more-hint">
                    已加载 {displayTracks.length} / {detail.track_count} 首，播放列表会按需继续加载
                  </small>
                )}
              </div>
            )}
            {generatedImages.length > 0 && (
              <div className="generated-stack">
                {generatedImages.slice(0, 12).map((asset) => {
                  const assetMeta = parseMeta<{ prompt?: string; style?: string; model?: string }>(asset.meta_json);
                  return (
                    <a href={assetUrl(asset.id, assetVersion(asset, detail.work.updated_at))} target="_blank" rel="noreferrer" key={asset.id}>
                      <img src={thumbUrl(asset.id, 360, assetVersion(asset, detail.work.updated_at))} alt="" loading="lazy" />
                      <span>{assetMeta.style ?? assetMeta.model ?? shortName(asset.path)}</span>
                    </a>
                  );
                })}
              </div>
            )}
            <button className="jobs-toggle" onClick={() => setJobOpen((value) => !value)}>
              <Gauge size={16} />
              <span>队列</span>
              <b>{jobs.filter((job) => job.status !== "done").length}</b>
            </button>
            <AnimatePresence>
              {jobOpen && (
                <motion.div className="job-list" initial={{ opacity: 0, height: 0 }} animate={{ opacity: 1, height: "auto" }} exit={{ opacity: 0, height: 0 }}>
                  {jobs.map((job) => (
                    <div className="job-line" key={job.id} data-status={job.status}>
                      <span>{jobLabel(job.job_type)}</span>
                      <b>{statusLabel(job.status)}</b>
                    </div>
                  ))}
                </motion.div>
              )}
            </AnimatePresence>
          </motion.div>
        ) : (
          <motion.div className="empty-detail" initial={{ opacity: 0 }} animate={{ opacity: 1 }}>
            <Sparkles />
          </motion.div>
        )}
      </AnimatePresence>
  );

  return (
    <aside className={detailClassName} onClick={(event) => event.stopPropagation()}>
      {detailContent}
    </aside>
  );
}

function workCoverUrl(work: { id: number; kind: string; cover_asset_id?: number | null; meta_json: string; updated_at?: string }) {
  if (work.kind === "comic-collection" || work.kind === "novel-collection" || work.kind === "coser-picture-collection") {
    const meta = parseMeta<{ first_work_id?: number }>(work.meta_json);
    if (meta.first_work_id) return coverUrl(meta.first_work_id, 480, work.updated_at);
    return work.cover_asset_id ? assetUrl(work.cover_asset_id, work.updated_at) : "";
  }
  if (work.cover_asset_id || isArchiveWorkKind(work.kind)) {
    return coverUrl(work.id, 480, work.updated_at);
  }
  return "";
}

function AudioDock({
  active,
  canPersistProgress,
  onClose,
  onProgressSaved,
  resumePosition
}: {
  active: ActiveAudioState | null;
  canPersistProgress: boolean;
  onClose: () => void;
  onProgressSaved: (id: number, progress: number, position?: string | null) => void;
  resumePosition?: string | null;
}) {
  const audioRef = useRef<HTMLAudioElement | null>(null);
  const lastProgressWrite = useRef(0);
  const resumeTrackRef = useRef(parseReadingPosition(resumePosition));
  const resumeConsumedRef = useRef(false);
  const [currentAsset, setCurrentAsset] = useState<Asset | null>(active?.asset ?? null);
  const [currentTime, setCurrentTime] = useState(0);
  const [duration, setDuration] = useState(0);
  const [isPlaying, setIsPlaying] = useState(false);
  const [repeatMode, setRepeatMode] = useState<AudioRepeatMode>("none");
  const [queueOpen, setQueueOpen] = useState(false);
  const [volume, setVolume] = useState(1);
  const initialPlaylistState = useMemo<AudioPlaylistState>(() => {
    const seed = active?.playlist ?? (active?.asset ? [active.asset] : []);
    return {
      items: seed,
      nextCursor: null,
      hasMore: Boolean(active && (active.playlistTotal ?? seed.length) > seed.length),
      startIndex: 0,
      nextPageStartIndex: seed.length,
      pageCursors: [{ startIndex: 0, cursor: null }],
      total: Math.max(active?.playlistTotal ?? seed.length, seed.length)
    };
  }, [active]);
  const [playlistState, setPlaylistState] = useState<AudioPlaylistState>(initialPlaylistState);
  const playlistStateRef = useRef<AudioPlaylistState>(initialPlaylistState);
  const playlistLoadingRef = useRef(false);
  const playlistRequestRef = useRef<AbortController | null>(null);
  const playlistGenerationRef = useRef(0);
  const [playlistLoading, setPlaylistLoading] = useState(false);
  const meta = parseMeta<{ title?: string }>(currentAsset?.meta_json);
  const playlist = playlistState.items;
  const currentIndex = Math.max(0, playlist.findIndex((asset) => asset.id === currentAsset?.id));
  const absoluteCurrentIndex = playlistState.startIndex + currentIndex;
  const progressPercent = duration > 0 ? Math.min(100, Math.max(0, (currentTime / duration) * 100)) : 0;
  const repeatLabel = repeatMode === "one" ? "单曲循环" : repeatMode === "all" ? "列表循环" : "不循环";
  const { flush: flushProgress, schedule: scheduleProgress } = useProgressQueue(
    active?.work.id ?? 0,
    canPersistProgress && Boolean(active),
    onProgressSaved,
    1200
  );

  const commitPlaylistState = useCallback((next: AudioPlaylistState) => {
    playlistStateRef.current = next;
    setPlaylistState(next);
  }, []);

  const loadTrackPage = useCallback(async (
    cursor: string | null,
    replace = false,
    direction: "append" | "prepend" = "append",
    requestedStartIndex?: number
  ) => {
    if (!active || playlistLoadingRef.current) return false;
    playlistLoadingRef.current = true;
    setPlaylistLoading(true);
    const generation = playlistGenerationRef.current;
    const controller = new AbortController();
    playlistRequestRef.current = controller;
    try {
      const response = await api.workAssets(active.work.id, {
        role: "track",
        cursor,
        limit: AUDIO_TRACK_PAGE_SIZE,
        signal: controller.signal
      });
      if (controller.signal.aborted || generation !== playlistGenerationRef.current) return false;
      const fetched = response.items.map(catalogAssetToAsset);
      const current = playlistStateRef.current;
      const next = mergeAudioTrackPage({
        current,
        fetched,
        pageCursor: cursor,
        responseNextCursor: response.next_cursor ?? null,
        responseTotal: response.total,
        activeAsset: active.asset,
        currentAssetId: currentAsset?.id,
        replace,
        direction,
        requestedStartIndex,
        maxCached: AUDIO_TRACK_MAX_CACHED
      });
      commitPlaylistState({
        ...next,
        total: Math.max(next.total, active.playlistTotal ?? 0)
      });
      return true;
    } catch {
      if (controller.signal.aborted || generation !== playlistGenerationRef.current) return false;
      // A failed prefetch must not stop the current track.  The next/queue
      // action can retry the same cursor.
      return false;
    } finally {
      if (playlistRequestRef.current === controller) {
        playlistRequestRef.current = null;
        playlistLoadingRef.current = false;
        setPlaylistLoading(false);
      }
    }
  }, [active, commitPlaylistState, currentAsset?.id]);

  const ensureNextTrackPage = useCallback(async () => {
    const current = playlistStateRef.current;
    if (!current.hasMore) return false;
    // The initial detail payload is a seed and does not carry its opaque
    // cursor.  Re-read page zero once to obtain a stable cursor before
    // advancing beyond it.
    return loadTrackPage(current.nextCursor, current.nextCursor === null);
  }, [loadTrackPage]);

  const ensurePreviousTrackPage = useCallback(async () => {
    const current = playlistStateRef.current;
    if (current.startIndex <= 0) return false;
    const targetStart = Math.max(0, current.startIndex - AUDIO_TRACK_PAGE_SIZE);
    const page = current.pageCursors
      .filter((candidate) => candidate.startIndex < current.startIndex)
      .sort((left, right) => right.startIndex - left.startIndex)[0];
    if (!page) return false;
    return loadTrackPage(page.cursor, false, "prepend", page.startIndex ?? targetStart);
  }, [loadTrackPage]);

  useEffect(() => {
    playlistGenerationRef.current += 1;
    playlistRequestRef.current?.abort();
    playlistRequestRef.current = null;
    playlistStateRef.current = initialPlaylistState;
    setPlaylistState(initialPlaylistState);
    playlistLoadingRef.current = false;
    setPlaylistLoading(false);
    if (
      active &&
      initialPlaylistState.hasMore
    ) {
      void loadTrackPage(null, true);
    }
    return () => {
      playlistGenerationRef.current += 1;
      playlistRequestRef.current?.abort();
      playlistRequestRef.current = null;
      playlistLoadingRef.current = false;
    };
  // The queue is reset only for a new playback session.  Track changes inside
  // the dock must not re-seed or refetch the queue.
  }, [active?.sessionId]);

  useEffect(() => {
    setCurrentAsset(active?.asset ?? null);
    lastProgressWrite.current = 0;
  }, [active?.asset.id]);

  useEffect(() => {
    setCurrentTime(0);
    setDuration(0);
    setIsPlaying(false);
  }, [currentAsset?.id]);

  useEffect(() => {
    if (audioRef.current) {
      audioRef.current.volume = volume;
    }
  }, [volume]);

  const saveAudioProgress = useCallback((currentTime: number, duration: number, ended = false, force = false) => {
    if (!canPersistProgress || !active || !currentAsset || !Number.isFinite(duration) || duration <= 0) return;
    const now = Date.now();
    if (!ended && !force && now - lastProgressWrite.current < 12000) return;
    lastProgressWrite.current = now;
    const progress = ended ? 1 : Math.min(0.995, Math.max(0, currentTime / duration));
    const position = `track:${currentAsset.id}:${Math.round(currentTime)}`;
    scheduleProgress(progress, position, ended || force);
  }, [active?.work.id, canPersistProgress, currentAsset, scheduleProgress]);

  useLayoutEffect(() => {
    return () => {
      const audio = audioRef.current;
      if (audio) {
        const audioDuration = audio.duration;
        const ended = audio.ended || (Number.isFinite(audioDuration) && audioDuration > 0 && audio.currentTime >= audioDuration - 0.25);
        saveAudioProgress(audio.currentTime, audioDuration, ended, true);
      }
      void flushProgress();
    };
  }, [currentAsset?.id, flushProgress, saveAudioProgress]);

  const changeTrack = async (offset: number, flushCurrent = true) => {
    if (playlistStateRef.current.items.length === 0) return false;
    const audio = audioRef.current;
    if (flushCurrent && audio) {
      saveAudioProgress(audio.currentTime, audio.duration || duration, audio.ended, true);
      void flushProgress();
    }
    let state = playlistStateRef.current;
    let currentLocalIndex = state.items.findIndex((asset) => asset.id === currentAsset?.id);
    if (currentLocalIndex < 0) currentLocalIndex = 0;
    let nextIndex = currentLocalIndex + offset;
    if (offset > 0 && nextIndex >= state.items.length && state.hasMore) {
      const loaded = await ensureNextTrackPage();
      if (loaded) {
        state = playlistStateRef.current;
        currentLocalIndex = state.items.findIndex((asset) => asset.id === currentAsset?.id);
        if (currentLocalIndex < 0) currentLocalIndex = 0;
        nextIndex = currentLocalIndex + offset;
      }
    }
    if (offset < 0 && nextIndex < 0 && state.startIndex > 0) {
      const loaded = await ensurePreviousTrackPage();
      if (loaded) {
        state = playlistStateRef.current;
        currentLocalIndex = state.items.findIndex((asset) => asset.id === currentAsset?.id);
        if (currentLocalIndex < 0) currentLocalIndex = 0;
        nextIndex = currentLocalIndex + offset;
      }
    }
    if (nextIndex < 0 || nextIndex >= state.items.length) {
      if (nextIndex < 0 && state.startIndex > 0) return false;
      if (nextIndex >= state.items.length && state.hasMore) return false;
      if (repeatMode !== "all" || state.items.length === 0) return false;
      setCurrentAsset(state.items[(nextIndex + state.items.length) % state.items.length]);
      lastProgressWrite.current = 0;
      return true;
    }
    setCurrentAsset(state.items[nextIndex]);
    lastProgressWrite.current = 0;
    return true;
  };

  const togglePlayback = () => {
    const audio = audioRef.current;
    if (!audio) return;
    if (audio.paused) {
      void audio.play().catch(() => setIsPlaying(false));
    } else {
      audio.pause();
    }
  };

  const seekTo = (nextTime: number) => {
    const audio = audioRef.current;
    if (!audio || !Number.isFinite(nextTime)) return;
    const safeTime = Math.min(Math.max(0, nextTime), Math.max(0, audio.duration || duration || 0));
    audio.currentTime = safeTime;
    setCurrentTime(safeTime);
    saveAudioProgress(safeTime, audio.duration || duration, false, true);
  };

  const cycleRepeatMode = () => {
    setRepeatMode((value) => (value === "none" ? "all" : value === "all" ? "one" : "none"));
  };

  const handleClose = () => {
    const audio = audioRef.current;
    if (audio) {
      saveAudioProgress(audio.currentTime, audio.duration || duration, audio.ended, true);
    }
    void flushProgress();
    onClose();
  };

  if (!active || !currentAsset) return null;

  const queueStartIndex = Math.max(0, currentIndex - AUDIO_QUEUE_WINDOW);
  const queueEndIndex = Math.min(playlist.length, currentIndex + AUDIO_QUEUE_WINDOW + 1);
  const queueItems = playlist.slice(queueStartIndex, queueEndIndex);

  const audioContent = (
    <>
      <button className="icon-btn compact audio-queue-toggle" onClick={() => setQueueOpen((value) => !value)} aria-label="播放列表">
        <ListMusic size={15} />
      </button>
      <span className="audio-title">{meta.title ?? shortName(currentAsset.path)}</span>
      <div className="audio-controls">
        <button className="icon-btn compact" onClick={() => void changeTrack(-1)} disabled={playlist.length < 2 && repeatMode !== "all"} aria-label="上一首">
          <SkipBack size={15} />
        </button>
        <button
          className="icon-btn compact play-toggle"
          onClick={togglePlayback}
          aria-label={isPlaying ? "暂停" : "播放"}
          title={isPlaying ? "暂停" : "播放"}
        >
          {isPlaying ? <Pause size={15} /> : <Play size={15} />}
        </button>
        <button className="icon-btn compact" onClick={() => void changeTrack(1)} disabled={playlist.length < 2 && repeatMode !== "all" || playlistLoading} aria-label="下一首">
          <SkipForward size={15} />
        </button>
        <button
          className={repeatMode === "none" ? "icon-btn compact" : "icon-btn compact active"}
          onClick={cycleRepeatMode}
          aria-label="循环模式"
          title={`循环：${repeatLabel}`}
        >
          {repeatMode === "one" ? <Repeat1 size={15} /> : <Repeat size={15} />}
        </button>
      </div>
      <div className="audio-progress" style={{ "--audio-progress": `${progressPercent}%` } as CSSProperties}>
        <input
          aria-label="播放进度"
          disabled={duration <= 0}
          max={duration > 0 ? duration : 1}
          min={0}
          onChange={(event) => seekTo(Number(event.currentTarget.value))}
          step={0.1}
          type="range"
          value={duration > 0 ? Math.min(currentTime, duration) : 0}
        />
        <div className="audio-time">
          <span>{formatAudioTime(currentTime)}</span>
          <span>{formatAudioTime(duration)}</span>
        </div>
      </div>
      <label className="audio-volume" style={{ "--audio-volume": `${Math.round(volume * 100)}%` } as CSSProperties}>
        <Volume2 size={15} />
        <input
          aria-label="音量"
          max={1}
          min={0}
          onChange={(event) => setVolume(Number(event.currentTarget.value))}
          step={0.01}
          type="range"
          value={volume}
        />
      </label>
      <audio
        className="audio-engine"
        ref={audioRef}
        autoPlay
        loop={repeatMode === "one"}
        preload="metadata"
        src={assetUrl(currentAsset.id, assetVersion(currentAsset, active.work.updated_at))}
        onLoadedMetadata={(event) => {
          let nextTime = event.currentTarget.currentTime;
          if (!resumeConsumedRef.current) {
            resumeConsumedRef.current = true;
            const resumeTrack = resumeTrackRef.current;
            if (resumeTrack.kind === "track" && resumeTrack.assetId === currentAsset.id && resumeTrack.seconds > 0) {
              const maxResumeTime = Number.isFinite(event.currentTarget.duration)
                ? Math.max(0, event.currentTarget.duration - 0.5)
                : resumeTrack.seconds;
              nextTime = Math.min(resumeTrack.seconds, maxResumeTime);
              event.currentTarget.currentTime = nextTime;
            }
          }
          setDuration(Number.isFinite(event.currentTarget.duration) ? event.currentTarget.duration : 0);
          setCurrentTime(nextTime);
          event.currentTarget.volume = volume;
        }}
        onDurationChange={(event) => setDuration(Number.isFinite(event.currentTarget.duration) ? event.currentTarget.duration : 0)}
        onEnded={(event) => {
          saveAudioProgress(event.currentTarget.duration, event.currentTarget.duration, true);
          void flushProgress();
          if (repeatMode === "one") return;
          void changeTrack(1, false).then((changed) => {
            if (!changed) onClose();
          });
        }}
        onPause={(event) => {
          setIsPlaying(false);
          saveAudioProgress(
            event.currentTarget.currentTime,
            event.currentTarget.duration,
            event.currentTarget.ended,
            true
          );
          void flushProgress();
        }}
        onPlay={() => setIsPlaying(true)}
        onTimeUpdate={(event) => {
          setCurrentTime(event.currentTarget.currentTime);
          saveAudioProgress(event.currentTarget.currentTime, event.currentTarget.duration);
        }}
      />
      <button className="icon-btn compact audio-close" onClick={handleClose} aria-label="关闭播放器">
        <X size={15} />
      </button>
      <AnimatePresence>
        {queueOpen && (
          <motion.div className="audio-queue" initial={{ opacity: 0, y: 8 }} animate={{ opacity: 1, y: 0 }} exit={{ opacity: 0, y: 8 }}>
            <div className="audio-queue-head">
              <b>播放列表</b>
              <span>{absoluteCurrentIndex + 1}/{playlistState.total}</span>
            </div>
            {queueStartIndex > 0 && <small className="audio-queue-window-hint">已隐藏前方轨道</small>}
            {queueItems.map((asset, index) => {
              const trackMeta = parseMeta<{ title?: string }>(asset.meta_json);
              return (
                <button
                  className={asset.id === currentAsset.id ? "active" : ""}
                  key={asset.id}
                  onClick={() => {
                    const audio = audioRef.current;
                    if (audio) {
                      saveAudioProgress(audio.currentTime, audio.duration || duration, audio.ended, true);
                      void flushProgress();
                    }
                    setCurrentAsset(asset);
                    lastProgressWrite.current = 0;
                  }}
                >
                  <em>{queueStartIndex + index + playlistState.startIndex + 1}</em>
                  <span>{trackMeta.title ?? shortName(asset.path)}</span>
                </button>
              );
            })}
            {queueEndIndex < playlist.length || playlistState.hasMore ? (
              <small className="audio-queue-window-hint">
                {playlistLoading ? "正在加载下一批…" : `已缓存 ${playlist.length} / ${playlistState.total} 首`}
              </small>
            ) : null}
          </motion.div>
        )}
      </AnimatePresence>
    </>
  );

  return (
    <motion.div
      className="audio-dock"
      initial={{ opacity: 0, transform: "translateY(24px)" }}
      animate={{ opacity: 1, transform: "translateY(0)" }}
      exit={{ opacity: 0, transform: "translateY(12px)" }}
      transition={{ duration: uiDuration.fade, ease: uiEaseOut }}
    >
      {audioContent}
    </motion.div>
  );
}

function ReaderOverlay({
  canPersistProgress,
  comicPrefetchPages = 5,
  comicAutoReadIntervalMs = defaultReaderSettings.comic_auto_read_interval_ms,
  detail,
  onClose,
  onPlayTrack,
  onProgressSaved,
  readerDerivativesEnabled,
  resumePosition
}: {
  canPersistProgress: boolean;
  comicPrefetchPages?: number;
  comicAutoReadIntervalMs?: number;
  detail: WorkDetail;
  onClose: () => void;
  onPlayTrack: (work: WorkDetail["work"], asset: Asset, playlist?: Asset[], playlistTotal?: number) => void;
  onProgressSaved: (id: number, progress: number, position?: string | null) => void;
  readerDerivativesEnabled: boolean;
  resumePosition?: string | null;
}) {
  const [pages, setPages] = useState<ComicPageInfo[]>([]);
  const [comicPageCount, setComicPageCount] = useState(0);
  const [page, setPage] = useState(0);
  const [comicMode, setComicMode] = useState<ComicReaderMode>("horizontal");
  const [comicZoom, setComicZoom] = useState(1);
  const [comicViewport, setComicViewport] = useState({ width: 0, height: 0 });
  const [comicScrollLeft, setComicScrollLeft] = useState(0);
  const [comicScrollTop, setComicScrollTop] = useState(0);
  const [comicAutoRead, setComicAutoRead] = useState(false);
  const [comicError, setComicError] = useState<string | null>(null);
  const [readerChromeVisible, setReaderChromeVisible] = useState(false);
  const comicStageRef = useRef<HTMLDivElement | null>(null);
  const readerChromeTimerRef = useRef<number | null>(null);
  const resumeAppliedRef = useRef(false);
  const suppressNextProgressRef = useRef(false);
  const needsComicResumeScrollRef = useRef(false);
  const resumeComicPageRef = useRef(0);
  const comicUserInteractedRef = useRef(false);
  const comicScrollFrameRef = useRef<number | null>(null);
  const comicPendingScrollRef = useRef<{ left: number; top: number; page: number } | null>(null);
  const comicLayoutKeyRef = useRef<string | null>(null);
  const comicPagePreloadsRef = useRef(new Map<string, HTMLImageElement>());
  const [readerAssets, setReaderAssets] = useState<Asset[]>(detail.assets);
  const [strmTask, setStrmTask] = useState<StrmTask | null>(null);
  const [strmPrepareError, setStrmPrepareError] = useState<string | null>(null);
  const [strmPreparing, setStrmPreparing] = useState(false);
  const strmArchive = detail.assets.find((asset) => asset.role === "archive" && asset.path.startsWith("qms-strm://"));
  const mediaImages = detail.assets.filter((asset) => ["generated", "image"].includes(asset.role) && asset.mime.startsWith("image/"));
  const comicArchiveVersion = assetVersion(detail.assets.find((asset) => asset.role === "archive"), detail.work.updated_at);
  const resumeTarget = useMemo(() => parseReadingPosition(resumePosition), [resumePosition]);
  const safeComicAutoReadIntervalMs = clampComicAutoReadIntervalMs(comicAutoReadIntervalMs);
  const { flush: flushProgress, schedule: scheduleProgress } = useProgressQueue(
    detail.work.id,
    canPersistProgress,
    onProgressSaved
  );

  useEffect(() => {
    setReaderAssets(detail.assets);
    setStrmTask(null);
    setStrmPrepareError(null);
  }, [detail.work.id, detail.assets]);

  useEffect(() => {
    if (!strmTask || ["done", "failed", "cancelled"].includes(strmTask.status)) return;
    const controller = new AbortController();
    const poll = async () => {
      try {
        const next = await api.strmTask(strmTask.id, controller.signal);
        if (controller.signal.aborted) return;
        setStrmTask(next);
        if (next.status === "done") {
          const refreshed = await api.work(detail.work.id, controller.signal, "legacy");
          if (!controller.signal.aborted) setReaderAssets(refreshed.assets);
        }
      } catch (error) {
        if (!controller.signal.aborted) setStrmPrepareError(error instanceof Error ? error.message : String(error));
      }
    };
    void poll();
    const timer = window.setInterval(() => void poll(), 2000);
    return () => {
      controller.abort();
      window.clearInterval(timer);
    };
  }, [detail.work.id, strmTask?.id, strmTask?.status]);

  const prepareStrmArchive = async () => {
    if (!strmArchive || strmPreparing) return;
    setStrmPreparing(true);
    setStrmPrepareError(null);
    try {
      const task = await api.prepareStrmAsset(strmArchive.id);
      setStrmTask(task);
    } catch (error) {
      setStrmPrepareError(error instanceof Error ? error.message : String(error));
    } finally {
      setStrmPreparing(false);
    }
  };

  useLayoutEffect(() => {
    return () => {
      void flushProgress();
    };
  }, [detail.work.id, flushProgress]);

  useEffect(() => {
    const body = document.body;
    const root = document.documentElement;
    const previousBodyOverflow = body.style.overflow;
    const previousRootOverflow = root.style.overflow;
    body.classList.add("reader-open");
    root.classList.add("reader-open");
    body.style.overflow = "hidden";
    root.style.overflow = "hidden";
    return () => {
      body.classList.remove("reader-open");
      root.classList.remove("reader-open");
      body.style.overflow = previousBodyOverflow;
      root.style.overflow = previousRootOverflow;
    };
  }, []);

  useEffect(() => {
    setPage(0);
    setPages([]);
    setComicPageCount(0);
    setComicMode("horizontal");
    setComicZoom(1);
    setComicViewport({ width: 0, height: 0 });
    setComicScrollLeft(0);
    setComicScrollTop(0);
    setComicAutoRead(false);
    setComicError(null);
    setReaderChromeVisible(false);
    if (readerChromeTimerRef.current !== null) {
      window.clearTimeout(readerChromeTimerRef.current);
      readerChromeTimerRef.current = null;
    }
    resumeAppliedRef.current = false;
    suppressNextProgressRef.current = false;
    needsComicResumeScrollRef.current = false;
    resumeComicPageRef.current = 0;
    comicUserInteractedRef.current = false;
    comicPendingScrollRef.current = null;
    comicLayoutKeyRef.current = null;
    if (comicScrollFrameRef.current !== null) {
      window.cancelAnimationFrame(comicScrollFrameRef.current);
      comicScrollFrameRef.current = null;
    }
    if (isArchiveWorkKind(detail.work.kind)) {
      const controller = new AbortController();
      api
        .comicPages(detail.work.id, controller.signal, comicArchiveVersion)
        .then((res) => {
          const loaded = res.pages.map(normalizeComicPageInfo);
          // The server now returns a bounded manifest slice plus its total.
          // Keep only the bounded manifest sample.  The total page count is
          // tracked separately, so a 100k-page archive does not allocate a
          // placeholder object for every unloaded index.
          const total = Math.min(COMIC_MAX_PAGE_COUNT, Math.max(res.total ?? loaded.length, loaded.length));
          setPages(loaded.slice(0, total));
          setComicPageCount(total);
          setComicError(null);
        })
        .catch((err) => {
          if (err instanceof DOMException && err.name === "AbortError") return;
          setPages([]);
          setComicPageCount(0);
          setComicError(err instanceof Error ? err.message : String(err));
        });
      return () => controller.abort();
    }
  }, [comicArchiveVersion, detail.work.id, detail.work.kind]);

  useEffect(() => {
    return () => {
      if (comicScrollFrameRef.current !== null) {
        window.cancelAnimationFrame(comicScrollFrameRef.current);
      }
      if (readerChromeTimerRef.current !== null) {
        window.clearTimeout(readerChromeTimerRef.current);
      }
    };
  }, []);

  const isArchiveReader = isArchiveWorkKind(detail.work.kind);
  const isNovel = detail.work.kind === "novel";
  const isGenerated = detail.work.kind === "generated";
  const isGallery = detail.work.kind === "gallery";
  const immersiveReader = isArchiveReader;
  const comicAspect = useMemo(() => comicAspectHint(pages), [pages]);
  const comicFallbackHeight = typeof window === "undefined" ? 720 : Math.max(360, window.innerHeight - 72);
  const comicMeasuredHeight = comicViewport.height > 24 ? comicViewport.height : comicFallbackHeight;
  const comicMeasuredWidth = comicViewport.width > 24 ? comicViewport.width : typeof window === "undefined" ? 960 : window.innerWidth;
  // Reader derivatives are an optimization behind Derivative Cache v2.  The
  // server falls back to the original page while the feature is disabled or
  // a derivative generation attempt fails.  Zooming beyond 1.25x requests
  // the original so the optional downsample never becomes a quality trap.
  const comicReaderSize = comicZoom > 1.25 ? undefined : comicMeasuredWidth >= 1440 ? 1920 : 1280;
  const [loadedComicUrl, setLoadedComicUrl] = useState("");
  const currentComicUrl = comicPageUrl(detail.work.id, page, comicArchiveVersion, comicReaderSize);
  const prefetchCount = Math.max(5, Math.min(10, Math.trunc(comicPrefetchPages) || 5));

  useEffect(() => {
    const cache = comicPagePreloadsRef.current;
    const visibleLoaded = comicStageRef.current?.querySelectorAll("img");
    const currentReady = loadedComicUrl === currentComicUrl || Array.from(visibleLoaded ?? []).some(
      image => image.getAttribute("src") === currentComicUrl && image.complete && image.naturalWidth > 0);
    if (!isArchiveReader || !currentReady || !allowsOriginalPreload()) return;
    let stopped = false;
    let active: HTMLImageElement | undefined;
    const desired = new Set(Array.from({ length: Math.min(prefetchCount, Math.max(0, comicPageCount - page - 1)) },
      (_, offset) => comicPageUrl(detail.work.id, page + offset + 1, comicArchiveVersion, comicReaderSize)));
    // Only retain the current page and the forward window in JS memory.
    for (const [url, image] of cache) {
      if (url === currentComicUrl || desired.has(url)) continue;
      image.removeAttribute("src");
      cache.delete(url);
    }
    void (async () => {
      for (const url of desired) {
        if (stopped) break;
        const existing = cache.get(url);
        if (existing?.complete && existing.naturalWidth > 0) continue;
        const image = new window.Image();
        active = image;
        image.fetchPriority = "low";
        const success = await new Promise<boolean>((resolve) => {
          image.onload = () => resolve(true);
          image.onerror = () => resolve(false);
          image.src = url;
        });
        image.onload = null;
        image.onerror = null;
        if (stopped || !success) break; // Do not hammer a failing/cloud-cooled source.
        cache.set(url, image);
      }
    })();
    return () => {
      stopped = true;
      if (active && !active.complete) {
        active.onerror?.(new Event("error"));
        active.removeAttribute("src");
      }
    };
  }, [comicArchiveVersion, comicPageCount, comicReaderSize, currentComicUrl,
    detail.work.id, isArchiveReader, loadedComicUrl, page, prefetchCount]);

  useEffect(() => () => clearGalleryPreloads(comicPagePreloadsRef.current), []);
  const comicSlotWidth = comicHorizontalSlotWidthFromSize(comicMeasuredWidth, comicMeasuredHeight, comicAspect, comicZoom);
  const comicWindowStart = comicMode === "horizontal" && comicMeasuredWidth > 0
    ? Math.max(0, Math.floor(comicScrollLeft / comicSlotWidth) - COMIC_HORIZONTAL_OVERSCAN)
    : 0;
  const comicWindowEnd = comicMode === "horizontal"
    ? comicMeasuredWidth > 0
      ? Math.min(
        comicPageCount,
        Math.ceil((comicScrollLeft + comicMeasuredWidth) / comicSlotWidth) + COMIC_HORIZONTAL_OVERSCAN
      )
      : Math.min(comicPageCount, COMIC_HORIZONTAL_OVERSCAN * 2 + 1)
    : comicPageCount;
  const horizontalComicIndexes = useMemo(
    () => Array.from({ length: Math.max(0, comicWindowEnd - comicWindowStart) }, (_, index) => comicWindowStart + index),
    [comicWindowEnd, comicWindowStart]
  );
  const comicTotalWidth = comicMode === "horizontal" ? comicSlotWidth * comicPageCount : 0;
  const comicVerticalMetrics = useMemo(() => {
    const horizontalPadding = Math.max(14, comicMeasuredWidth * 0.05) * 2;
    const imageWidth = Math.max(1, comicMeasuredWidth - horizontalPadding) * comicZoom;
    const itemHeight = Math.max(1, imageWidth / comicAspect + 16);
    return { imageWidth, itemHeight, totalHeight: itemHeight * comicPageCount };
  }, [comicAspect, comicMeasuredWidth, comicPageCount, comicZoom]);
  const comicVerticalWindowStart = comicMode === "scroll"
    ? Math.max(
      0,
      comicPageFromVerticalPosition(comicScrollTop, comicVerticalMetrics.itemHeight, comicPageCount) - COMIC_VERTICAL_OVERSCAN
    )
    : 0;
  const comicVerticalWindowEnd = comicMode === "scroll"
    ? Math.min(
      comicPageCount,
      comicPageFromVerticalPosition(
        comicScrollTop + Math.max(1, comicMeasuredHeight),
        comicVerticalMetrics.itemHeight,
        comicPageCount
      ) + COMIC_VERTICAL_OVERSCAN + 1
    )
    : 0;
  const verticalComicIndexes = useMemo(
    () => Array.from(
      { length: Math.max(0, comicVerticalWindowEnd - comicVerticalWindowStart) },
      (_, index) => comicVerticalWindowStart + index
    ),
    [comicVerticalWindowEnd, comicVerticalWindowStart]
  );

  const persistProgress = useCallback((progress: number, position: string, immediate = false) => {
    scheduleProgress(progress, position, immediate);
  }, [scheduleProgress]);

  const closeReader = useCallback(() => {
    void flushProgress();
    onClose();
  }, [flushProgress, onClose]);

  const toggleReaderChrome = () => {
    if (!immersiveReader) return;
    setReaderChromeVisible((visible) => {
      const next = !visible;
      if (readerChromeTimerRef.current !== null) {
        window.clearTimeout(readerChromeTimerRef.current);
        readerChromeTimerRef.current = null;
      }
      if (next) {
        readerChromeTimerRef.current = window.setTimeout(() => {
          setReaderChromeVisible(false);
          readerChromeTimerRef.current = null;
        }, 3200);
      }
      return next;
    });
  };

  useEffect(() => {
    if (!isArchiveReader || comicPageCount === 0 || resumeAppliedRef.current) return;
    let target = resumeTarget.kind === "page"
      ? resumeTarget.index
      : resumeTarget.kind === "start"
        ? 0
        : Math.floor((detail.work.progress || 0) * Math.max(0, comicPageCount - 1));
    target = Math.min(Math.max(target, 0), Math.max(0, comicPageCount - 1));
    setPage(target);
    resumeAppliedRef.current = true;
    suppressNextProgressRef.current = Boolean(resumeTarget.kind && resumeTarget.kind !== "start");
    resumeComicPageRef.current = target;
    comicUserInteractedRef.current = false;
    needsComicResumeScrollRef.current = target > 0;
  }, [comicPageCount, detail.work.progress, isArchiveReader, resumeTarget]);

  useEffect(() => {
    if (!isArchiveReader || !needsComicResumeScrollRef.current || comicPageCount < 2 || comicMode === "paged") return;
    const targetPage = resumeComicPageRef.current;
    const delays = [0, 250, 900, 1800, 3200];
    const timers = delays.map((delay, index) =>
      window.setTimeout(() => {
        if (!needsComicResumeScrollRef.current || comicUserInteractedRef.current) return;
        scrollComicStageToPage(
          comicStageRef.current,
          targetPage,
          comicPageCount,
          comicAspect,
          comicZoom,
          comicVerticalMetrics.itemHeight
        );
        if (index === delays.length - 1) {
          needsComicResumeScrollRef.current = false;
        }
      }, delay)
    );
    return () => timers.forEach((timer) => window.clearTimeout(timer));
  }, [comicAspect, comicMode, comicPageCount, comicVerticalMetrics.itemHeight, comicZoom, isArchiveReader]);

  useEffect(() => {
    if (!isArchiveReader || comicMode === "paged") return;
    const stage = comicStageRef.current;
    if (!stage) return;
    const measure = () => {
      const next = { width: stage.clientWidth, height: stage.clientHeight };
      setComicViewport((current) => current.width === next.width && current.height === next.height ? current : next);
    };
    return observeElementResize(stage, measure);
  }, [comicMode, isArchiveReader]);

  const comicLayoutKey = `${comicMode}:${comicZoom}:${comicMeasuredWidth}:${comicMeasuredHeight}:${comicPageCount}`;
  useLayoutEffect(() => {
    const previous = comicLayoutKeyRef.current;
    comicLayoutKeyRef.current = comicLayoutKey;
    if (
      previous === null ||
      previous === comicLayoutKey ||
      !isArchiveReader ||
      !resumeAppliedRef.current ||
      comicPageCount === 0 ||
      comicMode === "paged"
    ) return;
    const stage = comicStageRef.current;
    if (!stage) return;
    const targetPage = Math.min(Math.max(page, 0), comicPageCount - 1);
    const frame = window.requestAnimationFrame(() => {
      scrollComicStageToPage(
        stage,
        targetPage,
        comicPageCount,
        comicAspect,
        comicZoom,
        comicVerticalMetrics.itemHeight
      );
    });
    return () => window.cancelAnimationFrame(frame);
  }, [comicAspect, comicLayoutKey, comicMode, comicPageCount, comicVerticalMetrics.itemHeight, comicZoom, isArchiveReader]);

  useEffect(() => {
    if (!isArchiveReader || comicPageCount === 0) return;
    if (!resumeAppliedRef.current) return;
    if (suppressNextProgressRef.current) {
      suppressNextProgressRef.current = false;
      return;
    }
    persistProgress((page + 1) / comicPageCount, `page:${page}`);
  }, [comicPageCount, isArchiveReader, page]);

  const moveComic = (offset: number) => {
    setPage((value) => Math.min(Math.max(value + offset, 0), Math.max(0, comicPageCount - 1)));
  };

  const navigateComic = (offset: number, source: "manual" | "auto" = "manual") => {
    if (source === "manual") {
      comicUserInteractedRef.current = true;
      needsComicResumeScrollRef.current = false;
    }
    if (comicMode === "scroll") {
      comicStageRef.current?.scrollBy({
        top: offset * (comicStageRef.current.clientHeight * 0.82),
        behavior: "smooth"
      });
      return;
    }
    if (comicMode === "horizontal") {
      const stage = comicStageRef.current;
      if (!stage) return;
      const targetPage = Math.min(Math.max(page + offset, 0), Math.max(0, comicPageCount - 1));
      stage.scrollTo({
        left: comicHorizontalSlotWidth(stage, comicAspect, comicZoom) * targetPage,
        behavior: "smooth"
      });
      setPage(targetPage);
      return;
    }
    moveComic(offset);
  };

  useEffect(() => {
    if (!isArchiveReader || !comicAutoRead || comicPageCount === 0) return;
    if (page >= comicPageCount - 1) {
      setComicAutoRead(false);
      return;
    }
    const timer = window.setTimeout(() => {
      navigateComic(1, "auto");
    }, safeComicAutoReadIntervalMs);
    return () => window.clearTimeout(timer);
  }, [comicAspect, comicAutoRead, comicMode, comicPageCount, comicZoom, isArchiveReader, page, safeComicAutoReadIntervalMs]);

  const changeComicZoom = (delta: number) => {
    setComicZoom((value) => Math.min(1.8, Math.max(0.7, Number((value + delta).toFixed(2)))));
  };

  const scheduleComicScrollState = (left: number, top: number, nextPage: number) => {
    comicPendingScrollRef.current = { left, top, page: nextPage };
    if (comicScrollFrameRef.current !== null) return;
    comicScrollFrameRef.current = window.requestAnimationFrame(() => {
      comicScrollFrameRef.current = null;
      const pending = comicPendingScrollRef.current;
      comicPendingScrollRef.current = null;
      if (!pending) return;
      setComicScrollLeft((value) => (value === pending.left ? value : pending.left));
      setComicScrollTop((value) => (value === pending.top ? value : pending.top));
      setPage((value) => (value === pending.page ? value : pending.page));
    });
  };

  const onComicScroll = (event: UIEvent<HTMLDivElement>) => {
    if ((comicMode !== "scroll" && comicMode !== "horizontal") || comicPageCount < 2) return;
    const target = event.currentTarget;
    const nextPage = comicMode === "horizontal"
      ? comicPageFromHorizontalScroll(target, comicPageCount, comicAspect, comicZoom)
      : comicPageFromVerticalPosition(
        target.scrollTop + target.clientHeight / 2,
        comicVerticalMetrics.itemHeight,
        comicPageCount
      );
    scheduleComicScrollState(target.scrollLeft, target.scrollTop, nextPage);
  };

  const onHorizontalComicWheel = (event: { preventDefault: () => void; stopPropagation: () => void; deltaY: number; deltaX: number }) => {
    if (!isArchiveReader || comicMode !== "horizontal") return;
    const stage = comicStageRef.current;
    if (!stage) return;
    comicUserInteractedRef.current = true;
    needsComicResumeScrollRef.current = false;
    event.preventDefault();
    event.stopPropagation();
    stage.scrollLeft += event.deltaY + event.deltaX;
    scheduleComicScrollState(
      stage.scrollLeft,
      stage.scrollTop,
      comicPageFromHorizontalScroll(stage, comicPageCount, comicAspect, comicZoom)
    );
  };

  useEffect(() => {
    const onKeyDown = (event: KeyboardEvent) => {
      const target = event.target as HTMLElement | null;
      if (target?.closest("input, textarea, select")) return;
      if (event.key === "Escape") {
        if (isGallery && document.querySelector(".gallery-lightbox")) return;
        closeReader();
        return;
      }
      if (isArchiveReader) {
        if (event.key === "ArrowLeft" || event.key.toLowerCase() === "a") {
          event.preventDefault();
          navigateComic(-1);
        }
        if (event.key === "ArrowRight" || event.key === " " || event.key.toLowerCase() === "d") {
          event.preventDefault();
          navigateComic(1);
        }
        if (event.key === "+" || event.key === "=") changeComicZoom(0.1);
        if (event.key === "-" || event.key === "_") changeComicZoom(-0.1);
      }
    };
    window.addEventListener("keydown", onKeyDown);
    return () => window.removeEventListener("keydown", onKeyDown);
  }, [closeReader, comicAspect, comicMode, comicPageCount, comicZoom, isArchiveReader, isGallery, page]);

  const readerActionsContent = (
    <>
        {isArchiveReader && <b>{comicPageCount ? `${page + 1}/${comicPageCount}` : "0/0"}</b>}
        {isArchiveReader && comicPageCount > 0 && (
          <>
            <button className="icon-btn" onClick={() => navigateComic(-1)} aria-label="上一页">
              <ChevronLeft size={16} />
            </button>
            <button className="icon-btn" onClick={() => navigateComic(1)} aria-label="下一页">
              <ChevronRight size={16} />
            </button>
            <button
              className={comicMode !== "paged" ? "icon-btn active" : "icon-btn"}
              onClick={() => setComicMode((value) => (value === "paged" ? "scroll" : value === "scroll" ? "horizontal" : "paged"))}
              aria-label="切换图片阅读布局"
            >
              {comicMode === "horizontal" ? <GalleryHorizontal size={16} /> : comicMode === "scroll" ? <ListFilter size={16} /> : <BookOpen size={16} />}
            </button>
            <button
              className={comicAutoRead ? "icon-btn active" : "icon-btn"}
              onClick={() => setComicAutoRead((value) => !value)}
              aria-label={comicAutoRead ? "暂停自动阅读" : "自动阅读"}
              title={comicAutoRead ? "暂停自动阅读" : "自动阅读"}
            >
              {comicAutoRead ? <Pause size={16} /> : <Play size={16} />}
            </button>
            <button className="icon-btn" onClick={() => changeComicZoom(-0.1)} aria-label="缩小">
              <ZoomOut size={16} />
            </button>
            <button className="icon-btn" onClick={() => changeComicZoom(0.1)} aria-label="放大">
              <ZoomIn size={16} />
            </button>
          </>
        )}
    </>
  );
  const readerBarContent = (
    <>
      <button className="icon-btn reader-back-button" onClick={closeReader} aria-label="关闭">
        <ChevronLeft size={18} />
      </button>
      <span className="reader-title-pill">{detail.work.title}</span>
      <div className="reader-actions">{readerActionsContent}</div>
    </>
  );
  const readerClassName = [
    "reader",
    isNovel ? "reader-novel" : "",
    immersiveReader ? "reader-immersive" : "",
    immersiveReader ? (readerChromeVisible ? "chrome-visible" : "chrome-hidden") : ""
  ].filter(Boolean).join(" ");
  const readerBarClassName = [
    "reader-bar",
    isArchiveReader || isGallery ? "reader-bar-floating" : "reader-bar-docked",
    isGallery ? "reader-bar-gallery" : ""
  ].filter(Boolean).join(" ");

  return (
    <motion.div className="reader-backdrop" initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }}>
      <motion.div
        className={readerClassName}
        initial={{ scale: 0.98, y: 18 }}
        animate={{ scale: 1, y: 0 }}
        exit={{ scale: 0.98, y: 18 }}
        onWheel={onHorizontalComicWheel}
      >
        {!isNovel && <div className={readerBarClassName}>{readerBarContent}</div>}
        {isArchiveReader ? (
          <div
            className="comic-stage"
            data-mode={comicMode}
            onClick={(event) => {
              if ((event.target as HTMLElement).closest(".reader-bar, button, a")) return;
              toggleReaderChrome();
            }}
            onPointerDown={() => {
              comicUserInteractedRef.current = true;
              needsComicResumeScrollRef.current = false;
            }}
            onScroll={onComicScroll}
            onWheel={(event) => {
              onHorizontalComicWheel(event);
            }}
            ref={comicStageRef}
          >
            {comicError && <div className="reader-error archive-reader-error">{strmArchive ? strmReadErrorMessage(comicError) : comicError}</div>}
            {comicPageCount > 0 && comicMode === "paged" ? (
              <motion.img
                key={page}
                onLoad={(event) => setLoadedComicUrl(event.currentTarget.getAttribute("src") ?? "")}
                src={comicPageUrl(detail.work.id, page, comicArchiveVersion, comicReaderSize)}
                alt=""
                initial={{ opacity: 0 }}
                animate={{ opacity: 1 }}
                style={{ position: "absolute", inset: 0, width: "100%", height: "100%", maxWidth: "100%", maxHeight: "100%", objectFit: "contain" }}
              />
            ) : comicPageCount > 0 ? (
              comicMode === "horizontal" ? (
                <div className="comic-strip-spacer" style={{ width: `${comicTotalWidth}px` }}>
                  <div
                    className="comic-strip-window"
                    style={{ transform: `translateX(${Math.round(comicWindowStart * comicSlotWidth)}px)` }}
                  >
                    {horizontalComicIndexes.map((index) => {
                      return (
                        <div
                          className="comic-page-slot"
                          data-page-index={index}
                          key={`comic-page-${index}`}
                          style={{
                            height: `${Math.round(comicZoom * 100)}%`,
                            width: `${comicSlotWidth}px`
                          } as CSSProperties}
                        >
                          <img
                            alt=""
                            loading="lazy"
                            onLoad={(event) => { if (index === page) setLoadedComicUrl(event.currentTarget.getAttribute("src") ?? ""); }}
                            src={comicPageUrl(detail.work.id, index, comicArchiveVersion, comicReaderSize)}
                          />
                        </div>
                      );
                    })}
                  </div>
                </div>
              ) : (
                <div
                  className="comic-vertical-spacer"
                  style={{ height: `${Math.max(1, comicVerticalMetrics.totalHeight)}px` }}
                >
                  <div
                    className="comic-vertical-window"
                    style={{
                      transform: `translateY(${Math.round(comicVerticalWindowStart * comicVerticalMetrics.itemHeight)}px)`
                    }}
                  >
                    {verticalComicIndexes.map((index) => {
                      const slotHeight = Math.max(1, comicVerticalMetrics.itemHeight - 16);
                      return (
                        <div
                          className="comic-vertical-slot"
                          data-page-index={index}
                          key={`comic-page-${index}`}
                          style={{ height: `${slotHeight}px` }}
                        >
                          <img
                            alt=""
                            loading="lazy"
                            onLoad={(event) => { if (index === page) setLoadedComicUrl(event.currentTarget.getAttribute("src") ?? ""); }}
                            src={comicPageUrl(detail.work.id, index, comicArchiveVersion, comicReaderSize)}
                            style={{
                              height: `${slotHeight}px`,
                              width: `${Math.round(comicZoom * 100)}%`
                            }}
                          />
                        </div>
                      );
                    })}
                  </div>
                </div>
              )
            ) : (
              <Loader2 className="spin" />
            )}
          </div>
        ) : isNovel ? (
          <Suspense fallback={<Loader2 className="spin" />}>
            <NovelReader
              canPersistProgress={canPersistProgress}
              detail={detail}
              onClose={onClose}
              onProgressSaved={onProgressSaved}
              resumePosition={resumePosition}
            />
          </Suspense>
        ) : isGallery ? (
          <GalleryStage
            canPersistProgress={canPersistProgress}
            detail={detail}
            onProgressSaved={onProgressSaved}
            resumeTarget={resumeTarget}
          />
        ) : isGenerated ? (
          <div className="generated-stage">
            {mediaImages.map((asset, index) => {
              const assetMeta = parseMeta<{ prompt?: string; style?: string; model?: string }>(asset.meta_json);
              return (
                <motion.a
                  href={assetUrl(asset.id, assetVersion(asset, detail.work.updated_at))}
                  target="_blank"
                  rel="noreferrer"
                  key={asset.id}
                  data-image-index={index}
                  initial={{ opacity: 0 }}
                  animate={{ opacity: 1 }}
                  transition={{ duration: 0.18 }}
                  onClick={() => persistProgress((index + 1) / Math.max(1, mediaImages.length), `image:${index}`)}
                >
                  <img src={thumbUrl(asset.id, 360, assetVersion(asset, detail.work.updated_at))} alt="" loading="lazy" />
                  <span>{assetMeta.prompt ?? assetMeta.style ?? shortName(asset.path)}</span>
                </motion.a>
              );
            })}
          </div>
        ) : (
          <div className="audio-stage">
            <Headphones size={40} />
            <h2>{detail.work.title}</h2>
            {(() => {
              const tracks = preferredTrackVariants(
                readerAssets.filter((asset) => asset.role === "track" || asset.mime.startsWith("audio/"))
              );
              return tracks.length > 0 ? (
                <>
                  <p className="audio-reader-hint">
                    播放器已移至底部播放栏，轨道会按需分页加载（{tracks.length}/{detail.track_count ?? tracks.length}）
                  </p>
                  {tracks.slice(0, 32).map((asset) => (
                    <button
                      className="track-line audio-track-button"
                      key={asset.id}
                      onClick={() => onPlayTrack(detail.work, asset, tracks, detail.track_count ?? tracks.length)}
                    >
                      <Play size={15} />
                      <span>{parseMeta<{ title?: string }>(asset.meta_json).title ?? shortName(asset.path)}</span>
                    </button>
                  ))}
                </>
              ) : (
                <>
                  <p className="audio-reader-hint">
                    {strmArchive ? "这是一个远程归档音声，需要先读取目录并建立可播放轨道。" : "未找到可播放轨道"}
                  </p>
                  {strmArchive && (
                    <button className="primary-action" type="button" disabled={strmPreparing || ["downloading", "parsing-directory", "extracting"].includes(strmTask?.phase ?? "")} onClick={() => void prepareStrmArchive()}>
                      {strmPreparing ? "提交准备任务…" : strmTask?.status === "waiting-password" ? "等待输入密码" : strmTask?.message ?? "准备音声归档"}
                    </button>
                  )}
                  {strmPrepareError && <p className="reader-error">{strmReadErrorMessage(strmPrepareError)}</p>}
                  {strmTask && strmTask.status !== "done" && strmTask.message && <p className="audio-reader-hint">{strmTask.phase} · {strmReadErrorMessage(strmTask.message)}</p>}
                </>
              );
            })()}
          </div>
        )}
      </motion.div>
    </motion.div>
  );
}

const GALLERY_PAGE_SIZE = 60;
const GALLERY_MAX_CACHED_PAGES = 3;
const GALLERY_TILE_MIN_WIDTH = 220;
const GALLERY_TILE_GAP = 10;
const GALLERY_OVERSCAN_ROWS = 1;
const GALLERY_WINDOW_UPDATE_RATIO = 0.5;
const GALLERY_IMAGE_LOAD_MARGIN_ROWS = 1;
const GALLERY_THUMB_SIZE = 256;
const GALLERY_THUMB_PREHEAT_ROWS = 4;
const GALLERY_ORIGINAL_PREFETCH_RADIUS = 2;
const GALLERY_THUMB_PRELOAD_CACHE_LIMIT = 48;
const GALLERY_ORIGINAL_PRELOAD_CACHE_LIMIT = 5;

function GalleryStage({
  canPersistProgress,
  detail,
  onProgressSaved,
  resumeTarget
}: {
  canPersistProgress: boolean;
  detail: WorkDetail;
  onProgressSaved: (id: number, progress: number, position?: string | null) => void;
  resumeTarget: ReadingPosition;
}) {
  const stageRef = useRef<HTMLDivElement | null>(null);
  const loadingOffsetsRef = useRef(new Set<number>());
  const loadedOffsetsRef = useRef(new Set<number>());
  const pendingScrollIndexRef = useRef<number | null>(resumeTarget.kind === "image" ? resumeTarget.index : 0);
  const lastProgressWriteRef = useRef(0);
  const progressTimerRef = useRef<number | null>(null);
  const lastSavedIndexRef = useRef(-1);
  const hasGalleryScrolledRef = useRef(false);
  const scrollRafRef = useRef<number | null>(null);
  const pendingScrollTopRef = useRef(0);
  const liveScrollTopRef = useRef(0);
  const rowHeightRef = useRef(1);
  const columnsRef = useRef(1);
  const viewportRef = useRef({ width: 0, height: 0 });
  const galleryLayoutRestoringRef = useRef(false);
  const totalRef = useRef(0);
  const latestGalleryIndexRef = useRef(resumeTarget.kind === "image" ? resumeTarget.index : 0);
  const lastWindowRowRef = useRef(-1);
  const cacheCenterPageRef = useRef(0);
  const pendingActiveImageRef = useRef<number | null>(null);
  const lightboxWheelDeltaRef = useRef(0);
  const lastLightboxWheelAtRef = useRef(0);
  const preloadedThumbsRef = useRef(new Map<string, HTMLImageElement>());
  const preloadedOriginalsRef = useRef(new Map<string, HTMLImageElement>());
  const fetchControllersRef = useRef(new Map<number, AbortController>());
  // Maps a page start index to the opaque cursor that precedes it.  A deep
  // virtualized jump may still use one legacy numeric offset lookup, but all
  // sequential pages after that anchor advance by keyset.
  const galleryPageCursorsRef = useRef(new Map<number, string | null>([[0, null]]));
  const [itemsByIndex, setItemsByIndex] = useState<Record<number, Asset>>({});
  const [total, setTotal] = useState(0);
  const [loadedOnce, setLoadedOnce] = useState(false);
  const [viewport, setViewport] = useState({ width: 0, height: 0 });
  const [scrollTop, setScrollTop] = useState(0);
  const [activeImage, setActiveImage] = useState<{ asset: Asset; index: number } | null>(null);
  const [error, setError] = useState<string | null>(null);
  const galleryVersion = detail.work.updated_at;
  const galleryMeta = useMemo(() => parseMeta<{ image_count?: number }>(detail.work.meta_json), [detail.work.meta_json]);
  const { flush: flushGalleryProgress, schedule: scheduleGalleryProgress } = useProgressQueue(
    detail.work.id,
    canPersistProgress,
    onProgressSaved,
    1200
  );
  const shouldPreloadOriginals = useMemo(() => allowsOriginalPreload(), []);

  const initialGalleryIndex = useCallback(() => {
    if (resumeTarget.kind === "image") return resumeTarget.index;
    const imageCount = galleryMeta.image_count ?? 0;
    if (!imageCount || detail.work.progress <= 0) return 0;
    return Math.min(
      Math.max(0, Math.floor(detail.work.progress * Math.max(0, imageCount - 1))),
      Math.max(0, imageCount - 1)
    );
  }, [detail.work.progress, galleryMeta.image_count, resumeTarget]);

  const cancelGalleryFetchesOutsideCache = useCallback((centerPage: number) => {
    const pageRadius = Math.floor(GALLERY_MAX_CACHED_PAGES / 2);
    const keepMinPage = Math.max(0, centerPage - pageRadius);
    const keepMaxPage = centerPage + pageRadius;
    for (const [offset, controller] of fetchControllersRef.current) {
      const page = Math.floor(offset / GALLERY_PAGE_SIZE);
      if (page >= keepMinPage && page <= keepMaxPage) continue;
      fetchControllersRef.current.delete(offset);
      loadingOffsetsRef.current.delete(offset);
      controller.abort();
    }
  }, []);

  const fetchPage = useCallback(async (offset: number) => {
    const aligned = Math.max(0, Math.floor(offset / GALLERY_PAGE_SIZE) * GALLERY_PAGE_SIZE);
    if (loadingOffsetsRef.current.has(aligned) || loadedOffsetsRef.current.has(aligned)) return;
    loadingOffsetsRef.current.add(aligned);
    const controller = new AbortController();
    fetchControllersRef.current.set(aligned, controller);
    try {
      const pageCursor = galleryPageCursorsRef.current.get(aligned);
      const requestCursor = pageCursor === undefined ? aligned : pageCursor;
      const res = await api.galleryPage(detail.work.id, requestCursor, GALLERY_PAGE_SIZE, controller.signal, galleryVersion);
      if (controller.signal.aborted || fetchControllersRef.current.get(aligned) !== controller) return;
      if (res.next_cursor) {
        galleryPageCursorsRef.current.set(aligned + res.items.length, res.next_cursor);
      }
      const responsePage = Math.floor(aligned / GALLERY_PAGE_SIZE);
      const centerPage = cacheCenterPageRef.current;
      const pageRadius = Math.floor(GALLERY_MAX_CACHED_PAGES / 2);
      const keepMinPage = Math.max(0, centerPage - pageRadius);
      const keepMaxPage = centerPage + pageRadius;
      const keepResponse = responsePage >= keepMinPage && responsePage <= keepMaxPage;
      loadedOffsetsRef.current.add(aligned);
      for (const cachedOffset of [...loadedOffsetsRef.current]) {
        const cachedPage = Math.floor(cachedOffset / GALLERY_PAGE_SIZE);
        if (cachedPage < keepMinPage || cachedPage > keepMaxPage) {
          loadedOffsetsRef.current.delete(cachedOffset);
        }
      }
      setTotal(res.total);
      setItemsByIndex((prev) => {
        const next: Record<number, Asset> = {};
        for (const [key, asset] of Object.entries(prev)) {
          const index = Number(key);
          const page = Math.floor(index / GALLERY_PAGE_SIZE);
          if (page >= keepMinPage && page <= keepMaxPage) {
            next[index] = asset;
          }
        }
        if (keepResponse) {
          res.items.forEach((asset, index) => {
            next[aligned + index] = asset;
          });
        }
        return next;
      });
      setLoadedOnce(true);
      setError(null);
    } catch (err) {
      if (
        controller.signal.aborted ||
        fetchControllersRef.current.get(aligned) !== controller ||
        (err instanceof DOMException && err.name === "AbortError")
      ) return;
      setLoadedOnce(true);
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      if (fetchControllersRef.current.get(aligned) === controller) {
        fetchControllersRef.current.delete(aligned);
        loadingOffsetsRef.current.delete(aligned);
      }
    }
  }, [detail.work.id, galleryVersion]);

  const preloadThumb = useCallback((asset: Asset) => {
    rememberGalleryPreload(preloadedThumbsRef.current, thumbUrl(asset.id, GALLERY_THUMB_SIZE, assetVersion(asset, galleryVersion)), false, GALLERY_THUMB_PRELOAD_CACHE_LIMIT);
  }, [galleryVersion]);

  const preloadOriginal = useCallback((asset: Asset, decode = false) => {
    if (!shouldPreloadOriginals && !decode) return;
    rememberGalleryPreload(preloadedOriginalsRef.current, assetUrl(asset.id, assetVersion(asset, galleryVersion)), decode, GALLERY_ORIGINAL_PRELOAD_CACHE_LIMIT);
  }, [galleryVersion, shouldPreloadOriginals]);

  const commitScrollTop = useCallback((nextScrollTop: number) => {
    liveScrollTopRef.current = nextScrollTop;
    const rowHeight = Math.max(1, rowHeightRef.current);
    const bucketHeight = Math.max(1, rowHeight * GALLERY_WINDOW_UPDATE_RATIO);
    const nextBucket = Math.floor(nextScrollTop / bucketHeight);
    if (nextBucket === lastWindowRowRef.current) return;
    lastWindowRowRef.current = nextBucket;
    pendingScrollTopRef.current = nextScrollTop;
    if (scrollRafRef.current !== null) return;
    scrollRafRef.current = window.requestAnimationFrame(() => {
      scrollRafRef.current = null;
      setScrollTop(pendingScrollTopRef.current);
    });
  }, []);

  useEffect(() => {
    const startIndex = initialGalleryIndex();
    setItemsByIndex({});
    setTotal(0);
    setLoadedOnce(false);
    setScrollTop(0);
    setActiveImage(null);
    setError(null);
    loadingOffsetsRef.current.clear();
    for (const controller of fetchControllersRef.current.values()) controller.abort();
    fetchControllersRef.current.clear();
    loadedOffsetsRef.current.clear();
    galleryPageCursorsRef.current.clear();
    galleryPageCursorsRef.current.set(0, null);
    cacheCenterPageRef.current = Math.floor(startIndex / GALLERY_PAGE_SIZE);
    pendingScrollIndexRef.current = startIndex;
    lastProgressWriteRef.current = Date.now();
    if (progressTimerRef.current !== null) {
      window.clearTimeout(progressTimerRef.current);
      progressTimerRef.current = null;
    }
    lastSavedIndexRef.current = -1;
    hasGalleryScrolledRef.current = false;
    pendingScrollTopRef.current = 0;
    liveScrollTopRef.current = 0;
    totalRef.current = 0;
    latestGalleryIndexRef.current = startIndex;
    lastWindowRowRef.current = -1;
    galleryLayoutRestoringRef.current = false;
    pendingActiveImageRef.current = null;
    clearGalleryPreloads(preloadedThumbsRef.current);
    clearGalleryPreloads(preloadedOriginalsRef.current);
    void fetchPage(startIndex);
  }, [detail.work.id]);

  useEffect(() => {
    return () => {
      if (scrollRafRef.current !== null) {
        window.cancelAnimationFrame(scrollRafRef.current);
      }
      if (progressTimerRef.current !== null) window.clearTimeout(progressTimerRef.current);
      for (const controller of fetchControllersRef.current.values()) controller.abort();
      fetchControllersRef.current.clear();
      const currentTotal = totalRef.current;
      if (hasGalleryScrolledRef.current && currentTotal > 0) {
        const safeIndex = Math.min(Math.max(latestGalleryIndexRef.current, 0), currentTotal - 1);
        if (lastSavedIndexRef.current !== safeIndex) {
          lastSavedIndexRef.current = safeIndex;
          scheduleGalleryProgress(
            Math.min(0.995, Math.max(0, (safeIndex + 1) / currentTotal)),
            `image:${safeIndex}`,
            true
          );
        }
      }
      void flushGalleryProgress();
      clearGalleryPreloads(preloadedThumbsRef.current);
      clearGalleryPreloads(preloadedOriginalsRef.current);
    };
  }, [flushGalleryProgress, scheduleGalleryProgress]);

  useEffect(() => {
    const element = stageRef.current;
    if (!element) return;
    const measure = () => {
      const next = { width: element.clientWidth, height: element.clientHeight };
      const previous = viewportRef.current;
      if (previous.width > 0 && Math.abs(previous.width - next.width) > 1) {
        pendingScrollIndexRef.current = latestGalleryIndexRef.current;
        lastWindowRowRef.current = -1;
        galleryLayoutRestoringRef.current = true;
      }
      viewportRef.current = next;
      setViewport((current) => current.width === next.width && current.height === next.height ? current : next);
    };
    return observeElementResize(element, measure);
  }, []);

  const columns = Math.max(1, Math.floor((viewport.width + GALLERY_TILE_GAP) / (GALLERY_TILE_MIN_WIDTH + GALLERY_TILE_GAP)));
  const tileWidth = viewport.width > 0
    ? Math.max(132, (viewport.width - GALLERY_TILE_GAP * (columns - 1)) / columns)
    : GALLERY_TILE_MIN_WIDTH;
  const tileHeight = Math.round(tileWidth * 1.32);
  const rowHeight = tileHeight + GALLERY_TILE_GAP;
  columnsRef.current = columns;
  totalRef.current = total;
  const rowCount = Math.ceil(total / columns);
  rowHeightRef.current = Math.max(1, rowHeight);
  const rowAdvance = rowHeight * GALLERY_WINDOW_UPDATE_RATIO;
  const visibleStartRow = Math.max(0, Math.floor((scrollTop + rowAdvance) / rowHeight));
  const visibleEndRow = Math.min(
    rowCount,
    Math.ceil((scrollTop + Math.max(viewport.height, rowHeight) + rowAdvance) / rowHeight)
  );
  const windowStartRow = Math.max(0, visibleStartRow - GALLERY_OVERSCAN_ROWS);
  const windowEndRow = Math.min(rowCount, visibleEndRow + GALLERY_OVERSCAN_ROWS);
  const windowStartIndex = Math.min(total, windowStartRow * columns);
  const windowEndIndex = Math.min(total, Math.max(windowStartIndex, windowEndRow * columns));
  const offsetY = windowStartRow * rowHeight;
  const totalHeight = Math.max(viewport.height, rowCount * rowHeight);
  const visibleIndexes = useMemo(
    () => Array.from({ length: Math.max(0, windowEndIndex - windowStartIndex) }, (_, index) => windowStartIndex + index),
    [windowEndIndex, windowStartIndex]
  );

  useEffect(() => {
    if (windowEndIndex <= windowStartIndex) return;
    const centerPage = Math.floor(Math.max(0, (windowStartIndex + windowEndIndex - 1) / 2) / GALLERY_PAGE_SIZE);
    cacheCenterPageRef.current = centerPage;
    cancelGalleryFetchesOutsideCache(centerPage);
    const preheatStartIndex = Math.max(0, (windowStartRow - GALLERY_THUMB_PREHEAT_ROWS) * columns);
    const preheatEndIndex = Math.min(total, (windowEndRow + GALLERY_THUMB_PREHEAT_ROWS) * columns);
    const firstPage = Math.floor(preheatStartIndex / GALLERY_PAGE_SIZE) * GALLERY_PAGE_SIZE;
    const lastPage = Math.floor(Math.max(preheatStartIndex, preheatEndIndex - 1) / GALLERY_PAGE_SIZE) * GALLERY_PAGE_SIZE;
    for (let offset = firstPage; offset <= lastPage; offset += GALLERY_PAGE_SIZE) {
      void fetchPage(offset);
    }
  }, [cancelGalleryFetchesOutsideCache, columns, fetchPage, total, windowEndIndex, windowEndRow, windowStartIndex, windowStartRow]);

  useEffect(() => {
    if (total <= 0 || columns <= 0) return;
    const startIndex = Math.max(0, (visibleStartRow - GALLERY_THUMB_PREHEAT_ROWS) * columns);
    const endIndex = Math.min(total, (visibleEndRow + GALLERY_THUMB_PREHEAT_ROWS) * columns);
    for (let index = startIndex; index < endIndex; index += 1) {
      const asset = itemsByIndex[index];
      if (asset) preloadThumb(asset);
    }
  }, [columns, itemsByIndex, preloadThumb, total, visibleEndRow, visibleStartRow]);

  useEffect(() => {
    if (pendingScrollIndexRef.current === null || total <= 0 || viewport.width <= 0) return;
    const index = Math.min(Math.max(pendingScrollIndexRef.current, 0), total - 1);
    const row = Math.floor(index / columns);
    const top = row * rowHeight;
    hasGalleryScrolledRef.current = top > 0;
    stageRef.current?.scrollTo({ top, behavior: "auto" });
    liveScrollTopRef.current = top;
    pendingScrollTopRef.current = top;
    latestGalleryIndexRef.current = index;
    lastWindowRowRef.current = Math.floor(top / Math.max(1, rowHeight * GALLERY_WINDOW_UPDATE_RATIO));
    setScrollTop(top);
    pendingScrollIndexRef.current = null;
    galleryLayoutRestoringRef.current = false;
  }, [columns, rowHeight, total, viewport.width]);

  const saveGalleryProgress = useCallback((index: number) => {
    if (!canPersistProgress || total <= 0) return;
    const safeIndex = Math.min(Math.max(index, 0), total - 1);
    if (lastSavedIndexRef.current === safeIndex) return;
    lastSavedIndexRef.current = safeIndex;
    const progress = Math.min(0.995, Math.max(0, (safeIndex + 1) / total));
    const position = `image:${safeIndex}`;
    scheduleGalleryProgress(progress, position);
  }, [canPersistProgress, scheduleGalleryProgress, total]);

  useEffect(() => {
    if (total <= 0 || rowHeight <= 0) return;
    if (galleryLayoutRestoringRef.current) return;
    if (!hasGalleryScrolledRef.current && scrollTop <= 0) return;
    const saveCurrent = () => {
      lastProgressWriteRef.current = Date.now();
      saveGalleryProgress(Math.floor(liveScrollTopRef.current / rowHeight) * columns);
    };
    const elapsed = Date.now() - lastProgressWriteRef.current;
    if (elapsed >= 3000) saveCurrent();
    if (progressTimerRef.current !== null) window.clearTimeout(progressTimerRef.current);
    progressTimerRef.current = window.setTimeout(() => {
      progressTimerRef.current = null;
      saveCurrent();
    }, 700);
    return () => {
      if (progressTimerRef.current !== null) {
        window.clearTimeout(progressTimerRef.current);
        progressTimerRef.current = null;
      }
    };
  }, [columns, rowHeight, saveGalleryProgress, scrollTop, total]);

  const openGalleryImage = useCallback((index: number) => {
    if (total <= 0) return;
    const safeIndex = Math.min(Math.max(index, 0), total - 1);
    latestGalleryIndexRef.current = safeIndex;
    const page = Math.floor(safeIndex / GALLERY_PAGE_SIZE);
    cacheCenterPageRef.current = page;
    cancelGalleryFetchesOutsideCache(page);
    const asset = itemsByIndex[safeIndex];
    if (asset) {
      saveGalleryProgress(safeIndex);
      setActiveImage({ asset, index: safeIndex });
      preloadOriginal(asset, true);
      pendingActiveImageRef.current = null;
      return;
    }
    pendingActiveImageRef.current = safeIndex;
    void fetchPage(safeIndex);
  }, [cancelGalleryFetchesOutsideCache, fetchPage, itemsByIndex, preloadOriginal, saveGalleryProgress, total]);

  const closeGalleryImage = useCallback(() => {
    pendingActiveImageRef.current = null;
    setActiveImage(null);
  }, []);

  useEffect(() => {
    const pendingIndex = pendingActiveImageRef.current;
    if (pendingIndex === null) return;
    const asset = itemsByIndex[pendingIndex];
    if (!asset) return;
    pendingActiveImageRef.current = null;
    saveGalleryProgress(pendingIndex);
    setActiveImage({ asset, index: pendingIndex });
    preloadOriginal(asset, true);
  }, [itemsByIndex, preloadOriginal, saveGalleryProgress]);

  useEffect(() => {
    if (!activeImage || total <= 0) return;
    const centerPage = Math.floor(activeImage.index / GALLERY_PAGE_SIZE);
    cacheCenterPageRef.current = centerPage;
    cancelGalleryFetchesOutsideCache(centerPage);
    const radius = shouldPreloadOriginals ? GALLERY_ORIGINAL_PREFETCH_RADIUS : 0;
    const start = Math.max(0, activeImage.index - radius);
    const end = Math.min(total - 1, activeImage.index + radius);
    const firstPage = Math.floor(start / GALLERY_PAGE_SIZE) * GALLERY_PAGE_SIZE;
    const lastPage = Math.floor(end / GALLERY_PAGE_SIZE) * GALLERY_PAGE_SIZE;
    for (let offset = firstPage; offset <= lastPage; offset += GALLERY_PAGE_SIZE) {
      void fetchPage(offset);
    }
    for (let index = start; index <= end; index += 1) {
      const asset = itemsByIndex[index];
      if (asset) preloadOriginal(asset, Math.abs(index - activeImage.index) <= 1);
    }
  }, [activeImage, cancelGalleryFetchesOutsideCache, fetchPage, itemsByIndex, preloadOriginal, shouldPreloadOriginals, total]);

  useEffect(() => {
    if (!activeImage) return;
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape") {
        event.preventDefault();
        event.stopImmediatePropagation();
        closeGalleryImage();
      }
      if (event.key === "ArrowLeft") {
        event.preventDefault();
        openGalleryImage(activeImage.index - 1);
      }
      if (event.key === "ArrowRight" || event.key === " ") {
        event.preventDefault();
        openGalleryImage(activeImage.index + 1);
      }
    };
    window.addEventListener("keydown", onKeyDown);
    return () => window.removeEventListener("keydown", onKeyDown);
  }, [activeImage, closeGalleryImage, openGalleryImage]);

  useEffect(() => {
    if (activeImage) latestGalleryIndexRef.current = activeImage.index;
    lightboxWheelDeltaRef.current = 0;
    lastLightboxWheelAtRef.current = 0;
  }, [activeImage]);

  const onGalleryLightboxWheel = useCallback((event: WheelEvent<HTMLDivElement>) => {
    if (!activeImage) return;
    event.preventDefault();
    event.stopPropagation();
    const dominantDelta = Math.abs(event.deltaY) >= Math.abs(event.deltaX) ? event.deltaY : event.deltaX;
    if (Math.abs(dominantDelta) < 1) return;
    lightboxWheelDeltaRef.current += dominantDelta;
    const now = Date.now();
    if (now - lastLightboxWheelAtRef.current < 220) return;
    if (Math.abs(lightboxWheelDeltaRef.current) < 80) return;
    const direction = lightboxWheelDeltaRef.current > 0 ? 1 : -1;
    lightboxWheelDeltaRef.current = 0;
    lastLightboxWheelAtRef.current = now;
    openGalleryImage(activeImage.index + direction);
  }, [activeImage, openGalleryImage]);

  return (
    <div
      className="gallery-stage"
      ref={stageRef}
      onScroll={(event) => {
        const nextScrollTop = event.currentTarget.scrollTop;
        const currentTotal = totalRef.current;
        latestGalleryIndexRef.current = currentTotal > 0
          ? Math.min(
              Math.max(Math.floor(nextScrollTop / Math.max(1, rowHeightRef.current)) * columnsRef.current, 0),
              currentTotal - 1
            )
          : 0;
        if (nextScrollTop > 0 || hasGalleryScrolledRef.current) {
          hasGalleryScrolledRef.current = true;
        }
        commitScrollTop(nextScrollTop);
      }}
    >
      {error && <div className="reader-error">{error}</div>}
      {!loadedOnce && <Loader2 className="spin gallery-loader" />}
      {loadedOnce && total === 0 && !error && <div className="empty-shelf">图库中没有可显示图片</div>}
      {total > 0 && (
        <div className="gallery-grid-spacer" style={{ height: totalHeight }}>
          <div
            className="gallery-grid-window"
            style={{
              gridAutoRows: `${tileHeight}px`,
              gridTemplateColumns: `repeat(${columns}, minmax(0, 1fr))`,
              transform: `translateY(${offsetY}px)`
            }}
          >
            {visibleIndexes.map((index) => {
              const asset = itemsByIndex[index];
              const assetMeta = asset ? parseMeta<{ tags?: string[] }>(asset.meta_json) : {};
              const row = Math.floor(index / columns);
              const shouldLoadImage = Boolean(
                asset &&
                  row >= visibleStartRow - GALLERY_IMAGE_LOAD_MARGIN_ROWS &&
                  row < visibleEndRow + GALLERY_IMAGE_LOAD_MARGIN_ROWS
              );
              return (
                <button
                  className={asset ? "gallery-tile" : "gallery-tile loading"}
                  data-image-index={index}
                  disabled={!asset}
                  key={asset?.id ?? `placeholder-${index}`}
                  onClick={() => {
                    if (!asset) return;
                    openGalleryImage(index);
                  }}
                  type="button"
                >
                  {asset && shouldLoadImage ? (
                    <img src={thumbUrl(asset.id, GALLERY_THUMB_SIZE, assetVersion(asset, galleryVersion))} alt="" loading="lazy" decoding="async" />
                  ) : (
                    <span className="gallery-skeleton" />
                  )}
                  <b>{index + 1}</b>
                  {asset ? <em>{assetMeta.tags?.slice(0, 2).join(" ") || shortName(asset.path)}</em> : null}
                </button>
              );
            })}
          </div>
        </div>
      )}
      {typeof document !== "undefined"
        ? createPortal(
            <AnimatePresence>
              {activeImage && (
                <motion.div
                  className="gallery-lightbox"
                  initial={{ opacity: 0 }}
                  animate={{ opacity: 1 }}
                  exit={{ opacity: 0 }}
                  onClick={closeGalleryImage}
                  onWheel={onGalleryLightboxWheel}
                >
                  <button className="icon-btn lightbox-close" onClick={closeGalleryImage} aria-label="关闭图片">
                    <X size={18} />
                  </button>
                  <div className="gallery-lightbox-content" onClick={(event) => event.stopPropagation()}>
                    <button
                      className="icon-btn gallery-lightbox-nav prev"
                      disabled={activeImage.index <= 0}
                      onClick={() => openGalleryImage(activeImage.index - 1)}
                      aria-label="上一张"
                    >
                      <ChevronLeft size={20} />
                    </button>
                    <div className="gallery-lightbox-frame">
                      <img src={assetUrl(activeImage.asset.id, assetVersion(activeImage.asset, galleryVersion))} alt="" />
                    </div>
                    <button
                      className="icon-btn gallery-lightbox-nav next"
                      disabled={activeImage.index >= total - 1}
                      onClick={() => openGalleryImage(activeImage.index + 1)}
                      aria-label="下一张"
                    >
                      <ChevronRight size={20} />
                    </button>
                    <div className="gallery-lightbox-toolbar">
                      <span>{activeImage.index + 1}/{total}</span>
                      <a href={assetUrl(activeImage.asset.id, assetVersion(activeImage.asset, galleryVersion))} target="_blank" rel="noreferrer">
                        <ExternalLink size={14} />
                        原图
                      </a>
                    </div>
                  </div>
                </motion.div>
              )}
            </AnimatePresence>,
            document.body
          )
        : null}
    </div>
  );
}

function FallbackCover({ kind }: { kind: string }) {
  return (
    <div className="fallback-cover" data-kind={kind} data-state="missing" aria-label="暂无封面" title="暂无封面">
      {kindIcon[kind] ?? <Sparkles size={24} />}
    </div>
  );
}

type ShelfCollectionGroup = {
  items: WorkSummary[];
  title?: string;
};

function buildComicCollections(works: WorkSummary[]) {
  const groups = new Map<string, ShelfCollectionGroup>();
  for (const work of works) {
    if (work.kind !== "comic") {
      groups.set(`work:${work.id}`, { items: [work] });
      continue;
    }
    const artist = comicCollectionArtist(work);
    if (!artist) {
      groups.set(`work:${work.id}`, { items: [work] });
      continue;
    }
    const key = `artist:${artist.toLocaleLowerCase()}`;
    const group = groups.get(key) ?? { items: [], title: artist };
    group.items.push(work);
    groups.set(key, group);
  }

  let syntheticId = -10000;
  return [...groups.entries()].flatMap(([collectionKey, group]) => {
    const items = group.items;
    if (items.length <= 1) return items;
    const sorted = [...items].sort((a, b) => a.title.localeCompare(b.title, "zh-Hans"));
    const first = sorted[0];
    const latest = sorted.reduce((acc, item) => (new Date(item.updated_at).getTime() > new Date(acc.updated_at).getTime() ? item : acc), first);
    const pageCount = sorted.reduce((sum, item) => sum + (parseMeta<{ page_count?: number }>(item.meta_json).page_count ?? 0), 0);
    const collectionTitle = group.title || "未知作者";
    return [{
      ...first,
      id: syntheticId--,
      kind: "comic-collection",
      title: collectionTitle,
      subtitle: `${sorted.length}本`,
      category: "Comic Artist Collection",
      progress: sorted.reduce((sum, item) => sum + item.progress, 0) / sorted.length,
      asset_count: sorted.length,
      tag_count: new Set(sorted.flatMap((item) => (item.tag_keys ?? "").split(",").filter(Boolean))).size,
      tag_keys: [...new Set(sorted.flatMap((item) => (item.tag_keys ?? "").split(",").filter(Boolean)))].join(","),
      updated_at: latest.updated_at,
      meta_json: JSON.stringify({
        artist: collectionTitle,
        collection_key: collectionKey,
        first_work_id: first.id,
        page_count: pageCount,
        volume_ids: sorted.map((item) => item.id),
        volume_count: sorted.length,
        series: collectionTitle
      })
    } satisfies WorkSummary];
  });
}

function buildNovelCollections(works: WorkSummary[]) {
  const groups = new Map<string, ShelfCollectionGroup>();
  const novels: WorkSummary[] = [];
  for (const work of works) {
    if (work.kind !== "novel") {
      groups.set(`work:${work.id}`, { items: [work] });
      continue;
    }
    novels.push(work);
  }

  const folderGroups = new Map<string, ShelfCollectionGroup>();
  for (const work of novels) {
    const folder = novelParentFolder(work.source_path);
    if (!folder) continue;
    const key = `folder:${normalizeNovelFolder(folder.path)}`;
    const group = folderGroups.get(key) ?? { items: [], title: folder.name };
    group.items.push(work);
    folderGroups.set(key, group);
  }

  const groupedByFolder = new Set<number>();
  for (const [key, group] of folderGroups) {
    if (group.items.length <= 1) continue;
    groups.set(key, group);
    for (const item of group.items) {
      groupedByFolder.add(item.id);
    }
  }

  for (const work of novels) {
    if (groupedByFolder.has(work.id)) continue;
    const meta = parseMeta<{ series?: string; creator?: string }>(work.meta_json);
    const title = meta.series || stripNovelVolume(work.title);
    const key = `series:${normalizeNovelSeries(title)}`;
    const group = groups.get(key) ?? { items: [], title };
    group.items.push(work);
    groups.set(key, group);
  }

  let syntheticId = -1;
  return [...groups.entries()].flatMap(([collectionKey, group]) => {
    const items = group.items;
    if (items.length <= 1) return items;
    const sorted = [...items].sort((a, b) => a.title.localeCompare(b.title, "zh-Hans"));
    const first = sorted[0];
    const latest = sorted.reduce((acc, item) => (new Date(item.updated_at).getTime() > new Date(acc.updated_at).getTime() ? item : acc), first);
    const meta = parseMeta<Record<string, unknown>>(first.meta_json);
    const collectionTitle = group.title || String(meta.series || stripNovelVolume(first.title));
    return [{
      ...first,
      id: syntheticId--,
      kind: "novel-collection",
      title: collectionTitle,
      subtitle: `${sorted.length}卷`,
      category: "Light Novel Collection",
      progress: sorted.reduce((sum, item) => sum + item.progress, 0) / sorted.length,
      asset_count: sorted.length,
      tag_count: new Set(sorted.flatMap((item) => (item.tag_keys ?? "").split(",").filter(Boolean))).size,
      tag_keys: [...new Set(sorted.flatMap((item) => (item.tag_keys ?? "").split(",").filter(Boolean)))].join(","),
      updated_at: latest.updated_at,
      meta_json: JSON.stringify({
        ...meta,
        collection_key: collectionKey,
        first_work_id: first.id,
        volume_ids: sorted.map((item) => item.id),
        volume_count: sorted.length,
        series: collectionTitle
      })
    } satisfies WorkSummary];
  });
}

function buildCoserPictureCollections(works: WorkSummary[]) {
  const groups = new Map<string, ShelfCollectionGroup>();
  for (const work of works) {
    if (work.kind !== "coser-picture") {
      groups.set(`work:${work.id}`, { items: [work] });
      continue;
    }
    const coser = coserPictureCollectionName(work);
    if (!coser) {
      groups.set(`work:${work.id}`, { items: [work] });
      continue;
    }
    const key = `coser:${coser.toLocaleLowerCase()}`;
    const group = groups.get(key) ?? { items: [], title: coser };
    group.items.push(work);
    groups.set(key, group);
  }

  let syntheticId = -20000;
  return [...groups.entries()].flatMap(([collectionKey, group]) => {
    const items = group.items;
    if (items.length <= 1) return items;
    const sorted = [...items].sort((a, b) => a.title.localeCompare(b.title, "zh-Hans"));
    const first = sorted[0];
    const latest = sorted.reduce((acc, item) => (new Date(item.updated_at).getTime() > new Date(acc.updated_at).getTime() ? item : acc), first);
    const meta = parseMeta<Record<string, unknown>>(first.meta_json);
    const pageCount = sorted.reduce((sum, item) => sum + (parseMeta<{ page_count?: number }>(item.meta_json).page_count ?? 0), 0);
    const collectionTitle = group.title || "未知Coser";
    return [{
      ...first,
      id: syntheticId--,
      kind: "coser-picture-collection",
      title: collectionTitle,
      subtitle: `${sorted.length}套`,
      category: "CoserPicture Collection",
      progress: sorted.reduce((sum, item) => sum + item.progress, 0) / sorted.length,
      asset_count: sorted.length,
      tag_count: new Set(sorted.flatMap((item) => (item.tag_keys ?? "").split(",").filter(Boolean))).size,
      tag_keys: [...new Set(sorted.flatMap((item) => (item.tag_keys ?? "").split(",").filter(Boolean)))].join(","),
      updated_at: latest.updated_at,
      meta_json: JSON.stringify({
        ...meta,
        collection_key: collectionKey,
        coser: collectionTitle,
        first_work_id: first.id,
        page_count: pageCount,
        volume_ids: sorted.map((item) => item.id),
        volume_count: sorted.length,
        series: collectionTitle
      })
    } satisfies WorkSummary];
  });
}

type ReadingPosition =
  | { kind: "page"; index: number }
  | { kind: "chapter"; index: number }
  | { kind: "epub-cfi"; cfi: string }
  | { kind: "track"; assetId: number; seconds: number }
  | { kind: "image"; index: number }
  | { kind: "cover" }
  | { kind: "start" }
  | { kind: null };

function parseReadingPosition(value?: string | null): ReadingPosition {
  if (!value) return { kind: null };
  if (value === "cover") return { kind: "cover" };
  if (value === "start") return { kind: "start" };
  if (value.startsWith("epubcfi:")) {
    try {
      return { kind: "epub-cfi", cfi: decodeURIComponent(value.slice("epubcfi:".length)) };
    } catch {
      return { kind: null };
    }
  }
  const [kind, first, second] = value.split(":");
  if (kind === "page") return { kind: "page", index: safeIndex(first) };
  if (kind === "chapter") return { kind: "chapter", index: safeIndex(first) };
  if (kind === "image") return { kind: "image", index: safeIndex(first) };
  if (kind === "track") return { kind: "track", assetId: safeIndex(first), seconds: safeIndex(second) };
  return { kind: null };
}

function safeIndex(value?: string) {
  const parsed = Number.parseInt(value ?? "0", 10);
  return Number.isFinite(parsed) ? Math.max(0, parsed) : 0;
}

function formatAudioTime(value: number) {
  if (!Number.isFinite(value) || value <= 0) return "0:00";
  const totalSeconds = Math.max(0, Math.floor(value));
  const hours = Math.floor(totalSeconds / 3600);
  const minutes = Math.floor((totalSeconds % 3600) / 60);
  const seconds = totalSeconds % 60;
  if (hours > 0) {
    return `${hours}:${String(minutes).padStart(2, "0")}:${String(seconds).padStart(2, "0")}`;
  }
  return `${minutes}:${String(seconds).padStart(2, "0")}`;
}

function normalizeComicPageInfo(page: ComicPageInfo | string): ComicPageInfo {
  return typeof page === "string" ? { name: page } : page;
}

function comicAspectHint(pages: ComicPageInfo[]) {
  const aspects = pages
    .slice(0, 8)
    .map(comicPageAspect)
    .filter((aspect) => Number.isFinite(aspect) && aspect > 0);
  if (aspects.length === 0) return COMIC_DEFAULT_ASPECT;
  return aspects.sort((a, b) => a - b)[Math.floor(aspects.length / 2)];
}

function comicPageAspect(page: ComicPageInfo) {
  const width = Number(page.width);
  const height = Number(page.height);
  if (Number.isFinite(width) && Number.isFinite(height) && width > 0 && height > 0) {
    return Math.min(3.2, Math.max(0.35, width / height));
  }
  return COMIC_DEFAULT_ASPECT;
}

function comicHorizontalSlotWidthFromSize(width: number, height: number, aspect: number, zoom: number) {
  const viewportWidth = Math.max(1, width);
  const naturalWidth = Math.max(1, height * zoom * aspect);
  return Math.min(viewportWidth, naturalWidth);
}

function comicHorizontalSlotWidth(stage: HTMLDivElement, aspect: number, zoom: number) {
  return comicHorizontalSlotWidthFromSize(stage.clientWidth, stage.clientHeight, aspect, zoom);
}

function comicPageFromHorizontalScroll(stage: HTMLDivElement, pageCount: number, aspect: number, zoom: number) {
  const slotWidth = comicHorizontalSlotWidth(stage, aspect, zoom);
  const centerPage = (stage.scrollLeft + stage.clientWidth / 2) / slotWidth - 0.5;
  return Math.min(Math.max(Math.round(centerPage), 0), Math.max(0, pageCount - 1));
}

function comicPageFromVerticalPosition(position: number, itemHeight: number, pageCount: number) {
  if (pageCount <= 1 || itemHeight <= 0) return 0;
  return Math.min(Math.max(Math.floor(position / itemHeight), 0), pageCount - 1);
}

function scrollComicStageToPage(
  stage: HTMLDivElement | null,
  page: number,
  pageCount: number,
  aspect = COMIC_DEFAULT_ASPECT,
  zoom = 1,
  verticalItemHeight = 0
) {
  if (!stage || pageCount < 2) return;
  const slot = stage.querySelector<HTMLElement>(`[data-page-index="${page}"]`);
  if (slot) {
    stage.scrollTo({ left: slot.offsetLeft, top: slot.offsetTop, behavior: "auto" });
    return;
  }
  if (stage.dataset.mode === "horizontal") {
    stage.scrollTo({ left: comicHorizontalSlotWidth(stage, aspect, zoom) * page, behavior: "auto" });
    return;
  }
  if (stage.dataset.mode === "scroll" && verticalItemHeight > 0) {
    stage.scrollTo({ left: 0, top: verticalItemHeight * page, behavior: "auto" });
    return;
  }
  const maxScroll = stage.scrollHeight - stage.clientHeight;
  if (maxScroll <= 0) return;
  stage.scrollTo({ top: (maxScroll * page) / (pageCount - 1), behavior: "auto" });
}

function upsertLocalHistory(history: HistoryRecord[], work: WorkSummary | undefined, progress: number, position?: string | null) {
  if (!work) return history;
  const record: HistoryRecord = {
    work_id: work.id,
    kind: work.kind,
    title: work.title,
    subtitle: work.subtitle,
    cover_asset_id: work.cover_asset_id,
    progress,
    position: position ?? history.find((item) => item.work_id === work.id)?.position ?? null,
    last_opened_at: new Date().toISOString()
  };
  return [record, ...history.filter((item) => item.work_id !== work.id)].slice(0, 50);
}

function stripNovelVolume(title: string) {
  return title
    .replace(/\s*(?:vol(?:ume)?\.?|第)?\s*\d{1,3}\s*(?:卷|巻|册|話|话)?\s*$/i, "")
    .replace(/\s*[（(]?\d{1,3}[）)]?\s*$/, "")
    .trim() || title;
}

function normalizeNovelSeries(value: string) {
  return stripNovelVolume(value).toLocaleLowerCase();
}

function comicCollectionArtist(work: WorkSummary) {
  const meta = parseMeta<{ artist?: string; penciller?: string; creator?: string; writer?: string }>(work.meta_json);
  const fromMeta = meta.artist || meta.penciller || meta.creator;
  if (fromMeta && fromMeta.trim()) return fromMeta.trim();
  const artistTag = (work.tag_keys ?? "")
    .split(",")
    .map((key) => key.trim())
    .find((key) => key.startsWith("artist:"));
  if (artistTag) return shortTag(artistTag);
  return null;
}

function coserPictureCollectionName(work: WorkSummary) {
  const meta = parseMeta<{ coser?: string }>(work.meta_json);
  if (meta.coser && meta.coser.trim()) return meta.coser.trim();
  const parent = novelParentFolder(work.source_path);
  if (parent?.name.trim()) return parent.name.trim();
  const artistTag = (work.tag_keys ?? "")
    .split(",")
    .map((key) => key.trim())
    .find((key) => key.startsWith("artist:"));
  if (artistTag) return shortTag(artistTag);
  return null;
}

function novelParentFolder(path?: string | null) {
  if (!path) return null;
  const normalized = path.replace(/\\/g, "/").replace(/\/+$/, "");
  const index = normalized.lastIndexOf("/");
  if (index <= 0) return null;
  const parent = normalized.slice(0, index);
  const name = parent.split("/").filter(Boolean).pop();
  if (!name) return null;
  return { path: parent, name };
}

function normalizeNovelFolder(value: string) {
  return value.replace(/\\/g, "/").replace(/\/+$/, "").toLocaleLowerCase();
}

const LIBRARY_MUTATING_JOB_TYPES = new Set([
  "scan-library",
  "import-tag-translations",
  "rebuild-search-index"
]);

function isTerminalJob(job: Job) {
  return job.status === "done" || job.status === "failed";
}

function markLibraryTerminalJobsSeen(jobs: Job[], seenJobIds: Set<number>) {
  let foundNewTerminalJob = false;
  for (const job of jobs) {
    if (!LIBRARY_MUTATING_JOB_TYPES.has(job.job_type) || !isTerminalJob(job) || seenJobIds.has(job.id)) continue;
    seenJobIds.add(job.id);
    foundNewTerminalJob = true;
  }
  return foundNewTerminalJob;
}

function jobsEqual(left: Job[], right: Job[]) {
  if (left === right) return true;
  if (left.length !== right.length) return false;
  return left.every((job, index) => {
    const other = right[index];
    return Boolean(
      other &&
      job.id === other.id &&
      job.job_type === other.job_type &&
      job.status === other.status &&
      job.payload_json === other.payload_json &&
      job.attempts === other.attempts &&
      job.last_error === other.last_error &&
      job.updated_at === other.updated_at
    );
  });
}

function jobLabel(value: string) {
  const labels: Record<string, string> = {
    "scan-library": "扫描媒体库",
    "rebuild-search-index": "重建搜索索引",
    "import-tag-translations": "导入标签翻译",
    "enrich-lightnovel-work": "轻小说补全",
    "enrich-asmr-work": "音声补全",
    "generate-image-asset": "生成图片"
  };
  return labels[value] ?? value;
}

function statusLabel(value: string) {
  const labels: Record<string, string> = {
    queued: "等待中",
    running: "进行中",
    done: "完成",
    failed: "失败",
    retrying: "等待重试"
  };
  return labels[value] ?? value;
}

function tagKey(tag: Tag) {
  return `${tag.namespace}:${tag.key}`;
}

function tagNamespace(tag: Tag, language: TagLanguage) {
  if (language !== "translated") return tag.namespace;
  return tag.translated_namespace ?? namespaceLabel(tag.namespace);
}

function tagLabel(tag: Tag, language: TagLanguage) {
  return language === "translated" ? tag.translated_label ?? tag.label : tag.label;
}

function groupDetailTags(tags: Tag[], language: TagLanguage) {
  const groups: Array<{ namespace: string; tags: Tag[] }> = [];
  const byNamespace = new Map<string, Tag[]>();
  for (const tag of tags.slice(0, 64)) {
    const namespace = tagNamespace(tag, language);
    byNamespace.set(namespace, [...(byNamespace.get(namespace) ?? []), tag]);
  }
  for (const [namespace, items] of byNamespace) {
    groups.push({ namespace, tags: items });
  }
  return groups;
}

function cycleTagFilter(filters: Record<string, TagFilterMode>, key: string) {
  const next = { ...filters };
  if (!next[key]) next[key] = "include";
  else delete next[key];
  return next;
}

function normalizeSettingsDraft(settings: AppSettings): AppSettings {
  return {
    ...settings,
    reader: {
      ...defaultReaderSettings,
      ...(settings.reader ?? {}),
      comic_auto_read_interval_ms: clampComicAutoReadIntervalMs(settings.reader?.comic_auto_read_interval_ms)
    },
    media_dirs: {
      comics: settings.media_dirs?.comics ?? [],
      novels: settings.media_dirs?.novels ?? [],
      audio: settings.media_dirs?.audio ?? [],
      gallery: settings.media_dirs?.gallery ?? [],
      coser_picture: settings.media_dirs?.coser_picture ?? [],
      comic_scan_depth: Math.min(Math.max(Math.trunc(settings.media_dirs?.comic_scan_depth ?? 3), 1), 64)
    },
    cover_cache_dirs: {
      comic: settings.cover_cache_dirs?.comic ?? "",
      novel: settings.cover_cache_dirs?.novel ?? "",
      audio: settings.cover_cache_dirs?.audio ?? "",
      gallery: settings.cover_cache_dirs?.gallery ?? "",
      coser_picture: settings.cover_cache_dirs?.coser_picture ?? ""
    },
    media_sources: (settings.media_sources ?? []).map((source) => ({
      ...source,
      audio_grouping: source.audio_grouping ?? "auto"
    })),
    audio_grouping: settings.audio_grouping ?? "auto",
    qmediasync: settings.qmediasync ?? {
      enabled: false,
      base_url: "",
      strm_roots: []
    }
  };
}

function formatBytes(value: number) {
  if (!Number.isFinite(value) || value <= 0) return "0 B";
  const units = ["B", "KB", "MB", "GB", "TB"];
  let next = value;
  let unit = 0;
  while (next >= 1024 && unit < units.length - 1) {
    next /= 1024;
    unit += 1;
  }
  return `${next.toFixed(unit === 0 ? 0 : 1)} ${units[unit]}`;
}

function routePolicyLabel(policy: string) {
  const labels: Record<string, string> = {
    "qmediasync-strm": "qmediasync STRM",
    "app-proxy": "本项目代理",
    local: "本地文件"
  };
  return labels[policy] ?? policy;
}

function routeTransferLabel(transfer: string) {
  const labels: Record<string, string> = {
    "qmediasync-strm": "STRM直链",
    "app-proxy": "本项目代理"
  };
  return labels[transfer] ?? transfer;
}

function routeLabel(value: string) {
  const labels: Record<string, string> = {
    "local -> app -> browser": "本地文件 -> 本项目 -> 浏览器",
    "115 -> qmediasync -> STRM -> browser": "115 -> qmediasync -> STRM -> 浏览器",
    "115 -> qmediasync -> STRM -> app-cache -> browser": "115 -> qmediasync -> STRM -> 本项目缓存 -> 浏览器"
  };
  return labels[value] ?? value;
}

function routeNoteLabel(value: string) {
  const labels: Record<string, string> = {
    "qmediasync-strm-link": "通过 qmediasync 生成的 STRM 链路解析"
  };
  return labels[value] ?? value;
}

function shortTag(key: string) {
  return key.split(":").slice(-1)[0] ?? key;
}

function namespaceLabel(namespace: string) {
  const labels: Record<string, string> = {
    language: "语言",
    artist: "作者",
    group: "社团",
    series: "系列",
    ln: "轻小说",
    audio: "音声",
    source: "来源",
    folder: "文件夹",
    gallery: "图库",
    "coser-picture": "CoserPicture",
    circle: "社团",
    va: "声优",
    female: "女性",
    male: "男性",
    mixed: "混合",
    other: "其他"
  };
  return labels[namespace] ?? namespace;
}

function shortName(path: string) {
  return path.split(/[\\/]/).pop() ?? path;
}

function observeElementResize(element: Element, measure: () => void) {
  let frame: number | null = null;
  const schedule = () => {
    if (frame !== null) return;
    frame = window.requestAnimationFrame(() => {
      frame = null;
      measure();
    });
  };
  const observer = new ResizeObserver(schedule);
  observer.observe(element);
  schedule();
  return () => {
    observer.disconnect();
    if (frame !== null) window.cancelAnimationFrame(frame);
  };
}

function rememberGalleryPreload(cache: Map<string, HTMLImageElement>, url: string, decode = false, limit = 5) {
  if (typeof window === "undefined" || cache.has(url)) return;
  const image = new window.Image();
  image.decoding = "async";
  image.loading = "eager";
  image.src = url;
  cache.set(url, image);
  while (cache.size > limit) {
    const oldest = cache.keys().next().value;
    if (!oldest) break;
    const evicted = cache.get(oldest);
    if (evicted) evicted.removeAttribute("src");
    cache.delete(oldest);
  }
  if (decode && typeof image.decode === "function") {
    void image.decode().catch(() => {});
  }
}

function clearGalleryPreloads(cache: Map<string, HTMLImageElement>) {
  for (const image of cache.values()) image.removeAttribute("src");
  cache.clear();
}

function allowsOriginalPreload() {
  if (typeof navigator === "undefined") return false;
  const connection = (navigator as Navigator & {
    connection?: { saveData?: boolean; effectiveType?: string };
  }).connection;
  if (connection?.saveData) return false;
  return !connection?.effectiveType || !["slow-2g", "2g", "3g"].includes(connection.effectiveType);
}


function preferredTrackVariants(tracks: Asset[]) {
  const byKey = new Map<string, Asset>();
  for (const track of tracks) {
    const meta = parseMeta<{ track_key?: string; preferred_playback?: boolean }>(track.meta_json);
    const key = meta.track_key || `${track.position ?? track.id}:${shortName(track.path).replace(/\.[^.]+$/, "")}`;
    const current = byKey.get(key);
    if (!current || isPreferredTrack(track, current)) {
      byKey.set(key, track);
    }
  }
  return [...byKey.values()].sort((a, b) => (a.position ?? a.id) - (b.position ?? b.id));
}

function isPreferredTrack(candidate: Asset, current: Asset) {
  const candidateMeta = parseMeta<{ preferred_playback?: boolean }>(candidate.meta_json);
  const currentMeta = parseMeta<{ preferred_playback?: boolean }>(current.meta_json);
  if (candidateMeta.preferred_playback !== currentMeta.preferred_playback) {
    return candidateMeta.preferred_playback === true;
  }
  const candidateIsMp3 = candidate.mime.includes("mpeg") || candidate.path.toLowerCase().endsWith(".mp3");
  const currentIsMp3 = current.mime.includes("mpeg") || current.path.toLowerCase().endsWith(".mp3");
  if (candidateIsMp3 !== currentIsMp3) return candidateIsMp3;
  return (candidate.size ?? Number.MAX_SAFE_INTEGER) < (current.size ?? Number.MAX_SAFE_INTEGER);
}



// Keep source/network failures distinct from missing local metadata.
function strmReadErrorMessage(message: string): string {
  if (message.includes("STRM target resolves to a non-public address")) {
    return "漫画正文尚未读取：STRM 域名解析到了非公网地址，后端已阻止请求。请检查后端所在设备的 DNS 和代理设置（可能为 Fake-IP）。本地封面和标题加载成功不代表云盘可访问；重扫不会修复此问题。若确实使用内网媒体服务，请由管理员配置该来源的精确信任规则。";
  }
  if (message.includes("STRM DNS lookup")) {
    return "无法解析 STRM 域名，请检查后端所在设备的 DNS 和网络连接。";
  }
  if (message.includes("STRM source is cooling down") || message.includes("STRM source recently failed")) {
    return "云盘请求失败后正在等待冷却，请稍后重试，避免连续请求。";
  }
  return message;
}
