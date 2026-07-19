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
  FolderMinus,
  FolderPlus,
  Folders,
  Gauge,
  GalleryHorizontal,
  GalleryThumbnails,
  Headphones,
  History as HistoryIcon,
  Image,
  Info,
  KeyRound,
  Library,
  ListMusic,
  LayoutGrid,
  LayoutList,
  ListFilter,
  Loader2,
  LogOut,
  Moon,
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
  Sun,
  Tags,
  Volume2,
  X,
  ZoomIn,
  ZoomOut
} from "lucide-react";
import { api, assetUrl, assetVersion, comicPageUrl, coverUrl, parseMeta, thumbUrl, type AppSettings, type Asset, type AssetRouteInfo, type AuthSession, type ComicPageInfo, type GlassIntensity, type HistoryRecord, type Job, type LibraryResponse, type Tag, type ThemeMode, type UiMaterial, type WorkDetail, type WorkSummary } from "./api";
import { GlassFilterProvider, GlassSurface } from "./components/material";
import { useProgressQueue } from "./hooks/useProgressQueue";
const NovelReader = lazy(() => import("./components/NovelReader").then((module) => ({ default: module.NovelReader })));

type KindFilter = "history" | "comic" | "novel" | "audio" | "gallery" | "coser-picture";
type ViewMode = "grid" | "compact" | "list" | "cover";
type TagFilterMode = "include";
type TagLanguage = "translated" | "raw";
type ComicReaderMode = "paged" | "scroll" | "horizontal";
type ShelfDisplayMode = "collections" | "single";
type DetailMode = "modal" | "docked";
type AppearanceState = {
  material: UiMaterial;
  glass_intensity: GlassIntensity;
};
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
  resumePosition: string | null;
  sessionId: number;
};
type OpenCollectionDescriptor = {
  collectionKey: string;
  kind: WorkSummary["kind"];
};
type AudioRepeatMode = "none" | "all" | "one";

const COMIC_DEFAULT_ASPECT = 0.72;
const COMIC_HORIZONTAL_OVERSCAN = 4;
const COMIC_VERTICAL_OVERSCAN = 4;

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

const defaultAppearance: AppearanceState = {
  material: "liquid",
  glass_intensity: "standard"
};

const defaultReaderSettings = {
  comic_auto_read_interval_ms: 4000
};

function clampComicAutoReadIntervalMs(value: unknown) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) return defaultReaderSettings.comic_auto_read_interval_ms;
  return Math.round(Math.min(Math.max(numeric, 500), 120000));
}

export function App() {
  const [library, setLibrary] = useState<LibraryResponse>({ works: [], tags: [], jobs: [], history: [] });
  const [selectedId, setSelectedId] = useState<number | null>(null);
  const [detail, setDetail] = useState<WorkDetail | null>(null);
  const [kind, setKind] = useState<KindFilter>("comic");
  const [query, setQuery] = useState("");
  const [localSearch, setLocalSearch] = useState<LocalSearchState>({ query: "", ids: [], status: "idle" });
  const [tagQuery, setTagQuery] = useState("");
  const [tagFilters, setTagFilters] = useState<Record<string, TagFilterMode>>({});
  const [tagLanguage, setTagLanguage] = useState<TagLanguage>("translated");
  const [selectedTag, setSelectedTag] = useState<Tag | null>(null);
  const [viewMode, setViewMode] = useState<ViewMode>("cover");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [auth, setAuth] = useState<AuthSession>({ authenticated: false });
  const [loginPassword, setLoginPassword] = useState("");
  const [newAdminPassword, setNewAdminPassword] = useState("");
  const [passwordMessage, setPasswordMessage] = useState<string | null>(null);
  const [authBusy, setAuthBusy] = useState(false);
  const [readerOpen, setReaderOpen] = useState(false);
  const [pendingReaderId, setPendingReaderId] = useState<number | null>(null);
  const [readerResume, setReaderResume] = useState(true);
  const [readerPositionOverride, setReaderPositionOverride] = useState<string | null | undefined>(undefined);
  const [settings, setSettings] = useState<AppSettings | null>(null);
  const [settingsOpen, setSettingsOpen] = useState(false);
  const [detailMode, setDetailMode] = useState<DetailMode>("modal");
  const [detailModalOpen, setDetailModalOpen] = useState(false);
  const [collectionStack, setCollectionStack] = useState<WorkSummary[] | null>(null);
  const [theme, setTheme] = useState<ThemeMode>(() => {
    if (typeof window === "undefined") return "light";
    return (window.localStorage.getItem("media_shelf_theme") as ThemeMode | null) ?? "light";
  });
  const [comicDisplayMode, setComicDisplayMode] = useState<ShelfDisplayMode>("collections");
  const [novelDisplayMode, setNovelDisplayMode] = useState<ShelfDisplayMode>("collections");
  const [coserPictureDisplayMode, setCoserPictureDisplayMode] = useState<ShelfDisplayMode>("collections");
  const [activeAudio, setActiveAudio] = useState<ActiveAudioState | null>(null);
  const audioSessionIdRef = useRef(0);
  const selectedIdRef = useRef<number | null>(null);
  const libraryRequestRef = useRef<AbortController | null>(null);
  const libraryGenerationRef = useRef(0);
  const jobsSnapshotRef = useRef<Job[]>([]);
  const seenLibraryTerminalJobIdsRef = useRef(new Set<number>());
  const detailRequestRef = useRef<AbortController | null>(null);
  const detailGenerationRef = useRef(0);
  const detailRef = useRef<WorkDetail | null>(null);
  const openCollectionRef = useRef<OpenCollectionDescriptor | null>(null);
  const historyRequestRef = useRef<AbortController | null>(null);
  const historyGenerationRef = useRef(0);

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

  const refresh = useCallback(async () => {
    libraryRequestRef.current?.abort();
    const controller = new AbortController();
    const generation = ++libraryGenerationRef.current;
    libraryRequestRef.current = controller;
    setError(null);
    let firstPage: LibraryResponse;
    try {
      firstPage = await api.library({ limit: 100, includeContext: true, signal: controller.signal });
    } catch (error) {
      if (controller.signal.aborted || generation !== libraryGenerationRef.current) return;
      if (libraryRequestRef.current === controller) libraryRequestRef.current = null;
      throw error;
    }
    if (controller.signal.aborted || generation !== libraryGenerationRef.current) return;
    markLibraryTerminalJobsSeen(firstPage.jobs, seenLibraryTerminalJobIdsRef.current);
    jobsSnapshotRef.current = firstPage.jobs;
    setLibrary((current) => (
      !controller.signal.aborted && generation === libraryGenerationRef.current ? firstPage : current
    ));
    if (firstPage.works[0]) setSelectedId((current) => current ?? firstPage.works[0].id);

    const loadRemainingPages = async () => {
      let cursor = firstPage.next_cursor ?? null;
      const seenCursors = new Set<string>();
      try {
        while (cursor && !seenCursors.has(cursor)) {
          if (controller.signal.aborted || generation !== libraryGenerationRef.current) return;
          seenCursors.add(cursor);
          const page = await api.library({
            cursor,
            limit: 500,
            includeContext: false,
            signal: controller.signal
          });
          if (controller.signal.aborted || generation !== libraryGenerationRef.current) return;
          setLibrary((current) => {
            if (controller.signal.aborted || generation !== libraryGenerationRef.current) return current;
            const knownIds = new Set(current.works.map((work) => work.id));
            const appended = page.works.filter((work) => {
              if (knownIds.has(work.id)) return false;
              knownIds.add(work.id);
              return true;
            });
            return {
              ...current,
              works: appended.length > 0
                ? [...current.works, ...appended].sort(compareWorksByUpdatedAt)
                : current.works,
              next_cursor: page.next_cursor
            };
          });
          cursor = page.next_cursor ?? null;
        }
      } catch (error) {
        if (controller.signal.aborted || generation !== libraryGenerationRef.current) return;
        setError(`后台加载作品列表失败：${error instanceof Error ? error.message : String(error)}`);
      } finally {
        if (libraryRequestRef.current === controller && generation === libraryGenerationRef.current) {
          libraryRequestRef.current = null;
        }
      }
    };

    if (firstPage.next_cursor) {
      void loadRemainingPages();
    } else if (libraryRequestRef.current === controller) {
      libraryRequestRef.current = null;
    }
  }, []);

  useEffect(() => {
    document.documentElement.dataset.theme = theme;
    window.localStorage.setItem("media_shelf_theme", theme);
  }, [theme]);

  const appearance = settings?.appearance ?? defaultAppearance;
  const material = appearance.material ?? "liquid";
  const glassIntensity = appearance.glass_intensity ?? "standard";
  const isLiquid = material === "liquid";
  const comicAutoReadIntervalMs = clampComicAutoReadIntervalMs(settings?.reader?.comic_auto_read_interval_ms);

  useEffect(() => {
    document.documentElement.dataset.material = material;
    document.documentElement.dataset.glassIntensity = glassIntensity;
  }, [material, glassIntensity]);

  useEffect(() => {
    api.authSession().then(setAuth).catch(() => setAuth({ authenticated: false }));
    api
      .settings()
      .then((value) => {
        setSettings(value);
        setTheme(value.theme ?? "light");
        setDetailMode(value.detail_mode ?? "modal");
      })
      .catch((err) => setError(err.message));
    refresh().catch((err) => setError(err.message));
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
            setLibrary((prev) => jobsEqual(prev.jobs, nextJobs) ? prev : { ...prev, jobs: nextJobs });
          }
          if (refreshAfterTerminalJob) {
            void refresh().catch((err) => setError(err instanceof Error ? err.message : String(err)));
          }
        } catch {
          // Library refresh remains the source of truth when an event is malformed.
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
      events?.close();
      if (retryTimer !== null) window.clearTimeout(retryTimer);
    };
  }, [refresh]);

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
    api
      .work(selectedId, controller.signal)
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
  }, [query, kind]);

  const baseWorks = useMemo(() => library.works.filter((work) => work.kind !== "generated"), [library.works]);

  const scopedWorks = useMemo(() => {
    if (kind !== "history") return baseWorks;
    const byId = new Map(baseWorks.map((work) => [work.id, work]));
    return library.history
      .map((record) => byId.get(record.work_id))
      .filter((work): work is WorkSummary => Boolean(work));
  }, [baseWorks, kind, library.history]);

  const availableTagKeys = useMemo(() => {
    const keys = new Set<string>();
    for (const work of scopedWorks) {
      if (kind !== "history" && work.kind !== kind) continue;
      for (const key of (work.tag_keys ?? "").split(",").map((value) => value.trim()).filter(Boolean)) {
        keys.add(key);
      }
    }
    return keys;
  }, [scopedWorks, kind]);

  const visibleTags = useMemo(() => {
    if (availableTagKeys.size === 0) return [];
    const needle = tagQuery.trim().toLowerCase();
    return library.tags
      .filter((tag) => {
        if (availableTagKeys.size > 0 && !availableTagKeys.has(tagKey(tag))) return false;
        if (!needle) return true;
        return `${tag.namespace}:${tag.key} ${tag.label} ${tag.translated_label ?? ""}`.toLowerCase().includes(needle);
      })
      .slice(0, 120);
  }, [availableTagKeys, library.tags, tagQuery]);

  const includeTags = useMemo(
    () => Object.entries(tagFilters).filter(([, mode]) => mode === "include").map(([key]) => key),
    [tagFilters]
  );
  const filteredWorks = useMemo(() => {
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
  }, [scopedWorks, kind, query, localSearch, includeTags, detail]);

  const counts = useMemo(() => {
    return baseWorks.reduce<Record<string, number>>(
      (acc, work) => {
        acc[work.kind] = (acc[work.kind] ?? 0) + 1;
        return acc;
      },
      { history: library.history.length, comic: 0, novel: 0, audio: 0, gallery: 0, "coser-picture": 0 }
    );
  }, [baseWorks, library.history.length]);

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
    if (!auth.authenticated) {
      setError("请先使用管理员密码登录");
      return;
    }
    setBusy(true);
    setError(null);
    try {
      await api.scan(false);
      await refresh();
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setBusy(false);
    }
  };

  const runTagImport = async () => {
    if (!auth.authenticated) {
      setError("请先使用管理员密码登录");
      return;
    }
    setBusy(true);
    setError(null);
    try {
      await api.enrich("import-tag-translations");
      await refresh();
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setBusy(false);
    }
  };

  const login = async () => {
    if (!loginPassword.trim()) return;
    setAuthBusy(true);
    setError(null);
    setPasswordMessage(null);
    try {
      const session = await api.login(loginPassword);
      setAuth(session);
      setLoginPassword("");
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setAuthBusy(false);
    }
  };

  const changeAdminPassword = async () => {
    if (!newAdminPassword.trim()) return;
    setAuthBusy(true);
    setPasswordMessage(null);
    setError(null);
    try {
      await api.changePassword(newAdminPassword);
      setNewAdminPassword("");
      setPasswordMessage("管理员密码已更新");
    } catch (err) {
      setPasswordMessage(err instanceof Error ? err.message : String(err));
    } finally {
      setAuthBusy(false);
    }
  };

  const logout = async () => {
    setAuthBusy(true);
    setError(null);
    try {
      await api.logout();
      setAuth({ authenticated: false });
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setAuthBusy(false);
    }
  };

  const saveSettings = async (next: AppSettings) => {
    if (!auth.authenticated) {
      setError("请先使用管理员密码登录");
      return;
    }
    setBusy(true);
    setError(null);
    try {
      const saved = await api.updateSettings(next);
      setSettings(saved);
      setTheme(saved.theme);
      setDetailMode(saved.detail_mode ?? "modal");
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setBusy(false);
    }
  };

  const changeTheme = (next: ThemeMode) => {
    setTheme(next);
    setSettings((prev) => (prev ? { ...prev, theme: next } : prev));
  };

  const changeAppearance = (next: Partial<AppearanceState>) => {
    setSettings((prev) => {
      if (!prev) return prev;
      return {
        ...prev,
        appearance: {
          ...(prev.appearance ?? defaultAppearance),
          ...next
        }
      };
    });
  };

  const closeCollection = useCallback(() => {
    openCollectionRef.current = null;
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

  const playTrackInDock = (work: WorkDetail["work"], asset: Asset, playlist?: Asset[]) => {
    const fallback = historyByWorkId.get(work.id)?.position ?? null;
    void resolveExactHistoryPosition(work.id, fallback).then((result) => {
      if (!result.current) return;
      setActiveAudio({
        work,
        asset,
        playlist: playlist && playlist.length > 0 ? playlist : [asset],
        resumePosition: result.position,
        sessionId: ++audioSessionIdRef.current
      });
    });
  };

  const collectionShelfWorks = useMemo(() => {
    if (kind === "comic" && comicDisplayMode === "collections") return buildComicCollections(filteredWorks);
    if (kind === "novel" && novelDisplayMode === "collections") return buildNovelCollections(filteredWorks);
    if (kind === "coser-picture" && coserPictureDisplayMode === "collections") return buildCoserPictureCollections(filteredWorks);
    return filteredWorks;
  }, [comicDisplayMode, coserPictureDisplayMode, filteredWorks, kind, novelDisplayMode]);

  const displayedWorks = useMemo(
    () => collectionStack ?? collectionShelfWorks,
    [collectionShelfWorks, collectionStack]
  );

  useEffect(() => {
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
  }, [collectionShelfWorks, filteredWorks]);

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
    const candidates = (collectionStack ?? filteredWorks).filter((work) => work.kind === "comic");
    if (candidates.length === 0) return;
    const work = candidates[Math.floor(Math.random() * candidates.length)];
    void openComicReader(work, false, "start");
  };

  return (
    <GlassFilterProvider>
    <div
      className={detailMode === "docked" ? "app-shell has-detail-pane" : "app-shell modal-detail"}
      data-glass-intensity={glassIntensity}
      data-material={material}
    >
      {isLiquid ? (
      <GlassSurface as="aside" className="rail" variant="panel">
        <RailContent
          counts={counts}
          includeTags={includeTags}
          kind={kind}
          selectedTag={selectedTag}
          tagFilters={tagFilters}
          tagLanguage={tagLanguage}
          tagQuery={tagQuery}
          visibleTags={visibleTags}
          onKindChange={setKind}
          onSelectedTagChange={setSelectedTag}
          onTagFiltersChange={setTagFilters}
          onTagLanguageChange={setTagLanguage}
          onTagQueryChange={setTagQuery}
        />
      </GlassSurface>
      ) : (
      <aside className="rail">
        <RailContent
          counts={counts}
          includeTags={includeTags}
          kind={kind}
          selectedTag={selectedTag}
          tagFilters={tagFilters}
          tagLanguage={tagLanguage}
          tagQuery={tagQuery}
          visibleTags={visibleTags}
          onKindChange={setKind}
          onSelectedTagChange={setSelectedTag}
          onTagFiltersChange={setTagFilters}
          onTagLanguageChange={setTagLanguage}
          onTagQueryChange={setTagQuery}
        />
      </aside>
      )}

      <main className="workspace">
        {isLiquid ? (
        <GlassSurface as="header" className="toolbar" variant="panel">
          <ToolbarContent
            collectionStack={collectionStack}
            comicDisplayMode={comicDisplayMode}
            comicCount={(collectionStack ?? filteredWorks).filter((work) => work.kind === "comic").length}
            coserPictureDisplayMode={coserPictureDisplayMode}
            kind={kind}
            localSearch={localSearch}
            novelDisplayMode={novelDisplayMode}
            query={query}
            viewMode={viewMode}
            onCollectionBack={closeCollection}
            onComicDisplayModeChange={setComicDisplayMode}
            onCoserPictureDisplayModeChange={setCoserPictureDisplayMode}
            onOpenRandomComic={openRandomComic}
            onNovelDisplayModeChange={setNovelDisplayMode}
            onQueryChange={setQuery}
            onSettingsOpen={() => setSettingsOpen(true)}
            onViewModeChange={setViewMode}
          />
        </GlassSurface>
        ) : (
        <header className="toolbar">
          <ToolbarContent
            collectionStack={collectionStack}
            comicDisplayMode={comicDisplayMode}
            comicCount={(collectionStack ?? filteredWorks).filter((work) => work.kind === "comic").length}
            coserPictureDisplayMode={coserPictureDisplayMode}
            kind={kind}
            localSearch={localSearch}
            novelDisplayMode={novelDisplayMode}
            query={query}
            viewMode={viewMode}
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
        )}

        {error && (
          <motion.div className="error-strip" initial={{ opacity: 0, y: -8 }} animate={{ opacity: 1, y: 0 }}>
            {error}
          </motion.div>
        )}

        <VirtualShelf
          items={displayedWorks}
          itemKey={(work) => work.id}
          viewMode={viewMode}
          renderItem={(work, index) => (
            <WorkCard
              index={index}
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
          liquid={isLiquid}
        />
      )}
      <AudioDock
        key={activeAudio ? `${activeAudio.work.id}:${activeAudio.asset.id}:${activeAudio.sessionId}` : "idle"}
        active={activeAudio}
        canPersistProgress={true}
        onClose={() => setActiveAudio(null)}
        onProgressSaved={syncProgress}
        resumePosition={activeAudio?.resumePosition ?? null}
        liquid={isLiquid}
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
              liquid={isLiquid}
            />
          </motion.div>
        )}
        {settingsOpen && (
          <SettingsOverlay
            auth={auth}
            authBusy={authBusy}
            busy={busy}
            loginPassword={loginPassword}
            newAdminPassword={newAdminPassword}
            passwordMessage={passwordMessage}
            settings={settings}
            theme={theme}
            onAppearanceChange={changeAppearance}
            onChangeAdminPassword={changeAdminPassword}
            onClose={() => setSettingsOpen(false)}
            onLogin={login}
            onLogout={logout}
            onPasswordChange={setLoginPassword}
            onRescan={runScan}
            onSaveSettings={saveSettings}
            onTagImport={runTagImport}
            onThemeChange={changeTheme}
            onNewAdminPasswordChange={setNewAdminPassword}
            liquid={isLiquid}
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
            onProgressSaved={syncProgress}
            resumePosition={readerResume ? readerPositionOverride ?? historyByWorkId.get(detail.work.id)?.position ?? null : "start"}
            liquid={isLiquid}
            comicAutoReadIntervalMs={comicAutoReadIntervalMs}
          />
        )}
      </AnimatePresence>
    </div>
    </GlassFilterProvider>
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
  onKindChange,
  onSelectedTagChange,
  onTagFiltersChange,
  onTagLanguageChange,
  onTagQueryChange
}: {
  counts: Record<string, number>;
  includeTags: string[];
  kind: KindFilter;
  selectedTag: Tag | null;
  tagFilters: Record<string, TagFilterMode>;
  tagLanguage: TagLanguage;
  tagQuery: string;
  visibleTags: Tag[];
  onKindChange: (kind: KindFilter) => void;
  onSelectedTagChange: (tag: Tag | null) => void;
  onTagFiltersChange: (next: Record<string, TagFilterMode> | ((prev: Record<string, TagFilterMode>) => Record<string, TagFilterMode>)) => void;
  onTagLanguageChange: (next: TagLanguage | ((prev: TagLanguage) => TagLanguage)) => void;
  onTagQueryChange: (value: string) => void;
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
  onSettingsOpen: () => void;
  onViewModeChange: (value: ViewMode) => void;
}) {
  return (
    <>
      <div className="toolbar-left">
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

function AuthControls({
  auth,
  busy,
  message,
  newPassword,
  password,
  onChangePassword,
  onLogin,
  onLogout,
  onNewPasswordChange,
  onPasswordChange
}: {
  auth: AuthSession;
  busy: boolean;
  message: string | null;
  newPassword: string;
  password: string;
  onChangePassword: () => void;
  onLogin: () => void;
  onLogout: () => void;
  onNewPasswordChange: (value: string) => void;
  onPasswordChange: (value: string) => void;
}) {
  if (auth.authenticated) {
    return (
      <div className="auth-panel">
        <div className="auth-pill">
          <KeyRound size={15} />
          <span>{auth.user ?? "admin"}</span>
          <button className="icon-btn compact" disabled={busy} onClick={onLogout} aria-label="退出登录">
            <LogOut size={15} />
          </button>
        </div>
        <form
          className="auth-form auth-form-wide"
          onSubmit={(event) => {
            event.preventDefault();
            onChangePassword();
          }}
        >
          <KeyRound size={15} />
          <input
            autoComplete="new-password"
            placeholder="新的管理员密码"
            type="password"
            value={newPassword}
            onChange={(event) => onNewPasswordChange(event.target.value)}
          />
          <button className="auth-submit" disabled={busy || newPassword.trim().length < 8} type="submit">
            {busy ? <Loader2 className="spin" size={15} /> : <KeyRound size={15} />}
            <span>修改密码</span>
          </button>
        </form>
        {message && <span className="settings-message">{message}</span>}
      </div>
    );
  }

  return (
    <div className="auth-panel">
      <form
        className="auth-form auth-form-wide"
        onSubmit={(event) => {
          event.preventDefault();
          onLogin();
        }}
      >
        <KeyRound size={15} />
        <input
          autoComplete="current-password"
          placeholder="管理员密码"
          type="password"
          value={password}
          onChange={(event) => onPasswordChange(event.target.value)}
        />
        <button className="icon-btn compact" disabled={busy || !password.trim()} aria-label="登录">
          {busy ? <Loader2 className="spin" size={15} /> : <KeyRound size={15} />}
        </button>
      </form>
      <div className="auth-help-row"><span>忘记密码时请在服务器端更新配置或持久化密码文件。</span></div>
      {message && <span className="settings-message">{message}</span>}
    </div>
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
  auth,
  authBusy,
  busy,
  loginPassword,
  liquid = false,
  newAdminPassword,
  passwordMessage,
  settings,
  theme,
  onChangeAdminPassword,
  onClose,
  onLogin,
  onLogout,
  onNewAdminPasswordChange,
  onPasswordChange,
  onRescan,
  onSaveSettings,
  onTagImport,
  onThemeChange,
  onAppearanceChange
}: {
  auth: AuthSession;
  authBusy: boolean;
  busy: boolean;
  loginPassword: string;
  liquid?: boolean;
  newAdminPassword: string;
  passwordMessage: string | null;
  settings: AppSettings | null;
  theme: ThemeMode;
  onChangeAdminPassword: () => void;
  onClose: () => void;
  onLogin: () => void;
  onLogout: () => void;
  onNewAdminPasswordChange: (value: string) => void;
  onPasswordChange: (value: string) => void;
  onRescan: () => void;
  onSaveSettings: (settings: AppSettings) => void;
  onTagImport: () => void;
  onThemeChange: (value: ThemeMode) => void;
  onAppearanceChange: (next: Partial<AppearanceState>) => void;
}) {
  const [draft, setDraft] = useState<AppSettings | null>(settings);
  const [dirInputs, setDirInputs] = useState({ comics: "", novels: "", audio: "", gallery: "", coser_picture: "" });
  const [cloudInput, setCloudInput] = useState({
    kind: "comic" as AppSettings["media_sources"][number]["kind"],
    mount_name: "qms",
    root: "",
    scan_depth: "12"
  });
  const [cloudMessage, setCloudMessage] = useState<string | null>(null);
  const [cloudBusy, setCloudBusy] = useState(false);
  const [cloudStatus, setCloudStatus] = useState<{ bytes: number; files: number } | null>(null);

  useEffect(() => {
    setDraft(settings ? normalizeSettingsDraft(settings) : null);
  }, [settings]);

  useEffect(() => {
    if (!auth.authenticated) return;
    api
      .cloudStatus()
      .then((status) => setCloudStatus(status.cache))
      .catch(() => setCloudStatus(null));
  }, [auth.authenticated, settings]);

  const updateDraft = (updater: (value: AppSettings) => AppSettings) => {
    setDraft((prev) => (prev ? updater(prev) : prev));
  };

  const updateAppearance = (patch: Partial<AppearanceState>) => {
    updateDraft((prev) => ({
      ...prev,
      appearance: {
        ...(prev.appearance ?? defaultAppearance),
        ...patch
      }
    }));
    onAppearanceChange(patch);
  };

  const addDir = (kind: keyof AppSettings["media_dirs"]) => {
    const value = dirInputs[kind].trim();
    if (!value) return;
    updateDraft((prev) => ({
      ...prev,
      media_dirs: {
        ...prev.media_dirs,
        [kind]: prev.media_dirs[kind].includes(value) ? prev.media_dirs[kind] : [...prev.media_dirs[kind], value]
      }
    }));
    setDirInputs((prev) => ({ ...prev, [kind]: "" }));
  };

  const removeDir = (kind: keyof AppSettings["media_dirs"], value: string) => {
    updateDraft((prev) => ({
      ...prev,
      media_dirs: {
        ...prev.media_dirs,
        [kind]: prev.media_dirs[kind].filter((item) => item !== value)
      }
    }));
  };

  const mediaLabels: Record<keyof AppSettings["media_dirs"], string> = {
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

  const addCloudSource = () => {
    const mount = cloudInput.mount_name.trim();
    const root = normalizeStrmRoot(cloudInput.root);
    const scanDepth = Math.min(Math.max(Number.parseInt(cloudInput.scan_depth, 10) || 12, 1), 64);
    if (!mount || !root) return;
    updateDraft((prev) => {
      const source = {
        kind: cloudInput.kind,
        provider: "qmediasync" as const,
        root,
        mount_name: mount,
        enabled: true,
        scan_depth: scanDepth
      };
      const exists = prev.media_sources.some((item) =>
        item.kind === source.kind &&
        item.provider === source.provider &&
        item.root === source.root &&
        item.mount_name === source.mount_name
      );
      return {
        ...prev,
        media_sources: exists ? prev.media_sources : [...prev.media_sources, source],
        qmediasync: {
          ...prev.qmediasync,
          enabled: true,
          strm_roots: prev.qmediasync.strm_roots.includes(root)
            ? prev.qmediasync.strm_roots
            : [...prev.qmediasync.strm_roots, root]
        }
      };
    });
  };

  const removeCloudSource = (index: number) => {
    updateDraft((prev) => ({
      ...prev,
      media_sources: prev.media_sources.filter((_, itemIndex) => itemIndex !== index)
    }));
  };

  const testQMediaSyncRoot = async () => {
    setCloudBusy(true);
    setCloudMessage(null);
    try {
      const res = await api.testQMediaSyncStrmRoot({
        root: normalizeStrmRoot(cloudInput.root),
        kind: cloudInput.kind,
        scan_depth: Number.parseInt(cloudInput.scan_depth, 10) || 12
      });
      setCloudMessage(`STRM 可读：${res.root}，${res.works} 个作品，${res.strm_files} 个 STRM`);
    } catch (err) {
      setCloudMessage(err instanceof Error ? err.message : String(err));
    } finally {
      setCloudBusy(false);
    }
  };

  return (
    <motion.div className="settings-backdrop" initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }}>
      <motion.article className="settings-panel" initial={{ opacity: 0, y: 18, scale: 0.98 }} animate={{ opacity: 1, y: 0, scale: 1 }} exit={{ opacity: 0, y: 18, scale: 0.98 }}>
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
            <h3>本地后台</h3>
            <AuthControls
              auth={auth}
              busy={authBusy}
              message={passwordMessage}
              newPassword={newAdminPassword}
              password={loginPassword}
              onChangePassword={onChangeAdminPassword}
              onLogin={onLogin}
              onLogout={onLogout}
              onNewPasswordChange={onNewAdminPasswordChange}
              onPasswordChange={onPasswordChange}
            />
          </section>

          {auth.authenticated && (
            <>
              <section className="settings-section">
                <h3>外观</h3>
                <div className="segmented">
                  <button className={theme === "light" ? "active" : ""} onClick={() => { onThemeChange("light"); updateDraft((prev) => ({ ...prev, theme: "light" })); }}>
                    <Sun size={16} />
                    <span>浅色</span>
                  </button>
                  <button className={theme === "dark" ? "active" : ""} onClick={() => { onThemeChange("dark"); updateDraft((prev) => ({ ...prev, theme: "dark" })); }}>
                    <Moon size={16} />
                    <span>深色</span>
                  </button>
                </div>
                {draft && (
                  <>
                    <div className="settings-subtitle">界面材质</div>
                    <div className="segmented">
                      <button className={(draft.appearance?.material ?? "liquid") === "liquid" ? "active" : ""} onClick={() => updateAppearance({ material: "liquid" })}>
                        <Sparkles size={16} />
                        <span>液态玻璃</span>
                      </button>
                      <button className={(draft.appearance?.material ?? "liquid") === "classic" ? "active" : ""} onClick={() => updateAppearance({ material: "classic" })}>
                        <LayoutList size={16} />
                        <span>经典</span>
                      </button>
                    </div>
                    <div className="settings-subtitle">玻璃强度</div>
                    <div className="segmented">
                      {(["clear", "standard", "readable"] as GlassIntensity[]).map((value) => (
                        <button
                          className={(draft.appearance?.glass_intensity ?? "standard") === value ? "active" : ""}
                          key={value}
                          onClick={() => updateAppearance({ glass_intensity: value })}
                        >
                          <span>{value === "clear" ? "通透" : value === "readable" ? "清晰" : "标准"}</span>
                        </button>
                      ))}
                    </div>
                  </>
                )}
              </section>

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
                  <p className="settings-hint">用于漫画和 CoserPicture 阅读器的自动翻页按钮，支持 0.5 到 120 秒。</p>
                </section>
              )}

              {draft && (
                <section className="settings-section">
                  <h3>媒体目录</h3>
                  {(["comics", "novels", "audio", "gallery", "coser_picture"] as Array<keyof AppSettings["media_dirs"]>).map((dirKind) => (
                    <div className="directory-editor" key={dirKind}>
                      <b>{mediaLabels[dirKind]}</b>
                      <div className="directory-add">
                        <input
                          value={dirInputs[dirKind]}
                          onChange={(event) => {
                            const value = event.currentTarget.value;
                            setDirInputs((prev) => ({ ...prev, [dirKind]: value }));
                          }}
                          placeholder="D:\\Media\\..."
                        />
                        <button className="icon-btn compact" onClick={() => addDir(dirKind)} aria-label="添加目录">
                          <FolderPlus size={15} />
                        </button>
                      </div>
                      <div className="directory-list">
                        {draft.media_dirs[dirKind].map((path) => (
                          <span key={path}>
                            <em>{path}</em>
                            <button className="icon-btn compact" onClick={() => removeDir(dirKind, path)} aria-label="移除目录配置">
                              <FolderMinus size={15} />
                            </button>
                          </span>
                        ))}
                      </div>
                    </div>
                  ))}
                </section>
              )}

              {draft && (
                <section className="settings-section">
                  <h3>封面缓存目录</h3>
                  {(["comic", "novel", "audio", "gallery", "coser_picture"] as Array<keyof AppSettings["cover_cache_dirs"]>).map((cacheKind) => (
                    <label className="setting-field" key={cacheKind}>
                      <span>{coverCacheLabels[cacheKind]}</span>
                      <input
                        value={draft.cover_cache_dirs[cacheKind]}
                        onChange={(event) => {
                          const value = event.currentTarget.value;
                          updateDraft((prev) => ({
                            ...prev,
                            cover_cache_dirs: {
                              ...prev.cover_cache_dirs,
                              [cacheKind]: value
                            }
                          }));
                        }}
                        placeholder="D:\\ArisList\\cover-cache\\..."
                      />
                    </label>
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
                    {cloudMessage && <p className="settings-hint">{cloudMessage}</p>}
                    {cloudStatus && (
                      <p className="settings-hint">
                        云缓存 {formatBytes(cloudStatus.bytes)} / {cloudStatus.files} 文件
                      </p>
                    )}
                    <div className="cloud-source-add">
                      <select
                        value={cloudInput.kind}
                        onChange={(event) => {
                          const value = event.currentTarget.value as typeof cloudInput.kind;
                          setCloudInput((prev) => ({ ...prev, kind: value }));
                        }}
                      >
                        {(Object.keys(cloudKindLabels) as Array<typeof cloudInput.kind>).map((kind) => (
                          <option key={kind} value={kind}>{cloudKindLabels[kind]}</option>
                        ))}
                      </select>
                      <input
                        value={cloudInput.mount_name}
                        onChange={(event) => {
                          const value = event.currentTarget.value;
                          setCloudInput((prev) => ({ ...prev, mount_name: value }));
                        }}
                        placeholder="挂载名"
                      />
                      <input
                        value={cloudInput.root}
                        onChange={(event) => {
                          const value = event.currentTarget.value;
                          setCloudInput((prev) => ({ ...prev, root: value }));
                        }}
                        placeholder="STRM 根目录，例如 D:\\qms\\comics 或 /qms-strm/comics"
                      />
                      <input
                        value={cloudInput.scan_depth}
                        onChange={(event) => {
                          const value = event.currentTarget.value;
                          setCloudInput((prev) => ({ ...prev, scan_depth: value }));
                        }}
                        min={1}
                        max={64}
                        type="number"
                      />
                      <button className="icon-btn compact" disabled={cloudBusy || !cloudInput.root.trim()} onClick={testQMediaSyncRoot} aria-label="测试 STRM 目录">
                        {cloudBusy ? <Loader2 className="spin" size={15} /> : <Cloud size={15} />}
                      </button>
                      <button className="icon-btn compact" onClick={addCloudSource} aria-label="添加云盘源">
                        <FolderPlus size={15} />
                      </button>
                    </div>
                    <div className="directory-list cloud-source-list">
                      {draft.media_sources.map((source, index) => (
                        <span key={`${source.provider}-${source.kind}-${source.mount_name}-${source.root}`}>
                          <em>{cloudKindLabels[source.kind]} · {source.mount_name}:{source.root} · 深度 {source.scan_depth}</em>
                          <button className="icon-btn compact" onClick={() => removeCloudSource(index)} aria-label="移除云盘源">
                            <FolderMinus size={15} />
                          </button>
                        </span>
                      ))}
                    </div>
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
          )}
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
  viewMode: ViewMode;
};

function VirtualShelf<T>({ items, itemKey, renderItem, viewMode }: VirtualShelfProps<T>) {
  const ref = useRef<HTMLElement | null>(null);
  const [scrollTop, setScrollTop] = useState(0);
  const [viewport, setViewport] = useState({ width: 0, height: 0 });
  const gap = viewMode === "list" ? 9 : 12;
  const targetWidth = viewMode === "compact" ? 138 : viewMode === "cover" ? 240 : 182;
  const columns = viewMode === "list" ? 1 : Math.max(1, Math.floor((viewport.width + gap) / (targetWidth + gap)));
  const columnWidth = columns > 0 ? Math.max(viewMode === "compact" ? 120 : 160, (viewport.width - gap * (columns - 1)) / columns) : targetWidth;
  const copyReserve = viewMode === "cover" ? 88 : Math.max(104, Math.min(168, columnWidth * 0.42 + 20));
  const rowHeight = viewMode === "list" ? 128 + gap : Math.ceil(columnWidth * (4 / 3) + copyReserve + gap);
  const rowCount = Math.ceil(items.length / columns);
  const overscanRows = 4;
  const startRow = Math.max(0, Math.floor(scrollTop / rowHeight) - overscanRows);
  const endRow = Math.min(rowCount, Math.ceil((scrollTop + viewport.height) / rowHeight) + overscanRows);
  const startIndex = startRow * columns;
  const endIndex = Math.min(items.length, endRow * columns);
  const visibleItems = items.slice(startIndex, endIndex);
  const totalHeight = Math.max(0, rowCount * rowHeight - gap);
  const offsetY = startRow * rowHeight;

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
    "--virtual-columns": columns,
    "--virtual-gap": `${gap}px`,
    "--virtual-item-height": `${Math.max(80, rowHeight - gap)}px`,
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
            <AnimatePresence mode="popLayout">
              {visibleItems.map((item, localIndex) => (
                <div className="virtual-shelf-cell" key={itemKey(item)}>
                  {renderItem(item, startIndex + localIndex)}
                </div>
              ))}
            </AnimatePresence>
          </div>
        </div>
      )}
    </section>
  );
}

function CoverImage({ kind, loading = "lazy", src }: { kind: string; loading?: "eager" | "lazy"; src: string }) {
  const [failed, setFailed] = useState(false);

  useEffect(() => {
    setFailed(false);
  }, [src]);

  if (!src || failed) return <FallbackCover kind={kind} />;
  return <img src={src} alt="" loading={loading} onError={() => setFailed(true)} />;
}

function WorkCard({ work, index, selected, viewMode, onClick }: { work: WorkSummary; index: number; selected: boolean; viewMode: ViewMode; onClick: () => void }) {
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
    <motion.button
      layout
      className={selected ? "work-card selected" : "work-card"}
      data-view={viewMode}
      onClick={onClick}
      initial={{ opacity: 0, y: 18 }}
      animate={{ opacity: 1, y: 0 }}
      exit={{ opacity: 0, scale: 0.98 }}
      transition={{ delay: Math.min(index * 0.018, 0.18), duration: 0.22 }}
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
    </motion.button>
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
  onOpenReader,
  liquid = false
}: {
  detail: WorkDetail | null;
  jobs: Job[];
  tagLanguage: TagLanguage;
  variant: "modal" | "docked";
  onClose: () => void;
  onTagPick: (key: string) => void;
  onPlayTrack: (work: WorkDetail["work"], asset: Asset, playlist?: Asset[]) => void;
  onOpenReader: (resume?: boolean) => void;
  liquid?: boolean;
}) {
  const [jobOpen, setJobOpen] = useState(false);
  const tracks = detail?.assets.filter((asset) => asset.role === "track") ?? [];
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
      <AnimatePresence mode="wait">
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
              {groupedTags.map((group) => (
                <div className="tag-group" key={group.namespace}>
                  <span className="tag-group-name">{group.namespace}</span>
                  <div className="tag-group-items">
                    {group.tags.map((tag) => (
                      <button key={tagKey(tag)} onClick={() => onTagPick(tagKey(tag))}>
                        {tagLabel(tag, tagLanguage)}
                      </button>
                    ))}
                  </div>
                </div>
              ))}
            </div>
            {detail.work.description && <p className="description">{detail.work.description}</p>}
            {displayTracks.length > 0 && (
              <div className="track-stack">
                {displayTracks.slice(0, 10).map((track) => {
                  const trackMeta = parseMeta<{ title?: string; quality?: string }>(track.meta_json);
                  return (
                    <div className="track-line" key={track.id}>
                      <button className="inline-play" onClick={() => detail && onPlayTrack(detail.work, track, displayTracks)} aria-label="播放">
                        <Play size={14} />
                      </button>
                      <span>{trackMeta.title ?? shortName(track.path)}</span>
                      <b>{trackMeta.quality ?? track.variant}</b>
                    </div>
                  );
                })}
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

  return liquid ? (
    <GlassSurface as="aside" className={detailClassName} variant={variant === "modal" ? "floating" : "panel"} onClick={(event) => event.stopPropagation()}>
      {detailContent}
    </GlassSurface>
  ) : (
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
  resumePosition,
  liquid = false
}: {
  active: ActiveAudioState | null;
  canPersistProgress: boolean;
  onClose: () => void;
  onProgressSaved: (id: number, progress: number, position?: string | null) => void;
  resumePosition?: string | null;
  liquid?: boolean;
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
  const meta = parseMeta<{ title?: string }>(currentAsset?.meta_json);
  const playlist = useMemo(() => active?.playlist ?? (active?.asset ? [active.asset] : []), [active]);
  const currentIndex = Math.max(0, playlist.findIndex((asset) => asset.id === currentAsset?.id));
  const progressPercent = duration > 0 ? Math.min(100, Math.max(0, (currentTime / duration) * 100)) : 0;
  const repeatLabel = repeatMode === "one" ? "单曲循环" : repeatMode === "all" ? "列表循环" : "不循环";
  const { flush: flushProgress, schedule: scheduleProgress } = useProgressQueue(
    active?.work.id ?? 0,
    canPersistProgress && Boolean(active),
    onProgressSaved,
    1200
  );

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

  const changeTrack = (offset: number, flushCurrent = true) => {
    if (playlist.length === 0) return false;
    const audio = audioRef.current;
    if (flushCurrent && audio) {
      saveAudioProgress(audio.currentTime, audio.duration || duration, audio.ended, true);
      void flushProgress();
    }
    const nextIndex = currentIndex + offset;
    if (nextIndex < 0 || nextIndex >= playlist.length) {
      if (repeatMode !== "all") return false;
      setCurrentAsset(playlist[(nextIndex + playlist.length) % playlist.length]);
      lastProgressWrite.current = 0;
      return true;
    }
    setCurrentAsset(playlist[nextIndex]);
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

  const audioContent = (
    <>
      <button className="icon-btn compact audio-queue-toggle" onClick={() => setQueueOpen((value) => !value)} aria-label="播放列表">
        <ListMusic size={15} />
      </button>
      <span className="audio-title">{meta.title ?? shortName(currentAsset.path)}</span>
      <div className="audio-controls">
        <button className="icon-btn compact" onClick={() => changeTrack(-1)} disabled={playlist.length < 2 && repeatMode !== "all"} aria-label="上一首">
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
        <button className="icon-btn compact" onClick={() => changeTrack(1)} disabled={playlist.length < 2 && repeatMode !== "all"} aria-label="下一首">
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
          if (!changeTrack(1, false)) onClose();
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
              <span>{currentIndex + 1}/{playlist.length}</span>
            </div>
            {playlist.map((asset, index) => {
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
                  <em>{index + 1}</em>
                  <span>{trackMeta.title ?? shortName(asset.path)}</span>
                </button>
              );
            })}
          </motion.div>
        )}
      </AnimatePresence>
    </>
  );

  return liquid ? (
    <motion.div className="audio-dock-motion" initial={{ y: 80 }} animate={{ y: 0 }} exit={{ y: 80 }}>
      <GlassSurface className="audio-dock" variant="dock">
        {audioContent}
      </GlassSurface>
    </motion.div>
  ) : (
    <motion.div className="audio-dock" initial={{ y: 80 }} animate={{ y: 0 }} exit={{ y: 80 }}>
      {audioContent}
    </motion.div>
  );
}

function ReaderOverlay({
  canPersistProgress,
  comicAutoReadIntervalMs = defaultReaderSettings.comic_auto_read_interval_ms,
  detail,
  onClose,
  onProgressSaved,
  resumePosition,
  liquid = false
}: {
  canPersistProgress: boolean;
  comicAutoReadIntervalMs?: number;
  detail: WorkDetail;
  onClose: () => void;
  onProgressSaved: (id: number, progress: number, position?: string | null) => void;
  resumePosition?: string | null;
  liquid?: boolean;
}) {
  const [pages, setPages] = useState<ComicPageInfo[]>([]);
  const [page, setPage] = useState(0);
  const [comicMode, setComicMode] = useState<ComicReaderMode>("horizontal");
  const [comicZoom, setComicZoom] = useState(1);
  const [comicViewport, setComicViewport] = useState({ width: 0, height: 0 });
  const [comicScrollLeft, setComicScrollLeft] = useState(0);
  const [comicScrollTop, setComicScrollTop] = useState(0);
  const [comicAutoRead, setComicAutoRead] = useState(false);
  const [comicError, setComicError] = useState<string | null>(null);
  const [readerChromeVisible, setReaderChromeVisible] = useState(false);
  const lastAudioProgressWrite = useRef(0);
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
  const activeReaderAudioRef = useRef<{ asset: Asset; element: HTMLAudioElement } | null>(null);
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
          setPages(res.pages.map(normalizeComicPageInfo));
          setComicError(null);
        })
        .catch((err) => {
          if (err instanceof DOMException && err.name === "AbortError") return;
          setPages([]);
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
  const comicSlotWidth = comicHorizontalSlotWidthFromSize(comicMeasuredWidth, comicMeasuredHeight, comicAspect, comicZoom);
  const comicWindowStart = comicMode === "horizontal" && comicMeasuredWidth > 0
    ? Math.max(0, Math.floor(comicScrollLeft / comicSlotWidth) - COMIC_HORIZONTAL_OVERSCAN)
    : 0;
  const comicWindowEnd = comicMode === "horizontal"
    ? comicMeasuredWidth > 0
      ? Math.min(
        pages.length,
        Math.ceil((comicScrollLeft + comicMeasuredWidth) / comicSlotWidth) + COMIC_HORIZONTAL_OVERSCAN
      )
      : Math.min(pages.length, COMIC_HORIZONTAL_OVERSCAN * 2 + 1)
    : pages.length;
  const horizontalComicIndexes = useMemo(
    () => Array.from({ length: Math.max(0, comicWindowEnd - comicWindowStart) }, (_, index) => comicWindowStart + index),
    [comicWindowEnd, comicWindowStart]
  );
  const comicTotalWidth = comicMode === "horizontal" ? comicSlotWidth * pages.length : 0;
  const comicVerticalMetrics = useMemo(() => {
    const horizontalPadding = Math.max(14, comicMeasuredWidth * 0.05) * 2;
    const imageWidth = Math.max(1, comicMeasuredWidth - horizontalPadding) * comicZoom;
    const offsets = new Array<number>(pages.length + 1);
    offsets[0] = 0;
    for (let index = 0; index < pages.length; index += 1) {
      offsets[index + 1] = offsets[index] + imageWidth / comicPageAspect(pages[index]) + 16;
    }
    return { imageWidth, offsets, totalHeight: offsets[pages.length] ?? 0 };
  }, [comicMeasuredWidth, comicZoom, pages]);
  const comicVerticalWindowStart = comicMode === "scroll"
    ? Math.max(0, comicPageFromOffsets(comicVerticalMetrics.offsets, comicScrollTop) - COMIC_VERTICAL_OVERSCAN)
    : 0;
  const comicVerticalWindowEnd = comicMode === "scroll"
    ? Math.min(
      pages.length,
      comicPageFromOffsets(
        comicVerticalMetrics.offsets,
        comicScrollTop + Math.max(1, comicMeasuredHeight)
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
    if (!isArchiveReader || pages.length === 0 || resumeAppliedRef.current) return;
    let target = resumeTarget.kind === "page"
      ? resumeTarget.index
      : resumeTarget.kind === "start"
        ? 0
        : Math.floor((detail.work.progress || 0) * Math.max(0, pages.length - 1));
    target = Math.min(Math.max(target, 0), Math.max(0, pages.length - 1));
    setPage(target);
    resumeAppliedRef.current = true;
    suppressNextProgressRef.current = Boolean(resumeTarget.kind && resumeTarget.kind !== "start");
    resumeComicPageRef.current = target;
    comicUserInteractedRef.current = false;
    needsComicResumeScrollRef.current = target > 0;
  }, [detail.work.progress, isArchiveReader, pages.length, resumeTarget]);

  useEffect(() => {
    if (!isArchiveReader || !needsComicResumeScrollRef.current || pages.length < 2 || comicMode === "paged") return;
    const targetPage = resumeComicPageRef.current;
    const delays = [0, 250, 900, 1800, 3200];
    const timers = delays.map((delay, index) =>
      window.setTimeout(() => {
        if (!needsComicResumeScrollRef.current || comicUserInteractedRef.current) return;
        scrollComicStageToPage(
          comicStageRef.current,
          targetPage,
          pages.length,
          comicAspect,
          comicZoom,
          comicVerticalMetrics.offsets
        );
        if (index === delays.length - 1) {
          needsComicResumeScrollRef.current = false;
        }
      }, delay)
    );
    return () => timers.forEach((timer) => window.clearTimeout(timer));
  }, [comicAspect, comicMode, comicVerticalMetrics.offsets, comicZoom, isArchiveReader, pages.length]);

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

  const comicLayoutKey = `${comicMode}:${comicZoom}:${comicMeasuredWidth}:${comicMeasuredHeight}:${pages.length}`;
  useLayoutEffect(() => {
    const previous = comicLayoutKeyRef.current;
    comicLayoutKeyRef.current = comicLayoutKey;
    if (
      previous === null ||
      previous === comicLayoutKey ||
      !isArchiveReader ||
      !resumeAppliedRef.current ||
      pages.length === 0 ||
      comicMode === "paged"
    ) return;
    const stage = comicStageRef.current;
    if (!stage) return;
    const targetPage = Math.min(Math.max(page, 0), pages.length - 1);
    const frame = window.requestAnimationFrame(() => {
      scrollComicStageToPage(
        stage,
        targetPage,
        pages.length,
        comicAspect,
        comicZoom,
        comicVerticalMetrics.offsets
      );
    });
    return () => window.cancelAnimationFrame(frame);
  }, [comicAspect, comicLayoutKey, comicMode, comicVerticalMetrics.offsets, comicZoom, isArchiveReader, pages.length]);

  useEffect(() => {
    if (!isArchiveReader || pages.length === 0) return;
    if (!resumeAppliedRef.current) return;
    if (suppressNextProgressRef.current) {
      suppressNextProgressRef.current = false;
      return;
    }
    persistProgress((page + 1) / pages.length, `page:${page}`);
  }, [isArchiveReader, page, pages.length]);

  const moveComic = (offset: number) => {
    setPage((value) => Math.min(Math.max(value + offset, 0), Math.max(0, pages.length - 1)));
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
      const targetPage = Math.min(Math.max(page + offset, 0), Math.max(0, pages.length - 1));
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
    if (!isArchiveReader || !comicAutoRead || pages.length === 0) return;
    if (page >= pages.length - 1) {
      setComicAutoRead(false);
      return;
    }
    const timer = window.setTimeout(() => {
      navigateComic(1, "auto");
    }, safeComicAutoReadIntervalMs);
    return () => window.clearTimeout(timer);
  }, [comicAspect, comicAutoRead, comicMode, comicZoom, isArchiveReader, page, pages.length, safeComicAutoReadIntervalMs]);

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
    if ((comicMode !== "scroll" && comicMode !== "horizontal") || pages.length < 2) return;
    const target = event.currentTarget;
    const nextPage = comicMode === "horizontal"
      ? comicPageFromHorizontalScroll(target, pages.length, comicAspect, comicZoom)
      : comicPageFromOffsets(
        comicVerticalMetrics.offsets,
        target.scrollTop + target.clientHeight / 2
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
      comicPageFromHorizontalScroll(stage, pages.length, comicAspect, comicZoom)
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
  }, [closeReader, comicAspect, comicMode, comicZoom, isArchiveReader, isGallery, page, pages.length]);

  const saveTrackProgress = useCallback((asset: Asset, currentTime: number, duration: number, ended = false, force = false) => {
    if (!canPersistProgress || !Number.isFinite(duration) || duration <= 0) return;
    const now = Date.now();
    if (!ended && !force && now - lastAudioProgressWrite.current < 12000) return;
    lastAudioProgressWrite.current = now;
    const progress = ended ? 1 : Math.min(0.995, Math.max(0, currentTime / duration));
    persistProgress(progress, `track:${asset.id}:${Math.round(currentTime)}`, ended || force);
  }, [canPersistProgress, persistProgress]);

  useLayoutEffect(() => {
    return () => {
      const activeAudio = activeReaderAudioRef.current;
      if (activeAudio) {
        const { asset, element } = activeAudio;
        saveTrackProgress(asset, element.currentTime, element.duration, element.ended, true);
      }
      void flushProgress();
    };
  }, [detail.work.id, flushProgress, saveTrackProgress]);

  const readerActionsContent = (
    <>
        {isArchiveReader && <b>{pages.length ? `${page + 1}/${pages.length}` : "0/0"}</b>}
        {isArchiveReader && pages.length > 0 && (
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
  const liquidReaderBarClassName = [
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
        {!isNovel && (liquid ? (
          <div className={liquidReaderBarClassName}>
            <GlassSurface className="reader-back-surface" variant="dock">
              <button className="icon-btn reader-back-button" onClick={closeReader} aria-label="关闭">
                <ChevronLeft size={18} />
              </button>
            </GlassSurface>
            {!isGallery && (
              <GlassSurface className="reader-title-surface" variant="dock">
                <span className="reader-title-pill">{detail.work.title}</span>
              </GlassSurface>
            )}
            <GlassSurface className="reader-actions-surface" variant="dock">
              <div className="reader-actions">{readerActionsContent}</div>
            </GlassSurface>
          </div>
        ) : (
          <div className="reader-bar">{readerBarContent}</div>
        ))}
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
            {comicError && <div className="reader-error archive-reader-error">{comicError}</div>}
            {pages.length > 0 && comicMode === "paged" ? (
              <motion.img
                key={page}
                src={comicPageUrl(detail.work.id, page, comicArchiveVersion)}
                alt=""
                initial={{ opacity: 0, x: 16 }}
                animate={{ opacity: 1, x: 0 }}
                style={{ position: "absolute", inset: 0, width: "100%", height: "100%", maxWidth: "100%", maxHeight: "100%", objectFit: "contain" }}
              />
            ) : pages.length > 0 ? (
              comicMode === "horizontal" ? (
                <div className="comic-strip-spacer" style={{ width: `${comicTotalWidth}px` }}>
                  <div
                    className="comic-strip-window"
                    style={{ transform: `translateX(${Math.round(comicWindowStart * comicSlotWidth)}px)` }}
                  >
                    {horizontalComicIndexes.map((index) => {
                      const comicPage = pages[index];
                      return (
                        <div
                          className="comic-page-slot"
                          data-page-index={index}
                          key={comicPage.name || index}
                          style={{
                            height: `${Math.round(comicZoom * 100)}%`,
                            width: `${comicSlotWidth}px`
                          } as CSSProperties}
                        >
                          <img
                            alt=""
                            loading="lazy"
                            src={comicPageUrl(detail.work.id, index, comicArchiveVersion)}
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
                      transform: `translateY(${Math.round(comicVerticalMetrics.offsets[comicVerticalWindowStart] ?? 0)}px)`
                    }}
                  >
                    {verticalComicIndexes.map((index) => {
                      const comicPage = pages[index];
                      const slotHeight = Math.max(
                        1,
                        (comicVerticalMetrics.offsets[index + 1] ?? 0) -
                        (comicVerticalMetrics.offsets[index] ?? 0) - 16
                      );
                      return (
                        <div
                          className="comic-vertical-slot"
                          data-page-index={index}
                          key={comicPage.name || index}
                          style={{ height: `${slotHeight}px` }}
                        >
                          <img
                            alt=""
                            loading="lazy"
                            src={comicPageUrl(detail.work.id, index, comicArchiveVersion)}
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
                  initial={{ opacity: 0, y: 12 }}
                  animate={{ opacity: 1, y: 0 }}
                  transition={{ delay: Math.min(index * 0.025, 0.2), duration: 0.2 }}
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
            {detail.assets
              .filter((asset) => asset.role === "track")
              .map((asset) => (
                <div className="track-line" key={asset.id}>
                  <Play size={15} />
                  <span>{shortName(asset.path)}</span>
                  <audio
                    controls
                    preload="none"
                    src={assetUrl(asset.id, assetVersion(asset, detail.work.updated_at))}
                    onLoadedMetadata={(event) => {
                      if (resumeTarget.kind === "track" && resumeTarget.assetId === asset.id && resumeTarget.seconds > 0) {
                        event.currentTarget.currentTime = Math.min(resumeTarget.seconds, Math.max(0, event.currentTarget.duration - 0.5));
                      }
                    }}
                    onPlay={(event) => {
                      const previous = activeReaderAudioRef.current;
                      if (previous && previous.element !== event.currentTarget) {
                        saveTrackProgress(
                          previous.asset,
                          previous.element.currentTime,
                          previous.element.duration,
                          previous.element.ended,
                          true
                        );
                        previous.element.pause();
                        void flushProgress();
                      }
                      activeReaderAudioRef.current = { asset, element: event.currentTarget };
                      lastAudioProgressWrite.current = 0;
                    }}
                    onPause={(event) => {
                      saveTrackProgress(asset, event.currentTarget.currentTime, event.currentTarget.duration, event.currentTarget.ended, true);
                      void flushProgress();
                    }}
                    onEnded={(event) => {
                      saveTrackProgress(asset, event.currentTarget.duration, event.currentTarget.duration, true, true);
                      void flushProgress();
                    }}
                    onTimeUpdate={(event) => saveTrackProgress(asset, event.currentTarget.currentTime, event.currentTarget.duration)}
                  />
                </div>
              ))}
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
      const res = await api.galleryPage(detail.work.id, aligned, GALLERY_PAGE_SIZE, controller.signal, galleryVersion);
      if (controller.signal.aborted || fetchControllersRef.current.get(aligned) !== controller) return;
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
    <div className="fallback-cover" data-kind={kind}>
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

function comicPageFromOffsets(offsets: number[], position: number) {
  if (offsets.length <= 1) return 0;
  let low = 0;
  let high = offsets.length - 1;
  while (low < high) {
    const middle = Math.floor((low + high + 1) / 2);
    if (offsets[middle] <= position) low = middle;
    else high = middle - 1;
  }
  return Math.min(Math.max(low, 0), offsets.length - 2);
}

function scrollComicStageToPage(
  stage: HTMLDivElement | null,
  page: number,
  pageCount: number,
  aspect = COMIC_DEFAULT_ASPECT,
  zoom = 1,
  verticalOffsets: number[] = []
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
  if (stage.dataset.mode === "scroll" && verticalOffsets.length > page) {
    stage.scrollTo({ left: 0, top: verticalOffsets[page], behavior: "auto" });
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

function compareWorksByUpdatedAt(left: WorkSummary, right: WorkSummary) {
  const updated = right.updated_at.localeCompare(left.updated_at);
  return updated || right.id - left.id;
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
    appearance: settings.appearance ?? defaultAppearance,
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
      coser_picture: settings.media_dirs?.coser_picture ?? []
    },
    cover_cache_dirs: {
      comic: settings.cover_cache_dirs?.comic ?? "",
      novel: settings.cover_cache_dirs?.novel ?? "",
      audio: settings.cover_cache_dirs?.audio ?? "",
      gallery: settings.cover_cache_dirs?.gallery ?? "",
      coser_picture: settings.cover_cache_dirs?.coser_picture ?? ""
    },
    media_sources: settings.media_sources ?? [],
    qmediasync: settings.qmediasync ?? {
      enabled: false,
      base_url: "",
      strm_roots: []
    }
  };
}

function normalizeStrmRoot(value: string) {
  const trimmed = value.trim();
  if (!trimmed) return "";
  return trimmed.replace(/[\\/]+$/g, "");
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


