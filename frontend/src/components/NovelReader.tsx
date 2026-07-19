import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import {
  AlignJustify,
  BookOpen,
  ChevronLeft,
  ChevronRight,
  Columns3,
  Download,
  FileText,
  Loader2,
  Moon,
  Palette,
  PanelLeftOpen,
  PanelLeftClose,
  Rows3,
  Search,
  Settings,
  Sun,
  Type,
  X
} from "lucide-react";
import { BlobWriter, configure, HttpReader, TextWriter, ZipReader, type Entry } from "@zip.js/zip.js";
import { EPUB } from "foliate-js/epub.js";
import { api, assetUrl, assetVersion, type EpubChapter, type WorkDetail } from "../api";
import { useProgressQueue } from "../hooks/useProgressQueue";

type NovelReaderSettings = {
  theme: "paper" | "dark" | "sepia";
  fontFamily: "serif" | "sans" | "system";
  fontSize: number;
  fontWeight: number;
  lineHeight: number;
  maxInlineSize: number;
  flow: "paginated" | "scrolled";
  pageMode: "single" | "double";
  writingMode: "horizontal-tb" | "vertical-rl";
};

const MAX_EPUB_ENTRIES = 20_000;
const MAX_EPUB_TEXT_ENTRY_BYTES = 16 * 1024 * 1024;
const MAX_EPUB_BLOB_ENTRY_BYTES = 64 * 1024 * 1024;
const MAX_EPUB_ACTIVE_INFLATE_BYTES = 128 * 1024 * 1024;
const EPUB_ABORT_SETTLE_MS = 250;
const EPUB_ABORT_DRAIN_MS = 50;
const EMPTY_EPUB_DOCUMENT = '<?xml version="1.0" encoding="UTF-8"?><html xmlns="http://www.w3.org/1999/xhtml"><head><title></title></head><body></body></html>';

type NovelSearchHit = {
  cfi: string;
  label: string;
  excerpt: string;
};

type NovelReaderProps = {
  canPersistProgress: boolean;
  detail: WorkDetail;
  onClose: () => void;
  onProgressSaved: (id: number, progress: number, position?: string | null) => void;
  resumePosition?: string | null;
};

type NovelResumeTarget =
  | { kind: "epub-cfi"; cfi: string }
  | { kind: "chapter"; index: number }
  | { kind: "start" }
  | { kind: "cover" }
  | { kind: null };

const NOVEL_SETTINGS_KEY = "arislist_novel_reader_settings";
const defaultNovelSettings: NovelReaderSettings = {
  theme: "paper",
  fontFamily: "serif",
  fontSize: 20,
  fontWeight: 400,
  lineHeight: 1.82,
  maxInlineSize: 820,
  flow: "paginated",
  pageMode: "single",
  writingMode: "horizontal-tb"
};

export function NovelReader({ canPersistProgress, detail, onClose, onProgressSaved, resumePosition }: NovelReaderProps) {
  const bookAsset = detail.assets.find((asset) => asset.role === "book" && isEpubAsset(asset));
  const bookAssetId = bookAsset?.id ?? null;
  const bookVersion = assetVersion(bookAsset, detail.work.updated_at);
  const coverAsset = detail.assets.find((asset) => asset.id === detail.work.cover_asset_id);
  const coverAssetUrl = assetUrl(detail.work.cover_asset_id, assetVersion(coverAsset, detail.work.updated_at));
  const [settings, setSettings] = useState<NovelReaderSettings>(loadNovelSettings);
  const [engineError, setEngineError] = useState<string | null>(null);
  const [fallbackMode, setFallbackMode] = useState(false);
  const [loading, setLoading] = useState(true);
  const [toc, setToc] = useState<Array<FoliateTOCItem & { depth: number }>>([]);
  const [activePanel, setActivePanel] = useState<"search" | "settings" | null>(null);
  const [sidebarOpen, setSidebarOpen] = useState(true);
  const [chromeVisible, setChromeVisible] = useState(true);
  const [progress, setProgress] = useState({ fraction: detail.work.progress || 0, page: 0, total: 0, label: "" });
  const [searchTerm, setSearchTerm] = useState("");
  const [searchHits, setSearchHits] = useState<NovelSearchHit[]>([]);
  const [searchStatus, setSearchStatus] = useState<"idle" | "searching" | "done" | "error">("idle");
  const [searchProgress, setSearchProgress] = useState(0);
  const { flush: flushProgress, schedule: scheduleProgress } = useProgressQueue(
    detail.work.id,
    canPersistProgress,
    onProgressSaved,
    1200
  );
  const activeTocIndex = useMemo(() => toc.findIndex((item) => progress.label && item.label === progress.label), [progress.label, toc]);
  const containerRef = useRef<HTMLDivElement | null>(null);
  const viewRef = useRef<FoliateView | null>(null);
  const chromeTimerRef = useRef<number | null>(null);
  const rafRef = useRef<number | null>(null);
  const searchTokenRef = useRef(0);
  const searchIteratorRef = useRef<AsyncGenerator<FoliateSearchResult> | null>(null);
  const pendingRelocateRef = useRef<FoliateRelocateDetail | null>(null);
  const activePanelRef = useRef<typeof activePanel>(activePanel);
  const settingsRef = useRef<NovelReaderSettings>(settings);
  const wheelTurnRef = useRef(0);
  const pageTurnBusyRef = useRef(false);

  useEffect(() => {
    settingsRef.current = settings;
  }, [settings]);

  useEffect(() => {
    activePanelRef.current = activePanel;
  }, [activePanel]);

  const cancelSearch = useCallback((resetUi = false) => {
    const token = searchTokenRef.current + 1;
    searchTokenRef.current = token;
    const iterator = searchIteratorRef.current;
    const view = viewRef.current;
    view?.clearSearch?.();
    const stopped = iterator
      ? iterator.return(undefined).then(
          () => view?.clearSearch?.(),
          () => view?.clearSearch?.()
        )
      : Promise.resolve();
    if (resetUi) {
      setSearchHits([]);
      setSearchProgress(0);
      setSearchStatus("idle");
    }
    return { stopped, token };
  }, []);

  const clearChromeTimer = useCallback(() => {
    if (chromeTimerRef.current !== null) {
      window.clearTimeout(chromeTimerRef.current);
      chromeTimerRef.current = null;
    }
  }, []);

  const scheduleChromeHide = useCallback(() => {
    clearChromeTimer();
    chromeTimerRef.current = window.setTimeout(() => {
      if (!activePanelRef.current) setChromeVisible(false);
      chromeTimerRef.current = null;
    }, 2600);
  }, [clearChromeTimer]);

  const revealChrome = useCallback(
    (autoHide = true) => {
      setChromeVisible(true);
      clearChromeTimer();
      if (autoHide) scheduleChromeHide();
    },
    [clearChromeTimer, scheduleChromeHide]
  );

  useEffect(() => {
    if (activePanel) {
      revealChrome(false);
      return;
    }
    scheduleChromeHide();
  }, [activePanel, revealChrome, scheduleChromeHide]);

  useEffect(() => {
    revealChrome();
    return () => {
      clearChromeTimer();
    };
  }, [clearChromeTimer, revealChrome]);

  const persistProgress = useCallback(
    (nextProgress: number, position: string, immediate = false) => {
      scheduleProgress(nextProgress, position, immediate);
    },
    [scheduleProgress]
  );

  const turnPage = useCallback(
    (direction: "prev" | "next") => {
      const view = viewRef.current;
      if (!view || pageTurnBusyRef.current) return;
      pageTurnBusyRef.current = true;
      revealChrome();
      void (async () => {
        try {
          const move = direction === "prev" ? view.prev.bind(view) : view.next.bind(view);
          await runFoliateMove(move);
        } finally {
          window.setTimeout(() => {
            pageTurnBusyRef.current = false;
          }, 80);
        }
      })();
    },
    [revealChrome]
  );

  const handleWheelTurn = useCallback(
    (event: Pick<WheelEvent, "deltaX" | "deltaY" | "preventDefault" | "stopPropagation"> & { stopImmediatePropagation?: () => void }) => {
      if (activePanelRef.current || settingsRef.current.flow !== "paginated") return false;
      const absY = Math.abs(event.deltaY);
      if (absY < 18 || absY < Math.abs(event.deltaX)) return false;
      event.preventDefault();
      event.stopPropagation();
      event.stopImmediatePropagation?.();
      const now = window.performance.now();
      if (now - wheelTurnRef.current < 420) return true;
      wheelTurnRef.current = now;
      turnPage(event.deltaY > 0 ? "next" : "prev");
      return true;
    },
    [turnPage]
  );

  const tryEdgePageTurn = useCallback(
    (x: number, width: number, event: Pick<MouseEvent, "preventDefault" | "stopPropagation"> | React.MouseEvent<HTMLElement>) => {
      if (activePanelRef.current || !isDoublePageSettings(settingsRef.current) || width <= 0) return false;
      const edgeWidth = Math.min(220, Math.max(96, width * 0.14));
      if (x > edgeWidth && x < width - edgeWidth) return false;
      event.preventDefault();
      event.stopPropagation();
      turnPage(x <= edgeWidth ? "prev" : "next");
      return true;
    },
    [turnPage]
  );

  useEffect(() => {
    window.localStorage.setItem(NOVEL_SETTINGS_KEY, JSON.stringify(settings));
  }, [settings]);

  useEffect(() => {
    const controller = new AbortController();
    let sourceBook: Awaited<ReturnType<typeof openEpubBook>> | null = null;
    let sourceView: FoliateView | null = null;
    let coverObjectUrl: string | null = null;
    let disposed = false;
    let closing = false;
    let openTask: Promise<void> | null = null;
    setFallbackMode(false);
    setEngineError(null);
    setLoading(true);
    setToc([]);
    setSearchHits([]);
    setSearchStatus("idle");
    setProgress({ fraction: detail.work.progress || 0, page: 0, total: 0, label: "" });
    pendingRelocateRef.current = null;

    const disposeFoliate = async () => {
      if (disposed) return;
      disposed = true;
      await cancelSearch().stopped;
      const view = sourceView;
      sourceView = null;
      if (viewRef.current === view) viewRef.current = null;
      try {
        view?.close?.();
      } catch {
        // Continue releasing the book and object URL even if the custom element failed.
      }
      view?.remove();
      const openedBook = sourceBook;
      sourceBook = null;
      if (openedBook) await openedBook.close().catch(() => {});
      const objectUrl = coverObjectUrl;
      coverObjectUrl = null;
      if (objectUrl) URL.revokeObjectURL(objectUrl);
    };

    const open = async () => {
      try {
        if (!bookAssetId) throw new Error("没有找到 EPUB 文件资源");
        await import("foliate-js/view.js");
        if (controller.signal.aborted) return;
        // Validate server-side ZIP/chapter budgets before zip.js materializes
        // the ranged archive's client-side entry table.
        await api.epubManifest(detail.work.id, controller.signal, bookVersion);
        if (controller.signal.aborted) return;
        const openedBook = await openEpubBook(assetUrl(bookAssetId, bookVersion), controller.signal);
        if (controller.signal.aborted) {
          await openedBook.close().catch(() => {});
          return;
        }
        sourceBook = openedBook;

        const coverBlob = await sourceBook.book.getCover?.().catch(() => null);
        if (controller.signal.aborted) return;
        if (coverBlob) coverObjectUrl = URL.createObjectURL(coverBlob);

        const view = document.createElement("foliate-view") as FoliateView;
        view.className = "foliate-reader-view";
        containerRef.current?.replaceChildren(view);
        sourceView = view;
        viewRef.current = view;
        setToc(flattenToc(sourceBook.book.toc ?? []));

        view.addEventListener("load", (event) => {
          if (closing) return;
          const detail = (event as CustomEvent<{ doc?: Document; index?: number }>).detail;
          if (detail.doc) {
            applyDocumentSafety(detail.doc);
            if (coverObjectUrl && (detail.index === 0 || isCoverDocument(detail.doc))) {
              applyEpubCover(detail.doc, coverObjectUrl);
            }
            preparePrimaryVisualPage(detail.doc.body);
            detail.doc.addEventListener("pointerdown", () => revealChrome(), { passive: true });
            detail.doc.addEventListener(
              "click",
              (event) => {
                if (isReaderInteractiveTarget(event.target)) return;
                const width = detail.doc?.defaultView?.innerWidth ?? detail.doc?.documentElement.clientWidth ?? 0;
                if (tryEdgePageTurn(event.clientX, width, event)) return;
                revealChrome();
              },
              { passive: false }
            );
            detail.doc.addEventListener("touchstart", () => revealChrome(), { passive: true });
            const onWheel = (event: WheelEvent) => handleWheelTurn(event);
            const wheelOptions: AddEventListenerOptions = { passive: false, capture: true };
            detail.doc.addEventListener("wheel", onWheel, wheelOptions);
            detail.doc.defaultView?.addEventListener("wheel", onWheel, wheelOptions);
          }
        });
        view.addEventListener("relocate", (event) => {
          if (closing) return;
          pendingRelocateRef.current = (event as CustomEvent<FoliateRelocateDetail>).detail;
          if (rafRef.current !== null) return;
          rafRef.current = window.requestAnimationFrame(() => {
            rafRef.current = null;
            const detail = pendingRelocateRef.current;
            pendingRelocateRef.current = null;
            if (!detail) return;
            const fraction = Number.isFinite(detail.fraction) ? detail.fraction! : 0;
            const page = detail.location?.current != null ? detail.location.current + 1 : 0;
            const total = detail.location?.total ?? 0;
            const label = detail.tocItem?.label ?? "";
            setProgress({ fraction, page, total, label });
            if (detail.cfi) {
              persistProgress(fraction, `epubcfi:${encodeURIComponent(detail.cfi)}`);
            }
          });
        });

        await view.open(sourceBook.book);
        if (controller.signal.aborted) return;
        applyViewSettings(view, settingsRef.current);
        const initialTarget = initialFoliateTarget(parseNovelResumeTarget(resumePosition), detail.work.progress || 0);
        if (typeof initialTarget === "string" || typeof initialTarget === "number") {
          try {
            await view.init({ lastLocation: initialTarget });
          } catch {
            if (controller.signal.aborted) return;
            await view.goToFraction(Math.min(0.995, Math.max(0, detail.work.progress || 0)));
          }
        } else {
          await view.goToFraction(initialTarget.fraction);
        }
        if (!controller.signal.aborted && !closing) setLoading(false);
      } catch (error) {
        if (controller.signal.aborted || closing) return;
        await disposeFoliate();
        void flushProgress();
        if (controller.signal.aborted) return;
        setEngineError(error instanceof Error ? error.message : String(error));
        setFallbackMode(true);
        setLoading(false);
      }
    };

    openTask = open();
    return () => {
      closing = true;
      const view = sourceView;
      if (view) sourceBook?.prepareForAbort();
      controller.abort();
      void flushProgress();
      if (rafRef.current !== null) window.cancelAnimationFrame(rafRef.current);
      if (!view || !openTask) {
        void disposeFoliate();
        return;
      }
      // Foliate logs an expected section-load failure if its ZIP signal is
      // aborted while init() is still awaiting the current section. Detach
      // immediately, let the soft-abort fallback drain that promise, and only
      // then destroy the renderer and release the archive.
      view.remove();
      void (async () => {
        await Promise.race([openTask!.catch(() => {}), waitForTimeout(EPUB_ABORT_SETTLE_MS)]);
        await waitForTimeout(EPUB_ABORT_DRAIN_MS);
        await disposeFoliate();
      })();
    };
  }, [bookAssetId, bookVersion, cancelSearch, detail.work.id, detail.work.title, flushProgress, handleWheelTurn, persistProgress, revealChrome, tryEdgePageTurn]);

  useEffect(() => {
    if (!viewRef.current) return;
    applyViewSettings(viewRef.current, settings);
  }, [settings]);

  const onReadingStageWheel = (event: React.WheelEvent<HTMLElement>) => {
    const target = event.target as HTMLElement | null;
    if (target?.closest("button, a, input, textarea, select, .novel-reading-topbar, .novel-panel-sheet")) return;
    handleWheelTurn(event.nativeEvent);
  };

  useEffect(() => {
    const onKeyDown = (event: KeyboardEvent) => {
      const target = event.target as HTMLElement | null;
      if (target?.closest("input, textarea, select")) return;
      const view = viewRef.current;
      if (!view || fallbackMode) return;
      if (event.key === "ArrowLeft") {
        event.preventDefault();
        turnPage("prev");
      }
      if (event.key === "ArrowRight" || event.key === " ") {
        event.preventDefault();
        turnPage("next");
      }
    };
    window.addEventListener("keydown", onKeyDown);
    return () => window.removeEventListener("keydown", onKeyDown);
  }, [fallbackMode, turnPage]);

  const updateSettings = (patch: Partial<NovelReaderSettings>) => {
    setSettings((value) => ({ ...value, ...patch }));
  };

  const closeReader = () => {
    cancelSearch();
    flushProgress();
    onClose();
  };

  const openFloatingPanel = (panel: "search" | "settings") => {
    revealChrome(false);
    if (activePanelRef.current === "search") cancelSearch(true);
    setActivePanel((value) => (value === panel ? null : panel));
  };

  const closeFloatingPanel = () => {
    if (activePanelRef.current === "search") cancelSearch(true);
    setActivePanel(null);
  };

  const runSearch = async (term: string) => {
    const trimmed = term.trim();
    const cancellation = cancelSearch();
    setSearchTerm(term);
    setSearchHits([]);
    setSearchProgress(0);
    if (!trimmed) {
      setSearchStatus("idle");
      return;
    }
    await cancellation.stopped;
    const token = cancellation.token;
    if (searchTokenRef.current !== token) return;
    const view = viewRef.current;
    if (!view) {
      setSearchStatus("idle");
      return;
    }
    setSearchStatus("searching");
    const iterator = view.search({ query: trimmed });
    searchIteratorRef.current = iterator;
    try {
      const hits: NovelSearchHit[] = [];
      while (hits.length < 120) {
        const step = await iterator.next();
        if (searchTokenRef.current !== token) return;
        if (step.done) break;
        const result = step.value;
        if (result === "done") break;
        if ("progress" in result) {
          setSearchProgress(Math.round(result.progress * 100));
        } else if ("subitems" in result) {
          for (const item of result.subitems) {
            if (hits.length >= 120) break;
            hits.push({ cfi: item.cfi, label: result.label || "正文", excerpt: formatSearchExcerpt(item.excerpt) });
          }
          setSearchHits([...hits]);
        } else if ("cfi" in result) {
          hits.push({ cfi: result.cfi, label: "正文", excerpt: formatSearchExcerpt(result.excerpt) });
          setSearchHits([...hits]);
        }
        if (hits.length >= 120) {
          await iterator.return(undefined).catch(() => {});
          break;
        }
      }
      if (searchTokenRef.current === token) {
        setSearchProgress(100);
        setSearchStatus("done");
      }
    } catch {
      if (searchTokenRef.current === token) setSearchStatus("error");
    } finally {
      await iterator.return(undefined).catch(() => {});
      if (searchIteratorRef.current === iterator) searchIteratorRef.current = null;
    }
  };

  const onReadingStagePointerDown = (event: React.PointerEvent<HTMLElement>) => {
    if (isReaderInteractiveTarget(event.target)) return;
    revealChrome();
  };

  const onReadingStageClick = (event: React.MouseEvent<HTMLElement>) => {
    if (isReaderInteractiveTarget(event.target)) return;
    const rect = event.currentTarget.getBoundingClientRect();
    if (tryEdgePageTurn(event.clientX - rect.left, rect.width, event)) return;
    revealChrome();
  };

  if (fallbackMode) {
    return (
      <LegacyNovelReader
        canPersistProgress={canPersistProgress}
        detail={detail}
        engineError={engineError}
        onClose={closeReader}
        onProgressSaved={onProgressSaved}
        resumePosition={resumePosition}
        scheduleProgress={scheduleProgress}
      />
    );
  }

  const searchPanel = (
    <div className="novel-panel novel-search-panel">
      <div className="novel-search-box">
        <Search size={16} />
        <input
          aria-label="搜索书内文本"
          value={searchTerm}
          onChange={(event) => setSearchTerm(event.target.value)}
          onKeyDown={(event) => {
            if (event.key === "Enter") void runSearch(searchTerm);
          }}
          placeholder="搜索书内文本"
        />
        <button onClick={() => void runSearch(searchTerm)} aria-label="搜索">
          <Search size={15} />
        </button>
      </div>
      {searchStatus === "searching" && <div className="novel-search-progress">搜索中 {searchProgress}%</div>}
      {searchStatus === "done" && searchTerm.trim() && (
        <div className="novel-search-progress">{searchHits.length > 0 ? `找到 ${searchHits.length} 处匹配` : "没有匹配项"}</div>
      )}
      {searchStatus === "error" && <div className="novel-search-progress error">搜索失败</div>}
      <div className="novel-search-results">
        {searchHits.map((hit, index) => (
          <button
            key={`${hit.cfi}-${index}`}
            onClick={() => {
              revealChrome();
              closeFloatingPanel();
              void viewRef.current?.goTo(hit.cfi);
            }}
          >
            <span>{hit.label}</span>
            <b>{hit.excerpt}</b>
          </button>
        ))}
      </div>
    </div>
  );

  const settingsPanel = (
    <div className="novel-panel novel-settings-panel">
      <SettingGroup icon={<Type size={15} />} title="字体">
        <SettingRow label="字族">
          <button className={settings.fontFamily === "serif" ? "active" : ""} onClick={() => updateSettings({ fontFamily: "serif" })}>
            Serif
          </button>
          <button className={settings.fontFamily === "sans" ? "active" : ""} onClick={() => updateSettings({ fontFamily: "sans" })}>
            Sans
          </button>
          <button className={settings.fontFamily === "system" ? "active" : ""} onClick={() => updateSettings({ fontFamily: "system" })}>
            System
          </button>
        </SettingRow>
        <SettingRow label="字号">
          <input type="range" min={15} max={28} value={settings.fontSize} onChange={(event) => updateSettings({ fontSize: Number(event.target.value) })} />
          <span>{settings.fontSize}px</span>
        </SettingRow>
        <SettingRow label="字重">
          <input type="range" min={300} max={700} step={50} value={settings.fontWeight} onChange={(event) => updateSettings({ fontWeight: Number(event.target.value) })} />
          <span>{settings.fontWeight}</span>
        </SettingRow>
      </SettingGroup>
      <SettingGroup icon={<Rows3 size={15} />} title="布局">
        <SettingRow label="行高">
          <input type="range" min={1.35} max={2.2} step={0.05} value={settings.lineHeight} onChange={(event) => updateSettings({ lineHeight: Number(event.target.value) })} />
          <span>{settings.lineHeight.toFixed(2)}</span>
        </SettingRow>
        <SettingRow label="版心">
          <input type="range" min={560} max={1040} step={20} value={settings.maxInlineSize} onChange={(event) => updateSettings({ maxInlineSize: Number(event.target.value) })} />
          <span>{settings.maxInlineSize}px</span>
        </SettingRow>
        <SettingRow label="模式">
          <button aria-label="分页阅读" className={settings.flow === "paginated" ? "active" : ""} onClick={() => updateSettings({ flow: "paginated" })} title="分页">
            <Columns3 size={15} />
          </button>
          <button aria-label="滚动阅读" className={settings.flow === "scrolled" ? "active" : ""} onClick={() => updateSettings({ flow: "scrolled" })} title="滚动">
            <AlignJustify size={15} />
          </button>
        </SettingRow>
        <SettingRow label="页幅">
          <button className={settings.pageMode === "single" ? "active" : ""} onClick={() => updateSettings({ pageMode: "single" })}>
            单页
          </button>
          <button className={settings.pageMode === "double" ? "active" : ""} onClick={() => updateSettings({ pageMode: "double", flow: "paginated", writingMode: "horizontal-tb" })}>
            双页
          </button>
        </SettingRow>
        <SettingRow label="方向">
          <button className={settings.writingMode === "horizontal-tb" ? "active" : ""} onClick={() => updateSettings({ writingMode: "horizontal-tb" })}>
            横排
          </button>
          <button className={settings.writingMode === "vertical-rl" ? "active" : ""} onClick={() => updateSettings({ writingMode: "vertical-rl" })}>
            竖排
          </button>
        </SettingRow>
      </SettingGroup>
      <SettingGroup icon={<Palette size={15} />} title="颜色">
        <SettingRow label="主题">
          <button aria-label="纸张主题" className={settings.theme === "paper" ? "active" : ""} onClick={() => updateSettings({ theme: "paper" })} title="纸张">
            <Sun size={15} />
          </button>
          <button aria-label="暖色主题" className={settings.theme === "sepia" ? "active" : ""} onClick={() => updateSettings({ theme: "sepia" })} title="暖色">
            <FileText size={15} />
          </button>
          <button aria-label="深色主题" className={settings.theme === "dark" ? "active" : ""} onClick={() => updateSettings({ theme: "dark" })} title="深色">
            <Moon size={15} />
          </button>
        </SettingRow>
      </SettingGroup>
    </div>
  );

  const effectivePageMode =
    settings.pageMode === "double" && settings.flow === "paginated" && settings.writingMode === "horizontal-tb" ? "double" : "single";

  return (
    <div
      className="novel-reader-shell"
      data-chrome={chromeVisible || activePanel ? "visible" : "hidden"}
      data-flow={settings.flow}
      data-page-mode={effectivePageMode}
      data-panel={activePanel ?? "none"}
      data-sidebar={sidebarOpen ? "open" : "closed"}
      data-theme={settings.theme}
      >
      {sidebarOpen && <button className="novel-mobile-sidebar-backdrop" onClick={() => setSidebarOpen(false)} aria-label="关闭目录" />}
      <aside className="novel-sidebar" aria-label="小说目录">
        <div className="novel-sidebar-head">
          <div className="novel-book-summary">
            {detail.work.cover_asset_id ? <img src={coverAssetUrl} alt="" /> : <BookOpen size={24} />}
            <div>
              <b>{detail.work.title}</b>
              {detail.work.subtitle ? <span>{detail.work.subtitle}</span> : null}
              <em>{Math.round(Math.min(1, Math.max(0, progress.fraction)) * 100)}% 已读</em>
            </div>
          </div>
          <div className="novel-sidebar-actions">
            <button className="novel-reader-close" onClick={closeReader} aria-label="关闭阅读器" title="关闭阅读器">
              <X size={17} />
            </button>
            <button className="novel-sidebar-close" onClick={() => setSidebarOpen(false)} aria-label="收起目录" title="收起目录">
              <PanelLeftClose size={16} />
            </button>
          </div>
        </div>
        <div className="novel-sidebar-current">
          <span>当前位置</span>
          <b>{progress.label || "正在阅读"}</b>
          <em>{formatNovelPageStat(progress)}</em>
        </div>
        <div className="novel-sidebar-title">
          <BookOpen size={15} />
          <span>目录</span>
        </div>
        <div className="novel-panel novel-toc-panel">
          {toc.length === 0 ? <Loader2 className="spin" /> : null}
          {toc.map((item, index) => (
            <button
              aria-current={activeTocIndex === index ? "location" : undefined}
              className={activeTocIndex === index ? "active" : ""}
              key={`${item.href ?? item.label ?? index}-${index}`}
              onClick={() => {
                if (item.href) {
                  revealChrome();
                  setSidebarOpen((value) => (window.innerWidth <= 820 ? false : value));
                  void viewRef.current?.goTo(item.href);
                }
              }}
              style={{ paddingLeft: `${14 + item.depth * 16}px` }}
            >
              <span>{index + 1}</span>
              <b>{item.label || item.href || "章节"}</b>
            </button>
          ))}
        </div>
      </aside>
      {!sidebarOpen && (
        <button className="novel-sidebar-rail" onClick={() => setSidebarOpen(true)} aria-label="展开目录" title="展开目录">
          <PanelLeftOpen size={16} />
        </button>
      )}
      <main className="novel-foliate-stage" onClick={onReadingStageClick} onPointerDown={onReadingStagePointerDown} onWheelCapture={onReadingStageWheel}>
        {effectivePageMode === "double" && settings.flow === "paginated" && !activePanel && (
          <>
            <button
              className="novel-edge-turn-zone novel-edge-turn-zone-left"
              onClick={(event) => {
                event.preventDefault();
                event.stopPropagation();
                turnPage("prev");
              }}
              onPointerDown={(event) => event.stopPropagation()}
              aria-label="上一页"
              title="上一页"
            />
            <button
              className="novel-edge-turn-zone novel-edge-turn-zone-right"
              onClick={(event) => {
                event.preventDefault();
                event.stopPropagation();
                turnPage("next");
              }}
              onPointerDown={(event) => event.stopPropagation()}
              aria-label="下一页"
              title="下一页"
            />
          </>
        )}
        <div className="novel-reading-topbar">
          <div className="novel-topbar-actions">
            <button className={activePanel === "search" ? "active" : ""} onClick={() => openFloatingPanel("search")} aria-label="搜索" title="搜索">
              <Search size={16} />
            </button>
            <button className={activePanel === "settings" ? "active" : ""} onClick={() => openFloatingPanel("settings")} aria-label="阅读设置" title="阅读设置">
              <Settings size={16} />
            </button>
            {bookAsset && (
              <a className="novel-icon-link" href={assetUrl(bookAsset.id, bookVersion)} target="_blank" rel="noreferrer" aria-label="下载 EPUB" title="下载 EPUB">
                <Download size={16} />
              </a>
            )}
          </div>
        </div>
        {loading && (
          <div className="novel-loading">
            <Loader2 className="spin" />
          </div>
        )}
        <div className="novel-foliate-host" ref={containerRef} />
        <div className="novel-page-indicator" aria-label="当前页数">{formatNovelPageNumber(progress)}</div>
      </main>
      {activePanel && (
        <div className="novel-panel-layer">
          <button className="novel-panel-backdrop" onClick={closeFloatingPanel} aria-label="关闭面板" />
          <section className="novel-panel-sheet" role="dialog" aria-label={activePanel === "search" ? "书内搜索" : "阅读设置"}>
            <div className="novel-panel-sheet-head">
              <div>
                <span>{activePanel === "search" ? "书内搜索" : "阅读设置"}</span>
                <b>{activePanel === "search" ? "在当前 EPUB 中查找正文" : "字体、布局和颜色"}</b>
              </div>
              <button onClick={closeFloatingPanel} aria-label="关闭面板" title="关闭">
                <X size={16} />
              </button>
            </div>
            {activePanel === "search" ? searchPanel : settingsPanel}
          </section>
        </div>
      )}
    </div>
  );
}

function SettingGroup({ children, icon, title }: { children: React.ReactNode; icon: React.ReactNode; title: string }) {
  return (
    <section className="novel-setting-group">
      <h3>
        {icon}
        <span>{title}</span>
      </h3>
      {children}
    </section>
  );
}

function SettingRow({ children, label }: { children: React.ReactNode; label: string }) {
  return (
    <div className="novel-setting-row">
      <span>{label}</span>
      <div>{children}</div>
    </div>
  );
}

function LegacyNovelReader({
  canPersistProgress,
  detail,
  engineError,
  onClose,
  resumePosition,
  scheduleProgress
}: NovelReaderProps & {
  engineError: string | null;
  scheduleProgress: (progress: number, position: string, immediate?: boolean, keepalive?: boolean) => void;
}) {
  const [chapters, setChapters] = useState<EpubChapter[]>([]);
  const [chapter, setChapter] = useState(0);
  const [showCover, setShowCover] = useState(Boolean(detail.work.cover_asset_id));
  const [chapterHtml, setChapterHtml] = useState("");
  const [theme, setTheme] = useState<"paper" | "dark">("paper");
  const [error, setError] = useState<string | null>(null);
  const bookAsset = detail.assets.find((asset) => asset.role === "book" && isEpubAsset(asset));
  const bookVersion = assetVersion(bookAsset, detail.work.updated_at);
  const coverAsset = detail.assets.find((asset) => asset.id === detail.work.cover_asset_id);
  const coverAssetUrl = assetUrl(detail.work.cover_asset_id, assetVersion(coverAsset, detail.work.updated_at));
  const resumeTarget = useMemo(() => parseNovelResumeTarget(resumePosition), [resumePosition]);
  const appliedRef = useRef(false);
  const suppressNextProgressRef = useRef(false);

  useEffect(() => {
    const controller = new AbortController();
    setError(null);
    api
      .epubManifest(detail.work.id, controller.signal, bookVersion)
      .then((res) => {
        setChapters(res.chapters);
        setError(null);
      })
      .catch((err) => {
        if (!(err instanceof DOMException && err.name === "AbortError")) {
          setError(err instanceof Error ? err.message : String(err));
        }
      });
    return () => controller.abort();
  }, [bookVersion, detail.work.id]);

  useEffect(() => {
    if (chapters.length === 0 || appliedRef.current) return;
    if (resumeTarget.kind === "cover") {
      suppressNextProgressRef.current = true;
      setShowCover(Boolean(detail.work.cover_asset_id));
    } else {
      const target =
        resumeTarget.kind === "chapter"
          ? resumeTarget.index
          : resumeTarget.kind === "start"
            ? 0
             : Math.floor((detail.work.progress || 0) * Math.max(0, chapters.length - 1));
      suppressNextProgressRef.current = resumeTarget.kind !== null;
      setShowCover(false);
      setChapter(Math.min(Math.max(target, 0), Math.max(0, chapters.length - 1)));
    }
    appliedRef.current = true;
  }, [chapters.length, detail.work.cover_asset_id, detail.work.progress, resumeTarget]);

  useEffect(() => {
    if (showCover || chapters.length === 0) return;
    const controller = new AbortController();
    setChapterHtml("");
    setError(null);
    api
      .epubChapterHtml(detail.work.id, chapter, controller.signal, bookVersion)
      .then((html) => {
        setChapterHtml(html);
        setError(null);
      })
      .catch((err) => {
        if (!(err instanceof DOMException && err.name === "AbortError")) {
          setError(err instanceof Error ? err.message : String(err));
        }
      });
    return () => controller.abort();
  }, [bookVersion, chapter, chapters.length, detail.work.id, showCover]);

  useEffect(() => {
    if (!canPersistProgress || chapters.length === 0 || !appliedRef.current) return;
    if (suppressNextProgressRef.current) {
      suppressNextProgressRef.current = false;
      return;
    }
    const progress = showCover ? 0 : (chapter + 1) / chapters.length;
    const position = showCover ? "cover" : `chapter:${chapter}`;
    scheduleProgress(progress, position);
  }, [canPersistProgress, chapter, chapters.length, scheduleProgress, showCover]);

  const moveChapter = (offset: number) => {
    if (showCover && offset > 0) {
      setShowCover(false);
      setChapter(0);
      return;
    }
    if (!showCover && chapter === 0 && offset < 0 && detail.work.cover_asset_id) {
      setShowCover(true);
      return;
    }
    setShowCover(false);
    setChapter((value) => Math.min(Math.max(value + offset, 0), Math.max(0, chapters.length - 1)));
  };

  return (
    <div className="legacy-novel-shell">
      <div className="legacy-novel-toolbar">
        <button className="icon-btn" onClick={onClose} aria-label="关闭阅读器" title="关闭">
          <X size={17} />
        </button>
        <span>{detail.work.title}</span>
        <div>
          {bookAsset && (
            <a className="icon-btn" href={assetUrl(bookAsset.id, bookVersion)} target="_blank" rel="noreferrer" aria-label="下载 EPUB" title="下载 EPUB">
              <Download size={16} />
            </a>
          )}
        </div>
      </div>
      <div className="novel-stage legacy-novel-stage">
        <div className="chapter-list">
          {engineError && <div className="reader-error">Foliate 阅读器启动失败，已切换到兼容模式：{engineError}</div>}
          {chapters.length === 0 && !error ? <Loader2 className="spin" /> : null}
          {detail.work.cover_asset_id && (
            <button className={showCover ? "active" : ""} onClick={() => setShowCover(true)}>
              <span>1</span>
              <b>封面</b>
            </button>
          )}
          {chapters.map((item) => (
            <button key={item.index} className={!showCover && item.index === chapter ? "active" : ""} onClick={() => { setShowCover(false); setChapter(item.index); }}>
              <span>{detail.work.cover_asset_id ? item.index + 2 : item.index + 1}</span>
              <b>{item.title}</b>
            </button>
          ))}
        </div>
        <div className="chapter-reader">
          <div className="legacy-novel-actions">
            <button className="icon-btn" onClick={() => moveChapter(-1)} aria-label="上一章">
              <ChevronLeft size={16} />
            </button>
            <button className="icon-btn" onClick={() => moveChapter(1)} aria-label="下一章">
              <ChevronRight size={16} />
            </button>
            <button className="icon-btn" onClick={() => setTheme((value) => (value === "paper" ? "dark" : "paper"))} aria-label="切换阅读主题">
              {theme === "paper" ? <Moon size={16} /> : <Sun size={16} />}
            </button>
          </div>
          {error ? (
            <div className="reader-error">{error}</div>
          ) : showCover && detail.work.cover_asset_id ? (
            <div className="novel-cover-page">
              <img src={coverAssetUrl} alt="" />
            </div>
          ) : chapterHtml ? (
            <iframe title={chapters[chapter]?.title ?? detail.work.title} sandbox="" srcDoc={applyLegacyNovelTheme(chapterHtml, theme)} />
          ) : (
            <Loader2 className="spin" />
          )}
        </div>
      </div>
    </div>
  );
}

async function openEpubBook(url: string, signal: AbortSignal) {
  configure({ useWebWorkers: true });
  let softAbort = false;
  const reader = new ZipReader(
    new HttpReader(url, {
      useRangeHeader: true,
      forceRangeRequests: true,
      combineSizeEocd: true,
      preventHeadRequest: true
    }),
    { signal }
  );
  try {
    const entries = await reader.getEntries();
    if (entries.length > MAX_EPUB_ENTRIES) {
      throw new Error(`EPUB 条目过多（${entries.length}），上限为 ${MAX_EPUB_ENTRIES}`);
    }
    const map = new Map(entries.map((entry) => [entry.filename, entry]));
    const lowerMap = new Map<string, Entry | null>();
    for (const entry of entries) {
      const key = entry.filename.toLowerCase();
      lowerMap.set(key, lowerMap.has(key) ? null : entry);
    }
    const getEntry = (name: string) => map.get(name) ?? lowerMap.get(name.toLowerCase()) ?? null;
    let activeInflateBytes = 0;
    const extractWithBudget = async <T,>(entry: Entry, perEntryLimit: number, extract: () => Promise<T | undefined>) => {
      const size = entry.uncompressedSize;
      if (!Number.isSafeInteger(size) || size < 0 || size > perEntryLimit) {
        throw new Error(`EPUB 条目 ${entry.filename} 解压后大小 ${size} 超过安全上限 ${perEntryLimit}`);
      }
      if (activeInflateBytes + size > MAX_EPUB_ACTIVE_INFLATE_BYTES) {
        throw new Error("EPUB 同时解压的数据量超过安全上限，请稍后重试");
      }
      activeInflateBytes += size;
      try {
        return await extract();
      } finally {
        activeInflateBytes -= size;
      }
    };
    const loader = {
      entries,
      loadText: async (name: string) => {
        try {
          const entry = getEntry(name);
          return entry && !entry.directory
            ? (await extractWithBudget(entry, MAX_EPUB_TEXT_ENTRY_BYTES, () => entry.getData?.(new TextWriter(), { signal }))) ?? null
            : null;
        } catch (error) {
          if (softAbort && signal.aborted) return abortedEpubText(name);
          throw error;
        }
      },
      loadBlob: async (name: string, type?: string) => {
        try {
          const entry = getEntry(name);
          return entry && !entry.directory
            ? (await extractWithBudget(entry, MAX_EPUB_BLOB_ENTRY_BYTES, () => entry.getData?.(new BlobWriter(type), { signal }))) ?? null
            : null;
        } catch (error) {
          if (softAbort && signal.aborted) return abortedEpubBlob(name, type);
          throw error;
        }
      },
      getSize: (name: string) => getEntry(name)?.uncompressedSize ?? 0
    };
    const book = await new EPUB(loader).init();
    return {
      book,
      close: () => reader.close(),
      prepareForAbort: () => {
        softAbort = true;
      }
    };
  } catch (error) {
    await reader.close().catch(() => {});
    throw error;
  }
}

function abortedEpubText(name: string) {
  return isEpubMarkup(name) ? EMPTY_EPUB_DOCUMENT : "";
}

function abortedEpubBlob(name: string, type?: string) {
  const markup = isEpubMarkup(name) || /(?:html|xml)/i.test(type ?? "");
  return new Blob([markup ? EMPTY_EPUB_DOCUMENT : ""], {
    type: type || (markup ? "application/xhtml+xml" : "application/octet-stream")
  });
}

function isEpubMarkup(name: string) {
  return /\.(?:xhtml?|html?|xml|opf|ncx)$/i.test(name);
}

function preparePrimaryVisualPage(body: HTMLElement) {
  const meaningfulNodes = Array.from(body.childNodes).filter((node) => node.nodeType !== Node.TEXT_NODE || Boolean(node.textContent?.trim()));
  const elements = meaningfulNodes.filter((node): node is Element => node instanceof Element);
  if (meaningfulNodes.length !== 1 || elements.length !== 1) return;
  const onlyElement = elements[0];
  const visual =
    isVisualElement(onlyElement) || onlyElement.matches("p, div, figure")
      ? onlyElement.matches("p, div, figure")
        ? onlyElement.querySelector(":scope > img:only-child, :scope > svg:only-child")
        : onlyElement
      : null;
  if (visual instanceof HTMLElement || visual instanceof SVGElement) {
    visual.setAttribute("data-arislist-primary-visual", "true");
    body.setAttribute("data-arislist-primary-visual-body", "true");
  }
}

function isCoverDocument(doc: Document) {
  return /^(cover|封面)$/i.test(doc.title.trim());
}

function applyEpubCover(doc: Document, coverObjectUrl: string) {
  const image = doc.querySelector("svg image");
  if (image) {
    image.setAttribute("href", coverObjectUrl);
    image.setAttributeNS("http://www.w3.org/1999/xlink", "xlink:href", coverObjectUrl);
    return;
  }

  const img = doc.querySelector("img");
  if (img) img.src = coverObjectUrl;
}

function isVisualElement(element: Element) {
  return element.matches("img, svg");
}

function isReaderInteractiveTarget(target: EventTarget | null) {
  const element = target as { closest?: (selector: string) => Element | null } | null;
  return !!element?.closest?.("button, a, input, textarea, select, .novel-reading-topbar, .novel-page-indicator, .novel-panel-sheet");
}

async function runFoliateMove(move: () => Promise<void>) {
  await Promise.race([move(), waitForTimeout(1500)]);
}

function waitForTimeout(ms: number) {
  return new Promise<void>((resolve) => {
    window.setTimeout(resolve, ms);
  });
}

function applyViewSettings(view: FoliateView, settings: NovelReaderSettings) {
  const doublePage = isDoublePageSettings(settings);
  const pageMargin = doublePage ? "54px" : "60px";
  applyDoublePageEndPadding(view, doublePage);
  view.renderer?.setAttribute("flow", settings.flow);
  // Adjacent one-page spine sections share a spread in the Readest Foliate fork.
  // A percentage column gap makes their combined width smaller than the spread,
  // so the browser clamps the last scroll offset and repeats one page next turn.
  view.renderer?.setAttribute("gap", settings.flow === "paginated" ? (doublePage ? "0%" : "12%") : "0%");
  view.renderer?.removeAttribute("margin");
  view.renderer?.setAttribute("margin-top", settings.flow === "paginated" ? pageMargin : "56px");
  view.renderer?.setAttribute("margin-bottom", settings.flow === "paginated" ? pageMargin : "56px");
  view.renderer?.setAttribute("margin-left", settings.flow === "paginated" ? pageMargin : "56px");
  view.renderer?.setAttribute("margin-right", settings.flow === "paginated" ? pageMargin : "56px");
  view.renderer?.setAttribute("max-inline-size", `${settings.maxInlineSize}px`);
  view.renderer?.setAttribute("max-block-size", "1800px");
  view.renderer?.setAttribute("max-column-count", doublePage ? "2" : "1");
  view.renderer?.removeAttribute("max-column-count-portrait");
  if (settings.flow === "paginated" && !window.matchMedia("(prefers-reduced-motion: reduce)").matches) {
    view.renderer?.setAttribute("animated", "");
  } else {
    view.renderer?.removeAttribute("animated");
  }
  view.renderer?.removeAttribute("turn-style");
  view.renderer?.setStyles?.(novelStyleSheet(settings));
}

function applyDoublePageEndPadding(view: FoliateView, enabled: boolean) {
  const renderer = view.renderer;
  if (!renderer) return;

  renderer.toggleAttribute("data-arislist-spread-padding", enabled);
  const root = renderer.shadowRoot;
  if (!root || root.querySelector("#arislist-spread-padding-style")) return;

  const style = document.createElement("style");
  style.id = "arislist-spread-padding-style";
  style.textContent = `
    :host([data-arislist-spread-padding]:not([flow="scrolled"])) #container::after {
      content: "";
      flex: 0 0 50%;
      pointer-events: none;
    }
  `;
  root.append(style);
}

function novelStyleSheet(settings: NovelReaderSettings) {
  const colors = themeColors(settings.theme);
  const fontFamily =
    settings.fontFamily === "serif"
      ? `"Noto Serif SC", "Songti SC", "SimSun", serif`
      : settings.fontFamily === "sans"
        ? `"Noto Sans SC", "Microsoft YaHei", system-ui, sans-serif`
        : `system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif`;
  return `
    :root { color-scheme: ${settings.theme === "dark" ? "dark" : "light"}; }
    html, body {
      background: ${colors.background} !important;
      color: ${colors.text} !important;
      font-family: ${fontFamily} !important;
      font-size: ${settings.fontSize}px !important;
      font-weight: ${settings.fontWeight} !important;
      line-height: ${settings.lineHeight} !important;
      writing-mode: ${settings.writingMode};
    }
    body {
      margin: 0 !important;
      padding: 0 !important;
    }
    body[data-arislist-primary-visual-body="true"] {
      display: grid !important;
      place-items: center !important;
      min-height: calc(var(--available-height, 800) * 1px) !important;
      margin: 0 !important;
      padding: 0 !important;
    }
    p {
      margin: 0 0 1em !important;
    }
    h1, h2, h3 {
      line-height: 1.35 !important;
      margin: 1.2em 0 0.7em !important;
    }
    a { color: ${colors.link} !important; }
    img, svg {
      max-width: 100% !important;
      height: auto !important;
      display: block !important;
      margin: 1em auto !important;
    }
    body:has(> img:only-child),
    body:has(> svg:only-child),
    body:has(> p:only-child > img:only-child),
    body:has(> p:only-child > svg:only-child),
    body:has(> div:only-child > img:only-child),
    body:has(> div:only-child > svg:only-child),
    body:has(> figure:only-child > img:only-child),
    body:has(> figure:only-child > svg:only-child) {
      display: grid !important;
      place-items: center !important;
      min-height: calc(var(--available-height, 800) * 1px) !important;
    }
    body:has(> p:only-child > img:only-child) > p:only-child,
    body:has(> p:only-child > svg:only-child) > p:only-child,
    body:has(> div:only-child > img:only-child) > div:only-child,
    body:has(> div:only-child > svg:only-child) > div:only-child,
    body:has(> figure:only-child > img:only-child) > figure:only-child,
    body:has(> figure:only-child > svg:only-child) > figure:only-child {
      margin: 0 !important;
      padding: 0 !important;
    }
    body > img:only-child,
    body > svg:only-child,
    body > p:only-child > img:only-child,
    body > p:only-child > svg:only-child,
    body > div:only-child > img:only-child,
    body > div:only-child > svg:only-child,
    body > figure:only-child > img:only-child,
    body > figure:only-child > svg:only-child,
    [data-arislist-primary-visual="true"] {
      width: auto !important;
      height: auto !important;
      max-width: min(100%, calc(var(--available-width, 720) * 1px)) !important;
      max-height: calc(var(--available-height, 800) * 1px) !important;
      object-fit: contain !important;
      margin: 0 auto !important;
    }
    ::selection {
      background: ${colors.selection} !important;
    }
  `;
}

function isDoublePageSettings(settings: NovelReaderSettings) {
  return settings.pageMode === "double" && settings.flow === "paginated" && settings.writingMode === "horizontal-tb";
}

function applyDocumentSafety(doc: Document) {
  doc.querySelectorAll("script, iframe, object, embed").forEach((node) => node.remove());
  doc.querySelectorAll("[onclick], [onload], [onerror]").forEach((node) => {
    [...node.attributes].forEach((attr) => {
      if (attr.name.toLowerCase().startsWith("on")) node.removeAttribute(attr.name);
    });
  });
}

function themeColors(theme: NovelReaderSettings["theme"]) {
  if (theme === "dark") {
    return { background: "#111316", text: "#ece7dc", link: "#e0b66c", selection: "rgba(83, 199, 185, 0.35)" };
  }
  if (theme === "sepia") {
    return { background: "#f2e5cf", text: "#2d2418", link: "#8f4d34", selection: "rgba(188, 132, 72, 0.28)" };
  }
  return { background: "#f8f4eb", text: "#24211c", link: "#8f4d34", selection: "rgba(83, 199, 185, 0.25)" };
}

function flattenToc(items: FoliateTOCItem[], depth = 0): Array<FoliateTOCItem & { depth: number }> {
  return items.flatMap((item) => [
    { ...item, depth },
    ...flattenToc(item.subitems ?? [], depth + 1)
  ]);
}

function initialFoliateTarget(target: NovelResumeTarget, savedProgress: number): string | number | { fraction: number } {
  if (target.kind === "epub-cfi") return target.cfi;
  if (target.kind === "chapter") return target.index;
  if (target.kind === "start" || target.kind === "cover") return { fraction: 0 };
  return { fraction: Math.min(0.995, Math.max(0, savedProgress || 0)) };
}

function parseNovelResumeTarget(value?: string | null): NovelResumeTarget {
  if (!value) return { kind: null };
  if (value === "start") return { kind: "start" };
  if (value === "cover") return { kind: "cover" };
  if (value.startsWith("epubcfi:")) {
    try {
      return { kind: "epub-cfi", cfi: decodeURIComponent(value.slice("epubcfi:".length)) };
    } catch {
      return { kind: null };
    }
  }
  const [kind, index] = value.split(":");
  if (kind === "chapter") return { kind: "chapter", index: safeIndex(index) };
  return { kind: null };
}

function formatSearchExcerpt(excerpt?: FoliateSearchExcerpt) {
  if (!excerpt) return "";
  return `${excerpt.pre ?? ""}${excerpt.match ?? ""}${excerpt.post ?? ""}`.trim();
}

function formatNovelPageStat(progress: { fraction: number; page: number; total: number }) {
  const percent = `${Math.round(Math.min(1, Math.max(0, progress.fraction)) * 100)}%`;
  return progress.total > 0 ? `${progress.page}/${progress.total} · ${percent}` : percent;
}

function formatNovelPageNumber(progress: { fraction: number; page: number; total: number }) {
  if (progress.total > 0 && progress.page > 0) return `${progress.page}/${progress.total}`;
  return `${Math.round(Math.min(1, Math.max(0, progress.fraction)) * 100)}%`;
}

function loadNovelSettings(): NovelReaderSettings {
  if (typeof window === "undefined") return defaultNovelSettings;
  try {
    const parsed = JSON.parse(window.localStorage.getItem(NOVEL_SETTINGS_KEY) ?? "{}") as Partial<NovelReaderSettings>;
    return {
      ...defaultNovelSettings,
      ...parsed,
      fontSize: clampNumber(parsed.fontSize, 15, 28, defaultNovelSettings.fontSize),
      fontWeight: clampNumber(parsed.fontWeight, 300, 700, defaultNovelSettings.fontWeight),
      lineHeight: clampNumber(parsed.lineHeight, 1.35, 2.2, defaultNovelSettings.lineHeight),
      maxInlineSize: clampNumber(parsed.maxInlineSize, 560, 1040, defaultNovelSettings.maxInlineSize),
      theme: parsed.theme === "dark" || parsed.theme === "sepia" || parsed.theme === "paper" ? parsed.theme : defaultNovelSettings.theme,
      fontFamily: parsed.fontFamily === "serif" || parsed.fontFamily === "sans" || parsed.fontFamily === "system" ? parsed.fontFamily : defaultNovelSettings.fontFamily,
      flow: parsed.flow === "scrolled" || parsed.flow === "paginated" ? parsed.flow : defaultNovelSettings.flow,
      pageMode: parsed.pageMode === "double" || parsed.pageMode === "single" ? parsed.pageMode : defaultNovelSettings.pageMode,
      writingMode: parsed.writingMode === "vertical-rl" || parsed.writingMode === "horizontal-tb" ? parsed.writingMode : defaultNovelSettings.writingMode
    };
  } catch {
    return defaultNovelSettings;
  }
}

function clampNumber(value: unknown, min: number, max: number, fallback: number) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? Math.min(max, Math.max(min, parsed)) : fallback;
}

function safeIndex(value?: string) {
  const parsed = Number.parseInt(value ?? "0", 10);
  return Number.isFinite(parsed) ? Math.max(0, parsed) : 0;
}

function isEpubAsset(asset: { mime?: string | null; path?: string | null }) {
  return asset.mime === "application/epub+zip" || asset.path?.toLowerCase().endsWith(".epub");
}

function applyLegacyNovelTheme(html: string, theme: "paper" | "dark") {
  if (theme === "paper") return html;
  return html.replace(
    "</head>",
    `<style>body{background:#121316!important;color:#eee7d8!important;}a{color:#e0b66c!important;}</style></head>`
  );
}
