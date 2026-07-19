import { useCallback, useEffect, useRef } from "react";
import { api } from "../api";

type ProgressContext = {
  enabled: boolean;
  onProgressSaved: (id: number, progress: number, position?: string | null) => void;
  workId: number;
};

type ProgressUpdate = {
  attempts: number;
  context: ProgressContext;
  generation: number;
  position: string;
  progress: number;
  updateToken: number;
};

type InFlightProgress = {
  controller: AbortController | null;
  handedOff: boolean;
  update: ProgressUpdate;
};

const PROGRESS_REQUEST_TIMEOUT_MS = 15000;
const PROGRESS_RETRY_LIMIT = 4;
const PROGRESS_TOKEN_STORAGE_KEY = "arislist_progress_update_token";
let lastProgressUpdateToken = 0;
let progressTokenLoaded = false;

function nextProgressUpdateToken() {
  if (!progressTokenLoaded) {
    progressTokenLoaded = true;
    try {
      const stored = Number(window.sessionStorage.getItem(PROGRESS_TOKEN_STORAGE_KEY));
      if (Number.isSafeInteger(stored) && stored > 0) lastProgressUpdateToken = stored;
    } catch {
      // Storage can be unavailable in hardened/private browsing contexts.
    }
  }
  lastProgressUpdateToken = Math.max(Date.now() * 1000, lastProgressUpdateToken + 1);
  try {
    window.sessionStorage.setItem(PROGRESS_TOKEN_STORAGE_KEY, String(lastProgressUpdateToken));
  } catch {
    // The in-memory monotonic token still protects this page lifetime.
  }
  return lastProgressUpdateToken;
}

function notifyProgress(update: ProgressUpdate, progress: number, position: string | null | undefined) {
  try {
    update.context.onProgressSaved(
      update.context.workId,
      progress,
      position === undefined ? update.position : position
    );
  } catch {
    // Local state synchronization must not turn a successful server write into a retry.
  }
}

export function useProgressQueue(
  workId: number,
  enabled: boolean,
  onProgressSaved: (id: number, progress: number, position?: string | null) => void,
  delayMs = 900
) {
  const pendingRef = useRef<ProgressUpdate | null>(null);
  const latestUnacknowledgedRef = useRef<ProgressUpdate | null>(null);
  const timerRef = useRef<number | null>(null);
  const runningRef = useRef<Promise<void> | null>(null);
  const inFlightRef = useRef<InFlightProgress | null>(null);
  const keepaliveRef = useRef<{ generation: number; promise: Promise<void> } | null>(null);
  const generationRef = useRef(0);
  const mountedRef = useRef(false);
  const delayRef = useRef(delayMs);
  const contextRef = useRef<ProgressContext>({ enabled, onProgressSaved, workId });
  const drainRef = useRef<(keepalive?: boolean) => Promise<void>>(async () => {});
  delayRef.current = delayMs;
  contextRef.current = { enabled, onProgressSaved, workId };

  const clearTimer = useCallback(() => {
    if (timerRef.current !== null) {
      window.clearTimeout(timerRef.current);
      timerRef.current = null;
    }
  }, []);

  const scheduleRetry = useCallback((update: ProgressUpdate) => {
    const context = contextRef.current;
    if (
      !mountedRef.current ||
      update.attempts >= PROGRESS_RETRY_LIMIT ||
      latestUnacknowledgedRef.current?.generation !== update.generation ||
      !context.enabled ||
      context.workId !== update.context.workId
    ) {
      return false;
    }

    const retry = { ...update, attempts: update.attempts + 1 };
    pendingRef.current = retry;
    const retryDelay = Math.min(30000, Math.max(1500, delayRef.current * 2 ** retry.attempts));
    timerRef.current = window.setTimeout(() => {
      timerRef.current = null;
      void drainRef.current(false);
    }, retryDelay);
    return true;
  }, []);

  const sendKeepalive = useCallback((update: ProgressUpdate) => {
    const existing = keepaliveRef.current;
    if (existing?.generation === update.generation) return existing.promise;

    pendingRef.current = null;
    const inFlight = inFlightRef.current;
    if (inFlight) {
      inFlight.handedOff = true;
      inFlight.controller?.abort();
    }

    const promise = api
      .updateProgress(update.context.workId, update.progress, update.position, update.updateToken, { keepalive: true })
      .then((res) => {
        if (latestUnacknowledgedRef.current?.generation === update.generation) {
          latestUnacknowledgedRef.current = null;
        }
        notifyProgress(update, res.progress, res.position);
      })
      .catch(() => {
        if (latestUnacknowledgedRef.current?.generation === update.generation) {
          scheduleRetry(update);
        }
      })
      .finally(() => {
        if (keepaliveRef.current?.generation === update.generation) keepaliveRef.current = null;
      });
    keepaliveRef.current = { generation: update.generation, promise };
    return promise;
  }, [scheduleRetry]);

  const drain = useCallback((keepalive = false): Promise<void> => {
    clearTimer();
    if (keepalive) {
      const latest = latestUnacknowledgedRef.current ?? pendingRef.current ?? inFlightRef.current?.update;
      return latest ? sendKeepalive(latest) : Promise.resolve();
    }
    if (runningRef.current) return runningRef.current;

    const run = async () => {
      while (pendingRef.current) {
        const update = pendingRef.current;
        pendingRef.current = null;
        const controller = new AbortController();
        const inFlight: InFlightProgress = { controller, handedOff: false, update };
        inFlightRef.current = inFlight;
        const timeout = window.setTimeout(() => controller.abort(), PROGRESS_REQUEST_TIMEOUT_MS);
        try {
          const res = await api.updateProgress(update.context.workId, update.progress, update.position, update.updateToken, {
            signal: controller.signal
          });
          if (latestUnacknowledgedRef.current?.generation === update.generation) {
            latestUnacknowledgedRef.current = null;
          }
          notifyProgress(update, res.progress, res.position);
        } catch {
          if (!inFlight.handedOff) {
            const newer = pendingRef.current as ProgressUpdate | null;
            const isStillLatest = latestUnacknowledgedRef.current?.generation === update.generation;
            if (isStillLatest && (!newer || newer.generation <= update.generation)) {
              scheduleRetry(update);
              break;
            }
          }
        } finally {
          window.clearTimeout(timeout);
          if (inFlightRef.current === inFlight) inFlightRef.current = null;
        }
      }
    };

    let running: Promise<void>;
    running = run().finally(() => {
      if (runningRef.current === running) runningRef.current = null;
      if (pendingRef.current && timerRef.current === null && mountedRef.current) {
        void drainRef.current(false);
      }
    });
    runningRef.current = running;
    return running;
  }, [clearTimer, scheduleRetry, sendKeepalive]);
  drainRef.current = drain;

  const schedule = useCallback((progress: number, position: string, immediate = false, keepalive = false) => {
    const context = contextRef.current;
    if (!context.enabled) return;
    const update: ProgressUpdate = {
      attempts: 0,
      context: { ...context },
      generation: ++generationRef.current,
      position,
      progress: Math.min(1, Math.max(0, progress)),
      updateToken: nextProgressUpdateToken()
    };
    pendingRef.current = update;
    latestUnacknowledgedRef.current = update;
    notifyProgress(update, update.progress, update.position);
    clearTimer();
    if (immediate) {
      void drainRef.current(keepalive);
      return;
    }
    timerRef.current = window.setTimeout(() => {
      timerRef.current = null;
      void drainRef.current(false);
    }, delayRef.current);
  }, [clearTimer]);

  const cancel = useCallback(() => {
    clearTimer();
    pendingRef.current = null;
    latestUnacknowledgedRef.current = null;
    generationRef.current += 1;
    const inFlight = inFlightRef.current;
    if (inFlight) {
      inFlight.handedOff = true;
      inFlight.controller?.abort();
    }
  }, [clearTimer]);

  useEffect(() => {
    mountedRef.current = true;
    const onPageHide = () => {
      void drainRef.current(true);
    };
    window.addEventListener("pagehide", onPageHide);
    return () => {
      window.removeEventListener("pagehide", onPageHide);
      clearTimer();
      void drainRef.current(false);
      mountedRef.current = false;
    };
  }, [clearTimer]);

  return { cancel, flush: drain, schedule };
}
