export type WorkSummary = {
  id: number;
  kind: "comic" | "novel" | "audio" | "gallery" | "coser-picture" | string;
  title: string;
  subtitle?: string | null;
  category?: string | null;
  rating?: number | null;
  progress: number;
  source_path?: string | null;
  cover_asset_id?: number | null;
  meta_json: string;
  tag_keys?: string | null;
  tag_count: number;
  asset_count: number;
  updated_at: string;
};

export type Asset = {
  id: number;
  work_id: number;
  path: string;
  mime: string;
  role: string;
  variant?: string | null;
  position?: number | null;
  size?: number | null;
  meta_json: string;
};

export type Tag = {
  id: number;
  namespace: string;
  key: string;
  label: string;
  translated_label?: string | null;
  translated_namespace?: string | null;
  source: string;
  intro?: string | null;
  links?: string | null;
  count: number;
};

export type Job = {
  id: number;
  job_type: string;
  status: string;
  payload_json: string;
  attempts: number;
  last_error?: string | null;
  updated_at: string;
};

export type LibraryResponse = {
  works: WorkSummary[];
  tags: Tag[];
  jobs: Job[];
  history: HistoryRecord[];
  next_cursor?: string | null;
};

export type HistoryRecord = {
  work_id: number;
  kind: string;
  title: string;
  subtitle?: string | null;
  cover_asset_id?: number | null;
  progress: number;
  position?: string | null;
  last_opened_at: string;
};

export type WorkDetail = {
  work: {
    id: number;
    kind: string;
    title: string;
    subtitle?: string | null;
    category?: string | null;
    description?: string | null;
    rating?: number | null;
    progress: number;
    source_path?: string | null;
    cover_asset_id?: number | null;
    meta_json: string;
  };
  assets: Asset[];
  tags: Tag[];
  external_ids: Array<{ source: string; external_id: string; token?: string | null; url?: string | null }>;
};

export type EpubChapter = {
  index: number;
  title: string;
  href: string;
};

export type EpubManifestResponse = {
  chapters: EpubChapter[];
};

export type ComicPageInfo = {
  name: string;
  width?: number | null;
  height?: number | null;
};

export type GalleryPageResponse = {
  items: Asset[];
  next_cursor?: number | null;
  total: number;
};

export type QueueResponse = {
  job_id: number;
  status: string;
};

export type SearchResponse = {
  query: string;
  rebuilt: boolean;
  took_ms: number;
  hits: Array<{ work_id: number; score: number; title: string; kind: string }>;
};

export type AuthSession = {
  authenticated: boolean;
  csrf?: string | null;
  user?: string | null;
};

export type ThemeMode = "light" | "dark";
export type UiMaterial = "classic" | "liquid";
export type GlassIntensity = "clear" | "standard" | "readable";

export type AssetRouteInfo = {
  asset_id: number;
  provider: "local" | "qmediasync" | string;
  policy: "local" | "qmediasync-strm" | string;
  policy_label: string;
  transfer: "qmediasync-strm" | "app-proxy" | string;
  route_label: string;
  via_qmediasync: boolean;
  via_app: boolean;
  qmediasync_host?: string | null;
  target_host?: string | null;
  note?: string | null;
};

export type AppSettings = {
  theme: ThemeMode;
  detail_mode: "modal" | "docked";
  appearance: {
    material: UiMaterial;
    glass_intensity: GlassIntensity;
  };
  reader: {
    comic_auto_read_interval_ms: number;
  };
  media_dirs: {
    comics: string[];
    novels: string[];
    audio: string[];
    gallery: string[];
    coser_picture: string[];
  };
  cover_cache_dirs: {
    comic: string;
    novel: string;
    audio: string;
    gallery: string;
    coser_picture: string;
  };
  media_sources: Array<{
    kind: "comic" | "novel" | "audio" | "gallery" | "coser-picture";
    provider: "qmediasync" | "openlist";
    root: string;
    mount_name: string;
    enabled: boolean;
    scan_depth: number;
  }>;
  qmediasync: {
    enabled: boolean;
    base_url: string;
    strm_roots: string[];
  };
  scan: {
    enqueue_enrichment: boolean;
    file_watcher: boolean;
    enrichment_concurrency: number;
  };
  openai: {
    image_model: string;
    image_configured: boolean;
  };
};

let csrfToken = typeof window !== "undefined" ? window.localStorage.getItem("media_shelf_csrf") : null;

export function setCsrfToken(token?: string | null) {
  csrfToken = token ?? null;
  if (typeof window === "undefined") return;
  if (csrfToken) window.localStorage.setItem("media_shelf_csrf", csrfToken);
  else window.localStorage.removeItem("media_shelf_csrf");
}

async function request<T>(url: string, init?: RequestInit): Promise<T> {
  const method = (init?.method ?? "GET").toUpperCase();
  const headers = {
    "content-type": "application/json",
    ...(init?.headers ?? {})
  } as Record<string, string>;
  if (!["GET", "HEAD", "OPTIONS"].includes(method) && csrfToken) {
    headers["x-csrf-token"] = csrfToken;
  }
  const res = await fetch(url, {
    ...init,
    credentials: "same-origin",
    headers
  });
  if (!res.ok) {
    const body = await res.json().catch(() => ({ error: res.statusText }));
    throw new Error(body.error ?? res.statusText);
  }
  return res.json() as Promise<T>;
}

async function requestText(url: string): Promise<string> {
  const res = await fetch(url);
  if (!res.ok) {
    const body = await res.text().catch(() => res.statusText);
    throw new Error(body || res.statusText);
  }
  return res.text();
}

export const api = {
  authSession: async () => {
    const session = await request<AuthSession>("/api/auth/session");
    setCsrfToken(session.csrf);
    return session;
  },
  login: async (password: string) => {
    const session = await request<AuthSession>("/api/auth/login", {
      method: "POST",
      body: JSON.stringify({ password })
    });
    setCsrfToken(session.csrf);
    return session;
  },
  changePassword: (password: string) =>
    request<{ status: string }>("/api/auth/password", {
      method: "PATCH",
      body: JSON.stringify({ password })
    }),
  resetPassword: () =>
    request<{ status: string; password: string }>("/api/auth/password/reset", {
      method: "POST",
      body: JSON.stringify({})
    }),
  logout: async () => {
    await fetch("/api/auth/logout", {
      method: "POST",
      credentials: "same-origin",
      headers: csrfToken ? { "x-csrf-token": csrfToken } : undefined
    });
    setCsrfToken(null);
  },
  library: () => request<LibraryResponse>("/api/library"),
  settings: () => request<AppSettings>("/api/settings"),
  updateSettings: (settings: AppSettings) =>
    request<AppSettings>("/api/settings", {
      method: "PATCH",
      body: JSON.stringify(settings)
    }),
  search: (q: string, limit = 48) =>
    request<SearchResponse>(`/api/search?q=${encodeURIComponent(q)}&limit=${limit}`),
  work: (id: number) => request<WorkDetail>(`/api/works/${id}`),
  updateProgress: (id: number, progress: number, position?: string) =>
    request<{ status: string; progress: number; position?: string | null }>(`/api/works/${id}/progress`, {
      method: "PATCH",
      body: JSON.stringify({ progress, position })
    }),
  history: () => request<HistoryRecord[]>("/api/history"),
  cloudStatus: () => request<{ qmediasync: { enabled: boolean; base_url: string; configured: boolean; sources: number; strm_roots: number }; cache: { bytes: number; files: number } }>("/api/cloud/status"),
  testQMediaSyncStrmRoot: (input: { root: string; kind?: string; scan_depth?: number }) =>
    request<{ status: string; root: string; works: number; strm_files: number; samples: string[] }>("/api/cloud/qmediasync/test-strm-root", {
      method: "POST",
      body: JSON.stringify(input)
    }),
  galleryPage: (id: number, cursor = 0, limit = 120) =>
    request<GalleryPageResponse>(`/api/works/${id}/gallery?cursor=${cursor}&limit=${limit}`),
  assetRoute: (id: number) => request<AssetRouteInfo>(`/api/assets/${id}/route`),
  scan: (enqueue_enrichment = false) => request<{ comics: number; novels: number; audio: number; gallery: number; coser_picture: number; jobs_created: number }>("/api/scan", {
    method: "POST",
    body: JSON.stringify({ enqueue_enrichment })
  }),
  enrich: (kind = "import-tag-translations") =>
    request<{ job_id: number; status: string }>("/api/enrich", {
      method: "POST",
      body: JSON.stringify({ kind })
    }),
  generateAsset: (input: { prompt: string; style?: string; allow_cover_style?: boolean; sanitized_asset_id?: number }) =>
    request<QueueResponse>("/api/assets/generate", {
      method: "POST",
      body: JSON.stringify(input)
    }),
  comicPages: (id: number) => request<{ pages: Array<ComicPageInfo | string> }>(`/api/works/${id}/pages`),
  epubManifest: (id: number) => request<EpubManifestResponse>(`/api/works/${id}/epub`),
  epubChapterHtml: (id: number, chapter: number) => requestText(`/api/works/${id}/epub/${chapter}/html`)
};

export function assetUrl(id?: number | null) {
  return id ? `/api/assets/${id}/stream` : "";
}

export function thumbUrl(id?: number | null, size = 360) {
  return id ? `/api/assets/${id}/thumb?size=${size}` : "";
}

export function coverUrl(id?: number | null, size = 480) {
  return id ? `/api/works/${id}/cover?size=${size}` : "";
}

export function parseMeta<T extends Record<string, unknown>>(value?: string | null): T {
  if (!value) return {} as T;
  try {
    return JSON.parse(value) as T;
  } catch {
    return {} as T;
  }
}
