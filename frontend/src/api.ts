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
  created_at: string;
};

/**
 * Path-redacted asset shape returned by the paged catalog endpoint.  The
 * server deliberately exposes only a display name here; callers that need to
 * stream an asset should use its id with `assetUrl`.
 */
export type CatalogAssetItem = {
  id: number;
  work_id: number;
  name: string;
  mime: string;
  role: string;
  variant?: string | null;
  position?: number | null;
  size?: number | null;
  meta_json: string;
  created_at: string;
};

export type CatalogAssetsResponse = {
  items: CatalogAssetItem[];
  next_cursor?: string | null;
  total: number;
  source_version: string;
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

type LibraryRequestOptions = {
  cursor?: string | null;
  limit?: number;
  includeContext?: boolean;
  signal?: AbortSignal;
};

type ProgressRequestOptions = {
  keepalive?: boolean;
  signal?: AbortSignal;
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
    updated_at?: string;
  };
  assets: Asset[];
  tags: Tag[];
  external_ids: Array<{ source: string; external_id: string; token?: string | null; url?: string | null }>;
  asset_count?: number;
  track_count?: number;
  assets_complete?: boolean;
};

export type WorkDetailAssetMode = "legacy" | "summary";

export type EpubChapter = {
  index: number;
  title: string;
  href: string;
};

export type EpubManifestResponse = {
  chapters: EpubChapter[];
  total: number;
  next_cursor?: number | null;
};

export type ComicPageInfo = {
  name: string;
  width?: number | null;
  height?: number | null;
};

export type ComicPagesResponse = {
  pages: Array<ComicPageInfo | string>;
  total?: number;
  next_cursor?: number | null;
};

export type GalleryPageResponse = {
  items: Asset[];
  next_cursor?: string | null;
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
  reader: "production" | "shadow";
  canary?: {
    production_count: number;
    shadow_count: number;
    id_match: boolean;
    order_match: boolean;
  };
  hits: Array<{ work_id: number; score: number; title: string; kind: string }>;
};

export type HealthResponse = {
  status: string;
  features?: {
    catalog_v2?: boolean;
    derivative_cache_v2?: boolean;
    file_watcher?: boolean;
  };
};

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

export type StrmTask = {
  id: string;
  asset_id: number;
  status: string;
  phase: string;
  progress: number;
  total?: number | null;
  message?: string | null;
  cooling_until?: string | null;
  requires_password: boolean;
  missing_volumes: string[];
};

export type StrmDiagnostic = {
  asset_id: number;
  scope: string;
  remote_checked: boolean;
  target_host?: string | null;
  status?: number;
  range_supported: boolean;
  content_range?: string | null;
  content_length?: number | null;
  message: string;
};

export type AppSettings = {
  detail_mode: "modal" | "docked";
  reader: {
    comic_auto_read_interval_ms: number;
    comic_prefetch_pages: number;
  };
  media_dirs: {
    comics: string[];
    novels: string[];
    audio: string[];
    gallery: string[];
    coser_picture: string[];
    comic_scan_depth: number;
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
    audio_grouping: "rj" | "folder" | "auto";
  }>;
  audio_grouping: "rj" | "folder" | "auto";
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

export type CloudSourceStatus = {
  kind: string;
  provider: string;
  mount_name?: string | null;
  root: string;
  source: "explicit" | "env" | "legacy" | string;
  scan_depth: number;
  readable: boolean;
  status: "ready" | "failed" | string;
  discovered: number;
  message?: string | null;
};

export type CloudStatus = {
  qmediasync: {
    enabled: boolean;
    base_url: string;
    configured: boolean;
    sources: number;
    strm_roots: number;
    source_details: CloudSourceStatus[];
  };
  cache: { bytes: number; files: number; quota_bytes?: number };
};

async function request<T>(url: string, init?: RequestInit): Promise<T> {
  const headers = {
    "content-type": "application/json",
    ...(init?.headers ?? {})
  } as Record<string, string>;
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

async function requestText(url: string, init?: RequestInit): Promise<string> {
  const res = await fetch(url, { ...init, credentials: "same-origin" });
  if (!res.ok) {
    const body = await res.text().catch(() => res.statusText);
    throw new Error(body || res.statusText);
  }
  return res.text();
}

export const api = {
  health: () => request<HealthResponse>("/api/health"),
  library: ({ cursor, limit = 100, includeContext, signal }: LibraryRequestOptions = {}) => {
    const params = new URLSearchParams();
    if (cursor) params.set("cursor", cursor);
    params.set("limit", String(Math.min(500, Math.max(1, Math.trunc(limit)))));
    if (includeContext !== undefined) params.set("include_context", String(includeContext));
    return request<LibraryResponse>(`/api/library?${params.toString()}`, { signal });
  },
  settings: () => request<AppSettings>("/api/settings"),
  updateSettings: (settings: AppSettings) =>
    request<AppSettings>("/api/settings", {
      method: "PATCH",
      body: JSON.stringify(settings)
    }),
  search: (q: string, limit = 48, signal?: AbortSignal) =>
    request<SearchResponse>(`/api/search?q=${encodeURIComponent(q)}&limit=${limit}`, { signal }),
  work: (id: number, signal?: AbortSignal, assetMode: WorkDetailAssetMode = "legacy") => {
    const params = new URLSearchParams({ asset_mode: assetMode });
    return request<WorkDetail>(`/api/works/${id}?${params.toString()}`, { signal });
  },
  workAssets: (id: number, options: { role?: string; cursor?: string | null; limit?: number; signal?: AbortSignal } = {}) => {
    const params = new URLSearchParams();
    if (options.role) params.set("role", options.role);
    if (options.cursor) params.set("cursor", options.cursor);
    const limit = options.limit ?? 128;
    params.set("limit", String(Math.min(200, Math.max(1, Math.trunc(limit)))));
    return request<CatalogAssetsResponse>(`/api/works/${id}/assets?${params.toString()}`, { signal: options.signal });
  },
  workHistory: (id: number, signal?: AbortSignal) => request<HistoryRecord | null>(`/api/works/${id}/history`, { signal }),
  updateProgress: (id: number, progress: number, position: string | undefined, updateToken: number, options: ProgressRequestOptions = {}) =>
    request<{ status: string; accepted: boolean; progress: number; position?: string | null }>(`/api/works/${id}/progress`, {
      method: "PATCH",
      body: JSON.stringify({ progress, position, update_token: updateToken }),
      keepalive: options.keepalive,
      signal: options.signal
    }),
  history: (signal?: AbortSignal) => request<HistoryRecord[]>("/api/history", { signal }),
  cloudStatus: () => request<CloudStatus>("/api/cloud/status"),
  testQMediaSyncStrmRoot: (input: { root: string; kind?: string; scan_depth?: number }) =>
    request<{ status: string; scope: string; remote_checked: boolean; root: string; works: number; strm_files: number; samples: string[]; message?: string }>("/api/cloud/qmediasync/test-strm-root", {
      method: "POST",
      body: JSON.stringify(input)
    }),
  prepareStrmAsset: (assetId: number) =>
    request<StrmTask>(`/api/assets/${assetId}/prepare`, { method: "POST" }),
  strmTask: (taskId: string, signal?: AbortSignal) =>
    request<StrmTask>(`/api/strm/tasks/${encodeURIComponent(taskId)}`, { signal }),
  setStrmPassword: (taskId: string, password: string) =>
    request<StrmTask>(`/api/strm/tasks/${encodeURIComponent(taskId)}/password`, {
      method: "POST",
      body: JSON.stringify({ password })
    }),
  cancelStrmTask: (taskId: string) =>
    request<StrmTask>(`/api/strm/tasks/${encodeURIComponent(taskId)}/cancel`, { method: "POST" }),
  diagnoseStrmAsset: (assetId: number) =>
    request<StrmDiagnostic>(`/api/assets/${assetId}/diagnose`, { method: "POST" }),
  galleryPage: (id: number, cursor: string | number | null = null, limit = 120, signal?: AbortSignal, version?: string | null) => {
    const params = new URLSearchParams();
    if (cursor !== null && cursor !== undefined) params.set("cursor", String(cursor));
    params.set("limit", String(Math.min(240, Math.max(1, Math.trunc(limit)))));
    return request<GalleryPageResponse>(withVersion(`/api/works/${id}/gallery?${params.toString()}`, version), { signal });
  },
  assetRoute: (id: number, signal?: AbortSignal) => request<AssetRouteInfo>(`/api/assets/${id}/route`, { signal }),
  scan: (enqueue_enrichment = false) => request<{ job_id: number; status: "queued" | "already-queued" }>("/api/scan", {
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
  comicPages: (id: number, signal?: AbortSignal, version?: string | null, cursor?: number | null, limit = 200) => {
    const params = new URLSearchParams();
    if (cursor !== undefined && cursor !== null) params.set("cursor", String(Math.max(0, Math.trunc(cursor))));
    params.set("limit", String(Math.min(500, Math.max(1, Math.trunc(limit)))));
    if (version) params.set("v", version);
    return request<ComicPagesResponse>(`/api/works/${id}/pages?${params.toString()}`, { signal });
  },
  epubManifest: (
    id: number,
    signal?: AbortSignal,
    version?: string | null,
    cursor?: number | null,
    limit?: number
  ) => {
    const params = new URLSearchParams();
    if (cursor !== undefined && cursor !== null) {
      params.set("cursor", String(Math.max(0, Math.trunc(cursor))));
    }
    if (limit !== undefined) {
      params.set("limit", String(Math.min(500, Math.max(1, Math.trunc(limit)))));
    }
    if (version) params.set("v", version);
    const query = params.toString();
    return request<EpubManifestResponse>(`/api/works/${id}/epub${query ? `?${query}` : ""}`, { signal });
  },
  epubChapterHtml: (id: number, chapter: number, signal?: AbortSignal, version?: string | null) =>
    requestText(withVersion(`/api/works/${id}/epub/${chapter}/html`, version), { signal })
};

function withVersion(url: string, version?: string | null) {
  return version ? `${url}${url.includes("?") ? "&" : "?"}v=${encodeURIComponent(version)}` : url;
}

export function assetVersion(asset?: Pick<Asset, "created_at" | "size"> | null, _workVersion?: string | null) {
  if (!asset) return undefined;
  return `${asset.created_at}:${asset.size ?? "unknown"}`;
}

/** Convert the path-redacted catalog item to the legacy local asset shape.
 * `path` is intentionally only the display name and is never used to resolve
 * a filesystem path; streaming remains id-based through `assetUrl`.
 */
export function catalogAssetToAsset(item: CatalogAssetItem): Asset {
  return {
    id: item.id,
    work_id: item.work_id,
    path: item.name,
    mime: item.mime,
    role: item.role,
    variant: item.variant,
    position: item.position,
    size: item.size,
    meta_json: item.meta_json,
    created_at: item.created_at
  };
}

export function assetUrl(id?: number | null, version?: string | null) {
  return id ? withVersion(`/api/assets/${id}/stream`, version) : "";
}

export function thumbUrl(id?: number | null, size = 360, version?: string | null) {
  return id ? withVersion(`/api/assets/${id}/thumb?size=${size}`, version) : "";
}

export function coverUrl(id?: number | null, size = 480, version?: string | null) {
  return id ? withVersion(`/api/works/${id}/cover?size=${size}`, version) : "";
}

export function comicPageUrl(workId: number, page: number, version?: string | null, size?: number) {
  const query = size && size > 0 ? `?size=${Math.round(size)}` : "";
  return withVersion(`/api/works/${workId}/pages/${page}/stream${query}`, version);
}

export function parseMeta<T extends Record<string, unknown>>(value?: string | null): T {
  if (!value) return {} as T;
  try {
    return JSON.parse(value) as T;
  } catch {
    return {} as T;
  }
}
