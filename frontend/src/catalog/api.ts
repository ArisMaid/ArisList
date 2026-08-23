import type { HistoryRecord, Tag, WorkSummary } from "../api";

export type CatalogShelfMode = "works" | "collections" | "history";

export type CatalogShelfQuery = {
  kind: string;
  mode: CatalogShelfMode;
  collection?: string | null;
  includeTags: string[];
  query: string;
  limit?: number;
  refreshToken?: number;
};

export type CatalogPage = {
  items: WorkSummary[];
  history: HistoryRecord[];
  nextCursor: string | null;
  revision: number;
  activityRevision: number;
  backfillPending: boolean;
};

export type CatalogCounts = {
  kinds: Record<string, number>;
  total: number;
  catalog_revision: number;
  activity_revision: number;
};

export type CatalogFacetPage = {
  items: Tag[];
  nextCursor: string | null;
  revision: number;
};

type CatalogWork = {
  id: number;
  kind: string;
  title: string;
  subtitle?: string | null;
  cover_asset_id?: number | null;
  cover_version: string;
  progress: number;
  asset_count: number;
  tag_count: number;
  image_count: number;
  track_count: number;
  page_count: number;
  collection_key?: string | null;
  updated_at: string;
};

type CatalogCollection = {
  id: number;
  kind: string;
  title: string;
  subtitle: string;
  cover_asset_id?: number | null;
  cover_version: string;
  progress: number;
  work_count: number;
  tag_count: number;
  page_count: number;
  collection_key: string;
  first_work_id: number;
  updated_at: string;
};

type CatalogHistory = CatalogWork & {
  position?: string | null;
  last_opened_at: string;
};

type CatalogTag = {
  id: number;
  namespace: string;
  key: string;
  label: string;
  translated_label?: string | null;
  translated_namespace?: string | null;
  context_count: number;
};

async function requestJson<T>(url: string, signal?: AbortSignal): Promise<T> {
  const response = await fetch(url, { credentials: "same-origin", signal });
  if (!response.ok) {
    const body = await response.json().catch(() => ({ error: response.statusText }));
    throw new Error(body.error ?? response.statusText);
  }
  return response.json() as Promise<T>;
}

function baseParams(input: CatalogShelfQuery, cursor?: string | null) {
  const params = new URLSearchParams();
  if (input.kind && input.kind !== "history") params.set("kind", input.kind);
  if (input.collection) params.set("collection", input.collection);
  if (input.includeTags.length > 0) params.set("include_tag", [...input.includeTags].sort().join(","));
  if (input.query.trim()) params.set("q", input.query.trim());
  if (cursor) params.set("cursor", cursor);
  params.set("limit", String(Math.min(200, Math.max(1, Math.trunc(input.limit ?? 60)))));
  return params;
}

function workSummary(item: CatalogWork): WorkSummary {
  return {
    id: item.id,
    kind: item.kind,
    title: item.title,
    subtitle: item.subtitle,
    category: null,
    rating: null,
    progress: item.progress,
    source_path: null,
    cover_asset_id: item.cover_asset_id,
    meta_json: JSON.stringify({
      page_count: item.page_count,
      image_count: item.image_count,
      track_count: item.track_count,
      collection_key: item.collection_key
    }),
    tag_keys: null,
    tag_count: item.tag_count,
    asset_count: item.asset_count,
    updated_at: item.cover_version || item.updated_at
  };
}

function collectionSummary(item: CatalogCollection): WorkSummary {
  return {
    id: item.id,
    kind: item.kind,
    title: item.title,
    subtitle: item.subtitle,
    category: item.kind.endsWith("-collection") ? "Collection" : null,
    rating: null,
    progress: item.progress,
    source_path: null,
    cover_asset_id: item.cover_asset_id,
    meta_json: JSON.stringify({
      collection_key: item.collection_key,
      first_work_id: item.first_work_id,
      page_count: item.page_count,
      volume_count: item.work_count,
      series: item.title
    }),
    tag_keys: null,
    tag_count: item.tag_count,
    asset_count: item.work_count,
    updated_at: item.cover_version || item.updated_at
  };
}

export async function loadCatalogPage(
  input: CatalogShelfQuery,
  cursor: string | null,
  signal?: AbortSignal
): Promise<CatalogPage> {
  if (input.mode === "history") {
    const params = baseParams(input, cursor);
    const response = await requestJson<{
      items: CatalogHistory[];
      next_cursor?: string | null;
      catalog_revision: number;
      activity_revision: number;
    }>(`/api/catalog/history?${params.toString()}`, signal);
    return {
      items: response.items.map(workSummary),
      history: response.items.map((item) => ({
        work_id: item.id,
        kind: item.kind,
        title: item.title,
        subtitle: item.subtitle,
        cover_asset_id: item.cover_asset_id,
        progress: item.progress,
        position: item.position,
        last_opened_at: item.last_opened_at
      })),
      nextCursor: response.next_cursor ?? null,
      revision: response.catalog_revision,
      activityRevision: response.activity_revision,
      backfillPending: false
    };
  }
  if (input.mode === "collections" && !input.collection) {
    const params = baseParams(input, cursor);
    const response = await requestJson<{
      items: CatalogCollection[];
      next_cursor?: string | null;
      catalog_revision: number;
      activity_revision: number;
      backfill_pending: boolean;
    }>(`/api/catalog/collections?${params.toString()}`, signal);
    return {
      items: response.items.map(collectionSummary),
      history: [],
      nextCursor: response.next_cursor ?? null,
      revision: response.catalog_revision,
      activityRevision: response.activity_revision,
      backfillPending: response.backfill_pending
    };
  }
  const params = baseParams(input, cursor);
  const response = await requestJson<{
    items: CatalogWork[];
    next_cursor?: string | null;
    catalog_revision: number;
    activity_revision: number;
  }>(`/api/catalog/works?${params.toString()}`, signal);
  return {
    items: response.items.map(workSummary),
    history: [],
    nextCursor: response.next_cursor ?? null,
    revision: response.catalog_revision,
    activityRevision: response.activity_revision,
    backfillPending: false
  };
}

export async function loadRandomCatalogWork(
  input: CatalogShelfQuery,
  signal?: AbortSignal
): Promise<WorkSummary | null> {
  const params = baseParams(input);
  params.delete("limit");
  const response = await requestJson<{
    item?: CatalogWork | null;
    catalog_revision: number;
    activity_revision: number;
  }>(`/api/catalog/random?${params.toString()}`, signal);
  return response.item ? workSummary(response.item) : null;
}

export async function loadCatalogCounts(
  includeTags: string[],
  query: string,
  signal?: AbortSignal
): Promise<CatalogCounts> {
  const params = new URLSearchParams();
  if (includeTags.length > 0) params.set("include_tag", [...includeTags].sort().join(","));
  if (query.trim()) params.set("q", query.trim());
  return requestJson<CatalogCounts>(`/api/catalog/counts?${params.toString()}`, signal);
}

export async function loadCatalogFacets(
  input: CatalogShelfQuery,
  tagQuery: string,
  cursor: string | null,
  signal?: AbortSignal
): Promise<CatalogFacetPage> {
  const params = baseParams({ ...input, limit: 120 }, cursor);
  params.delete("limit");
  params.set("limit", "120");
  if (tagQuery.trim()) params.set("tag_q", tagQuery.trim());
  const response = await requestJson<{
    items: CatalogTag[];
    next_cursor?: string | null;
    catalog_revision: number;
  }>(`/api/catalog/facets/tags?${params.toString()}`, signal);
  return {
    items: response.items.map((item) => ({
      id: item.id,
      namespace: item.namespace,
      key: item.key,
      label: item.label,
      translated_label: item.translated_label,
      translated_namespace: item.translated_namespace,
      source: "catalog",
      intro: null,
      links: null,
      count: item.context_count
    })),
    nextCursor: response.next_cursor ?? null,
    revision: response.catalog_revision
  };
}

export async function loadCatalogJobs(signal?: AbortSignal) {
  return requestJson<import("../api").Job[]>("/api/jobs", signal);
}
