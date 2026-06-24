use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use axum::extract::{Query, State};
use axum::http::HeaderMap;
use axum::Json;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sqlx::Row;
use tantivy::collector::TopDocs;
use tantivy::query::QueryParser;
use tantivy::schema::{Field, Schema, STORED, STRING, TEXT};
use tantivy::{doc, Document, Index, TantivyDocument, Term};

use crate::auth;
use crate::error::{AppError, Result};
use crate::AppState;

#[derive(Debug, Deserialize)]
pub struct SearchRequest {
    pub q: Option<String>,
    pub limit: Option<usize>,
}

#[derive(Debug, Serialize)]
pub struct SearchResponse {
    pub query: String,
    pub hits: Vec<SearchHit>,
    pub rebuilt: bool,
    pub took_ms: u128,
}

#[derive(Debug, Serialize)]
pub struct SearchHit {
    pub work_id: i64,
    pub score: f32,
    pub title: String,
    pub kind: String,
}

#[derive(Debug, Clone)]
struct SearchIndexRow {
    id: i64,
    kind: String,
    title: String,
    category: Option<String>,
    description: Option<String>,
    source_path: Option<String>,
    tags: Option<String>,
}

#[derive(Clone, Copy)]
struct SearchFields {
    work_id: Field,
    kind: Field,
    title: Field,
    body: Field,
}

pub async fn search(
    State(state): State<Arc<AppState>>,
    Query(input): Query<SearchRequest>,
) -> Result<Json<SearchResponse>> {
    let query = input.q.unwrap_or_default().trim().to_string();
    let limit = input.limit.unwrap_or(48).clamp(1, 200);
    if query.is_empty() {
        return Ok(Json(SearchResponse {
            query,
            hits: Vec::new(),
            rebuilt: false,
            took_ms: 0,
        }));
    }

    let started = Instant::now();
    let index_dir = search_index_dir(&state);
    let mut rebuilt = false;
    if !index_dir.join("meta.json").exists() {
        rebuild_search_index(state.clone()).await?;
        rebuilt = true;
    }
    let hits = query_search_index(index_dir, query.clone(), limit).await?;
    Ok(Json(SearchResponse {
        query,
        hits,
        rebuilt,
        took_ms: started.elapsed().as_millis(),
    }))
}

pub async fn enqueue_rebuild(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Json<Value>> {
    auth::require_csrf(&state, &headers, "search.rebuild").await?;
    let id = state
        .db
        .create_job("rebuild-search-index", "queued", json!({ "source": "api" }))
        .await?;
    state
        .db
        .audit("search.rebuild", "queued", json!({ "job_id": id }))
        .await?;
    Ok(Json(json!({ "job_id": id, "status": "queued" })))
}

pub async fn rebuild_search_index(state: Arc<AppState>) -> Result<usize> {
    let rows = search_index_rows(&state).await?;
    let count = rows.len();
    let index_dir = search_index_dir(&state);
    tokio::task::spawn_blocking(move || rebuild_search_index_blocking(index_dir, rows))
        .await
        .map_err(|e| AppError::Other(format!("search index worker failed: {e}")))??;
    state
        .db
        .audit("search.rebuild", "done", json!({ "works": count }))
        .await?;
    Ok(count)
}

async fn search_index_rows(state: &AppState) -> Result<Vec<SearchIndexRow>> {
    let rows = sqlx::query(
        r#"
        SELECT
            w.id,
            w.kind,
            w.title,
            w.category,
            w.description,
            w.source_path,
            GROUP_CONCAT(
                DISTINCT t.namespace || ':' || t.key || ' ' || t.label || ' ' || COALESCE(t.translated_label, '')
            ) AS tags
        FROM works w
        LEFT JOIN work_tags wt ON wt.work_id = w.id
        LEFT JOIN tags t ON t.id = wt.tag_id
        GROUP BY w.id
        ORDER BY w.updated_at DESC, w.id DESC
        "#,
    )
    .fetch_all(state.db.pool())
    .await?;

    Ok(rows
        .into_iter()
        .map(|row| SearchIndexRow {
            id: row.get("id"),
            kind: row.get("kind"),
            title: row.get("title"),
            category: row.get("category"),
            description: row.get("description"),
            source_path: row.get("source_path"),
            tags: row.get("tags"),
        })
        .collect())
}

async fn query_search_index(
    index_dir: PathBuf,
    query: String,
    limit: usize,
) -> Result<Vec<SearchHit>> {
    tokio::task::spawn_blocking(move || query_search_index_blocking(index_dir, query, limit))
        .await
        .map_err(|e| AppError::Other(format!("search query worker failed: {e}")))?
}

fn rebuild_search_index_blocking(index_dir: PathBuf, rows: Vec<SearchIndexRow>) -> Result<()> {
    std::fs::create_dir_all(&index_dir)?;
    let (index, fields) = open_or_create_index(&index_dir)?;
    let mut writer = index.writer(50_000_000).map_err(search_error)?;
    for row in rows {
        let id = row.id.to_string();
        writer.delete_term(Term::from_field_text(fields.work_id, &id));
        let body = [
            row.category.as_deref().unwrap_or_default(),
            row.description.as_deref().unwrap_or_default(),
            row.source_path.as_deref().unwrap_or_default(),
            row.tags.as_deref().unwrap_or_default(),
        ]
        .join(" ");
        writer
            .add_document(doc!(
                fields.work_id => id,
                fields.kind => row.kind,
                fields.title => row.title,
                fields.body => body,
            ))
            .map_err(search_error)?;
    }
    writer.commit().map_err(search_error)?;
    Ok(())
}

fn query_search_index_blocking(
    index_dir: PathBuf,
    query: String,
    limit: usize,
) -> Result<Vec<SearchHit>> {
    let (index, fields) = open_or_create_index(&index_dir)?;
    let reader = index.reader().map_err(search_error)?;
    let searcher = reader.searcher();
    let schema = index.schema();
    let parser = QueryParser::for_index(&index, vec![fields.title, fields.body, fields.kind]);
    let query_text = safe_query_text(&query);
    if query_text.is_empty() {
        return Ok(Vec::new());
    }
    let parsed = parser
        .parse_query(&query_text)
        .or_else(|_| parser.parse_query(&format!("\"{query_text}\"")))
        .map_err(search_error)?;
    let top_docs = searcher
        .search(&parsed, &TopDocs::with_limit(limit))
        .map_err(search_error)?;

    let mut hits = Vec::with_capacity(top_docs.len());
    for (score, address) in top_docs {
        let doc: TantivyDocument = searcher.doc(address).map_err(search_error)?;
        let value: Value =
            serde_json::from_str(&doc.to_json(&schema)).unwrap_or_else(|_| json!({}));
        let Some(work_id) =
            first_stored_text(&value, "work_id").and_then(|id| id.parse::<i64>().ok())
        else {
            continue;
        };
        hits.push(SearchHit {
            work_id,
            score,
            title: first_stored_text(&value, "title").unwrap_or_default(),
            kind: first_stored_text(&value, "kind").unwrap_or_default(),
        });
    }
    Ok(hits)
}

fn open_or_create_index(index_dir: &Path) -> Result<(Index, SearchFields)> {
    if index_dir.join("meta.json").exists() {
        let index = Index::open_in_dir(index_dir).map_err(search_error)?;
        let schema = index.schema();
        let fields = fields_from_schema(&schema);
        return Ok((index, fields));
    }
    let (schema, fields) = build_schema();
    let index = Index::create_in_dir(index_dir, schema).map_err(search_error)?;
    Ok((index, fields))
}

fn build_schema() -> (Schema, SearchFields) {
    let mut builder = Schema::builder();
    let work_id = builder.add_text_field("work_id", STRING | STORED);
    let kind = builder.add_text_field("kind", STRING | STORED);
    let title = builder.add_text_field("title", TEXT | STORED);
    let body = builder.add_text_field("body", TEXT);
    (
        builder.build(),
        SearchFields {
            work_id,
            kind,
            title,
            body,
        },
    )
}

fn fields_from_schema(schema: &Schema) -> SearchFields {
    SearchFields {
        work_id: schema
            .get_field("work_id")
            .expect("search schema missing work_id"),
        kind: schema
            .get_field("kind")
            .expect("search schema missing kind"),
        title: schema
            .get_field("title")
            .expect("search schema missing title"),
        body: schema
            .get_field("body")
            .expect("search schema missing body"),
    }
}

fn first_stored_text(value: &Value, field: &str) -> Option<String> {
    value
        .get(field)
        .and_then(Value::as_array)
        .and_then(|items| items.first())
        .and_then(Value::as_str)
        .map(str::to_string)
}

fn safe_query_text(value: &str) -> String {
    value
        .chars()
        .map(|ch| {
            if ch.is_alphanumeric() || ch.is_whitespace() || matches!(ch, '_' | '-' | ':') {
                ch
            } else {
                ' '
            }
        })
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

fn search_index_dir(state: &AppState) -> PathBuf {
    state.config.data_dir.join("search-index")
}

fn search_error(error: impl std::fmt::Display) -> AppError {
    AppError::Other(format!("search index error: {error}"))
}
