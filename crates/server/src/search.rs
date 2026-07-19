use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock};
use std::time::Instant;

use axum::extract::{Query, State};
use axum::http::HeaderMap;
use axum::Json;
use futures::TryStreamExt;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sqlx::Row;
use tantivy::collector::TopDocs;
use tantivy::query::QueryParser;
use tantivy::schema::{
    Field, IndexRecordOption, Schema, TextFieldIndexing, TextOptions, STORED, STRING, TEXT,
};
use tantivy::tokenizer::NgramTokenizer;
use tantivy::{doc, Document, Index, TantivyDocument};
use tokio::sync::{mpsc, Mutex as AsyncMutex};

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
    title_ngram: Field,
    body: Field,
    body_ngram: Field,
}

const CJK_NGRAM_TOKENIZER: &str = "cjk_ngram_v1";
const SEARCH_INDEX_CHANNEL_CAPACITY: usize = 256;
static SEARCH_REBUILD_LOCK: LazyLock<AsyncMutex<()>> = LazyLock::new(|| AsyncMutex::new(()));

const SEARCH_INDEX_SQL: &str = r#"
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
"#;

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
    let rebuilt = ensure_search_index(state.clone()).await?;
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
    let (id, created) = state
        .db
        .create_job_if_absent("rebuild-search-index", "queued", json!({ "source": "api" }))
        .await?;
    if created {
        state
            .db
            .audit("search.rebuild", "queued", json!({ "job_id": id }))
            .await?;
    }
    Ok(Json(
        json!({ "job_id": id, "status": if created { "queued" } else { "active" } }),
    ))
}

pub async fn rebuild_search_index(state: Arc<AppState>) -> Result<usize> {
    let _guard = SEARCH_REBUILD_LOCK.lock().await;
    rebuild_search_index_unlocked(state).await
}

async fn ensure_search_index(state: Arc<AppState>) -> Result<bool> {
    let index_dir = search_index_dir(&state);
    if index_dir.join("meta.json").exists() {
        return Ok(false);
    }
    let _guard = SEARCH_REBUILD_LOCK.lock().await;
    if index_dir.join("meta.json").exists() {
        return Ok(false);
    }
    rebuild_search_index_unlocked(state).await?;
    Ok(true)
}

async fn rebuild_search_index_unlocked(state: Arc<AppState>) -> Result<usize> {
    let index_dir = search_index_dir(&state);
    let (sender, receiver) = mpsc::channel(SEARCH_INDEX_CHANNEL_CAPACITY);
    let worker = tokio::task::spawn_blocking(move || {
        rebuild_search_index_from_receiver(index_dir, receiver)
    });

    let mut rows = sqlx::query(SEARCH_INDEX_SQL).fetch(state.db.pool());
    loop {
        match rows.try_next().await {
            Ok(Some(row)) => {
                let item = SearchIndexRow {
                    id: row.get("id"),
                    kind: row.get("kind"),
                    title: row.get("title"),
                    category: row.get("category"),
                    description: row.get("description"),
                    source_path: row.get("source_path"),
                    tags: row.get("tags"),
                };
                if sender.send(Ok(item)).await.is_err() {
                    break;
                }
            }
            Ok(None) => break,
            Err(err) => {
                let _ = sender.send(Err(err.into())).await;
                break;
            }
        }
    }
    drop(rows);
    drop(sender);
    let count = worker
        .await
        .map_err(|e| AppError::Other(format!("search index worker failed: {e}")))??;
    state
        .db
        .audit("search.rebuild", "done", json!({ "works": count }))
        .await?;
    Ok(count)
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

#[cfg(test)]
fn rebuild_search_index_blocking(index_dir: PathBuf, rows: Vec<SearchIndexRow>) -> Result<()> {
    rebuild_search_index_from_iter(index_dir, rows.into_iter().map(Ok)).map(|_| ())
}

fn rebuild_search_index_from_receiver(
    index_dir: PathBuf,
    mut receiver: mpsc::Receiver<Result<SearchIndexRow>>,
) -> Result<usize> {
    let rows = std::iter::from_fn(move || receiver.blocking_recv());
    rebuild_search_index_from_iter(index_dir, rows)
}

fn rebuild_search_index_from_iter<I>(index_dir: PathBuf, rows: I) -> Result<usize>
where
    I: IntoIterator<Item = Result<SearchIndexRow>>,
{
    std::fs::create_dir_all(&index_dir)?;
    let (index, fields) = open_or_create_index(&index_dir)?;
    let mut writer = index.writer(50_000_000).map_err(search_error)?;
    writer.delete_all_documents().map_err(search_error)?;
    let mut count = 0;
    for row in rows {
        let row = row?;
        let id = row.id.to_string();
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
                fields.title => row.title.clone(),
                fields.title_ngram => row.title,
                fields.body => body.clone(),
                fields.body_ngram => body,
            ))
            .map_err(search_error)?;
        count += 1;
    }
    writer.commit().map_err(search_error)?;
    Ok(count)
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
    let parser = QueryParser::for_index(
        &index,
        vec![
            fields.title,
            fields.title_ngram,
            fields.body,
            fields.body_ngram,
            fields.kind,
        ],
    );
    let query_text = safe_query_text(&query);
    if query_text.is_empty() {
        return Ok(Vec::new());
    }
    let parsed = parser
        .parse_query(&query_text)
        .or_else(|_| parser.parse_query(&format!("\"{query_text}\"")))
        .map_err(search_error)?;
    let top_docs = searcher
        .search(&parsed, &TopDocs::with_limit(limit).order_by_score())
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
    let (index, fields) = if index_dir.join("meta.json").exists() {
        let index = Index::open_in_dir(index_dir).map_err(search_error)?;
        let schema = index.schema();
        let fields = fields_from_schema(&schema);
        (index, fields)
    } else {
        let (schema, fields) = build_schema();
        let index = Index::create_in_dir(index_dir, schema).map_err(search_error)?;
        (index, fields)
    };
    index.tokenizers().register(
        CJK_NGRAM_TOKENIZER,
        NgramTokenizer::new(2, 3, false).map_err(search_error)?,
    );
    Ok((index, fields))
}

fn build_schema() -> (Schema, SearchFields) {
    let mut builder = Schema::builder();
    let work_id = builder.add_text_field("work_id", STRING | STORED);
    let kind = builder.add_text_field("kind", STRING | STORED);
    let title = builder.add_text_field("title", TEXT | STORED);
    let ngram_options = TextOptions::default().set_indexing_options(
        TextFieldIndexing::default()
            .set_tokenizer(CJK_NGRAM_TOKENIZER)
            .set_index_option(IndexRecordOption::WithFreqsAndPositions),
    );
    let title_ngram = builder.add_text_field("title_ngram", ngram_options.clone());
    let body = builder.add_text_field("body", TEXT);
    let body_ngram = builder.add_text_field("body_ngram", ngram_options);
    (
        builder.build(),
        SearchFields {
            work_id,
            kind,
            title,
            title_ngram,
            body,
            body_ngram,
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
        title_ngram: schema
            .get_field("title_ngram")
            .expect("search schema missing title_ngram"),
        body: schema
            .get_field("body")
            .expect("search schema missing body"),
        body_ngram: schema
            .get_field("body_ngram")
            .expect("search schema missing body_ngram"),
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
    state.config.data_dir.join("search-index-v2")
}

fn search_error(error: impl std::fmt::Display) -> AppError {
    AppError::Other(format!("search index error: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(id: i64, title: &str) -> SearchIndexRow {
        SearchIndexRow {
            id,
            kind: "novel".to_string(),
            title: title.to_string(),
            category: None,
            description: None,
            source_path: None,
            tags: None,
        }
    }

    #[test]
    fn cjk_substring_searches_and_rebuild_removes_stale_documents() {
        let temp = tempfile::tempdir().unwrap();
        let index_dir = temp.path().join("index");

        rebuild_search_index_blocking(index_dir.clone(), vec![row(7, "败犬女主太多了")]).unwrap();
        let hits = query_search_index_blocking(index_dir.clone(), "败犬".to_string(), 10).unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].work_id, 7);

        rebuild_search_index_blocking(index_dir.clone(), Vec::new()).unwrap();
        let hits = query_search_index_blocking(index_dir, "败犬".to_string(), 10).unwrap();
        assert!(hits.is_empty());
    }
}
