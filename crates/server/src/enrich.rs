use std::sync::Arc;
use std::time::Duration;

use axum::extract::State;
use axum::http::HeaderMap;
use axum::Json;
use base64::Engine;
use futures::{SinkExt, StreamExt};
use regex::Regex;
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::Message;

use crate::auth;
use crate::error::{AppError, Result};
use crate::scanner;
use crate::search;
use crate::AppState;

#[derive(Debug, Deserialize)]
pub struct EnrichRequest {
    pub kind: Option<String>,
    pub work_id: Option<i64>,
}

pub async fn enqueue_enrich(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(input): Json<EnrichRequest>,
) -> Result<Json<Value>> {
    auth::require_csrf(&state, &headers, "enrich").await?;
    let kind = input
        .kind
        .unwrap_or_else(|| "import-tag-translations".to_string());
    let id = state
        .db
        .create_job(&kind, "queued", json!({ "work_id": input.work_id }))
        .await?;
    state
        .db
        .audit(
            "enrich",
            "queued",
            json!({ "job_id": id, "kind": kind, "work_id": input.work_id }),
        )
        .await?;
    Ok(Json(json!({ "job_id": id, "status": "queued" })))
}

pub async fn run_job(state: Arc<AppState>, id: i64, job_type: &str, payload: Value) -> Result<()> {
    match job_type {
        "import-tag-translations" => import_tag_translations(state).await,
        "scan-library" => {
            let enqueue = payload
                .get("enqueue_enrichment")
                .and_then(Value::as_bool)
                .unwrap_or(true);
            scanner::scan_all(&state, enqueue).await.map(|_| ())
        }
        "enrich-asmr-work" | "enrich-lightnovel-work" => Ok(()),
        "generate-image-asset" => generate_image_asset(state, id, payload).await,
        "rebuild-search-index" => search::rebuild_search_index(state).await.map(|_| ()),
        _ => Err(AppError::BadRequest(format!(
            "unknown job type: {job_type}"
        ))),
    }
}

async fn enrich_asmr_work(state: Arc<AppState>, payload: Value) -> Result<()> {
    let work_id = payload
        .get("work_id")
        .and_then(Value::as_i64)
        .ok_or_else(|| AppError::BadRequest("enrich-asmr-work requires work_id".to_string()))?;
    let rj = payload
        .get("rj")
        .and_then(Value::as_str)
        .ok_or_else(|| AppError::BadRequest("enrich-asmr-work requires rj".to_string()))?;

    let metadata_url = format!("https://asmr.one/api/workInfo/{rj}");
    let details_url = format!("https://asmr.one/api/work/{rj}");
    let tracks_url = format!("https://asmr.one/api/tracks/{rj}?v=2");

    let metadata = state
        .http
        .get(&metadata_url)
        .send()
        .await?
        .error_for_status()?
        .json::<Value>()
        .await?;
    let details = fetch_optional_json(&state.http, &details_url).await;
    let tracks = fetch_optional_json(&state.http, &tracks_url).await;

    let (title, circle, description, rating, cover, named_tags, vas) = {
        let source = details.as_ref().unwrap_or(&metadata);
        (
            string_field(source, &["title", "name"]),
            source
                .get("circle")
                .and_then(|circle| string_field(circle, &["name", "source_name"]))
                .or_else(|| string_field(source, &["circle_name", "maker_name"])),
            string_field(source, &["description", "intro", "review"]),
            number_field(source, &["rating", "rate_average_2dp", "rate_average"]),
            string_field(source, &["mainCoverUrl", "main_cover_url", "cover_url"]),
            collect_named_items(source.get("tags")),
            collect_named_items(source.get("vas")),
        )
    };

    state
        .db
        .update_work_enrichment(
            work_id,
            title.as_deref(),
            Some("Audio"),
            description.as_deref(),
            rating,
            json!({
                "rj": rj,
                "asmr_metadata": metadata,
                "asmr_details": details,
                "asmr_tracks": tracks,
                "cover_url": cover,
                "circle": circle.clone(),
            }),
        )
        .await?;

    if let Some(circle) = circle.as_deref() {
        link_tag(
            &state,
            work_id,
            "circle",
            &normalize_key(circle),
            circle,
            "asmr.one",
        )
        .await?;
    }
    for tag in named_tags {
        link_tag(
            &state,
            work_id,
            "audio",
            &normalize_key(&tag),
            &tag,
            "asmr.one",
        )
        .await?;
    }
    for va in vas {
        link_tag(&state, work_id, "va", &normalize_key(&va), &va, "asmr.one").await?;
    }
    Ok(())
}

async fn enrich_lightnovel_work(state: Arc<AppState>, payload: Value) -> Result<()> {
    let work_id = payload
        .get("work_id")
        .and_then(Value::as_i64)
        .ok_or_else(|| {
            AppError::BadRequest("enrich-lightnovel-work requires work_id".to_string())
        })?;
    let detail = state.db.work_detail(work_id).await?;
    let title = payload
        .get("title")
        .and_then(Value::as_str)
        .map(str::to_string)
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| detail.work.title.clone());
    let series = payload
        .get("series")
        .and_then(Value::as_str)
        .filter(|s| !s.trim().is_empty());
    let creator = payload
        .get("creator")
        .and_then(Value::as_str)
        .filter(|s| !s.trim().is_empty());
    let subjects = payload
        .get("subjects")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(Value::as_str)
                .map(str::trim)
                .filter(|item| !item.is_empty())
                .map(str::to_string)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    let mut queries = Vec::new();
    push_unique(&mut queries, title.as_str());
    if let Some(series) = series {
        push_unique(&mut queries, series);
    }
    if let Some(cleaned) = title
        .split(|ch| matches!(ch, '-' | '：' | ':' | '（' | '('))
        .next()
    {
        push_unique(&mut queries, cleaned);
    }

    let (candidate, search_payload) =
        find_lightnovel_candidate(&state, &queries, &subjects, creator).await?;
    let Some(candidate) = candidate else {
        let meta = merge_work_meta(
            &detail.work.meta_json,
            json!({
                "lightnovel": {
                    "status": "not-found",
                    "queries": queries,
                    "fallback": "epub"
                }
            }),
        );
        state
            .db
            .update_work_enrichment(work_id, None, Some("Light Novel"), None, None, meta)
            .await?;
        return Ok(());
    };

    let remote_id = string_field(&candidate, &["Id", "id"]).or_else(|| {
        number_field(&candidate, &["Id", "id"])
            .map(|v| v as i64)
            .map(|v| v.to_string())
    });
    let detail_payload = match remote_id.as_deref().and_then(|id| id.parse::<i64>().ok()) {
        Some(id) => lightnovel_invoke_any(&state, "GetBookInfo", json!({ "Id": id }))
            .await
            .ok(),
        None => None,
    };
    let book = detail_payload
        .as_ref()
        .and_then(|value| value.get("Book"))
        .unwrap_or(&candidate);

    let remote_title = string_field(book, &["Title", "Name", "BookName", "OriginalName"]);
    let description = string_field(book, &["Description", "Intro", "Summary", "Remark"]);
    let rating = number_field(book, &["Rating", "Rate", "Score"]);
    let cover = string_field(book, &["Cover", "CoverUrl", "CoverPath"]);
    let user_name = book
        .get("User")
        .and_then(|user| string_field(user, &["UserName", "Name"]))
        .or_else(|| string_field(book, &["Author", "AuthorName", "Writer"]));
    let remote_series = string_field(book, &["Series", "SeriesName", "Collection"]);
    let tags = collect_named_items(book.get("Tags"))
        .into_iter()
        .chain(collect_named_items(book.get("Tag")))
        .collect::<Vec<_>>();

    let meta = merge_work_meta(
        &detail.work.meta_json,
        json!({
            "lightnovel": {
                "status": "ok",
                "id": remote_id.clone(),
                "cover_url": cover.clone(),
                "search": search_payload.clone(),
                "detail": detail_payload.clone(),
            }
        }),
    );
    state
        .db
        .update_work_enrichment(
            work_id,
            remote_title.as_deref(),
            Some("Light Novel"),
            description.as_deref(),
            rating,
            meta,
        )
        .await?;

    if let Some(id) = remote_id.as_deref() {
        state
            .db
            .upsert_external_id(
                work_id,
                "lightnovel",
                id,
                None,
                Some(&format!("https://www.lightnovel.app/book/info/{id}")),
            )
            .await?;
        link_tag(
            &state,
            work_id,
            "source",
            "lightnovel.app",
            "LightNovel.app",
            "lightnovel.app",
        )
        .await?;
    }
    if let Some(author) = user_name.as_deref() {
        link_tag(
            &state,
            work_id,
            "artist",
            &normalize_key(author),
            author,
            "lightnovel.app",
        )
        .await?;
    }
    if let Some(series) = remote_series.as_deref().or(series) {
        link_tag(
            &state,
            work_id,
            "series",
            &normalize_key(series),
            series,
            "lightnovel.app",
        )
        .await?;
    }
    for tag in tags {
        link_tag(
            &state,
            work_id,
            "ln",
            &normalize_key(&tag),
            &tag,
            "lightnovel.app",
        )
        .await?;
    }
    Ok(())
}

async fn find_lightnovel_candidate(
    state: &AppState,
    queries: &[String],
    tag_queries: &[String],
    creator: Option<&str>,
) -> Result<(Option<Value>, Option<Value>)> {
    let mut last_error = None;
    for query in queries {
        let request = json!({
            "Page": 1,
            "Size": 8,
            "KeyWords": query,
            "IgnoreJapanese": false,
            "IgnoreAI": false,
        });
        for method in ["GetBookListByName", "GetBookListByTitle", "GetBookList"] {
            match lightnovel_invoke_any(state, method, request.clone()).await {
                Ok(response) => {
                    let books = extract_lightnovel_books(&response);
                    if let Some(book) = choose_lightnovel_book(books, query, creator) {
                        return Ok((Some(book), Some(response)));
                    }
                }
                Err(err) => last_error = Some(err.to_string()),
            }
        }
    }
    if !tag_queries.is_empty() {
        let tag_query = tag_queries
            .iter()
            .take(6)
            .cloned()
            .collect::<Vec<_>>()
            .join(" ");
        let request = json!({
            "Page": 1,
            "Size": 8,
            "KeyWords": tag_query,
            "IgnoreJapanese": false,
            "IgnoreAI": false,
        });
        match lightnovel_invoke_any(state, "GetBookListByTags", request).await {
            Ok(response) => {
                let books = extract_lightnovel_books(&response);
                if let Some(book) = choose_lightnovel_book(
                    books,
                    queries.first().map(String::as_str).unwrap_or(""),
                    creator,
                ) {
                    return Ok((Some(book), Some(response)));
                }
            }
            Err(err) => last_error = Some(err.to_string()),
        }
    }
    if let Some(error) = last_error {
        return Err(AppError::Other(format!(
            "LightNovelShelf enrichment failed: {error}"
        )));
    }
    Ok((None, None))
}

fn extract_lightnovel_books(value: &Value) -> Vec<Value> {
    if let Some(items) = value.as_array() {
        return items.clone();
    }
    for field in ["Data", "Items", "Books", "List"] {
        if let Some(items) = value.get(field).and_then(Value::as_array) {
            return items.clone();
        }
    }
    if let Some(book) = value.get("Book").and_then(Value::as_object) {
        return vec![Value::Object(book.clone())];
    }
    Vec::new()
}

fn choose_lightnovel_book(books: Vec<Value>, query: &str, creator: Option<&str>) -> Option<Value> {
    let query_key = normalize_key(query);
    let creator_key = creator.map(normalize_key);
    books.into_iter().max_by_key(|book| {
        let title =
            string_field(book, &["Title", "Name", "BookName", "OriginalName"]).unwrap_or_default();
        let title_key = normalize_key(&title);
        let mut score = 0;
        if !query_key.is_empty() && title_key == query_key {
            score += 100;
        }
        if !query_key.is_empty()
            && (title_key.contains(&query_key) || query_key.contains(&title_key))
        {
            score += 50;
        }
        if let Some(creator_key) = creator_key.as_deref() {
            let author = book
                .get("User")
                .and_then(|user| string_field(user, &["UserName", "Name"]))
                .or_else(|| string_field(book, &["Author", "AuthorName", "Writer"]))
                .unwrap_or_default();
            if normalize_key(&author).contains(creator_key) {
                score += 20;
            }
        }
        score
    })
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SignalrNegotiate {
    connection_id: Option<String>,
    connection_token: Option<String>,
}

async fn lightnovel_invoke_any(state: &AppState, target: &str, argument: Value) -> Result<Value> {
    let mut last_error = None;
    for base in &state.config.lightnovel_api_bases {
        match lightnovel_invoke(
            &state.http,
            base,
            state.config.lightnovel_access_token.as_deref(),
            target,
            argument.clone(),
        )
        .await
        {
            Ok(value) => return Ok(value),
            Err(err) => last_error = Some(err.to_string()),
        }
    }
    Err(AppError::Other(format!(
        "all LightNovelShelf API bases failed: {}",
        last_error.unwrap_or_else(|| "no API base configured".to_string())
    )))
}

async fn lightnovel_invoke(
    http: &reqwest::Client,
    base: &str,
    access_token: Option<&str>,
    target: &str,
    argument: Value,
) -> Result<Value> {
    let mut request = http.post(format!(
        "{}/hub/api/negotiate?negotiateVersion=1",
        base.trim_end_matches('/')
    ));
    if let Some(token) = access_token {
        request = request.bearer_auth(token);
    }
    let negotiate: SignalrNegotiate = request.send().await?.error_for_status()?.json().await?;
    let token = negotiate
        .connection_token
        .or(negotiate.connection_id)
        .ok_or_else(|| {
            AppError::Other(
                "LightNovelShelf negotiate did not return a connection token".to_string(),
            )
        })?;

    let mut ws_url = url::Url::parse(base).map_err(|e| AppError::BadRequest(e.to_string()))?;
    ws_url
        .set_scheme(if ws_url.scheme() == "https" {
            "wss"
        } else {
            "ws"
        })
        .map_err(|_| AppError::BadRequest("invalid LightNovelShelf API scheme".to_string()))?;
    ws_url.set_path("/hub/api");
    {
        let mut query = ws_url.query_pairs_mut();
        query.clear().append_pair("id", &token);
        if let Some(access_token) = access_token {
            query.append_pair("access_token", access_token);
        }
    }

    let (mut socket, _) = timeout(
        Duration::from_secs(12),
        tokio_tungstenite::connect_async(ws_url.as_str()),
    )
    .await
    .map_err(|_| AppError::Other("LightNovelShelf websocket connect timed out".to_string()))?
    .map_err(|e| AppError::Other(format!("LightNovelShelf websocket connect failed: {e}")))?;

    send_signalr_json(&mut socket, json!({ "protocol": "json", "version": 1 })).await?;
    let _ = read_signalr_json(&mut socket).await?;
    send_signalr_json(
        &mut socket,
        json!({
            "type": 1,
            "target": target,
            "arguments": [argument, { "UseGzip": false }],
            "invocationId": "1"
        }),
    )
    .await?;

    loop {
        for message in read_signalr_json(&mut socket).await? {
            if message.get("type").and_then(Value::as_i64) == Some(3)
                && message.get("invocationId").and_then(Value::as_str) == Some("1")
            {
                if let Some(error) = message.get("error").and_then(Value::as_str) {
                    return Err(AppError::Other(format!(
                        "LightNovelShelf {target} failed: {error}"
                    )));
                }
                let result = message.get("result").cloned().unwrap_or(Value::Null);
                return unwrap_lightnovel_result(target, result);
            }
        }
    }
}

async fn send_signalr_json<S>(
    socket: &mut tokio_tungstenite::WebSocketStream<S>,
    value: Value,
) -> Result<()>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let text = format!("{}\u{1e}", value);
    socket
        .send(Message::Text(text.into()))
        .await
        .map_err(|e| AppError::Other(format!("SignalR send failed: {e}")))
}

async fn read_signalr_json<S>(
    socket: &mut tokio_tungstenite::WebSocketStream<S>,
) -> Result<Vec<Value>>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    loop {
        let frame = timeout(Duration::from_secs(20), socket.next())
            .await
            .map_err(|_| AppError::Other("SignalR read timed out".to_string()))?
            .ok_or_else(|| AppError::Other("SignalR socket closed".to_string()))?
            .map_err(|e| AppError::Other(format!("SignalR read failed: {e}")))?;
        let text = match frame {
            Message::Text(text) => text.to_string(),
            Message::Binary(bytes) => String::from_utf8_lossy(&bytes).to_string(),
            Message::Ping(_) | Message::Pong(_) => continue,
            Message::Close(_) => return Err(AppError::Other("SignalR socket closed".to_string())),
            _ => continue,
        };
        let messages = text
            .split('\u{1e}')
            .map(str::trim)
            .filter(|part| !part.is_empty())
            .map(|part| {
                serde_json::from_str(part)
                    .map_err(|e| AppError::Other(format!("SignalR JSON parse failed: {e}")))
            })
            .collect::<Result<Vec<_>>>()?;
        return Ok(messages);
    }
}

fn unwrap_lightnovel_result(target: &str, result: Value) -> Result<Value> {
    if result.get("Success").and_then(Value::as_bool) == Some(false) {
        let status = result.get("Status").and_then(Value::as_i64).unwrap_or(500);
        let message = result
            .get("Msg")
            .and_then(Value::as_str)
            .unwrap_or("request failed");
        return Err(AppError::Other(format!(
            "LightNovelShelf {target} returned {status}: {message}"
        )));
    }
    Ok(result.get("Response").cloned().unwrap_or(result))
}

fn merge_work_meta(existing: &str, patch: Value) -> Value {
    let mut base = serde_json::from_str(existing).unwrap_or_else(|_| json!({}));
    merge_json(&mut base, patch);
    base
}

fn merge_json(base: &mut Value, patch: Value) {
    match (base, patch) {
        (Value::Object(base), Value::Object(patch)) => {
            for (key, value) in patch {
                merge_json(base.entry(key).or_insert(Value::Null), value);
            }
        }
        (base, patch) => *base = patch,
    }
}

fn push_unique(items: &mut Vec<String>, value: &str) {
    let value = value.trim();
    if value.is_empty() {
        return;
    }
    if !items.iter().any(|item| item.eq_ignore_ascii_case(value)) {
        items.push(value.to_string());
    }
}

pub async fn import_tag_translations(state: Arc<AppState>) -> Result<()> {
    let value: Value = state
        .http
        .get(&state.config.ehtt_url)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let data = value.get("data").and_then(Value::as_array).ok_or_else(|| {
        AppError::BadRequest("EhTagTranslation payload missing data[]".to_string())
    })?;

    for namespace_entry in data {
        let namespace = namespace_entry
            .get("namespace")
            .and_then(Value::as_str)
            .unwrap_or("other");
        let Some(tags) = namespace_entry.get("data").and_then(Value::as_object) else {
            continue;
        };
        for (key, item) in tags {
            let raw_name = item.get("name").and_then(Value::as_str).unwrap_or(key);
            let label = strip_html(raw_name);
            let intro = item.get("intro").and_then(Value::as_str).map(strip_html);
            let links = item.get("links").and_then(Value::as_str).map(strip_html);
            state
                .db
                .upsert_tag(
                    namespace,
                    key,
                    key,
                    Some(&label),
                    translated_namespace(namespace),
                    "EhTagTranslation",
                    intro.as_deref(),
                    links.as_deref(),
                )
                .await?;
        }
    }
    Ok(())
}

async fn generate_image_asset(state: Arc<AppState>, job_id: i64, payload: Value) -> Result<()> {
    let api_key = state
        .config
        .openai_api_key
        .clone()
        .ok_or_else(|| AppError::BadRequest("OPENAI_API_KEY is not configured".to_string()))?;
    let prompt = payload
        .get("prompt")
        .and_then(Value::as_str)
        .ok_or_else(|| AppError::BadRequest("prompt is required".to_string()))?;
    let style = payload
        .get("style")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string);
    let variant = style.clone().unwrap_or_else(|| "ui".to_string());
    let sanitized_asset_id = payload.get("sanitized_asset_id").and_then(Value::as_i64);
    let model = state.config.openai_image_model.clone();

    let response: Value = state
        .http
        .post("https://api.openai.com/v1/images/generations")
        .bearer_auth(api_key)
        .json(&json!({
            "model": model,
            "prompt": prompt,
            "size": "1536x1024",
            "quality": "auto",
            "output_format": "png"
        }))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;

    let b64 = response
        .get("data")
        .and_then(Value::as_array)
        .and_then(|items| items.first())
        .and_then(|item| item.get("b64_json"))
        .and_then(Value::as_str)
        .ok_or_else(|| {
            AppError::Other("OpenAI image response did not contain b64_json".to_string())
        })?;
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(b64)
        .map_err(|e| AppError::Other(format!("image decode failed: {e}")))?;
    let size = bytes.len() as i64;
    let filename = format!("generated-{}.png", uuid::Uuid::new_v4());
    let path = state.config.generated_dir.join(filename);
    tokio::fs::write(&path, &bytes).await?;

    let path_string = path.to_string_lossy().to_string();
    let work_id = state.db.generated_assets_work().await?;
    let asset_id = state
        .db
        .upsert_asset(
            work_id,
            &path_string,
            "image/png",
            "generated",
            Some(&variant),
            None,
            Some(size),
            json!({
                "job_id": job_id,
                "prompt": prompt,
                "style": style,
                "model": model.clone(),
                "sanitized_asset_id": sanitized_asset_id,
                "filename": path.file_name().and_then(|name| name.to_str()),
            }),
        )
        .await?;
    state.db.set_work_cover(work_id, asset_id).await?;
    link_tag(
        &state,
        work_id,
        "asset",
        "generated-ui",
        "Generated UI",
        "local",
    )
    .await?;
    link_tag(
        &state,
        work_id,
        "source",
        &normalize_key(&model),
        &model,
        "OpenAI",
    )
    .await?;
    state
        .db
        .audit(
            "assets.generate",
            "done",
            json!({
                "job_id": job_id,
                "work_id": work_id,
                "asset_id": asset_id,
                "path": path_string,
                "size": size,
                "model": model
            }),
        )
        .await?;
    Ok(())
}

fn strip_html(value: &str) -> String {
    let tag_re = Regex::new(r"<[^>]+>").unwrap();
    html_escape::decode_html_entities(tag_re.replace_all(value, "").trim()).to_string()
}

fn translated_namespace(namespace: &str) -> Option<&'static str> {
    match namespace {
        "rows" => Some("命名空间"),
        "reclass" => Some("重分类"),
        "language" => Some("语言"),
        "parody" => Some("原作"),
        "character" => Some("角色"),
        "group" => Some("团队"),
        "artist" => Some("艺术家"),
        "cosplayer" => Some("Coser"),
        "male" => Some("男性"),
        "female" => Some("女性"),
        "mixed" => Some("混合"),
        "other" => Some("其他"),
        "location" => Some("地点"),
        _ => None,
    }
}

async fn fetch_optional_json(http: &reqwest::Client, url: &str) -> Option<Value> {
    let response = http.get(url).send().await.ok()?.error_for_status().ok()?;
    response.json::<Value>().await.ok()
}

async fn link_tag(
    state: &AppState,
    work_id: i64,
    namespace: &str,
    key: &str,
    label: &str,
    source: &str,
) -> Result<()> {
    let tag_id = state
        .db
        .upsert_tag(namespace, key, label, None, None, source, None, None)
        .await?;
    state.db.link_tag(work_id, tag_id).await
}

fn string_field(value: &Value, names: &[&str]) -> Option<String> {
    names
        .iter()
        .find_map(|name| value.get(*name).and_then(Value::as_str))
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
}

fn number_field(value: &Value, names: &[&str]) -> Option<f64> {
    names.iter().find_map(|name| {
        value.get(*name).and_then(|item| {
            item.as_f64()
                .or_else(|| item.as_str().and_then(|text| text.parse().ok()))
        })
    })
}

fn collect_named_items(value: Option<&Value>) -> Vec<String> {
    value
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(|item| {
                    item.as_str()
                        .map(str::to_string)
                        .or_else(|| string_field(item, &["name", "i18n", "text", "label"]))
                })
                .filter(|item| !item.trim().is_empty())
                .collect()
        })
        .unwrap_or_default()
}

fn normalize_key(value: &str) -> String {
    value
        .trim()
        .to_ascii_lowercase()
        .replace('_', " ")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}
