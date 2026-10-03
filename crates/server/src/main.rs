mod archive;
mod assets;
mod atomic_file;
mod catalog;
mod catalog_reconciliation;
mod catalog_writer;
mod config;
mod db;
mod derivative;
mod enrich;
mod error;
mod inventory;
mod jobs;
mod migrations;
mod models;
mod resource;
mod routes;
mod scanner;
mod search;
mod security;
mod settings;
mod strm;
mod vfs;
mod watcher;

extern crate self as sqlx;

pub use sqlx_core::error::Error;
pub use sqlx_core::from_row::FromRow;
pub use sqlx_core::pool::Pool;
pub use sqlx_core::query::query;
pub use sqlx_core::query_as::query_as;
pub use sqlx_core::query_scalar::query_scalar;
pub use sqlx_core::row::Row;
pub use sqlx_sqlite::Sqlite;

pub mod sqlite {
    pub use sqlx_sqlite::{
        SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteRow, SqliteSynchronous,
    };
}

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::http::{header, Extensions, HeaderMap, StatusCode, Version};
use axum::Router;
use config::Config;
use db::Db;
use resource::{ResourceGovernor, ResourceLimits};
use tower_http::compression::predicate::{DefaultPredicate, NotForContentType, Predicate};
use tower_http::compression::CompressionLayer;
use tower_http::services::ServeDir;
use tower_http::trace::TraceLayer;

#[derive(Clone)]
pub struct AppState {
    pub config: Config,
    pub db: Db,
    pub http: reqwest::Client,
    pub resources: ResourceGovernor,
    pub derivatives: derivative::DerivativeCache,
    pub catalog_runtime: catalog::CatalogRuntime,
    pub search_runtime: search::SearchRuntime,
    pub comic_page_cache: Arc<assets::ComicPageCache>,
}

fn response_has_no_byte_ranges(
    _status: StatusCode,
    _version: Version,
    headers: &HeaderMap,
    _extensions: &Extensions,
) -> bool {
    !headers.contains_key(header::ACCEPT_RANGES)
}

fn media_compression_predicate() -> impl Predicate {
    DefaultPredicate::new()
        .and(NotForContentType::const_new("audio/"))
        .and(NotForContentType::const_new("video/"))
        .and(NotForContentType::const_new("application/zip"))
        .and(NotForContentType::const_new("application/x-zip-compressed"))
        .and(NotForContentType::const_new("application/epub+zip"))
        .and(NotForContentType::const_new(
            "application/vnd.comicbook+zip",
        ))
        .and(response_has_no_byte_ranges)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let config = Config::from_env()?;
    let resource_limits = ResourceLimits::from_env()?;
    tracing::info!(
        profile = %resource_limits.profile,
        processing_memory_bytes = resource_limits.processing_memory_bytes,
        inflight_media_bytes = resource_limits.inflight_media_bytes,
        memory_soft_limit_bytes = resource_limits.memory_soft_limit_bytes,
        memory_resume_limit_bytes = resource_limits.memory_resume_limit_bytes,
        archive_manifest_cache_bytes = resource_limits.archive_manifest_cache_bytes,
        interactive_wait_timeout_millis = resource_limits.interactive_wait_timeout_millis,
        "configured resource profile"
    );
    let resources = ResourceGovernor::new(resource_limits);
    let _resource_memory_monitor = resources.spawn_memory_monitor();
    tokio::fs::create_dir_all(&config.data_dir).await?;
    tokio::fs::create_dir_all(&config.generated_dir).await?;

    // Claim the configured listener before touching persisted job/lease state.
    // A second process using the same data directory and bind address must fail
    // here instead of running startup recovery and deleting the active
    // process's scanner lease.
    let addr: SocketAddr = config.bind.parse()?;
    let listener = tokio::net::TcpListener::bind(addr).await?;

    let db = Db::connect(&config.database_url).await?;
    db.migrate().await?;
    let sqlite_runtime = db.runtime_snapshot().await;
    tracing::info!(
        profile = %sqlite_runtime.config.profile,
        max_connections = sqlite_runtime.config.max_connections,
        cache_kib_per_connection = sqlite_runtime.config.cache_kib_per_connection,
        mmap_size_bytes = sqlite_runtime.config.mmap_size_bytes,
        busy_timeout_millis = sqlite_runtime.config.busy_timeout_millis,
        wal_autocheckpoint_pages = sqlite_runtime.config.wal_autocheckpoint_pages,
        journal_size_limit_bytes = sqlite_runtime.config.journal_size_limit_bytes,
        writer_queue_max_depth = sqlite_runtime.config.writer_queue_max_depth,
        writer_queue_max_bytes = sqlite_runtime.config.writer_queue_max_bytes,
        "configured SQLite runtime"
    );
    let derivatives = derivative::DerivativeCache::new(
        db.clone(),
        config.derivative_cache_v2_enabled,
        config.derivative_cache_dir.clone(),
        config.derivative_cache_max_bytes,
        config.derivative_cache_low_watermark_bytes,
    )?;
    derivatives.recover_startup().await?;
    let _derivative_eviction_worker = derivatives.spawn_eviction_worker(resources.clone());
    let recovered_jobs = db.requeue_interrupted_running_jobs().await?;
    if recovered_jobs > 0 {
        tracing::warn!(
            count = recovered_jobs,
            "requeued interrupted jobs from previous process"
        );
    }
    let recovered_inventory = inventory::recover_interrupted_coordinator(&db).await?;
    if recovered_inventory > 0 {
        tracing::warn!(
            count = recovered_inventory,
            "returned interrupted inventory events to the durable queue"
        );
    }

    let http = reqwest::Client::builder()
        .user_agent("LocalMediaShelf/0.1 (+private local deployment)")
        .cookie_store(true)
        .connect_timeout(Duration::from_secs(10))
        .read_timeout(Duration::from_secs(60))
        .redirect(reqwest::redirect::Policy::limited(5))
        .build()?;

    let state = Arc::new(AppState {
        config: config.clone(),
        db,
        http,
        resources,
        derivatives,
        catalog_runtime: catalog::CatalogRuntime::default(),
        search_runtime: search::SearchRuntime::default(),
        comic_page_cache: Arc::new(Default::default()),
    });
    if !state.config.search_incremental_reader_enabled {
        let prewarm_started = std::time::Instant::now();
        let rebuilt = search::prewarm_production_index(state.clone()).await?;
        tracing::info!(
            rebuilt,
            elapsed_millis = prewarm_started.elapsed().as_millis(),
            "production search index prewarm completed before readiness"
        );
        if state.config.search_reader_prewarm_enabled {
            let reader_started = std::time::Instant::now();
            match tokio::time::timeout(
                Duration::from_secs(30),
                search::prewarm_production_reader(state.clone()),
            )
            .await
            {
                Ok(Ok(())) => tracing::info!(
                    elapsed_millis = reader_started.elapsed().as_millis(),
                    "production search reader prewarm completed before readiness"
                ),
                Ok(Err(error)) => tracing::warn!(
                    error = %error,
                    elapsed_millis = reader_started.elapsed().as_millis(),
                    "production search reader prewarm failed; keeping lazy reader path"
                ),
                Err(_) => tracing::warn!(
                    elapsed_millis = reader_started.elapsed().as_millis(),
                    "production search reader prewarm timed out; keeping lazy reader path"
                ),
            }
        }
    }
    let _catalog_stats_backfill =
        catalog::spawn_stats_backfill(state.db.clone(), state.resources.clone());
    let _shadow_search_worker = search::outbox::spawn_shadow_worker(state.clone());
    let _shadow_search_reconciliation_worker =
        search::spawn_shadow_reconciliation_worker(state.clone());
    if state.config.search_incremental_reader_enabled {
        if state.config.search_reader_prewarm_enabled {
            let reader_started = std::time::Instant::now();
            match tokio::time::timeout(
                Duration::from_secs(30),
                search::prewarm_incremental_reader(state.clone()),
            )
            .await
            {
                Ok(Ok(())) => tracing::info!(
                    elapsed_millis = reader_started.elapsed().as_millis(),
                    "incremental search reader gate and prewarm completed before readiness"
                ),
                Ok(Err(error)) => tracing::warn!(
                    error = %error,
                    elapsed_millis = reader_started.elapsed().as_millis(),
                    "incremental search reader gate/prewarm failed; keeping fail-closed lazy path"
                ),
                Err(_) => tracing::warn!(
                    elapsed_millis = reader_started.elapsed().as_millis(),
                    "incremental search reader prewarm timed out; keeping fail-closed lazy path"
                ),
            }
        } else {
            match search::arm_incremental_reader(&state).await {
                Ok(()) => tracing::info!("incremental search reader gate is ready"),
                Err(error) => tracing::warn!(
                    error = %error,
                    "incremental search reader is fail-closed until the shadow worker catches up"
                ),
            }
        }
    }
    jobs::spawn_recovery_worker(state.clone());
    watcher::spawn_library_watcher(state.clone());

    let api = routes::router(state.clone())
        .layer(CompressionLayer::new().compress_when(media_compression_predicate()));
    let static_dir = std::env::var("STATIC_DIR").unwrap_or_else(|_| "frontend/dist".to_string());
    let static_files = Router::new()
        .fallback_service(ServeDir::new(static_dir).append_index_html_on_directories(true))
        .layer(CompressionLayer::new());
    let app = Router::new()
        .nest("/api", api)
        .merge(static_files)
        .layer(TraceLayer::new_for_http());

    tracing::info!("serving on http://{}", addr);

    axum::serve(listener, app).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::response::Response;

    fn response(content_type: &str) -> Response<Body> {
        Response::builder()
            .header(header::CONTENT_TYPE, content_type)
            .body(Body::from(vec![0_u8; 64]))
            .unwrap()
    }

    #[test]
    fn media_compression_skips_precompressed_and_range_responses() {
        let predicate = media_compression_predicate();
        assert!(!predicate.should_compress(&response("audio/mpeg")));
        assert!(!predicate.should_compress(&response("video/mp4")));
        assert!(!predicate.should_compress(&response("application/zip")));
        assert!(!predicate.should_compress(&response("application/epub+zip")));

        let ranged = Response::builder()
            .header(header::CONTENT_TYPE, "application/json")
            .header(header::ACCEPT_RANGES, "bytes")
            .body(Body::from(vec![0_u8; 64]))
            .unwrap();
        assert!(!predicate.should_compress(&ranged));
        assert!(predicate.should_compress(&response("application/json")));
    }
}
