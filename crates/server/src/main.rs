mod archive;
mod assets;
mod atomic_file;
mod auth;
mod config;
mod db;
mod enrich;
mod error;
mod jobs;
mod models;
mod routes;
mod scanner;
mod search;
mod security;
mod settings;
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
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Duration;

use axum::http::{header, Extensions, HeaderMap, StatusCode, Version};
use axum::Router;
use config::Config;
use db::Db;
use tower_http::compression::predicate::{DefaultPredicate, NotForContentType, Predicate};
use tower_http::compression::CompressionLayer;
use tower_http::services::ServeDir;
use tower_http::trace::TraceLayer;

#[derive(Clone)]
pub struct AppState {
    pub config: Config,
    pub db: Db,
    pub http: reqwest::Client,
    pub comic_page_cache: Arc<assets::ComicPageCache>,
    pub auth_epoch: Arc<tokio::sync::RwLock<String>>,
    pub admin_password_persisted: Arc<AtomicBool>,
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
    if config.admin_password_ephemeral {
        tracing::warn!(
            password = %config.app_admin_password,
            "generated an ephemeral admin password for loopback-only access; set APP_ADMIN_PASSWORD or change it in the application to persist a password"
        );
    }
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
    let recovered_jobs = db.requeue_interrupted_running_jobs().await?;
    if recovered_jobs > 0 {
        tracing::warn!(
            count = recovered_jobs,
            "requeued interrupted jobs from previous process"
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
        comic_page_cache: Arc::new(Default::default()),
        auth_epoch: Arc::new(tokio::sync::RwLock::new(uuid::Uuid::new_v4().to_string())),
        admin_password_persisted: Arc::new(AtomicBool::new(config.admin_password_persisted)),
    });
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
