mod assets;
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

use std::net::SocketAddr;
use std::sync::Arc;

use axum::Router;
use config::Config;
use db::Db;
use tower_http::cors::CorsLayer;
use tower_http::services::ServeDir;
use tower_http::trace::TraceLayer;

#[derive(Clone)]
pub struct AppState {
    pub config: Config,
    pub db: Db,
    pub http: reqwest::Client,
    pub comic_page_cache: Arc<assets::ComicPageCache>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let config = Config::from_env()?;
    tokio::fs::create_dir_all(&config.data_dir).await?;
    tokio::fs::create_dir_all(&config.generated_dir).await?;

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
        .build()?;

    let state = Arc::new(AppState {
        config: config.clone(),
        db,
        http,
        comic_page_cache: Arc::new(Default::default()),
    });
    jobs::spawn_recovery_worker(state.clone());
    if config.enable_file_watcher {
        watcher::spawn_library_watcher(state.clone());
    }

    let api = routes::router(state.clone());
    let static_dir = std::env::var("STATIC_DIR").unwrap_or_else(|_| "frontend/dist".to_string());
    let app = Router::new()
        .nest("/api", api)
        .fallback_service(ServeDir::new(static_dir).append_index_html_on_directories(true))
        .layer(CorsLayer::permissive())
        .layer(TraceLayer::new_for_http());

    let addr: SocketAddr = config.bind.parse()?;
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!("serving on http://{}", addr);

    axum::serve(listener, app).await?;
    Ok(())
}
