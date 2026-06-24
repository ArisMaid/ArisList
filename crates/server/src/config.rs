use std::env;
use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct Config {
    pub bind: String,
    pub database_url: String,
    pub data_dir: PathBuf,
    pub comics_dir: PathBuf,
    pub novels_dir: PathBuf,
    pub audio_dir: PathBuf,
    pub gallery_dir: PathBuf,
    pub generated_dir: PathBuf,
    pub app_admin_password: String,
    pub lightnovel_api_bases: Vec<String>,
    pub lightnovel_access_token: Option<String>,
    pub enrichment_concurrency: usize,
    pub ehtt_url: String,
    pub openai_api_key: Option<String>,
    pub openai_image_model: String,
    pub qmediasync_base_url: String,
    pub session_secret: String,
    pub enable_file_watcher: bool,
    pub watch_debounce_seconds: u64,
}

impl Config {
    pub fn from_env() -> anyhow::Result<Self> {
        let database_url =
            env::var("DATABASE_URL").unwrap_or_else(|_| "sqlite://data/library.sqlite".to_string());
        let data_dir = env::var("DATA_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from("data"));

        Ok(Self {
            bind: env::var("APP_BIND").unwrap_or_else(|_| "127.0.0.1:8787".to_string()),
            database_url,
            data_dir,
            comics_dir: env::var("COMICS_DIR")
                .map(PathBuf::from)
                .unwrap_or_else(|_| PathBuf::from("漫画")),
            novels_dir: env::var("NOVELS_DIR")
                .map(PathBuf::from)
                .unwrap_or_else(|_| PathBuf::from("轻小说")),
            audio_dir: env::var("AUDIO_DIR")
                .map(PathBuf::from)
                .unwrap_or_else(|_| PathBuf::from("音声")),
            gallery_dir: env::var("GALLERY_DIR")
                .map(PathBuf::from)
                .unwrap_or_else(|_| PathBuf::from("图库")),
            generated_dir: env::var("GENERATED_DIR")
                .map(PathBuf::from)
                .unwrap_or_else(|_| PathBuf::from("generated")),
            app_admin_password: env::var("APP_ADMIN_PASSWORD")
                .unwrap_or_else(|_| "admin".to_string()),
            lightnovel_api_bases: env::var("LIGHTNOVEL_API_BASES")
                .unwrap_or_else(|_| {
                    "https://api.lightnovel.life,https://cf-api.lightnovel.life".to_string()
                })
                .split(',')
                .map(str::trim)
                .filter(|base| !base.is_empty())
                .map(|base| base.trim_end_matches('/').to_string())
                .collect(),
            lightnovel_access_token: env::var("LIGHTNOVEL_ACCESS_TOKEN")
                .ok()
                .filter(|v| !v.trim().is_empty()),
            enrichment_concurrency: env::var("ENRICHMENT_CONCURRENCY")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(1),
            ehtt_url: env::var("EHTT_URL").unwrap_or_else(|_| {
                "https://fastly.jsdelivr.net/gh/EhTagTranslation/DatabaseReleases/db.html.json"
                    .to_string()
            }),
            openai_api_key: env::var("OPENAI_API_KEY")
                .ok()
                .filter(|v| !v.trim().is_empty()),
            openai_image_model: env::var("OPENAI_IMAGE_MODEL")
                .unwrap_or_else(|_| "gpt-image-2".to_string()),
            qmediasync_base_url: env::var("QMEDIASYNC_BASE_URL").unwrap_or_default(),
            session_secret: env::var("SESSION_SECRET")
                .unwrap_or_else(|_| "dev-only-change-me".to_string()),
            enable_file_watcher: env::var("ENABLE_FILE_WATCHER")
                .map(|value| {
                    matches!(
                        value.to_ascii_lowercase().as_str(),
                        "1" | "true" | "yes" | "on"
                    )
                })
                .unwrap_or(false),
            watch_debounce_seconds: env::var("WATCH_DEBOUNCE_SECONDS")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(20),
        })
    }
}
