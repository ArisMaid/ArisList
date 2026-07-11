use std::path::PathBuf;

use axum::extract::State;
use axum::http::HeaderMap;
use axum::Json;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::sync::Arc;

use crate::auth;
use crate::config::Config;
use crate::error::{AppError, Result};
use crate::AppState;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ThemeMode {
    Light,
    Dark,
}

impl Default for ThemeMode {
    fn default() -> Self {
        Self::Light
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct MediaDirectorySettings {
    pub comics: Vec<String>,
    pub novels: Vec<String>,
    pub audio: Vec<String>,
    #[serde(default)]
    pub gallery: Vec<String>,
    #[serde(default)]
    pub coser_picture: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MediaSourceSettings {
    pub kind: String,
    pub provider: String,
    pub root: String,
    pub mount_name: String,
    pub enabled: bool,
    #[serde(default = "default_scan_depth")]
    pub scan_depth: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CoverCacheDirectorySettings {
    #[serde(default)]
    pub comic: String,
    #[serde(default)]
    pub novel: String,
    #[serde(default)]
    pub audio: String,
    #[serde(default)]
    pub gallery: String,
    #[serde(default)]
    pub coser_picture: String,
}

impl CoverCacheDirectorySettings {
    pub fn defaults(config: &Config) -> Self {
        Self {
            comic: path_string(&config.comic_cover_cache_dir),
            novel: path_string(&config.novel_cover_cache_dir),
            audio: path_string(&config.audio_cover_cache_dir),
            gallery: path_string(&config.gallery_cover_cache_dir),
            coser_picture: path_string(&config.coser_picture_cover_cache_dir),
        }
    }

    fn normalized(mut self, config: &Config) -> Self {
        let defaults = Self::defaults(config);
        self.comic = normalize_cache_dir(self.comic, defaults.comic);
        self.novel = normalize_cache_dir(self.novel, defaults.novel);
        self.audio = normalize_cache_dir(self.audio, defaults.audio);
        self.gallery = normalize_cache_dir(self.gallery, defaults.gallery);
        self.coser_picture = normalize_cache_dir(self.coser_picture, defaults.coser_picture);
        self
    }

    pub fn for_work_kind(&self, kind: &str) -> PathBuf {
        match kind {
            "comic" => PathBuf::from(&self.comic),
            "novel" => PathBuf::from(&self.novel),
            "audio" => PathBuf::from(&self.audio),
            "gallery" => PathBuf::from(&self.gallery),
            "coser-picture" => PathBuf::from(&self.coser_picture),
            _ => PathBuf::from(&self.gallery),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QMediaSyncSettings {
    pub enabled: bool,
    pub base_url: String,
    #[serde(default)]
    pub strm_roots: Vec<String>,
}

impl Default for QMediaSyncSettings {
    fn default() -> Self {
        Self {
            enabled: false,
            base_url: String::new(),
            strm_roots: Vec::new(),
        }
    }
}

fn default_scan_depth() -> usize {
    12
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScanSettings {
    pub enqueue_enrichment: bool,
    pub file_watcher: bool,
    pub enrichment_concurrency: usize,
}

impl Default for ScanSettings {
    fn default() -> Self {
        Self {
            enqueue_enrichment: false,
            file_watcher: false,
            enrichment_concurrency: 1,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpenAiSettings {
    pub image_model: String,
    pub image_configured: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum UiMaterial {
    Classic,
    Liquid,
}

impl Default for UiMaterial {
    fn default() -> Self {
        Self::Liquid
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum GlassIntensity {
    Clear,
    Standard,
    Readable,
}

impl Default for GlassIntensity {
    fn default() -> Self {
        Self::Standard
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct AppearanceSettings {
    #[serde(default)]
    pub material: UiMaterial,
    #[serde(default)]
    pub glass_intensity: GlassIntensity,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReaderSettings {
    #[serde(default = "default_comic_auto_read_interval_ms")]
    pub comic_auto_read_interval_ms: u64,
}

impl Default for ReaderSettings {
    fn default() -> Self {
        Self {
            comic_auto_read_interval_ms: default_comic_auto_read_interval_ms(),
        }
    }
}

fn default_comic_auto_read_interval_ms() -> u64 {
    4000
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum DetailPaneMode {
    Modal,
    Docked,
}

impl Default for DetailPaneMode {
    fn default() -> Self {
        Self::Modal
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppSettings {
    pub theme: ThemeMode,
    #[serde(default)]
    pub detail_mode: DetailPaneMode,
    #[serde(default)]
    pub appearance: AppearanceSettings,
    #[serde(default)]
    pub reader: ReaderSettings,
    pub media_dirs: MediaDirectorySettings,
    #[serde(default)]
    pub cover_cache_dirs: CoverCacheDirectorySettings,
    #[serde(default)]
    pub media_sources: Vec<MediaSourceSettings>,
    #[serde(default)]
    pub qmediasync: QMediaSyncSettings,
    pub scan: ScanSettings,
    pub openai: OpenAiSettings,
}

impl AppSettings {
    pub fn defaults(config: &Config) -> Self {
        Self {
            theme: ThemeMode::Light,
            detail_mode: DetailPaneMode::Modal,
            appearance: AppearanceSettings::default(),
            reader: ReaderSettings::default(),
            media_dirs: MediaDirectorySettings {
                comics: vec![path_string(&config.comics_dir)],
                novels: vec![path_string(&config.novels_dir)],
                audio: vec![path_string(&config.audio_dir)],
                gallery: vec![path_string(&config.gallery_dir)],
                coser_picture: vec![path_string(&config.coser_picture_dir)],
            },
            cover_cache_dirs: CoverCacheDirectorySettings::defaults(config),
            media_sources: Vec::new(),
            qmediasync: QMediaSyncSettings {
                enabled: false,
                base_url: config.qmediasync_base_url.clone(),
                strm_roots: Vec::new(),
            },
            scan: ScanSettings {
                enqueue_enrichment: false,
                file_watcher: config.enable_file_watcher,
                enrichment_concurrency: config.enrichment_concurrency.clamp(1, 8),
            },
            openai: OpenAiSettings {
                image_model: config.openai_image_model.clone(),
                image_configured: config.openai_api_key.is_some(),
            },
        }
    }

    pub fn comic_roots(&self) -> Vec<PathBuf> {
        roots_from_strings(&self.media_dirs.comics)
    }

    pub fn novel_roots(&self) -> Vec<PathBuf> {
        roots_from_strings(&self.media_dirs.novels)
    }

    pub fn audio_roots(&self) -> Vec<PathBuf> {
        roots_from_strings(&self.media_dirs.audio)
    }

    pub fn gallery_roots(&self) -> Vec<PathBuf> {
        roots_from_strings(&self.media_dirs.gallery)
    }

    pub fn coser_picture_roots(&self) -> Vec<PathBuf> {
        roots_from_strings(&self.media_dirs.coser_picture)
    }

    pub fn all_media_roots(&self) -> Vec<PathBuf> {
        let mut roots = Vec::new();
        roots.extend(self.comic_roots());
        roots.extend(self.novel_roots());
        roots.extend(self.audio_roots());
        roots.extend(self.gallery_roots());
        roots.extend(self.coser_picture_roots());
        roots.extend(self.qmediasync_roots());
        roots
    }

    pub fn qmediasync_roots(&self) -> Vec<PathBuf> {
        let mut roots = roots_from_strings(&self.qmediasync.strm_roots);
        for source in &self.media_sources {
            if source.enabled && source.provider == "qmediasync" {
                let root = PathBuf::from(source.root.trim());
                if !roots.iter().any(|existing| existing == &root) {
                    roots.push(root);
                }
            }
        }
        roots
    }

    fn normalized(mut self, config: &Config) -> Self {
        self.media_dirs.comics = normalize_dirs(self.media_dirs.comics);
        self.media_dirs.novels = normalize_dirs(self.media_dirs.novels);
        self.media_dirs.audio = normalize_dirs(self.media_dirs.audio);
        self.media_dirs.gallery = normalize_dirs(self.media_dirs.gallery);
        self.media_dirs.coser_picture = normalize_dirs(self.media_dirs.coser_picture);
        self.cover_cache_dirs = self.cover_cache_dirs.normalized(config);
        self.media_sources = normalize_sources(self.media_sources);
        self.qmediasync.strm_roots = normalize_dirs(self.qmediasync.strm_roots);
        self.reader.comic_auto_read_interval_ms =
            self.reader.comic_auto_read_interval_ms.clamp(500, 120_000);
        if self.qmediasync.base_url.trim().is_empty() {
            self.qmediasync.base_url = config.qmediasync_base_url.clone();
        } else {
            self.qmediasync.base_url = self
                .qmediasync
                .base_url
                .trim()
                .trim_end_matches('/')
                .to_string();
        }
        self.scan.enqueue_enrichment = false;
        self.scan.enrichment_concurrency = self.scan.enrichment_concurrency.clamp(1, 8);
        self.openai.image_model = config.openai_image_model.clone();
        self.openai.image_configured = config.openai_api_key.is_some();
        self
    }
}

pub async fn get_settings(State(state): State<Arc<AppState>>) -> Result<Json<AppSettings>> {
    Ok(Json(public_settings(load_settings(&state.config).await?)))
}

pub async fn update_settings(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(input): Json<AppSettings>,
) -> Result<Json<AppSettings>> {
    auth::require_csrf(&state, &headers, "settings.update").await?;
    let settings = input.normalized(&state.config);
    save_settings(&state.config, &settings).await?;
    state
        .db
        .audit(
            "settings.update",
            "saved",
            json!({
                "comics": settings.media_dirs.comics.len(),
                "novels": settings.media_dirs.novels.len(),
                "audio": settings.media_dirs.audio.len(),
                "gallery": settings.media_dirs.gallery.len(),
                "coser_picture": settings.media_dirs.coser_picture.len(),
                "cover_cache_dirs": 5,
                "media_sources": settings.media_sources.len(),
                "qmediasync_enabled": settings.qmediasync.enabled,
                "qmediasync_roots": settings.qmediasync.strm_roots.len(),
                "theme": &settings.theme,
                "appearance_material": &settings.appearance.material,
                "glass_intensity": &settings.appearance.glass_intensity,
                "comic_auto_read_interval_ms": settings.reader.comic_auto_read_interval_ms,
            }),
        )
        .await?;
    Ok(Json(public_settings(settings)))
}

pub async fn load_settings(config: &Config) -> Result<AppSettings> {
    let path = settings_path(config);
    if !path.exists() {
        return Ok(AppSettings::defaults(config));
    }
    let raw = tokio::fs::read_to_string(&path).await?;
    let raw_value = serde_json::from_str::<Value>(&raw)
        .map_err(|err| AppError::Other(format!("settings file is invalid: {err}")))?;
    let missing_coser_picture = raw_value
        .get("media_dirs")
        .and_then(|value| value.get("coser_picture"))
        .is_none();
    let mut settings = serde_json::from_value::<AppSettings>(raw_value)
        .map_err(|err| AppError::Other(format!("settings file is invalid: {err}")))?;
    if missing_coser_picture {
        settings.media_dirs.coser_picture = vec![path_string(&config.coser_picture_dir)];
    }
    Ok(settings.normalized(config))
}

async fn save_settings(config: &Config, settings: &AppSettings) -> Result<()> {
    tokio::fs::create_dir_all(&config.data_dir).await?;
    let raw = serde_json::to_string_pretty(settings)
        .map_err(|err| AppError::Other(format!("settings serialization failed: {err}")))?;
    tokio::fs::write(settings_path(config), raw).await?;
    Ok(())
}

fn settings_path(config: &Config) -> PathBuf {
    config.data_dir.join("app-settings.json")
}

fn public_settings(settings: AppSettings) -> AppSettings {
    settings
}

fn normalize_dirs(values: Vec<String>) -> Vec<String> {
    let mut out = Vec::new();
    for value in values {
        let value = value.trim();
        if value.is_empty() || out.iter().any(|existing| existing == value) {
            continue;
        }
        out.push(value.to_string());
    }
    out
}

fn normalize_cache_dir(value: String, default: String) -> String {
    let value = value.trim();
    if value.is_empty() {
        return default;
    }
    value.trim_end_matches(['/', '\\']).to_string()
}

fn normalize_sources(values: Vec<MediaSourceSettings>) -> Vec<MediaSourceSettings> {
    let mut out: Vec<MediaSourceSettings> = Vec::new();
    for mut value in values {
        value.kind = value.kind.trim().to_ascii_lowercase();
        value.provider = value.provider.trim().to_ascii_lowercase();
        if value.provider == "openlist" {
            value.provider = "qmediasync".to_string();
        }
        value.root = normalize_cloud_root(&value.root);
        value.mount_name = value.mount_name.trim().to_string();
        value.scan_depth = value.scan_depth.clamp(1, 64);
        if !matches!(
            value.kind.as_str(),
            "comic" | "novel" | "audio" | "gallery" | "coser-picture"
        )
            || value.provider != "qmediasync"
            || value.root.is_empty()
            || value.mount_name.is_empty()
        {
            continue;
        }
        if out.iter().any(|existing| {
            existing.kind == value.kind
                && existing.provider == value.provider
                && existing.root == value.root
                && existing.mount_name == value.mount_name
        }) {
            continue;
        }
        out.push(value);
    }
    out
}

fn normalize_cloud_root(value: &str) -> String {
    let value = value.trim();
    if value.is_empty() {
        return String::new();
    }
    value.trim_end_matches(['/', '\\']).to_string()
}

fn roots_from_strings(values: &[String]) -> Vec<PathBuf> {
    values
        .iter()
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .collect()
}

fn path_string(path: &std::path::Path) -> String {
    path.to_string_lossy().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn qmediasync_settings_default_roots() {
        let settings = serde_json::from_value::<QMediaSyncSettings>(serde_json::json!({
            "enabled": true,
            "base_url": "http://qmediasync:8095"
        }))
        .unwrap();
        assert!(settings.strm_roots.is_empty());
        assert_eq!(settings.base_url, "http://qmediasync:8095");
    }

    #[test]
    fn normalizes_legacy_openlist_sources_and_coser_picture_kind() {
        let sources = normalize_sources(vec![MediaSourceSettings {
            kind: " Coser-Picture ".to_string(),
            provider: " OpenList ".to_string(),
            root: " /qms/coser/ ".to_string(),
            mount_name: "cloud".to_string(),
            enabled: true,
            scan_depth: 128,
        }]);

        assert_eq!(sources.len(), 1);
        assert_eq!(sources[0].kind, "coser-picture");
        assert_eq!(sources[0].provider, "qmediasync");
        assert_eq!(sources[0].root, "/qms/coser");
        assert_eq!(sources[0].scan_depth, 64);
    }
}
