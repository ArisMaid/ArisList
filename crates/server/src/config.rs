use std::collections::BTreeSet;
use std::env;
use std::io::Read;
use std::net::SocketAddr;
use std::path::PathBuf;

use anyhow::{bail, Context};
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use rand::RngCore;

const DEFAULT_ADMIN_PASSWORD: &str = "admin";
const DEFAULT_SESSION_SECRET: &str = "dev-only-change-me";
const MIN_ADMIN_PASSWORD_LEN: usize = 8;
const MIN_SESSION_SECRET_LEN: usize = 32;
const DEFAULT_CLOUD_CACHE_MAX_BYTES: u64 = 64 * 1024 * 1024 * 1024;
const DEFAULT_THUMBNAIL_CACHE_MAX_BYTES_PER_DIR: u64 = 8 * 1024 * 1024 * 1024;
const DEFAULT_DERIVATIVE_CACHE_MAX_BYTES: u64 = 32 * 1024 * 1024 * 1024;
const DEFAULT_DERIVATIVE_CACHE_LOW_WATERMARK_BYTES: u64 = 28 * 1024 * 1024 * 1024;
const MAX_ADMIN_PASSWORD_FILE_BYTES: u64 = 4 * 1024;

/// Media kinds supported by the bounded Inventory/Catalog v2 rollout.
///
/// The list is intentionally centralized so configuration parsing, health
/// reporting, and promotion checks cannot silently disagree about a kind.
pub const INVENTORY_KINDS: [&str; 5] = ["novel", "comic", "coser-picture", "audio", "gallery"];

#[derive(Debug, Clone)]
pub struct Config {
    pub bind: String,
    pub database_url: String,
    pub data_dir: PathBuf,
    pub cover_cache_dir: PathBuf,
    pub comic_cover_cache_dir: PathBuf,
    pub novel_cover_cache_dir: PathBuf,
    pub audio_cover_cache_dir: PathBuf,
    pub gallery_cover_cache_dir: PathBuf,
    pub coser_picture_cover_cache_dir: PathBuf,
    pub comics_dir: PathBuf,
    pub novels_dir: PathBuf,
    pub audio_dir: PathBuf,
    pub gallery_dir: PathBuf,
    pub coser_picture_dir: PathBuf,
    pub generated_dir: PathBuf,
    pub app_admin_password: String,
    pub admin_password_persisted: bool,
    pub admin_password_ephemeral: bool,
    pub lightnovel_api_bases: Vec<String>,
    pub lightnovel_access_token: Option<String>,
    pub enrichment_concurrency: usize,
    pub ehtt_url: String,
    pub openai_api_key: Option<String>,
    pub openai_image_model: String,
    pub qmediasync_base_url: String,
    pub cloud_cache_max_bytes: u64,
    pub thumbnail_cache_max_bytes_per_dir: u64,
    pub catalog_v2_enabled: bool,
    pub facet_bitmap_enabled: bool,
    pub inventory_scanner_enabled: bool,
    /// Subset of media kinds allowed to use the shadow/incremental Inventory
    /// coordinator.  An empty environment value means all kinds for backward
    /// compatibility; the parsed field is always explicit and non-empty when
    /// Inventory is enabled.
    pub inventory_scanner_kinds: BTreeSet<String>,
    pub search_outbox_shadow_enabled: bool,
    pub search_shadow_canary_enabled: bool,
    pub search_incremental_reader_enabled: bool,
    /// Open the production Tantivy reader before readiness so the first
    /// search-backed Catalog request does not pay reader mmap/segment setup.
    /// This remains opt-in until the target N100 RSS/startup Gate is passed.
    pub search_reader_prewarm_enabled: bool,
    pub derivative_cache_v2_enabled: bool,
    /// Use JPEG's native DCT scaling for generated thumbnails when enabled.
    /// This remains an explicit rollout flag until the target NAS Gate has
    /// compared latency, RSS, and image compatibility against the image crate.
    pub jpeg_thumbnail_downscale_enabled: bool,
    pub derivative_cache_dir: PathBuf,
    pub derivative_cache_max_bytes: u64,
    pub derivative_cache_low_watermark_bytes: u64,
    pub session_secret: String,
    pub enable_file_watcher: bool,
    pub watch_debounce_seconds: u64,
}

impl Config {
    pub fn from_env() -> anyhow::Result<Self> {
        let bind = env_non_empty("APP_BIND").unwrap_or_else(|| "127.0.0.1:8787".to_string());
        let bind_addr: SocketAddr = bind
            .parse()
            .with_context(|| format!("APP_BIND must be a socket address, got {bind:?}"))?;
        let loopback_bind = bind_addr.ip().is_loopback();
        let database_url =
            env::var("DATABASE_URL").unwrap_or_else(|_| "sqlite://data/library.sqlite".to_string());
        let data_dir = env::var("DATA_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from("data"));
        let cover_cache_dir = env_path_or("COVER_CACHE_DIR", data_dir.join("cover-cache"));
        let derivative_cache_dir =
            env_path_or("DERIVATIVE_CACHE_DIR", data_dir.join("derivatives"));
        let derivative_cache_max_bytes = env_positive_u64(
            "DERIVATIVE_CACHE_MAX_BYTES",
            DEFAULT_DERIVATIVE_CACHE_MAX_BYTES,
        );
        let derivative_cache_low_watermark_bytes = env_positive_u64(
            "DERIVATIVE_CACHE_LOW_WATERMARK_BYTES",
            DEFAULT_DERIVATIVE_CACHE_LOW_WATERMARK_BYTES,
        );
        if derivative_cache_low_watermark_bytes >= derivative_cache_max_bytes {
            bail!(
                "DERIVATIVE_CACHE_LOW_WATERMARK_BYTES must be lower than DERIVATIVE_CACHE_MAX_BYTES"
            );
        }
        let cover_cache_child =
            |env_name: &str, child: &str| env_path_or(env_name, cover_cache_dir.join(child));

        let configured_admin_password = env_non_empty("APP_ADMIN_PASSWORD");
        let persisted_admin_password = match read_persisted_admin_password(&data_dir) {
            Ok(password) => password,
            Err(err) if loopback_bind => {
                tracing::warn!(error = %err, "ignoring an invalid persisted admin password on loopback");
                None
            }
            Err(err) => return Err(err),
        };
        let admin_password_persisted = persisted_admin_password.is_some();
        let admin_password_ephemeral =
            persisted_admin_password.is_none() && configured_admin_password.is_none();
        let app_admin_password = persisted_admin_password
            .or(configured_admin_password)
            .unwrap_or_else(|| {
                if loopback_bind {
                    generate_admin_password()
                } else {
                    DEFAULT_ADMIN_PASSWORD.to_string()
                }
            });
        if !loopback_bind && is_weak_admin_password(&app_admin_password) {
            bail!(
                "refusing non-loopback bind with a default or weak admin password; set APP_ADMIN_PASSWORD (at least {MIN_ADMIN_PASSWORD_LEN} characters) or persist a stronger password in DATA_DIR/admin-password.txt"
            );
        }
        let session_secret =
            session_secret_for_bind(bind_addr, env_non_empty("SESSION_SECRET").as_deref())?;
        let search_outbox_shadow_enabled = env_flag("SEARCH_OUTBOX_SHADOW_ENABLED", false);
        let search_shadow_canary_enabled = env_flag("SEARCH_SHADOW_CANARY_ENABLED", false);
        let search_incremental_reader_enabled =
            env_flag("SEARCH_INCREMENTAL_READER_ENABLED", false);
        let search_reader_prewarm_enabled = env_flag("SEARCH_READER_PREWARM_ENABLED", false);
        let inventory_scanner_enabled = env_flag("INVENTORY_SCANNER_ENABLED", false);
        let inventory_scanner_kinds = parse_inventory_kinds(
            env::var("INVENTORY_SCANNER_KINDS").ok(),
            inventory_scanner_enabled,
        )?;
        validate_search_flags(
            search_outbox_shadow_enabled,
            search_shadow_canary_enabled,
            search_incremental_reader_enabled,
        )?;

        Ok(Self {
            bind,
            database_url,
            data_dir,
            comic_cover_cache_dir: cover_cache_child("COMIC_COVER_CACHE_DIR", "comic"),
            novel_cover_cache_dir: cover_cache_child("NOVEL_COVER_CACHE_DIR", "novel"),
            audio_cover_cache_dir: cover_cache_child("AUDIO_COVER_CACHE_DIR", "audio"),
            gallery_cover_cache_dir: cover_cache_child("GALLERY_COVER_CACHE_DIR", "gallery"),
            coser_picture_cover_cache_dir: cover_cache_child(
                "COSER_PICTURE_COVER_CACHE_DIR",
                "coser-picture",
            ),
            cover_cache_dir,
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
            coser_picture_dir: env::var("COSER_PICTURE_DIR")
                .map(PathBuf::from)
                .unwrap_or_else(|_| PathBuf::from("COS图")),
            generated_dir: env::var("GENERATED_DIR")
                .map(PathBuf::from)
                .unwrap_or_else(|_| PathBuf::from("generated")),
            app_admin_password,
            admin_password_persisted,
            admin_password_ephemeral,
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
            cloud_cache_max_bytes: env::var("CLOUD_CACHE_MAX_BYTES")
                .ok()
                .and_then(|value| value.parse::<u64>().ok())
                .filter(|value| *value > 0)
                .unwrap_or(DEFAULT_CLOUD_CACHE_MAX_BYTES),
            thumbnail_cache_max_bytes_per_dir: env::var("THUMBNAIL_CACHE_MAX_BYTES_PER_DIR")
                .ok()
                .and_then(|value| value.parse::<u64>().ok())
                .filter(|value| *value > 0)
                .unwrap_or(DEFAULT_THUMBNAIL_CACHE_MAX_BYTES_PER_DIR),
            catalog_v2_enabled: env_flag("CATALOG_V2_ENABLED", true),
            facet_bitmap_enabled: env_flag("FACET_BITMAP_ENABLED", false),
            inventory_scanner_enabled,
            inventory_scanner_kinds,
            search_outbox_shadow_enabled,
            search_shadow_canary_enabled,
            search_incremental_reader_enabled,
            search_reader_prewarm_enabled,
            derivative_cache_v2_enabled: env_flag("DERIVATIVE_CACHE_V2_ENABLED", false),
            jpeg_thumbnail_downscale_enabled: env_flag("JPEG_THUMBNAIL_DOWNSCALE_ENABLED", false),
            derivative_cache_dir,
            derivative_cache_max_bytes,
            derivative_cache_low_watermark_bytes,
            session_secret,
            enable_file_watcher: env_flag("ENABLE_FILE_WATCHER", false),
            watch_debounce_seconds: env::var("WATCH_DEBOUNCE_SECONDS")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(20),
        })
    }

    pub fn is_loopback_bind(&self) -> bool {
        self.bind
            .parse::<SocketAddr>()
            .map(|address| address.ip().is_loopback())
            .unwrap_or(false)
    }

    /// Whether the bounded coordinator is allowed to touch one media kind.
    /// Keeping the global flag and the kind set in one helper prevents a
    /// partial rollout from accidentally stopping the legacy writer for a
    /// kind that was not selected.
    pub fn inventory_kind_enabled(&self, kind: &str) -> bool {
        self.inventory_scanner_enabled
            && (self.inventory_scanner_kinds.is_empty()
                || self
                    .inventory_scanner_kinds
                    .contains(kind.trim().to_ascii_lowercase().as_str()))
    }

    pub fn inventory_any_kind_enabled(&self) -> bool {
        self.inventory_scanner_enabled
    }

    pub fn inventory_enabled_kinds(&self) -> BTreeSet<String> {
        if !self.inventory_scanner_enabled {
            return BTreeSet::new();
        }
        if self.inventory_scanner_kinds.is_empty() {
            return INVENTORY_KINDS
                .iter()
                .map(|kind| (*kind).to_string())
                .collect();
        }
        self.inventory_scanner_kinds.clone()
    }
}

fn env_non_empty(name: &str) -> Option<String> {
    non_empty_env_value(env::var(name).ok())
}

fn non_empty_env_value(value: Option<String>) -> Option<String> {
    value
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn env_path_or(name: &str, fallback: PathBuf) -> PathBuf {
    env_non_empty(name).map(PathBuf::from).unwrap_or(fallback)
}

fn env_positive_u64(name: &str, fallback: u64) -> u64 {
    env::var(name)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(fallback)
}

fn env_flag(name: &str, fallback: bool) -> bool {
    env::var(name)
        .map(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(fallback)
}

fn parse_inventory_kinds(raw: Option<String>, enabled: bool) -> anyhow::Result<BTreeSet<String>> {
    let raw = raw.unwrap_or_default();
    let value = raw.trim();
    let mut kinds = BTreeSet::new();
    if value.is_empty() || value.eq_ignore_ascii_case("all") {
        kinds.extend(INVENTORY_KINDS.iter().map(|kind| (*kind).to_string()));
    } else {
        for item in value.split(',') {
            let kind = item.trim().to_ascii_lowercase();
            if kind.is_empty() {
                continue;
            }
            if !INVENTORY_KINDS.contains(&kind.as_str()) {
                bail!(
                    "INVENTORY_SCANNER_KINDS contains unsupported media kind {kind:?}; expected one of {}",
                    INVENTORY_KINDS.join(", ")
                );
            }
            kinds.insert(kind);
        }
        if kinds.is_empty() {
            bail!("INVENTORY_SCANNER_KINDS must contain at least one media kind");
        }
    }
    if enabled {
        Ok(kinds)
    } else {
        // Validate the value even when the global switch is off, but keep the
        // runtime set empty so status/promotion checks remain fail-closed.
        Ok(BTreeSet::new())
    }
}

fn validate_search_flags(
    shadow_enabled: bool,
    canary_enabled: bool,
    incremental_reader_enabled: bool,
) -> anyhow::Result<()> {
    if incremental_reader_enabled && !shadow_enabled {
        bail!("SEARCH_INCREMENTAL_READER_ENABLED requires SEARCH_OUTBOX_SHADOW_ENABLED");
    }
    if incremental_reader_enabled && !canary_enabled {
        bail!(
            "SEARCH_INCREMENTAL_READER_ENABLED requires SEARCH_SHADOW_CANARY_ENABLED so the persisted fact reconciliation worker can run"
        );
    }
    Ok(())
}

fn read_persisted_admin_password(data_dir: &std::path::Path) -> anyhow::Result<Option<String>> {
    let path = data_dir.join("admin-password.txt");
    let file = match std::fs::File::open(&path) {
        Ok(file) => file,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(err).with_context(|| format!("failed to open {}", path.display())),
    };
    let mut bytes = Vec::new();
    file.take(MAX_ADMIN_PASSWORD_FILE_BYTES + 1)
        .read_to_end(&mut bytes)
        .with_context(|| format!("failed to read {}", path.display()))?;
    if bytes.len() as u64 > MAX_ADMIN_PASSWORD_FILE_BYTES {
        bail!(
            "{} exceeds {MAX_ADMIN_PASSWORD_FILE_BYTES} bytes",
            path.display()
        );
    }
    let password = std::str::from_utf8(&bytes)
        .with_context(|| format!("{} is not valid UTF-8", path.display()))?
        .trim();
    if password.is_empty() {
        bail!("{} is empty", path.display());
    }
    Ok(Some(password.to_string()))
}

pub(crate) fn is_weak_admin_password(value: &str) -> bool {
    let normalized = value.trim();
    if normalized.chars().count() < MIN_ADMIN_PASSWORD_LEN {
        return true;
    }
    let mut characters = normalized.chars();
    let first = characters.next().unwrap_or_default();
    let repetitive = characters.all(|character| character.eq_ignore_ascii_case(&first));
    normalized.chars().all(|ch| ch.is_ascii_digit())
        || repetitive
        || matches!(
            normalized.to_ascii_lowercase().as_str(),
            "admin"
                | "change-me"
                | "changeme"
                | "password"
                | "password1"
                | "qwerty123"
                | "letmein123"
                | "replace-with-a-strong-password"
        )
}

fn session_secret_for_bind(bind: SocketAddr, configured: Option<&str>) -> anyhow::Result<String> {
    let loopback = bind.ip().is_loopback();
    let value = configured.map(str::trim).filter(|value| !value.is_empty());
    let placeholder = value.is_none_or(|secret| {
        let normalized = secret.to_ascii_lowercase();
        secret == DEFAULT_SESSION_SECRET
            || normalized == "change-me"
            || (normalized.starts_with("replace-with-") && normalized.contains("random"))
    });

    if placeholder {
        if loopback {
            return Ok(generate_session_secret());
        }
        bail!(
            "refusing non-loopback bind without a unique SESSION_SECRET of at least {MIN_SESSION_SECRET_LEN} bytes"
        );
    }

    let secret = value.expect("placeholder was false, so SESSION_SECRET is present");
    if secret.len() < MIN_SESSION_SECRET_LEN {
        bail!("SESSION_SECRET must be at least {MIN_SESSION_SECRET_LEN} bytes");
    }
    Ok(secret.to_string())
}

fn generate_session_secret() -> String {
    let mut bytes = [0_u8; 32];
    rand::rngs::OsRng.fill_bytes(&mut bytes);
    URL_SAFE_NO_PAD.encode(bytes)
}

fn generate_admin_password() -> String {
    let mut bytes = [0_u8; 18];
    rand::rngs::OsRng.fill_bytes(&mut bytes);
    URL_SAFE_NO_PAD.encode(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> Config {
        let root = PathBuf::from("test-data");
        Config {
            bind: "127.0.0.1:8787".to_string(),
            database_url: "sqlite::memory:".to_string(),
            data_dir: root.clone(),
            cover_cache_dir: root.join("cover-cache"),
            comic_cover_cache_dir: root.join("cover-cache/comic"),
            novel_cover_cache_dir: root.join("cover-cache/novel"),
            audio_cover_cache_dir: root.join("cover-cache/audio"),
            gallery_cover_cache_dir: root.join("cover-cache/gallery"),
            coser_picture_cover_cache_dir: root.join("cover-cache/coser-picture"),
            comics_dir: root.join("comics"),
            novels_dir: root.join("novels"),
            audio_dir: root.join("audio"),
            gallery_dir: root.join("gallery"),
            coser_picture_dir: root.join("coser-picture"),
            generated_dir: root.join("generated"),
            app_admin_password: "admin".to_string(),
            admin_password_persisted: false,
            admin_password_ephemeral: false,
            lightnovel_api_bases: Vec::new(),
            lightnovel_access_token: None,
            enrichment_concurrency: 1,
            ehtt_url: String::new(),
            openai_api_key: None,
            openai_image_model: "gpt-image-2".to_string(),
            qmediasync_base_url: String::new(),
            cloud_cache_max_bytes: DEFAULT_CLOUD_CACHE_MAX_BYTES,
            thumbnail_cache_max_bytes_per_dir: DEFAULT_THUMBNAIL_CACHE_MAX_BYTES_PER_DIR,
            catalog_v2_enabled: true,
            facet_bitmap_enabled: false,
            inventory_scanner_enabled: false,
            inventory_scanner_kinds: BTreeSet::new(),
            search_outbox_shadow_enabled: false,
            search_shadow_canary_enabled: false,
            search_incremental_reader_enabled: false,
            search_reader_prewarm_enabled: false,
            derivative_cache_v2_enabled: false,
            jpeg_thumbnail_downscale_enabled: false,
            derivative_cache_dir: root.join("derivatives"),
            derivative_cache_max_bytes: DEFAULT_DERIVATIVE_CACHE_MAX_BYTES,
            derivative_cache_low_watermark_bytes: DEFAULT_DERIVATIVE_CACHE_LOW_WATERMARK_BYTES,
            session_secret: "test-session-secret".to_string(),
            enable_file_watcher: false,
            watch_debounce_seconds: 20,
        }
    }

    #[test]
    fn non_loopback_rejects_default_session_secret() {
        let bind: SocketAddr = "0.0.0.0:8787".parse().unwrap();

        assert!(session_secret_for_bind(bind, None).is_err());
        assert!(session_secret_for_bind(bind, Some(DEFAULT_SESSION_SECRET)).is_err());
        assert!(
            session_secret_for_bind(bind, Some("replace-with-at-least-32-random-characters"))
                .is_err()
        );
    }

    #[test]
    fn loopback_generates_a_unique_session_secret_when_missing() {
        let bind: SocketAddr = "127.0.0.1:8787".parse().unwrap();
        let first = session_secret_for_bind(bind, None).unwrap();
        let second = session_secret_for_bind(bind, None).unwrap();

        assert!(first.len() >= MIN_SESSION_SECRET_LEN);
        assert_ne!(first, second);
        assert_ne!(first, DEFAULT_SESSION_SECRET);
    }

    #[test]
    fn configured_session_secret_must_be_long_enough() {
        let bind: SocketAddr = "127.0.0.1:8787".parse().unwrap();

        assert!(session_secret_for_bind(bind, Some("too-short")).is_err());
        assert_eq!(
            session_secret_for_bind(bind, Some("01234567890123456789012345678901")).unwrap(),
            "01234567890123456789012345678901"
        );
    }

    #[test]
    fn weak_passwords_are_rejected_for_public_binds() {
        assert!(is_weak_admin_password("admin"));
        assert!(is_weak_admin_password("change-me"));
        assert!(is_weak_admin_password("1234567"));
        assert!(is_weak_admin_password("12345678"));
        assert!(is_weak_admin_password("bbbbbbbb"));
        assert!(!is_weak_admin_password("correct horse battery staple"));
    }

    #[test]
    fn empty_cache_paths_use_the_configured_parent() {
        let fallback = PathBuf::from("data/cover-cache/comic");

        assert_eq!(non_empty_env_value(Some("   ".to_string())), None);
        assert_eq!(
            env_path_or("__MEDIA_SHELF_MISSING__", fallback.clone()),
            fallback
        );
    }

    #[test]
    fn cloud_cache_default_is_bounded() {
        assert_eq!(DEFAULT_CLOUD_CACHE_MAX_BYTES, 64 * 1024 * 1024 * 1024);
        assert_eq!(
            DEFAULT_THUMBNAIL_CACHE_MAX_BYTES_PER_DIR,
            8 * 1024 * 1024 * 1024
        );
        assert_eq!(DEFAULT_DERIVATIVE_CACHE_MAX_BYTES, 32 * 1024 * 1024 * 1024);
        assert_eq!(
            DEFAULT_DERIVATIVE_CACHE_LOW_WATERMARK_BYTES,
            28 * 1024 * 1024 * 1024
        );
    }

    #[test]
    fn incremental_reader_requires_shadow_and_canary_workers() {
        assert!(validate_search_flags(false, false, true).is_err());
        assert!(validate_search_flags(true, false, true).is_err());
        assert!(validate_search_flags(true, true, true).is_ok());
        assert!(validate_search_flags(false, false, false).is_ok());
    }

    #[test]
    fn inventory_kind_filter_defaults_to_all_and_rejects_unknown_values() {
        let all = parse_inventory_kinds(None, true).unwrap();
        assert_eq!(all.len(), INVENTORY_KINDS.len());
        assert!(all.contains("novel"));

        let selected = parse_inventory_kinds(Some(" novel, COMIC ".to_string()), true).unwrap();
        assert_eq!(
            selected.into_iter().collect::<Vec<_>>(),
            vec!["comic", "novel"]
        );

        let disabled = parse_inventory_kinds(Some("novel".to_string()), false).unwrap();
        assert!(disabled.is_empty());
        assert!(parse_inventory_kinds(Some("novel,video".to_string()), true).is_err());
    }

    #[test]
    fn promotion_kind_gate_is_fail_closed_for_unselected_media() {
        let mut config = test_config();
        config.inventory_scanner_enabled = true;
        config.inventory_scanner_kinds = parse_inventory_kinds(Some("novel".to_string()), true)
            .expect("novel is a supported rollout kind");

        assert!(config.inventory_kind_enabled("novel"));
        assert!(
            !config.inventory_kind_enabled("comic"),
            "promotion must reject a kind outside INVENTORY_SCANNER_KINDS"
        );

        config.inventory_scanner_enabled = false;
        assert!(
            !config.inventory_kind_enabled("novel"),
            "promotion must reject every kind while the Inventory coordinator is disabled"
        );
    }

    #[test]
    fn persisted_admin_password_reader_is_strict_and_bounded() {
        let temp = tempfile::tempdir().unwrap();
        assert_eq!(read_persisted_admin_password(temp.path()).unwrap(), None);

        let path = temp.path().join("admin-password.txt");
        std::fs::write(&path, b"  strong persisted passphrase  ").unwrap();
        assert_eq!(
            read_persisted_admin_password(temp.path()).unwrap(),
            Some("strong persisted passphrase".to_string())
        );
        std::fs::write(&path, b"   ").unwrap();
        assert!(read_persisted_admin_password(temp.path()).is_err());
        std::fs::write(
            &path,
            vec![b'x'; MAX_ADMIN_PASSWORD_FILE_BYTES as usize + 1],
        )
        .unwrap();
        assert!(read_persisted_admin_password(temp.path()).is_err());
    }
}
