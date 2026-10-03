use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock, RwLock};

use tokio::sync::Semaphore;

use crate::config::Config;
use crate::error::{AppError, Result};

const CANONICAL_ROOT_CACHE_LIMIT: usize = 512;
static CANONICAL_ROOT_CACHE: LazyLock<RwLock<HashMap<PathBuf, PathBuf>>> =
    LazyLock::new(|| RwLock::new(HashMap::new()));
static PATH_CANONICALIZE_WORKERS: LazyLock<Arc<Semaphore>> =
    LazyLock::new(|| Arc::new(Semaphore::new(4)));

pub fn ensure_asset_path_allowed_with_roots(
    config: &Config,
    raw: &str,
    additional_roots: &[PathBuf],
) -> Result<PathBuf> {
    let mut roots = vec![
        config.comics_dir.as_path(),
        config.novels_dir.as_path(),
        config.audio_dir.as_path(),
        config.gallery_dir.as_path(),
        config.coser_picture_dir.as_path(),
        config.generated_dir.as_path(),
    ];
    roots.extend(additional_roots.iter().map(|path| path.as_path()));
    let canonical_roots = roots
        .into_iter()
        .filter_map(cached_canonical_root)
        .collect::<Vec<_>>();

    let mut candidates = vec![PathBuf::from(raw)];
    if let Some(mapped) = legacy_container_asset_path(config, raw) {
        if !candidates.iter().any(|path| path == &mapped) {
            candidates.push(mapped);
        }
    }
    if let Some(mapped) = legacy_root_alias_asset_path(config, raw) {
        if !candidates.iter().any(|path| path == &mapped) {
            candidates.push(mapped);
        }
    }
    if let Some(repaired) = repair_utf8_mojibake_path(raw) {
        let repaired_path = PathBuf::from(&repaired);
        if !candidates.iter().any(|path| path == &repaired_path) {
            candidates.push(repaired_path);
        }
        if let Some(mapped) = legacy_container_asset_path(config, &repaired) {
            if !candidates.iter().any(|path| path == &mapped) {
                candidates.push(mapped);
            }
        }
        if let Some(mapped) = configured_root_name_asset_path(config, &repaired) {
            if !candidates.iter().any(|path| path == &mapped) {
                candidates.push(mapped);
            }
        }
        if let Some(mapped) = legacy_root_alias_asset_path(config, &repaired) {
            if !candidates.iter().any(|path| path == &mapped) {
                candidates.push(mapped);
            }
        }
    }

    let mut last_io_error = None;
    let mut found_outside_root = false;
    for candidate in candidates {
        match candidate.canonicalize() {
            Ok(canonical) => {
                if canonical_roots
                    .iter()
                    .any(|root| canonical.starts_with(root))
                {
                    return Ok(canonical);
                }
                found_outside_root = true;
            }
            Err(err) => last_io_error = Some(err),
        }
    }

    if found_outside_root {
        return Err(AppError::Unauthorized(format!(
            "asset path is outside configured libraries: {raw}"
        )));
    }
    Err(last_io_error
        .map(AppError::from)
        .unwrap_or_else(|| AppError::Unauthorized(format!("asset path is not readable: {raw}"))))
}

pub async fn ensure_asset_path_allowed_with_roots_async(
    config: &Config,
    raw: &str,
    additional_roots: &[PathBuf],
) -> Result<PathBuf> {
    let permit = PATH_CANONICALIZE_WORKERS
        .clone()
        .acquire_owned()
        .await
        .map_err(|_| AppError::Other("path canonicalizer is closed".to_string()))?;
    let config = config.clone();
    let raw = raw.to_string();
    let additional_roots = additional_roots.to_vec();
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        ensure_asset_path_allowed_with_roots(&config, &raw, &additional_roots)
    })
    .await
    .map_err(|err| AppError::Other(format!("path canonicalizer task failed: {err}")))?
}

fn cached_canonical_root(path: &Path) -> Option<PathBuf> {
    if let Ok(cache) = CANONICAL_ROOT_CACHE.read() {
        if let Some(canonical) = cache.get(path) {
            return Some(canonical.clone());
        }
    }

    let canonical = path.canonicalize().ok()?;
    if let Ok(mut cache) = CANONICAL_ROOT_CACHE.write() {
        if cache.len() >= CANONICAL_ROOT_CACHE_LIMIT {
            cache.clear();
        }
        cache.insert(path.to_path_buf(), canonical.clone());
    }
    Some(canonical)
}

fn repair_utf8_mojibake_path(raw: &str) -> Option<String> {
    let mut bytes = Vec::with_capacity(raw.len());
    for ch in raw.chars() {
        let value = ch as u32;
        if value > u8::MAX as u32 {
            return None;
        }
        bytes.push(value as u8);
    }
    String::from_utf8(bytes).ok().filter(|value| value != raw)
}

fn legacy_container_asset_path(config: &Config, raw: &str) -> Option<PathBuf> {
    let normalized = raw.trim().replace('\\', "/");
    let mappings = [
        ("/library/comics", &config.comics_dir),
        ("/library/novels", &config.novels_dir),
        ("/library/audio", &config.audio_dir),
        ("/library/gallery", &config.gallery_dir),
        ("/library/coser-picture", &config.coser_picture_dir),
        ("/app/generated", &config.generated_dir),
    ];
    for (prefix, root) in mappings {
        if normalized == prefix || normalized.starts_with(&format!("{prefix}/")) {
            let tail = normalized[prefix.len()..].trim_start_matches('/');
            let mut mapped = root.clone();
            for part in tail.split('/').filter(|part| !part.is_empty()) {
                mapped.push(part);
            }
            return Some(mapped);
        }
    }
    None
}

fn configured_root_name_asset_path(config: &Config, raw: &str) -> Option<PathBuf> {
    let normalized = raw.trim().replace('\\', "/");
    let (head, tail) = normalized.split_once('/')?;
    let roots = [
        &config.comics_dir,
        &config.novels_dir,
        &config.audio_dir,
        &config.gallery_dir,
        &config.coser_picture_dir,
        &config.generated_dir,
    ];
    let root = roots.iter().find(|root| {
        root.file_name()
            .and_then(|value| value.to_str())
            .map(|name| name == head)
            .unwrap_or(false)
    })?;
    let mut mapped = (*root).clone();
    for part in tail.split('/').filter(|part| !part.is_empty()) {
        mapped.push(part);
    }
    Some(mapped)
}

fn legacy_root_alias_asset_path(config: &Config, raw: &str) -> Option<PathBuf> {
    let normalized = raw.trim().replace('\\', "/");
    let (head, tail) = normalized.split_once('/')?;
    let root = match head {
        "漫画" => &config.comics_dir,
        "轻小说" => &config.novels_dir,
        "音声" => &config.audio_dir,
        "图库" => &config.gallery_dir,
        "COS图" | "CoserPicture" | "coser-picture" => &config.coser_picture_dir,
        _ => return None,
    };
    let mut mapped = root.clone();
    for part in tail.split('/').filter(|part| !part.is_empty()) {
        mapped.push(part);
    }
    Some(mapped)
}

pub fn path_mime(path: &Path) -> String {
    mime_guess::from_path(path)
        .first_or_octet_stream()
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config(root: &Path) -> Config {
        Config {
            bind: "127.0.0.1:0".to_string(),
            database_url: "sqlite::memory:".to_string(),
            data_dir: root.join("data"),
            cover_cache_dir: root.join("cover-cache"),
            comic_cover_cache_dir: root.join("cover-cache").join("comic"),
            novel_cover_cache_dir: root.join("cover-cache").join("novel"),
            audio_cover_cache_dir: root.join("cover-cache").join("audio"),
            gallery_cover_cache_dir: root.join("cover-cache").join("gallery"),
            coser_picture_cover_cache_dir: root.join("cover-cache").join("coser-picture"),
            comics_dir: root.join("comics"),
            novels_dir: root.join("novels"),
            audio_dir: root.join("audio"),
            gallery_dir: root.join("gallery"),
            coser_picture_dir: root.join("coser-picture"),
            generated_dir: root.join("generated"),
            lightnovel_api_bases: Vec::new(),
            lightnovel_access_token: None,
            enrichment_concurrency: 1,
            ehtt_url: String::new(),
            openai_api_key: None,
            openai_image_model: "gpt-image-2".to_string(),
            qmediasync_base_url: String::new(),
            qmediasync_strm_dir: None,
            cloud_cache_max_bytes: 64 * 1024 * 1024 * 1024,
            thumbnail_cache_max_bytes_per_dir: 8 * 1024 * 1024 * 1024,
            catalog_v2_enabled: true,
            facet_bitmap_enabled: false,
            inventory_scanner_enabled: false,
            inventory_scanner_kinds: std::collections::BTreeSet::new(),
            search_outbox_shadow_enabled: false,
            search_shadow_canary_enabled: false,
            search_incremental_reader_enabled: false,
            search_reader_prewarm_enabled: false,
            derivative_cache_v2_enabled: false,
            jpeg_thumbnail_downscale_enabled: false,
            derivative_cache_dir: root.join("derivatives"),
            derivative_cache_max_bytes: 1024,
            derivative_cache_low_watermark_bytes: 512,
            enable_file_watcher: false,
            watch_debounce_seconds: 20,
        }
    }

    #[test]
    fn maps_legacy_library_paths_to_configured_roots() {
        let temp = tempfile::tempdir().unwrap();
        let config = test_config(temp.path());
        let asset = config.comics_dir.join("book").join("cover.jpg");
        std::fs::create_dir_all(asset.parent().unwrap()).unwrap();
        std::fs::write(&asset, b"jpg").unwrap();

        let resolved =
            ensure_asset_path_allowed_with_roots(&config, "/library/comics/book/cover.jpg", &[])
                .unwrap();

        assert_eq!(resolved, asset.canonicalize().unwrap());
    }

    #[test]
    fn maps_legacy_generated_paths_to_configured_root() {
        let temp = tempfile::tempdir().unwrap();
        let config = test_config(temp.path());
        let asset = config.generated_dir.join("covers").join("1.jpg");
        std::fs::create_dir_all(asset.parent().unwrap()).unwrap();
        std::fs::write(&asset, b"jpg").unwrap();

        let resolved =
            ensure_asset_path_allowed_with_roots(&config, "/app/generated/covers/1.jpg", &[])
                .unwrap();

        assert_eq!(resolved, asset.canonicalize().unwrap());
    }

    #[test]
    fn repairs_utf8_mojibake_paths_when_resolving_assets() {
        let temp = tempfile::tempdir().unwrap();
        let mut config = test_config(temp.path());
        config.coser_picture_dir = temp.path().join("COS图");
        let asset = config.coser_picture_dir.join("Aram").join("set.zip");
        std::fs::create_dir_all(asset.parent().unwrap()).unwrap();
        std::fs::write(&asset, b"zip").unwrap();

        let mojibake = String::from_utf8(vec![
            b'C', b'O', b'S', 0xc3, 0xa5, 0xc2, 0x9b, 0xc2, 0xbe, b'/', b'A', b'r', b'a', b'm',
            b'/', b's', b'e', b't', b'.', b'z', b'i', b'p',
        ])
        .unwrap();
        let resolved = ensure_asset_path_allowed_with_roots(&config, &mojibake, &[]).unwrap();

        assert_eq!(resolved, asset.canonicalize().unwrap());
    }

    #[test]
    fn maps_legacy_coser_root_alias_to_configured_root() {
        let temp = tempfile::tempdir().unwrap();
        let mut config = test_config(temp.path());
        config.coser_picture_dir = temp.path().join("library").join("coser-picture");
        let asset = config.coser_picture_dir.join("Aram").join("set.zip");
        std::fs::create_dir_all(asset.parent().unwrap()).unwrap();
        std::fs::write(&asset, b"zip").unwrap();

        let resolved =
            ensure_asset_path_allowed_with_roots(&config, "COS图/Aram/set.zip", &[]).unwrap();

        assert_eq!(resolved, asset.canonicalize().unwrap());
    }
}
