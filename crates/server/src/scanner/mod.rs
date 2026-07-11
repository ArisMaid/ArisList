use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::LazyLock;

use lofty::prelude::*;
use lofty::probe::Probe;
use regex::Regex;
use serde::Deserialize;
use serde_json::json;
use walkdir::WalkDir;
use zip::ZipArchive;

use crate::error::{AppError, Result};
use crate::models::ScanResponse;
use crate::settings;
use crate::vfs;
use crate::AppState;

static NATURAL_NUMBER_RE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"\d+").unwrap());

#[derive(Default)]
pub struct ScanStats {
    pub comics: usize,
    pub novels: usize,
    pub audio: usize,
    pub gallery: usize,
    pub coser_picture: usize,
    pub jobs_created: usize,
}

pub async fn scan_all(state: &AppState, _enqueue_enrichment: bool) -> Result<ScanResponse> {
    let mut stats = ScanStats::default();
    stats.comics = scan_comics(state).await?;
    stats.novels = scan_novels(state).await?;
    stats.audio = scan_audio(state).await?;
    stats.gallery = scan_gallery(state).await?;
    stats.coser_picture = scan_coser_pictures(state).await?;
    stats.coser_picture += scan_qmediasync_coser_pictures(state, &settings::load_settings(&state.config).await?).await?;
    stats.jobs_created += 1;
    state
        .db
        .create_job(
            "rebuild-search-index",
            "queued",
            json!({ "source": "scan" }),
        )
        .await?;

    Ok(ScanResponse {
        comics: stats.comics,
        novels: stats.novels,
        audio: stats.audio,
        gallery: stats.gallery,
        coser_picture: stats.coser_picture,
        jobs_created: stats.jobs_created,
    })
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
struct ComicInfo {
    series: Option<String>,
    alternate_series: Option<String>,
    writer: Option<String>,
    penciller: Option<String>,
    genre: Option<String>,
    page_count: Option<i64>,
    language_iso: Option<String>,
    community_rating: Option<f64>,
}

async fn scan_comics(state: &AppState) -> Result<usize> {
    let app_settings = settings::load_settings(&state.config).await?;
    let roots = app_settings.comic_roots();
    let mut count = 0;
    for root in roots {
        if !root.exists() {
            continue;
        }
        for entry in WalkDir::new(&root)
            .min_depth(1)
            .max_depth(2)
            .into_iter()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_type().is_file() && extension_is(e.path(), &["cbz"]))
        {
            let cbz_path = entry.path().to_path_buf();
            let dir = cbz_path.parent().unwrap_or(root.as_path());
            let comic_info = read_comic_info(dir).unwrap_or_default();
            let title = clean_title(
                comic_info
                    .series
                    .as_deref()
                    .filter(|s| !s.trim().is_empty())
                    .unwrap_or_else(|| {
                        dir.file_name()
                            .and_then(|v| v.to_str())
                            .unwrap_or("Untitled comic")
                    }),
            );
            let page_count = comic_info
                .page_count
                .or_else(|| count_cbz_pages(&cbz_path).ok())
                .unwrap_or(0);
            let rating = comic_info.community_rating;

            let work_id = state
                .db
                .upsert_work(
                    "comic",
                    &title,
                    Some(&path_string(&cbz_path)),
                    Some("Doujinshi"),
                    comic_info.alternate_series.as_deref(),
                    rating,
                    json!({
                        "page_count": page_count,
                        "writer": comic_info.writer.clone(),
                        "penciller": comic_info.penciller.clone(),
                        "language_iso": comic_info.language_iso.clone(),
                    }),
                )
                .await?;

            let size = std::fs::metadata(&cbz_path).ok().map(|m| m.len() as i64);
            state
                .db
                .upsert_asset(
                    work_id,
                    &path_string(&cbz_path),
                    "application/vnd.comicbook+zip",
                    "archive",
                    Some("cbz"),
                    None,
                    size,
                    json!({ "page_count": page_count }),
                )
                .await?;

            if let Some(cover) = find_cover_file(dir) {
                let mime = mime_guess::from_path(&cover)
                    .first_or_octet_stream()
                    .to_string();
                let size = std::fs::metadata(&cover).ok().map(|m| m.len() as i64);
                state
                    .db
                    .upsert_asset(
                        work_id,
                        &path_string(&cover),
                        &mime,
                        "cover",
                        None,
                        None,
                        size,
                        json!({}),
                    )
                    .await?;
            }

            if let Some(genre) = comic_info.genre.as_deref() {
                for tag in parse_comic_genre_tags(genre) {
                    link_tag(
                        state,
                        work_id,
                        &tag.namespace,
                        &tag.key,
                        &tag.label,
                        "comic-info",
                    )
                    .await?;
                }
            }
            for (namespace, value) in [
                ("artist", comic_info.penciller.as_deref()),
                ("group", comic_info.writer.as_deref()),
            ] {
                if let Some(value) = value.filter(|v| !v.trim().is_empty()) {
                    link_tag(
                        state,
                        work_id,
                        namespace,
                        &normalize_key(value),
                        value,
                        "comic-info",
                    )
                    .await?;
                }
            }
            if let Some(lang) = comic_info.language_iso.as_deref() {
                let label = match lang {
                    "zh" | "cn" => "chinese",
                    "ja" => "japanese",
                    "en" => "english",
                    other => other,
                };
                link_tag(state, work_id, "language", label, label, "comic-info").await?;
            }
            count += 1;
        }
    }
    count += scan_qmediasync_comics(state, &app_settings).await?;
    Ok(count)
}

async fn scan_novels(state: &AppState) -> Result<usize> {
    let app_settings = settings::load_settings(&state.config).await?;
    let roots = app_settings.novel_roots();
    let mut count = 0;
    for root in roots {
        if !root.exists() {
            continue;
        }
        for entry in WalkDir::new(&root)
            .min_depth(1)
            .into_iter()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_type().is_file() && extension_is(e.path(), &["epub"]))
        {
            let epub_path = entry.path().to_path_buf();
            let meta = read_epub_metadata(&epub_path).unwrap_or_default();
            let title = meta.title.clone().unwrap_or_else(|| {
                epub_path
                    .file_stem()
                    .and_then(|v| v.to_str())
                    .unwrap_or("Untitled novel")
                    .to_string()
            });
            let series = meta.series.clone().or_else(|| {
                epub_path
                    .parent()
                    .and_then(|p| p.file_name())
                    .and_then(|v| v.to_str())
                    .map(|s| s.to_string())
            });

            let work_id = state
                .db
                .upsert_work(
                    "novel",
                    &clean_title(&title),
                    Some(&path_string(&epub_path)),
                    Some("Light Novel"),
                    meta.description.as_deref(),
                    None,
                    json!({
                        "creator": meta.creator.clone(),
                        "language": meta.language.clone(),
                        "series": series.clone(),
                        "volume": meta.volume.clone(),
                        "source": meta.source.clone(),
                    }),
                )
                .await?;

            let size = std::fs::metadata(&epub_path).ok().map(|m| m.len() as i64);
            state
                .db
                .upsert_asset(
                    work_id,
                    &path_string(&epub_path),
                    "application/epub+zip",
                    "book",
                    Some("epub"),
                    None,
                    size,
                    json!({}),
                )
                .await?;

            if let Ok(Some(cover)) =
                extract_epub_cover(&epub_path, &state.config.generated_dir, work_id)
            {
                let mime = mime_guess::from_path(&cover)
                    .first_or_octet_stream()
                    .to_string();
                let size = std::fs::metadata(&cover).ok().map(|m| m.len() as i64);
                state
                    .db
                    .upsert_asset(
                        work_id,
                        &path_string(&cover),
                        &mime,
                        "cover",
                        Some("epub-extracted"),
                        None,
                        size,
                        json!({ "source": "epub" }),
                    )
                    .await?;
            }

            if let Some(series) = series.as_deref() {
                link_tag(
                    state,
                    work_id,
                    "series",
                    &normalize_key(series),
                    series,
                    "epub",
                )
                .await?;
            }
            if let Some(author) = meta.creator.as_deref() {
                link_tag(
                    state,
                    work_id,
                    "artist",
                    &normalize_key(author),
                    author,
                    "epub",
                )
                .await?;
            }
            for subject in &meta.subjects {
                link_tag(
                    state,
                    work_id,
                    "ln",
                    &normalize_key(subject),
                    subject,
                    "epub",
                )
                .await?;
            }
            if let Some(lang) = meta.language.as_deref() {
                link_tag(
                    state,
                    work_id,
                    "language",
                    &normalize_key(lang),
                    lang,
                    "epub",
                )
                .await?;
            }
            count += 1;
        }
    }
    Ok(count)
}

async fn scan_audio(state: &AppState) -> Result<usize> {
    let app_settings = settings::load_settings(&state.config).await?;
    let roots = app_settings.audio_roots();
    let rj_re = Regex::new(r"RJ\d{6,9}").unwrap();
    let mut count = 0;
    for audio_dir in roots {
        if !audio_dir.exists() {
            continue;
        }
        let mut groups: BTreeMap<String, Vec<PathBuf>> = BTreeMap::new();
        for entry in WalkDir::new(&audio_dir)
            .min_depth(1)
            .into_iter()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_type().is_file())
        {
            let path = entry.path();
            if !extension_is(
                path,
                &[
                    "mp3", "wav", "flac", "ogg", "m4a", "jpg", "jpeg", "png", "webp", "txt",
                ],
            ) {
                continue;
            }
            let full = path_string(path);
            if let Some(m) = rj_re.find(&full) {
                groups
                    .entry(m.as_str().to_string())
                    .or_default()
                    .push(path.to_path_buf());
            }
        }

        for (rj, mut files) in groups {
            files.sort();
            let root = common_rj_root(&audio_dir, &rj, &files);
            let title = infer_audio_title(&root, &rj);
            let track_count = files
                .iter()
                .filter(|p| extension_is(p, &["mp3", "wav", "flac", "ogg", "m4a"]))
                .count();

            let work_id = state
                .db
                .upsert_work(
                    "audio",
                    &title,
                    Some(&path_string(&root)),
                    Some("Audio"),
                    read_first_text_summary(&files).as_deref(),
                    None,
                    json!({ "rj": rj.clone(), "track_count": track_count }),
                )
                .await?;

            state
                .db
                .upsert_external_id(
                    work_id,
                    "asmr",
                    &rj,
                    None,
                    Some(&format!("https://asmr.one/work/{rj}")),
                )
                .await?;
            state
                .db
                .upsert_external_id(
                    work_id,
                    "dlsite",
                    &rj,
                    None,
                    Some(&format!(
                        "https://www.dlsite.com/maniax/work/=/product_id/{rj}.html"
                    )),
                )
                .await?;
            link_tag(state, work_id, "audio", "asmr", "ASMR", "audio-folder").await?;
            link_tag(
                state,
                work_id,
                "source",
                &rj.to_lowercase(),
                &rj,
                "audio-folder",
            )
            .await?;

            let mut next_position = 0_i64;
            let mut track_positions = BTreeMap::new();
            let mut seen_track_names = BTreeSet::new();
            for file in files
                .iter()
                .filter(|p| extension_is(p, &["mp3", "wav", "flac", "ogg", "m4a"]))
            {
                let ext = file
                    .extension()
                    .and_then(|v| v.to_str())
                    .unwrap_or("")
                    .to_lowercase();
                let variant = infer_audio_variant(file);
                let stem = file
                    .file_stem()
                    .and_then(|v| v.to_str())
                    .unwrap_or_default()
                    .to_string();
                let track_key = normalize_track_key(&stem);
                let position = *track_positions.entry(track_key.clone()).or_insert_with(|| {
                    let current = next_position;
                    next_position += 1;
                    current
                });
                let size = std::fs::metadata(file).ok().map(|m| m.len() as i64);
                let mime = mime_guess::from_path(file)
                    .first_or_octet_stream()
                    .to_string();
                let mut meta = read_audio_metadata(file, &stem, &ext);
                if let Some(meta) = meta.as_object_mut() {
                    meta.insert("track_key".to_string(), json!(track_key));
                    meta.insert("format".to_string(), json!(ext.clone()));
                    meta.insert(
                        "preferred_playback".to_string(),
                        json!(mime == "audio/mpeg"),
                    );
                }
                state
                    .db
                    .upsert_asset(
                        work_id,
                        &path_string(file),
                        &mime,
                        "track",
                        Some(&variant),
                        Some(position),
                        size,
                        meta,
                    )
                    .await?;
                seen_track_names.insert(variant);
            }

            for file in files
                .iter()
                .filter(|p| extension_is(p, &["jpg", "jpeg", "png", "webp"]))
            {
                let file_name = file
                    .file_name()
                    .and_then(|v| v.to_str())
                    .unwrap_or_default();
                if file_name.contains("ジャケット")
                    || file_name.to_ascii_lowercase().contains("cover")
                    || file_name.to_ascii_lowercase().contains("jacket")
                {
                    let mime = mime_guess::from_path(file)
                        .first_or_octet_stream()
                        .to_string();
                    let size = std::fs::metadata(file).ok().map(|m| m.len() as i64);
                    state
                        .db
                        .upsert_asset(
                            work_id,
                            &path_string(file),
                            &mime,
                            "cover",
                            None,
                            None,
                            size,
                            json!({}),
                        )
                        .await?;
                    break;
                }
            }
            if let Some(cover) = audio_cover_candidate(&files, &root) {
                let mime = mime_guess::from_path(&cover)
                    .first_or_octet_stream()
                    .to_string();
                let size = std::fs::metadata(&cover).ok().map(|m| m.len() as i64);
                state
                    .db
                    .upsert_asset(
                        work_id,
                        &path_string(&cover),
                        &mime,
                        "cover",
                        None,
                        None,
                        size,
                        json!({}),
                    )
                    .await?;
            }

            for variant in seen_track_names {
                link_tag(
                    state,
                    work_id,
                    "audio",
                    &normalize_key(&variant),
                    &variant,
                    "audio-folder",
                )
                .await?;
            }
            count += 1;
        }
    }
    Ok(count)
}

async fn scan_gallery(state: &AppState) -> Result<usize> {
    let app_settings = settings::load_settings(&state.config).await?;
    let roots = app_settings.gallery_roots();
    let mut count = 0;
    for root in roots {
        if !root.exists() {
            continue;
        }
        let mut groups: BTreeMap<PathBuf, Vec<PathBuf>> = BTreeMap::new();
        for entry in WalkDir::new(&root)
            .min_depth(1)
            .into_iter()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_type().is_file() && gallery_image_name(e.path()))
        {
            let path = entry.path().to_path_buf();
            let parent = path.parent().unwrap_or(root.as_path()).to_path_buf();
            groups.entry(parent).or_default().push(path);
        }

        for (folder, mut files) in groups {
            files.sort_by(|a, b| {
                naturalish_key(&path_string(a)).cmp(&naturalish_key(&path_string(b)))
            });
            let title = folder
                .file_name()
                .and_then(|value| value.to_str())
                .filter(|value| !value.trim().is_empty())
                .unwrap_or("图库")
                .to_string();
            let relative = folder
                .strip_prefix(&root)
                .ok()
                .map(|value| value.to_string_lossy().to_string())
                .filter(|value| !value.is_empty());
            let work_id = state
                .db
                .upsert_work(
                    "gallery",
                    &clean_title(&title),
                    Some(&path_string(&folder)),
                    Some("Gallery"),
                    relative.as_deref(),
                    None,
                    json!({
                        "image_count": files.len(),
                        "root": path_string(&root),
                        "folder": path_string(&folder),
                    }),
                )
                .await?;

            let cover_path = gallery_cover_candidate(&files).cloned();
            if let Some(cover) = cover_path.as_ref() {
                let mime = mime_guess::from_path(cover)
                    .first_or_octet_stream()
                    .to_string();
                let size = std::fs::metadata(cover).ok().map(|m| m.len() as i64);
                state
                    .db
                    .upsert_asset(
                        work_id,
                        &path_string(cover),
                        &mime,
                        "cover",
                        None,
                        None,
                        size,
                        json!({ "source": "gallery" }),
                    )
                    .await?;
            }

            for (index, file) in files.iter().enumerate() {
                let mime = mime_guess::from_path(file)
                    .first_or_octet_stream()
                    .to_string();
                let size = std::fs::metadata(file).ok().map(|m| m.len() as i64);
                state
                    .db
                    .upsert_asset(
                        work_id,
                        &path_string(file),
                        &mime,
                        "image",
                        None,
                        Some(index as i64),
                        size,
                        json!({ "source": "gallery" }),
                    )
                    .await?;
            }

            link_tag(
                state,
                work_id,
                "gallery",
                "image-set",
                "图库",
                "gallery-folder",
            )
            .await?;
            link_tag(
                state,
                work_id,
                "folder",
                &normalize_key(&title),
                &title,
                "gallery-folder",
            )
            .await?;
            if let Some(top) = folder
                .strip_prefix(&root)
                .ok()
                .and_then(|path| path.components().next())
                .and_then(|part| part.as_os_str().to_str())
                .filter(|value| !value.trim().is_empty())
            {
                link_tag(
                    state,
                    work_id,
                    "artist",
                    &normalize_key(top),
                    top,
                    "gallery-folder",
                )
                .await?;
            }
            let mut filename_tags = BTreeSet::new();
            for file in &files {
                filename_tags.extend(gallery_filename_tags(file));
            }
            for tag in filename_tags {
                link_tag(
                    state,
                    work_id,
                    "gallery",
                    &normalize_key(&tag),
                    &tag,
                    "gallery-filename",
                )
                .await?;
            }
            count += 1;
        }
    }
    Ok(count)
}

async fn scan_coser_pictures(state: &AppState) -> Result<usize> {
    let app_settings = settings::load_settings(&state.config).await?;
    let roots = app_settings.coser_picture_roots();
    let mut count = 0;
    for root in roots {
        if !root.exists() {
            continue;
        }
        for entry in WalkDir::new(&root)
            .min_depth(1)
            .into_iter()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_type().is_file() && extension_is(e.path(), &["zip"]))
        {
            let zip_path = entry.path().to_path_buf();
            let page_count = count_cbz_pages(&zip_path).unwrap_or(0);
            if page_count <= 0 {
                continue;
            }
            let title = zip_path
                .file_stem()
                .and_then(|value| value.to_str())
                .filter(|value| !value.trim().is_empty())
                .unwrap_or("Untitled CoserPicture")
                .to_string();
            let coser = zip_path
                .parent()
                .and_then(|path| path.file_name())
                .and_then(|value| value.to_str())
                .filter(|value| !value.trim().is_empty())
                .unwrap_or("CoserPicture")
                .to_string();
            let relative = zip_path
                .strip_prefix(&root)
                .ok()
                .map(|value| value.to_string_lossy().to_string())
                .filter(|value| !value.is_empty());

            let work_id = state
                .db
                .upsert_work(
                    "coser-picture",
                    &clean_title(&title),
                    Some(&path_string(&zip_path)),
                    Some("CoserPicture"),
                    relative.as_deref(),
                    None,
                    json!({
                        "page_count": page_count,
                        "root": path_string(&root),
                        "archive": path_string(&zip_path),
                        "coser": coser.clone(),
                    }),
                )
                .await?;

            let size = std::fs::metadata(&zip_path).ok().map(|m| m.len() as i64);
            state
                .db
                .upsert_asset(
                    work_id,
                    &path_string(&zip_path),
                    "application/zip",
                    "archive",
                    Some("zip"),
                    None,
                    size,
                    json!({ "source": "coser-picture", "page_count": page_count }),
                )
                .await?;

            link_tag(
                state,
                work_id,
                "coser-picture",
                "image-set",
                "CoserPicture",
                "coser-picture-zip",
            )
            .await?;
            link_tag(
                state,
                work_id,
                "folder",
                &normalize_key(&coser),
                &coser,
                "coser-picture-zip",
            )
            .await?;
            link_tag(
                state,
                work_id,
                "artist",
                &normalize_key(&coser),
                &coser,
                "coser-picture-zip",
            )
            .await?;
            count += 1;
        }
    }
    Ok(count)
}

async fn scan_qmediasync_comics(
    state: &AppState,
    app_settings: &settings::AppSettings,
) -> Result<usize> {
    let mut count = 0;
    for source in vfs::qmediasync_scan_sources(app_settings, "comic") {
        let root = PathBuf::from(&source.root);
        if !root.exists() || !root.is_dir() {
            continue;
        }
        for entry in WalkDir::new(&root)
            .min_depth(1)
            .max_depth(source.scan_depth.clamp(1, 64))
            .into_iter()
            .filter_map(|entry| entry.ok())
            .filter(|entry| entry.file_type().is_file())
        {
            let archive_path = entry.path().to_path_buf();
            let is_strm = is_strm_file(&archive_path);
            if !is_strm && !extension_is(&archive_path, &["cbz"]) {
                continue;
            }
            let dir = archive_path.parent().unwrap_or(root.as_path());
            let comic_info = read_comic_info(dir).unwrap_or_default();
            let title = clean_title(
                comic_info
                    .series
                    .as_deref()
                    .filter(|s| !s.trim().is_empty())
                    .unwrap_or_else(|| {
                        dir.file_name()
                            .and_then(|v| v.to_str())
                            .or_else(|| archive_path.file_stem().and_then(|v| v.to_str()))
                            .unwrap_or("qmediasync comic")
                    }),
            );
            let page_count = comic_info
                .page_count
                .or_else(|| {
                    (!is_strm)
                        .then(|| count_cbz_pages(&archive_path).ok())
                        .flatten()
                })
                .unwrap_or(0);
            let archive_uri = if is_strm {
                let relative = archive_path
                    .strip_prefix(&root)
                    .unwrap_or(&archive_path)
                    .to_string_lossy()
                    .replace('\\', "/");
                vfs::qms_strm_uri(&source.mount_name, &relative)
            } else {
                path_string(&archive_path)
            };
            let work_id = state
                .db
                .upsert_work(
                    "comic",
                    &title,
                    Some(&archive_uri),
                    Some("Doujinshi"),
                    comic_info.alternate_series.as_deref(),
                    comic_info.community_rating,
                    json!({
                        "source": "qmediasync",
                        "provider": "qmediasync",
                        "mount_name": source.mount_name,
                        "strm_root": path_string(&root),
                        "page_count": page_count,
                        "writer": comic_info.writer.clone(),
                        "penciller": comic_info.penciller.clone(),
                        "language_iso": comic_info.language_iso.clone(),
                    }),
                )
                .await?;

            let size = std::fs::metadata(&archive_path)
                .ok()
                .map(|m| m.len() as i64);
            let meta = if is_strm {
                let relative = archive_path
                    .strip_prefix(&root)
                    .unwrap_or(&archive_path)
                    .to_string_lossy()
                    .replace('\\', "/");
                let target_url = match vfs::read_qms_strm_url(&archive_path) {
                    Ok(target_url) => target_url,
                    Err(err) => {
                        tracing::warn!(
                            path = %archive_path.to_string_lossy(),
                            error = %err,
                            "skipping invalid qmediasync STRM file"
                        );
                        continue;
                    }
                };
                vfs::qms_strm_meta_json(
                    &source.mount_name,
                    &root,
                    &archive_path,
                    &relative,
                    &target_url,
                )
            } else {
                json!({ "source": "qmediasync", "provider": "qmediasync", "page_count": page_count })
            };
            state
                .db
                .upsert_asset(
                    work_id,
                    &archive_uri,
                    "application/vnd.comicbook+zip",
                    "archive",
                    Some(if is_strm { "cbz-strm" } else { "cbz" }),
                    None,
                    size,
                    meta,
                )
                .await?;

            if let Some(cover) = find_cover_file(dir) {
                let mime = mime_guess::from_path(&cover)
                    .first_or_octet_stream()
                    .to_string();
                let size = std::fs::metadata(&cover).ok().map(|m| m.len() as i64);
                state
                    .db
                    .upsert_asset(
                        work_id,
                        &path_string(&cover),
                        &mime,
                        "cover",
                        None,
                        None,
                        size,
                        json!({ "source": "qmediasync" }),
                    )
                    .await?;
            }

            if let Some(genre) = comic_info.genre.as_deref() {
                for tag in parse_comic_genre_tags(genre) {
                    link_tag(
                        state,
                        work_id,
                        &tag.namespace,
                        &tag.key,
                        &tag.label,
                        "comic-info",
                    )
                    .await?;
                }
            }
            for (namespace, value) in [
                ("artist", comic_info.penciller.as_deref()),
                ("group", comic_info.writer.as_deref()),
            ] {
                if let Some(value) = value.filter(|v| !v.trim().is_empty()) {
                    link_tag(
                        state,
                        work_id,
                        namespace,
                        &normalize_key(value),
                        value,
                        "comic-info",
                    )
                    .await?;
                }
            }
            if let Some(lang) = comic_info.language_iso.as_deref() {
                let label = match lang {
                    "zh" | "cn" => "chinese",
                    "ja" => "japanese",
                    "en" => "english",
                    other => other,
                };
                link_tag(state, work_id, "language", label, label, "comic-info").await?;
            }
            link_tag(
                state,
                work_id,
                "source",
                "qmediasync",
                "qmediasync",
                "qmediasync",
            )
            .await?;
            count += 1;
        }
    }
    Ok(count)
}

async fn scan_qmediasync_coser_pictures(
    state: &AppState,
    app_settings: &settings::AppSettings,
) -> Result<usize> {
    let mut count = 0;
    for source in vfs::qmediasync_scan_sources(app_settings, "coser-picture") {
        let root = PathBuf::from(&source.root);
        if !root.exists() || !root.is_dir() {
            continue;
        }
        for entry in WalkDir::new(&root)
            .min_depth(1)
            .max_depth(source.scan_depth.clamp(1, 64))
            .into_iter()
            .filter_map(|entry| entry.ok())
            .filter(|entry| entry.file_type().is_file())
        {
            let archive_path = entry.path().to_path_buf();
            let is_strm = is_strm_file(&archive_path);
            if !is_strm && !extension_is(&archive_path, &["zip"]) {
                continue;
            }
            let dir = archive_path.parent().unwrap_or(root.as_path());
            let page_count = if is_strm {
                0
            } else {
                count_cbz_pages(&archive_path).unwrap_or(0)
            };
            let title = archive_path
                .file_stem()
                .and_then(|value| value.to_str())
                .filter(|value| !value.trim().is_empty())
                .or_else(|| dir.file_name().and_then(|value| value.to_str()))
                .unwrap_or("qmediasync CoserPicture")
                .to_string();
            let coser = dir
                .file_name()
                .and_then(|value| value.to_str())
                .filter(|value| !value.trim().is_empty())
                .unwrap_or("CoserPicture")
                .to_string();
            let relative = archive_path
                .strip_prefix(&root)
                .unwrap_or(&archive_path)
                .to_string_lossy()
                .replace('\\', "/");
            let archive_uri = if is_strm {
                vfs::qms_strm_uri(&source.mount_name, &relative)
            } else {
                path_string(&archive_path)
            };
            let work_id = state
                .db
                .upsert_work(
                    "coser-picture",
                    &clean_title(&title),
                    Some(&archive_uri),
                    Some("CoserPicture"),
                    Some(&relative),
                    None,
                    json!({
                        "source": "qmediasync",
                        "provider": "qmediasync",
                        "mount_name": source.mount_name,
                        "strm_root": path_string(&root),
                        "page_count": page_count,
                        "coser": coser.clone(),
                    }),
                )
                .await?;

            let size = std::fs::metadata(&archive_path)
                .ok()
                .map(|m| m.len() as i64);
            let meta = if is_strm {
                let target_url = match vfs::read_qms_strm_url(&archive_path) {
                    Ok(target_url) => target_url,
                    Err(err) => {
                        tracing::warn!(
                            path = %archive_path.to_string_lossy(),
                            error = %err,
                            "skipping invalid qmediasync CoserPicture STRM file"
                        );
                        continue;
                    }
                };
                vfs::qms_strm_meta_json(
                    &source.mount_name,
                    &root,
                    &archive_path,
                    &relative,
                    &target_url,
                )
            } else {
                json!({ "source": "qmediasync", "provider": "qmediasync", "page_count": page_count })
            };
            state
                .db
                .upsert_asset(
                    work_id,
                    &archive_uri,
                    if is_strm {
                        "application/zip"
                    } else {
                        "application/zip"
                    },
                    "archive",
                    Some(if is_strm { "zip-strm" } else { "zip" }),
                    None,
                    size,
                    meta,
                )
                .await?;

            if let Some(cover) = find_cover_file(dir) {
                let mime = mime_guess::from_path(&cover)
                    .first_or_octet_stream()
                    .to_string();
                let size = std::fs::metadata(&cover).ok().map(|m| m.len() as i64);
                state
                    .db
                    .upsert_asset(
                        work_id,
                        &path_string(&cover),
                        &mime,
                        "cover",
                        None,
                        None,
                        size,
                        json!({ "source": "qmediasync" }),
                    )
                    .await?;
            }

            link_tag(
                state,
                work_id,
                "coser-picture",
                "image-set",
                "CoserPicture",
                "qmediasync",
            )
            .await?;
            link_tag(
                state,
                work_id,
                "artist",
                &normalize_key(&coser),
                &coser,
                "qmediasync",
            )
            .await?;
            link_tag(
                state,
                work_id,
                "source",
                "qmediasync",
                "qmediasync",
                "qmediasync",
            )
            .await?;
            count += 1;
        }
    }
    Ok(count)
}

fn is_strm_file(path: &Path) -> bool {
    extension_is(path, &["strm"])
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

fn read_comic_info(dir: &Path) -> Option<ComicInfo> {
    let path = dir.join("ComicInfo.xml");
    let xml = std::fs::read_to_string(path).ok()?;
    quick_xml::de::from_str(&xml).ok()
}

fn count_cbz_pages(path: &Path) -> Result<i64> {
    let file = File::open(path)?;
    let mut archive = ZipArchive::new(file).map_err(|e| AppError::Other(e.to_string()))?;
    let mut count = 0;
    for i in 0..archive.len() {
        let entry = archive
            .by_index(i)
            .map_err(|e| AppError::Other(e.to_string()))?;
        if image_name(entry.name()) {
            count += 1;
        }
    }
    Ok(count)
}

#[derive(Default)]
struct EpubMetadata {
    title: Option<String>,
    creator: Option<String>,
    description: Option<String>,
    language: Option<String>,
    source: Option<String>,
    series: Option<String>,
    volume: Option<String>,
    subjects: Vec<String>,
}

fn read_epub_metadata(path: &Path) -> Option<EpubMetadata> {
    let file = File::open(path).ok()?;
    let mut archive = ZipArchive::new(file).ok()?;
    let mut opf_name = None;
    for i in 0..archive.len() {
        let entry = archive.by_index(i).ok()?;
        if entry.name().ends_with(".opf") {
            opf_name = Some(entry.name().to_string());
            break;
        }
    }
    let opf_name = opf_name?;
    let mut opf = String::new();
    let mut opf_file = archive.by_name(&opf_name).ok()?;
    opf_file.read_to_string(&mut opf).ok()?;
    Some(EpubMetadata {
        title: capture_xml(&opf, "dc:title"),
        creator: capture_xml(&opf, "dc:creator"),
        description: capture_xml(&opf, "dc:description")
            .map(|v| html_escape::decode_html_entities(&v).to_string()),
        language: capture_xml(&opf, "dc:language"),
        source: capture_xml(&opf, "dc:identifier"),
        series: capture_meta_property(&opf, "belongs-to-collection"),
        volume: capture_meta_refine_property(&opf, "group-position"),
        subjects: capture_all_xml(&opf, "dc:subject"),
    })
}

#[derive(Debug, Clone)]
struct EpubManifestItem {
    id: String,
    href: String,
    media_type: String,
    properties: String,
}

fn extract_epub_cover(
    epub_path: &Path,
    generated_dir: &Path,
    work_id: i64,
) -> Result<Option<PathBuf>> {
    let file = File::open(epub_path)?;
    let mut archive = ZipArchive::new(file).map_err(|e| AppError::Other(e.to_string()))?;
    let opf_name = epub_opf_name(&mut archive)?;
    let opf = read_zip_text(&mut archive, &opf_name)?;
    let base = zip_parent(&opf_name);
    let items = parse_epub_manifest_items(&opf);

    let mut candidates = Vec::new();
    if let Some(cover_id) = capture_meta_name_content(&opf, "cover") {
        for item in &items {
            if item.id == cover_id || item.href == cover_id {
                candidates.push(join_zip_path(&base, &item.href));
            }
        }
    }
    for item in &items {
        if item
            .properties
            .split_whitespace()
            .any(|property| property == "cover-image")
        {
            candidates.push(join_zip_path(&base, &item.href));
        }
    }
    for item in &items {
        let lower = item.href.to_ascii_lowercase();
        if item.media_type.starts_with("image/")
            && (lower.contains("cover") || lower.contains("thumb") || lower.contains("title"))
        {
            candidates.push(join_zip_path(&base, &item.href));
        }
    }
    for item in &items {
        if item.media_type.starts_with("image/") {
            candidates.push(join_zip_path(&base, &item.href));
        }
    }

    let mut seen = BTreeSet::new();
    for candidate in candidates {
        if !seen.insert(candidate.clone()) {
            continue;
        }
        if !image_name(&candidate) {
            continue;
        }
        let mut entry = match archive.by_name(&candidate) {
            Ok(entry) => entry,
            Err(_) => continue,
        };
        if entry.size() == 0 || entry.size() > 12 * 1024 * 1024 {
            continue;
        }
        let mut bytes = Vec::new();
        entry.read_to_end(&mut bytes)?;
        let ext = Path::new(&candidate)
            .extension()
            .and_then(|v| v.to_str())
            .unwrap_or("jpg");
        std::fs::create_dir_all(generated_dir)?;
        let out = generated_dir.join(format!("epub-cover-{work_id}.{ext}"));
        std::fs::write(&out, bytes)?;
        return Ok(Some(out));
    }
    Ok(None)
}

fn epub_opf_name(archive: &mut ZipArchive<File>) -> Result<String> {
    if let Ok(container) = read_zip_text(archive, "META-INF/container.xml") {
        if let Some(path) = first_xml_attr(&container, "rootfile", "full-path") {
            return Ok(path);
        }
    }
    for i in 0..archive.len() {
        let entry = archive
            .by_index(i)
            .map_err(|e| AppError::Other(e.to_string()))?;
        if entry.name().ends_with(".opf") {
            return Ok(entry.name().to_string());
        }
    }
    Err(AppError::NotFound(
        "EPUB OPF package file not found".to_string(),
    ))
}

fn read_zip_text(archive: &mut ZipArchive<File>, name: &str) -> Result<String> {
    let mut entry = archive
        .by_name(name)
        .map_err(|e| AppError::NotFound(format!("EPUB entry {name} not found: {e}")))?;
    let mut text = String::new();
    entry.read_to_string(&mut text)?;
    Ok(text)
}

fn parse_epub_manifest_items(opf: &str) -> Vec<EpubManifestItem> {
    let item_re = Regex::new(r#"(?is)<item\s+[^>]+>"#).unwrap();
    item_re
        .find_iter(opf)
        .filter_map(|item| {
            let tag = item.as_str();
            Some(EpubManifestItem {
                id: attr_value(tag, "id")?,
                href: attr_value(tag, "href")?,
                media_type: attr_value(tag, "media-type").unwrap_or_default(),
                properties: attr_value(tag, "properties").unwrap_or_default(),
            })
        })
        .collect()
}

fn capture_meta_name_content(xml: &str, name: &str) -> Option<String> {
    let meta_re = Regex::new(r#"(?is)<meta\s+[^>]+>"#).ok()?;
    let result = meta_re.find_iter(xml).find_map(|tag| {
        let tag = tag.as_str();
        (attr_value(tag, "name").as_deref() == Some(name))
            .then(|| attr_value(tag, "content"))
            .flatten()
    });
    result
}

fn first_xml_attr(xml: &str, tag_name: &str, attr_name: &str) -> Option<String> {
    let tag_re = Regex::new(&format!(r#"(?is)<{}\s+[^>]+>"#, regex::escape(tag_name))).ok()?;
    tag_re
        .find(xml)
        .and_then(|tag| attr_value(tag.as_str(), attr_name))
}

fn attr_value(tag: &str, attr_name: &str) -> Option<String> {
    let attr_re =
        Regex::new(r#"(?is)([A-Za-z_:][-A-Za-z0-9_:.]*)\s*=\s*("([^"]*)"|'([^']*)')"#).ok()?;
    let result = attr_re.captures_iter(tag).find_map(|caps| {
        let name = caps.get(1)?.as_str();
        if name.eq_ignore_ascii_case(attr_name) {
            caps.get(3)
                .or_else(|| caps.get(4))
                .map(|value| html_escape::decode_html_entities(value.as_str()).to_string())
        } else {
            None
        }
    });
    result
}

fn join_zip_path(base: &str, href: &str) -> String {
    let href = href
        .split(|ch| ch == '?' || ch == '#')
        .next()
        .unwrap_or(href)
        .replace('\\', "/")
        .trim_start_matches('/')
        .to_string();
    let combined = if base.is_empty() {
        href
    } else {
        format!("{base}/{href}")
    };
    let mut parts = Vec::new();
    for part in combined.split('/') {
        match part {
            "" | "." => {}
            ".." => {
                parts.pop();
            }
            other => parts.push(other),
        }
    }
    parts.join("/")
}

fn zip_parent(path: &str) -> String {
    path.rsplit_once('/')
        .map(|(parent, _)| parent.to_string())
        .unwrap_or_default()
}

fn capture_xml(xml: &str, tag: &str) -> Option<String> {
    let re = Regex::new(&format!(r#"(?s)<{tag}[^>]*>(.*?)</{tag}>"#)).ok()?;
    re.captures(xml)
        .and_then(|c| c.get(1))
        .map(|m| html_escape::decode_html_entities(m.as_str().trim()).to_string())
}

fn capture_all_xml(xml: &str, tag: &str) -> Vec<String> {
    let re = Regex::new(&format!(r#"(?s)<{tag}[^>]*>(.*?)</{tag}>"#)).unwrap();
    re.captures_iter(xml)
        .filter_map(|c| c.get(1))
        .map(|m| html_escape::decode_html_entities(m.as_str().trim()).to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

fn capture_meta_property(xml: &str, property: &str) -> Option<String> {
    let re = Regex::new(&format!(
        r#"(?s)<meta[^>]+property=["']{}["'][^>]*>(.*?)</meta>"#,
        regex::escape(property)
    ))
    .ok()?;
    re.captures(xml)
        .and_then(|c| c.get(1))
        .map(|m| html_escape::decode_html_entities(m.as_str().trim()).to_string())
}

fn capture_meta_refine_property(xml: &str, property: &str) -> Option<String> {
    capture_meta_property(xml, property)
}

#[derive(Debug, Clone)]
pub struct ParsedTag {
    pub namespace: String,
    pub key: String,
    pub label: String,
}

pub fn parse_comic_genre_tags(genre: &str) -> Vec<ParsedTag> {
    genre
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|raw| {
            let (namespace, label) = if let Some(rest) = raw.strip_prefix("f:") {
                ("female", rest)
            } else if let Some(rest) = raw.strip_prefix("m:") {
                ("male", rest)
            } else if let Some(rest) = raw.strip_prefix("x:") {
                ("mixed", rest)
            } else if let Some((ns, rest)) = raw.split_once(':') {
                (ns, rest)
            } else {
                ("other", raw)
            };
            ParsedTag {
                namespace: namespace.to_string(),
                key: normalize_key(label),
                label: label.to_string(),
            }
        })
        .collect()
}

fn infer_audio_title(root: &Path, rj: &str) -> String {
    root.file_name()
        .and_then(|v| v.to_str())
        .filter(|v| !v.eq_ignore_ascii_case(rj))
        .map(clean_title)
        .unwrap_or_else(|| rj.to_string())
}

fn infer_audio_variant(path: &Path) -> String {
    path.parent()
        .and_then(|p| p.file_name())
        .and_then(|v| v.to_str())
        .unwrap_or("audio")
        .to_string()
}

fn common_rj_root(audio_dir: &Path, rj: &str, files: &[PathBuf]) -> PathBuf {
    let rj_root = audio_dir.join(rj);
    if let Ok(children) = std::fs::read_dir(&rj_root) {
        let first_dir = children
            .filter_map(|entry| entry.ok())
            .map(|entry| entry.path())
            .find(|path| path.is_dir());
        if let Some(path) = first_dir {
            return path;
        }
    }
    files
        .first()
        .and_then(|file| file.parent())
        .map(Path::to_path_buf)
        .unwrap_or(rj_root)
}

fn read_first_text_summary(files: &[PathBuf]) -> Option<String> {
    let txt = files.iter().find(|p| extension_is(p, &["txt"]))?;
    let raw = std::fs::read_to_string(txt).ok()?;
    Some(raw.chars().take(1600).collect())
}

fn read_audio_metadata(path: &Path, fallback_title: &str, quality: &str) -> serde_json::Value {
    let tagged_file = Probe::open(path).and_then(|probe| probe.read()).ok();
    let Some(tagged_file) = tagged_file else {
        return json!({ "title": fallback_title, "quality": quality });
    };
    let properties = tagged_file.properties();
    let tag = tagged_file
        .primary_tag()
        .or_else(|| tagged_file.first_tag());
    json!({
        "title": tag.and_then(|tag| tag.title().map(|value| value.to_string())).unwrap_or_else(|| fallback_title.to_string()),
        "artist": tag.and_then(|tag| tag.artist().map(|value| value.to_string())),
        "album": tag.and_then(|tag| tag.album().map(|value| value.to_string())),
        "genre": tag.and_then(|tag| tag.genre().map(|value| value.to_string())),
        "track": tag.and_then(|tag| tag.track()),
        "quality": quality,
        "duration_seconds": properties.duration().as_secs_f64(),
        "audio_bitrate": properties.audio_bitrate(),
        "sample_rate": properties.sample_rate(),
        "channels": properties.channels(),
    })
}

fn audio_cover_candidate(files: &[PathBuf], root: &Path) -> Option<PathBuf> {
    let images = files
        .iter()
        .filter(|p| extension_is(p, &["jpg", "jpeg", "png", "webp"]))
        .collect::<Vec<_>>();
    images
        .iter()
        .copied()
        .find(|file| {
            let file_name = file
                .file_name()
                .and_then(|v| v.to_str())
                .unwrap_or_default()
                .to_ascii_lowercase();
            file_name.contains("cover")
                || file_name.contains("jacket")
                || file_name.contains("thumb")
                || file_name.contains("folder")
                || file_name.contains("ジャケット")
        })
        .or_else(|| images.first().copied())
        .cloned()
        .or_else(|| find_audio_cover_near_root(root))
}

fn find_audio_cover_near_root(root: &Path) -> Option<PathBuf> {
    root.ancestors().take(3).find_map(find_cover_file)
}

fn find_cover_file(dir: &Path) -> Option<PathBuf> {
    for name in [
        "thumb.jpg",
        "thumb.webp",
        "thumb.png",
        "cover.jpg",
        "cover.webp",
        "cover.png",
    ] {
        let path = dir.join(name);
        if path.exists() {
            return Some(path);
        }
    }
    None
}

fn extension_is(path: &Path, extensions: &[&str]) -> bool {
    path.extension()
        .and_then(|v| v.to_str())
        .map(|ext| {
            extensions
                .iter()
                .any(|wanted| ext.eq_ignore_ascii_case(wanted))
        })
        .unwrap_or(false)
}

pub(crate) fn image_name(name: &str) -> bool {
    let lower = name.to_ascii_lowercase();
    [".jpg", ".jpeg", ".png", ".webp", ".gif", ".avif"]
        .iter()
        .any(|ext| lower.ends_with(ext))
}

fn gallery_image_name(path: &Path) -> bool {
    extension_is(path, &["jpg", "jpeg", "png", "webp", "gif", "avif", "bmp"])
}

fn gallery_filename_tags(path: &Path) -> Vec<String> {
    let Some(stem) = path.file_stem().and_then(|value| value.to_str()) else {
        return Vec::new();
    };
    let mut tags = BTreeSet::new();
    let mut iter = stem.char_indices().peekable();
    while let Some((_, ch)) = iter.next() {
        if ch != '#' {
            continue;
        }
        let mut value = String::new();
        while let Some((_, next)) = iter.peek().copied() {
            if next == '#' || next.is_whitespace() || is_gallery_tag_separator(next) {
                break;
            }
            value.push(next);
            iter.next();
        }
        let tag = value
            .trim_matches(|c: char| c == '#' || c.is_whitespace())
            .trim_matches(|c: char| is_gallery_tag_separator(c) || matches!(c, '.' | '_' | '-'))
            .trim()
            .to_string();
        if !tag.is_empty() {
            tags.insert(tag);
        }
    }
    tags.into_iter().collect()
}

fn is_gallery_tag_separator(ch: char) -> bool {
    matches!(
        ch,
        ',' | '，' | ';' | '；' | '、' | '[' | ']' | '(' | ')' | '（' | '）'
    )
}

fn gallery_cover_candidate(files: &[PathBuf]) -> Option<&PathBuf> {
    files
        .iter()
        .find(|path| {
            path.file_name()
                .and_then(|value| value.to_str())
                .map(|name| {
                    let lower = name.to_ascii_lowercase();
                    lower.contains("cover")
                        || lower.contains("thumb")
                        || lower.contains("jacket")
                        || lower.contains("封面")
                })
                .unwrap_or(false)
        })
        .or_else(|| files.first())
}

pub(crate) fn naturalish_key(value: &str) -> String {
    NATURAL_NUMBER_RE
        .replace_all(value, |caps: &regex::Captures| format!("{:0>12}", &caps[0]))
        .to_string()
}

pub fn normalize_key(value: &str) -> String {
    value
        .trim()
        .to_ascii_lowercase()
        .replace('_', " ")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

fn normalize_track_key(value: &str) -> String {
    let value = Regex::new(r"(?i)\b(mp3|wav|flac|ogg|m4a|効果音なし|seなし|bonus|特典)\b")
        .unwrap()
        .replace_all(value, " ");
    normalize_key(&value)
}

fn clean_title(value: &str) -> String {
    value.trim().replace('\u{fffd}', "").replace("  ", " ")
}

fn path_string(path: &Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::sync::Arc;

    use crate::assets;
    use crate::config::Config;
    use crate::db::Db;
    use crate::AppState;

    #[test]
    fn parses_comic_prefix_tags() {
        let tags = parse_comic_genre_tags("m:bbm, f:ahegao, x:group, full color");
        assert_eq!(tags[0].namespace, "male");
        assert_eq!(tags[1].namespace, "female");
        assert_eq!(tags[2].namespace, "mixed");
        assert_eq!(tags[3].namespace, "other");
        assert_eq!(tags[1].key, "ahegao");
    }

    #[test]
    fn parses_gallery_filename_hash_tags() {
        let tags = gallery_filename_tags(Path::new("artist/set #black #white #girl.png"));
        assert_eq!(
            tags,
            vec!["black".to_string(), "girl".to_string(), "white".to_string()]
        );
    }

    #[test]
    fn finds_audio_cover_near_work_root() {
        let temp = tempfile::tempdir().unwrap();
        let work_root = temp.path().join("RJ123456");
        std::fs::create_dir_all(work_root.join("tracks")).unwrap();
        let track = work_root.join("tracks").join("01.mp3");
        let cover = work_root.join("thumb.jpg");
        std::fs::write(&track, b"audio").unwrap();
        std::fs::write(&cover, b"image").unwrap();

        assert_eq!(
            audio_cover_candidate(&[track], &work_root),
            Some(cover)
        );
    }

    #[tokio::test]
    async fn scans_coser_picture_zip_archives() {
        let temp = tempfile::tempdir().unwrap();
        let coser_root = temp.path().join("COS图");
        let coser_dir = coser_root.join("CoserA");
        std::fs::create_dir_all(&coser_dir).unwrap();
        let archive_path = coser_dir.join("set.zip");
        write_test_zip(&archive_path, &["2.jpg", "10.jpg", "1.jpg"]);

        let data_dir = temp.path().join("data");
        let generated_dir = temp.path().join("generated");
        let db_path = data_dir.join("library.sqlite");
        std::fs::create_dir_all(&data_dir).unwrap();
        std::fs::create_dir_all(&generated_dir).unwrap();
        let database_url = format!("sqlite://{}", path_string(&db_path));
        let db = Db::connect(&database_url).await.unwrap();
        db.migrate().await.unwrap();
        let state = AppState {
            config: Config {
                bind: "127.0.0.1:0".to_string(),
                database_url,
                data_dir,
                cover_cache_dir: temp.path().join("cover-cache"),
                comic_cover_cache_dir: temp.path().join("cover-cache").join("comic"),
                novel_cover_cache_dir: temp.path().join("cover-cache").join("novel"),
                audio_cover_cache_dir: temp.path().join("cover-cache").join("audio"),
                gallery_cover_cache_dir: temp.path().join("cover-cache").join("gallery"),
                coser_picture_cover_cache_dir: temp
                    .path()
                    .join("cover-cache")
                    .join("coser-picture"),
                comics_dir: temp.path().join("漫画"),
                novels_dir: temp.path().join("轻小说"),
                audio_dir: temp.path().join("音声"),
                gallery_dir: temp.path().join("图库"),
                coser_picture_dir: coser_root,
                generated_dir,
                app_admin_password: "admin".to_string(),
                lightnovel_api_bases: Vec::new(),
                lightnovel_access_token: None,
                enrichment_concurrency: 1,
                ehtt_url: String::new(),
                openai_api_key: None,
                openai_image_model: "gpt-image-2".to_string(),
                qmediasync_base_url: String::new(),
                session_secret: "test-secret".to_string(),
                enable_file_watcher: false,
                watch_debounce_seconds: 20,
            },
            db,
            http: reqwest::Client::new(),
            comic_page_cache: Arc::new(assets::ComicPageCache::default()),
        };

        let response = scan_all(&state, false).await.unwrap();
        assert_eq!(response.coser_picture, 1);

        let library = state.db.library().await.unwrap();
        let work = library
            .works
            .iter()
            .find(|work| work.kind == "coser-picture")
            .expect("coser-picture work");
        assert_eq!(work.title, "set");
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&work.meta_json)
                .unwrap()
                .get("page_count")
                .and_then(|value| value.as_i64()),
            Some(3)
        );
        assert!(work
            .tag_keys
            .as_deref()
            .unwrap_or_default()
            .contains("artist:cosera"));
        assert!(work
            .tag_keys
            .as_deref()
            .unwrap_or_default()
            .contains("coser-picture:image-set"));

        let detail = state.db.work_detail(work.id).await.unwrap();
        let archive = detail
            .assets
            .iter()
            .find(|asset| asset.role == "archive")
            .expect("archive asset");
        assert_eq!(archive.mime, "application/zip");
        assert_eq!(archive.variant.as_deref(), Some("zip"));
    }

    #[tokio::test]
    async fn scans_qmediasync_plain_strm_comics() {
        let temp = tempfile::tempdir().unwrap();
        let qms_root = temp.path().join("qms");
        let work_dir = qms_root.join("Remote Book");
        std::fs::create_dir_all(&work_dir).unwrap();
        std::fs::write(work_dir.join("book.strm"), "https://example.test/book.cbz\n").unwrap();
        std::fs::write(
            work_dir.join("ComicInfo.xml"),
            "<ComicInfo><Series>Remote Book</Series><PageCount>12</PageCount><Penciller>Artist A</Penciller></ComicInfo>",
        )
        .unwrap();
        std::fs::write(work_dir.join("thumb.jpg"), b"cover").unwrap();

        let data_dir = temp.path().join("data");
        let generated_dir = temp.path().join("generated");
        let db_path = data_dir.join("library.sqlite");
        std::fs::create_dir_all(&data_dir).unwrap();
        std::fs::create_dir_all(&generated_dir).unwrap();
        let database_url = format!("sqlite://{}", path_string(&db_path));
        let db = Db::connect(&database_url).await.unwrap();
        db.migrate().await.unwrap();
        let config = Config {
            bind: "127.0.0.1:0".to_string(),
            database_url,
            data_dir,
            cover_cache_dir: temp.path().join("cover-cache"),
            comic_cover_cache_dir: temp.path().join("cover-cache").join("comic"),
            novel_cover_cache_dir: temp.path().join("cover-cache").join("novel"),
            audio_cover_cache_dir: temp.path().join("cover-cache").join("audio"),
            gallery_cover_cache_dir: temp.path().join("cover-cache").join("gallery"),
            coser_picture_cover_cache_dir: temp
                .path()
                .join("cover-cache")
                .join("coser-picture"),
            comics_dir: temp.path().join("comics"),
            novels_dir: temp.path().join("novels"),
            audio_dir: temp.path().join("audio"),
            gallery_dir: temp.path().join("gallery"),
            coser_picture_dir: temp.path().join("coser-picture"),
            generated_dir,
            app_admin_password: "admin".to_string(),
            lightnovel_api_bases: Vec::new(),
            lightnovel_access_token: None,
            enrichment_concurrency: 1,
            ehtt_url: String::new(),
            openai_api_key: None,
            openai_image_model: "gpt-image-2".to_string(),
            qmediasync_base_url: String::new(),
            session_secret: "test-secret".to_string(),
            enable_file_watcher: false,
            watch_debounce_seconds: 20,
        };
        let mut app_settings = settings::AppSettings::defaults(&config);
        app_settings.qmediasync.enabled = true;
        app_settings
            .qmediasync
            .strm_roots
            .push(path_string(&qms_root));
        let state = AppState {
            config,
            db,
            http: reqwest::Client::new(),
            comic_page_cache: Arc::new(assets::ComicPageCache::default()),
        };

        let count = scan_qmediasync_comics(&state, &app_settings).await.unwrap();
        assert_eq!(count, 1);

        let library = state.db.library().await.unwrap();
        let work = library
            .works
            .iter()
            .find(|work| work.kind == "comic")
            .expect("comic work");
        assert_eq!(work.title, "Remote Book");
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&work.meta_json)
                .unwrap()
                .get("page_count")
                .and_then(|value| value.as_i64()),
            Some(12)
        );
        assert!(work
            .tag_keys
            .as_deref()
            .unwrap_or_default()
            .contains("artist:artist a"));

        let detail = state.db.work_detail(work.id).await.unwrap();
        let archive = detail
            .assets
            .iter()
            .find(|asset| asset.role == "archive")
            .expect("archive asset");
        assert_eq!(archive.path, "qms-strm://qms/Remote Book/book.strm");
        assert_eq!(archive.mime, "application/vnd.comicbook+zip");
        assert_eq!(archive.variant.as_deref(), Some("cbz-strm"));
        assert!(detail.assets.iter().any(|asset| asset.role == "cover"));
    }

    fn write_test_zip(path: &Path, entries: &[&str]) {
        let file = File::create(path).unwrap();
        let mut zip = zip::ZipWriter::new(file);
        let options = zip::write::SimpleFileOptions::default();
        for entry in entries {
            zip.start_file(entry, options).unwrap();
            zip.write_all(b"test-image-bytes").unwrap();
        }
        zip.finish().unwrap();
    }
}
