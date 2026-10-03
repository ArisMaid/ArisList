//! Pure database-side novel inspection.
//!
//! The inspector reads one inventory work key and returns a bounded
//! WorkMutation; it never allocates database IDs or writes catalog tables.
//! Extracted covers are content-addressed and atomically published so retries
//! converge on the same derivative file before the fenced writer commits it.

#![allow(dead_code)]

use std::collections::BTreeSet;
use std::path::{Component, Path, PathBuf};

use serde_json::json;

use super::super::{
    clean_title, extract_epub_cover_content_addressed_blocking, fingerprint_paths, normalize_key,
    path_string, read_epub_metadata_blocking,
};
use crate::catalog_writer::{
    AssetMutation, MutationFence, MutationOwner, MutationSource, TagMutation, WorkMutation,
    WorkMutationFields,
};
use crate::error::{AppError, Result};
use crate::resource::ResourceGovernor;

const NOVEL_INSPECTOR_VERSION: &str = "novel-v1";

#[derive(Debug, Clone)]
pub(crate) struct NovelInspectionRequest {
    pub root_id: i64,
    pub root_generation: i64,
    pub scan_token: String,
    pub provider: String,
    pub root: PathBuf,
    pub work_key: String,
    pub previous_work_key: Option<String>,
    pub generated_dir: PathBuf,
}

pub(crate) async fn inspect(
    resources: &ResourceGovernor,
    request: NovelInspectionRequest,
) -> Result<WorkMutation> {
    validate_request(&request)?;
    let relative = Path::new(&request.work_key);
    let epub_path = request.root.join(relative);
    if !epub_path.is_file() {
        return Err(AppError::NotFound(format!(
            "novel inventory work key is not a readable file: {}",
            request.work_key
        )));
    }
    if !epub_path
        .extension()
        .and_then(|value| value.to_str())
        .is_some_and(|extension| extension.eq_ignore_ascii_case("epub"))
    {
        return Err(AppError::BadRequest(format!(
            "novel inventory work key is not an EPUB: {}",
            request.work_key
        )));
    }

    let fingerprint = fingerprint_paths(resources, vec![epub_path.clone()]).await?;
    let source_version = fingerprint
        .source_version(&epub_path)
        .ok_or_else(|| {
            AppError::Other(format!(
                "novel fingerprint did not retain source metadata for {}",
                request.work_key
            ))
        })?
        .to_string();
    let fingerprint_value = format!("{NOVEL_INSPECTOR_VERSION}:{}", fingerprint.value);
    let metadata = read_epub_metadata_blocking(resources, epub_path.clone()).await?;
    tokio::fs::create_dir_all(&request.generated_dir).await?;
    let extracted_cover = extract_epub_cover_content_addressed_blocking(
        resources,
        epub_path.clone(),
        request.generated_dir.clone(),
    )
    .await?;

    let title = metadata.title.clone().unwrap_or_else(|| {
        epub_path
            .file_stem()
            .and_then(|value| value.to_str())
            .unwrap_or("Untitled novel")
            .to_string()
    });
    let title = clean_title(&title);
    let title = if title.is_empty() {
        "Untitled novel".to_string()
    } else {
        title
    };
    let series = metadata.series.clone().or_else(|| {
        epub_path
            .parent()
            .and_then(|parent| parent.file_name())
            .and_then(|value| value.to_str())
            .map(str::to_string)
    });

    let mut assets = vec![AssetMutation {
        path: path_string(&epub_path),
        mime: "application/epub+zip".to_string(),
        role: "book".to_string(),
        variant: Some("epub".to_string()),
        position: None,
        size: fingerprint.size(&epub_path),
        source_version,
        meta: json!({}),
    }];
    if let Some(cover) = extracted_cover {
        assets.push(AssetMutation {
            mime: mime_guess::from_path(&cover.path)
                .first_or_octet_stream()
                .to_string(),
            path: path_string(&cover.path),
            role: "cover".to_string(),
            variant: Some("epub-extracted".to_string()),
            position: None,
            size: cover.size,
            source_version: cover.source_version,
            meta: json!({ "source": "epub" }),
        });
    }

    let mut tags = Vec::new();
    let mut identities = BTreeSet::new();
    if let Some(value) = series.as_deref() {
        push_tag(&mut tags, &mut identities, "series", value);
    }
    if let Some(value) = metadata.creator.as_deref() {
        push_tag(&mut tags, &mut identities, "artist", value);
    }
    for value in &metadata.subjects {
        push_tag(&mut tags, &mut identities, "ln", value);
    }
    if let Some(value) = metadata.language.as_deref() {
        push_tag(&mut tags, &mut identities, "language", value);
    }

    Ok(WorkMutation {
        source: MutationSource {
            kind: "novel".to_string(),
            root_id: request.root_id,
            work_key: request.work_key,
            provider: request.provider,
        },
        previous_work_key: request.previous_work_key,
        fence: MutationFence {
            root_generation: request.root_generation,
            scan_token: request.scan_token,
            complete_snapshot: true,
            preserve_scanner_tags: false,
        },
        fingerprint: fingerprint_value,
        work: WorkMutationFields {
            title,
            subtitle: None,
            category: Some("Light Novel".to_string()),
            description: metadata.description,
            rating: None,
            source_path: path_string(&epub_path),
            meta: json!({
                "creator": metadata.creator,
                "language": metadata.language,
                "series": series,
                "volume": metadata.volume,
                "source": metadata.source,
            }),
        },
        assets,
        tags,
        external_ids: Vec::new(),
    })
}

fn validate_request(request: &NovelInspectionRequest) -> Result<()> {
    if request.root_id <= 0 || request.root_generation < 0 {
        return Err(AppError::BadRequest(
            "novel inspection root fence is invalid".to_string(),
        ));
    }
    for (label, value) in [
        ("scan token", request.scan_token.as_str()),
        ("provider", request.provider.as_str()),
        ("work key", request.work_key.as_str()),
    ] {
        if value.trim().is_empty() {
            return Err(AppError::BadRequest(format!(
                "novel inspection {label} must not be empty"
            )));
        }
    }
    let relative = Path::new(&request.work_key);
    if relative.is_absolute()
        || relative.components().any(|component| {
            matches!(
                component,
                Component::ParentDir | Component::RootDir | Component::Prefix(_)
            )
        })
    {
        return Err(AppError::BadRequest(
            "novel inspection work key must remain below its root".to_string(),
        ));
    }
    Ok(())
}

fn push_tag(
    tags: &mut Vec<TagMutation>,
    identities: &mut BTreeSet<(String, String)>,
    namespace: &str,
    value: &str,
) {
    let label = value.trim();
    let key = normalize_key(label);
    if label.is_empty() || key.is_empty() {
        return;
    }
    if !identities.insert((namespace.to_string(), key.clone())) {
        return;
    }
    tags.push(TagMutation {
        namespace: namespace.to_string(),
        key,
        label: label.to_string(),
        translated_label: None,
        translated_namespace: None,
        source: "epub".to_string(),
        intro: None,
        links: None,
        owner: MutationOwner::Scanner,
    });
}

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::io::Write;

    use super::*;
    use crate::db::Db;
    use crate::resource::ResourceLimits;

    fn database_url(temp: &tempfile::TempDir) -> String {
        format!("sqlite://{}", temp.path().join("novel.sqlite").display())
    }

    fn write_epub(path: &Path) {
        let file = File::create(path).unwrap();
        let mut archive = zip::ZipWriter::new(file);
        let options = zip::write::SimpleFileOptions::default();
        archive
            .start_file("META-INF/container.xml", options)
            .unwrap();
        archive
            .write_all(
                br#"<?xml version="1.0"?><container><rootfiles><rootfile full-path="OPS/content.opf" media-type="application/oebps-package+xml"/></rootfiles></container>"#,
            )
            .unwrap();
        archive.start_file("OPS/content.opf", options).unwrap();
        archive
            .write_all(
                br#"<?xml version="1.0"?><package><metadata><dc:title>  Fixture Novel  </dc:title><dc:creator>Fixture Author</dc:creator><dc:description>Fixture Description</dc:description><dc:language>zh-CN</dc:language><dc:identifier>fixture-id</dc:identifier><dc:subject>Fantasy</dc:subject><dc:subject>Fantasy</dc:subject><meta property="belongs-to-collection">Fixture Series</meta><meta property="group-position">2</meta></metadata><manifest><item id="cover" href="cover.jpg" media-type="image/jpeg" properties="cover-image"/><item id="chapter" href="chapter.xhtml" media-type="application/xhtml+xml"/></manifest><spine><itemref idref="chapter"/></spine></package>"#,
            )
            .unwrap();
        archive.start_file("OPS/cover.jpg", options).unwrap();
        archive
            .write_all(&[0xff, 0xd8, 0xff, 0xe0, 1, 2, 3, 4, 0xff, 0xd9])
            .unwrap();
        archive.start_file("OPS/chapter.xhtml", options).unwrap();
        archive
            .write_all(b"<html><body>fixture</body></html>")
            .unwrap();
        archive.finish().unwrap();
    }

    #[tokio::test]
    async fn inspector_emits_a_bounded_complete_mutation_and_writer_commits_it() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("novels");
        let series = root.join("Fixture Series");
        std::fs::create_dir_all(&series).unwrap();
        write_epub(&series.join("book.epub"));
        let generated_dir = temp.path().join("generated");
        let db = Db::connect(&database_url(&temp)).await.unwrap();
        db.migrate().await.unwrap();
        let root_id = sqlx::query_scalar::<_, i64>(
            r#"
            INSERT INTO library_roots (
                kind, provider, root, generation, status, active_token
            )
            VALUES ('novel', 'local', ?1, 7, 'scanning', 'novel-scan')
            RETURNING id
            "#,
        )
        .bind(path_string(&root))
        .fetch_one(db.pool())
        .await
        .unwrap();
        sqlx::query(
            "UPDATE catalog_kind_ownership SET authoritative_writer = 'catalog-v2' WHERE kind = 'novel'",
        )
        .execute(db.pool())
        .await
        .unwrap();
        let resources = ResourceGovernor::new(ResourceLimits::nas_n100_4g());
        let request = NovelInspectionRequest {
            root_id,
            root_generation: 7,
            scan_token: "novel-scan".to_string(),
            provider: "local".to_string(),
            root: root.clone(),
            work_key: "Fixture Series/book.epub".to_string(),
            generated_dir: generated_dir.clone(),
            previous_work_key: None,
        };
        let first = inspect(&resources, request.clone()).await.unwrap();
        assert!(first.fence.complete_snapshot);
        assert_eq!(first.work.title, "Fixture Novel");
        assert_eq!(first.assets.len(), 2);
        assert_eq!(first.tags.len(), 4);
        assert!(first
            .assets
            .iter()
            .any(|asset| asset.role == "cover" && asset.size == Some(10)));
        let first_cover = first
            .assets
            .iter()
            .find(|asset| asset.role == "cover")
            .unwrap()
            .path
            .clone();
        assert!(Path::new(&first_cover).starts_with(&generated_dir));

        let second = inspect(&resources, request).await.unwrap();
        assert_eq!(
            second
                .assets
                .iter()
                .find(|asset| asset.role == "cover")
                .unwrap()
                .path,
            first_cover
        );
        assert_eq!(second.fingerprint, first.fingerprint);

        let committed = db.apply_catalog_work_mutation(first).await.unwrap();
        assert!(committed.changed);
        assert_eq!(committed.asset_ids.len(), 2);
        assert_eq!(
            sqlx::query_as::<_, (String, i64, i64)>(
                r#"
                SELECT work.title, stats.asset_count, stats.tag_count
                FROM works AS work
                JOIN work_stats AS stats ON stats.work_id = work.id
                WHERE work.id = ?1
                "#,
            )
            .bind(committed.work_id)
            .fetch_one(db.pool())
            .await
            .unwrap(),
            ("Fixture Novel".to_string(), 2, 4)
        );
    }

    #[tokio::test]
    async fn inspector_rejects_work_keys_that_escape_the_inventory_root() {
        let resources = ResourceGovernor::new(ResourceLimits::nas_n100_4g());
        let error = inspect(
            &resources,
            NovelInspectionRequest {
                root_id: 1,
                root_generation: 1,
                scan_token: "scan".to_string(),
                provider: "local".to_string(),
                root: PathBuf::from("/library/novels"),
                work_key: "../escape.epub".to_string(),
                previous_work_key: None,
                generated_dir: PathBuf::from("/generated"),
            },
        )
        .await
        .unwrap_err()
        .to_string();
        assert!(error.contains("remain below its root"));
    }
}
