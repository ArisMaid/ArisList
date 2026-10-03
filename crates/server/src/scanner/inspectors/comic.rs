//! Bounded, database-free comic archive inspection.
//!
//! The inspector is intentionally a pure candidate until the comic kind is
//! switched in `catalog_kind_ownership`.  It reads one relative work key,
//! applies the same ZIP safety/page-count and ComicInfo helpers as the legacy
//! scanner, and returns one fenced `WorkMutation` without allocating IDs or
//! writing catalog tables.

#![allow(dead_code)]

use std::collections::BTreeSet;
use std::path::{Component, Path, PathBuf};

use serde_json::json;

use super::super::{
    count_cbz_pages_blocking, fingerprint_archive, local_comic_archive_type, path_string,
    read_comic_info_blocking,
};
use crate::catalog_writer::{
    AssetMutation, MutationFence, MutationOwner, MutationSource, TagMutation, WorkMutation,
    WorkMutationFields,
};
use crate::error::{AppError, Result};
use crate::resource::ResourceGovernor;
use crate::vfs;

use super::super::comic_info::COMIC_FINGERPRINT_PREFIX;

#[derive(Debug, Clone)]
pub(crate) struct ComicInspectionRequest {
    pub root_id: i64,
    pub root_generation: i64,
    pub scan_token: String,
    pub provider: String,
    pub qmediasync_mount_name: Option<String>,
    pub root: PathBuf,
    pub work_key: String,
    pub previous_work_key: Option<String>,
}

pub(crate) async fn inspect(
    resources: &ResourceGovernor,
    request: ComicInspectionRequest,
) -> Result<WorkMutation> {
    validate_request(&request)?;
    let relative = Path::new(&request.work_key);
    let archive_path = request.root.join(relative);
    if !archive_path.is_file() {
        return Err(AppError::NotFound(format!(
            "comic inventory work key is not a readable archive: {}",
            request.work_key
        )));
    }

    let dir = archive_path
        .parent()
        .unwrap_or(request.root.as_path())
        .to_path_buf();
    let archive_state = fingerprint_archive(resources, archive_path.clone(), true, true).await?;
    let is_qmediasync = vfs::is_qmediasync_provider(&request.provider);
    let archive_source_version = archive_state
        .fingerprint
        .source_version(&archive_path)
        .ok_or_else(|| {
            AppError::Other(format!(
                "comic fingerprint did not retain source metadata for {}",
                request.work_key
            ))
        })?
        .to_string();
    let comic_info_read = read_comic_info_blocking(resources, dir.clone()).await?;
    let preserve_scanner_tags = comic_info_read.is_missing();
    let comic_info = comic_info_read.into_info();
    let page_count = if is_qmediasync {
        request.qmediasync_mount_name.as_deref().ok_or_else(|| {
            AppError::BadRequest("qmediasync comic inspection requires a mount name".to_string())
        })?;
        // The local STRM stub does not contain the remote archive.  Trust an
        // explicitly supplied ComicInfo page count when present, otherwise
        // leave it unknown instead of downloading the archive during scan.
        comic_info.page_count.unwrap_or(0)
    } else {
        let archive_page_count = count_cbz_pages_blocking(resources, archive_path.clone()).await?;
        comic_info.page_count.unwrap_or(archive_page_count)
    };
    let title = comic_info.display_title(&archive_state.fallback_title);
    let (archive_mime, archive_variant) = if is_qmediasync {
        ("application/vnd.comicbook+zip", "cbz-strm")
    } else {
        local_comic_archive_type(&archive_path)
    };
    let archive_path_reference = if is_qmediasync {
        let mount_name = request
            .qmediasync_mount_name
            .as_deref()
            .expect("validated qmediasync mount name");
        let target_url =
            super::super::read_qms_strm_url_blocking(resources, archive_path.clone()).await?;
        let volume_paths = super::super::qms_volume_paths(&request.root, mount_name, &archive_path);
        let missing_volumes = super::super::qms_volume_missing_names(&archive_path);
        let meta = vfs::qms_strm_meta_json_with_volumes(
            mount_name,
            &request.root,
            &archive_path,
            &request.work_key,
            &target_url,
            &volume_paths,
            &missing_volumes,
        )
        .await;
        Some((vfs::qms_strm_uri(mount_name, &request.work_key), meta))
    } else {
        None
    };
    let archive_reference = archive_path_reference
        .as_ref()
        .map(|(path, _)| path.clone())
        .unwrap_or_else(|| path_string(&archive_path));
    let archive_meta = archive_path_reference
        .as_ref()
        .map(|(_, meta)| meta.clone())
        .unwrap_or_else(|| json!({ "page_count": page_count }));

    let mut assets = vec![AssetMutation {
        path: archive_reference.clone(),
        mime: archive_mime.to_string(),
        role: "archive".to_string(),
        variant: Some(archive_variant.to_string()),
        position: None,
        size: archive_state.fingerprint.size(&archive_path),
        source_version: archive_source_version,
        meta: archive_state
            .fingerprint
            .asset_meta(&archive_path, archive_meta),
    }];
    if let Some(cover) = archive_state.cover {
        let cover_source_version = archive_state
            .fingerprint
            .source_version(&cover)
            .ok_or_else(|| {
                AppError::Other(format!(
                    "comic fingerprint lost cover metadata for {}",
                    cover.display()
                ))
            })?
            .to_string();
        assets.push(AssetMutation {
            mime: mime_guess::from_path(&cover)
                .first_or_octet_stream()
                .to_string(),
            path: path_string(&cover),
            role: "cover".to_string(),
            variant: None,
            position: None,
            size: archive_state.fingerprint.size(&cover),
            source_version: cover_source_version,
            meta: archive_state.fingerprint.asset_meta(&cover, json!({})),
        });
    }

    let mut tags = Vec::new();
    let mut identities = BTreeSet::new();
    for tag in comic_info.tags() {
        push_tag(
            &mut tags,
            &mut identities,
            &tag.namespace,
            &tag.key,
            &tag.label,
        );
    }

    Ok(WorkMutation {
        source: MutationSource {
            kind: "comic".to_string(),
            root_id: request.root_id,
            work_key: request.work_key,
            provider: request.provider,
        },
        previous_work_key: request.previous_work_key,
        fence: MutationFence {
            root_generation: request.root_generation,
            scan_token: request.scan_token,
            complete_snapshot: true,
            preserve_scanner_tags,
        },
        fingerprint: format!(
            "{COMIC_FINGERPRINT_PREFIX}{}",
            archive_state.fingerprint.value
        ),
        work: WorkMutationFields {
            title,
            subtitle: comic_info.subtitle(),
            category: Some("Doujinshi".to_string()),
            description: comic_info.description(),
            rating: comic_info.community_rating,
            source_path: archive_reference,
            meta: comic_info.meta_with_sidecar_status(
                page_count,
                if preserve_scanner_tags {
                    "missing"
                } else {
                    "present"
                },
            ),
        },
        assets,
        tags,
        external_ids: Vec::new(),
    })
}

fn validate_request(request: &ComicInspectionRequest) -> Result<()> {
    if request.root_id <= 0 || request.root_generation < 0 {
        return Err(AppError::BadRequest(
            "comic inspection root fence is invalid".to_string(),
        ));
    }
    for (label, value) in [
        ("scan token", request.scan_token.as_str()),
        ("provider", request.provider.as_str()),
        ("work key", request.work_key.as_str()),
    ] {
        if value.trim().is_empty() {
            return Err(AppError::BadRequest(format!(
                "comic inspection {label} must not be empty"
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
            "comic inspection work key must remain below its root".to_string(),
        ));
    }
    let extension = relative
        .extension()
        .and_then(|value| value.to_str())
        .unwrap_or_default();
    let is_qmediasync = vfs::is_qmediasync_provider(&request.provider);
    if is_qmediasync {
        if !extension.eq_ignore_ascii_case("strm") {
            return Err(AppError::BadRequest(
                "qmediasync comic inspection work key must use STRM".to_string(),
            ));
        }
        let mount_name = request
            .qmediasync_mount_name
            .as_deref()
            .and_then(|value| (!value.trim().is_empty()).then_some(value));
        if mount_name.is_none() {
            return Err(AppError::BadRequest(
                "qmediasync comic inspection mount name is missing".to_string(),
            ));
        }
        return Ok(());
    }
    if !extension.eq_ignore_ascii_case("cbz") && !extension.eq_ignore_ascii_case("zip") {
        return Err(AppError::BadRequest(
            "comic inspection work key must use CBZ or ZIP".to_string(),
        ));
    }
    Ok(())
}

fn push_tag(
    tags: &mut Vec<TagMutation>,
    identities: &mut BTreeSet<(String, String)>,
    namespace: &str,
    key: &str,
    label: &str,
) {
    let label = label.trim();
    let key = key.trim();
    if label.is_empty()
        || key.is_empty()
        || !identities.insert((namespace.to_string(), key.to_string()))
    {
        return;
    }
    tags.push(TagMutation {
        namespace: namespace.to_string(),
        key: key.to_string(),
        label: label.to_string(),
        translated_label: None,
        translated_namespace: None,
        source: "comic-info".to_string(),
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
    use crate::resource::ResourceLimits;

    fn write_archive(path: &Path, comic_info: &str) {
        let file = File::create(path).unwrap();
        let mut archive = zip::ZipWriter::new(file);
        let options = zip::write::SimpleFileOptions::default();
        archive.start_file("ComicInfo.xml", options).unwrap();
        archive.write_all(comic_info.as_bytes()).unwrap();
        archive.start_file("001.jpg", options).unwrap();
        archive.write_all(&[0xff, 0xd8, 0xff, 0xd9]).unwrap();
        archive.start_file("002.png", options).unwrap();
        archive.write_all(&[0x89, b'P', b'N', b'G']).unwrap();
        archive.finish().unwrap();
    }

    fn request(root: &Path, name: &str) -> ComicInspectionRequest {
        ComicInspectionRequest {
            root_id: 1,
            root_generation: 3,
            scan_token: "comic-scan".to_string(),
            provider: "local".to_string(),
            qmediasync_mount_name: None,
            root: root.to_path_buf(),
            work_key: name.to_string(),
            previous_work_key: None,
        }
    }

    #[tokio::test]
    async fn cbz_and_zip_emit_equivalent_bounded_mutations() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("comics");
        let book_dir = root.join("Author");
        std::fs::create_dir_all(&book_dir).unwrap();
        std::fs::write(book_dir.join("cover.jpg"), [1_u8, 2, 3]).unwrap();
        let info = r#"<ComicInfo><Series>  Fixture Series  </Series><AlternateSeries>Alt</AlternateSeries><Writer>Circle</Writer><Penciller>Artist</Penciller><Genre>f:Romance, m:Action, f:Romance</Genre><PageCount>9</PageCount><LanguageIso>pt-BR</LanguageIso><CommunityRating>4.5</CommunityRating></ComicInfo>"#;
        std::fs::write(book_dir.join("ComicInfo.xml"), info).unwrap();
        write_archive(&book_dir.join("book.cbz"), info);
        write_archive(&book_dir.join("book.zip"), info);
        let resources = ResourceGovernor::new(ResourceLimits::nas_n100_4g());
        let cbz = inspect(&resources, request(&root, "Author/book.cbz"))
            .await
            .unwrap();
        let zip = inspect(&resources, request(&root, "Author/book.zip"))
            .await
            .unwrap();
        assert_eq!(cbz.work.title, "Alt");
        assert_eq!(cbz.work.subtitle.as_deref(), Some("Fixture Series"));
        assert_eq!(cbz.work.description, None);
        assert_eq!(cbz.work.rating, Some(4.5));
        assert_eq!(cbz.work.meta["page_count"], 9);
        assert_eq!(cbz.assets.len(), 2);
        assert_eq!(cbz.tags.len(), 5);
        assert!(cbz.tags.iter().any(|tag| {
            tag.namespace == "language" && tag.key == "pt-BR" && tag.label == "pt-BR"
        }));
        assert_eq!(zip.work.title, cbz.work.title);
        assert_eq!(zip.work.meta["page_count"], cbz.work.meta["page_count"]);
        assert_eq!(zip.assets.len(), cbz.assets.len());
        assert_eq!(
            cbz.assets
                .iter()
                .map(|asset| &asset.role)
                .collect::<Vec<_>>(),
            zip.assets
                .iter()
                .map(|asset| &asset.role)
                .collect::<Vec<_>>()
        );
        assert_eq!(
            cbz.fingerprint.len(),
            COMIC_FINGERPRINT_PREFIX.len()
                + super::super::super::comic_info::METADATA_VERSION.len()
                + 1
                + 64
        );
    }

    #[tokio::test]
    async fn qmediasync_strm_uses_remote_asset_identity_without_archive_read() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("qms");
        let book_dir = root.join("Author");
        std::fs::create_dir_all(&book_dir).unwrap();
        std::fs::write(
            book_dir.join("ComicInfo.xml"),
            r#"<ComicInfo><Series>Remote Book</Series><PageCount>42</PageCount></ComicInfo>"#,
        )
        .unwrap();
        std::fs::write(book_dir.join("cover.jpg"), [1_u8, 2, 3]).unwrap();
        std::fs::write(
            book_dir.join("book.cbz.strm"),
            "https://qmediasync.example/library/book.cbz\n",
        )
        .unwrap();
        let mut request = request(&root, "Author/book.cbz.strm");
        request.provider = "qmediasync:qms".to_string();
        request.qmediasync_mount_name = Some("qms".to_string());
        let resources = ResourceGovernor::new(ResourceLimits::nas_n100_4g());

        let mutation = inspect(&resources, request).await.unwrap();

        assert_eq!(mutation.work.title, "Remote Book");
        assert_eq!(mutation.work.meta["page_count"], 42);
        assert_eq!(
            mutation.work.source_path,
            "qms-strm://qms/Author/book.cbz.strm"
        );
        assert_eq!(
            mutation.assets[0].path,
            "qms-strm://qms/Author/book.cbz.strm"
        );
        assert_eq!(mutation.assets[0].variant.as_deref(), Some("cbz-strm"));
        assert_eq!(mutation.assets[0].meta["provider"], "qmediasync");
        assert_eq!(mutation.assets.len(), 2);
    }

    #[tokio::test]
    async fn invalid_archive_and_escape_keys_are_rejected_before_io() {
        let temp = tempfile::tempdir().unwrap();
        let resources = ResourceGovernor::new(ResourceLimits::nas_n100_4g());
        let mut escape = request(temp.path(), "../outside.cbz");
        let error = inspect(&resources, escape).await.unwrap_err().to_string();
        assert!(error.contains("remain below its root"));

        escape = request(temp.path(), "not-a-comic.epub");
        let error = inspect(&resources, escape).await.unwrap_err().to_string();
        assert!(error.contains("CBZ or ZIP"));
    }

    #[tokio::test]
    async fn malformed_zip_preserves_a_safe_inspector_error() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("comics");
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(root.join("broken.cbz"), b"not a zip").unwrap();
        let resources = ResourceGovernor::new(ResourceLimits::nas_n100_4g());
        let error = inspect(&resources, request(&root, "broken.cbz"))
            .await
            .unwrap_err()
            .to_string();
        assert!(!error.is_empty());
        assert!(!error.contains("panicked"));
    }
}
