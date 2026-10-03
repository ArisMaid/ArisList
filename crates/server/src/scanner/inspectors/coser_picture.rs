//! Bounded, database-free local CoserPicture ZIP inspection.
//!
//! One inventory work key maps to one ZIP archive and one complete typed
//! mutation. The inspector shares the legacy ZIP page counter and scanner
//! resource governor, but never allocates database IDs or writes catalog
//! tables directly.

#![allow(dead_code)]

use std::collections::BTreeSet;
use std::path::{Component, Path, PathBuf};

use serde_json::json;

use super::super::{
    count_cbz_pages_blocking, fingerprint_archive_with_sidecar_version, normalize_key, path_string,
    read_comic_info_blocking,
};
use crate::catalog_writer::{
    AssetMutation, MutationFence, MutationOwner, MutationSource, TagMutation, WorkMutation,
    WorkMutationFields,
};
use crate::error::{AppError, Result};
use crate::resource::ResourceGovernor;
use crate::vfs;

const COSER_PICTURE_INSPECTOR_VERSION: &str = "coser-picture-v2";

#[derive(Debug, Clone)]
pub(crate) struct CoserPictureInspectionRequest {
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
    request: CoserPictureInspectionRequest,
) -> Result<WorkMutation> {
    validate_request(&request)?;
    let relative = Path::new(&request.work_key);
    let archive_path = request.root.join(relative);
    if !archive_path.is_file() {
        return Err(AppError::NotFound(format!(
            "CoserPicture inventory work key is not a readable archive: {}",
            request.work_key
        )));
    }

    let is_qmediasync = vfs::is_qmediasync_provider(&request.provider);
    let archive_state = fingerprint_archive_with_sidecar_version(
        resources,
        archive_path.clone(),
        true,
        COSER_PICTURE_INSPECTOR_VERSION,
    )
    .await?;
    let fingerprint = archive_state.fingerprint;
    let source_version = fingerprint
        .source_version(&archive_path)
        .ok_or_else(|| {
            AppError::Other(format!(
                "CoserPicture fingerprint did not retain source metadata for {}",
                request.work_key
            ))
        })?
        .to_string();
    if is_qmediasync {
        request.qmediasync_mount_name.as_deref().ok_or_else(|| {
            AppError::BadRequest(
                "qmediasync CoserPicture inspection requires a mount name".to_string(),
            )
        })?;
    }
    let page_count = if is_qmediasync {
        0
    } else {
        count_cbz_pages_blocking(resources, archive_path.clone()).await?
    };
    if page_count <= 0 && !is_qmediasync {
        return Err(AppError::Other(format!(
            "CoserPicture archive contains no readable image pages: {}",
            request.work_key
        )));
    }

    let sidecar = read_comic_info_blocking(
        resources,
        archive_path
            .parent()
            .unwrap_or(request.root.as_path())
            .to_path_buf(),
    )
    .await?;
    let sidecar_missing = sidecar.is_missing();
    let comic_info = sidecar.into_info();
    let title = comic_info.display_title(&archive_state.fallback_title);
    let coser = archive_path
        .parent()
        .and_then(|path| path.file_name())
        .and_then(|value| value.to_str())
        .filter(|value| !value.trim().is_empty())
        .unwrap_or("CoserPicture")
        .to_string();

    let mut tags = Vec::with_capacity(3 + comic_info.tags().len());
    let mut identities = BTreeSet::new();
    push_tag(
        &mut tags,
        &mut identities,
        "coser-picture",
        "image-set",
        "CoserPicture",
    );
    let coser_key = normalize_key(&coser);
    push_tag(&mut tags, &mut identities, "folder", &coser_key, &coser);
    push_tag(&mut tags, &mut identities, "artist", &coser_key, &coser);
    for tag in comic_info.tags() {
        push_tag(
            &mut tags,
            &mut identities,
            &tag.namespace,
            &tag.key,
            &tag.label,
        );
    }

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
        (vfs::qms_strm_uri(mount_name, &request.work_key), meta)
    } else {
        (
            path_string(&archive_path),
            json!({
                "source": "coser-picture",
                "page_count": page_count
            }),
        )
    };

    Ok(WorkMutation {
        source: MutationSource {
            kind: "coser-picture".to_string(),
            root_id: request.root_id,
            work_key: request.work_key,
            provider: request.provider,
        },
        previous_work_key: request.previous_work_key,
        fence: MutationFence {
            root_generation: request.root_generation,
            scan_token: request.scan_token,
            complete_snapshot: true,
            preserve_scanner_tags: sidecar_missing,
        },
        fingerprint: fingerprint.value.clone(),
        work: WorkMutationFields {
            title,
            subtitle: None,
            category: Some("CoserPicture".to_string()),
            description: comic_info.description(),
            rating: comic_info.community_rating,
            source_path: archive_path_reference.0.clone(),
            meta: {
                let mut meta = comic_info.meta(page_count);
                meta["page_count"] = json!(page_count);
                meta["root"] = json!(path_string(&request.root));
                meta["archive"] = json!(path_string(&archive_path));
                meta["coser"] = json!(coser);
                if sidecar_missing {
                    meta["comic_info"]["sidecar_status"] = json!("missing");
                }
                meta
            },
        },
        assets: vec![AssetMutation {
            path: archive_path_reference.0,
            mime: "application/zip".to_string(),
            role: "archive".to_string(),
            variant: Some("zip".to_string()),
            position: None,
            size: fingerprint.size(&archive_path),
            source_version,
            meta: fingerprint.asset_meta(&archive_path, archive_path_reference.1),
        }],
        tags,
        external_ids: Vec::new(),
    })
}

fn validate_request(request: &CoserPictureInspectionRequest) -> Result<()> {
    if request.root_id <= 0 || request.root_generation < 0 {
        return Err(AppError::BadRequest(
            "CoserPicture inspection root fence is invalid".to_string(),
        ));
    }
    for (label, value) in [
        ("scan token", request.scan_token.as_str()),
        ("provider", request.provider.as_str()),
        ("work key", request.work_key.as_str()),
    ] {
        if value.trim().is_empty() {
            return Err(AppError::BadRequest(format!(
                "CoserPicture inspection {label} must not be empty"
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
            "CoserPicture inspection work key must remain below its root".to_string(),
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
                "qmediasync CoserPicture inspection work key must use STRM".to_string(),
            ));
        }
        if request
            .qmediasync_mount_name
            .as_deref()
            .map(|value| value.trim().is_empty())
            .unwrap_or(true)
        {
            return Err(AppError::BadRequest(
                "qmediasync CoserPicture inspection mount name is missing".to_string(),
            ));
        }
        return Ok(());
    }
    if !extension.eq_ignore_ascii_case("zip") {
        return Err(AppError::BadRequest(
            "CoserPicture inspection work key must use ZIP".to_string(),
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
        source: "coser-picture-zip".to_string(),
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

    fn write_archive(path: &Path) {
        let file = File::create(path).unwrap();
        let mut archive = zip::ZipWriter::new(file);
        let options = zip::write::SimpleFileOptions::default();
        archive.start_file("001.jpg", options).unwrap();
        archive.write_all(&[0xff, 0xd8, 0xff, 0xd9]).unwrap();
        archive.start_file("002.png", options).unwrap();
        archive.write_all(&[0x89, b'P', b'N', b'G']).unwrap();
        archive.finish().unwrap();
    }

    fn request(root: &Path, work_key: &str) -> CoserPictureInspectionRequest {
        CoserPictureInspectionRequest {
            root_id: 1,
            root_generation: 2,
            scan_token: "coser-picture-scan".to_string(),
            provider: "local".to_string(),
            qmediasync_mount_name: None,
            root: root.to_path_buf(),
            work_key: work_key.to_string(),
            previous_work_key: None,
        }
    }

    #[tokio::test]
    async fn inspector_emits_one_archive_and_bounded_legacy_equivalent_tags() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("coser-picture");
        let author = root.join("Alice");
        std::fs::create_dir_all(&author).unwrap();
        write_archive(&author.join("set.zip"));
        let resources = ResourceGovernor::new(ResourceLimits::nas_n100_4g());

        let mutation = inspect(&resources, request(&root, "Alice/set.zip"))
            .await
            .unwrap();

        assert_eq!(mutation.work.title, "set");
        assert_eq!(mutation.work.meta["page_count"], 2);
        assert_eq!(mutation.assets.len(), 1);
        assert_eq!(mutation.assets[0].role, "archive");
        assert_eq!(mutation.tags.len(), 3);
        assert_eq!(
            mutation
                .tags
                .iter()
                .map(|tag| tag.namespace.as_str())
                .collect::<Vec<_>>(),
            vec!["coser-picture", "folder", "artist"]
        );
        assert_eq!(mutation.fingerprint.len(), "coser-picture-v2:".len() + 64);
    }

    #[tokio::test]
    async fn qmediasync_strm_keeps_remote_identity_and_skips_page_count_read() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("qms");
        let author = root.join("Alice");
        std::fs::create_dir_all(&author).unwrap();
        std::fs::write(
            author.join("set.zip.strm"),
            "https://qmediasync.example/library/set.zip\n",
        )
        .unwrap();
        let mut request = request(&root, "Alice/set.zip.strm");
        request.provider = "qmediasync:qms".to_string();
        request.qmediasync_mount_name = Some("qms".to_string());
        let resources = ResourceGovernor::new(ResourceLimits::nas_n100_4g());

        let mutation = inspect(&resources, request).await.unwrap();

        assert_eq!(
            mutation.work.source_path,
            "qms-strm://qms/Alice/set.zip.strm"
        );
        assert_eq!(mutation.assets[0].path, "qms-strm://qms/Alice/set.zip.strm");
        assert_eq!(mutation.work.meta["page_count"], 0);
        assert_eq!(mutation.assets[0].meta["provider"], "qmediasync");
    }

    #[tokio::test]
    async fn invalid_extension_escape_and_broken_zip_are_rejected_safely() {
        let temp = tempfile::tempdir().unwrap();
        let resources = ResourceGovernor::new(ResourceLimits::nas_n100_4g());
        let error = inspect(&resources, request(temp.path(), "../outside.zip"))
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("remain below its root"));

        let error = inspect(&resources, request(temp.path(), "set.cbz"))
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("must use ZIP"));

        std::fs::write(temp.path().join("broken.zip"), b"not a zip").unwrap();
        let error = inspect(&resources, request(temp.path(), "broken.zip"))
            .await
            .unwrap_err()
            .to_string();
        assert!(!error.is_empty());
        assert!(!error.contains("panicked"));
    }
}
