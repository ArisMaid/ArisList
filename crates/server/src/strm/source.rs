//! Classification of a STRM target without contacting the remote source.

use std::path::Path;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceKind {
    Audio,
    Image,
    Archive,
    Text,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VolumePart {
    pub group_key: String,
    pub index: u32,
    pub is_primary: bool,
}

impl SourceKind {
    pub fn is_audio(self) -> bool {
        matches!(self, Self::Audio)
    }

    pub fn is_image(self) -> bool {
        matches!(self, Self::Image)
    }

    pub fn is_archive(self) -> bool {
        matches!(self, Self::Archive)
    }
}

const AUDIO_EXTENSIONS: &[&str] = &[
    "aac", "aiff", "ape", "flac", "m4a", "mka", "mp3", "oga", "ogg", "opus", "wav", "wma",
];
const IMAGE_EXTENSIONS: &[&str] = &["avif", "gif", "jpeg", "jpg", "png", "webp"];
const ARCHIVE_EXTENSIONS: &[&str] = &["7z", "cb7", "cbz", "rar", "zip"];
const TEXT_EXTENSIONS: &[&str] = &["ass", "cue", "srt", "txt", "vtt"];

/// Classify a local STRM filename and, when available, its target URL.
///
/// A `.strm` file is only a pointer.  The extension immediately before
/// `.strm` wins, followed by the URL path extension.  Query strings and
/// fragments are ignored.  Split archive names such as `.part01.rar` and
/// `.r00` are treated as archives as well.
pub fn classify(path: &Path, target: Option<&str>) -> SourceKind {
    let path_hint = path
        .file_name()
        .and_then(|value| value.to_str())
        .and_then(|name| {
            let name = name.strip_suffix(".strm").unwrap_or(name);
            extension_from_name(name)
        });
    let target_hint = target.and_then(extension_from_target);

    classify_extension(path_hint.or(target_hint))
}

pub fn classify_extension(extension: Option<&str>) -> SourceKind {
    let Some(extension) = extension.map(|value| value.trim_start_matches('.').to_ascii_lowercase())
    else {
        return SourceKind::Unknown;
    };

    if AUDIO_EXTENSIONS.contains(&extension.as_str()) {
        SourceKind::Audio
    } else if IMAGE_EXTENSIONS.contains(&extension.as_str()) {
        SourceKind::Image
    } else if ARCHIVE_EXTENSIONS.contains(&extension.as_str())
        || is_split_archive_extension(&extension)
    {
        SourceKind::Archive
    } else if TEXT_EXTENSIONS.contains(&extension.as_str()) {
        SourceKind::Text
    } else {
        SourceKind::Unknown
    }
}

pub fn extension_from_target(target: &str) -> Option<&str> {
    let target = target.split(['?', '#']).next().unwrap_or(target);
    let name = target.rsplit('/').next().unwrap_or(target);
    extension_from_name(name)
}

/// Return the normalized group and ordinal for a supported multi-volume name.
///
/// The group key intentionally excludes the volume suffix but keeps the
/// directory out of the value. Callers combine it with their already-scoped
/// directory when grouping files. A normal `.zip`, `.rar`, or `.7z` is treated
/// as a potential primary and becomes a group only when a sibling volume is
/// present.
pub fn volume_part(path: &Path) -> Option<VolumePart> {
    let name = path
        .file_name()?
        .to_str()?
        .strip_suffix(".strm")
        .unwrap_or_else(|| {
            path.file_name()
                .and_then(|value| value.to_str())
                .unwrap_or_default()
        });
    let lower = name.to_ascii_lowercase();

    if let Some(part_start) = lower.rfind(".part") {
        let suffix = &lower[part_start + ".part".len()..];
        if let Some(number) = suffix.strip_suffix(".rar") {
            if let Some(index) = parse_digits(number) {
                return Some(VolumePart {
                    group_key: lower[..part_start].to_string(),
                    index,
                    is_primary: index == 1,
                });
            }
        }
    }

    for archive in ["7z", "zip", "rar"] {
        let marker = format!(".{archive}.");
        if let Some(volume_start) = lower.rfind(&marker) {
            if let Some(index) = parse_digits(&lower[volume_start + marker.len()..]) {
                return Some(VolumePart {
                    group_key: lower[..volume_start].to_string(),
                    index,
                    is_primary: index == 1,
                });
            }
        }
    }

    let (base, extension) = lower.rsplit_once('.')?;
    if extension.len() == 3 && extension[1..].chars().all(|value| value.is_ascii_digit()) {
        if let Some(number) = extension.strip_prefix('r').and_then(parse_digits) {
            let group_key = base.strip_suffix(".rar").unwrap_or(base);
            return Some(VolumePart {
                group_key: group_key.to_string(),
                index: number.saturating_add(2),
                is_primary: false,
            });
        }
        if let Some(number) = extension.strip_prefix('z').and_then(parse_digits) {
            let group_key = base.strip_suffix(".zip").unwrap_or(base);
            return Some(VolumePart {
                group_key: group_key.to_string(),
                index: number.saturating_add(1),
                is_primary: false,
            });
        }
    }

    if matches!(extension, "7z" | "zip" | "rar") {
        return Some(VolumePart {
            group_key: base.to_string(),
            index: 1,
            is_primary: true,
        });
    }
    None
}

pub fn volume_group_key(path: &Path) -> Option<String> {
    volume_part(path).map(|part| part.group_key)
}

pub fn is_primary_volume(path: &Path) -> bool {
    volume_part(path).is_some_and(|part| part.is_primary)
}

fn extension_from_name(name: &str) -> Option<&str> {
    let name = name.strip_suffix('/').unwrap_or(name);
    let extension = Path::new(name).extension()?.to_str()?;
    if extension.len() == 3
        && extension.chars().all(|value| value.is_ascii_digit())
        && Path::new(name)
            .file_stem()
            .and_then(|value| Path::new(value).extension())
            .and_then(|value| value.to_str())
            .is_some_and(|value| {
                matches!(value.to_ascii_lowercase().as_str(), "7z" | "zip" | "rar")
            })
    {
        return Path::new(name)
            .file_stem()
            .and_then(|value| Path::new(value).extension())
            .and_then(|value| value.to_str());
    }
    Some(extension)
}

pub fn is_secondary_volume(path: &Path) -> bool {
    volume_part(path).is_some_and(|part| !part.is_primary)
}

fn parse_digits(value: &str) -> Option<u32> {
    (!value.is_empty() && value.chars().all(|item| item.is_ascii_digit()))
        .then(|| value.parse().ok())
        .flatten()
}

fn is_split_archive_extension(extension: &str) -> bool {
    let extension = extension.to_ascii_lowercase();
    extension.starts_with('r')
        && extension.len() == 3
        && extension[1..].chars().all(|value| value.is_ascii_digit())
        || extension.starts_with('z')
            && extension.len() == 3
            && extension[1..].chars().all(|value| value.is_ascii_digit())
        || extension.starts_with("part")
            && extension.ends_with("rar")
            && extension[4..extension.len() - 4]
                .chars()
                .all(|value| value.is_ascii_digit())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn classifies_pointer_stem_before_target_url() {
        assert_eq!(
            classify(
                Path::new("track.mp3.strm"),
                Some("https://example.test/a.zip")
            ),
            SourceKind::Audio
        );
        assert_eq!(
            classify(
                Path::new("book.strm"),
                Some("https://example.test/a.cbz?x=1")
            ),
            SourceKind::Archive
        );
    }

    #[test]
    fn classifies_split_archive_parts() {
        assert_eq!(classify_extension(Some("r00")), SourceKind::Archive);
        assert_eq!(classify_extension(Some("z01")), SourceKind::Archive);
        assert_eq!(classify_extension(Some("part01.rar")), SourceKind::Archive);
        assert_eq!(
            classify(Path::new("book.7z.001.strm"), None),
            SourceKind::Archive
        );
        assert!(is_secondary_volume(Path::new("book.rar.r00.strm")));
        assert!(is_secondary_volume(Path::new("book.7z.002.strm")));
        assert!(!is_secondary_volume(Path::new("book.7z.001.strm")));
        assert!(!is_secondary_volume(Path::new("book.part01.rar.strm")));
        assert_eq!(
            volume_part(Path::new("book.rar.r00.strm")),
            Some(VolumePart {
                group_key: "book".to_string(),
                index: 2,
                is_primary: false,
            })
        );
        assert_eq!(
            volume_part(Path::new("book.part01.rar.strm")),
            Some(VolumePart {
                group_key: "book".to_string(),
                index: 1,
                is_primary: true,
            })
        );
    }

    #[test]
    fn does_not_treat_unknown_targets_as_audio() {
        assert_eq!(
            classify(
                Path::new("unknown.strm"),
                Some("https://example.test/a.bin")
            ),
            SourceKind::Unknown
        );
    }
}
