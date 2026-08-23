//! Stable audio work grouping rules shared by the legacy scanner and the
//! bounded inventory/catalog path.
//!
//! Audio libraries are commonly a mix of RJ-labelled products and ordinary
//! folder trees.  Keeping the decision in one small, database-free helper is
//! important: a watcher event must derive the same work identity as a full
//! inventory scan, otherwise a rename can create a duplicate or tombstone the
//! wrong work.

use regex::Regex;
use serde::{Deserialize, Serialize};
use std::sync::LazyLock;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub(crate) enum AudioGroupingMode {
    Rj,
    Folder,
    #[default]
    Auto,
}

impl AudioGroupingMode {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Rj => "rj",
            Self::Folder => "folder",
            Self::Auto => "auto",
        }
    }
}

static AUDIO_WORK_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)RJ\d{6,9}").expect("valid audio work regex"));

/// Return the first RJ identifier in a portable relative path.
pub(crate) fn rj_key(value: &str) -> Option<String> {
    AUDIO_WORK_RE
        .find(value)
        .map(|value| value.as_str().to_ascii_uppercase())
}

/// Derive a stable work key for an inventory file or watcher event.
///
/// `parent_key` is the portable parent directory for a file.  For directory
/// events callers pass the directory itself as `relative_path`; preserving it
/// as the folder fallback lets a targeted rescan cover the whole subtree.
pub(crate) fn derive_work_key(
    mode: AudioGroupingMode,
    relative_path: &str,
    parent_key: &str,
    is_directory: bool,
) -> String {
    match mode {
        AudioGroupingMode::Folder => folder_key(relative_path, parent_key, is_directory),
        AudioGroupingMode::Rj | AudioGroupingMode::Auto => rj_key(relative_path)
            .unwrap_or_else(|| folder_key(relative_path, parent_key, is_directory)),
    }
}

fn folder_key(relative_path: &str, parent_key: &str, is_directory: bool) -> String {
    if is_directory {
        relative_path.to_string()
    } else {
        parent_key.to_string()
    }
}

pub(crate) fn is_rj_work(mode: AudioGroupingMode, work_key: &str) -> bool {
    if !matches!(mode, AudioGroupingMode::Rj | AudioGroupingMode::Auto) {
        return false;
    }
    let normalized = work_key.trim().to_ascii_uppercase();
    rj_key(&normalized).as_deref() == Some(normalized.as_str())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn auto_prefers_rj_and_falls_back_to_folder() {
        assert_eq!(
            derive_work_key(
                AudioGroupingMode::Auto,
                "Author/RJ123456/track.mp3",
                "Author/RJ123456",
                false,
            ),
            "RJ123456"
        );
        assert_eq!(
            derive_work_key(
                AudioGroupingMode::Auto,
                "Author/Set/track.mp3",
                "Author/Set",
                false,
            ),
            "Author/Set"
        );
    }

    #[test]
    fn explicit_modes_are_deterministic_for_nested_and_mixed_trees() {
        assert_eq!(
            derive_work_key(
                AudioGroupingMode::Folder,
                "Author/RJ123456/Disc/track.mp3",
                "Author/RJ123456/Disc",
                false,
            ),
            "Author/RJ123456/Disc"
        );
        assert_eq!(
            derive_work_key(
                AudioGroupingMode::Rj,
                "Author/RJ123456/Disc/track.mp3",
                "Author/RJ123456/Disc",
                false,
            ),
            "RJ123456"
        );
        assert_eq!(
            derive_work_key(
                AudioGroupingMode::Rj,
                "Author/Set/track.mp3",
                "Author/Set",
                false,
            ),
            "Author/Set"
        );
    }

    #[test]
    fn directory_events_keep_their_prefix() {
        assert_eq!(
            derive_work_key(AudioGroupingMode::Auto, "Author/Set", "Author", true),
            "Author/Set"
        );
    }
}
