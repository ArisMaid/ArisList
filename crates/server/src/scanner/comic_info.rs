//! Shared sidecar mapping. Reading it never requires opening the remote archive.
use std::collections::BTreeSet;
use std::path::Path;

use serde::Deserialize;
use serde_json::{json, Value};

use super::{clean_title, normalize_key, parse_comic_genre_tags, ParsedTag};

pub(crate) const METADATA_VERSION: &str = "comic-info-v2";
pub(crate) const COMIC_FINGERPRINT_PREFIX: &str = "comic-v2:";

#[derive(Debug)]
pub(crate) enum ComicInfoRead {
    Present(ComicInfo),
    Missing,
}

impl ComicInfoRead {
    pub(crate) fn is_missing(&self) -> bool {
        matches!(self, Self::Missing)
    }

    pub(crate) fn into_info(self) -> ComicInfo {
        match self {
            Self::Present(info) => info,
            Self::Missing => ComicInfo::default(),
        }
    }
}

/// Only scanners opting into this sidecar contract use these merge rules.
/// Keep an existing title on fallback and only erase a legacy description
/// when it is demonstrably the old alternate title, not user-written prose.
pub(crate) fn merge_existing_fields(
    title: &str,
    subtitle: Option<&str>,
    description: Option<&str>,
    meta: &Value,
    existing: Option<(&str, Option<&str>, Option<&str>, &Value)>,
) -> (String, Option<String>, Option<String>) {
    let mut title = title.to_string();
    let mut subtitle = subtitle.map(ToOwned::to_owned);
    let mut description = description.map(ToOwned::to_owned);
    if let Some((old_title, old_subtitle, old_description, old_meta)) = existing {
        if meta["comic_info"]["title_source"] == "fallback" && !old_title.trim().is_empty() {
            title = old_title.to_string();
            subtitle = old_subtitle.map(ToOwned::to_owned);
        }
        if description.is_none() {
            let legacy_alternate = meta["comic_info"]["alternate_series"].as_str();
            let previous_alternate = old_meta["comic_info"]["alternate_series"].as_str();
            if !old_description
                .is_some_and(|old| Some(old) == legacy_alternate || Some(old) == previous_alternate)
            {
                description = old_description.map(ToOwned::to_owned);
            }
        }
    }
    (title, subtitle, description)
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub(crate) struct ComicInfo {
    pub title: Option<String>,
    pub series: Option<String>,
    pub alternate_series: Option<String>,
    pub summary: Option<String>,
    pub writer: Option<String>,
    pub penciller: Option<String>,
    pub genre: Option<String>,
    pub characters: Option<String>,
    pub teams: Option<String>,
    pub web: Option<String>,
    pub page_count: Option<i64>,
    #[serde(rename = "LanguageISO", alias = "LanguageIso")]
    pub language_iso: Option<String>,
    pub community_rating: Option<f64>,
}

fn text(value: Option<&str>) -> Option<&str> {
    value.map(str::trim).filter(|value| !value.is_empty())
}

impl ComicInfo {
    pub fn title_candidate(&self) -> Option<(&'static str, &str)> {
        [
            ("AlternateSeries", self.alternate_series.as_deref()),
            ("Title", self.title.as_deref()),
            ("Series", self.series.as_deref()),
        ]
        .into_iter()
        .find_map(|(field, value)| text(value).map(|value| (field, value)))
    }

    pub fn display_title(&self, fallback: &str) -> String {
        self.title_candidate()
            .map(|(_, value)| clean_title(value))
            .unwrap_or_else(|| fallback.to_string())
    }

    pub fn subtitle(&self) -> Option<String> {
        text(self.series.as_deref())
            .filter(|value| {
                self.title_candidate()
                    .is_none_or(|(_, title)| *value != title)
            })
            .map(ToOwned::to_owned)
    }

    pub fn description(&self) -> Option<String> {
        text(self.summary.as_deref()).map(ToOwned::to_owned)
    }

    pub fn meta(&self, page_count: i64) -> Value {
        self.meta_with_sidecar_status(page_count, "present")
    }

    pub fn meta_with_sidecar_status(&self, page_count: i64, sidecar_status: &str) -> Value {
        json!({
            "page_count": page_count,
            "writer": self.writer,
            "penciller": self.penciller,
            "language_iso": self.language_iso,
            "comic_info": {
                "version": METADATA_VERSION,
                "sidecar_status": sidecar_status,
                "title_source": self.title_candidate().map(|(field, _)| field).unwrap_or("fallback"),
                "title": self.title,
                "series": self.series,
                "alternate_series": self.alternate_series,
                "subtitle": self.subtitle(),
                "web": self.web,
            }
        })
    }

    pub fn tags(&self) -> Vec<ParsedTag> {
        let mut tags = self
            .genre
            .as_deref()
            .map(parse_comic_genre_tags)
            .unwrap_or_default();
        for (namespace, value) in [
            ("artist", self.penciller.as_deref()),
            ("group", self.writer.as_deref()),
            ("character", self.characters.as_deref()),
            ("team", self.teams.as_deref()),
        ] {
            for label in value
                .into_iter()
                .flat_map(|value| value.split(','))
                .filter_map(|value| text(Some(value)))
            {
                tags.push(ParsedTag {
                    namespace: namespace.to_string(),
                    key: normalize_key(label),
                    label: label.to_string(),
                });
            }
        }
        if let Some(language) = text(self.language_iso.as_deref()) {
            let label = match language {
                "zh" | "cn" => "chinese",
                "ja" => "japanese",
                "en" => "english",
                other => other,
            };
            tags.push(ParsedTag {
                namespace: "language".to_string(),
                key: label.to_string(),
                label: label.to_string(),
            });
        }
        let mut seen = BTreeSet::new();
        tags.retain(|tag| {
            !tag.key.is_empty() && seen.insert((tag.namespace.clone(), tag.key.clone()))
        });
        tags
    }
}

/// Called in the scanner blocking pool along with the sidecar fingerprint.
pub(crate) fn fallback_title(archive: &Path) -> String {
    let parent = archive.parent();
    // A directory title only identifies a work when it contains one archive.
    let shared = parent
        .and_then(|p| std::fs::read_dir(p).ok())
        .is_some_and(|entries| {
            entries
                .filter_map(std::result::Result::ok)
                .filter(|entry| {
                    let path = entry.path();
                    path.is_file()
                        && path
                            .extension()
                            .and_then(|ext| ext.to_str())
                            .is_some_and(|ext| {
                                ["strm", "cbz", "zip", "rar", "7z"]
                                    .iter()
                                    .any(|kind| ext.eq_ignore_ascii_case(kind))
                            })
                        && !crate::strm::source::is_secondary_volume(&path)
                })
                .take(2)
                .count()
                > 1
        });
    let fallback = if shared {
        archive.file_stem()
    } else {
        parent.and_then(Path::file_name)
    }
    .or_else(|| archive.file_stem())
    .and_then(|name| name.to_str())
    .unwrap_or("Untitled comic");
    clean_title(fallback)
}
