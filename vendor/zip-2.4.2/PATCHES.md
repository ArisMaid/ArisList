# Local metadata limits patch

This directory vendors `zip` 2.4.2 from <https://github.com/zip-rs/zip2>
(MIT license; see `LICENSE`). The application uses it through the root
`[patch.crates-io]` entry.

The local patch adds opt-in `zip::read::ReadLimits` and
`ZipArchive::{with_limits,with_config_and_limits}`. Limits are checked for
every EOCD/ZIP64 fallback candidate before allocating central-directory or
ZIP64 extensible metadata. It also bounds each central-directory record by the
footer-declared directory size and uses fallible reservation for the entry
vector.

Existing `ZipArchive::new` and `with_config` remain unlimited for upstream API
compatibility. Media Shelf routes all production CBZ/EPUB/cloud archive reads
through `crates/server/src/archive.rs`, which enables the limits.

When upgrading `zip`, first verify whether upstream provides equivalent entry,
central-directory byte, ZIP64 EOCD, and per-record boundary limits. Keep the
server regression tests for duplicate central-directory names, SFX prefixes,
declared directory bounds, and ZIP64 extensible records.
