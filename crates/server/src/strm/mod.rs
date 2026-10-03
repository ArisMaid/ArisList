//! Shared STRM source, request, cache and preparation primitives.
//!
//! The module is intentionally kept separate from the legacy `vfs` surface.
//! Existing callers continue to use `vfs`, while new STRM behaviour can be
//! added here without making the local-file paths depend on remote details.

pub mod archive;
pub mod cache;
pub mod http;
pub mod jobs;
pub mod source;
