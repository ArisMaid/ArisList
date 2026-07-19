/// Configuration for reading ZIP archives.
#[repr(transparent)]
#[derive(Debug, Default, Clone, Copy)]
pub struct Config {
    /// An offset into the reader to use to find the start of the archive.
    pub archive_offset: ArchiveOffset,
}

/// Resource limits applied while reading ZIP metadata.
///
/// Limits are checked for every EOCD candidate before allocating storage for
/// the central directory. `None` preserves the crate's historical unlimited
/// behavior.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ReadLimits {
    /// Maximum number of central-directory entries, including duplicate names.
    pub max_entries: Option<u64>,
    /// Maximum number of bytes declared for the central directory.
    pub max_central_directory_size: Option<u64>,
    /// Maximum total size of the ZIP64 EOCD record, including its 12-byte header.
    pub max_zip64_eocd_size: Option<u64>,
}

/// The offset of the start of the archive from the beginning of the reader.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ArchiveOffset {
    /// Try to detect the archive offset automatically.
    ///
    /// This will look at the central directory specified by `FromCentralDirectory` for a header.
    /// If missing, this will behave as if `None` were specified.
    #[default]
    Detect,
    /// Use the central directory length and offset to determine the start of the archive.
    #[deprecated(since = "2.3.0", note = "use `Detect` instead")]
    FromCentralDirectory,
    /// Specify a fixed archive offset.
    Known(u64),
}
