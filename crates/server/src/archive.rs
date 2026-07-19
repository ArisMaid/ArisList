use std::io::{Read, Seek};

use zip::read::ReadLimits;
use zip::ZipArchive;

use crate::error::{AppError, Result};

pub const MAX_ARCHIVE_ENTRIES: u64 = 50_000;
pub const MAX_CENTRAL_DIRECTORY_BYTES: u64 = 64 * 1024 * 1024;
pub const MAX_ZIP64_END_RECORD_BYTES: u64 = 1024 * 1024;

pub fn open_media_zip<R: Read + Seek>(reader: R, label: &str) -> Result<ZipArchive<R>> {
    open_zip_with_limits(reader, label, media_zip_limits())
}

fn media_zip_limits() -> ReadLimits {
    ReadLimits {
        max_entries: Some(MAX_ARCHIVE_ENTRIES),
        max_central_directory_size: Some(MAX_CENTRAL_DIRECTORY_BYTES),
        max_zip64_eocd_size: Some(MAX_ZIP64_END_RECORD_BYTES),
    }
}

fn open_zip_with_limits<R: Read + Seek>(
    reader: R,
    label: &str,
    limits: ReadLimits,
) -> Result<ZipArchive<R>> {
    ZipArchive::with_limits(reader, limits).map_err(|err| {
        AppError::BadRequest(format!(
            "{label} is invalid or exceeds metadata limits: {err}"
        ))
    })
}

#[cfg(test)]
mod tests {
    use std::io::{Cursor, Write};

    use super::*;

    fn archive_with_names(names: &[&str]) -> Cursor<Vec<u8>> {
        let mut writer = zip::ZipWriter::new(Cursor::new(Vec::new()));
        let options = zip::write::SimpleFileOptions::default();
        for name in names {
            writer.start_file(*name, options).unwrap();
            writer.write_all(b"x").unwrap();
        }
        writer.finish().unwrap()
    }

    fn archive_with_duplicate_central_names() -> Cursor<Vec<u8>> {
        let mut archive = archive_with_names(&["one.jpg", "two.jpg", "tri.jpg"]);
        let bytes = archive.get_mut();
        let mut offset = 0;
        let mut rewritten = 0;
        while offset + 46 <= bytes.len() {
            let Some(relative) = bytes[offset..]
                .windows(4)
                .position(|window| window == b"PK\x01\x02")
            else {
                break;
            };
            let header = offset + relative;
            let name_len = u16::from_le_bytes([bytes[header + 28], bytes[header + 29]]) as usize;
            let extra_len = u16::from_le_bytes([bytes[header + 30], bytes[header + 31]]) as usize;
            let comment_len = u16::from_le_bytes([bytes[header + 32], bytes[header + 33]]) as usize;
            assert_eq!(name_len, "one.jpg".len());
            bytes[header + 46..header + 46 + name_len].copy_from_slice(b"one.jpg");
            rewritten += 1;
            offset = header + 46 + name_len + extra_len + comment_len;
        }
        assert_eq!(rewritten, 3);
        archive.set_position(0);
        archive
    }

    fn empty_zip64(extensible_size: usize, prefix: &[u8]) -> Cursor<Vec<u8>> {
        let mut bytes = prefix.to_vec();
        let zip64_offset = 0_u64;
        bytes.extend_from_slice(&0x0606_4b50_u32.to_le_bytes());
        bytes.extend_from_slice(&(44_u64 + extensible_size as u64).to_le_bytes());
        bytes.extend_from_slice(&45_u16.to_le_bytes());
        bytes.extend_from_slice(&45_u16.to_le_bytes());
        bytes.extend_from_slice(&0_u32.to_le_bytes());
        bytes.extend_from_slice(&0_u32.to_le_bytes());
        bytes.extend_from_slice(&0_u64.to_le_bytes());
        bytes.extend_from_slice(&0_u64.to_le_bytes());
        bytes.extend_from_slice(&0_u64.to_le_bytes());
        bytes.extend_from_slice(&0_u64.to_le_bytes());
        bytes.resize(bytes.len() + extensible_size, 0);
        bytes.extend_from_slice(&0x0706_4b50_u32.to_le_bytes());
        bytes.extend_from_slice(&0_u32.to_le_bytes());
        bytes.extend_from_slice(&zip64_offset.to_le_bytes());
        bytes.extend_from_slice(&1_u32.to_le_bytes());
        bytes.extend_from_slice(&0x0605_4b50_u32.to_le_bytes());
        bytes.extend_from_slice(&0_u16.to_le_bytes());
        bytes.extend_from_slice(&0_u16.to_le_bytes());
        bytes.extend_from_slice(&u16::MAX.to_le_bytes());
        bytes.extend_from_slice(&u16::MAX.to_le_bytes());
        bytes.extend_from_slice(&u32::MAX.to_le_bytes());
        bytes.extend_from_slice(&u32::MAX.to_le_bytes());
        bytes.extend_from_slice(&0_u16.to_le_bytes());
        Cursor::new(bytes)
    }

    #[test]
    fn entry_limit_counts_duplicate_central_directory_records() {
        let limits = ReadLimits {
            max_entries: Some(2),
            max_central_directory_size: Some(1024 * 1024),
            max_zip64_eocd_size: Some(1024),
        };
        let duplicate_archive = archive_with_duplicate_central_names();
        assert!(open_zip_with_limits(duplicate_archive.clone(), "test archive", limits).is_err());

        let duplicate_archive = open_zip_with_limits(
            duplicate_archive,
            "test archive",
            ReadLimits {
                max_entries: Some(3),
                ..limits
            },
        )
        .unwrap();
        assert_eq!(duplicate_archive.len(), 1);

        let archive = open_zip_with_limits(
            archive_with_names(&["one.jpg", "two.jpg"]),
            "test archive",
            limits,
        )
        .unwrap();
        assert_eq!(archive.len(), 2);
    }

    #[test]
    fn central_directory_bounds_are_checked_before_variable_fields() {
        let mut archive = archive_with_names(&["one.jpg"]);
        let bytes = archive.get_mut();
        let eocd = bytes
            .windows(4)
            .rposition(|window| window == b"PK\x05\x06")
            .unwrap();
        bytes[eocd + 12..eocd + 16].copy_from_slice(&46_u32.to_le_bytes());
        archive.set_position(0);
        assert!(open_zip_with_limits(archive, "test archive", media_zip_limits()).is_err());
    }

    #[test]
    fn limited_reader_preserves_self_extracting_prefix_detection() {
        let archive = archive_with_names(&["one.jpg"]);
        let mut prefixed = b"MZ-test-prefix".to_vec();
        prefixed.extend_from_slice(archive.get_ref());
        let opened =
            open_zip_with_limits(Cursor::new(prefixed), "test archive", media_zip_limits())
                .unwrap();
        assert_eq!(opened.len(), 1);
    }

    #[test]
    fn zip64_end_record_limit_precedes_extensible_allocation_and_supports_sfx() {
        let extensible_size = 64_u64;
        let total_record_size = 56 + extensible_size;
        let limits = ReadLimits {
            max_entries: Some(1),
            max_central_directory_size: Some(1024),
            max_zip64_eocd_size: Some(total_record_size - 1),
        };
        assert!(open_zip_with_limits(
            empty_zip64(extensible_size as usize, b""),
            "ZIP64 test archive",
            limits,
        )
        .is_err());

        let allowed = ReadLimits {
            max_zip64_eocd_size: Some(total_record_size),
            ..limits
        };
        assert_eq!(
            open_zip_with_limits(
                empty_zip64(extensible_size as usize, b"MZ-zip64-prefix"),
                "ZIP64 SFX test archive",
                allowed,
            )
            .unwrap()
            .len(),
            0
        );
    }
}
