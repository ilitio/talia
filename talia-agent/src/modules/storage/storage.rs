//! Filesystem storage collection helpers for Talia.

use std::collections::BTreeMap;
use std::fs;

use rustix::fs::StatVfs;
use rustix::fs::statvfs;
use thiserror::Error;

const MOUNTINFO_PATH: &str = "/proc/self/mountinfo";

/// Filesystem capacity and utilization sample for one mountpoint.
#[derive(Clone, Debug, PartialEq)]
pub struct FilesystemSample {
    /// Mountpoint path that was sampled.
    pub mountpoint: String,
    /// Filesystem type from Linux mountinfo, when available.
    pub filesystem_type: Option<String>,
    /// Mount mode from mountinfo, usually `ro` or `rw`.
    pub mode: Option<String>,
    /// Total filesystem capacity in bytes.
    pub limit_bytes: u64,
    /// Used bytes.
    pub used_bytes: u64,
    /// Free bytes available to unprivileged users.
    pub free_bytes: u64,
    /// Reserved bytes available only to privileged users.
    pub reserved_bytes: u64,
    /// Fraction of total bytes currently used.
    pub used_ratio: f64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct MountInfo {
    filesystem_type: String,
    mode: Option<String>,
}

/// Errors returned while collecting filesystem samples.
#[derive(Debug, Error)]
pub enum StorageError {
    /// Linux mountinfo could not be read.
    #[error("failed to read mount info from {path}: {source}")]
    ReadMountInfo {
        /// Mountinfo path that failed to be read.
        path: &'static str,
        /// Underlying filesystem error.
        source: std::io::Error,
    },
    /// A mountinfo line did not match the expected Linux format.
    #[error("invalid mountinfo line: {0}")]
    InvalidMountInfo(String),
    /// Filesystem statistics could not be read for a mountpoint.
    #[error("failed to stat filesystem {mountpoint}: {source}")]
    StatVfs {
        /// Mountpoint that failed to be sampled.
        mountpoint: String,
        /// Underlying rustix errno.
        source: rustix::io::Errno,
    },
    /// All configured mountpoints failed to produce samples.
    #[error("failed to collect any configured storage mount")]
    NoFilesystemSamples,
}

/// Collects filesystem samples for the configured mountpoints.
pub fn collect_filesystems(mounts: &[String]) -> Result<Vec<FilesystemSample>, StorageError> {
    let mount_info = read_mount_info()?;
    let mut samples = Vec::new();
    for mount in mounts {
        match collect_filesystem(mount, &mount_info) {
            Ok(sample) => samples.push(sample),
            Err(error) => {
                tracing::warn!(
                    mountpoint = %mount,
                    error = %error,
                    "talia_storage_mount_collection_failed"
                );
            },
        }
    }
    if !mounts.is_empty() && samples.is_empty() {
        return Err(StorageError::NoFilesystemSamples);
    }
    Ok(samples)
}

fn collect_filesystem(
    mountpoint: &str,
    mount_info: &BTreeMap<String, MountInfo>,
) -> Result<FilesystemSample, StorageError> {
    let stat = statvfs(mountpoint).map_err(|source| StorageError::StatVfs {
        mountpoint: mountpoint.to_string(),
        source,
    })?;
    let info = mount_info.get(mountpoint);
    let bytes = filesystem_bytes(&stat);
    Ok(FilesystemSample {
        mountpoint: mountpoint.to_string(),
        filesystem_type: info.map(|info| info.filesystem_type.clone()),
        mode: info.and_then(|info| info.mode.clone()),
        limit_bytes: bytes.limit,
        used_bytes: bytes.used,
        free_bytes: bytes.free,
        reserved_bytes: bytes.reserved,
        used_ratio: bytes.used_ratio(),
    })
}

fn read_mount_info() -> Result<BTreeMap<String, MountInfo>, StorageError> {
    let contents =
        fs::read_to_string(MOUNTINFO_PATH).map_err(|source| StorageError::ReadMountInfo {
            path: MOUNTINFO_PATH,
            source,
        })?;
    parse_mount_info(&contents)
}

fn parse_mount_info(contents: &str) -> Result<BTreeMap<String, MountInfo>, StorageError> {
    let mut mounts = BTreeMap::new();
    for line in contents.lines().filter(|line| !line.trim().is_empty()) {
        let (before_dash, after_dash) = line
            .split_once(" - ")
            .ok_or_else(|| StorageError::InvalidMountInfo(line.to_string()))?;
        let fields: Vec<&str> = before_dash.split_whitespace().collect();
        let fs_fields: Vec<&str> = after_dash.split_whitespace().collect();
        if fields.len() < 6 || fs_fields.is_empty() {
            return Err(StorageError::InvalidMountInfo(line.to_string()));
        }
        let mountpoint = decode_mountinfo_path(fields[4]);
        let mode = fields[5]
            .split(',')
            .next()
            .filter(|mode| *mode == "rw" || *mode == "ro")
            .map(ToString::to_string);
        mounts.insert(
            mountpoint,
            MountInfo {
                filesystem_type: fs_fields[0].to_string(),
                mode,
            },
        );
    }
    Ok(mounts)
}

fn decode_mountinfo_path(value: &str) -> String {
    let mut output = String::with_capacity(value.len());
    let mut chars = value.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch != '\\' {
            output.push(ch);
            continue;
        }
        let mut octal = String::new();
        for _ in 0..3 {
            match chars.peek().copied() {
                Some(next) if next.is_ascii_digit() && next < '8' => {
                    octal.push(next);
                    chars.next();
                },
                _ => break,
            }
        }
        if octal.len() == 3
            && let Ok(byte) = u8::from_str_radix(&octal, 8)
        {
            output.push(char::from(byte));
            continue;
        }
        output.push('\\');
        output.push_str(&octal);
    }
    output
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct FilesystemBytes {
    limit: u64,
    used: u64,
    free: u64,
    reserved: u64,
}

impl FilesystemBytes {
    fn used_ratio(self) -> f64 {
        if self.limit == 0 {
            0.0
        } else {
            self.used as f64 / self.limit as f64
        }
    }
}

fn filesystem_bytes(stat: &StatVfs) -> FilesystemBytes {
    filesystem_bytes_from_blocks(stat.f_frsize, stat.f_blocks, stat.f_bfree, stat.f_bavail)
}

fn filesystem_bytes_from_blocks(
    fragment_size: u64,
    blocks: u64,
    blocks_free: u64,
    blocks_available: u64,
) -> FilesystemBytes {
    let limit = blocks.saturating_mul(fragment_size);
    let free_all = blocks_free.saturating_mul(fragment_size);
    let free = blocks_available.saturating_mul(fragment_size);
    let reserved = blocks_free
        .saturating_sub(blocks_available)
        .saturating_mul(fragment_size);
    let used = limit.saturating_sub(free_all);
    FilesystemBytes {
        limit,
        used,
        free,
        reserved,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_mountinfo_filesystem_type_and_mode() {
        // Given: the scenario for parses mountinfo filesystem type and mode is prepared.
        let contents = "\
29 23 0:25 / / rw,relatime - ext4 /dev/sda1 rw\n\
30 23 0:26 / /mnt/data\\040disk ro,relatime - xfs /dev/sdb1 ro\n";

        // When: the behavior under test runs.
        let mounts = parse_mount_info(contents).expect("mountinfo should parse");

        // Then: the assertions confirm that parses mountinfo filesystem type and mode.
        assert_eq!(
            mounts.get("/").expect("root mount"),
            &MountInfo {
                filesystem_type: "ext4".to_string(),
                mode: Some("rw".to_string())
            }
        );
        assert_eq!(
            mounts.get("/mnt/data disk").expect("escaped mount"),
            &MountInfo {
                filesystem_type: "xfs".to_string(),
                mode: Some("ro".to_string())
            }
        );
    }

    #[test]
    fn storage_math_keeps_reserved_space_separate() {
        // Given: the scenario for storage math keeps reserved space separate is prepared.
        let fragment_size = 4096;
        let blocks = 100;
        let blocks_free = 20;
        let blocks_available = 15;

        // When: the behavior under test runs.
        let bytes =
            filesystem_bytes_from_blocks(fragment_size, blocks, blocks_free, blocks_available);

        // Then: the assertions confirm that storage math keeps reserved space separate.
        assert_eq!(bytes.limit, 409_600);
        assert_eq!(bytes.used, 327_680);
        assert_eq!(bytes.free, 61_440);
        assert_eq!(bytes.reserved, 20_480);
        assert_eq!(bytes.used_ratio(), 0.8);
    }

    #[test]
    fn reports_error_when_all_configured_mounts_fail() {
        // Given: the scenario for reports error when all configured mounts fail is prepared.
        let missing_mount =
            std::env::temp_dir().join(format!("talia-missing-mount-{}", std::process::id()));
        let mounts = vec![missing_mount.display().to_string()];

        // When: the behavior under test runs.
        let result = collect_filesystems(&mounts);

        // Then: the assertions confirm that reports error when all configured mounts fail.
        assert!(matches!(result, Err(StorageError::NoFilesystemSamples)));
    }
}
