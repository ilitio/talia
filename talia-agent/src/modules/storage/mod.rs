//! Filesystem storage collection.

#[path = "storage.rs"]
mod filesystem;
mod provider;

pub use filesystem::FilesystemSample;
pub use filesystem::StorageError;
pub use filesystem::collect_filesystems;
pub use provider::FILESYSTEM_LIMIT_SAMPLE;
pub use provider::FILESYSTEM_USAGE_SAMPLE;
pub use provider::FILESYSTEM_UTILIZATION_SAMPLE;
pub use provider::StorageProvider;
