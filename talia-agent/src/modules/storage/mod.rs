//! Filesystem storage collection.

#[path = "storage.rs"]
mod filesystem;

pub use filesystem::FilesystemSample;
pub use filesystem::StorageError;
pub use filesystem::collect_filesystems;
