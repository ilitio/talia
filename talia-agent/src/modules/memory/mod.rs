//! Memory pressure collection.

#[path = "memory.rs"]
mod pressure;

pub use pressure::MemoryCollector;
pub use pressure::MemoryError;
pub use pressure::MemorySample;
