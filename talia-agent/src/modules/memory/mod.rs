//! Memory pressure collection.

#[path = "memory.rs"]
mod pressure;
mod provider;

pub use pressure::MemoryCollector;
pub use pressure::MemoryError;
pub use pressure::MemorySample;
pub use provider::MEMORY_USAGE_SAMPLE;
pub use provider::MEMORY_UTILIZATION_SAMPLE;
pub use provider::MemoryProvider;
pub use provider::SWAP_IO_SAMPLE;
pub use provider::SWAP_USAGE_SAMPLE;
pub use provider::SWAP_UTILIZATION_SAMPLE;
