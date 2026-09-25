//! CPU scheduler accounting collection.

#[path = "cpu.rs"]
mod collector;

pub use collector::CpuCollector;
pub use collector::CpuCollectorError;
pub use collector::CpuSnapshot;
