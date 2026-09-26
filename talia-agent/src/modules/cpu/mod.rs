//! CPU scheduler accounting collection.

#[path = "cpu.rs"]
mod collector;
mod provider;

pub use collector::CpuCollector;
pub use collector::CpuCollectorError;
pub use collector::CpuSnapshot;
pub use provider::CPU_LOGICAL_NUMBER_ATTRIBUTE;
pub use provider::CPU_STATE_ATTRIBUTE;
pub use provider::CPU_UTILIZATION_SAMPLE;
pub use provider::CpuProvider;
