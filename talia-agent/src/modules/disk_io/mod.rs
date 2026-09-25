//! Disk I/O accounting collection.

mod collector;

pub use collector::DiskIoCollector;
pub use collector::DiskIoCollectorError;
pub use collector::DiskIoDirection;
pub use collector::DiskIoSample;
pub use collector::DiskIoSnapshot;
