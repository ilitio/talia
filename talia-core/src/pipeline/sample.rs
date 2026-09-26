//! Neutral telemetry data model: [`Sample`] and [`SampleValue`].

use std::collections::BTreeMap;
use std::time::SystemTime;

/// A single measured value emitted by a provider.
#[derive(Clone, Debug, PartialEq)]
pub struct Sample {
    /// Dotted metric name, e.g. `system.filesystem.usage`.
    pub name: String,
    /// The measured value.
    pub value: SampleValue,
    /// Dimensions describing the measurement, e.g. `mountpoint` -> `/`.
    ///
    /// Transport-level attributes (such as the active config version) are
    /// added by the sink, not the provider.
    pub attributes: BTreeMap<String, String>,
    /// When the measurement was taken.
    pub timestamp: SystemTime,
}

/// The value carried by a [`Sample`].
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum SampleValue {
    /// A monotonically increasing count, e.g. bytes transferred.
    Counter(u64),
    /// A point-in-time unsigned measurement, e.g. bytes used.
    GaugeU64(u64),
    /// A point-in-time signed measurement, e.g. in-flight operations.
    GaugeI64(i64),
    /// A point-in-time float measurement, e.g. a utilization ratio.
    GaugeF64(f64),
}
