//! [`Provider`](talia_core::pipeline::Provider) implementation for disk I/O
//! accounting.

use std::collections::BTreeMap;
use std::time::Duration;
use std::time::SystemTime;

use talia_core::pipeline::Provider;
use talia_core::pipeline::ProviderError;
use talia_core::pipeline::Sample;
use talia_core::pipeline::SampleValue;

use crate::modules::disk_io::DiskIoCollector;
use crate::modules::disk_io::DiskIoSample;

/// Sample name for disk I/O bytes, split by the `disk.io.direction`
/// attribute (`read`, `write`).
pub const DISK_IO_SAMPLE: &str = "system.disk.io";
/// Sample name for disk I/O operations, split by direction.
pub const DISK_OPERATIONS_SAMPLE: &str = "system.disk.operations";
/// Sample name for disk I/O errors, split by direction.
pub const DISK_ERRORS_SAMPLE: &str = "system.disk.errors";
/// Sample name for average issue-to-completion latency in milliseconds,
/// split by direction. Only emitted when the window completed operations.
pub const DISK_LATENCY_SAMPLE: &str = "system.disk.io.latency";
/// Sample name for the current in-flight operation count, split by direction.
pub const DISK_QUEUE_DEPTH_SAMPLE: &str = "system.disk.io.queue_depth";
/// Sample name for the current in-flight bytes, split by direction.
pub const DISK_IN_FLIGHT_BYTES_SAMPLE: &str = "system.disk.io.in_flight";

/// Attribute carrying the I/O direction.
pub const DISK_DIRECTION_ATTRIBUTE: &str = "disk.io.direction";

/// Disk I/O accounting data provider.
///
/// Wraps the eBPF [`DiskIoCollector`]; loading attaches the eBPF program and
/// can fail, so construction is fallible. The sampling window tracks the
/// agent's configured disk I/O interval and is refreshed by the collection
/// loop before every collection.
pub struct DiskIoProvider {
    collector: DiskIoCollector,
    window: Duration,
}

impl DiskIoProvider {
    /// Loads the eBPF program and creates a provider for the given window.
    pub fn load(window: Duration) -> Result<Self, ProviderError> {
        DiskIoCollector::load()
            .map(|collector| Self { collector, window })
            .map_err(|source| ProviderError::new("disk_io", source))
    }
}

impl Provider for DiskIoProvider {
    fn name(&self) -> &'static str {
        "disk_io"
    }

    /// Updates the sampling window, e.g. after a config change.
    fn set_window(&mut self, window: Duration) {
        self.window = window;
    }

    fn collect(&mut self) -> Result<Vec<Sample>, ProviderError> {
        self.collector
            .collect(self.window)
            .map(|snapshot| {
                let timestamp = SystemTime::now();
                snapshot
                    .samples
                    .iter()
                    .flat_map(|sample| disk_io_samples(sample, timestamp))
                    .collect()
            })
            .map_err(|source| ProviderError::new(self.name(), source))
    }
}

/// Converts one [`DiskIoSample`] into neutral pipeline samples.
///
/// The emitted names, values and attributes mirror what the agent previously
/// recorded directly into OTLP instruments, so behavior is unchanged.
fn disk_io_samples(sample: &DiskIoSample, timestamp: SystemTime) -> Vec<Sample> {
    let mut attributes = BTreeMap::new();
    attributes.insert(
        DISK_DIRECTION_ATTRIBUTE.to_string(),
        sample.direction.as_str().to_string(),
    );

    let mut samples = Vec::with_capacity(6);
    let mut push = |name: &str, value: SampleValue| {
        samples.push(Sample {
            name: name.to_string(),
            value,
            attributes: attributes.clone(),
            timestamp,
        });
    };

    push(DISK_IO_SAMPLE, SampleValue::Counter(sample.bytes));
    push(
        DISK_OPERATIONS_SAMPLE,
        SampleValue::Counter(sample.operations),
    );
    push(DISK_ERRORS_SAMPLE, SampleValue::Counter(sample.errors));
    if let Some(latency) = sample.average_latency {
        push(
            DISK_LATENCY_SAMPLE,
            SampleValue::GaugeF64(latency.as_secs_f64() * 1_000.0),
        );
    }
    push(
        DISK_QUEUE_DEPTH_SAMPLE,
        SampleValue::GaugeI64(sample.in_flight_operations),
    );
    push(
        DISK_IN_FLIGHT_BYTES_SAMPLE,
        SampleValue::GaugeI64(sample.in_flight_bytes),
    );
    samples
}

#[cfg(test)]
mod tests {
    use super::DISK_DIRECTION_ATTRIBUTE;
    use super::DISK_ERRORS_SAMPLE;
    use super::DISK_IN_FLIGHT_BYTES_SAMPLE;
    use super::DISK_IO_SAMPLE;
    use super::DISK_LATENCY_SAMPLE;
    use super::DISK_OPERATIONS_SAMPLE;
    use super::DISK_QUEUE_DEPTH_SAMPLE;
    use super::disk_io_samples;
    use crate::modules::disk_io::DiskIoDirection;
    use crate::modules::disk_io::DiskIoSample;
    use std::time::Duration;
    use std::time::SystemTime;
    use talia_core::pipeline::SampleValue;

    fn sample_fixture() -> DiskIoSample {
        DiskIoSample {
            direction: DiskIoDirection::Read,
            bytes: 1_000,
            operations: 10,
            errors: 1,
            average_latency: Some(Duration::from_millis(5)),
            in_flight_operations: 3,
            in_flight_bytes: 4_096,
        }
    }

    #[test]
    fn disk_io_sample_converts_to_six_samples() {
        let samples = disk_io_samples(&sample_fixture(), SystemTime::now());

        assert_eq!(samples.len(), 6);
        let names: Vec<_> = samples.iter().map(|sample| sample.name.as_str()).collect();
        assert_eq!(
            names,
            vec![
                DISK_IO_SAMPLE,
                DISK_OPERATIONS_SAMPLE,
                DISK_ERRORS_SAMPLE,
                DISK_LATENCY_SAMPLE,
                DISK_QUEUE_DEPTH_SAMPLE,
                DISK_IN_FLIGHT_BYTES_SAMPLE,
            ]
        );
        for sample in &samples {
            assert_eq!(sample.attributes[DISK_DIRECTION_ATTRIBUTE].as_str(), "read");
        }
        assert_eq!(samples[0].value, SampleValue::Counter(1_000));
        assert_eq!(samples[3].value, SampleValue::GaugeF64(5.0));
        assert_eq!(samples[4].value, SampleValue::GaugeI64(3));
        assert_eq!(samples[5].value, SampleValue::GaugeI64(4_096));
    }

    #[test]
    fn disk_io_sample_without_latency_skips_latency_sample() {
        let mut fixture = sample_fixture();
        fixture.average_latency = None;

        let samples = disk_io_samples(&fixture, SystemTime::now());

        assert_eq!(samples.len(), 5);
        assert!(
            samples
                .iter()
                .all(|sample| sample.name != DISK_LATENCY_SAMPLE)
        );
    }
}
