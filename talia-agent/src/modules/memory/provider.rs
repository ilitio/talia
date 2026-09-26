//! [`Provider`](talia_core::pipeline::Provider) implementation for memory pressure.

use std::collections::BTreeMap;
use std::time::SystemTime;

use talia_core::pipeline::Provider;
use talia_core::pipeline::ProviderError;
use talia_core::pipeline::Sample;
use talia_core::pipeline::SampleValue;

use crate::modules::memory::pressure::MemoryCollector;
use crate::modules::memory::pressure::MemorySample;

/// Sample name for physical memory bytes, split by the `system.memory.state`
/// attribute (`total`, `used`, `available`, `free`, `cached`).
pub const MEMORY_USAGE_SAMPLE: &str = "system.memory.usage";
/// Sample name for the fraction of memory that is not available.
pub const MEMORY_UTILIZATION_SAMPLE: &str = "system.memory.utilization";
/// Sample name for swap bytes, split by the `system.linux.memory.swap.state`
/// attribute (`total`, `used`, `free`).
pub const SWAP_USAGE_SAMPLE: &str = "system.linux.memory.swap.usage";
/// Sample name for the fraction of swap currently used.
pub const SWAP_UTILIZATION_SAMPLE: &str = "system.linux.memory.swap.utilization";
/// Sample name for swap I/O bytes, split by the
/// `system.linux.memory.swap.direction` attribute (`in`, `out`).
pub const SWAP_IO_SAMPLE: &str = "system.linux.memory.swap.io";

/// Attribute carrying the memory usage state.
pub const MEMORY_STATE_ATTRIBUTE: &str = "system.memory.state";
/// Attribute carrying the swap usage state.
pub const SWAP_STATE_ATTRIBUTE: &str = "system.linux.memory.swap.state";
/// Attribute carrying the swap I/O direction.
pub const SWAP_DIRECTION_ATTRIBUTE: &str = "system.linux.memory.swap.direction";

/// Memory pressure data provider.
pub struct MemoryProvider {
    collector: MemoryCollector,
}

impl MemoryProvider {
    /// Creates a provider reading host memory pressure.
    pub fn new() -> Self {
        Self {
            collector: MemoryCollector::new(),
        }
    }
}

impl Default for MemoryProvider {
    fn default() -> Self {
        Self::new()
    }
}

impl Provider for MemoryProvider {
    fn name(&self) -> &'static str {
        "memory"
    }

    fn collect(&mut self) -> Result<Vec<Sample>, ProviderError> {
        self.collector
            .collect()
            .map(|sample| memory_samples(&sample))
            .map_err(|source| ProviderError::new(self.name(), source))
    }
}

/// Converts one [`MemorySample`] into neutral pipeline samples.
///
/// The emitted names, values and attributes mirror what the agent previously
/// recorded directly into OTLP instruments, so behavior is unchanged.
fn memory_samples(sample: &MemorySample) -> Vec<Sample> {
    let timestamp = SystemTime::now();
    let mut samples = Vec::with_capacity(12);

    for (state, bytes) in [
        ("total", sample.total_bytes),
        ("used", sample.used_bytes),
        ("available", sample.available_bytes),
        ("free", sample.free_bytes),
        ("cached", sample.cached_bytes),
    ] {
        samples.push(Sample {
            name: MEMORY_USAGE_SAMPLE.to_string(),
            value: SampleValue::GaugeU64(bytes),
            attributes: state_attributes(MEMORY_STATE_ATTRIBUTE, state),
            timestamp,
        });
    }
    samples.push(Sample {
        name: MEMORY_UTILIZATION_SAMPLE.to_string(),
        value: SampleValue::GaugeF64(sample.used_ratio),
        attributes: BTreeMap::new(),
        timestamp,
    });

    for (state, bytes) in [
        ("total", sample.swap_total_bytes),
        ("used", sample.swap_used_bytes),
        ("free", sample.swap_free_bytes),
    ] {
        samples.push(Sample {
            name: SWAP_USAGE_SAMPLE.to_string(),
            value: SampleValue::GaugeU64(bytes),
            attributes: state_attributes(SWAP_STATE_ATTRIBUTE, state),
            timestamp,
        });
    }
    samples.push(Sample {
        name: SWAP_UTILIZATION_SAMPLE.to_string(),
        value: SampleValue::GaugeF64(sample.swap_used_ratio),
        attributes: BTreeMap::new(),
        timestamp,
    });

    for (direction, bytes) in [("in", sample.swap_in_bytes), ("out", sample.swap_out_bytes)] {
        samples.push(Sample {
            name: SWAP_IO_SAMPLE.to_string(),
            value: SampleValue::Counter(bytes),
            attributes: state_attributes(SWAP_DIRECTION_ATTRIBUTE, direction),
            timestamp,
        });
    }

    samples
}

/// Builds the single state/direction attribute shared by grouped samples.
fn state_attributes(key: &str, value: &str) -> BTreeMap<String, String> {
    let mut attributes = BTreeMap::new();
    attributes.insert(key.to_string(), value.to_string());
    attributes
}

#[cfg(test)]
mod tests {
    use super::MEMORY_STATE_ATTRIBUTE;
    use super::MEMORY_USAGE_SAMPLE;
    use super::MEMORY_UTILIZATION_SAMPLE;
    use super::MemoryProvider;
    use super::SWAP_DIRECTION_ATTRIBUTE;
    use super::SWAP_IO_SAMPLE;
    use super::SWAP_STATE_ATTRIBUTE;
    use super::SWAP_USAGE_SAMPLE;
    use super::SWAP_UTILIZATION_SAMPLE;
    use super::memory_samples;
    use crate::modules::memory::pressure::MemorySample;
    use talia_core::pipeline::Provider;
    use talia_core::pipeline::SampleValue;

    fn sample_fixture() -> MemorySample {
        MemorySample {
            total_bytes: 100,
            available_bytes: 60,
            used_bytes: 40,
            free_bytes: 30,
            cached_bytes: 30,
            swap_total_bytes: 50,
            swap_free_bytes: 40,
            swap_used_bytes: 10,
            used_ratio: 0.4,
            swap_used_ratio: 0.2,
            swap_in_bytes: 7,
            swap_out_bytes: 8,
        }
    }

    #[test]
    fn provider_name_is_memory() {
        assert_eq!(MemoryProvider::new().name(), "memory");
    }

    #[test]
    fn memory_sample_converts_to_twelve_samples() {
        let samples = memory_samples(&sample_fixture());
        assert_eq!(samples.len(), 12);

        let usage: Vec<_> = samples
            .iter()
            .filter(|sample| sample.name == MEMORY_USAGE_SAMPLE)
            .collect();
        assert_eq!(usage.len(), 5);
        let states: Vec<_> = usage
            .iter()
            .map(|sample| sample.attributes[MEMORY_STATE_ATTRIBUTE].as_str())
            .collect();
        assert_eq!(states, vec!["total", "used", "available", "free", "cached"]);
        assert!(
            usage
                .iter()
                .all(|sample| matches!(sample.value, SampleValue::GaugeU64(_)))
        );

        let utilization = samples
            .iter()
            .find(|sample| sample.name == MEMORY_UTILIZATION_SAMPLE)
            .expect("utilization sample present");
        assert_eq!(utilization.value, SampleValue::GaugeF64(0.4));
        assert!(utilization.attributes.is_empty());

        let swap_usage: Vec<_> = samples
            .iter()
            .filter(|sample| sample.name == SWAP_USAGE_SAMPLE)
            .collect();
        assert_eq!(swap_usage.len(), 3);
        let swap_states: Vec<_> = swap_usage
            .iter()
            .map(|sample| sample.attributes[SWAP_STATE_ATTRIBUTE].as_str())
            .collect();
        assert_eq!(swap_states, vec!["total", "used", "free"]);

        let swap_utilization = samples
            .iter()
            .find(|sample| sample.name == SWAP_UTILIZATION_SAMPLE)
            .expect("swap utilization sample present");
        assert_eq!(swap_utilization.value, SampleValue::GaugeF64(0.2));

        let swap_io: Vec<_> = samples
            .iter()
            .filter(|sample| sample.name == SWAP_IO_SAMPLE)
            .collect();
        assert_eq!(swap_io.len(), 2);
        assert!(
            swap_io
                .iter()
                .all(|sample| matches!(sample.value, SampleValue::Counter(_)))
        );
        let directions: Vec<_> = swap_io
            .iter()
            .map(|sample| sample.attributes[SWAP_DIRECTION_ATTRIBUTE].as_str())
            .collect();
        assert_eq!(directions, vec!["in", "out"]);
    }
}
