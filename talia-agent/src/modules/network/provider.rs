//! [`Provider`](talia_core::pipeline::Provider) implementation for network
//! traffic accounting.

use std::collections::BTreeMap;
use std::time::Duration;
use std::time::SystemTime;

use talia_core::pipeline::Provider;
use talia_core::pipeline::ProviderError;
use talia_core::pipeline::Sample;
use talia_core::pipeline::SampleValue;

use crate::modules::network::NetworkCollector;
use crate::modules::network::NetworkSample;

/// Sample name for host-wide network bytes, split by the
/// `network.io.direction` attribute (`receive`, `transmit`).
pub const NETWORK_IO_SAMPLE: &str = "system.network.io";

/// Attribute carrying the traffic direction.
pub const NETWORK_DIRECTION_ATTRIBUTE: &str = "network.io.direction";

/// Network traffic accounting data provider.
///
/// Wraps the eBPF [`NetworkCollector`]; loading attaches the eBPF program and
/// can fail, so construction is fallible. The sampling window tracks the
/// agent's configured network interval and is refreshed by the collection
/// loop before every collection.
pub struct NetworkProvider {
    collector: NetworkCollector,
    window: Duration,
}

impl NetworkProvider {
    /// Loads the eBPF program and creates a provider for the given window.
    pub fn load(window: Duration) -> Result<Self, ProviderError> {
        NetworkCollector::load()
            .map(|collector| Self { collector, window })
            .map_err(|source| ProviderError::new("network", source))
    }
}

impl Provider for NetworkProvider {
    fn name(&self) -> &'static str {
        "network"
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
                    .map(|sample| network_sample(sample, timestamp))
                    .collect()
            })
            .map_err(|source| ProviderError::new(self.name(), source))
    }
}

/// Converts one [`NetworkSample`] into a neutral pipeline sample.
///
/// The emitted name, value and attributes mirror what the agent previously
/// recorded directly into OTLP instruments, so behavior is unchanged. Packet
/// counts are observed but were never exported as metrics, so they stay out
/// of the samples.
fn network_sample(sample: &NetworkSample, timestamp: SystemTime) -> Sample {
    let mut attributes = BTreeMap::new();
    attributes.insert(
        NETWORK_DIRECTION_ATTRIBUTE.to_string(),
        sample.direction.as_str().to_string(),
    );
    Sample {
        name: NETWORK_IO_SAMPLE.to_string(),
        value: SampleValue::Counter(sample.bytes),
        attributes,
        timestamp,
    }
}

#[cfg(test)]
mod tests {
    use super::NETWORK_DIRECTION_ATTRIBUTE;
    use super::NETWORK_IO_SAMPLE;
    use super::network_sample;
    use crate::modules::network::NetworkDirection;
    use crate::modules::network::NetworkSample;
    use std::time::SystemTime;
    use talia_core::pipeline::SampleValue;

    #[test]
    fn network_sample_converts_to_one_counter_sample() {
        let sample = NetworkSample {
            direction: NetworkDirection::Ingress,
            bytes: 1_234,
            packets: 56,
        };

        let converted = network_sample(&sample, SystemTime::now());

        assert_eq!(converted.name, NETWORK_IO_SAMPLE);
        assert_eq!(converted.value, SampleValue::Counter(1_234));
        assert_eq!(
            converted.attributes[NETWORK_DIRECTION_ATTRIBUTE].as_str(),
            "receive"
        );
    }
}
