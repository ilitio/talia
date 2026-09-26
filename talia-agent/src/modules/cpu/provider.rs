//! [`Provider`](talia_core::pipeline::Provider) implementation for CPU
//! scheduler accounting.

use std::collections::BTreeMap;
use std::time::Duration;
use std::time::SystemTime;

use talia_core::pipeline::Provider;
use talia_core::pipeline::ProviderError;
use talia_core::pipeline::Sample;
use talia_core::pipeline::SampleValue;

use crate::modules::cpu::CpuCollector;
use crate::modules::cpu::CpuSnapshot;

/// Sample name for the fraction of time a CPU spent in each state.
pub const CPU_UTILIZATION_SAMPLE: &str = "system.cpu.utilization";

/// Attribute carrying the logical CPU number.
pub const CPU_LOGICAL_NUMBER_ATTRIBUTE: &str = "system.cpu.logical_number";
/// Attribute carrying the CPU state (`idle`, `user`, `system`).
pub const CPU_STATE_ATTRIBUTE: &str = "system.cpu.state";

/// CPU scheduler accounting data provider.
///
/// Wraps the eBPF [`CpuCollector`]; loading attaches the eBPF program and can
/// fail, so construction is fallible. The sampling window tracks the agent's
/// configured CPU interval and is refreshed by the collection loop before
/// every collection.
pub struct CpuProvider {
    collector: CpuCollector,
    window: Duration,
}

impl CpuProvider {
    /// Loads the eBPF program and creates a provider for the given window.
    pub fn load(window: Duration) -> Result<Self, ProviderError> {
        CpuCollector::load()
            .map(|collector| Self { collector, window })
            .map_err(|source| ProviderError::new("cpu", source))
    }

    /// Updates the sampling window, e.g. after a config change.
    pub fn set_window(&mut self, window: Duration) {
        self.window = window;
    }
}

impl Provider for CpuProvider {
    fn name(&self) -> &'static str {
        "cpu"
    }

    fn collect(&mut self) -> Result<Vec<Sample>, ProviderError> {
        self.collector
            .collect(self.window)
            .map(|snapshots| {
                let timestamp = SystemTime::now();
                snapshots
                    .iter()
                    .flat_map(|snapshot| cpu_samples(snapshot, timestamp))
                    .collect()
            })
            .map_err(|source| ProviderError::new(self.name(), source))
    }
}

/// Converts one [`CpuSnapshot`] into neutral pipeline samples.
///
/// The emitted names, values and attributes mirror what the agent previously
/// recorded directly into OTLP instruments, so behavior is unchanged.
fn cpu_samples(snapshot: &CpuSnapshot, timestamp: SystemTime) -> Vec<Sample> {
    let cpu = snapshot.cpu.to_string();
    [
        ("idle", snapshot.idle_ratio()),
        ("user", snapshot.user_ratio()),
        ("system", snapshot.system_ratio()),
    ]
    .into_iter()
    .map(|(state, ratio)| {
        let mut attributes = BTreeMap::new();
        attributes.insert(CPU_LOGICAL_NUMBER_ATTRIBUTE.to_string(), cpu.clone());
        attributes.insert(CPU_STATE_ATTRIBUTE.to_string(), state.to_string());
        Sample {
            name: CPU_UTILIZATION_SAMPLE.to_string(),
            value: SampleValue::GaugeF64(ratio),
            attributes,
            timestamp,
        }
    })
    .collect()
}

#[cfg(test)]
mod tests {
    use super::CPU_LOGICAL_NUMBER_ATTRIBUTE;
    use super::CPU_STATE_ATTRIBUTE;
    use super::CPU_UTILIZATION_SAMPLE;
    use super::cpu_samples;
    use crate::modules::cpu::CpuSnapshot;
    use std::time::Duration;
    use std::time::SystemTime;
    use talia_core::pipeline::SampleValue;

    #[test]
    fn cpu_snapshot_converts_to_three_state_samples() {
        let snapshot = CpuSnapshot {
            cpu: 2,
            window_duration: Duration::from_secs(10),
            idle_ns: 6_000_000_000,
            user_ns: 3_000_000_000,
            system_ns: 1_000_000_000,
        };

        let samples = cpu_samples(&snapshot, SystemTime::now());

        assert_eq!(samples.len(), 3);
        let states: Vec<_> = samples
            .iter()
            .map(|sample| sample.attributes[CPU_STATE_ATTRIBUTE].as_str())
            .collect();
        assert_eq!(states, vec!["idle", "user", "system"]);
        for sample in &samples {
            assert_eq!(sample.name, CPU_UTILIZATION_SAMPLE);
            assert_eq!(
                sample.attributes[CPU_LOGICAL_NUMBER_ATTRIBUTE].as_str(),
                "2"
            );
            assert!(matches!(sample.value, SampleValue::GaugeF64(_)));
        }
        let ratios: Vec<f64> = samples
            .iter()
            .map(|sample| match sample.value {
                SampleValue::GaugeF64(ratio) => ratio,
                _ => panic!("expected gauge"),
            })
            .collect();
        assert_eq!(ratios, vec![0.6, 0.3, 0.1]);
    }
}
