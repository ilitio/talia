//! Sink trait: destinations for collected samples.

use super::sample::Sample;

/// A destination that exports collected [`Sample`]s.
///
/// Sinks own every transport-specific decision: instrument names, labels,
/// formats, and endpoints. Providers stay transport-agnostic; they only
/// produce neutral samples.
pub trait Sink: Send + Sync {
    /// Stable sink name, e.g. `"otlp"`.
    fn name(&self) -> &'static str;

    /// Emits one neutral sample to the destination.
    ///
    /// `config_version` is Talia's runtime config version; sinks may attach
    /// it as metadata or ignore it.
    fn emit(&self, sample: &Sample, config_version: &str);
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Mutex;
    use std::time::SystemTime;

    use super::super::sample::SampleValue;
    use super::*;

    /// A sink that records what it was asked to emit, for tests.
    struct RecordingSink {
        emitted: Mutex<Vec<(String, String)>>,
    }

    impl Sink for RecordingSink {
        fn name(&self) -> &'static str {
            "recording"
        }

        fn emit(&self, sample: &Sample, config_version: &str) {
            self.emitted
                .lock()
                .unwrap()
                .push((sample.name.clone(), config_version.to_string()));
        }
    }

    #[test]
    fn sink_receives_samples_through_a_trait_object() {
        // Given: a sink held behind `dyn Sink`, the way agent loops hold it.
        let recording = RecordingSink {
            emitted: Mutex::new(Vec::new()),
        };
        let sink: &dyn Sink = &recording;
        let sample = Sample {
            name: "system.cpu.utilization".to_string(),
            value: SampleValue::GaugeF64(0.5),
            attributes: BTreeMap::new(),
            timestamp: SystemTime::now(),
        };

        // When: emitting one sample.
        sink.emit(&sample, "v42");

        // Then: the sink observed the sample name and the config version.
        assert_eq!(sink.name(), "recording");
        assert_eq!(
            *recording.emitted.lock().unwrap(),
            [("system.cpu.utilization".to_string(), "v42".to_string())]
        );
    }
}
