//! [`Sink`](super::super::pipeline::Sink) that prints samples as JSON lines.
//!
//! Used by the `query` CLI command for bpftrace-style one-shot collection.

use std::time::UNIX_EPOCH;

use talia_core::pipeline::Sample;
use talia_core::pipeline::SampleValue;
use talia_core::pipeline::Sink;

/// Prints one JSON object per sample to stdout.
pub struct StdoutSink;

impl StdoutSink {
    /// Creates the sink.
    pub fn new() -> Self {
        Self
    }
}

impl Default for StdoutSink {
    fn default() -> Self {
        Self::new()
    }
}

impl Sink for StdoutSink {
    fn name(&self) -> &'static str {
        "stdout"
    }

    fn emit(&self, sample: &Sample, config_version: &str) {
        let value = match sample.value {
            SampleValue::Counter(value) => serde_json::json!(value),
            SampleValue::GaugeU64(value) => serde_json::json!(value),
            SampleValue::GaugeI64(value) => serde_json::json!(value),
            SampleValue::GaugeF64(value) => serde_json::json!(value),
        };
        let timestamp = sample
            .timestamp
            .duration_since(UNIX_EPOCH)
            .map(|elapsed| elapsed.as_secs_f64())
            .unwrap_or(0.0);
        let line = serde_json::json!({
            "name": sample.name,
            "value": value,
            "attributes": sample.attributes,
            "timestamp": timestamp,
            "config_version": config_version,
        });
        println!("{line}");
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::time::SystemTime;

    use super::StdoutSink;
    use talia_core::pipeline::Sample;
    use talia_core::pipeline::SampleValue;
    use talia_core::pipeline::Sink;

    #[test]
    fn stdout_sink_has_stable_name() {
        assert_eq!(StdoutSink::new().name(), "stdout");
    }

    #[test]
    fn emit_does_not_panic_on_any_value_kind() {
        let sink = StdoutSink::new();
        for value in [
            SampleValue::Counter(1),
            SampleValue::GaugeU64(2),
            SampleValue::GaugeI64(-3),
            SampleValue::GaugeF64(4.5),
        ] {
            sink.emit(
                &Sample {
                    name: "test.sample".to_string(),
                    value,
                    attributes: BTreeMap::new(),
                    timestamp: SystemTime::now(),
                },
                "test-version",
            );
        }
    }
}
