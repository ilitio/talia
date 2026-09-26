//! Processor plugin trait: transforms or drops samples mid-pipeline.

use std::sync::Arc;

use super::sample::Sample;

/// A pipeline stage between providers and sinks: sees every sample and may
/// change it or drop it.
///
/// Processors are deliberately synchronous like providers: the runner decides
/// *when* samples flow, the processor only decides *what* happens to each one.
/// Stateful processors (e.g. aggregation) should use interior mutability.
pub trait Processor: Send + Sync {
    /// Stable name, e.g. `"name_filter"`. Used in config and CLI output.
    fn name(&self) -> &'static str;

    /// Processes one sample, returning [`None`] to drop it from the pipeline.
    fn process(&self, sample: Sample) -> Option<Sample>;
}

/// Runs a sample through every processor in order.
///
/// Returns [`None`] as soon as one processor drops the sample.
pub fn process_sample(processors: &[Arc<dyn Processor>], sample: Sample) -> Option<Sample> {
    let mut current = sample;
    for processor in processors {
        current = processor.process(current)?;
    }
    Some(current)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::Processor;
    use super::Sample;
    use super::process_sample;
    use crate::pipeline::SampleValue;

    struct KeepAll;
    struct DropAll;
    struct Rename(&'static str);

    impl Processor for KeepAll {
        fn name(&self) -> &'static str {
            "keep_all"
        }

        fn process(&self, sample: Sample) -> Option<Sample> {
            Some(sample)
        }
    }

    impl Processor for DropAll {
        fn name(&self) -> &'static str {
            "drop_all"
        }

        fn process(&self, sample: Sample) -> Option<Sample> {
            let _ = sample;
            None
        }
    }

    impl Processor for Rename {
        fn name(&self) -> &'static str {
            "rename"
        }

        fn process(&self, mut sample: Sample) -> Option<Sample> {
            sample.name = self.0.to_string();
            Some(sample)
        }
    }

    fn sample_named(name: &str) -> Sample {
        Sample {
            name: name.to_string(),
            value: SampleValue::Counter(1),
            attributes: Default::default(),
            timestamp: std::time::SystemTime::now(),
        }
    }

    #[test]
    fn empty_chain_keeps_sample() {
        let processors: Vec<Arc<dyn Processor>> = vec![];
        let sample = process_sample(&processors, sample_named("a")).expect("kept");
        assert_eq!(sample.name, "a");
    }

    #[test]
    fn chain_applies_in_order() {
        let processors: Vec<Arc<dyn Processor>> =
            vec![Arc::new(KeepAll), Arc::new(Rename("b")), Arc::new(KeepAll)];
        let sample = process_sample(&processors, sample_named("a")).expect("kept");
        assert_eq!(sample.name, "b");
    }

    #[test]
    fn chain_stops_at_first_drop() {
        let processors: Vec<Arc<dyn Processor>> =
            vec![Arc::new(KeepAll), Arc::new(DropAll), Arc::new(Rename("b"))];
        assert!(process_sample(&processors, sample_named("a")).is_none());
    }
}
