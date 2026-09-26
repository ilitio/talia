//! [`Processor`](talia_core::pipeline::Processor) that keeps samples by name.

use talia_core::pipeline::Processor;
use talia_core::pipeline::Sample;

/// Keeps samples whose name starts with one of the configured prefixes,
/// drops the rest. An empty prefix list keeps everything.
pub struct NameFilter {
    prefixes: Vec<String>,
}

impl NameFilter {
    /// Creates a filter keeping names starting with any of `prefixes`.
    pub fn new(prefixes: Vec<String>) -> Self {
        Self { prefixes }
    }
}

impl Processor for NameFilter {
    fn name(&self) -> &'static str {
        "name_filter"
    }

    fn process(&self, sample: Sample) -> Option<Sample> {
        if self.prefixes.is_empty()
            || self
                .prefixes
                .iter()
                .any(|prefix| sample.name.starts_with(prefix))
        {
            Some(sample)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::time::SystemTime;

    use super::NameFilter;
    use talia_core::pipeline::Processor;
    use talia_core::pipeline::Sample;
    use talia_core::pipeline::SampleValue;

    fn sample_named(name: &str) -> Sample {
        Sample {
            name: name.to_string(),
            value: SampleValue::Counter(1),
            attributes: BTreeMap::new(),
            timestamp: SystemTime::now(),
        }
    }

    #[test]
    fn keeps_names_matching_a_prefix() {
        let filter = NameFilter::new(vec!["system.cpu.".to_string()]);
        let kept = filter.process(sample_named("system.cpu.usage"));
        assert_eq!(
            kept.map(|sample| sample.name),
            Some("system.cpu.usage".to_string())
        );
    }

    #[test]
    fn drops_names_matching_no_prefix() {
        let filter = NameFilter::new(vec!["system.cpu.".to_string()]);
        assert!(
            filter
                .process(sample_named("system.memory.usage"))
                .is_none()
        );
    }

    #[test]
    fn empty_prefix_list_keeps_everything() {
        let filter = NameFilter::new(vec![]);
        let kept = filter.process(sample_named("anything.at.all"));
        assert!(kept.is_some());
    }
}
