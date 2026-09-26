//! [`Provider`](talia_core::pipeline::Provider) implementation for filesystem storage.

use std::collections::BTreeMap;
use std::time::SystemTime;

use crate::modules::storage::filesystem::FilesystemSample;
use crate::modules::storage::filesystem::collect_filesystems;
use talia_core::config::AgentRuntimeConfig;
use talia_core::pipeline::Provider;
use talia_core::pipeline::ProviderError;
use talia_core::pipeline::Sample;
use talia_core::pipeline::SampleValue;

/// Sample name for total filesystem capacity in bytes.
pub const FILESYSTEM_LIMIT_SAMPLE: &str = "system.filesystem.limit";
/// Sample name for filesystem bytes, split by the `system.filesystem.state`
/// attribute (`used`, `free`, `reserved`).
pub const FILESYSTEM_USAGE_SAMPLE: &str = "system.filesystem.usage";
/// Sample name for the fraction of filesystem bytes currently used.
pub const FILESYSTEM_UTILIZATION_SAMPLE: &str = "system.filesystem.utilization";

/// Attribute carrying the mountpoint a sample was taken from.
pub const MOUNTPOINT_ATTRIBUTE: &str = "system.filesystem.mountpoint";
/// Attribute carrying the filesystem type, when known.
pub const FILESYSTEM_TYPE_ATTRIBUTE: &str = "system.filesystem.type";
/// Attribute carrying the mount mode (`ro`/`rw`), when known.
pub const FILESYSTEM_MODE_ATTRIBUTE: &str = "system.filesystem.mode";
/// Attribute carrying the usage state (`used`, `free`, `reserved`).
pub const FILESYSTEM_STATE_ATTRIBUTE: &str = "system.filesystem.state";

/// Filesystem storage data provider.
pub struct StorageProvider {
    mounts: Vec<String>,
}

impl StorageProvider {
    /// Creates a provider collecting the given mountpoints.
    pub fn new(mounts: Vec<String>) -> Self {
        Self { mounts }
    }
}

impl Provider for StorageProvider {
    fn name(&self) -> &'static str {
        "storage"
    }

    fn collect(&mut self) -> Result<Vec<Sample>, ProviderError> {
        collect_filesystems(&self.mounts)
            .map(|samples| samples.iter().flat_map(filesystem_samples).collect())
            .map_err(|source| ProviderError::new(self.name(), source))
    }

    /// Picks up mount list changes without rebuilding the provider.
    fn reconfigure(&mut self, config: &AgentRuntimeConfig) {
        if self.mounts != config.storage.mounts {
            self.mounts = config.storage.mounts.clone();
        }
    }
}

/// Converts one [`FilesystemSample`] into neutral pipeline samples.
///
/// The emitted names, values and attributes mirror what the agent previously
/// recorded directly into OTLP instruments, so behavior is unchanged.
fn filesystem_samples(sample: &FilesystemSample) -> Vec<Sample> {
    let timestamp = SystemTime::now();
    let base_attributes = base_attributes(sample);

    let mut samples = Vec::with_capacity(5);
    samples.push(Sample {
        name: FILESYSTEM_LIMIT_SAMPLE.to_string(),
        value: SampleValue::GaugeU64(sample.limit_bytes),
        attributes: base_attributes.clone(),
        timestamp,
    });

    for (state, bytes) in [
        ("used", sample.used_bytes),
        ("free", sample.free_bytes),
        ("reserved", sample.reserved_bytes),
    ] {
        let mut attributes = base_attributes.clone();
        attributes.insert(FILESYSTEM_STATE_ATTRIBUTE.to_string(), state.to_string());
        samples.push(Sample {
            name: FILESYSTEM_USAGE_SAMPLE.to_string(),
            value: SampleValue::GaugeU64(bytes),
            attributes,
            timestamp,
        });
    }

    let mut utilization_attributes = base_attributes;
    utilization_attributes.insert(FILESYSTEM_STATE_ATTRIBUTE.to_string(), "used".to_string());
    samples.push(Sample {
        name: FILESYSTEM_UTILIZATION_SAMPLE.to_string(),
        value: SampleValue::GaugeF64(sample.used_ratio),
        attributes: utilization_attributes,
        timestamp,
    });

    samples
}

/// Attributes shared by every sample from one mountpoint.
fn base_attributes(sample: &FilesystemSample) -> BTreeMap<String, String> {
    let mut attributes = BTreeMap::new();
    attributes.insert(MOUNTPOINT_ATTRIBUTE.to_string(), sample.mountpoint.clone());
    if let Some(filesystem_type) = &sample.filesystem_type {
        attributes.insert(
            FILESYSTEM_TYPE_ATTRIBUTE.to_string(),
            filesystem_type.clone(),
        );
    }
    if let Some(mode) = &sample.mode {
        attributes.insert(FILESYSTEM_MODE_ATTRIBUTE.to_string(), mode.clone());
    }
    attributes
}

#[cfg(test)]
mod tests {
    use super::FILESYSTEM_LIMIT_SAMPLE;
    use super::FILESYSTEM_STATE_ATTRIBUTE;
    use super::FILESYSTEM_USAGE_SAMPLE;
    use super::FILESYSTEM_UTILIZATION_SAMPLE;
    use super::MOUNTPOINT_ATTRIBUTE;
    use super::StorageProvider;
    use super::filesystem_samples;
    use crate::modules::storage::filesystem::FilesystemSample;
    use talia_core::pipeline::Provider;
    use talia_core::pipeline::SampleValue;

    fn sample_fixture() -> FilesystemSample {
        FilesystemSample {
            mountpoint: "/".to_string(),
            filesystem_type: Some("ext4".to_string()),
            mode: Some("rw".to_string()),
            limit_bytes: 100,
            used_bytes: 40,
            free_bytes: 50,
            reserved_bytes: 10,
            used_ratio: 0.4,
        }
    }

    #[test]
    fn provider_name_is_storage() {
        assert_eq!(StorageProvider::new(Vec::new()).name(), "storage");
    }

    #[test]
    fn filesystem_sample_converts_to_five_samples() {
        let samples = filesystem_samples(&sample_fixture());
        assert_eq!(samples.len(), 5);

        assert_eq!(samples[0].name, FILESYSTEM_LIMIT_SAMPLE);
        assert_eq!(samples[0].value, SampleValue::GaugeU64(100));
        assert_eq!(samples[0].attributes[MOUNTPOINT_ATTRIBUTE], "/");

        let usage: Vec<_> = samples
            .iter()
            .filter(|sample| sample.name == FILESYSTEM_USAGE_SAMPLE)
            .collect();
        assert_eq!(usage.len(), 3);
        let states: Vec<_> = usage
            .iter()
            .map(|sample| sample.attributes[FILESYSTEM_STATE_ATTRIBUTE].as_str())
            .collect();
        assert_eq!(states, vec!["used", "free", "reserved"]);

        let utilization = samples
            .iter()
            .find(|sample| sample.name == FILESYSTEM_UTILIZATION_SAMPLE)
            .expect("utilization sample present");
        assert_eq!(utilization.value, SampleValue::GaugeF64(0.4));
        assert_eq!(utilization.attributes[FILESYSTEM_STATE_ATTRIBUTE], "used");
    }
}
