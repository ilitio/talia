//! eBPF-backed network byte accounting.

use std::collections::BTreeMap;
use std::convert::TryInto;
use std::time::Duration;

use aya::Ebpf;
use aya::Pod;
use aya::include_bytes_aligned;
use aya::maps::MapData;
use aya::maps::PerCpuHashMap;
use aya::maps::PerCpuValues;
use aya::programs::TracePoint;
use thiserror::Error;

/// One host-wide network traffic sample.
#[derive(Clone, Debug, PartialEq)]
pub struct NetworkSample {
    /// Traffic direction.
    pub direction: NetworkDirection,
    /// Bytes observed during the sampling window.
    pub bytes: u64,
    /// Packets observed during the sampling window.
    pub packets: u64,
}

/// Network samples for one collection window.
#[derive(Clone, Debug, PartialEq)]
pub struct NetworkSnapshot {
    /// Sampling window represented by these samples.
    pub window_duration: Duration,
    /// Host-wide traffic samples.
    pub samples: Vec<NetworkSample>,
}

/// Traffic direction.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NetworkDirection {
    /// Bytes received by an interface.
    Ingress,
    /// Bytes transmitted by an interface.
    Egress,
}

impl NetworkDirection {
    /// Returns the OpenTelemetry direction attribute value.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ingress => "receive",
            Self::Egress => "transmit",
        }
    }
}

/// Errors returned by the eBPF network collector.
#[derive(Debug, Error)]
pub enum NetworkCollectorError {
    /// The embedded eBPF object could not be loaded.
    #[error("failed to load Talia network BPF object: {0}")]
    Load(#[from] aya::EbpfError),
    /// The embedded eBPF object did not contain the expected program.
    #[error("missing Talia network BPF program {0}")]
    MissingProgram(&'static str),
    /// An eBPF program could not be configured or attached.
    #[error("failed to configure Talia network BPF program: {0}")]
    Program(#[from] aya::programs::ProgramError),
    /// The embedded eBPF object did not contain the expected map.
    #[error("missing Talia network BPF map {0}")]
    MissingMap(&'static str),
    /// An eBPF map could not be opened or read.
    #[error("failed to open or read Talia network BPF map: {0}")]
    Map(#[from] aya::maps::MapError),
}

/// Stateful eBPF collector for lightweight network traffic metrics.
pub struct NetworkCollector {
    _bpf: Ebpf,
    network_counters_map: PerCpuHashMap<MapData, RawNetworkKey, RawNetworkCounters>,
    previous_network_counters: BTreeMap<RawNetworkKey, RawNetworkCounters>,
}

impl NetworkCollector {
    /// Loads and attaches the eBPF programs required for network collection.
    pub fn load() -> Result<Self, NetworkCollectorError> {
        let mut bpf = Ebpf::load(include_bytes_aligned!(concat!(
            env!("OUT_DIR"),
            "/talia_network.bpf.o"
        )))?;
        load_tracepoint(
            &mut bpf,
            "talia_netif_receive_skb",
            "net",
            "netif_receive_skb",
        )?;
        load_tracepoint(&mut bpf, "talia_net_dev_xmit", "net", "net_dev_xmit")?;
        let network_counters_map =
            PerCpuHashMap::try_from(bpf.take_map("network_counters_per_cpu_map").ok_or(
                NetworkCollectorError::MissingMap("network_counters_per_cpu_map"),
            )?)?;
        Ok(Self {
            _bpf: bpf,
            network_counters_map,
            previous_network_counters: BTreeMap::new(),
        })
    }

    /// Collects host-wide network traffic snapshots.
    pub fn collect(
        &mut self,
        window_duration: Duration,
    ) -> Result<NetworkSnapshot, NetworkCollectorError> {
        let mut samples = Vec::new();
        for key in self.network_counters_map.keys() {
            let key = key?;
            let Some(direction) = key.direction() else {
                continue;
            };
            let current = sum_counters(&self.network_counters_map.get(&key, 0)?);
            let previous = self
                .previous_network_counters
                .insert(key, current)
                .unwrap_or_default();
            let bytes = current.bytes.saturating_sub(previous.bytes);
            let packets = current.packets.saturating_sub(previous.packets);
            samples.push(NetworkSample {
                direction,
                bytes,
                packets,
            });
        }
        Ok(NetworkSnapshot {
            window_duration,
            samples,
        })
    }
}

fn load_tracepoint(
    bpf: &mut Ebpf,
    program_name: &'static str,
    category: &'static str,
    name: &'static str,
) -> Result<(), NetworkCollectorError> {
    let program: &mut TracePoint = bpf
        .program_mut(program_name)
        .ok_or(NetworkCollectorError::MissingProgram(program_name))?
        .try_into()?;
    program.load()?;
    program.attach(category, name)?;
    Ok(())
}

fn sum_counters(counters: &PerCpuValues<RawNetworkCounters>) -> RawNetworkCounters {
    counters
        .iter()
        .copied()
        .fold(RawNetworkCounters::default(), |mut total, value| {
            total.bytes = total.bytes.saturating_add(value.bytes);
            total.packets = total.packets.saturating_add(value.packets);
            total
        })
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, Ord, PartialEq, PartialOrd)]
struct RawNetworkKey {
    direction: u8,
    _pad: [u8; 7],
}

impl RawNetworkKey {
    fn direction(self) -> Option<NetworkDirection> {
        match self.direction {
            1 => Some(NetworkDirection::Ingress),
            2 => Some(NetworkDirection::Egress),
            _ => None,
        }
    }
}

unsafe impl Pod for RawNetworkKey {}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
struct RawNetworkCounters {
    bytes: u64,
    packets: u64,
}

unsafe impl Pod for RawNetworkCounters {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn raw_key_decodes_direction() {
        // Given: the scenario for raw key decodes direction is prepared.
        let key = RawNetworkKey {
            direction: 1,
            _pad: [0; 7],
        };

        // When: the behavior under test runs.
        // Then: the assertions confirm that raw key decodes direction.
        assert_eq!(key.direction(), Some(NetworkDirection::Ingress));
    }
}
