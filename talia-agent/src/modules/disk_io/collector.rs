//! eBPF-backed disk I/O byte accounting.

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

/// One host-wide disk I/O sample.
#[derive(Clone, Debug, PartialEq)]
pub struct DiskIoSample {
    /// I/O direction.
    pub direction: DiskIoDirection,
    /// Bytes observed during the sampling window.
    pub bytes: u64,
    /// Operations observed during the sampling window.
    pub operations: u64,
    /// Errors observed during the sampling window.
    pub errors: u64,
    /// Average issue-to-completion latency for completed operations.
    pub average_latency: Option<Duration>,
    /// Current in-flight request count for this direction.
    pub in_flight_operations: i64,
    /// Current in-flight request bytes for this direction.
    pub in_flight_bytes: i64,
}

/// Disk I/O samples for one collection window.
#[derive(Clone, Debug, PartialEq)]
pub struct DiskIoSnapshot {
    /// Sampling window represented by these samples.
    pub window_duration: Duration,
    /// Host-wide disk I/O samples.
    pub samples: Vec<DiskIoSample>,
}

/// Disk I/O direction.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DiskIoDirection {
    /// Bytes read from block devices.
    Read,
    /// Bytes written to block devices.
    Write,
}

impl DiskIoDirection {
    /// Returns the OpenTelemetry direction attribute value.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Read => "read",
            Self::Write => "write",
        }
    }
}

/// Errors returned by the eBPF disk I/O collector.
#[derive(Debug, Error)]
pub enum DiskIoCollectorError {
    /// The embedded eBPF object could not be loaded.
    #[error("failed to load Talia disk I/O BPF object: {0}")]
    Load(#[from] aya::EbpfError),
    /// The embedded eBPF object did not contain the expected program.
    #[error("missing Talia disk I/O BPF program {0}")]
    MissingProgram(&'static str),
    /// An eBPF program could not be configured or attached.
    #[error("failed to configure Talia disk I/O BPF program: {0}")]
    Program(#[from] aya::programs::ProgramError),
    /// The embedded eBPF object did not contain the expected map.
    #[error("missing Talia disk I/O BPF map {0}")]
    MissingMap(&'static str),
    /// An eBPF map could not be opened or read.
    #[error("failed to open or read Talia disk I/O BPF map: {0}")]
    Map(#[from] aya::maps::MapError),
}

/// Stateful eBPF collector for lightweight disk I/O traffic metrics.
pub struct DiskIoCollector {
    _bpf: Ebpf,
    disk_io_counters_map: PerCpuHashMap<MapData, RawDiskIoKey, RawDiskIoCounters>,
    previous_disk_io_counters: BTreeMap<RawDiskIoKey, RawDiskIoCounters>,
}

impl DiskIoCollector {
    /// Loads and attaches the eBPF program required for disk I/O collection.
    pub fn load() -> Result<Self, DiskIoCollectorError> {
        let mut bpf = Ebpf::load(include_bytes_aligned!(concat!(
            env!("OUT_DIR"),
            "/talia_disk_io.bpf.o"
        )))?;
        load_tracepoint(&mut bpf, "talia_block_rq_issue", "block", "block_rq_issue")?;
        load_tracepoint(
            &mut bpf,
            "talia_block_rq_complete",
            "block",
            "block_rq_complete",
        )?;
        let disk_io_counters_map =
            PerCpuHashMap::try_from(bpf.take_map("disk_io_counters_per_cpu_map").ok_or(
                DiskIoCollectorError::MissingMap("disk_io_counters_per_cpu_map"),
            )?)?;
        Ok(Self {
            _bpf: bpf,
            disk_io_counters_map,
            previous_disk_io_counters: BTreeMap::new(),
        })
    }

    /// Collects host-wide disk I/O snapshots.
    pub fn collect(
        &mut self,
        window_duration: Duration,
    ) -> Result<DiskIoSnapshot, DiskIoCollectorError> {
        let mut samples = Vec::new();
        for key in self.disk_io_counters_map.keys() {
            let key = key?;
            let Some(direction) = key.direction() else {
                continue;
            };
            let current = sum_counters(&self.disk_io_counters_map.get(&key, 0)?);
            let previous = self
                .previous_disk_io_counters
                .insert(key, current)
                .unwrap_or_default();
            let bytes = current.bytes.saturating_sub(previous.bytes);
            let operations = current.operations.saturating_sub(previous.operations);
            let latency_ns = current.latency_ns.saturating_sub(previous.latency_ns);
            let errors = current.errors.saturating_sub(previous.errors);
            samples.push(DiskIoSample {
                direction,
                bytes,
                operations,
                errors,
                average_latency: average_latency(latency_ns, operations),
                in_flight_operations: current.in_flight_operations,
                in_flight_bytes: current.in_flight_bytes,
            });
        }
        Ok(DiskIoSnapshot {
            window_duration,
            samples,
        })
    }
}

fn average_latency(latency_ns: u64, operations: u64) -> Option<Duration> {
    if operations == 0 {
        return None;
    }
    Some(Duration::from_nanos(latency_ns / operations))
}

fn load_tracepoint(
    bpf: &mut Ebpf,
    program_name: &'static str,
    category: &'static str,
    name: &'static str,
) -> Result<(), DiskIoCollectorError> {
    let program: &mut TracePoint = bpf
        .program_mut(program_name)
        .ok_or(DiskIoCollectorError::MissingProgram(program_name))?
        .try_into()?;
    program.load()?;
    program.attach(category, name)?;
    Ok(())
}

fn sum_counters(counters: &PerCpuValues<RawDiskIoCounters>) -> RawDiskIoCounters {
    counters
        .iter()
        .copied()
        .fold(RawDiskIoCounters::default(), |mut total, value| {
            total.bytes = total.bytes.saturating_add(value.bytes);
            total.operations = total.operations.saturating_add(value.operations);
            total.latency_ns = total.latency_ns.saturating_add(value.latency_ns);
            total.errors = total.errors.saturating_add(value.errors);
            total.in_flight_bytes = total.in_flight_bytes.saturating_add(value.in_flight_bytes);
            total.in_flight_operations = total
                .in_flight_operations
                .saturating_add(value.in_flight_operations);
            total
        })
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, Ord, PartialEq, PartialOrd)]
struct RawDiskIoKey {
    direction: u8,
    _pad: [u8; 7],
}

impl RawDiskIoKey {
    fn direction(self) -> Option<DiskIoDirection> {
        match self.direction {
            1 => Some(DiskIoDirection::Read),
            2 => Some(DiskIoDirection::Write),
            _ => None,
        }
    }
}

unsafe impl Pod for RawDiskIoKey {}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
struct RawDiskIoCounters {
    bytes: u64,
    operations: u64,
    latency_ns: u64,
    errors: u64,
    in_flight_bytes: i64,
    in_flight_operations: i64,
}

unsafe impl Pod for RawDiskIoCounters {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn raw_key_decodes_direction() {
        // Given: the scenario for raw key decodes direction is prepared.
        let key = RawDiskIoKey {
            direction: 2,
            _pad: [0; 7],
        };

        // When: the behavior under test runs.
        // Then: the assertions confirm that raw key decodes direction.
        assert_eq!(key.direction(), Some(DiskIoDirection::Write));
    }

    #[test]
    fn average_latency_uses_completed_operations() {
        // Given: the scenario for average latency uses completed operations is prepared.
        // When: the behavior under test runs.
        let latency = average_latency(12_000, 3);

        // Then: the assertions confirm that average latency uses completed operations.
        assert_eq!(latency, Some(Duration::from_nanos(4_000)));
        assert_eq!(average_latency(12_000, 0), None);
    }
}
