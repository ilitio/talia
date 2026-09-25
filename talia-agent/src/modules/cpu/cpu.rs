//! eBPF-backed CPU collector for lightweight scheduler accounting.

use std::convert::TryInto;
use std::time::Duration;

use aya::Btf;
use aya::Ebpf;
use aya::Pod;
use aya::include_bytes_aligned;
use aya::maps::MapData;
use aya::maps::PerCpuArray;
use aya::maps::PerCpuValues;
use aya::programs::BtfTracePoint;
use thiserror::Error;

/// CPU scheduler activity observed for one logical CPU.
#[derive(Clone, Debug, PartialEq)]
pub struct CpuSnapshot {
    /// Logical CPU number.
    pub cpu: u32,
    /// Sampling window represented by this snapshot.
    pub window_duration: Duration,
    /// Nanoseconds spent with the idle task scheduled.
    pub idle_ns: u64,
    /// Nanoseconds spent running regular user tasks.
    pub user_ns: u64,
    /// Nanoseconds spent running kernel threads.
    pub system_ns: u64,
}

impl CpuSnapshot {
    /// Returns total accounted nanoseconds accumulated during the sampling window.
    pub fn total_ns(&self) -> u64 {
        self.idle_ns
            .saturating_add(self.user_ns)
            .saturating_add(self.system_ns)
    }

    /// Returns the idle fraction for this CPU over the sampling window.
    pub fn idle_ratio(&self) -> f64 {
        ratio(self.idle_ns, self.total_ns())
    }

    /// Returns the user-task fraction for this CPU over the sampling window.
    pub fn user_ratio(&self) -> f64 {
        ratio(self.user_ns, self.total_ns())
    }

    /// Returns the kernel-thread fraction for this CPU over the sampling window.
    pub fn system_ratio(&self) -> f64 {
        ratio(self.system_ns, self.total_ns())
    }
}

/// Errors returned by the eBPF CPU collector.
#[derive(Debug, Error)]
pub enum CpuCollectorError {
    /// The embedded eBPF object could not be loaded.
    #[error("failed to load Talia CPU BPF object: {0}")]
    Load(#[from] aya::EbpfError),
    /// Kernel BTF metadata could not be loaded.
    #[error("failed to load kernel BTF for Talia CPU collector: {0}")]
    Btf(#[from] aya::BtfError),
    /// The embedded eBPF object did not contain the expected program.
    #[error("missing Talia CPU BPF program {0}")]
    MissingProgram(&'static str),
    /// An eBPF program could not be configured or attached.
    #[error("failed to configure Talia CPU BPF program: {0}")]
    Program(#[from] aya::programs::ProgramError),
    /// The embedded eBPF object did not contain the expected map.
    #[error("missing Talia CPU BPF map {0}")]
    MissingMap(&'static str),
    /// An eBPF map could not be opened or read.
    #[error("failed to open or read Talia CPU BPF map: {0}")]
    Map(#[from] aya::maps::MapError),
}

/// Stateful eBPF collector for lightweight Talia CPU metrics.
pub struct CpuCollector {
    _bpf: Ebpf,
    cpu_counters_map: PerCpuArray<MapData, RawCpuCounters>,
    previous_cpu_counters: Vec<RawCpuCounters>,
}

impl CpuCollector {
    /// Loads and attaches the eBPF program required for CPU collection.
    pub fn load() -> Result<Self, CpuCollectorError> {
        let mut bpf = Ebpf::load(include_bytes_aligned!(concat!(
            env!("OUT_DIR"),
            "/talia_cpu.bpf.o"
        )))?;
        let btf = Btf::from_sys_fs()?;
        load_btf_tracepoint(&mut bpf, &btf, "talia_sched_switch", "sched_switch")?;
        let cpu_counters_map = PerCpuArray::try_from(
            bpf.take_map("cpu_counters_per_cpu_map")
                .ok_or(CpuCollectorError::MissingMap("cpu_counters_per_cpu_map"))?,
        )?;
        Ok(Self {
            _bpf: bpf,
            cpu_counters_map,
            previous_cpu_counters: Vec::new(),
        })
    }

    /// Collects CPU snapshots for one sampling window.
    pub fn collect(
        &mut self,
        window_duration: Duration,
    ) -> Result<Vec<CpuSnapshot>, CpuCollectorError> {
        let cpu_counters = self.cpu_counters_map.get(&0, 0)?;
        Ok(self.cpu_snapshots(&cpu_counters, window_duration))
    }

    fn cpu_snapshots(
        &mut self,
        cpu_counters: &PerCpuValues<RawCpuCounters>,
        window_duration: Duration,
    ) -> Vec<CpuSnapshot> {
        if self.previous_cpu_counters.len() < cpu_counters.len() {
            self.previous_cpu_counters
                .resize(cpu_counters.len(), RawCpuCounters::default());
        }

        cpu_counters
            .iter()
            .enumerate()
            .map(|(cpu, current)| {
                let previous = self.previous_cpu_counters[cpu];
                self.previous_cpu_counters[cpu] = *current;
                CpuSnapshot {
                    cpu: cpu as u32,
                    window_duration,
                    idle_ns: current.idle_ns.saturating_sub(previous.idle_ns),
                    user_ns: current.user_ns.saturating_sub(previous.user_ns),
                    system_ns: current.system_ns.saturating_sub(previous.system_ns),
                }
            })
            .collect()
    }
}

fn load_btf_tracepoint(
    bpf: &mut Ebpf,
    btf: &Btf,
    program_name: &'static str,
    tracepoint_name: &'static str,
) -> Result<(), CpuCollectorError> {
    let program: &mut BtfTracePoint = bpf
        .program_mut(program_name)
        .ok_or(CpuCollectorError::MissingProgram(program_name))?
        .try_into()?;
    program.load(tracepoint_name, btf)?;
    program.attach()?;
    Ok(())
}

fn ratio(value: u64, total: u64) -> f64 {
    if total == 0 {
        0.0
    } else {
        value as f64 / total as f64
    }
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
struct RawCpuCounters {
    last_switch_ns: u64,
    active_task_user_ns: u64,
    active_task_system_ns: u64,
    idle_ns: u64,
    user_ns: u64,
    system_ns: u64,
}

unsafe impl Pod for RawCpuCounters {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ratios_use_cpu_time_delta() {
        // Given: the scenario for ratios use cpu time delta is prepared.
        let snapshot = CpuSnapshot {
            cpu: 0,
            window_duration: Duration::from_secs(1),
            idle_ns: 250,
            user_ns: 700,
            system_ns: 50,
        };

        // When: the behavior under test runs.
        // Then: the assertions confirm that ratios use cpu time delta.
        assert_eq!(snapshot.total_ns(), 1_000);
        assert_eq!(snapshot.idle_ratio(), 0.25);
        assert_eq!(snapshot.user_ratio(), 0.7);
        assert_eq!(snapshot.system_ratio(), 0.05);
    }
}
