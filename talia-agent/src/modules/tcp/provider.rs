//! [`Provider`](talia_core::pipeline::Provider) implementation for per-pod
//! TCP metrics read from pod network namespaces.
//!
//! Every interval the provider asks [`PodDiscovery`](crate::pod_discovery::PodDiscovery)
//! for the pods on this node, reads `/proc/<pid>/net/snmp` and
//! `/proc/<pid>/net/netstat` inside each pod's network namespace, and emits
//! per-interval counter deltas attributed with the pod's Kubernetes
//! identity.
//!
//! The counters in `/proc/<pid>/net` are cumulative since the network
//! namespace was created, so the provider keeps the previous snapshot per
//! pod UID. The first sighting of a pod only establishes a baseline for the
//! counters; a PID change or a counter decrease (namespace recreated)
//! re-baselines silently. Disappeared pods drop their state. `CurrEstab`
//! is a gauge, so it is emitted on every readable snapshot, even when no
//! baseline exists for the counters.

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::time::SystemTime;

use talia_core::config::AgentRuntimeConfig;
use talia_core::pipeline::Provider;
use talia_core::pipeline::ProviderError;
use talia_core::pipeline::Sample;
use talia_core::pipeline::SampleValue;

use super::proc_net_netstat::parse_netstat;
use super::proc_net_snmp::parse_snmp;
use super::tcp_snapshot::TcpSnapshot;
use crate::pod_discovery::PodDiscovery;
use crate::pod_discovery::PodUid;
use crate::pod_discovery::SocketSelection;

/// Sample name for per-interval retransmitted segments (`RetransSegs` delta).
pub const TCP_RETRANSMITS_SAMPLE: &str = "system.network.tcp.retransmits";
/// Sample name for per-interval sent segments (`OutSegs` delta).
pub const TCP_SEGMENTS_SENT_SAMPLE: &str = "system.network.tcp.segments.sent";
/// Sample name for per-interval received segments (`InSegs` delta).
pub const TCP_SEGMENTS_RECEIVED_SAMPLE: &str = "system.network.tcp.segments.received";
/// Sample name for per-interval retransmission timeouts (`TCPTimeouts` delta).
pub const TCP_TIMEOUTS_SAMPLE: &str = "system.network.tcp.timeouts";
/// Sample name for per-interval fast retransmits (`TCPFastRetrans` delta).
pub const TCP_FAST_RETRANSMITS_SAMPLE: &str = "system.network.tcp.fast_retransmits";
/// Sample name for currently established connections (`CurrEstab` gauge).
pub const TCP_ESTABLISHED_SAMPLE: &str = "system.network.tcp.connections.established";

/// Attribute carrying the pod name.
pub const POD_NAME_ATTRIBUTE: &str = "k8s.pod.name";
/// Attribute carrying the pod's namespace.
pub const POD_NAMESPACE_ATTRIBUTE: &str = "k8s.namespace.name";
/// Attribute carrying the pod UID (stable pod identity).
pub const POD_UID_ATTRIBUTE: &str = "k8s.pod.uid";

/// Previous snapshot for one pod, used to compute deltas.
struct PodBaseline {
    pid: u32,
    snapshot: TcpSnapshot,
}

/// TCP provider with per-pod samples.
///
/// Holds shared [`PodDiscovery`] and per-UID baselines. All failure modes
/// are churn, never fatal: an unreachable CRI socket fails the collection
/// (the runner logs and retries next interval), while a pod that vanished
/// between discovery and reading is simply skipped.
pub struct TcpProvider {
    discovery: PodDiscovery,
    previous: HashMap<PodUid, PodBaseline>,
}

impl TcpProvider {
    /// Creates the provider. Connections to the CRI sockets are lazy: the
    /// first [`collect`](Self::collect) dials them.
    pub fn new() -> Self {
        Self {
            discovery: PodDiscovery::new(SocketSelection::All),
            previous: HashMap::new(),
        }
    }

    /// Reads and parses both procfs files for one pod's network namespace.
    fn read_pod_snapshot(pid: u32) -> Option<TcpSnapshot> {
        let snmp = std::fs::read_to_string(format!("/proc/{pid}/net/snmp")).ok()?;
        let netstat = std::fs::read_to_string(format!("/proc/{pid}/net/netstat")).ok()?;
        let snapshot = TcpSnapshot::combine(parse_snmp(&snmp).ok()?, parse_netstat(&netstat).ok()?);
        Some(snapshot)
    }
}

impl Default for TcpProvider {
    fn default() -> Self {
        Self::new()
    }
}

impl Provider for TcpProvider {
    fn name(&self) -> &'static str {
        "tcp"
    }

    /// Picks up `cri_socket` changes without rebuilding the provider.
    fn reconfigure(&mut self, config: &AgentRuntimeConfig) {
        self.discovery
            .set_selection(SocketSelection::parse(&config.pod_discovery.cri_socket));
    }

    fn collect(&mut self) -> Result<Vec<Sample>, ProviderError> {
        let pods = self
            .discovery
            .refresh()
            .map_err(|source| ProviderError::new(self.name(), source))?;

        let timestamp = SystemTime::now();
        let mut samples = Vec::new();
        // UIDs seen this round; anything left in `previous` afterwards is
        // a disappeared pod whose baseline can be dropped.
        let mut seen: Vec<PodUid> = Vec::with_capacity(pods.len());

        for pod in &pods {
            seen.push(pod.uid.clone());
            let Some(snapshot) = Self::read_pod_snapshot(pod.pid) else {
                tracing::debug!(uid = %pod.uid, pid = pod.pid, "tcp_unreadable");
                continue;
            };
            let attributes = pod_attributes(pod);
            // CurrEstab is a gauge: it describes the current snapshot, so
            // it is emitted on every readable snapshot, even when counter
            // deltas are unavailable (first sighting, PID change, reset).
            samples.push(Sample {
                name: TCP_ESTABLISHED_SAMPLE.to_string(),
                value: SampleValue::GaugeU64(snapshot.curr_estab),
                attributes: attributes.clone(),
                timestamp,
            });
            let baseline = self.previous.get(&pod.uid);
            let deltas = match baseline {
                // First sighting, PID change, or counter reset: re-baseline,
                // emit no counter deltas this interval.
                None => None,
                Some(previous) if previous.pid != pod.pid => None,
                Some(previous) if snapshot_reset(&previous.snapshot, &snapshot) => None,
                Some(previous) => Some(delta_samples(&previous.snapshot, &snapshot)),
            };
            self.previous.insert(
                pod.uid.clone(),
                PodBaseline {
                    pid: pod.pid,
                    snapshot,
                },
            );
            let Some((retrans, sent, received, timeouts, fast_retrans)) = deltas else {
                continue;
            };
            samples.push(counter_sample(
                TCP_RETRANSMITS_SAMPLE,
                retrans,
                &attributes,
                timestamp,
            ));
            samples.push(counter_sample(
                TCP_SEGMENTS_SENT_SAMPLE,
                sent,
                &attributes,
                timestamp,
            ));
            samples.push(counter_sample(
                TCP_SEGMENTS_RECEIVED_SAMPLE,
                received,
                &attributes,
                timestamp,
            ));
            samples.push(counter_sample(
                TCP_TIMEOUTS_SAMPLE,
                timeouts,
                &attributes,
                timestamp,
            ));
            samples.push(counter_sample(
                TCP_FAST_RETRANSMITS_SAMPLE,
                fast_retrans,
                &attributes,
                timestamp,
            ));
        }
        self.previous.retain(|uid, _| seen.contains(uid));

        Ok(samples)
    }
}

/// True when any cumulative counter moved backwards: the network namespace
/// was recreated and the snapshot is from a new epoch.
fn snapshot_reset(previous: &TcpSnapshot, current: &TcpSnapshot) -> bool {
    current.retrans_segs < previous.retrans_segs
        || current.out_segs < previous.out_segs
        || current.in_segs < previous.in_segs
        || current.tcp_timeouts < previous.tcp_timeouts
        || current.tcp_fast_retrans < previous.tcp_fast_retrans
}

/// Per-interval deltas, in field order:
/// (retrans_segs, out_segs, in_segs, tcp_timeouts, tcp_fast_retrans).
fn delta_samples(previous: &TcpSnapshot, current: &TcpSnapshot) -> (u64, u64, u64, u64, u64) {
    (
        current.retrans_segs - previous.retrans_segs,
        current.out_segs - previous.out_segs,
        current.in_segs - previous.in_segs,
        current.tcp_timeouts - previous.tcp_timeouts,
        current.tcp_fast_retrans - previous.tcp_fast_retrans,
    )
}

fn pod_attributes(pod: &crate::pod_discovery::PodInfo) -> BTreeMap<String, String> {
    let mut attributes = BTreeMap::new();
    attributes.insert(POD_NAME_ATTRIBUTE.to_string(), pod.name.clone());
    attributes.insert(POD_NAMESPACE_ATTRIBUTE.to_string(), pod.namespace.clone());
    attributes.insert(POD_UID_ATTRIBUTE.to_string(), pod.uid.as_str().to_string());
    attributes
}

fn counter_sample(
    name: &str,
    value: u64,
    attributes: &BTreeMap<String, String>,
    timestamp: SystemTime,
) -> Sample {
    Sample {
        name: name.to_string(),
        value: SampleValue::Counter(value),
        attributes: attributes.clone(),
        timestamp,
    }
}

#[cfg(test)]
mod tests {
    use super::delta_samples;
    use super::snapshot_reset;
    use crate::modules::tcp::tcp_snapshot::TcpSnapshot;

    fn snapshot(retrans: u64, out: u64, inn: u64, timeouts: u64, fast: u64) -> TcpSnapshot {
        TcpSnapshot {
            retrans_segs: retrans,
            out_segs: out,
            in_segs: inn,
            curr_estab: 3,
            tcp_timeouts: timeouts,
            tcp_fast_retrans: fast,
        }
    }

    #[test]
    fn deltas_subtract_previous_from_current() {
        let previous = snapshot(10, 100, 200, 4, 6);
        let current = snapshot(15, 150, 260, 5, 9);

        assert_eq!(delta_samples(&previous, &current), (5, 50, 60, 1, 3));
    }

    #[test]
    fn counter_decrease_counts_as_reset() {
        let previous = snapshot(10, 100, 200, 4, 6);
        let reset = snapshot(2, 100, 200, 4, 6);

        assert!(snapshot_reset(&previous, &reset));
        assert!(!snapshot_reset(&previous, &previous));
    }
}
