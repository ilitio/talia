//! Pod discovery over the Kubernetes CRI (Container Runtime Interface).
//!
//! This module answers one question for per-pod metric collectors: *which
//! pods are running on this node, and which PID's `/proc/<pid>/net`
//! exposes each pod's network namespace?*
//!
//! It speaks CRI gRPC directly over the runtime's Unix socket, so the same
//! code works against K3s/containerd, stock Kubernetes with containerd or
//! CRI-O, and Docker Engine via cri-dockerd — only the socket path differs.
//! Which sockets to use is configurable ([`SocketSelection`]); the default
//! queries every reachable well-known socket and merges pods by UID.
//!
//! The module is intentionally provider-agnostic: any collector that wants
//! per-pod metrics (TCP retransmits, CPU throttling, …) reuses
//! [`PodDiscovery`] instead of reimplementing runtime talk.

mod client;
mod cri;
mod pid;

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::mpsc;
use std::time::Duration;

use thiserror::Error;

use client::RuntimeServiceClient;
use cri::NamespaceMode;
use cri::PodSandboxFilter;
use cri::PodSandboxState;
use cri::PodSandboxStateValue;
use pid::resolve_pids;

/// Well-known CRI socket paths, probed in order.
const WELL_KNOWN_SOCKETS: &[&str] = &[
    "/run/k3s/containerd/containerd.sock",
    "/run/containerd/containerd.sock",
    "/run/crio/crio.sock",
    "/run/cri-dockerd.sock",
];

/// How long [`PodDiscovery::refresh`] waits for the discovery worker
/// before giving up instead of blocking the caller forever.
const REFRESH_TIMEOUT: Duration = Duration::from_secs(30);
/// Leave time for the worker to report its own timeout before the caller's
/// receive deadline, so a stalled CRI request cannot strand the worker.
const WORKER_REFRESH_TIMEOUT: Duration = Duration::from_secs(25);

/// Which CRI sockets pod discovery queries.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SocketSelection {
    /// Query exactly this socket path.
    Explicit(PathBuf),
    /// Query every reachable well-known socket, merging pods by UID.
    All,
    /// Use the first reachable well-known socket.
    FirstWins,
}

impl SocketSelection {
    /// Parses the `cri_socket` config value: `"all"`, `"first"`, or an
    /// explicit socket path. Anything else is treated as a path.
    pub fn parse(value: &str) -> Self {
        match value {
            "all" => Self::All,
            "first" => Self::FirstWins,
            path => Self::Explicit(PathBuf::from(path)),
        }
    }
}

/// Kubernetes pod UID: the stable identity of a pod.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct PodUid(String);

impl PodUid {
    /// Wraps a raw UID string.
    pub fn new(uid: String) -> Self {
        Self(uid)
    }

    /// The raw UID string.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<String> for PodUid {
    fn from(uid: String) -> Self {
        Self(uid)
    }
}

impl std::fmt::Display for PodUid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// A running pod with a known network namespace representative.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PodInfo {
    /// Pod UID (stable identity across restarts of the discovery loop).
    pub uid: PodUid,
    /// Pod name.
    pub name: String,
    /// Pod namespace.
    pub namespace: String,
    /// A PID whose `/proc/<pid>/net` exposes the pod's network namespace.
    pub pid: u32,
}

/// Errors returned by pod discovery.
#[derive(Debug, Error)]
pub enum DiscoveryError {
    /// No CRI socket from the selection was reachable.
    #[error("no CRI socket reachable")]
    NoSocket,
    /// Every CRI socket connected, but all sandbox listings failed: the
    /// caller must not mistake this for "no pods on this node".
    #[error("all CRI sandbox listings failed")]
    AllListsFailed,
    /// The discovery worker thread is gone.
    #[error("pod discovery worker is gone")]
    WorkerGone,
    /// The discovery worker did not answer in time.
    #[error("pod discovery timed out")]
    RefreshTimeout,
    /// The CRI API rejected a request.
    #[error("CRI request failed: {0}")]
    Rpc(#[from] tonic::Status),
    /// A transport-level failure talking to the CRI socket.
    #[error("CRI transport failed: {0}")]
    Transport(#[from] tonic::transport::Error),
}

/// One discovery pass requested from the worker thread.
struct DiscoveryJob {
    selection: SocketSelection,
    reply: mpsc::Sender<Result<Vec<PodInfo>, DiscoveryError>>,
}

/// One connected CRI runtime: which socket it was dialed on, and the
/// client for it.
struct CriClient {
    socket: PathBuf,
    client: RuntimeServiceClient,
}

/// Discovers pods on this node through the CRI.
///
/// All CRI I/O runs on a dedicated worker thread with its own tokio
/// runtime, so the synchronous [`refresh`](Self::refresh) never touches
/// the agent's async runtime and can never nest a `block_on` inside it:
/// it only blocks the calling thread waiting for the worker's reply.
/// Call it from a blocking context (the runner uses `spawn_blocking`);
/// it waits at most [`REFRESH_TIMEOUT`], it never hangs forever.
pub struct PodDiscovery {
    selection: SocketSelection,
    jobs: mpsc::Sender<DiscoveryJob>,
}

impl PodDiscovery {
    /// Creates a discovery instance and starts its worker thread.
    /// Connections are lazy: the first [`refresh`](Self::refresh) dials
    /// the sockets.
    pub fn new(selection: SocketSelection) -> Self {
        let (jobs, inbox) = mpsc::channel();
        std::thread::Builder::new()
            .name("talia-pod-discovery".to_string())
            .spawn(move || worker_loop(inbox))
            .expect("pod discovery worker thread failed to start");
        Self { selection, jobs }
    }

    /// Switches the socket selection. The worker drops its existing
    /// connections on the next refresh.
    pub fn set_selection(&mut self, selection: SocketSelection) {
        self.selection = selection;
    }

    /// Refreshes the pod list. Host-network pods are skipped (their
    /// netns is the host's). Pods whose network namespace cannot be
    /// resolved to a PID are skipped until they can.
    ///
    /// Blocks the calling thread until the worker answers, at most
    /// [`REFRESH_TIMEOUT`].
    pub fn refresh(&self) -> Result<Vec<PodInfo>, DiscoveryError> {
        let (reply_tx, reply_rx) = mpsc::channel();
        self.jobs
            .send(DiscoveryJob {
                selection: self.selection.clone(),
                reply: reply_tx,
            })
            .map_err(|_| DiscoveryError::WorkerGone)?;
        match reply_rx.recv_timeout(REFRESH_TIMEOUT) {
            Ok(result) => result,
            Err(mpsc::RecvTimeoutError::Timeout) => Err(DiscoveryError::RefreshTimeout),
            Err(mpsc::RecvTimeoutError::Disconnected) => Err(DiscoveryError::WorkerGone),
        }
    }
}

impl Drop for PodDiscovery {
    fn drop(&mut self) {
        // Closing the channel lets the worker thread exit on its own.
    }
}

/// Worker thread body: owns the tokio runtime and the CRI clients, and
/// answers one [`DiscoveryJob`] at a time.
fn worker_loop(inbox: mpsc::Receiver<DiscoveryJob>) {
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            tracing::error!(%error, "pod_discovery_runtime_failed");
            for job in inbox {
                let _ = job.reply.send(Err(DiscoveryError::WorkerGone));
            }
            return;
        },
    };
    let mut clients: Vec<CriClient> = Vec::new();
    let mut active_selection: Option<SocketSelection> = None;
    for job in inbox {
        if active_selection.as_ref() != Some(&job.selection) {
            clients.clear();
            active_selection = Some(job.selection.clone());
        }
        let result = runtime.block_on(async {
            tokio::time::timeout(
                WORKER_REFRESH_TIMEOUT,
                refresh_async(&job.selection, &mut clients),
            )
            .await
        });
        let result = match result {
            Ok(result) => result,
            Err(_) => {
                clients.clear();
                Err(DiscoveryError::RefreshTimeout)
            },
        };
        let _ = job.reply.send(result);
    }
}

/// Resolves which sockets to dial for a selection.
fn target_sockets(selection: &SocketSelection) -> Vec<PathBuf> {
    match selection {
        SocketSelection::Explicit(path) => vec![path.clone()],
        SocketSelection::All | SocketSelection::FirstWins => WELL_KNOWN_SOCKETS
            .iter()
            .map(PathBuf::from)
            .filter(|p| p.exists())
            .collect(),
    }
}

/// Dials the selected sockets, keeping already-connected clients.
///
/// For [`SocketSelection::FirstWins`] the well-known sockets are tried in
/// order until the first one that actually connects; an existing but
/// unreachable socket file does not stop the search.
async fn ensure_clients(
    selection: &SocketSelection,
    clients: &mut Vec<CriClient>,
) -> Result<(), DiscoveryError> {
    let wanted = target_sockets(selection);
    let first_wins = matches!(selection, SocketSelection::FirstWins);
    // Drop clients whose socket is no longer wanted.
    clients.retain(|endpoint| wanted.contains(&endpoint.socket));
    if first_wins && !clients.is_empty() {
        return Ok(());
    }
    for socket in wanted {
        if clients.iter().any(|endpoint| endpoint.socket == socket) {
            continue;
        }
        match RuntimeServiceClient::connect(&socket).await {
            Ok(client) => {
                clients.push(CriClient { socket, client });
                if first_wins {
                    return Ok(());
                }
            },
            Err(error) => {
                tracing::debug!(socket = %socket.display(), %error, "cri_connect_failed");
                if matches!(selection, SocketSelection::Explicit(_)) {
                    return Err(DiscoveryError::Transport(error));
                }
            },
        }
    }
    if clients.is_empty() {
        return Err(DiscoveryError::NoSocket);
    }
    Ok(())
}

/// One READY sandbox as listed by a CRI runtime, before its network mode
/// and PID are known.
struct DiscoveredSandbox {
    socket: PathBuf,
    sandbox_id: String,
    name: String,
    namespace: String,
}

/// A sandbox that survived the host-network filter and is waiting for PID
/// resolution.
struct PodCandidate {
    uid: PodUid,
    name: String,
    namespace: String,
}

/// One full discovery pass: list READY sandboxes on every client, read
/// verbose status for network mode and PID hints, resolve PIDs.
async fn refresh_async(
    selection: &SocketSelection,
    clients: &mut Vec<CriClient>,
) -> Result<Vec<PodInfo>, DiscoveryError> {
    ensure_clients(selection, clients).await?;

    // Sandbox listing per socket, merged by pod UID.
    let mut sandboxes: HashMap<PodUid, DiscoveredSandbox> = HashMap::new();
    let mut listed_ok = false;
    let mut failed: Vec<PathBuf> = Vec::new();
    for endpoint in clients.iter_mut() {
        let filter = PodSandboxFilter {
            id: String::new(),
            state: Some(PodSandboxStateValue {
                state: PodSandboxState::Ready as i32,
            }),
            label_selector: HashMap::new(),
        };
        match endpoint.client.list_pod_sandboxes(filter).await {
            Ok(items) => {
                listed_ok = true;
                for item in items {
                    if let Some(meta) = item.metadata {
                        sandboxes
                            .entry(PodUid::new(meta.uid))
                            .or_insert(DiscoveredSandbox {
                                socket: endpoint.socket.clone(),
                                sandbox_id: item.id,
                                name: meta.name,
                                namespace: meta.namespace,
                            });
                    }
                }
            },
            Err(error) => {
                tracing::debug!(socket = %endpoint.socket.display(), %error, "cri_list_failed");
                failed.push(endpoint.socket.clone());
            },
        }
    }
    // Drop clients that failed; the next refresh redials them.
    clients.retain(|endpoint| !failed.contains(&endpoint.socket));
    if !listed_ok {
        // Every listing failed: report the outage instead of an empty pod
        // list, so callers cannot mistake it for "no pods".
        return Err(DiscoveryError::AllListsFailed);
    }

    // Verbose status per sandbox: network mode + sandbox PID hint.
    let mut candidates: Vec<PodCandidate> = Vec::new();
    let mut info_pids: HashMap<PodUid, Option<u32>> = HashMap::new();
    let mut client_for: HashMap<PathBuf, usize> = HashMap::new();
    for (index, endpoint) in clients.iter().enumerate() {
        client_for.entry(endpoint.socket.clone()).or_insert(index);
    }
    for (uid, sandbox) in &sandboxes {
        let Some(index) = client_for.get(&sandbox.socket) else {
            continue;
        };
        let client = &mut clients[*index].client;
        match client.pod_sandbox_status(&sandbox.sandbox_id).await {
            Ok(response) => {
                let host_network = response
                    .status
                    .as_ref()
                    .and_then(|s| s.linux.as_ref())
                    .and_then(|l| l.namespaces.as_ref())
                    .and_then(|n| n.options.as_ref())
                    .is_some_and(|o| o.network == NamespaceMode::Node as i32);
                if host_network {
                    continue;
                }
                // Keep pods without a runtime hint in the map so the
                // fallback cgroup scan can still resolve their PID.
                info_pids.insert(uid.clone(), pid::pid_from_status_info(&response.info));
                candidates.push(PodCandidate {
                    uid: uid.clone(),
                    name: sandbox.name.clone(),
                    namespace: sandbox.namespace.clone(),
                });
            },
            Err(error) => {
                tracing::debug!(uid = %uid, %error, "cri_status_failed");
            },
        }
    }

    let pids = resolve_pids(&info_pids);
    Ok(candidates
        .into_iter()
        .filter_map(|candidate| {
            pids.get(&candidate.uid).map(|pid| PodInfo {
                uid: candidate.uid,
                name: candidate.name,
                namespace: candidate.namespace,
                pid: *pid,
            })
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::PodUid;
    use super::SocketSelection;
    use std::path::PathBuf;

    #[test]
    fn selection_parses_config_values() {
        assert_eq!(SocketSelection::parse("all"), SocketSelection::All);
        assert_eq!(SocketSelection::parse("first"), SocketSelection::FirstWins);
        assert_eq!(
            SocketSelection::parse("/run/custom/cri.sock"),
            SocketSelection::Explicit(PathBuf::from("/run/custom/cri.sock"))
        );
    }

    #[test]
    fn target_sockets_explicit_returns_path_verbatim() {
        let path = PathBuf::from("/run/does/not/exist.sock");
        // An explicit path is used as-is, even if nothing is there: the
        // connection attempt (not path probing) decides reachability.
        assert_eq!(
            super::target_sockets(&SocketSelection::Explicit(path.clone())),
            vec![path]
        );
    }

    #[test]
    fn target_sockets_probe_modes_return_only_existing_paths() {
        for selection in [SocketSelection::All, SocketSelection::FirstWins] {
            for socket in super::target_sockets(&selection) {
                assert!(socket.exists(), "{socket:?} must exist");
            }
        }
    }

    #[test]
    fn well_known_sockets_cover_k3s_containerd_crio_dockerd() {
        let sockets: Vec<&str> = super::WELL_KNOWN_SOCKETS.to_vec();
        assert!(sockets.contains(&"/run/k3s/containerd/containerd.sock"));
        assert!(sockets.contains(&"/run/containerd/containerd.sock"));
        assert!(sockets.contains(&"/run/crio/crio.sock"));
        assert!(sockets.contains(&"/run/cri-dockerd.sock"));
    }

    #[test]
    fn pod_uid_round_trips_through_display() {
        let uid = PodUid::new("9d3b7f2e-7a1b-4c5d-8e6f-1a2b3c4d5e6f".to_string());
        assert_eq!(uid.as_str(), "9d3b7f2e-7a1b-4c5d-8e6f-1a2b3c4d5e6f");
        assert_eq!(uid.to_string(), "9d3b7f2e-7a1b-4c5d-8e6f-1a2b3c4d5e6f");
        assert_eq!(PodUid::from(uid.as_str().to_string()), uid);
    }
}
