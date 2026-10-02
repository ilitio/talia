//! Maps a pod (by UID) to a PID whose `/proc/<pid>/net` exposes the pod's
//! network namespace.
//!
//! Preferred source is the runtime-reported sandbox PID (containerd puts
//! a JSON object containing `pid` under `info["info"]` in verbose status).
//! The fallback scans `/proc/*/cgroup` once per refresh for the pod UID.
//! This avoids depending on runtime-specific info keys.

use std::collections::HashMap;
use std::fs;
use std::path::Path;

use serde_json::Value;

use super::PodUid;

/// Extracts a sandbox PID from verbose CRI status info. Containerd places
/// its JSON object under the `info` key; accept a direct `pid` key too for
/// runtimes that expose it separately.
pub(super) fn pid_from_status_info(info: &HashMap<String, String>) -> Option<u32> {
    let direct = info
        .get("pid")
        .and_then(|value| value.parse::<u32>().ok())
        .filter(|pid| *pid > 0);
    direct.or_else(|| {
        let nested: Value = serde_json::from_str(info.get("info")?).ok()?;
        let value = nested.get("pid")?;
        let pid = value
            .as_u64()
            .or_else(|| value.as_str()?.parse::<u64>().ok())?;
        u32::try_from(pid).ok().filter(|pid| *pid > 0)
    })
}

/// Resolves one PID per pod UID, in a single `/proc` pass.
///
/// `info_pids` maps pod UIDs to the PID from verbose CRI status info;
/// entries that are missing, unparsable, or no longer alive fall back to the
/// cgroup scan. Only PIDs whose `/proc/<pid>/net/snmp` is readable are
/// returned.
pub fn resolve_pids(info_pids: &HashMap<PodUid, Option<u32>>) -> HashMap<PodUid, u32> {
    let mut resolved = HashMap::new();
    let mut need_scan = Vec::new();

    for (uid, pid_hint) in info_pids {
        let hinted =
            pid_hint.filter(|pid| net_snmp_readable(*pid) && cgroup_matches_uid(*pid, uid));
        match hinted {
            Some(pid) => {
                resolved.insert(uid.clone(), pid);
            },
            None => need_scan.push(uid.clone()),
        }
    }

    if !need_scan.is_empty() {
        for (uid, pid) in scan_proc_for_uids(&need_scan) {
            resolved.insert(uid, pid);
        }
    }

    resolved
}

/// Returns the UID spellings matched against cgroup content.
fn uid_spellings(uid: &str) -> [String; 4] {
    [
        uid.to_string(),
        uid.replace('-', "_"),
        uid.replace('-', "\\x2d"),
        uid.chars().filter(|c| *c != '-').collect(),
    ]
}

/// Returns true when the PID's cgroup mentions the pod UID (any spelling).
fn cgroup_matches_uid(pid: u32, uid: &PodUid) -> bool {
    fs::read_to_string(format!("/proc/{pid}/cgroup"))
        .map(|cgroup| {
            uid_spellings(uid.as_str())
                .iter()
                .any(|v| cgroup.contains(v))
        })
        .unwrap_or(false)
}

/// Returns true when `/proc/<pid>/net/snmp` exists and is a file.
fn net_snmp_readable(pid: u32) -> bool {
    Path::new(&format!("/proc/{pid}/net/snmp")).is_file()
}

/// Single `/proc` pass mapping each wanted UID to a PID in its cgroup.
///
/// The UID is matched as a substring against `/proc/<pid>/cgroup` in several
/// spellings (with dashes, dashes as underscores, URL-escaped dashes, and
/// dashes removed) because cgroup v1 and v2 (systemd) encode it differently.
fn scan_proc_for_uids(uids: &[PodUid]) -> Vec<(PodUid, u32)> {
    let spellings: Vec<[String; 4]> = uids.iter().map(|uid| uid_spellings(uid.as_str())).collect();

    let mut found: Vec<Option<u32>> = vec![None; uids.len()];
    let mut remaining = uids.len();

    let entries = fs::read_dir("/proc").into_iter().flatten().flatten();
    for entry in entries {
        if remaining == 0 {
            break;
        }
        let pid: u32 = match entry.file_name().to_str().and_then(|n| n.parse().ok()) {
            Some(pid) => pid,
            None => continue,
        };
        let cgroup = match fs::read_to_string(format!("/proc/{pid}/cgroup")) {
            Ok(content) => content,
            Err(_) => continue,
        };
        for (index, variants) in spellings.iter().enumerate() {
            if found[index].is_none()
                && variants.iter().any(|v| cgroup.contains(v))
                && net_snmp_readable(pid)
            {
                found[index] = Some(pid);
                remaining -= 1;
            }
        }
    }

    found
        .into_iter()
        .enumerate()
        .filter_map(|(index, pid)| pid.map(|pid| (uids[index].clone(), pid)))
        .collect()
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::pid_from_status_info;

    #[test]
    fn extracts_pid_from_containerd_info_json() {
        let info = HashMap::from([("info".to_string(), r#"{"pid":1234}"#.to_string())]);
        assert_eq!(pid_from_status_info(&info), Some(1234));
    }

    #[test]
    fn accepts_direct_pid_and_ignores_invalid_hints() {
        let direct = HashMap::from([("pid".to_string(), "1234".to_string())]);
        assert_eq!(pid_from_status_info(&direct), Some(1234));

        for value in ["0", "-1", "not-a-pid"] {
            let invalid = HashMap::from([("pid".to_string(), value.to_string())]);
            assert_eq!(pid_from_status_info(&invalid), None);
        }
    }

    #[test]
    fn uid_spellings_cover_cgroup_variants() {
        let uid = "9d3b7f2e-7a1b-4c5d-8e6f-1a2b3c4d5e6f";
        let variants = [
            uid.to_string(),
            uid.replace('-', "_"),
            uid.replace('-', "\\x2d"),
            uid.chars().filter(|c| *c != '-').collect::<String>(),
        ];
        // cgroup v1 style
        let v1 = "10:memory:/kubepods/besteffort/pod9d3b7f2e-7a1b-4c5d-8e6f-1a2b3c4d5e6f/cri-containerd-abc";
        // cgroup v2 systemd style (dashes escaped)
        let v2 = "0::/kubepods.slice/kubepods-besteffort.slice/kubepods-besteffort-pod9d3b7f2e_7a1b_4c5d_8e6f_1a2b3c4d5e6f.slice/crio-abc.scope";
        assert!(variants.iter().any(|v| v1.contains(v)));
        assert!(variants.iter().any(|v| v2.contains(v)));
    }
}
