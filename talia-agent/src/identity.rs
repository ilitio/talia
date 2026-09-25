//! Host and agent identity helpers for Talia.

use std::fs::OpenOptions;
use std::fs::{self};
use std::io::Write;
#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;
use std::path::PathBuf;

use rustix::system::uname;
use thiserror::Error;
use uuid::Uuid;

const AGENT_ID_FILE: &str = "agent-id";
const BOOT_ID_PATH: &str = "/proc/sys/kernel/random/boot_id";

/// Errors returned while loading or creating a persistent Talia agent identity.
#[derive(Debug, Error)]
pub enum IdentityError {
    /// The agent state directory could not be created.
    #[error("failed to create state directory {path}: {source}")]
    CreateStateDir {
        /// Directory path that failed to be created.
        path: PathBuf,
        /// Underlying filesystem error.
        source: std::io::Error,
    },
    /// The existing agent id file could not be read.
    #[error("failed to read agent id from {path}: {source}")]
    ReadAgentId {
        /// Agent id file path that failed to be read.
        path: PathBuf,
        /// Underlying filesystem error.
        source: std::io::Error,
    },
    /// A new agent id file could not be written.
    #[error("failed to write agent id to {path}: {source}")]
    WriteAgentId {
        /// Agent id file path that failed to be written.
        path: PathBuf,
        /// Underlying filesystem error.
        source: std::io::Error,
    },
    /// The existing agent id file did not contain an id.
    #[error("agent id file {path} is empty")]
    EmptyAgentId {
        /// Agent id file path that was empty.
        path: PathBuf,
    },
}

/// Returns the current kernel hostname.
pub fn hostname() -> String {
    uname().nodename().to_string_lossy().into_owned()
}

/// Loads a persistent agent id from state, creating one if none exists.
pub fn load_or_create_agent_id(state_dir: &Path) -> Result<String, IdentityError> {
    fs::create_dir_all(state_dir).map_err(|source| IdentityError::CreateStateDir {
        path: state_dir.to_path_buf(),
        source,
    })?;
    let path = state_dir.join(AGENT_ID_FILE);
    if path.exists() {
        let agent_id = fs::read_to_string(&path).map_err(|source| IdentityError::ReadAgentId {
            path: path.clone(),
            source,
        })?;
        let agent_id = agent_id.trim();
        if agent_id.is_empty() {
            return Err(IdentityError::EmptyAgentId { path });
        }
        return Ok(agent_id.to_string());
    }

    let agent_id = Uuid::new_v4().to_string();
    write_agent_id_file(&path, &agent_id)
        .map_err(|source| IdentityError::WriteAgentId { path, source })?;
    Ok(agent_id)
}

fn write_agent_id_file(path: &Path, agent_id: &str) -> std::io::Result<()> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    options.mode(0o600);
    let mut file = options.open(path)?;
    writeln!(file, "{agent_id}")
}

/// Reads the Linux boot id when available.
pub fn read_boot_id() -> Option<String> {
    read_trimmed(BOOT_ID_PATH)
}

fn read_trimmed(path: impl AsRef<Path>) -> Option<String> {
    fs::read_to_string(path)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(unix)]
    #[test]
    fn load_or_create_agent_id_creates_private_file() {
        use std::os::unix::fs::PermissionsExt;

        // Given: the scenario for load or create agent id creates private file is prepared.
        let state_dir = std::env::temp_dir().join(format!("talia-identity-{}", Uuid::new_v4()));

        // When: the behavior under test runs.
        let agent_id =
            load_or_create_agent_id(&state_dir).expect("agent id should be created successfully");

        // Then: the assertions confirm that load or create agent id creates private file.
        let agent_id_path = state_dir.join(AGENT_ID_FILE);
        let mode = fs::metadata(&agent_id_path)
            .expect("agent id metadata should be readable")
            .permissions()
            .mode()
            & 0o777;
        let _ = fs::remove_dir_all(&state_dir);
        assert!(!agent_id.is_empty());
        assert_eq!(mode, 0o600);
    }
}
