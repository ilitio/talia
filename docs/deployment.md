# Release, deployment, and rollback

Repository maintainers own release creation, these deployment artifacts, and
rollback support. Operators own each host rollout and its credentials.

## Release

A `vMAJOR.MINOR.PATCH` tag builds a Linux release with embedded eBPF programs.
The release contains both binaries, example configuration, the systemd unit,
deployment scripts, and a SHA-256 checksum. The release workflow fails instead
of publishing an agent with placeholder eBPF objects.

Create a release from a reviewed commit on `main`:

```sh
git tag -a v0.1.2 -m "Talia v0.1.2"
git push origin v0.1.2
```

Update both binary package versions to match the tag before creating it.

## Deploy the agent

Download the archive and checksum from the matching GitHub release, then verify
them before extracting:

```sh
sha256sum --check talia-0.1.2-x86_64-unknown-linux-gnu.tar.gz.sha256
tar -xzf talia-0.1.2-x86_64-unknown-linux-gnu.tar.gz
cd talia-0.1.2-x86_64-unknown-linux-gnu
```

For the first managed installation, pass the host's prepared config:

```sh
sudo ./install-agent.sh 0.1.2 --initial-config /secure/path/talia-agent.toml
```

Add `--enable-ebpf` when the CPU, network, or disk I/O collectors are enabled.
This installs the reviewed systemd privilege drop-in shipped in the release.

Later upgrades preserve `/etc/talia/talia-agent.toml` and the optional
`/etc/talia/talia-agent.env` secrets file:

```sh
sudo ./install-agent.sh 0.2.0
```

The installer puts immutable versions under `/opt/talia/releases`, switches the
`/opt/talia/current` symlink, and restarts the service. If startup fails, it
automatically restores the previous binary when one exists. On an existing
unmanaged installation, it saves `/usr/local/bin/talia-agent` as the first
rollback target. Copy the existing config and environment values to the neutral
paths before running the installer.

Roll out one host at a time. Continue only after the service is active and the
telemetry backend shows a fresh sample from that host.

## Roll back

Rollback changes only the binary. It preserves host identity, configuration,
last-known-good runtime state, and secrets:

```sh
sudo /opt/talia/rollback-agent.sh
```

The script restarts and verifies the previous release. If that release fails to
start, it restores the binary that was active before the rollback attempt.
