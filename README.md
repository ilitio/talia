# Talia

Talia is a Linux host monitoring agent. It validates the loop:

- collect host filesystem state in Rust
- export metrics over OTLP
- ingest through an OTLP-compatible telemetry backend
- query and visualize storage pressure
- keep alerting outside the agent

The agent is intentionally split into two planes:

- data plane: `talia-agent` exports OTel metrics to the configured OTLP endpoint
- control/meta plane: `talia-agent` connects outbound to `talia-control` over
  WebSocket and fetches runtime config over HTTP

`talia-control` is an MVP control server. It keeps connected agents in memory,
serves runtime config from TOML defaults plus enrolled hostname overrides, and
can broadcast `config_changed` after an admin reload. It is not a production
fleet control plane yet.

The control server exposes its independent OpenAPI 3.1 contract at
`GET /openapi.json`. The document describes the health, agent, and admin HTTP
routes, both bearer-token roles, required agent headers, and the schemas used by
the WebSocket protocol.

## Binaries

- `talia-core`: shared config and control protocol contract
- `talia-agent`: Linux-only host agent
- `talia-control`: minimal config/control server

Run the control server:

```sh
export TALIA_CONTROL_TOKEN=replace-with-local-dev-token
export TALIA_ADMIN_TOKEN=replace-with-local-admin-token
cargo run -p talia-control -- \
  --config examples/control.toml
```

Run the agent:

```sh
export TALIA_CONTROL_TOKEN=replace-with-local-dev-token
export TALIA_AGENT_CONFIG_TOKEN=replace-with-agent-secret
export OTEL_EXPORTER_OTLP_HEADERS=authorization=<otlp-authorization-value>
export OTEL_RESOURCE_ATTRIBUTES=deployment.environment.name=development
cargo run -p talia-agent -- \
  --config examples/agent.toml
```

The OTLP endpoint comes from the agent config. OTLP authorization uses the
standard OTel `OTEL_EXPORTER_OTLP_HEADERS` environment variable so it stays
consistent with the webserver setup. Set `OTEL_RESOURCE_ATTRIBUTES` to
`deployment.environment.name=staging` or
`deployment.environment.name=production` in each deployed agent's environment
file. Talia always adds `service.namespace=talia` itself.

## Runtime Config

The local agent config bootstraps:

- control HTTP URL
- control WebSocket URL
- optional explicit opt-in for private internal http/ws control URLs
- bearer token, usually through `TALIA_CONTROL_TOKEN`
- per-agent config token for enrolled agents, usually through
  `TALIA_AGENT_CONFIG_TOKEN`
- OTLP endpoint
- state directory
- fallback runtime config
- optional eBPF CPU collector settings
- optional eBPF network collector settings
- optional eBPF disk I/O collector settings
- memory pressure collector settings

The control server config is TOML with defaults, optional agent enrollments, and
hostname overrides. Every remote agent must be enrolled with a per-agent config
token that matches the token sent by that agent. Host-specific overrides also
require the enrollment to map the persisted `agent_id` to the hostname:

Remote agents reject non-loopback plaintext control URLs by default. Set
`allow_insecure_control_url = true` in the local agent config only for private
internal http/ws deployments.

```toml
[agents."agent-1"]
hostname = "prod-vps-1"
config_token = "replace-with-agent-secret"

[defaults.agent]
config_poll_interval_seconds = 900
heartbeat_interval_seconds = 60

[defaults.storage]
enabled = true
interval_seconds = 60
mounts = ["/"]

[defaults.memory]
enabled = true
interval_seconds = 60

[defaults.cpu]
enabled = false
interval_seconds = 1

[defaults.network]
enabled = false
interval_seconds = 1

[defaults.disk_io]
enabled = false
interval_seconds = 1

[hosts."prod-vps-1".storage]
mounts = ["/", "/var/lib/docker"]

[hosts."prod-vps-1".cpu]
enabled = true

[hosts."prod-vps-1".network]
enabled = true

[hosts."prod-vps-1".disk_io]
enabled = true
```

The agent fetches full config through:

```text
GET /agent/config
Authorization: Bearer <control-token>
x-talia-agent-id: <agent-id>
x-talia-config-session-id: <session-id from hello_ack>
x-talia-agent-config-token: <per-agent config token>
```

The WebSocket only carries control messages:

- agent `hello`
- agent `heartbeat`
- server `hello_ack` with the config session id
- server `config_changed`

On fetch failure or invalid config, the agent keeps the last-known-good config.
It stores that config in its state directory as `last-config.json`.

Admin routes under `/admin/*` require `TALIA_ADMIN_TOKEN`. Keep it distinct
from `TALIA_CONTROL_TOKEN`; the control server rejects startup when both tokens
match so an enrolled agent cannot call admin APIs.

Remote control URLs must use `https://` and `wss://` because the agent sends a
bearer token to the control server. Plain `http://` and `ws://` are accepted
only for loopback development endpoints.

Default intervals:

- storage collection: `60s`
- memory pressure collection: `60s`
- eBPF CPU collection window: `1s`
- eBPF network collection window: `1s`
- eBPF disk I/O collection window: `1s`
- control heartbeat: `60s`
- config polling fallback: `15m` plus deterministic per-agent jitter

## CPU Metrics

The CPU collector uses CO-RE eBPF and is disabled by default because it requires
BPF privileges. The agent vendors a minimal `src/bpf/vmlinux.h` with only the
scalar helper types, map constants, and `task_struct` fields needed by the
collector. Runtime CO-RE relocation still uses the target kernel BTF, and the
agent compiles the `tp_btf/sched_switch` program into the binary at build time.

Fedora build prerequisite:

```sh
sudo dnf install clang
```

Linux builds compile the BPF object when the local toolchain supports it. If
the BPF compile step fails, the normal build writes a placeholder object so
non-BPF development and CI can still compile the agent. Set
`TALIA_AGENT_REQUIRE_BPF=1` for release packaging or Fedora validation to make
BPF compilation mandatory.

Runtime prerequisites:

- readable kernel BTF at `/sys/kernel/btf/vmlinux`
- BPF syscall/JIT, BPF events, perf events, and ftrace enabled
- root or equivalent BPF/perf capabilities; the checked Fedora hosts have
  `kernel.unprivileged_bpf_disabled = 2`

The BPF program keeps monotonic per-CPU counters in BPF maps. It does not
emit one event per context switch. Userspace reads the maps on the configured
interval, computes deltas from the previous read, and exports compact OTLP
metrics.

The CPU BPF program does not read PID or TGID. It classifies idle time from
`task_struct.flags` and computes user/system time from each scheduled task's
`utime` and `stime` counters.

BPF map naming rules:

- BPF map objects must end with `_map`, or `_per_cpu_map` for per-CPU maps.
- BPF value structs must not share the same name as the map object.
- Use plain value names such as `cpu_counters` and explicit map names such as
  `cpu_counters_per_cpu_map`.

Metrics:

- `system.cpu.utilization`, unit `1`, attributes:
  - `system.cpu.logical_number`
  - `system.cpu.state = idle|user|system`
  - `talia.config.version`

The always-on CPU collector intentionally does not report which process used the
CPU. Process attribution belongs in a later diagnostic sampler that can be
started only when host CPU crosses a configured threshold.

## Network Metrics

The network collector uses two tracepoints: `net:netif_receive_skb` for ingress
and `net:net_dev_xmit` for successful egress. It intentionally does not inspect
interfaces, IP headers, ports, or transport protocols. The BPF side only keeps
per-CPU byte and packet counters by direction, and userspace reads those
counters once per interval.

Metrics:

- `system.network.io`, unit `By`, attributes:
  - `network.io.direction = receive|transmit`
  - `talia.config.version`

Packet counts stay inside the collector logs for now. The exported metric tracks
traffic volume, which is the data needed for host traffic budgets.

## Disk I/O Metrics

The disk I/O collector uses `block:block_rq_issue` and
`block:block_rq_complete`. The issue hook records a short-lived start timestamp;
the completion hook accounts successful bytes, operations, errors, and
issue-to-completion latency. It does not read process state or device names.

The BPF program keeps per-CPU counters by direction plus a bounded in-flight
request map. Userspace reads deltas once per interval.

Metrics:

- `system.disk.io`, unit `By`, attributes:
  - `disk.io.direction = read|write`
  - `talia.config.version`
- `system.disk.operations`, unit `{operation}`, same attributes
- `system.disk.errors`, unit `{error}`, same attributes
- `system.disk.io.latency`, unit `ms`, same attributes
- `system.disk.io.queue_depth`, unit `{operation}`, same attributes
- `system.disk.io.in_flight`, unit `By`, same attributes

Latency is the average for completed operations in the collection interval. Queue
depth and in-flight bytes are point-in-time gauges.

## Storage Metrics

The storage collector is host filesystem only. Container runtime storage is a
future optional collector.

Talia uses OTel semantic metric names where OTel covers the concept:

- `system.filesystem.usage`, unit `By`
- `system.filesystem.limit`, unit `By`
- `system.filesystem.utilization`, unit `1`

Resource attributes:

- `service.name = "talia-agent"`
- `service.namespace = "talia"`
- `service.version`
- `host.name`
- `deployment.environment.name`, supplied through `OTEL_RESOURCE_ATTRIBUTES`

Talia does not export persisted agent IDs or raw machine IDs as metric resource
attributes. The control plane still uses the persisted agent id for enrollment
state, but metrics avoid persistent device identifiers.

Metric attributes:

- `system.filesystem.mountpoint`
- `system.filesystem.type`
- `system.filesystem.mode`
- `system.filesystem.state = used|free|reserved` on usage metrics
- `talia.config.version`

Talia uses its own attribute for the config version
because OTel does not define one for this agent-specific control metadata.

## Memory Metrics

The memory collector reads `/proc/meminfo` and `/proc/vmstat` once per interval.
It is enabled by default and uses the same `60s` default interval as filesystem
storage. Swap activity comes from `pswpin` and `pswpout` deltas, converted from
pages to bytes with the host page size.

Metrics:

- `system.memory.usage`, unit `By`, attributes:
  - `system.memory.state = total|used|available|free|cached`
  - `talia.config.version`
- `system.memory.utilization`, unit `1`, attributes:
  - `talia.config.version`
- `system.linux.memory.swap.usage`, unit `By`, attributes:
  - `system.linux.memory.swap.state = total|used|free`
  - `talia.config.version`
- `system.linux.memory.swap.utilization`, unit `1`, attributes:
  - `talia.config.version`
- `system.linux.memory.swap.io`, unit `By`, attributes:
  - `system.linux.memory.swap.direction = in|out`
  - `talia.config.version`

Swap utilization tells whether swap is occupied. Swap I/O tells whether the host
is actively paging now, which is the stronger pressure signal.

## Telemetry Backend

Talia exports metrics over OTLP to the configured `otlp_endpoint`. It does not
own the backend schema, dashboards, or alerts. Configure authentication through
standard OTel exporter environment variables such as
`OTEL_EXPORTER_OTLP_HEADERS`.

## Privilege Model

The storage MVP does not require root. Run `talia-agent` as a dedicated
unprivileged user with write access only to its state directory and read access
to its config/token files.

Future BPF/perf collectors must be enabled by short-lived leases and should use
the narrowest Linux capabilities that work on the target kernel. Do not make the
always-on base agent root by default.

## mTLS Follow-Up

MVP authentication uses static bearer tokens. Per-agent mTLS is future work.

## License

Licensed under either the Apache License, Version 2.0 or the MIT License, at
your option.
