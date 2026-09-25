#!/usr/bin/env bash
set -euo pipefail

version="${1:?usage: install-agent.sh VERSION [INITIAL_CONFIG]}"
initial_config="${2:-}"
bundle_directory="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
release_directory="/opt/talia/releases/${version}"

if [[ "${EUID}" -ne 0 ]]; then
  echo "install-agent.sh must run as root" >&2
  exit 1
fi

if [[ ! -x "${bundle_directory}/bin/talia-agent" ]]; then
  echo "the release bundle does not contain bin/talia-agent" >&2
  exit 1
fi

if ! id talia >/dev/null 2>&1; then
  useradd --system --home-dir /var/lib/talia --shell /usr/sbin/nologin talia
fi

install -d -m 0755 /opt/talia/releases /etc/talia
install -d -m 0750 -o talia -g talia /var/lib/talia
install -d -m 0755 "$release_directory"
install -m 0755 "${bundle_directory}/bin/talia-agent" "${release_directory}/talia-agent"

if [[ ! -f /etc/talia/talia-agent.toml ]]; then
  if [[ -z "$initial_config" ]]; then
    echo "pass the initial agent config as the second argument" >&2
    exit 1
  fi
  install -m 0640 -o root -g talia "$initial_config" /etc/talia/talia-agent.toml
fi

previous_target=""
if [[ -L /opt/talia/current ]]; then
  previous_target="$(readlink -f /opt/talia/current)"
elif [[ -x /usr/local/bin/talia-agent ]]; then
  previous_target="/opt/talia/releases/pre-managed"
  install -d -m 0755 "$previous_target"
  install -m 0755 /usr/local/bin/talia-agent "${previous_target}/talia-agent"
fi

if [[ -n "$previous_target" && "$previous_target" != "$release_directory" ]]; then
  ln -sfn "$previous_target" /opt/talia/previous
fi
ln -sfn "$release_directory" /opt/talia/current.next
mv -Tf /opt/talia/current.next /opt/talia/current

install -m 0644 "${bundle_directory}/systemd/talia-agent.service" \
  /etc/systemd/system/talia-agent.service
systemctl daemon-reload
systemctl enable talia-agent.service

if ! systemctl restart talia-agent.service || ! systemctl is-active --quiet talia-agent.service; then
  if [[ -L /opt/talia/previous ]]; then
    ln -sfn "$(readlink -f /opt/talia/previous)" /opt/talia/current.next
    mv -Tf /opt/talia/current.next /opt/talia/current
    systemctl restart talia-agent.service || true
  fi
  echo "Talia failed to start; the previous binary was restored when available" >&2
  exit 1
fi

echo "Talia ${version} is active"
