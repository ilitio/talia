#!/usr/bin/env bash
set -euo pipefail

if [[ "${EUID}" -ne 0 ]]; then
  echo "rollback-agent.sh must run as root" >&2
  exit 1
fi

if [[ ! -L /opt/talia/current || ! -L /opt/talia/previous ]]; then
  echo "both current and previous managed releases are required" >&2
  exit 1
fi

current_target="$(readlink -f /opt/talia/current)"
previous_target="$(readlink -f /opt/talia/previous)"

ln -sfn "$previous_target" /opt/talia/current.next
mv -Tf /opt/talia/current.next /opt/talia/current

if systemctl restart talia-agent.service && systemctl is-active --quiet talia-agent.service; then
  ln -sfn "$current_target" /opt/talia/previous
  echo "Talia rolled back to $(basename "$previous_target")"
  exit 0
fi

ln -sfn "$current_target" /opt/talia/current.next
mv -Tf /opt/talia/current.next /opt/talia/current
systemctl restart talia-agent.service || true
echo "rollback failed; restored $(basename "$current_target")" >&2
exit 1
