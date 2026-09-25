#!/usr/bin/env bash
set -euo pipefail

version="${1:?usage: package-release.sh VERSION [OUTPUT_DIRECTORY]}"
output_directory="${2:-dist}"

if [[ ! "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  echo "version must use MAJOR.MINOR.PATCH" >&2
  exit 1
fi

case "$(uname -m)" in
  x86_64) target="x86_64-unknown-linux-gnu" ;;
  aarch64) target="aarch64-unknown-linux-gnu" ;;
  *) echo "unsupported release architecture: $(uname -m)" >&2; exit 1 ;;
esac

root_directory="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
bundle="talia-${version}-${target}"
staging_directory="${output_directory}/${bundle}"

cd "$root_directory"
TALIA_AGENT_REQUIRE_BPF=1 cargo build \
  --locked \
  --release \
  -p talia-agent \
  -p talia-control

rm -rf "$staging_directory"
mkdir -p "$staging_directory/bin" "$staging_directory/examples" "$staging_directory/systemd"
install -m 0755 target/release/talia-agent "$staging_directory/bin/talia-agent"
install -m 0755 target/release/talia-control "$staging_directory/bin/talia-control"
install -m 0755 scripts/install-agent.sh "$staging_directory/install-agent.sh"
install -m 0755 scripts/rollback-agent.sh "$staging_directory/rollback-agent.sh"
install -m 0644 systemd/talia-agent.service "$staging_directory/systemd/talia-agent.service"
install -m 0644 examples/agent.toml "$staging_directory/examples/agent.toml"
install -m 0644 examples/control.toml "$staging_directory/examples/control.toml"
install -m 0644 docs/deployment.md "$staging_directory/DEPLOYMENT.md"
install -m 0644 LICENSE-APACHE "$staging_directory/LICENSE-APACHE"
install -m 0644 LICENSE-MIT "$staging_directory/LICENSE-MIT"
printf '%s\n' "$version" > "$staging_directory/VERSION"

mkdir -p "$output_directory"
tar -C "$output_directory" -czf "${output_directory}/${bundle}.tar.gz" "$bundle"
(
  cd "$output_directory"
  sha256sum "${bundle}.tar.gz" > "${bundle}.tar.gz.sha256"
)
