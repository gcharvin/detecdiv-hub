#!/usr/bin/env bash
set -euo pipefail
repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
config_path="${1:?Usage: install_gitlab_ssh_lan_proxy.sh /path/to/proxy.env}"
test "$(id -u)" != 0 || { echo "Run as the service user, not root." >&2; exit 1; }
test -x /usr/bin/socat
install -D -m 0644 "$repo_root/deploy/systemd/gitlab-ssh-lan-proxy.service" "$HOME/.config/systemd/user/gitlab-ssh-lan-proxy.service"
if ! test "$config_path" -ef "$HOME/.config/gitlab-ssh-lan-proxy.env"; then
  install -D -m 0600 "$config_path" "$HOME/.config/gitlab-ssh-lan-proxy.env"
fi
loginctl enable-linger "$(id -un)"
systemctl --user daemon-reload
systemctl --user enable --now gitlab-ssh-lan-proxy.service
systemctl --user is-active gitlab-ssh-lan-proxy.service
loginctl show-user "$(id -un)" -p Linger
