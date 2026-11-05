#!/usr/bin/env bash
# Install only the readonly homes2 view. No API restart or URI/cache change.
set -Eeuo pipefail
[[ "${EUID}" -eq 0 ]] || { echo 'Run this installer with sudo.' >&2; exit 1; }
source_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
unit=/etc/systemd/system/detecdiv-homes2-share.service
dropin=/etc/systemd/system/detecdiv-hub-after-data.service.d/homes2.conf
target=/homes2/maliavko/DetecdivHub
[[ -s /root/.ssh/detecdiv_hub_data ]] || { echo 'Existing root data-bridge SSH identity is unavailable.' >&2; exit 1; }
[[ -x /usr/bin/sshfs && -x /usr/bin/fusermount3 ]] || { echo 'Expected existing SSHFS tools are unavailable.' >&2; exit 1; }
[[ -f "${source_dir}/detecdiv-homes2-share.service" ]] || exit 1
if [[ -e "${unit}" ]]; then
    cmp -s "${source_dir}/detecdiv-homes2-share.service" "${unit}" || { echo 'Existing homes2 unit requires review.' >&2; exit 1; }
fi
if [[ -e "${dropin}" ]]; then
    cmp -s "${source_dir}/homes2.conf" "${dropin}" || { echo 'Existing boot dependency requires review.' >&2; exit 1; }
fi
for directory in /homes2 /homes2/maliavko "${target}"; do
    [[ ! -L "${directory}" ]] || { echo 'Unexpected symbolic link in mount target.' >&2; exit 1; }
    if [[ ! -e "${directory}" ]]; then install -d -m 0755 -- "${directory}"; fi
    [[ -d "${directory}" ]] || exit 1
done
if mountpoint -q "${target}"; then
    [[ "$(findmnt -rn -M "${target}" -o SOURCE)" == 'charvin-admin@10.20.11.100:/homes2/maliavko/DetecdivHub' ]] || { echo 'Unexpected existing mount.' >&2; exit 1; }
else
    [[ -z "$(find "${target}" -mindepth 1 -maxdepth 1 -print -quit)" ]] || { echo 'Mount target is not empty.' >&2; exit 1; }
fi
install -m 0644 -- "${source_dir}/detecdiv-homes2-share.service" "${unit}"
install -D -m 0644 -- "${source_dir}/homes2.conf" "${dropin}"
systemctl daemon-reload
systemctl enable --now detecdiv-homes2-share.service
for attempt in {1..30}; do
    if mountpoint -q "${target}" && [[ -d "${target}/raw" && -d "${target}/projects" ]]; then
        [[ ",$(findmnt -rn -M "${target}" -o OPTIONS)," == *,ro,* ]] || { echo 'Expected read-only mount.' >&2; exit 1; }
        findmnt -rn -M "${target}" -o SOURCE,TARGET,FSTYPE,OPTIONS
        echo 'Read-only homes2 mount is ready. API container and all cache references are unchanged.'
        exit 0
    fi
    sleep 2
done
echo 'Mount is not ready. Preserve API caches and inspect detecdiv-homes2-share.service.' >&2
exit 1
