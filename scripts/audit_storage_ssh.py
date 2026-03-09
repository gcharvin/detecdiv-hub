from __future__ import annotations

import argparse
import json
import shlex
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path


@dataclass(frozen=True)
class FileTypeSpec:
    label: str
    patterns: tuple[str, ...]


FILE_TYPE_SPECS = (
    FileTypeSpec(label="tif", patterns=("*.tif", "*.tiff")),
    FileTypeSpec(label="h5", patterns=("*.h5",)),
    FileTypeSpec(label="mat", patterns=("*.mat",)),
)

SPECIAL_DIRECTORY_NAMES = ("#recycle", "#snapshot", "@eaDir", "@sharesnapshot")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run a read-only storage audit over SSH and emit a reusable JSON report."
    )
    parser.add_argument("ssh_host", help="SSH host or alias, for example Gilles@10.20.11.250 or nas-syno")
    parser.add_argument("target_path", help="Remote path to audit, for example /data")
    parser.add_argument("--depth", type=int, default=2, help="Directory depth for du summaries")
    parser.add_argument("--top-dirs", type=int, default=30, help="Number of largest directories to keep")
    parser.add_argument("--top-files", type=int, default=100, help="Number of largest matching files to keep")
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Optional output JSON path. Defaults to stdout only.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    report = {
        "audit_version": 1,
        "captured_at": datetime.now(timezone.utc).isoformat(),
        "ssh_host": args.ssh_host,
        "target_path": args.target_path,
        "top_n": {
            "directories": args.top_dirs,
            "files": args.top_files,
        },
        "disk_usage": get_disk_usage(args.ssh_host, args.target_path),
        "top_directories": get_top_directories(
            args.ssh_host,
            args.target_path,
            depth=args.depth,
            limit=args.top_dirs,
        ),
        "largest_files": get_largest_files(
            args.ssh_host,
            args.target_path,
            limit=args.top_files,
        ),
        "file_type_summary": get_file_type_summary(args.ssh_host, args.target_path),
        "special_directories": get_special_directories(args.ssh_host, args.target_path),
    }

    rendered = json.dumps(report, indent=2)
    if args.output is not None:
        args.output.write_text(rendered + "\n", encoding="utf-8")
    print(rendered)
    return 0


def run_ssh_command(ssh_host: str, command: str) -> str:
    result = subprocess.run(
        ["ssh", ssh_host, "sh", "-lc", command],
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    if result.returncode != 0:
        stderr = result.stderr.strip() or "unknown remote error"
        raise RuntimeError(f"SSH command failed for {ssh_host}: {stderr}")
    return result.stdout


def shell_quote(value: str) -> str:
    return shlex.quote(value)


def build_find_name_clause(patterns: tuple[str, ...]) -> str:
    tokens: list[str] = []
    for index, pattern in enumerate(patterns):
        if index > 0:
            tokens.append("-o")
        tokens.extend(["-iname", shell_quote(pattern)])
    return "\\( " + " ".join(tokens) + " \\)"


def get_disk_usage(ssh_host: str, target_path: str) -> dict:
    command = f"df -Pk {shell_quote(target_path)}"
    output = run_ssh_command(ssh_host, command)
    lines = [line.strip() for line in output.splitlines() if line.strip()]
    if len(lines) < 2:
        raise RuntimeError(f"Unexpected df output for {target_path!r}: {output!r}")

    parts = lines[-1].split()
    if len(parts) < 6:
        raise RuntimeError(f"Unexpected df output row for {target_path!r}: {lines[-1]!r}")

    return {
        "filesystem": parts[0],
        "total_bytes": kib_to_bytes(parts[1]),
        "used_bytes": kib_to_bytes(parts[2]),
        "available_bytes": kib_to_bytes(parts[3]),
        "used_percent": parts[4],
        "mount_point": parts[5],
    }


def get_top_directories(ssh_host: str, target_path: str, *, depth: int, limit: int) -> list[dict]:
    command = (
        f"du -x -k -d {depth} {shell_quote(target_path)} 2>/dev/null "
        f"| sort -n | tail -n {limit}"
    )
    output = run_ssh_command(ssh_host, command)
    rows = []
    for line in output.splitlines():
        line = line.strip()
        if not line:
            continue
        size_text, path_text = line.split(maxsplit=1)
        rows.append(
            {
                "path": path_text,
                "bytes": kib_to_bytes(size_text),
            }
        )
    rows.sort(key=lambda item: item["bytes"], reverse=True)
    return rows


def get_largest_files(ssh_host: str, target_path: str, *, limit: int) -> list[dict]:
    predicates = " -o ".join(build_find_name_clause(spec.patterns) for spec in FILE_TYPE_SPECS)
    command = (
        f"find {shell_quote(target_path)} -xdev -type f \\( {predicates} \\) "
        f"-exec stat -c '%s\\t%n' {{}} + 2>/dev/null | sort -nr | head -n {limit}"
    )
    output = run_ssh_command(ssh_host, command)
    rows = []
    for line in output.splitlines():
        line = line.strip()
        if not line:
            continue
        size_text, path_text = line.split("\t", maxsplit=1)
        rows.append(
            {
                "path": path_text,
                "bytes": int(size_text),
                "kind": infer_file_kind(path_text),
            }
        )
    return rows


def get_file_type_summary(ssh_host: str, target_path: str) -> dict[str, dict]:
    summary: dict[str, dict] = {}
    for spec in FILE_TYPE_SPECS:
        predicate = build_find_name_clause(spec.patterns)
        command = (
            f"find {shell_quote(target_path)} -xdev -type f {predicate} "
            f"-exec stat -c '%s' {{}} + 2>/dev/null "
            f"| awk 'BEGIN {{ count=0; total=0 }} {{ count += 1; total += $1 }} "
            f"END {{ printf \"%d\\t%d\\n\", count, total }}'"
        )
        output = run_ssh_command(ssh_host, command).strip()
        if not output:
            count = 0
            total = 0
        else:
            count_text, total_text = output.split("\t", maxsplit=1)
            count = int(count_text)
            total = int(total_text)
        summary[spec.label] = {
            "count": count,
            "bytes": total,
        }
    return summary


def get_special_directories(ssh_host: str, target_path: str) -> list[dict]:
    name_clause = " -o ".join(f"-name {shell_quote(name)}" for name in SPECIAL_DIRECTORY_NAMES)
    command = (
        f"find {shell_quote(target_path)} -xdev -type d \\( {name_clause} \\) -print 2>/dev/null "
        f"| while IFS= read -r dir; do "
        f"size=$(du -x -s -k \"$dir\" 2>/dev/null | awk '{{print $1}}'); "
        f"printf '%s\\t%s\\n' \"${{size:-0}}\" \"$dir\"; "
        f"done | sort -nr"
    )
    output = run_ssh_command(ssh_host, command)
    rows = []
    for line in output.splitlines():
        line = line.strip()
        if not line:
            continue
        size_text, path_text = line.split("\t", maxsplit=1)
        rows.append(
            {
                "path": path_text,
                "bytes": kib_to_bytes(size_text),
                "kind": infer_special_directory_kind(path_text),
            }
        )
    return rows


def infer_file_kind(path_text: str) -> str:
    suffix = Path(path_text).suffix.lower()
    if suffix in {".tif", ".tiff"}:
        return "tif"
    if suffix == ".h5":
        return "h5"
    if suffix == ".mat":
        return "mat"
    return "other"


def infer_special_directory_kind(path_text: str) -> str:
    lowered = path_text.lower()
    if lowered.endswith("/#recycle"):
        return "recycle_bin"
    if lowered.endswith("/#snapshot"):
        return "snapshot"
    if lowered.endswith("/@sharesnapshot"):
        return "share_snapshot"
    if lowered.endswith("/@eadir"):
        return "synology_metadata"
    return "other"


def kib_to_bytes(value: str) -> int:
    return int(value) * 1024


if __name__ == "__main__":
    raise SystemExit(main())
