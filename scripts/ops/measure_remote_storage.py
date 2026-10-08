"""Read-only native NAS du with durable progress and previously measured exclusions."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path, PurePosixPath
import shlex
import subprocess


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--compute-host", required=True)
    parser.add_argument("--nas", required=True)
    parser.add_argument("--nas-user", required=True)
    parser.add_argument("--key-on-compute", required=True)
    parser.add_argument("--root", required=True)
    parser.add_argument("--ssh-exe", default="C:/Windows/System32/OpenSSH/ssh.exe")
    parser.add_argument("--seed", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    root = PurePosixPath(args.root)
    if not root.is_absolute() or len(root.parts) < 4:
        parser.error("Choose a specific absolute NAS data directory.")
    seed = json.loads(args.seed.read_text(encoding="utf-8")) if args.seed else {}
    completed = seed.get("completed_directories", {})
    selected: dict[str, int] = {}
    for path, size in sorted(completed.items(), key=lambda item: len(PurePosixPath(item[0]).parts)):
        candidate = PurePosixPath(path)
        if not candidate.is_relative_to(root) or candidate == root or int(size) < 0:
            parser.error("Seed directory must be a descendant of the selected root.")
        if not any(candidate.is_relative_to(PurePosixPath(parent)) for parent in selected):
            selected[path] = int(size)
    du = ["nice", "-n", "19", "du", "--apparent-size", "-B1", "-x", "--max-depth=2",
          "--exclude=@eaDir", "--exclude=#recycle", "--exclude=#snapshot"]
    du.extend(f"--exclude={path}" for path in selected)
    du.append(str(root))
    nested = ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10", "-o", "IdentitiesOnly=yes",
              "-i", args.key_on_compute, f"{args.nas_user}@{args.nas}", shlex.join(du)]
    command = [args.ssh_exe, "-o", "BatchMode=yes", "-o", "ConnectTimeout=10", args.compute_host, shlex.join(nested)]
    report = {"started_at": datetime.now(timezone.utc).isoformat(), "nas": args.nas, "root": str(root),
              "measurement": "apparent bytes; native du; Synology metadata, recycle and snapshots excluded",
              "seed_source": str(args.seed) if args.seed else None,
              "completed_before_scan": selected, "observed_directories": {}, "diagnostics": [],
              "status": "running", "complete": False, "total_bytes": None}

    def persist() -> None:
        report["updated_at"] = datetime.now(timezone.utc).isoformat()
        temporary = args.output.with_suffix(args.output.suffix + ".tmp")
        temporary.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
        temporary.replace(args.output)

    persist()
    with subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                          text=True, encoding="utf-8", errors="replace") as process:
        report["ssh_pid"] = process.pid
        for line in process.stdout:
            line = line.rstrip("\r\n")
            fields = line.split("\t", 1)
            if len(fields) == 2 and fields[0].isdigit() and PurePosixPath(fields[1]).is_relative_to(root):
                path = PurePosixPath(fields[1])
                adjusted = int(fields[0]) + sum(size for old, size in selected.items()
                                               if PurePosixPath(old).is_relative_to(path))
                report["observed_directories"][str(path)] = adjusted
                if path == root:
                    report["total_bytes"] = adjusted
            elif line:
                report["diagnostics"].append(line[:1000])
            persist()
        result = process.wait()
    report["exit_code"] = result
    report["complete"] = result == 0 and report["total_bytes"] is not None and not report["diagnostics"]
    report["status"] = "complete" if report["complete"] else "incomplete"
    persist()
    print(json.dumps({"report": str(args.output), "status": report["status"], "total_bytes": report["total_bytes"]}))
    return result if result else (0 if report["complete"] else 1)


if __name__ == "__main__":
    raise SystemExit(main())
