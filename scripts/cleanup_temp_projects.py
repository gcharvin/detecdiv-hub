from __future__ import annotations

import argparse
import json
import re
import shutil
from dataclasses import asdict, dataclass
from pathlib import Path


CONFIRMATION_TEXT = "DELETE_TEMP_PROJECT_ARTIFACTS"
TEMP_PROJECT_FILE_NAME = "temp-project.mat"
TEMP_POSITION_PATTERN = re.compile(r"temp-pos\d+$", re.IGNORECASE)


@dataclass(frozen=True)
class CleanupTarget:
    project_file: str
    target_path: str
    target_kind: str
    bytes: int
    file_count: int


def discover_cleanup_targets(
    roots: list[Path],
    *,
    project_files: list[Path] | None = None,
) -> list[CleanupTarget]:
    targets: list[CleanupTarget] = []
    seen_targets: set[Path] = set()
    resolved_roots = [root.expanduser().resolve() for root in roots]
    for root in resolved_roots:
        if not root.is_dir():
            raise ValueError(f"Cleanup root does not exist or is not a directory: {root}")

    if project_files is not None:
        candidates = [(find_containing_root(path, roots=resolved_roots), path) for path in project_files]
    else:
        candidates = (
            (root, project_file)
            for root in resolved_roots
            for project_file in root.rglob("*.mat")
            if project_file.name.lower() == TEMP_PROJECT_FILE_NAME
        )

    for root, project_file in candidates:
        if project_file.name.lower() == TEMP_PROJECT_FILE_NAME and project_file.is_file():
            targets.extend(
                targets_for_temp_project(
                    root=root,
                    project_file=project_file,
                    seen_targets=seen_targets,
                )
            )
    return sorted(targets, key=lambda item: item.target_path.lower())


def find_containing_root(path: Path, *, roots: list[Path]) -> Path:
    absolute_path = path.expanduser().absolute()
    for root in roots:
        try:
            absolute_path.relative_to(root)
            return root
        except ValueError:
            continue
    raise ValueError(f"Candidate project file is outside every cleanup root: {absolute_path}")


def targets_for_temp_project(
    *,
    root: Path,
    project_file: Path,
    seen_targets: set[Path] | None = None,
) -> list[CleanupTarget]:
    root = root.resolve()
    project_file = project_file.resolve()
    ensure_within_root(project_file, root=root)
    if project_file.name.lower() != TEMP_PROJECT_FILE_NAME:
        return []

    seen_targets = seen_targets if seen_targets is not None else set()
    candidate_paths = [project_file]
    candidate_paths.extend(
        child
        for child in project_file.parent.iterdir()
        if TEMP_POSITION_PATTERN.fullmatch(child.name) and (child.is_dir() or child.is_symlink())
    )

    targets: list[CleanupTarget] = []
    for candidate in candidate_paths:
        resolved_candidate = candidate.resolve() if not candidate.is_symlink() else candidate.absolute()
        ensure_within_root(resolved_candidate, root=root)
        if resolved_candidate in seen_targets:
            continue
        seen_targets.add(resolved_candidate)
        size_bytes, file_count = measure_path(candidate)
        targets.append(
            CleanupTarget(
                project_file=str(project_file),
                target_path=str(candidate),
                target_kind="directory" if candidate.is_dir() and not candidate.is_symlink() else "file",
                bytes=size_bytes,
                file_count=file_count,
            )
        )
    return targets


def ensure_within_root(path: Path, *, root: Path) -> None:
    try:
        path.relative_to(root)
    except ValueError as exc:
        raise ValueError(f"Refusing target outside cleanup root: {path}") from exc


def measure_path(path: Path) -> tuple[int, int]:
    if path.is_symlink():
        return 0, 1
    if path.is_file():
        try:
            return int(path.stat().st_size), 1
        except OSError:
            return 0, 1
    total_bytes = 0
    file_count = 0
    for child in path.rglob("*"):
        if child.is_symlink():
            file_count += 1
            continue
        if not child.is_file():
            continue
        file_count += 1
        try:
            total_bytes += int(child.stat().st_size)
        except OSError:
            continue
    return total_bytes, file_count


def execute_cleanup(targets: list[CleanupTarget]) -> None:
    for target in targets:
        path = Path(target.target_path)
        if path.is_symlink() or path.is_file():
            path.unlink(missing_ok=True)
        elif path.is_dir():
            shutil.rmtree(path)


def build_summary(targets: list[CleanupTarget], *, executed: bool) -> dict:
    return {
        "mode": "execute" if executed else "dry-run",
        "temp_project_count": len({target.project_file for target in targets}),
        "target_count": len(targets),
        "directory_count": sum(target.target_kind == "directory" for target in targets),
        "file_count": sum(target.file_count for target in targets),
        "total_bytes": sum(target.bytes for target in targets),
        "targets": [asdict(target) for target in targets],
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Inventory or delete legacy MATLAB temp-project.mat files and their immediate "
            "temp-posN directories. Dry-run is the default."
        )
    )
    parser.add_argument("roots", nargs="+", type=Path)
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--confirm", default="")
    parser.add_argument("--json", action="store_true", dest="json_output")
    parser.add_argument(
        "--candidate-file",
        type=Path,
        help="Optional newline-delimited temp-project.mat path list for a catalog-driven audit.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.execute and args.confirm != CONFIRMATION_TEXT:
        raise SystemExit(
            f"Execution requires --confirm {CONFIRMATION_TEXT}. Dry-run requires no confirmation."
        )

    project_files = None
    if args.candidate_file is not None:
        project_files = [
            Path(line.strip())
            for line in args.candidate_file.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
    targets = discover_cleanup_targets(args.roots, project_files=project_files)
    if args.execute:
        execute_cleanup(targets)
    summary = build_summary(targets, executed=args.execute)
    if args.json_output:
        print(json.dumps(summary, indent=2, ensure_ascii=False))
    else:
        print(
            f"mode={summary['mode']} temp_projects={summary['temp_project_count']} "
            f"targets={summary['target_count']} directories={summary['directory_count']} "
            f"files={summary['file_count']} bytes={summary['total_bytes']}"
        )
        for target in targets:
            print(
                f"{target.target_kind}\t{target.bytes}\t{target.file_count}\t"
                f"{target.target_path}"
            )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
