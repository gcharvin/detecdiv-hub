from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from pathlib import Path

from sqlalchemy.orm import Session

from api.models import ExperimentProject, Project, StorageMigrationBatch, StorageMigrationItem, User
from api.schemas import StorageMigrationPlanCreate
from api.services.external_publications import ensure_publication_records
from api.services.project_indexing import build_project_key, iter_project_candidates, slugify


RAW_DATASET_SUFFIXES = {
    ".ome.tif",
    ".ome.tiff",
    ".tif",
    ".tiff",
    ".nd2",
    ".czi",
    ".lif",
    ".ims",
}
RAW_METADATA_FILE_NAMES = {
    "metadata.txt",
    "acquisitionmetadata.txt",
    "displaysettings.txt",
}


@dataclass
class MigrationCandidate:
    item_type: str
    legacy_path: str
    legacy_key: str | None
    display_name: str
    action: str
    proposed_experiment_key: str | None
    proposed_project_key: str | None
    metadata_json: dict


def create_migration_plan(
    session: Session,
    *,
    payload: StorageMigrationPlanCreate,
    current_user: User,
) -> StorageMigrationBatch:
    source_root = Path(payload.source_path).expanduser().resolve()
    if not source_root.exists():
        raise ValueError(f"Migration source does not exist: {source_root}")
    if not source_root.is_dir():
        raise ValueError(f"Migration source is not a directory: {source_root}")

    candidates = discover_candidates(
        source_root=source_root,
        source_kind=payload.source_kind,
        strategy=payload.strategy,
        max_items=min(max(payload.max_items, 1), 5000),
    )

    batch = StorageMigrationBatch(
        owner_user_id=current_user.id,
        batch_name=payload.batch_name,
        source_kind=payload.source_kind,
        source_path=str(source_root),
        storage_root_name=payload.storage_root_name,
        host_scope=payload.host_scope,
        root_type=payload.root_type,
        strategy=payload.strategy,
        status="planned",
        metadata_json=payload.metadata_json,
        summary_json=build_summary_json(candidates),
    )
    session.add(batch)
    session.flush()

    for candidate in candidates:
        session.add(
            StorageMigrationItem(
                batch_id=batch.id,
                item_type=candidate.item_type,
                legacy_path=candidate.legacy_path,
                legacy_key=candidate.legacy_key,
                display_name=candidate.display_name,
                status="planned",
                action=candidate.action,
                proposed_experiment_key=candidate.proposed_experiment_key,
                proposed_project_key=candidate.proposed_project_key,
                metadata_json=candidate.metadata_json,
            )
        )
    session.flush()
    return batch


def materialize_migration_item(
    session: Session,
    *,
    batch: StorageMigrationBatch,
    item: StorageMigrationItem,
    current_user: User,
) -> ExperimentProject:
    experiment_key = item.proposed_experiment_key or default_experiment_key(item.display_name, "")
    experiment = session.query(ExperimentProject).filter(ExperimentProject.experiment_key == experiment_key).first()
    if experiment is None:
        experiment = ExperimentProject(
            owner_user_id=batch.owner_user_id or current_user.id,
            experiment_key=experiment_key,
            title=item.display_name,
            visibility="private",
            status="placeholder",
            summary=f"Placeholder experiment created from migration plan '{batch.batch_name}'.",
            metadata_json={
                "migration": {
                    "batch_id": str(batch.id),
                    "item_id": item.id,
                    "legacy_path": item.legacy_path,
                    "item_type": item.item_type,
                }
            },
        )
        session.add(experiment)
        session.flush()
        ensure_publication_records(session, experiment=experiment)

    if item.item_type == "detecdiv_project":
        project_key = item.proposed_project_key or item.legacy_key
        if project_key:
            project = session.query(Project).filter(Project.project_key == project_key).first()
            if project is not None:
                project.experiment_project_id = experiment.id

    merged_metadata = dict(item.metadata_json or {})
    merged_metadata["materialized_experiment_id"] = str(experiment.id)
    item.metadata_json = merged_metadata
    item.status = "materialized"
    item.action = "placeholder_created"
    batch.status = "in_progress"
    session.flush()
    return experiment


def attach_item_to_existing_experiment(
    session: Session,
    *,
    batch: StorageMigrationBatch,
    item: StorageMigrationItem,
    experiment: ExperimentProject,
) -> ExperimentProject:
    if item.item_type == "detecdiv_project":
        project_key = item.proposed_project_key or item.legacy_key
        if project_key:
            project = session.query(Project).filter(Project.project_key == project_key).first()
            if project is not None:
                project.experiment_project_id = experiment.id

    merged_metadata = dict(item.metadata_json or {})
    merged_metadata["attached_experiment_id"] = str(experiment.id)
    item.metadata_json = merged_metadata
    item.status = "attached"
    item.action = "attached_to_existing"
    batch.status = "in_progress"
    ensure_publication_records(session, experiment=experiment)
    session.flush()
    return experiment


def execute_pilot_batch(
    session: Session,
    *,
    batch: StorageMigrationBatch,
    current_user: User,
    max_items: int = 25,
) -> list[ExperimentProject]:
    items = (
        session.query(StorageMigrationItem)
        .filter(
            StorageMigrationItem.batch_id == batch.id,
            StorageMigrationItem.status == "planned",
            StorageMigrationItem.action.in_(("review_for_pilot", "create_placeholder_experiment")),
        )
        .order_by(StorageMigrationItem.id.asc())
        .limit(max_items)
        .all()
    )
    experiments: list[ExperimentProject] = []
    for item in items:
        experiments.append(materialize_migration_item(session, batch=batch, item=item, current_user=current_user))
    if items and all(item.status == "materialized" for item in items):
        batch.status = "pilot_materialized"
    session.flush()
    return experiments


def discover_candidates(
    *,
    source_root: Path,
    source_kind: str,
    strategy: str,
    max_items: int,
) -> list[MigrationCandidate]:
    if source_kind == "legacy_project_root":
        return discover_project_candidates(source_root=source_root, strategy=strategy, max_items=max_items)
    if source_kind == "legacy_raw_root":
        return discover_raw_candidates(source_root=source_root, strategy=strategy, max_items=max_items)
    raise ValueError("Unsupported migration source_kind. Use legacy_project_root or legacy_raw_root.")


def discover_project_candidates(
    *,
    source_root: Path,
    strategy: str,
    max_items: int,
) -> list[MigrationCandidate]:
    candidates: list[MigrationCandidate] = []
    for mat_path, project_dir in iter_project_candidates(source_root):
        relative_parent = project_relative_parent_safe(source_root, mat_path)
        project_key = build_project_key(str(mat_path), relative_parent, mat_path.stem)
        experiment_key = default_experiment_key(mat_path.stem, relative_parent)
        candidates.append(
            MigrationCandidate(
                item_type="detecdiv_project",
                legacy_path=str(project_dir),
                legacy_key=project_key,
                display_name=mat_path.stem,
                action=project_strategy_action(strategy),
                proposed_experiment_key=experiment_key,
                proposed_project_key=project_key,
                metadata_json={
                    "project_mat_path": str(mat_path),
                    "project_dir_path": str(project_dir),
                    "relative_parent": relative_parent,
                    "strategy": strategy,
                },
            )
        )
        if len(candidates) >= max_items:
            break
    return candidates


def discover_raw_candidates(
    *,
    source_root: Path,
    strategy: str,
    max_items: int,
) -> list[MigrationCandidate]:
    candidates: list[MigrationCandidate] = []
    seen_paths: set[str] = set()
    roots_to_scan = [source_root, *[path for path in source_root.iterdir() if path.is_dir()]]

    for path in roots_to_scan:
        if len(candidates) >= max_items:
            break
        dataset_dir = classify_raw_dataset_dir(path)
        if dataset_dir is None:
            continue
        normalized = str(dataset_dir.resolve())
        if normalized in seen_paths:
            continue
        seen_paths.add(normalized)
        rel_parent = project_relative_parent_safe(source_root, dataset_dir)
        experiment_key = default_experiment_key(dataset_dir.name, rel_parent)
        candidates.append(
            MigrationCandidate(
                item_type="raw_dataset",
                legacy_path=normalized,
                legacy_key=None,
                display_name=dataset_dir.name,
                action=raw_strategy_action(strategy),
                proposed_experiment_key=experiment_key,
                proposed_project_key=None,
                metadata_json={
                    "dataset_dir_path": normalized,
                    "relative_parent": rel_parent,
                    "strategy": strategy,
                },
            )
        )
    return candidates


def classify_raw_dataset_dir(path: Path) -> Path | None:
    if not path.is_dir():
        return None

    try:
        entries = list(path.iterdir())
    except OSError:
        return None

    file_names = {entry.name.lower() for entry in entries if entry.is_file()}
    if file_names.intersection(RAW_METADATA_FILE_NAMES):
        return path

    for entry in entries:
        if entry.is_file() and has_raw_dataset_suffix(entry.name):
            return path

    for child in entries:
        if not child.is_dir():
            continue
        try:
            child_files = list(child.iterdir())
        except OSError:
            continue
        child_file_names = {entry.name.lower() for entry in child_files if entry.is_file()}
        if child_file_names.intersection(RAW_METADATA_FILE_NAMES):
            return child
        for entry in child_files:
            if entry.is_file() and has_raw_dataset_suffix(entry.name):
                return child
    return None


def has_raw_dataset_suffix(file_name: str) -> bool:
    lower = file_name.lower()
    return any(lower.endswith(suffix) for suffix in RAW_DATASET_SUFFIXES)


def build_summary_json(candidates: list[MigrationCandidate]) -> dict:
    counts = Counter(candidate.item_type for candidate in candidates)
    actions = Counter(candidate.action for candidate in candidates)
    return {
        "candidate_count": len(candidates),
        "item_type_counts": dict(counts),
        "action_counts": dict(actions),
    }


def project_strategy_action(strategy: str) -> str:
    if strategy == "placeholder_experiments":
        return "create_placeholder_experiment"
    if strategy == "pilot":
        return "review_for_pilot"
    return "review"


def raw_strategy_action(strategy: str) -> str:
    if strategy == "placeholder_experiments":
        return "create_placeholder_experiment"
    if strategy == "pilot":
        return "review_for_pilot"
    return "review"


def default_experiment_key(name: str, relative_parent: str) -> str:
    label = str(Path(relative_parent) / name) if relative_parent else name
    return f"exp_{slugify(label)}"


def project_relative_parent_safe(root_path: Path, child_path: Path) -> str:
    try:
        relative_parent = child_path.parent.relative_to(root_path)
    except ValueError:
        return ""
    rel_text = str(relative_parent)
    return "" if rel_text == "." else rel_text
