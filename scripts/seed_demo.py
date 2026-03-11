from __future__ import annotations

import uuid
from datetime import datetime, timezone

from sqlalchemy import select

from api.db import SessionLocal
from api.config import get_settings
from api.models import (
    ExecutionTarget,
    ExperimentProject,
    ExperimentRawLink,
    Project,
    ProjectLocation,
    RawDataset,
    RawDatasetLocation,
    StorageRoot,
)
from api.services.users import get_or_create_user


def get_or_create_storage_root(session, *, name: str, root_type: str, host_scope: str, path_prefix: str):
    root = session.scalars(select(StorageRoot).where(StorageRoot.name == name)).first()
    if root is None:
        root = StorageRoot(
            name=name,
            root_type=root_type,
            host_scope=host_scope,
            path_prefix=path_prefix,
        )
        session.add(root)
        session.flush()
    return root


def get_or_create_execution_target(session, *, target_key: str, display_name: str, **kwargs):
    target = session.scalars(
        select(ExecutionTarget).where(ExecutionTarget.target_key == target_key)
    ).first()
    if target is None:
        target = ExecutionTarget(
            target_key=target_key,
            display_name=display_name,
            **kwargs,
        )
        session.add(target)
        session.flush()
    return target


def create_demo_project(session, *, owner, project_key: str, project_name: str, linux_root, windows_root):
    project = session.scalars(select(Project).where(Project.project_key == project_key)).first()
    if project is not None:
        return project

    project = Project(
        id=uuid.uuid4(),
        owner_user_id=owner.id,
        project_key=project_key,
        project_name=project_name,
        visibility="private",
        status="indexed",
        health_status="ok",
        project_mat_bytes=0,
        project_dir_bytes=0,
        total_bytes=0,
        metadata_json={
            "source": "seed_demo",
            "matlab_loader": "shallowLoad",
        },
    )
    session.add(project)
    session.flush()

    locations = [
        ProjectLocation(
            project_id=project.id,
            storage_root_id=linux_root.id,
            relative_path="projects/demo/" + project_key,
            project_file_name=f"{project_key}.mat",
            access_mode="readwrite",
            is_preferred=True,
        ),
        ProjectLocation(
            project_id=project.id,
            storage_root_id=windows_root.id,
            relative_path="projects\\demo\\" + project_key,
            project_file_name=f"{project_key}.mat",
            access_mode="readwrite",
            is_preferred=False,
        ),
    ]
    session.add_all(locations)
    session.flush()
    return project


def create_demo_raw_dataset(session, *, owner, linux_root, windows_root):
    raw_dataset = session.scalars(select(RawDataset).where(RawDataset.external_key == "demo_raw_alpha")).first()
    if raw_dataset is not None:
        return raw_dataset

    raw_dataset = RawDataset(
        id=uuid.uuid4(),
        owner_user_id=owner.id,
        external_key="demo_raw_alpha",
        microscope_name="DemoScope",
        acquisition_label="Demo acquisition alpha",
        visibility="private",
        status="indexed",
        completeness_status="complete",
        total_bytes=1024 * 1024 * 512,
        started_at=datetime.now(timezone.utc),
        ended_at=datetime.now(timezone.utc),
        metadata_json={"source": "seed_demo", "instrument": "Micro-Manager"},
    )
    session.add(raw_dataset)
    session.flush()

    session.add_all(
        [
            RawDatasetLocation(
                raw_dataset_id=raw_dataset.id,
                storage_root_id=linux_root.id,
                relative_path="raw/demo/demo_raw_alpha",
                access_mode="read",
                is_preferred=True,
            ),
            RawDatasetLocation(
                raw_dataset_id=raw_dataset.id,
                storage_root_id=windows_root.id,
                relative_path="raw\\demo\\demo_raw_alpha",
                access_mode="read",
                is_preferred=False,
            ),
        ]
    )
    session.flush()
    return raw_dataset


def create_demo_experiment(session, *, owner, raw_dataset, project):
    experiment = session.scalars(
        select(ExperimentProject).where(ExperimentProject.experiment_key == "demo_experiment_alpha")
    ).first()
    if experiment is not None:
        return experiment

    experiment = ExperimentProject(
        id=uuid.uuid4(),
        owner_user_id=owner.id,
        experiment_key="demo_experiment_alpha",
        title="Demo Experiment Alpha",
        visibility="private",
        status="indexed",
        summary="Demo experiment linking one acquisition and one DetecDiv analysis project.",
        total_raw_bytes=raw_dataset.total_bytes,
        last_indexed_at=datetime.now(timezone.utc),
        metadata_json={
            "source": "seed_demo",
            "external_records": {"labguru": {"status": "pending"}},
        },
    )
    session.add(experiment)
    session.flush()

    session.add(
        ExperimentRawLink(
            experiment_project_id=experiment.id,
            raw_dataset_id=raw_dataset.id,
            link_type="acquisition",
        )
    )
    project.experiment_project_id = experiment.id
    session.flush()
    return experiment


def main() -> None:
    settings = get_settings()
    with SessionLocal() as session:
        owner = get_or_create_user(session, user_key=settings.default_user_key, display_name=settings.default_user_key)
        linux_projects = get_or_create_storage_root(
            session,
            name="server_projects",
            root_type="project_root",
            host_scope="server",
            path_prefix="/srv/detecdiv",
        )
        windows_projects = get_or_create_storage_root(
            session,
            name="windows_projects_share",
            root_type="project_root",
            host_scope="client",
            path_prefix=r"Z:\detecdiv",
        )
        linux_raw = get_or_create_storage_root(
            session,
            name="server_raw",
            root_type="raw_root",
            host_scope="server",
            path_prefix="/srv/microscope",
        )
        windows_raw = get_or_create_storage_root(
            session,
            name="windows_raw_share",
            root_type="raw_root",
            host_scope="client",
            path_prefix=r"Z:\microscope",
        )

        get_or_create_execution_target(
            session,
            target_key="server_gpu",
            display_name="Central GPU Server",
            target_kind="server",
            host_name="detecdiv-server",
            supports_gpu=True,
            supports_matlab=True,
            supports_python=True,
            status="online",
            metadata_json={"source": "seed_demo"},
        )

        project_alpha = create_demo_project(
            session,
            owner=owner,
            project_key="demo_project_alpha",
            project_name="Demo Project Alpha",
            linux_root=linux_projects,
            windows_root=windows_projects,
        )
        create_demo_project(
            session,
            owner=owner,
            project_key="demo_project_beta",
            project_name="Demo Project Beta",
            linux_root=linux_projects,
            windows_root=windows_projects,
        )
        raw_alpha = create_demo_raw_dataset(
            session,
            owner=owner,
            linux_root=linux_raw,
            windows_root=windows_raw,
        )
        create_demo_experiment(
            session,
            owner=owner,
            raw_dataset=raw_alpha,
            project=project_alpha,
        )

        session.commit()

    print("Seeded demo storage roots, execution target, raw data, experiments, and projects.")


if __name__ == "__main__":
    main()
