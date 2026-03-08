from __future__ import annotations

import uuid

from sqlalchemy import select

from api.db import SessionLocal
from api.models import ExecutionTarget, Project, ProjectLocation, StorageRoot


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


def create_demo_project(session, *, project_key: str, project_name: str, linux_root, windows_root):
    project = session.scalars(select(Project).where(Project.project_key == project_key)).first()
    if project is not None:
        return project

    project = Project(
        id=uuid.uuid4(),
        project_key=project_key,
        project_name=project_name,
        status="indexed",
        health_status="ok",
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


def main() -> None:
    with SessionLocal() as session:
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

        create_demo_project(
            session,
            project_key="demo_project_alpha",
            project_name="Demo Project Alpha",
            linux_root=linux_projects,
            windows_root=windows_projects,
        )
        create_demo_project(
            session,
            project_key="demo_project_beta",
            project_name="Demo Project Beta",
            linux_root=linux_projects,
            windows_root=windows_projects,
        )

        session.commit()

    print("Seeded demo storage roots, execution target, and projects.")


if __name__ == "__main__":
    main()

