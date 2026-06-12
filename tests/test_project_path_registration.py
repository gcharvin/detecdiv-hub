from pathlib import Path

from api.routes_projects import infer_registration_root_path, safe_relative_path


def test_infer_registration_root_path_uses_data_owner_root():
    mat_path = Path("/data/Gilles/antoine/test_single_cell_oscillations2.mat")

    root_path = infer_registration_root_path(mat_path=mat_path, root_path=None)

    assert root_path == Path("/data/Gilles")


def test_safe_relative_path_returns_project_dir_under_root():
    root_path = Path("/data/Gilles")
    project_dir = Path("/data/Gilles/antoine/test_single_cell_oscillations2")

    assert safe_relative_path(project_dir, root_path) == "antoine/test_single_cell_oscillations2"
