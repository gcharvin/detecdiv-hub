from pathlib import Path

from api.services.runtime_version import _compute_code_fingerprint, _iter_fingerprint_paths


def test_iter_fingerprint_paths_ignores_generated_cache_files(tmp_path: Path):
    (tmp_path / "api").mkdir()
    (tmp_path / "worker").mkdir()
    (tmp_path / "db").mkdir()

    source_file = tmp_path / "api" / "module.py"
    source_file.write_text("print('hello')\n", encoding="utf-8")
    pycache_dir = tmp_path / "api" / "__pycache__"
    pycache_dir.mkdir()
    (pycache_dir / "module.cpython-312.pyc").write_bytes(b"pyc-bytes")
    (tmp_path / "worker" / "task.py").write_text("print('worker')\n", encoding="utf-8")
    (tmp_path / "worker" / "__pycache__").mkdir()
    (tmp_path / "worker" / "__pycache__" / "task.cpython-312.pyc").write_bytes(b"worker-pyc")
    schema_file = tmp_path / "db" / "schema.sql"
    schema_file.write_text("select 1;\n", encoding="utf-8")
    pyproject_file = tmp_path / "pyproject.toml"
    pyproject_file.write_text("[project]\nname = 'demo'\n", encoding="utf-8")

    relative_paths = [path.relative_to(tmp_path).as_posix() for path in _iter_fingerprint_paths(tmp_path)]

    assert relative_paths == [
        "api/module.py",
        "worker/task.py",
        "db/schema.sql",
        "pyproject.toml",
    ]


def test_compute_code_fingerprint_is_stable_without_cache_files(tmp_path: Path):
    (tmp_path / "api").mkdir()
    (tmp_path / "worker").mkdir()
    (tmp_path / "db").mkdir()

    (tmp_path / "api" / "module.py").write_text("print('hello')\n", encoding="utf-8")
    (tmp_path / "worker" / "task.py").write_text("print('worker')\n", encoding="utf-8")
    (tmp_path / "db" / "schema.sql").write_text("select 1;\n", encoding="utf-8")
    (tmp_path / "pyproject.toml").write_text("[project]\nname = 'demo'\n", encoding="utf-8")

    baseline = _compute_code_fingerprint(tmp_path)

    (tmp_path / "api" / "__pycache__").mkdir()
    (tmp_path / "api" / "__pycache__" / "module.cpython-312.pyc").write_bytes(b"pyc-bytes")
    (tmp_path / "worker" / "__pycache__").mkdir()
    (tmp_path / "worker" / "__pycache__" / "task.cpython-312.pyc").write_bytes(b"worker-pyc")

    assert _compute_code_fingerprint(tmp_path) == baseline
