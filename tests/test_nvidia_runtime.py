import pytest

from worker.nvidia_runtime import matching_nvidia_environment


def test_staged_runtime_is_scoped_to_loaded_kernel_driver(tmp_path):
    version = tmp_path / "version"
    version.write_text("NVRM version: NVIDIA UNIX x86_64 Kernel Module 580.173.02")
    root = tmp_path / "runtimes"
    libraries = root / "580.173.02/usr/lib/x86_64-linux-gnu"
    libraries.mkdir(parents=True)
    for name in ("libcuda.so.1", "libnvidia-ml.so.1"):
        (libraries / name).write_bytes(b"staged library fixture")
    original = {"LD_LIBRARY_PATH": "/existing", "OTHER": "preserved"}
    result = matching_nvidia_environment(original, runtime_root=root, version_file=version)
    assert result["LD_LIBRARY_PATH"] == str(libraries) + ":/existing"
    assert result["DETECDIV_HUB_NVIDIA_RUNTIME_VERSION"] == "580.173.02"
    assert result["OTHER"] == "preserved"
    assert original == {"LD_LIBRARY_PATH": "/existing", "OTHER": "preserved"}
    version.write_text("NVRM version: NVIDIA UNIX x86_64 Kernel Module 580.178.04")
    assert matching_nvidia_environment(original, runtime_root=root, version_file=version) is original


def test_absent_runtime_leaves_environment_unchanged(tmp_path):
    original = {"OTHER": "preserved"}
    assert matching_nvidia_environment(original, runtime_root=tmp_path / "absent") is original


def test_incomplete_matching_runtime_refuses_to_launch(tmp_path):
    version = tmp_path / "version"
    version.write_text("NVRM version: NVIDIA UNIX x86_64 Kernel Module 580.173.02")
    root = tmp_path / "runtimes"
    (root / "580.173.02").mkdir(parents=True)
    with pytest.raises(RuntimeError, match="Incomplete staged NVIDIA runtime"):
        matching_nvidia_environment({}, runtime_root=root, version_file=version)
