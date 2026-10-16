from types import SimpleNamespace

from api.services.job_priority_settings import JobResourceRuntimeConfig
from worker.job_resources import allocation_fits, resource_config_for_target


def config(capacity=24576):
    return JobResourceRuntimeConfig({}, 36, capacity, 12)


def test_target_budget_blocks_a_job_that_only_fits_the_linux_gpu():
    bounded = resource_config_for_target(config(), SimpleNamespace(metadata_json={"gpu_vram_capacity_mb": 14336}))
    totals = {"cpu_cores": 0, "memory_mb": 0, "disk_io_units": 0, "gpu_exclusive": False, "gpu_jobs": 0, "gpu_vram_mb": 0}
    fits, reason = allocation_fits({"cpu_cores": 1, "memory_mb": 1024, "gpu_required": True, "gpu_vram_mb": 16384}, totals=totals, config=bounded)
    assert not fits and reason == "gpu_vram_capacity"
    assert bounded.gpu_vram_capacity_mb == 14336


def test_target_budget_preserves_global_exclusivity_and_lower_limit():
    target = SimpleNamespace(metadata_json={"gpu_vram_capacity_mb": 14336})
    assert resource_config_for_target(config(0), target).gpu_vram_capacity_mb == 0
    assert resource_config_for_target(config(8192), target).gpu_vram_capacity_mb == 8192


def test_unconfigured_targets_keep_existing_admission():
    original = config()
    assert resource_config_for_target(original, None) is original
    assert resource_config_for_target(original, SimpleNamespace(metadata_json={})) is original
