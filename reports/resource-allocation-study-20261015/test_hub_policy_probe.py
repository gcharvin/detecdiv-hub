"""Isolated probes of current Hub code: all sessions and host values mocked."""
from contextlib import contextmanager
from types import SimpleNamespace

from api.services.job_priority_settings import JobResourceRuntimeConfig, default_job_resource_profiles
from worker import memory_resources, record_worker_exit
from worker.job_resources import allocation_fits


def test_actual_dynamic_policy_reserves_potential_idle_worker_overhead(monkeypatch):
    for key in ['DETECDIV_HUB_WORKER_CPU_LIMIT','DETECDIV_HUB_HOST_MEMORY_HEADROOM_MB']:
        monkeypatch.delenv(key,raising=False)
    monkeypatch.setenv('DETECDIV_HUB_WORKER_DYNAMIC_RESOURCES','1')
    monkeypatch.setenv('DETECDIV_HUB_WORKER_MEMORY_BUDGET_MB','98304')
    monkeypatch.setenv('DETECDIV_HUB_HOST_MEMORY_RESERVE_MB','24576')
    monkeypatch.setenv('DETECDIV_HUB_WORKER_CPU_BUDGET','36')
    monkeypatch.setattr(memory_resources.psutil,'virtual_memory',lambda:SimpleNamespace(
        total=128*2**30,available=100*2**30))
    target=SimpleNamespace(metadata_json={'memory_capacity_mb':98304,
        'worker_cpu_capacity':36,'worker_autoscale_enabled':True})
    policy=memory_resources.memory_policy(target)
    assert policy['capacity_mb']==78*1024
    assert policy['worker_limit_mb']==78*1024
    job=SimpleNamespace(params_json={'resources':{'memory_mb':48*1024}})
    assert memory_resources.requested_memory_mb(job,policy=policy)==48*1024


def test_three_existing_24gib_reservations_have_no_24gib_loan_headroom():
    config=JobResourceRuntimeConfig(default_job_resource_profiles(),36,24576,12)
    totals=dict(memory_mb=72*1024,memory_capacity_mb=78*1024,
                cpu_cores=24,disk_io_units=9,gpu_jobs=3,gpu_vram_mb=24576,
                gpu_exclusive=False)
    delta=dict(memory_mb=24*1024,cpu_cores=0,disk_io_units=0,gpu_required=False)
    assert allocation_fits(delta,totals=totals,config=config)==(False,'memory_capacity')


def test_oom_attribution_uses_job_allocation_with_complete_fixture(monkeypatch):
    monkeypatch.setenv('SERVICE_RESULT','oom-kill')
    monkeypatch.setenv('DETECDIV_HUB_WORKER_MEMORY_LIMIT_MB','32768')
    job=SimpleNamespace(id='only-this-job',status='running',params_json={
        '_hub_resource_allocation':{'memory_mb':24576,'swap_limit_mb':1536}})
    session=SimpleNamespace(scalars=lambda _:SimpleNamespace(first=lambda:SimpleNamespace(
        current_job_id=job.id)),get=lambda *a:job)
    @contextmanager
    def scope():
        yield session
    monkeypatch.setattr(record_worker_exit,'session_scope',scope)
    monkeypatch.setattr(record_worker_exit,'get_settings',lambda:SimpleNamespace(worker_target_key='fake'))
    monkeypatch.setattr(record_worker_exit,'get_worker_instance_id',lambda:'@2')
    failures=[]
    monkeypatch.setattr(record_worker_exit,'mark_job_failed',lambda *args:failures.append(args))
    record_worker_exit.record_exit()
    assert failures[0][0]==job.id
    assert '24576 MiB RAM, 1536 MiB swap' in failures[0][1]
    job.status='done';record_worker_exit.record_exit()
    assert len(failures)==1
