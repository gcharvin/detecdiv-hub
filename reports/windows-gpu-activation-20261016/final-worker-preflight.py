from pathlib import Path
import hashlib
import json
import os
import traceback
from sqlalchemy import select
from api.db import SessionLocal
from api.config import get_settings
from api.models import Job,ExecutionTarget
from api.services.job_priority_settings import resolve_job_resource_runtime_config
from api.services.runtime_version import get_runtime_version_info
from worker.job_resources import resource_config_for_target,resolve_job_resource_allocation,allocation_fits
from worker.pipeline_run_executor import normalize_pipeline_run_payload,normalize_pipeline_ref_paths_for_posix
from worker.path_mappings import parse_worker_path_mappings,map_payload_path_fields

stage=Path('C:/ProgramData/DetecDivHub/gpu-validation-20261016')
result={}
try:
    with SessionLocal() as s:
        j=s.get(Job,'43637ab4-4044-443d-b8f9-3372935f8356')
        t=s.get(ExecutionTarget,j.execution_target_id)
        assert j.status=='queued' and t.target_key=='windows-10-20-11-56' and t.metadata_json['drain_new_jobs']
        assert t.supports_gpu
        config=resource_config_for_target(resolve_job_resource_runtime_config(s),t)
        assert config.gpu_vram_capacity_mb==14336
        a=resolve_job_resource_allocation(s,job=j,target=t,config=config)
        assert a['gpu_required'] and a['gpu_vram_mb']==8192
        settings=get_settings()
        mappings=parse_worker_path_mappings(settings.worker_path_mappings)
        payload=map_payload_path_fields(normalize_pipeline_run_payload(s,job=j),mappings)
        payload=normalize_pipeline_ref_paths_for_posix(payload=payload,job=j,path_mappings=mappings,runtime_dir=stage)
        pipeline=json.loads(Path(payload['pipeline_ref']['pipeline_json_path']).read_text())
        nodes=pipeline['nodes'] if isinstance(pipeline['nodes'],list) else [pipeline['nodes']]
        package=Path(nodes[0]['customPackageDir'])
        assert package.is_dir()
        assert hashlib.sha256((package/'train.m').read_bytes()).hexdigest()=='255e058e53858873f6742bff3a710f57e766ca591d7c95242e94ab93cce21dfa'
        assert Path(os.environ['DETECDIV_PHASE_CELLPOSE_PYTHON']).as_posix()=='C:/ProgramData/DetecDivHub/cellpose-gpu-v1/Scripts/python.exe'
        totals={'cpu_cores':0,'memory_mb':0,'disk_io_units':0,'gpu_exclusive':False,'gpu_jobs':0,'gpu_vram_mb':0}
        assert allocation_fits(a,totals=totals,config=config)[0]
        large={**a,'gpu_vram_mb':16384}
        assert allocation_fits(large,totals=totals,config=config)==(False,'gpu_vram_capacity')
        result={'status':'passed','fingerprint':get_runtime_version_info()['code_fingerprint'],'allocation':a,'target_gpu_capacity_mib':config.gpu_vram_capacity_mb,'package':str(package),'mapped_pipeline':payload['pipeline_ref']['pipeline_json_path']}
except Exception:
    result={'status':'failed','traceback':traceback.format_exc()}
(stage/'worker-preflight-result.json').write_text(json.dumps(result,indent=2)+'\n')
print(json.dumps(result))
if result['status']!='passed': raise SystemExit(1)
