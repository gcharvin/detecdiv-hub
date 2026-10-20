import json
from datetime import datetime,timezone
from sqlalchemy import select,text
from api.db import SessionLocal
from api.models import ExecutionTarget,Job,User
from api.routes_execution_targets import update_execution_target
from api.routes_pipeline_runs import update_pipeline_run
from api.schemas import ExecutionTargetUpdate,PipelineRunUpdateRequest,PipelineRunCreateRequest
from api.services.pipeline_runs import preflight_pipeline_run_request

with SessionLocal() as s:
    s.execute(text("SET LOCAL lock_timeout='5s'"))
    t=s.scalar(select(ExecutionTarget).where(ExecutionTarget.target_key=='windows-10-20-11-56').with_for_update())
    assert t.metadata_json.get('drain_new_jobs') and t.metadata_json.get('windows_gpu_activation',{}).get('id')=='windows-gpu-20261016'
    j=s.scalar(select(Job).where(Job.id=='43637ab4-4044-443d-b8f9-3372935f8356').with_for_update())
    assert j.status=='queued' and j.started_at is None and j.execution_target_id==t.id
    run=j.params_json['run_request']
    duplicates=list(s.scalars(select(Job).where(Job.id!=j.id,Job.status.in_(['queued','running','cancelling']),Job.params_json['run_request']['run_id'].astext==run['run_id'])))
    assert not duplicates,'Duplicate pilot job found'
    u=s.scalar(select(User).where(User.user_key=='gilles'))
    update_execution_target(t.id,ExecutionTargetUpdate(supports_gpu=True,metadata_json={'gpu_vram_capacity_mb':14336,'gpu_model':'NVIDIA RTX 2000 Ada Generation','gpu_validation':{'at':datetime.now(timezone.utc).isoformat(),'account':'GMGM\\Charvin-Admin','session_id':1,'matlab_gpu_device':'passed','cellposesam_gpu_inference':'passed','runtime':'C:/ProgramData/DetecDivHub/cellpose-gpu-v1/Scripts/python.exe'}}),db=s,current_user=u)
    new_path='/data/Gilles/analysis/classifiers/phase_cellposesam_v001_1/pipeline_templates/train_phase_cellposesam_v001/windows_gpu_v1/pipeline.json'
    update_pipeline_run(j.id,PipelineRunUpdateRequest(pipeline_ref={'pipeline_json_path':new_path},client_context={'runtime_amendment':'/data/Gilles/analysis/classifiers/phase_cellposesam_v001_1/runtime_amendment_windows_gpu_v1.json','windows_gpu_activation':{'state':'validated_pending_dispatch','original_target':'0d9f55dc-54ce-4527-af5b-ff4b5d0d64e9','authorized_by':'gilles','at':datetime.now(timezone.utc).isoformat()}}),db=s,current_user=u)
    assert j.params_json['run_request']==run,'Scientific run request changed'
    payload=PipelineRunCreateRequest(project_id=j.project_id,pipeline_id=j.pipeline_id,execution_target_id=j.execution_target_id,requested_mode=j.requested_mode,priority=j.priority,requested_by=j.requested_by,project_ref=j.params_json['project_ref'],pipeline_ref=j.params_json['pipeline_ref'],run_request=run,execution=j.params_json['execution'],client_context=j.params_json['client_context'])
    preflight=preflight_pipeline_run_request(s,payload=payload,current_user=u)
    assert preflight.can_submit,preflight.model_dump(mode='json')
    print(json.dumps({'job_id':str(j.id),'status':j.status,'target':t.target_key,'supports_gpu':t.supports_gpu,'gpu_vram_capacity_mb':t.metadata_json['gpu_vram_capacity_mb'],'drain':t.metadata_json['drain_new_jobs'],'duplicate_jobs':len(duplicates),'preflight':preflight.model_dump(mode='json')},default=str))
