from pathlib import Path
from datetime import datetime,timezone
import json
import sys
sys.path.insert(0,str(Path(__file__).resolve().parents[2]/'scripts/ops'))
import windows_gpu_activation as op

hub=json.loads(op.api((op.OUT/'job-snapshot.py').read_text()))
server=json.loads(op.linux(r'''
from pathlib import Path
import json,hashlib
r=Path('/data/Gilles/analysis/classifiers/phase_cellposesam_v001_1')
p=r/'pilot_python.log'
log=p.read_text(errors='replace').replace('\x00','') if p.exists() else ''
started=r/'fit_v001/training_started.json'
print(json.dumps({'training_started':json.loads(started.read_text()) if started.exists() else None,'python_log_tail':log[-2400:],'protocol_sha256':hashlib.sha256((r/'protocol_frozen.json').read_bytes()).hexdigest(),'final_manifest_present':(r/'fit_v001/manifest.json').exists()}))
'''))
windows=json.loads(op.win(r'''
$ps=@(Get-CimInstance Win32_Process | Where-Object {$_.Name -eq 'python.exe' -and $_.CommandLine -match '[+]phaseCellposeSAM.*pilot[.]py'})
$ids=@($ps | ForEach-Object ProcessId)
$launches=@($ps | Where-Object {$_.ParentProcessId -notin $ids})
$gpu=(& nvidia-smi --query-gpu=memory.used,utilization.gpu --format=csv,noheader,nounits)
@{pilot_processes=@($ps | Select-Object ProcessId,ParentProcessId,SessionId,ExecutablePath);pilot_launch_count=$launches.Count;gpu_usage_csv=$gpu;temporary_tasks_remaining=@(Get-ScheduledTask | Where-Object {$_.TaskName -like 'DetecDiv GPU *20261016'}).Count} | ConvertTo-Json -Compress -Depth 5
'''))
reservation=json.loads((op.OUT/'reservation.json').read_text())
# Runtime preparation adds allocation/provenance fields, not scientific inputs.
compare=json.loads(op.api(r'''
import json
from api.db import SessionLocal
from api.models import Job
with SessionLocal() as s:
 j=s.get(Job,'43637ab4-4044-443d-b8f9-3372935f8356')
 print(json.dumps({'run_request':j.params_json['run_request'],'core_commit':j.params_json['execution']['code_commit'],'attempt_history_count':len(j.result_json.get('attempt_history',[]))}))
'''))
assert reservation['params']['run_request']==compare['run_request']
assert not hub['other_jobs_same_run']
assert hub['target']=='windows-10-20-11-56' and hub['status']=='running'
assert windows['pilot_launch_count']==1
assert server['training_started']['gpu']=='NVIDIA RTX 2000 Ada Generation'
summary={
 'checked_at_utc':datetime.now(timezone.utc).isoformat(),
 'status':'operational; existing job training on Windows GPU',
 'job':{k:hub[k] for k in ['id','status','target','started_at','heartbeat_at','allocation','workers','other_jobs_same_run']},
 'no_duplicate':{'same_job_id_preserved':True,'other_jobs_same_run':0,'windows_pilot_launches':windows['pilot_launch_count'],'prior_failed_pretraining_attempts_retained':compare['attempt_history_count']},
 'validation':{'account':'GMGM\\Charvin-Admin','session_id':1,'matlab_gpu_device':'passed','cuda_forward_backward':'passed','cellposesam_gpu_inference':'passed','dataset_frames_verified':100,'shared_output_write':'passed','full_pipeline_dryrun':'passed','hub_tests_passed':14,'hoct_pilot_tests_passed':9,'mapping_regressions_passed_each_platform':4},
 'configuration':{'supports_gpu':True,'gpu_vram_admission_budget_mib':14336,'max_concurrent_jobs':1,'python':'C:/ProgramData/DetecDivHub/cellpose-gpu-v1/Scripts/python.exe','worker_fingerprint':'957aa209126d','hub_commit':'58b3d18','core_commit':compare['core_commit'],'launcher_commit':'45f68ffc19e6faee2d727506a6bd4fa3b41f3a82'},
 'scientific_run_request_unchanged':True,
 'protocol_sha256':server['protocol_sha256'],
 'training':server,
 'windows_runtime':windows,
 'evidence_directory':'reports/windows-gpu-activation-20261016',
 'note':'The first real attempt failed during path validation before any training. It is retained in the same job history. Full training and paired evaluation remain in progress.'}
(op.ROOT/'reports/windows-gpu-activation-20261016.json').write_text(json.dumps(summary,indent=2)+'\n')
(op.OUT/'final-job-snapshot.json').write_text(json.dumps(hub,indent=2)+'\n')
print(json.dumps({'job_status':hub['status'],'gpu':server['training_started']['gpu'],'launch_count':windows['pilot_launch_count'],'gpu_usage_csv':windows['gpu_usage_csv'],'log_tail':server['python_log_tail'][-1000:]}))
