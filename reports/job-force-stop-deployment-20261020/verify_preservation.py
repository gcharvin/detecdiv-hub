import json
import ops

before=json.loads((ops.OUT/'before.json').read_text())
after=ops.save('preservation-snapshot.json',ops.snapshot())
jobs={j['id']:j for j in after['jobs']}
protected=[j for j in before['jobs'] if j['status'] in ('running','cancelling')]
for old in protected:
    new=jobs[old['id']]
    for field in ('started_at','params_sha256','allocation','execution_target_id'):
        assert new[field]==old[field],(old['id'],field)
    assert new['heartbeat_at']>old['heartbeat_at']
workers={(w['target_key'],w['worker_instance']):w for w in after['workers']}
for old in before['workers']:
    if old['current_job_id']:
        new=workers[(old['target_key'],old['worker_instance'])]
        assert new['process_id']==old['process_id']
        assert new['current_job_id']==old['current_job_id']
for t in after['targets']:
    assert not (t['metadata_json'] or {}).get('force_stop_rollout')
    assert not (t['metadata_json'] or {}).get('force_stop_smoke_admission')
    assert not (t['metadata_json'] or {}).get('drain_new_jobs')
ops.save('protected-jobs-verification.json',{'verified':True,'ids':[j['id'] for j in protected],
         'checks':['same worker PIDs','same job parameter hashes','same allocations','same start times','advancing heartbeats','admission restored']})
print('Protected jobs preserved:',[j['id'] for j in protected])
for label in ('linux','windows'):
    result=ops.remote(label, '''
import json,hashlib,subprocess,os
from pathlib import Path
root=Path.cwd();stage=root/'.deploy-staging/job-force-stop-20261020'
m=json.loads((stage/'manifest.json').read_text())
checks={}
for name,item in m['files'].items():
 p=root/name;live=hashlib.sha256(p.read_bytes()).hexdigest() if p.exists() else None
 assert live in (item['before'],item['after']),name
 assert hashlib.sha256((stage/'files'/name).read_bytes()).hexdigest()==item['after'],name
 checks[name]='active' if live==item['after'] else 'staged'
if os.name=='nt':
 ps="""$t=Get-ScheduledTask -TaskName 'DetecDiv Hub Force Stop Activation 20261020';$i=$t|Get-ScheduledTaskInfo;@{state=[string]$t.State;lastResult=$i.LastTaskResult;nextRun=$i.NextRunTime.ToString('o');worker=[string](Get-ScheduledTask -TaskName 'DetecDiv Hub Worker').State;tunnel=[string](Get-ScheduledTask -TaskName 'DetecDiv Hub Database Tunnel').State}|ConvertTo-Json -Compress"""
 r=subprocess.run(['powershell.exe','-NoProfile','-NonInteractive','-Command',ps],capture_output=True,text=True);assert r.returncode==0,r.stderr
 guard=json.loads(r.stdout.strip());assert guard['lastResult']==0 and guard['tunnel']=='Running'
else:
 env={**os.environ,'XDG_RUNTIME_DIR':'/run/user/'+str(os.getuid())}
 guard=subprocess.check_output(['systemctl','--user','show','detecdiv-force-stop-activation-20261020.timer','-p','ActiveState','-p','NextElapseUSecMonotonic'],env=env,text=True)
 assert 'ActiveState=active' in guard
activation=json.loads((stage/'activation-last.json').read_text());assert not activation.get('error'),activation
print(json.dumps({'checks':checks,'expected_fingerprint':m['expected_fingerprint'],'watcher':guard,'activation':activation}))
''')
    ops.save(label+'-activation-verification.json',result)
    print(label,result['expected_fingerprint'],result['activation'])
