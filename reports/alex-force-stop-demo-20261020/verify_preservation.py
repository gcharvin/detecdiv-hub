import json
from common import ops, OUT

before=json.loads((OUT/'before.json').read_text())
after=ops.save('after.json',ops.snapshot())
current={j['id']:j for j in after['jobs']}
others=[j for j in before['jobs'] if j['status'] in ('running','cancelling') and j['id']!='c0fc44c4-4380-4c48-8f9d-8ad2b3286e6e']
heartbeat_observations=[]
for old in others:
    new=current[old['id']]
    for field in ('params_sha256','allocation','started_at','execution_target_id'):
        assert new[field]==old[field],(old['id'],field)
    advanced=new['heartbeat_at']>old['heartbeat_at']
    heartbeat_observations.append({'job_id':old['id'],'before':old['heartbeat_at'],'after':new['heartbeat_at'],'advanced':advanced})
    if old['id']=='137eec41-3bfc-4c7a-bbbf-03d5a948a329':
        assert advanced
workers={(w['target_key'],w['worker_instance']):w for w in after['workers']}
for old in before['workers']:
    if old['current_job_id'] in {j['id'] for j in others}:
        new=workers[(old['target_key'],old['worker_instance'])]
        assert new['process_id']==old['process_id']
        assert new['current_job_id']==old['current_job_id']
for target in after['targets']:
    meta=target['metadata_json'] or {}
    assert not meta.get('alex_legacy_stop_20261020')
    assert not meta.get('alex_force_stop_demo_admission')
    assert not meta.get('drain_new_jobs')
result={'database_state_preserved':True,'protected_job_ids':[j['id'] for j in others],
        'checks':['unchanged registered worker PIDs','unchanged start times','unchanged allocations','unchanged parameter hashes','Linux heartbeat advancing','admission restored'],
        'heartbeat_observations':heartbeat_observations,
        'limitation':'Windows heartbeat was already stale at the initial snapshot (13:21 UTC); registered PID 31820 is absent on the host at final verification. No Windows process was modified by this operation. See windows-final-state.json. Database state does not establish Windows process health.'}
ops.save('other-jobs-preserved.json',result)
print(result)
