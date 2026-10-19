import json
import ops
from rollout import snapshot_code

current = ops.api(snapshot_code)
ops.save('final-live-snapshot.json', current)
before = json.loads((ops.OUT / 'before-activation.json').read_text())
old_jobs = {job['id']: job for job in before['jobs']}
jobs = {job['id']: job for job in current['jobs']}
for key, original in old_jobs.items():
    assert jobs[key]['params_hash'] == original['params_hash']
    assert jobs[key]['allocation'] == original['allocation']
    assert jobs[key]['started_at'] == original['started_at']
    assert jobs[key]['heartbeat'] > original['heartbeat']
for original in before['workers']:
    if original['job']:
        w = next(w for w in current['workers'] if w['target'] == original['target'] and w['instance'] == original['instance'])
        assert w['pid'] == original['pid'] and w['job'] == original['job']
linux = [w for w in current['workers'] if w['target'] == 'detecdiv-server']
active = [w['instance'] for w in linux if w['fingerprint'] == 'c367c1f32f1e']
pending = [w['instance'] for w in linux if w['fingerprint'] != 'c367c1f32f1e']
assert len(active) == 5 and pending == ['@4']
for target in current['targets'].values():
    assert not target['drain_new_jobs'] and not target['roi_memory_rollout']
summary = {
 'as_of':current['at'],
 'status':'linux_policy_active_busy_pollers_deferred',
 'linux':{'active_instances':sorted(active),'pending_instances':pending,'fingerprint':'c367c1f32f1e',
          'auto_memory_mib':{'minimum':24576,'preferred':49152},
          'idle_activation':'systemd user timer every 30 seconds'},
 'windows':{'status':'merged_staged_and_verified_busy_activation_deferred',
            'active_fingerprint':'957aa209126d','intended_fingerprint':'89081645c071',
            'dynamic_memory':False,'default_memory_mib':24576,
            'idle_activation':'SYSTEM scheduled task every minute; self-removes after verification'},
 'protected_jobs':list(old_jobs),
 'preservation_verified':['poller PIDs','job parameter hashes','resource allocations','start times','advancing heartbeats'],
 'tests':{'local_python_passed':83,'linux_staged_targeted_passed':65,'windows_staged_targeted_passed':65,
          'matlab_previous_implementation_passed':11},
 'test_scope':'Targeted resource admission and adjacent worker tests; not the complete remote test suite. Broader probes encountered existing host-version mismatches in unchanged reporting, cancellation and audit modules.',
 'policy_probe':'Read-only DB transaction; real deployed Linux modules and merged staged Windows modules; no queued synthetic or scientific jobs',
 'api':'not updated; health and DB status verified OK',
 'rollback':'host-local .deploy-backups/roi-auto-memory-20261019; activation manifests preserve exact before/after hashes',
 'pending_activation_is_not_complete':True,
}
ops.save('summary.json', summary)
print(json.dumps(summary))
