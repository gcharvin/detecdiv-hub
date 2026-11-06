"""Read existing detached state only; never launch or restart any task."""
from datetime import datetime, timezone
import json
from pathlib import Path
import sys

HERE=Path(__file__).resolve().parent
sys.path.insert(0,str(HERE.parent/'homes2-access-audit-20261101'))
from activate_windows_mapping import remote

CODE=r'''
import json,subprocess,re
from pathlib import Path
w=Path('/home/charvin-admin/storage-transfers/alexander-2024-20261105')
def load(name):
 p=w/name
 return json.loads(p.read_text()) if p.exists() else None
def tail(name,size=4000):
 p=w/name
 if not p.exists():return ''
 with p.open('rb') as f:
  f.seek(max(0,p.stat().st_size-size));return f.read().decode(errors='replace')
state=load('state.json')
summary={'copy_state':state,'controller_exit':tail('controller.exit'),'controller_log_tail':tail('controller.log'),
 'project_discovery_progress':load('project-discovery-progress.json'),'project_discovery_result':load('project-discovery-result.json'),
 'project_discovery_exit':tail('project-discovery.exit'),'project_discovery_log_tail':tail('project-discovery.log'),
 'sessions':subprocess.run(['tmux','list-sessions','-F','#{session_name}'],capture_output=True,text=True).stdout.splitlines()}
summary['project_copy_state']=load('project-copies/state.json')
summary['project_copy_exit']=tail('project-copies/controller.exit')
summary['project_copy_log_tail']=tail('project-copies/controller.log')
summary['supplemental_progress']=load('project-discovery-supplemental/project-discovery-progress.json')
summary['supplemental_result']=load('project-discovery-supplemental/project-discovery-result.json')
summary['supplemental_exit']=tail('project-discovery-supplemental/project-discovery.exit')
summary['supplemental_log_tail']=tail('project-discovery-supplemental/project-discovery.log')
summary['legacy_review_progress']=load('legacy-path-review/progress.json')
summary['legacy_review_result']=load('legacy-path-review/result.json')
summary['legacy_review_exit']=tail('legacy-path-review/controller.exit')
summary['legacy_review_log_tail']=tail('legacy-path-review/controller.log')
summary['legacy_retry_progress']=load('legacy-path-review-retry-20261106/progress.json')
summary['legacy_retry_result']=load('legacy-path-review-retry-20261106/result.json')
summary['legacy_retry_exit']=tail('legacy-path-review-retry-20261106/controller.exit')
summary['legacy_retry_log_tail']=tail('legacy-path-review-retry-20261106/controller.log')
summary['legacy_retry2_progress']=load('legacy-path-review-retry2-20261106/progress.json')
summary['legacy_retry2_result']=load('legacy-path-review-retry2-20261106/result.json')
summary['legacy_retry2_exit']=tail('legacy-path-review-retry2-20261106/controller.exit')
summary['legacy_retry2_log_tail']=tail('legacy-path-review-retry2-20261106/controller.log')
summary['exception_copy_state']=load('exception-copies-20261106/state.json')
summary['exception_copy_exit']=tail('exception-copies-20261106/controller.exit')
summary['exception_copy_log_tail']=tail('exception-copies-20261106/controller.log')
summary['identity_batch1_progress']=load('identity-audit-20261106-batch1/progress.json')
summary['identity_batch1_exit']=tail('identity-audit-20261106-batch1/controller.exit')
summary['identity_batch1_log_tail']=tail('identity-audit-20261106-batch1/controller.log')
summary['identity_batch1_result_exists']=(w/'identity-audit-20261106-batch1/result.json').exists()
summary['identity_batch2_progress']=load('identity-audit-20261106-batch2/progress.json')
summary['identity_batch2_partial']=load('identity-audit-20261106-batch2/result.partial.json')
summary['identity_batch2_result_exists']=(w/'identity-audit-20261106-batch2/result.json').exists()
summary['identity_batch2_exit']=tail('identity-audit-20261106-batch2/controller.exit')
summary['identity_batch2_log_tail']=tail('identity-audit-20261106-batch2/controller.log')
summary['persistent_batch1_progress']=load('persistent-path-audit-20261106-batch1/progress.json')
summary['persistent_batch1_result']=load('persistent-path-audit-20261106-batch1/result.json')
summary['persistent_batch1_exit']=tail('persistent-path-audit-20261106-batch1/controller.exit')
summary['persistent_batch1_log_tail']=tail('persistent-path-audit-20261106-batch1/controller.log')
summary['size_diagnosis_v2_exit']=tail('saved-size-diagnosis-v2-20261106/controller.exit')
if state:
 year=state.get('current_year')
 if year in ['2024_1','2024_2']:
  summary['copy_progress_tail']=tail(year+'/copy.log',1400)
  summary['verification_log_bytes']=(w/year/'verify-full-sha1.log').stat().st_size if (w/year/'verify-full-sha1.log').exists() else None
 pids=[str(state[k]) for k in ['runner_pid','child_pid'] if state.get(k)]
 if pids:summary['copy_processes']=subprocess.run(['ps','-p',','.join(pids),'-o','pid=,comm=,etimes='],capture_output=True,text=True).stdout.strip()
receipt=w/'project-discovery-receipt.jsonl';matches=[];review=[];count=0
if receipt.exists():
 for line in receipt.open():
  try:r=json.loads(line)
  except json.JSONDecodeError:continue
  count+=1
  if r.get('matching_acquisitions') or r.get('explicit_2024_reference'):matches.append(r)
  if r['status']=='requires_review':review.append({k:r.get(k) for k in ['project_id','name','owner','file','error']})
summary.update(project_receipts=count,project_candidates=matches,project_requires_review=review)
print(json.dumps(summary))
'''

if __name__=='__main__':
    result=remote('linux',CODE,timeout=90)
    private=HERE/'runtime';private.mkdir(exist_ok=True)
    (private/'latest-status.json').write_text(json.dumps(result,indent=2)+'\n',encoding='utf-8')
    summary={k:v for k,v in result.items() if k not in ['project_candidates','project_requires_review','identity_batch2_partial']}
    partial=result.get('identity_batch2_partial') or []
    summary['identity_batch2_projects']=[{k:p.get(k) for k in ['project_id','name','success','fovs','rois','error','completed_fovs_in_partial','main_original_and_copy_hashes_unchanged']} for p in partial]
    summary['candidate_projects']=[{'id':p['project_id'],'name':p['name'],'owner':p['owner'],'fovs':p['fovs'],'matching_acquisitions':p['matching_acquisitions']} for p in result['project_candidates']]
    summary['requires_review_count']=len(result['project_requires_review'])
    summary['review_examples']=result['project_requires_review'][:4]
    import re
    matches=re.findall(r'(?:^|[\r\n])\s*([\d.,]+)\s+\d+%',result.get('copy_progress_tail',''))
    if matches:
        transferred=int(re.sub(r'[^\d]','',matches[-1]))
        summary['copied_bytes']=transferred
        state=result['copy_state'];year=state.get('current_year');total=state['years'][year].get('source_inventory',{}).get('bytes')
        if total:summary['copy_percent_of_complete_year']=round(transferred/total*100,4)
    summary['rsync_display_percent_is_incremental_file_list_not_full_year']=True
    summary['checked_at']=datetime.now(timezone.utc).isoformat()
    (HERE/'latest-status.json').write_text(json.dumps(summary,indent=2)+'\n',encoding='utf-8')
    print(json.dumps(summary,indent=2))
