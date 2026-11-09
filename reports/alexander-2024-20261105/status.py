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
summary['project_copy_continuation_state']=load('project-copies-continuation-20261106/state.json')
summary['project_copy_continuation_exit']=tail('project-copies-continuation-20261106/controller.exit')
summary['project_copy_continuation_log_tail']=tail('project-copies-continuation-20261106/controller.log')
combined=(summary['project_copy_state'] or {}).get('projects',[])+(summary['project_copy_continuation_state'] or {}).get('projects',[])
assert len({p['project_id'] for p in combined})==len(combined),'Duplicate verified project identities need review'
summary['combined_standard_project_copies_verified']=len(combined)
summary['main_only_copy_result']=load('main-only-copy-20261106/result.json')
summary['main_only_copy_exit']=tail('main-only-copy-20261106/controller.exit')
summary['relink_batch1_state']=load('relink-batch1-20261106/state.json')
summary['relink_batch1_exit']=tail('relink-batch1-20261106/controller.exit')
summary['relink_batch1_progress']=load('relink-batch1-20261106/progress.json')
summary['relink_batch1_visitor_progress']=load('relink-batch1-20261106/visitor-progress.json')
summary['relink_batch1_modified_mat_receipts']=len(list((w/'relink-batch1-20261106').glob('modified-*.json')))
summary['relink_batch1_log_tail']=tail('relink-batch1-20261106/controller.log',1200)
summary['relink_batch1_matlab_log_tail']=tail('relink-batch1-20261106/matlab.log',1200)
summary['relink_batch2_state']=load('relink-batch2-20261107/state.json')
summary['relink_batch2_exit']=tail('relink-batch2-20261107/controller.exit')
summary['relink_batch2_progress']=load('relink-batch2-20261107/progress.json')
summary['relink_batch2_visitor_progress']=load('relink-batch2-20261107/visitor-progress.json')
summary['relink_batch2_modified_mat_receipts']=len(list((w/'relink-batch2-20261107').glob('modified-*.json')))
summary['relink_batch2_log_tail']=tail('relink-batch2-20261107/controller.log',1200)
summary['relink_batch2_matlab_log_tail']=tail('relink-batch2-20261107/matlab.log',1200)
summary['relink_batch3_state']=load('relink-batch3-20261107/state.json')
summary['relink_batch3_exit']=tail('relink-batch3-20261107/controller.exit')
summary['relink_batch3_progress']=load('relink-batch3-20261107/progress.json')
summary['relink_batch3_modified_mat_receipts']=len(list((w/'relink-batch3-20261107').glob('modified-*.json')))
summary['relink_batch3_log_tail']=tail('relink-batch3-20261107/controller.log',1200)
summary['relink_batch4_state']=load('relink-batch4-20261107/state.json')
summary['relink_batch4_exit']=tail('relink-batch4-20261107/controller.exit')
summary['relink_batch4_progress']=load('relink-batch4-20261107/progress.json')
summary['relink_batch4_modified_mat_receipts']=len(list((w/'relink-batch4-20261107').glob('modified-*.json')))
summary['relink_batch4_log_tail']=tail('relink-batch4-20261107/controller.log',1200)
publication_work='year1-prepublication-audit-20261109'
summary['year1_prepublication_audit']={'state':load(publication_work+'/state.json'),
 'exit':tail(publication_work+'/controller.exit'),'log_tail':tail(publication_work+'/controller.log',1200)}
publication_result=load(publication_work+'/result.json')
if publication_result:
 summary['year1_prepublication_audit']['result_summary']={k:publication_result[k] for k in ['success','read_only','source_deleted','published','catalog_changed','fresh_publication_admission_and_host_catalog_api_gates_still_required']}
 summary['year1_prepublication_audit']['result_summary']['projects']=len(publication_result['projects'])
summary['year1_final_publication']={'state':load('year1-final-publication-20261109/state.json'),
 'exit':tail('year1-final-publication-20261109/controller.exit'),
 'log_tail':tail('year1-final-publication-20261109/controller.log',1200)}
summary['year1_final_linux_hosts']={'state':load('year1-final-hosts-20261109/host-linux-state.json'),
 'progress':load('year1-final-hosts-20261109/host-linux-progress.json'),
 'exit':tail('year1-final-hosts-20261109/controller.exit'),
 'log_tail':tail('year1-final-hosts-20261109/controller.log',1200)}
lox_work='lox-metadata-preparation-20261107'
summary['lox_metadata_preparation']={'exit':tail(lox_work+'/controller.exit'),
 'path_progress':load(lox_work+'/paths/progress.json'),'path_result':load(lox_work+'/paths/result.json'),
 'type_progress':load(lox_work+'/types/progress.json'),'log_tail':tail(lox_work+'/controller.log',1200)}
lox_types=load(lox_work+'/types/result.json')
if lox_types:
 summary['lox_metadata_preparation']['type_summary']={'success':lox_types.get('success'),
  'files':len(lox_types['files']),'gpu_values':sum(len(f['gpu_values']) for f in lox_types['files']),
  'function_handles':sum(len(f['function_handles']) for f in lox_types['files']),
  'nonpublic_properties':sum(len(f['nonpublic_persistent_properties']) for f in lox_types['files'])}
for label,name in [('remaining','relink-remaining-mains-20261107'),('gilles_recovery','relink-gilles-recovery-20261107')]:
 summary['relink_'+label]={'state':load(name+'/state.json'),'exit':tail(name+'/controller.exit'),
  'progress':load(name+'/progress.json'),'matlab_exit':tail(name+'/matlab.exit'),
  'modified_mat_receipts':len(list((w/name).glob('modified-[0-9]*.json'))),
  'modified_json_receipts':len(list((w/name).glob('modified-json-*.json'))),
  'log_tail':tail(name+'/controller.log',1200)}
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
summary['reference_followups']={}
for name in ['persistent-path-audit-20261106-batch2','historical-reference-baseline-20261106',
             'blank-roi-layout-20261106','historical-reference-parity-20261106',
             'historical-name-search-20261106','blank-roi-identity-continuation-20261106',
             'identity-audit-20261106-batch3','identity-audit-20261106-batch4',
             'persistent-path-audit-20261106-batch3','figure-path-audit-20261106-batch2',
             'exception-identity-20261106','gilles-roi-layout-20261106',
             'gilles-hdf5-identity-20261106','relink-value-types-20261106',
             'exception-path-audit-20261106','existing-layout-review-20261106',
             'missing-project-directory-search-20261106','identity-audit-20261106-batch5',
             'persistent-path-audit-20261106-batch4','availability-variants-identity-20261106',
             'relink-batch1-gpu-postflight-20261107','availability-variants-identity-v2-20261107',
             'persistent-path-audit-20261107-batch5','figure-path-audit-20261107-batch3',
             'relink-value-types-20261107-batch2','gilles-hdf5-metadata-20261107',
             'persistent-path-audit-20261107-batch6','relink-value-types-20261107-batch3',
             'figure-path-audit-20261107-batch4','pending-availability-diagnosis-20261107',
             'pending-availability-diagnosis-v2-20261107','pending-reference-parity-20261107',
             'relink-value-types-20261107-batch4','figure-path-audit-20261107-batch5',
             'movie-cache-identity-20261107','persistent-path-audit-movie-20261107',
             'relink-batch2-gpu-postflight-20261107','relink-value-types-remaining-mains-20261107',
             'gilles-main-typed-paths-20261107','lox-available-fields-20261107']:
 item={'exit':tail(name+'/controller.exit'),'progress':load(name+'/progress.json'),'log_tail':tail(name+'/controller.log',1200)}
 result=load(name+'/result.json')
 if result:
  item['result_summary']={k:result[k] for k in ['success','loop_completed','read_only','source_deleted','reference_count','unique_paths',
     'availability_matches_initial_native_manifest','target_current_presence_and_sizes_match_initial_manifest',
     'available_fields_verification_success','available_fovs','missing_fovs_unresolved',
     'new_source_samples','target_samples','strict_project_access_gate_passed','cleanup_gate_passed'] if k in result}
  if 'projects' in result:
   item['projects']=[{k:p[k] for k in ['project_id','name','success','error','metadata_files_audited','files_needing_rebase',
       'files_with_unresolved_paths','rois','blank_roi_ids_preserved','completed_fovs_reused'] if k in p} for p in result['projects']]
   if any('files_with_unresolved_paths' in p for p in result['projects']):
    item['result_summary']['all_paths_resolved']=all(p.get('files_with_unresolved_paths',0)==0 for p in result['projects'])
    item['result_summary']['unresolved_path_files']=sum(p.get('files_with_unresolved_paths',0) for p in result['projects'])
  if name=='historical-name-search-20261106':
   item['search_results']=[{'target':p['target'],'inventory_counts':p['inventory_counts'],'candidate_count':len(p['candidates'])} for p in result['results']]
  if name in ['relink-value-types-20261106','relink-value-types-20261107-batch2','relink-batch1-gpu-postflight-20261107','relink-value-types-20261107-batch3','relink-value-types-20261107-batch4','relink-batch2-gpu-postflight-20261107']:
   item['scientific_type_files']=len(result['files'])
   item['gpu_values']=sum(len(p['gpu_values']) for p in result['files'])
   item['function_handles']=sum(len(p['function_handles']) for p in result['files'])
   item['nonpublic_properties']=sum(len(p['nonpublic_persistent_properties']) for p in result['files'])
 summary['reference_followups'][name]=item
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
    local_state=private/'year1-advance-state.json'
    if local_state.exists():
        result['year1_local_continuation']=json.loads(local_state.read_text(encoding='utf-8'))
    # Archived reports only: never query a diagnostic task after its cleanup.
    windows_status=private/'year1-windows-status.json'
    if windows_status.exists():
        archived=json.loads(windows_status.read_text(encoding='utf-8'))
        result['year1_final_windows_hosts_archived']={k:v for k,v in archived.items()
          if k in ['task','host-windows-state.json','host-windows-progress.json']}
        result['year1_final_windows_hosts_archived']['cleanup_receipt_exists']=(HERE/'year1-windows-cleanup.json').exists()
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
