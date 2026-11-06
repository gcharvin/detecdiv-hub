"""Correctly normalize single Windows separators in the 28 JSON alternatives."""
import json
from pathlib import Path
import sys

HERE=Path(__file__).resolve().parent
sys.path.insert(0,str(HERE.parent/'homes2-access-audit-20261101'))
from activate_windows_mapping import remote

CODE=r'''
import json,hashlib,re
from pathlib import Path
root=Path('/home/charvin-admin/storage-transfers/alexander-2024-20261105')
plan=json.loads((root/'project-discovery-format-review.json').read_text())
original=json.loads((root/'project-discovery-request.json').read_text())
output=root/'legacy-path-review/json-alternatives-normalized-20261106.json'
assert not output.exists(),'Bounded review already exists'
def strings(value):
 if isinstance(value,str):yield value
 elif isinstance(value,list):
  for item in value:yield from strings(item)
 elif isinstance(value,dict):
  for item in value.values():yield from strings(item)
def normalized(value):return '/'+re.sub('/+','/',value.replace(chr(92),'/')).strip('/')+'/'
assert '/22032024_DHY210_nts1ng/' in normalized('Z:'+chr(92)+'Alexander'+chr(92)+'22032024_DHY210_nts1ng'+chr(92)+'Pos1')
rows=[]
for item in plan['missing_json_alternatives']:
 row={k:item[k] for k in ['id','name','owner','files']};row.update(matches=[],source_files=[],explicit_2024_reference=False)
 for file in item['files']:
  p=Path(file);before=p.stat();data=p.read_bytes();document=json.loads(data.decode('utf-8-sig'));count=0
  for value in strings(document):
   v=normalized(value);count+=1
   if '/Alexander/data/2024_1/' in v or '/Alexander/data/2024_2/' in v:row['explicit_2024_reference']=True
   for acq in original['acquisitions']:
    if '/'+acq['name']+'/' in v:row['matches'].append({'acquisition':acq['year']+'/'+acq['name'],'value':value})
  after=p.stat();assert (before.st_ino,before.st_size,before.st_mtime_ns)==(after.st_ino,after.st_size,after.st_mtime_ns)
  row['source_files'].append({'file':file,'bytes':len(data),'sha256':hashlib.sha256(data).hexdigest(),'strings_inspected':count,'metadata_stable':True})
 rows.append(row)
output.write_text(json.dumps(rows,indent=2)+'\n')
print(json.dumps({'success':True,'projects':len(rows),'candidate_projects':[{'id':r['id'],'name':r['name'],'owner':r['owner']} for r in rows if r['matches'] or r['explicit_2024_reference']],
 'receipt_sha256':hashlib.sha256(output.read_bytes()).hexdigest(),'single_windows_separator_fixture_passed':True,'source_saved':False,
 'supersedes_initial_python_json_normalization':True,'private_receipt':str(output)}))
'''

if __name__=='__main__':
    result=remote('linux',CODE,timeout=90)
    with (HERE/'json-alternatives-normalization-verification.json').open('x',encoding='utf-8') as f:json.dump(result,f,indent=2);f.write('\n')
    print(json.dumps(result,indent=2))
