"""Prepare the small second-pass manifest from revisited non-project containers."""
import json
from pathlib import Path, PurePosixPath

repo=Path(__file__).resolve().parents[2]
discovery=json.loads((repo/'reports/sauvegarde-project-discovery-20261001.json').read_text(encoding='utf-8'))
expanded=[PurePosixPath(path) for path in discovery.get('expanded_unconfirmed',[])]
extra=[row for row in discovery['candidates'] if any(
    PurePosixPath(row['relative_path']).is_relative_to(folder) for folder in expanded)]
payload={'read_only':True,'complete':True,'candidates':extra}
output=repo/'reports/sauve-extra-discovery-20261001.json'
output.write_text(json.dumps(payload,indent=2)+'\n',encoding='utf-8')
print(json.dumps({'extra_candidates':len(extra),'report':str(output)}))
