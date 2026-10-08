"""Normalize legacy format classification from already-read MAT header facts.

The first native run exposed a MATLAB row/column logical-expansion bug. This
reclassifies only that specific error, using retained name/class/scalar-size
facts; read failures, empty files and unknown formats are not accepted.
"""
import argparse
import json
from pathlib import Path


def main() -> None:
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input',type=Path)
    parser.add_argument('--allow-partial',action='store_true')
    parser.add_argument('--extra-report',type=Path)
    args=parser.parse_args()
    repo = Path(__file__).resolve().parents[2]
    discovery = json.loads((repo/'reports/sauvegarde-project-discovery-20261001.json').read_text(encoding='utf-8'))
    path = repo/'reports/sauvegarde-project-headers-20261001.json'
    headers = json.loads((args.input or path).read_text(encoding='utf-8'))
    if args.extra_report:
        extra=json.loads(args.extra_report.read_text(encoding='utf-8'))
        assert extra['complete']
        extra_rows=extra['candidates']
        if isinstance(extra_rows,dict):
            extra_rows=[extra_rows]
        headers['candidates'].extend(extra_rows)
    assert args.allow_partial or (headers['complete'] and len(headers['candidates'])==len(discovery['candidates']))
    assert len({row['relativePath'] for row in headers['candidates']})==len(headers['candidates'])
    items = {row['relative_path']:row for row in discovery['candidates']}
    fixed = 0
    for row in headers['candidates']:
        item = items[row['relativePath']]
        variables = row['variables']
        if isinstance(variables,dict):
            variables=[variables]
        is_legacy = item['legacy_filename'] and any(v['name']=='timeLapse' and
            v['class']=='struct' and v['size']==[1,1] for v in variables)
        if (row['status']=='inspection_failed' and
                row['review'].startswith('Operands to the logical AND') and is_legacy):
            row['classifier_note'] = 'Normalized retained scalar timeLapse header after MATLAB logical shape error'
            row['status']='valid_project_header'
            row['projectKind']='legacy_matlab_timelapse'
            row['review']=''
            fixed+=1
    headers['legacy_classifier_normalized_count']=fixed
    path.write_text(json.dumps(headers,indent=2)+'\n',encoding='utf-8')
    counts={}
    for row in headers['candidates']:
        key=row['projectKind'] or row['status']
        counts[key]=counts.get(key,0)+1
    print(json.dumps({'headers':len(headers['candidates']),'normalized':fixed,'counts':counts}))


if __name__ == '__main__':
    main()
