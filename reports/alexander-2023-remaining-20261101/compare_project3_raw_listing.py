"""Read-only metadata evidence for candidate aliases; never authorizes deletion."""
from collections import Counter
import json
from pathlib import Path, PurePosixPath


def main():
    work = Path('/home/charvin-admin/storage-transfers/alexander-2023-remaining-20261101')
    assert (work/'project3-listing-export.exit').read_text().strip() == '0'
    projects = json.loads((work/'project3-listing-export.json').read_text())
    plan = json.loads((work/'2023_3-project-candidate-plan.json').read_text())['candidate_projects']
    by_id = {p['project_id']: p for p in plan}
    with (work/'2023_3/source-before.jsonl').open() as stream:
        inventory = {x['path']: x for line in stream if (x := json.loads(line))['kind'] == 'file'}
    results = []
    for p in projects:
        candidate = by_id[p['project_id']]
        counts = Counter()
        offsets = Counter()
        examples = []
        unique = set()
        with Path(p['listing_file']).open() as stream:
            for line in stream:
                row = json.loads(line)
                counts['listing_entries'] += 1
                directory = row['legacy_directory'].replace('\\', '/').rstrip('/')
                acqs = [a for a in candidate['candidate_raw_acquisitions'] if '/'+a+'/' in directory]
                if len(acqs) != 1:
                    counts['ambiguous_path'] += 1
                    continue
                acq = acqs[0]
                suffix = directory.split('/'+acq+'/', 1)[1]
                name = row['name'].replace('\\', '/')
                if '/' in name:
                    counts['non_basename_saved_entry'] += 1
                    continue
                relative = str(PurePosixPath(acq)/suffix/name)
                target = inventory.get(relative)
                unique.add(relative)
                if target is None:
                    counts['missing_candidate_file'] += 1
                elif 'bytes' not in row or int(row['bytes']) != target['bytes']:
                    counts['size_mismatch_or_unavailable'] += 1
                elif 'datenum' not in row:
                    counts['saved_timestamp_unavailable'] += 1
                else:
                    counts['name_size_match'] += 1
                    delta = target['mtime_ns']/1e9 - (row['datenum']-719529)*86400
                    hour_offset = round(delta/3600)*3600
                    offsets[hour_offset] += 1
                    if hour_offset in (-7200, -3600, 0, 3600, 7200) and abs(delta-hour_offset) <= 1.1:
                        counts['timestamp_matches_with_fixed_timezone_offset'] += 1
                    else:
                        counts['timestamp_requires_review'] += 1
                        if len(examples) < 10:
                            examples.append({'relative': relative, 'mtime_delta_seconds': delta})
        results.append({'project_id': p['project_id'], 'name': p['name'],
                        'unique_candidate_files': len(unique), 'counts': dict(counts),
                        'rounded_hour_offsets_seconds': dict(offsets), 'examples': examples,
                        'project_saved': False, 'catalog_changed': False,
                        'status': 'metadata_evidence_only_legacy_alias_content_identity_requires_review'})
    output = {'projects': results, 'source_manifest_sha256': '4abdfbfff41264b9bc069c3968ac742ac06fa9b7c52127bbf4369a7d95306fd3',
              'source_deleted': False, 'project_paths_rewritten': False,
              'note': 'Saved file metadata narrows legacy aliases. It is not a content checksum or a source-retirement authorization.'}
    target = work/'project3-raw-listing-metadata-comparison.json'
    assert not target.exists()
    target.write_text(json.dumps(output, indent=2)+'\n')
    print(json.dumps(output))


if __name__ == '__main__':
    main()
