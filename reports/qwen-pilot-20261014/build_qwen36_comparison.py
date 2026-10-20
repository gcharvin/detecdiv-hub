"""Build a private paired report only after all eleven new outputs are reviewed."""
import collections
import hashlib
import html
import json
import statistics
from pathlib import Path

root = Path(__file__).resolve().parent
read = lambda name: json.loads((root / name).read_text(encoding='utf-8'))
escape = lambda value: html.escape(str(value))
label = 'qwen36-27b-awq-int4-g128'
result = read('results-' + label + '.json')
baseline = read('results-qwen3-14b-awq-int4.json')
review = read('qwen36-quality-review.json')
baseline_review = read('main-quality-review.json')['arms']['qwen3-14b-awq-int4']
conclusions = read('qwen36-conclusions.json')
assert result['status'] == 'complete_needs_review'
assert len(result['cases']) == len(review['cases']) == len(baseline['cases']) == 11
assert hashlib.sha256((root / 'corpus.json').read_bytes()).hexdigest() == result['corpus_sha256'] == baseline['corpus_sha256']
assert [c['job_id'] for c in result['cases']] == [c['job_id'] for c in baseline['cases']]
for c, r in zip(result['cases'], review['cases']):
    assert c['job_id'].startswith(r['job_prefix'])
controllers = {}
measurements = {}
runtimes = {}
for path in root.glob('coexistence-controller-' + label + '*.json'):
    item = read(path.name)
    controllers[item['owner']] = item
for path in root.glob('coexistence-vram-measurements-' + label + '*.json'):
    item = read(path.name)
    measurements[item['owner']] = item
for path in root.glob('runtime-' + label + '*.json'):
    item = read(path.name)
    runtimes[item['watchdog']['owner']] = item
for owner, c in controllers.items():
    assert c['cleanup']['original_hash_restored'] and c['cleanup']['service_status'] == 'inactive'
    assert c['admission']['status'] == 'released'
    assert owner in measurements and owner in runtimes
loading, residence, all_samples = [], [], []
for owner, memory in measurements.items():
    samples = memory['samples']
    all_samples.extend(samples)
    ready = next((e['time'] for e in runtimes[owner]['journal_records'] if 'Application startup complete.' in e['message']), None)
    loading.extend(s['service_mib'] for s in samples if ready is None or s['time'] <= ready)
    if ready is not None:
        residence.extend(s['service_mib'] for s in samples if s['time'] > ready and s['service_mib'] > 0)
latencies = [sum(m['latency_seconds'] for m in c['metrics']) for c in result['cases']]
usage = {k: sum((m.get('usage') or {}).get(k, 0) for c in result['cases'] for m in c['metrics']) for k in ('prompt_tokens', 'completion_tokens', 'total_tokens')}
calls = collections.Counter(call['name'] for c in result['cases'] for call in c['calls'])
new = {'model': result['model'], 'label': label, 'revision': result['inference_settings']['revision'],
       'cases': 11, 'response_status_counts': dict(collections.Counter(c['status'] for c in result['cases'])),
       'latency_seconds': {'median': statistics.median(latencies), 'min': min(latencies), 'max': max(latencies), 'sum': sum(latencies), 'case_values': latencies},
       'vram_mib': {'loading_peak_sampled': max(loading), 'residence_median_after_ready': statistics.median(residence),
                    'service_peak_sampled': max(s['service_mib'] for s in all_samples),
                    'physical_minimum_free': 24576 - max(s['total_used_mib'] for s in all_samples)},
       'tokens': usage, 'provider_requests': sum(len(c['metrics']) for c in result['cases']),
       'tool_calls': dict(calls), 'inference_settings': result['inference_settings'],
       'waves': [{'owner': c['owner'], 'error': c.get('error'), 'benchmark_exit': c.get('benchmark_exit'), 'duration_seconds': c['finished_at']-c['started_at']} for c in controllers.values()]}
old = next(a for a in read('main-comparison.json')['arms'] if a['label'] == 'qwen3-14b-awq-int4')
record = {'status': 'complete_reviewed', 'corpus_sha256': result['corpus_sha256'], 'responses_reviewed': 22,
          'baseline': old, 'qwen36': new, 'review': review, 'conclusions': conclusions}
(root/'qwen36-vs-14b-comparison.json').write_text(json.dumps(record, ensure_ascii=False, indent=2)+'\n', encoding='utf-8')
page = '''<!doctype html><html lang="fr"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Qwen3.6 27B / Qwen3 14B — DetecDiv Hub</title><style>body{font:16px/1.55 system-ui;color:#173049;max-width:1300px;margin:36px auto;padding:0 24px}table{border-collapse:collapse;width:100%;margin:24px 0}th,td{padding:12px;border:1px solid #bdcad7;text-align:left;vertical-align:top}th{background:#e6eef6}pre{white-space:pre-wrap;overflow-wrap:anywhere;padding:18px;background:#f2f5f8}summary{cursor:pointer}.notice{padding:18px;background:#fff2d5}.wrap{overflow-x:auto}</style><h1>Qwen3.6-27B AWQ 4 bits face au Qwen3-14B AWQ</h1><p class="notice">Les 11 mêmes cas historiques ont été évalués et les 22 sorties relues. Captures du 14 octobre 2026 : aucun état scientifique live n’est déduit de ces réponses.</p>'''
for paragraph in conclusions['summary']:
    page += '<p>' + escape(paragraph) + '</p>'
page += '<h2>Mesures</h2><div class="wrap"><table><tr><th>Modèle</th><th>Médiane provider</th><th>Pic chargement</th><th>Résidence médiane</th><th>Pic service</th><th>Marge physique minimale</th></tr>'
for name, latency, memory in [('Qwen3-14B-AWQ', old['latency_seconds']['median_conversation_provider'], old['vram_mib']), ('Qwen3.6-27B AWQ communautaire', new['latency_seconds']['median'], new['vram_mib'])]:
    values = [name, f'{latency:.2f} s'] + [f'{memory[k]/1024:.2f} Gio' for k in ('loading_peak_sampled', 'residence_median_after_ready', 'service_peak_sampled', 'physical_minimum_free')]
    page += '<tr>'+''.join('<td>'+escape(v)+'</td>' for v in values)+'</tr>'
page += '</table></div><h2>Conditions, limites et exploitation</h2>'
for paragraph in conclusions['method'] + conclusions['operations']:
    page += '<p>' + escape(paragraph) + '</p>'
page += '<h2>Tokens, outils et états des réponses</h2><pre>'+escape(json.dumps({'baseline':{k:old[k] for k in ('tokens','provider_requests','tool_calls','response_status_counts')},'qwen36':{k:new[k] for k in ('tokens','provider_requests','tool_calls','response_status_counts')}},ensure_ascii=False,indent=2))+'</pre>'
page += '<h2>Revue des 11 paires</h2>'
for i, (a, b, ar, br) in enumerate(zip(baseline['cases'], result['cases'], baseline_review['cases'], review['cases']), 1):
    page += '<h3>Cas '+str(i)+'</h3>'
    for name, c, r in [('14B AWQ', a, ar), ('27B AWQ', b, br)]:
        page += '<details><summary>'+escape(name+' — '+r['judgement'])+'</summary><p>'+escape(r['finding'])+'</p><p>'+escape(c['status'])+'</p><pre>'+escape(c.get('answer',''))+'</pre><small>'+escape(json.dumps(c['calls'],ensure_ascii=False))+'</small></details>'
page += '</html>'
(root/'qwen36-vs-14b-comparison.html').write_text(page,encoding='utf-8')
print(json.dumps({'qwen36':new,'baseline_median_s':old['latency_seconds']['median_conversation_provider']},ensure_ascii=True))
