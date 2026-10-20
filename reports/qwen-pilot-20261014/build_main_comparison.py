"""Build private measured tables and a case-by-case report after manual review."""
import collections
import hashlib
import html
import json
import re
import statistics
from pathlib import Path

root = Path(__file__).resolve().parent
read = lambda name: json.loads((root / name).read_text(encoding='utf-8'))
escape = lambda value: html.escape(str(value))
plan = read('model-plan.json')
reviews = read('main-quality-review.json')
conclusions = read('main-conclusions.json')
corpus_hash = hashlib.sha256((root / 'corpus.json').read_bytes()).hexdigest()
assert corpus_hash == 'e3fc6d927b24cbb18bf2b936d7405e4bd7b42eadee0be76c2dbc6ea216e276ce'
arms = []
all_results = {}
for candidate in plan['candidates']:
    label = candidate['model'].split('/')[-1].lower().replace('-awq', '-awq-int4')
    result = read('results-' + label + '.json')
    controller = read('coexistence-controller-' + label + '.json')
    memory = read('coexistence-vram-measurements-' + label + '.json')
    runtime = read('runtime-' + label + '.json')
    review = reviews['arms'][label]
    assert result['corpus_sha256'] == corpus_hash
    assert result['status'] == 'complete_needs_review' and len(result['cases']) == 11
    assert review['reviewed_cases'] == len(review['cases']) == 11
    assert controller['cleanup']['original_hash_restored'] and controller['cleanup']['service_status'] == 'inactive'
    assert controller['admission']['status'] == 'released'
    assert controller['owner'] == memory['owner'] == runtime['watchdog']['owner']
    ready = next(e['time'] for e in runtime['journal_records'] if 'Application startup complete.' in e['message'])
    samples = memory['samples']
    loading = [s['service_mib'] for s in samples if s['time'] <= ready]
    residence = [s['service_mib'] for s in samples if s['time'] > ready and s['service_mib'] > 0]
    latencies = [sum(m['latency_seconds'] for m in c['metrics']) for c in result['cases']]
    token_usage = {key: sum((m.get('usage') or {}).get(key, 0) for c in result['cases'] for m in c['metrics'])
                   for key in ('prompt_tokens', 'completion_tokens', 'total_tokens')}
    calls = collections.Counter(call['name'] for c in result['cases'] for call in c['calls'])
    kernel_lines = [e['message'] for e in runtime['journal_records']
                    if ('Using MarlinLinearKernel' in e['message'] or 'Selected MarlinFP8' in e['message']
                        or 'Weight-only FP8 compression' in e['message'])]
    weights = re.search(r'Model loading took ([0-9.]+) GiB memory', runtime['journal'])
    arm = {'label': label, 'model': candidate['model'], 'revision': candidate['revision'],
           'cases': 11, 'response_status_counts': dict(collections.Counter(c['status'] for c in result['cases'])),
           'quality_summary': review['summary'], 'reviewed_cases': 11,
           'latency_seconds': {'median_conversation_provider': statistics.median(latencies),
                               'min': min(latencies), 'max': max(latencies), 'sum': sum(latencies),
                               'case_values': latencies},
           'tokens': token_usage, 'tool_calls': dict(calls),
           'provider_requests': sum(len(c['metrics']) for c in result['cases']),
           'vram_mib': {'loading_peak_sampled': max(loading),
                        'residence_median_after_ready': statistics.median(residence),
                        'residence_peak_after_ready': max(residence),
                        'service_peak_sampled': max(s['service_mib'] for s in samples),
                        'physical_peak': max(s['total_used_mib'] for s in samples),
                        'physical_minimum_free': 24576 - max(s['total_used_mib'] for s in samples)},
           'weight_loading_memory_gib_runtime': float(weights[1]) if weights else None,
           'kernel_evidence': kernel_lines, 'ready_at_unix': ready,
           'controller_duration_seconds': controller['finished_at'] - controller['started_at'],
           'inference_settings': result['inference_settings'], 'cleanup_verified': True}
    arms.append(arm)
    all_results[label] = result

report = {'corpus_sha256': corpus_hash, 'corpus_cases': 11, 'total_responses_reviewed': 44,
          'method': reviews['method'], 'arms': arms, 'conclusions': conclusions,
          'limits': ['11 cas historiques, une conversation déterministe par cas et par bras ; aucune estimation statistique de supériorité.',
                     'Latence = somme des durées HTTP provider par conversation : préfill, sélection d’outils et génération inclus ; exécution locale des outils, SSH, gardes et démarrage exclus.',
                     'Longueur des réponses et préfixes mis en cache varient. Une durée plus faible ne mesure pas seule la vitesse du noyau.',
                     'Caps vLLM différents selon le plan ; le cache KV automatique et le format changent la VRAM. Ne pas attribuer tout écart aux seuls poids.',
                     'Mesures VRAM par cgroups et nvidia-smi environ toutes les secondes : des transitoires plus courts peuvent échapper aux pics échantillonnés.',
                     'FP8 sur SM86 utilise Marlin avec compression des poids ; aucun calcul FP8 natif. Résultat propre à RTX3090/vLLM0.29.0.',
                     'Aucun test de coexistence scientifique pour ces bras principaux exclusifs. Aucun modèle validé pour lancer ou corriger une analyse sans supervision.']}
(root / 'main-comparison.json').write_text(json.dumps(report, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')

page = '''<!doctype html><html lang="fr"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Qwen 8B / 14B — AWQ / FP8 — DetecDiv Hub</title>
<style>body{font:16px/1.5 system-ui;color:#172b3c;max-width:1500px;margin:36px auto;padding:0 24px}h1,h2{line-height:1.2}table{border-collapse:collapse;width:100%;margin:20px 0}td,th{border:1px solid #cad5df;padding:9px;text-align:left;vertical-align:top}th{background:#e8eef4}pre{white-space:pre-wrap;overflow-wrap:anywhere;background:#f2f5f8;padding:15px}summary{cursor:pointer}code{overflow-wrap:anywhere}.notice{padding:16px;background:#fff2cf;border-left:4px solid #c99313}.wrap{overflow-x:auto}small{color:#52677a}</style>
<h1>Qwen3 pour DetecDiv Hub : 8B / 14B, AWQ 4 bits / FP8</h1>
<p>Campagne principale achevée le 15 octobre 2026. 44 réponses relues, sur les mêmes 11 cas historiques : 3 succès techniques, 5 échecs et 3 suivis.</p>
<p class="notice">Les captures scientifiques sont figées au 14 octobre. Les réponses ci-dessous décrivent ces captures, jamais un état live. La réussite technique ne vaut pas validation biologique.</p>'''
page += '<h2>Recommandation</h2>' + ''.join('<p>' + escape(p) + '</p>' for p in conclusions['recommendation'])
page += '<h2>Qualité, latence, VRAM et marge</h2><div class="wrap"><table><tr><th>Checkpoint</th><th>Qualité relue</th><th>Médiane provider / conversation</th><th>Pic chargement</th><th>Résidence médiane</th><th>Pic service</th><th>Marge physique minimale</th></tr>'
for a in arms:
    v = a['vram_mib']
    values = [a['model'], a['quality_summary'], f"{a['latency_seconds']['median_conversation_provider']:.2f} s"]
    values += [f'{v[k]/1024:.2f} Gio' for k in ('loading_peak_sampled', 'residence_median_after_ready', 'service_peak_sampled', 'physical_minimum_free')]
    page += '<tr>' + ''.join('<td>' + escape(x) + '</td>' for x in values) + '</tr>'
page += '</table></div><p>Pic = maximum échantillonné, pas une garantie absolue. Résidence = échantillons après disponibilité du serveur, requêtes et périodes d’attente incluses. Marge = total physique moins pic total GPU, incluant le fond système.</p>'
page += '<h2>Comparaisons à taille et format égaux</h2>' + ''.join('<p>' + escape(p) + '</p>' for p in conclusions['comparisons'])
page += '<h2>Paramètres et méthode</h2><p>Contexte 8192, concurrence 1, batch 256, plafond de sortie 1100 par requête, température 0, seed requête 42, thinking désactivé, preuves standard identiques, choix automatique d’outils avec Hermes, eager, aucun offload CPU ni cache KV explicite. VLLM_USE_FLASHINFER_SAMPLER=0. Un seul checkpoint en VRAM à la fois. Ordre : 14B AWQ, 14B FP8, 8B AWQ, 8B FP8.</p>'
page += '<p>Captures lues localement ; seuls les modèles hébergés sur detecdiv-server ont reçu les preuves. Aucun envoi de données scientifiques à un modèle externe. Corpus SHA256 : <code>' + corpus_hash + '</code>.</p>'
page += '<table><tr><th>Bras</th><th>Tokens entrée / sortie</th><th>Requêtes provider</th><th>Appels outils réels</th><th>Révision officielle</th></tr>'
for a in arms:
    page += '<tr>' + ''.join('<td>' + escape(x) + '</td>' for x in [a['label'], f"{a['tokens']['prompt_tokens']} / {a['tokens']['completion_tokens']}", a['provider_requests'], json.dumps(a['tool_calls']), a['revision']]) + '</tr>'
page += '</table><h2>Revue des 44 réponses</h2><p>' + escape(reviews['method']) + '</p>'
for a in arms:
    label = a['label']
    page += '<h3>' + escape(a['model']) + '</h3>'
    for c, review in zip(all_results[label]['cases'], reviews['arms'][label]['cases']):
        assert c['job_id'].startswith(review['job_prefix'])
        page += '<details><summary>' + escape(c['job_id'][:8] + ' — ' + review['judgement']) + '</summary><p>' + escape(review['finding']) + '</p><p>État de réponse : ' + escape(c['status']) + '</p><pre>' + escape(c.get('answer', '')) + '</pre><small>Outils : ' + escape(json.dumps(c['calls'], ensure_ascii=False)) + '</small></details>'
page += '<h2>Exploitation et restauration</h2>' + ''.join('<p>' + escape(p) + '</p>' for p in conclusions['operations'])
page += '<h2>Limites</h2><ul>' + ''.join('<li>' + escape(p) + '</li>' for p in report['limits']) + '</ul>'
page += '<p>Les bras supplémentaires 4B et 8B avec offload ont déjà été rapportés séparément. Ils ne sont pas relancés et ne constituent pas la comparaison principale, car contexte, preuves et plafond de sortie diffèrent.</p></html>'
(root / 'main-comparison.html').write_text(page, encoding='utf-8')
print(json.dumps({'responses_reviewed': 44, 'arms': [{'label': a['label'], 'latency_median_s': a['latency_seconds']['median_conversation_provider'], 'vram': a['vram_mib'], 'tools': a['tool_calls']} for a in arms]}, ensure_ascii=True))
