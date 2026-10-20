import hashlib
import html
import json
import statistics
from datetime import UTC, datetime
from pathlib import Path

from scripts.evaluate_qwen_runs import compact_evidence, write_json

ROOT = Path(__file__).resolve().parent
LABEL4 = 'qwen3-4b-awq-ctx4096-kv640m-paired'
LABEL8 = 'qwen3-8b-awq-ctx4096-kv640m-offload2p5-copy'
EXPECTED_HASH = 'e3fc6d927b24cbb18bf2b936d7405e4bd7b42eadee0be76c2dbc6ea216e276ce'

def read(name):
    return json.loads((ROOT / name).read_text(encoding='utf-8'))

corpus = read('corpus.json')
assert hashlib.sha256((ROOT / 'corpus.json').read_bytes()).hexdigest() == EXPECTED_HASH
results = {label: read(f'results-{label}.json') for label in (LABEL4, LABEL8)}
for value in results.values():
    assert value['corpus_sha256'] == EXPECTED_HASH
    assert value['evidence_profile'] == 'compact4096' and value['max_new_tokens'] == 512
by_model = {label: {c['job_id']: c for c in value['cases']} for label, value in results.items()}
common = set(by_model[LABEL4]) & set(by_model[LABEL8])

# Relecture de chaque réponse contre les champs de preuve, sans score lexical.
notes = {
 '224d2': ('Réussite 2/2; error_text vide; dependency_audit est un affichage de struct.',
           'Réussite reconnue, mais invente une erreur et un audit incomplet à partir de dependency_audit.',
           'Réussite technique reconnue; réserve la validation biologique. Plus fidèle.'),
 'bf584': ('Réussite 2/2; aucune mesure de qualité des sorties dans les captures.',
           'Affirme résultats valides et aucune vérification supplémentaire nécessaire : non étayé.',
           'Demande de vérifier sorties et canaux; évite cette validation injustifiée.'),
 '941c': ('Statut done, mais doneNodes=0 / totalNodes=1, deux nœuds seulement sélectionnés.',
           'Cite les compteurs 0/1 et demande confirmation; invente un message d’erreur non spécifique.',
           'Affirme que les deux nœuds ont exécuté : contradiction avec les compteurs, malgré les réserves finales.'),
 '909a': ('error_text : OOM RAM 24576 MiB, swap 1536 MiB; gpu_required=false.',
           'Bonne famille RAM et bonnes actions budget/pool/batch; formulation OOM maladroite.',
           'Bonne identification RAM/swap et corrections cohérentes. Pas de confusion VRAM.'),
 '7aa': ('LegacyFrameSelectionLimit : allocation par nombre de frames, indexation absolue; seul préfixe contigu 1:N sûr.',
          'Réduit le problème à une taille de ROI trop petite; perd la contrainte du préfixe.',
          'Identifie le démarrage à frame 1, mais omet la contiguïté et le mécanisme allocation/indexation. Amélioration partielle.'),
 'f9d': ('Sortie computeDrift DONE frames=12; pas de fraction globale, ni de rythme global établi.',
          'Cite 12 frames et refuse ETA; absence de comparaison temporelle. Mention de reprise peu utile.',
          'Refuse ETA mais ignore la sortie computeDrift et dit aucun indicateur fourni. Prudence, moins informatif.'),
 'e2a': ('Événement scope=parallel_fov, extraction ROI 86,3%, 6/9 FOV; ce n’est pas tout le pipeline.',
          'Pourcentage peu cadré; invente une possibilité de 1–2 heures sans vitesse ni historique.',
          'Cadre le pourcentage à l’extraction et refuse ETA. Dit historique pourrait aider, sans l’appeler.'),
 '5cb': ('Segmenting frame 47/100, localValue=0.46, value=0.245; heartbeat n’établit pas vitesse/progrès.',
          'Heartbeat présenté comme preuve de progrès; invente 1 minute/frame et ETA 53 minutes.',
          'Refuse ETA et distingue valeur locale; la portée de 24,5% reste insuffisamment expliquée. Historique non appelé.'),
 'd72': ('error_text : classifier_cnn_lstm_1, FOV8 ROI1, MATLAB:table:RowDimensionMismatch; statut capture failed.',
          'Bonne erreur et localisation; présente orphaned/annulé comme état courant malgré failed dans la capture.',
          'Bonne erreur et localisation; vérification générique des dimensions. Aucun des deux ne prouve la cause racine.'),
 'de164': ('Index in position 4 : quatrième sous-indice/dimension, maximum 13, valeur fautive non connue.',
            'Confond position 4 avec valeur d’index 4 et étape 4; propose réinitialisation sans preuve.',
            'Évite cette erreur mais reste générique; n’explique pas la quatrième dimension ni une réparation précise.'),
 '393f': ('cellpose_sam_segmentatio : no input channel configured. 0 TL _z2 est le canal de roipattern, pas une exigence Cellpose prouvée.',
           'Identifie absence de configuration et bon nœud; demande de configurer un canal sans imposer une identité non prouvée. Plus fidèle que le 8B.',
           'Bonne famille et nœud; impose 0 TL _z2 et vérification de chemin sans établir leur nécessité.'),
}

reviews = []
for jid in corpus['cases']:
    prefix = next(p for p in notes if jid.startswith(p))
    truth, four, eight = notes[prefix]
    reviews.append({'job_id': jid, 'paired': jid in common, 'evidence_basis': truth,
                    '4b_review': four, '8b_review': eight,
                    'evidence': compact_evidence(corpus, 'get_job_evidence', jid),
                    '4b_trial': by_model[LABEL4].get(jid), '8b_trial': by_model[LABEL8].get(jid)})

def metrics(label):
    trials = list(by_model[label].values())
    names = [f'coexistence-vram-measurements-{label}-wave{i}.json' for i in (1, 2)]
    waves = [read(name) for name in names]
    samples = [s for wave in waves for s in wave['samples']]
    durations = [sum(m['latency_seconds'] for m in c['metrics']) for c in trials]
    common_durations = [sum(m['latency_seconds'] for m in c['metrics']) for c in trials if c['job_id'] in common]
    return {'cases_completed': len(trials), 'corpus_cases': len(corpus['cases']),
            'median_inference_seconds': statistics.median(durations),
            'median_common_cases_seconds': statistics.median(common_durations),
            'peak_service_mib': max(w['peak_service_mib'] for w in waves),
            'median_nonzero_service_mib': statistics.median(s['service_mib'] for s in samples if s['service_mib']),
            'peak_total_gpu_mib': max(s['total_used_mib'] for s in samples),
            'minimum_physical_free_mib': 24576 - max(s['total_used_mib'] for s in samples),
            'headroom_after_two_scientific_reservations_and_service_peak_mib': 24576 - 16384 - max(w['peak_service_mib'] for w in waves),
            'evidence_tool_calls': sum(sum(c['name'] == 'get_job_evidence' for c in t['calls']) for t in trials),
            'progress_history_calls': sum(sum(c['name'] == 'get_progress_history' for c in t['calls']) for t in trials),
            'prompt_tokens_total': sum(m['usage']['prompt_tokens'] for c in trials for m in c['metrics']),
            'completion_tokens_total': sum(m['usage']['completion_tokens'] for c in trials for m in c['metrics']),
            'all_responses_stop': all(c['finish_reason'] == 'stop' for c in trials),
            'cleanup_verified': all(w['service_status'] == 'inactive' and w['original_hash_restored'] for w in waves),
            'measurement_files': names}

report = {
 'generated_at': datetime.now(UTC).isoformat(), 'corpus_sha256': EXPECTED_HASH,
 'scope': 'Captures historiques figées; outils de lecture uniquement. Relecture qualitative par Codex contre les preuves, pas validation biologique humaine.',
 'status': 'Deux bras terminés; comparaison sur 11 cas communs après reprise du seul cas 4B préempté',
 'shared_conditions': {'context_tokens': 4096, 'kv_cache_mib': 640, 'kv_cache_dtype': 'default float16',
                       'concurrency': 1, 'thinking': False, 'temperature': 0, 'seed': 42,
                       'max_new_tokens': 512, 'evidence_profile': 'compact4096',
                       'response_instruction': '160 mots maximum', 'batch_tokens': 256,
                       'gpu_memory_utilization': 0.23, 'allocator_gc': 0.15},
 '4b_conditions': {'model': 'Qwen/Qwen3-4B-AWQ', 'revision': '74d4bd2bd4bff9cafc9345221320bffb08b406a3', 'cpu_offload_gib': 0},
 '8b_conditions': {'model': 'Qwen/Qwen3-8B-AWQ', 'revision': '4da05a8edb55c6046cce958586c33b61da07bb79', 'cpu_offload_gib': 2.5, 'uva_disabled': True},
 'metrics': {label: metrics(label) for label in results}, 'common_cases': len(common),
 'campaign_code_commit': '307b36a', 'postflight_file': 'reduced-postflight.json',
 'reviews': reviews,
 'recommendation': '8B plus prudent sur ETA et certaines erreurs, mais avantage non systématique : le 4B est plus fidèle sur les compteurs de réussite partielle et le canal manquant. 8B utile pour diagnostic supervisé, trop lent ici pour un chat fluide, pas prêt au lancement autonome. 4B réduit plus rapide mais hallucine plusieurs conclusions. Pour le suivi, appeler l’historique depuis le Hub de manière déterministe plutôt que laisser ce choix au modèle.',
 'limits': ['Petit corpus, un essai par cas; aucune conclusion générale de précision.',
            'Les deux modèles sont AWQ 4 bits; ce bras ne mesure pas 4 contre 8 bits.',
            'L’offload CPU du 8B confond comparaison de vitesse et taille; sa RAM et son coût CPU ne sont pas instrumentés ici.',
            'Contexte, cache et evidence réduits diffèrent du 4B initial 8192; seules les paires compactes isolent ces différences.',
            'Pics VRAM échantillonnés environ chaque seconde, pas une garantie d’absence de transitoires plus courts.',
            'Charges scientifiques variables; aucune mesure contrôlée d’impact sur leur vitesse. Aucun arrêt ni modification de ces analyses.',
            'Nouveau job 19f1b395-a3d1-466c-bf4d-2539a570c802 : préemption, restauration, puis admission released. Ce job a quitté les états actifs avant la reprise du seul cas restant.'],
 'startup_variants_not_diagnostic_trials': [
   {'offload_gib': 1.5, 'uva': True, 'peak_mib': 6212},
   {'offload_gib': 2.5, 'uva': True, 'peak_mib': 7318},
   {'offload_gib': 2.5, 'uva': True, 'allocator_gc': 0.15, 'peak_mib': 6910}],
 'original_4b_8192_reference': {'cases': 11, 'peak_service_mib': 5810, 'median_inference_seconds': 20.928,
                               'file': 'results-qwen3-4b-awq-coexistence.json', 'not_paired_with_reduced_8b': True},
}
write_json(ROOT / 'reduced-4b-8b-comparison.json', report)
esc = html.escape
rows = ''
for label, m in report['metrics'].items():
    rows += '<tr>' + ''.join(f'<td>{esc(str(x))}</td>' for x in (
        '4B AWQ réduit' if label == LABEL4 else '8B AWQ réduit + CPU',
        f"{m['cases_completed']}/11", f"{m['peak_service_mib']/1024:.2f} Gio",
        f"{m['median_nonzero_service_mib']/1024:.2f} Gio", f"{m['median_common_cases_seconds']:.2f} s",
        f"{m['headroom_after_two_scientific_reservations_and_service_peak_mib']/1024:.2f} Gio",
        m['progress_history_calls'])) + '</tr>'
details = ''
for r in reviews:
    answers = ''
    for label, field in ((LABEL4, '4b'), (LABEL8, '8b')):
        trial = r[field + '_trial']
        answers += f'<h3>{field.upper()}</h3><p>{esc(r[field+"_review"])}</p><pre>{esc(trial["answer"] if trial else "Non achevé : préemption scientifique")}</pre>'
    details += f'<details><summary>{esc(r["job_id"])} — {"paire" if r["paired"] else "8B seul"}</summary><p><b>Preuve :</b> {esc(r["evidence_basis"])}</p>{answers}<details><summary>Preuve fournie aux modèles</summary><pre>{esc(json.dumps(r["evidence"],ensure_ascii=False,indent=2))}</pre></details></details>'
document = f'''<!doctype html><html lang="fr"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Qwen 4B et 8B — VRAM réduite</title>
<style>body{{font:16px/1.55 system-ui,sans-serif;max-width:1100px;margin:32px auto;padding:0 20px;color:#17243a;background:#f7f9fc}}h1{{font-size:28px}}table{{border-collapse:collapse;width:100%;background:white}}td,th{{padding:12px;border-bottom:1px solid #d9e0ea;text-align:left}}details{{background:white;padding:14px;margin:12px 0;border:1px solid #d9e0ea;border-radius:8px}}summary{{cursor:pointer;font-weight:600}}pre{{white-space:pre-wrap;overflow-wrap:anywhere;background:#edf2f8;padding:14px;font-size:14px}}.scroll{{overflow:auto}}</style>
<h1>Qwen 4B et 8B avec VRAM réduite</h1><p>{esc(report['status'])}.</p><p>{esc(report['scope'])}</p>
<p>Contexte 4096 tokens · cache KV fixe 640 MiB · concurrence 1 · sorties limitées à 512 tokens · même vue compacte des preuves. Le 8B utilise en plus 2,5 Gio d’offload CPU avec copie des poids.</p>
<div class="scroll"><table><thead><tr><th>Modèle</th><th>Cas</th><th>Pic propre VRAM</th><th>Médiane résidence*</th><th>Inférence médiane, {len(common)} paires</th><th>Marge réservations**</th><th>Appels historique</th></tr></thead><tbody>{rows}</tbody></table></div>
<p>* Échantillons non nuls, démarrage inclus. ** 24 Gio moins deux réservations scientifiques de 8 Gio et le pic propre Qwen; ≥2 Gio requis. Après arrivée du troisième job, Qwen a été arrêté. Durées = somme des requêtes d’inférence par conversation, hors SSH et contrôles; le placement CPU ralentit le 8B.</p>
<p><b>Recommandation :</b> {esc(report['recommendation'])}</p><p>Le 4B initial, contexte 8192, avait 11 cas, pic 5,67 Gio et médiane 20,93 s; ce résultat reste une référence distincte.</p>
<h2>Limites et protection des analyses</h2><ul>{''.join('<li>'+esc(s)+'</li>' for s in report['limits'])}</ul>
<p>Chaque vague a vérifié service inactif et hash du lanceur original restauré. Admission libérée. Le contrôle final confirme aucun processus d’inférence actif et les deux runs scientifiques toujours running avec heartbeats récents. Le locker API est terminé (PID48 zombie sans mémoire ni verrou, en attente de collecte par PID1); aucun redémarrage API pour le retirer. Les variantes UVA ont dépassé le plafond à l’initialisation (6212 / 7318 / 6910 MiB) et ont été arrêtées sans diagnostic : elles ne comptent pas comme réponses échouées.</p>
<h2>Relecture des diagnostics</h2>{details}<h2>Fichiers détaillés</h2><ul><li><a href="reduced-4b-8b-comparison.json">Mesures, conditions et relecture JSON</a></li><li><a href="reduced-postflight.json">Contrôle final de restauration et des jobs</a></li><li><a href="results-{LABEL4}.json">Traces 4B réduit</a></li><li><a href="results-{LABEL8}.json">Traces 8B réduit</a></li><li><a href="report.html">Campagne initiale</a></li></ul><p>Code committé en 307b36a; 14 tests passent, lint propre. Corpus SHA256 : <code>{EXPECTED_HASH}</code>. Généré {esc(report['generated_at'])}.</p></html>'''
(ROOT / 'comparison-4b-8b-reduced.html').write_text(document,encoding='utf-8')
print(json.dumps(report['metrics'],ensure_ascii=True))
