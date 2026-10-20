"""Render offline study evidence; reads local JSON only, no production writes."""
from pathlib import Path
import json
import statistics
from html import escape

ROOT = Path(__file__).resolve().parent
def read(name):
    return json.loads((ROOT / name).read_text(encoding='utf-8-sig'))

before, after = read('live-before.json'), read('live-after.json')
bjobs = {j['id']: j for j in before['jobs']}
ajobs = {j['id']: j for j in after['jobs']}
checks = {
    'before': before['checked_at'], 'after': after['checked_at'],
    'both_database_transactions_read_only': before['read_only'] and after['read_only'],
    'target_settings_unchanged': before['targets'] == after['targets'],
    'resource_and_priority_settings_unchanged': before['settings'] == after['settings'],
    'same_active_and_queued_job_ids': set(bjobs) == set(ajobs),
    'jobs': [{
        'id': jid, 'status_before': j['status'], 'status_after': ajobs.get(jid, {}).get('status'),
        'allocation_unchanged': j['allocation'] == ajobs.get(jid, {}).get('allocation'),
        'code_and_runtime_unchanged': all(j[k] == ajobs.get(jid, {}).get(k)
                                         for k in ['code_commit', 'runtime'])
    } for jid, j in bjobs.items()],
    'actions': 'No submission, cancellation, deployment, service restart, quota change or scientific data write. Synthetic tests ran on the local workstation.'
}
assert checks['both_database_transactions_read_only']
assert checks['target_settings_unchanged'] and checks['resource_and_priority_settings_unchanged']
assert checks['same_active_and_queued_job_ids']
assert all(j['allocation_unchanged'] and j['code_and_runtime_unchanged'] for j in checks['jobs'])
(ROOT / 'non_interference_check.json').write_text(json.dumps(checks, indent=2), encoding='utf-8')

sim, storage = read('simulation_results.json'), read('storage_results.json')
policies = {'fixed24': '24 Gio fixes', 'preferred48': '48 Gio fixes',
            'greedy_max': 'Maximum libre fixe', 'elastic_fifo': 'Élastique FIFO',
            'elastic_fair': 'Élastique avec équité'}
names = {'one_job': 'Un job seul', 'late_arrivals': 'Un long job, deux arrivées tardives',
         'memory_flat': 'Même charge, sans accélération avec davantage de RAM',
         'legacy_no_checkpoint': 'Ancien code sans point de reprise coopératif',
         'many_jobs_one_user': 'Neuf jobs du même utilisateur, un autre utilisateur',
         'mixed': '18 jobs hétérogènes, trois utilisateurs',
         'large_minimum': 'Un job exige réellement 64 Gio'}
def table(headers, rows):
    return '<table><thead><tr>' + ''.join('<th>'+escape(str(h))+'</th>' for h in headers) + '</tr></thead><tbody>' + ''.join('<tr>'+''.join('<td>'+escape(str(v))+'</td>' for v in row)+'</tr>' for row in rows) + '</tbody></table>'

scenario_tables = ''
for workload, title in names.items():
    rows = []
    for policy, label in policies.items():
        selected = [r for r in sim['results'] if r['workload'] == workload
                    and r['policy'] == policy and r['capacity']['memory'] == 78
                    and r['storage_model'] == 'no_shared_io_ceiling']
        rows.append([label] + [f'{statistics.mean(r[k] for r in selected):.1f}'
                               for k in ['makespan', 'mean_response', 'max_wait']])
    scenario_tables += '<h3>'+title+'</h3>'+table(['Politique', 'Fin de toute la charge', 'Délai moyen arrivée → fin', 'Attente initiale maximale'], rows)

storage_rows = [[r['format'], r['framesPerBlock'], r['blocks'],
                 f"{(r['logicalImageBytesRead']+r['logicalImageBytesWritten'])/2**20:.2f}",
                 f"{r['seconds']:.3f}", 'Oui' if r['pixelsExact'] else 'NON']
                for r in storage['rows']]
conditional = sim['conditional_release_comparison']
conditional_table = table(['Libération au point de reprise', 'Durée virtuelle du job seul'],
    [['Systématique', conditional['mandatory_release']['makespan']],
     ['Seulement si une demande concurrente ne tient pas', conditional['only_when_contended']['makespan']]])
sensitivity_table = table(['Intervalle entre points sûrs', 'Coût de reprise', 'Fin de toute la charge', 'Délai moyen'],
    [[r['quantum'], r['checkpoint_cost'], r['makespan'], round(r['mean_response'], 2)] for r in sim['sensitivity']])

html = '''<!doctype html><html lang="fr"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>DetecDiv — étude des allocations de ressources</title>
<style>body{font:17px/1.55 system-ui,sans-serif;max-width:1080px;margin:40px auto;padding:0 24px;color:#183047;background:#fafcfe}h1{line-height:1.15}h2{margin-top:2.5em}h3{margin-top:1.8em}table{border-collapse:collapse;width:100%;font-size:14px;background:white;margin:20px 0}td,th{border-bottom:1px solid #dce4eb;padding:10px;text-align:right}th:first-child,td:first-child{text-align:left}th{background:#eaf1f7}.callout{border-left:5px solid #19717d;background:#e9f6f5;padding:16px 22px}a{color:#075b9a}code{font-size:.9em}small{color:#52667a}</style>
<h1>Prêter la mémoire libre sans bloquer les prochains utilisateurs</h1>
<p>Étude locale du 15 octobre 2026 · DetecDiv Hub · simulations, tests unitaires et essai MATLAB synthétique.</p>
<div class="callout"><strong>Recommandation :</strong> garder une allocation minimale garantie, accorder un surplus seulement s’il accélère le module, et récupérer ce surplus à un point de reprise coopératif lorsqu’une autre demande en a besoin. Augmenter uniformément les jobs à 48 ou 96 Gio peut dégrader le débit global. Le mécanisme de récupération n’existe pas encore dans le Hub.</div>
<p>Aucune limite, allocation, sélection scientifique, release, donnée de projet ou configuration de worker n’a été modifiée. Les tests ont tourné sur le poste local. Les observations du Hub proviennent de transactions SQL explicitement en lecture seule. Les trois jobs actifs, leurs allocations et leurs répertoires de code sont identiques avant et après l’étude ; paramètres des cibles et priorités inchangés. Voir <a href="non_interference_check.json">preuve avant/après</a>.</p>

<h2>Ce que le Hub fait déjà</h2>
<p>L’admission actuelle tient compte des CPU, de la RAM, de la réservation GPU et d’unités relatives d’accès disque. Une demande explicite de 48 Gio est possible : 24 Gio est le profil par défaut, pas une limite absolue du logiciel. L’allocation reste fixe pendant toute la tentative du job. Le scheduler protège aussi les ressources nécessaires à une demande prioritaire bloquée.</p>
<p>Sur Linux, le pool de calcul est limité à 96 Gio, 36 CPU, 24 Gio de VRAM réservables et 12 unités disque. La politique actuelle soustrait 512 Mio par worker potentiel : avec un plafond d’autoscaling de 36, elle conserve 18 Gio et admet effectivement 78 Gio de jobs. Six pollers sont souhaités actuellement. Cette réserve prudente mérite un examen séparé avec l’autoscaler ; la réduire sans coordination pourrait dépasser le budget.</p>
<p>Au moment des observations, trois jobs réservent chacun 24 Gio, 8 CPU et 8 Gio de GPU : 72 Gio de RAM et toute la VRAM réservable sont engagés. La consommation physique est inférieure aux réservations, mais cela ne garantit pas l’absence de pics simultanés. Donner plus de RAM ne crée aucune capacité GPU supplémentaire.</p>
<p>Windows est actuellement configuré pour un seul job concurrent, sans la même gestion dynamique que Linux. Une future mise en œuvre doit y définir une protection équivalente et des mesures propres, plutôt que supposer que les limites cgroup Linux s’y appliquent.</p>

<h2>Expériences de planification</h2>
<p>1 680 simulations hors ligne : sept charges, douze graines, cinq politiques, deux budgets RAM (96 et 78 Gio), avec ou sans plafond d’accès partagé au stockage. S’ajoutent neuf essais de sensibilité et une comparaison de reprise conditionnelle. Les chiffres ci-dessous sont des <strong>minutes virtuelles</strong>, jamais une estimation de la durée des vrais jobs.</p>
<p>Hypothèses : demande usuelle minimale 24 Gio, préférée 48 Gio, maximale 96 Gio ; 8 CPU, 8 Gio GPU et 3 unités disque par job. Une formule simplifiée issue du budget de lecture d’Alex donne un lecteur à 24 Gio et huit à 48 Gio. Elle sature ensuite à huit CPU : 96 Gio n’apporte ici aucun gain. Le temps est modélisé par une partie séquentielle et une partie parallélisable ; le gain réel dépendra du NAS, des caches et des modèles.</p>
<p>Les politiques élastiques simulées récupèrent toutes les ressources d’un job uniquement après un point de reprise explicite qui préserve le travail et accuse réception de la libération. Elles n’écrasent pas une limite mémoire en cours de calcul. Le code non coopératif conserve son allocation fixe jusqu’à sa fin. L’équité est inspirée de la part dominante cumulée par utilisateur ; ce prototype discret n’est pas une implémentation formellement équivalente à DRF.</p>
<p><strong>Lecture des résultats :</strong> sur la charge hétérogène à 78 Gio, 48 Gio fixes augmentent le délai moyen d’environ 65 % face à 24 Gio. L’élasticité avec équité réduit ici la durée globale et l’attente initiale maximale, mais augmente le délai moyen. Il faut donc choisir et mesurer le compromis entre débit, attente, temps total et équité ; aucune politique n’est gagnante partout.</p>
<p>Tableaux à 78 Gio, sans plafond supplémentaire de stockage ; valeurs moyennées sur les douze graines. Les charges déterministes sont répétées à l’identique.</p>
''' + scenario_tables + '''

<h3>Ne pas payer une reprise sans besoin</h3>
<p>Avec un point sûr toutes les deux minutes virtuelles et un coût de reprise de deux minutes, forcer la libération à chaque frontière pénalise même un job seul. Conserver le pool et les modèles tant qu’il n’existe pas de demande concurrente évite ce coût dans le modèle. Une frontière n’implique donc pas de redémarrer MATLAB systématiquement.</p>
''' + conditional_table + '''
<p>Sensibilité sur le scénario des arrivées tardives à 96 Gio. Un point sûr trop rare retarde la restitution ; un point trop fréquent peut coûter cher lorsqu’il déclenche réellement une reprise.</p>
''' + sensitivity_table + '''

<h2>Taille des blocs et stockage : essai MATLAB local</h2>
<p>Deux ROI synthétiques de 16 × 16 × 6 × 851 pixels uint16, sans compression dans les deux formats, MATLAB R2026a Update 2 limité à un thread. Comparaison d’un MAT complet relu puis réécrit à chaque bloc avec des écritures partielles HDF5 par canal. Les images finales sont strictement identiques dans les six essais. Les fichiers temporaires propres à cet essai ont été nettoyés.</p>
''' + table(['Format', 'Frames/bloc', 'Blocs', 'Volume logique images lu + écrit (Mio)', 'Temps local (s)', 'Pixels identiques'], storage_rows) + '''
<p>Le passage de 88 à 44 frames double presque le volume logique réécrit en MAT. Les écritures partielles HDF5 gardent ici le même volume utile. <strong>Pourtant HDF5 est plus lent sur ces petits fichiers locaux en cache</strong> : le coût des nombreux petits appels et des métadonnées domine. Ce test démontre une réduction des réécritures, pas une accélération sur le NAS. Il utilise directement h5create/h5write ; le comportement réel de roi.save est couvert séparément par deux tests existants, sans modification.</p>
<p>À titre de calcul de volume uniquement, pour 51 ROI de 171 × 171 × 6 × 851 pixels : environ 269,5 Gio d’images lues et réécrites à blocs de 88, contre 553,1 Gio à blocs de 44 avec le mécanisme MAT complet. Le volume utile des images écrites une fois vaut environ 14,2 Gio. Ces valeurs excluent compression, métadonnées, cache système et trafic réellement mesuré ; elles ne prédisent pas la durée.</p>

<h2>Évolution proposée, par étapes</h2>
<ol>
<li><strong>Mesurer par module.</strong> Déclarer un minimum, une allocation utile et un maximum, ainsi que les points de reprise supportés. Enregistrer pics RAM/VRAM, attente, CPU, débits disque, durée des blocs et coût du pool. Une demande réellement supérieure à 24 Gio conserve son minimum légitime.</li>
<li><strong>Prêter seulement le surplus utile.</strong> Accorder davantage au démarrage lorsqu’il reste de la capacité après les minimums et réserves. Utiliser le gain mesuré par Gio, plafonner les lecteurs par les CPU et le débit stockage, et maintenir l’équité par utilisateur sur l’ensemble de ses jobs. Ajouter une priorité liée à l’attente pour prévenir la famine.</li>
<li><strong>Rendre la reprise fiable avant l’élasticité.</strong> Écrire une progression durable et idempotente par ROI ou bloc ; préférer les écritures HDF5 partielles déjà prévues par le backend. Le script historique d’Alex ne devient pas récupérable au milieu d’un bloc par simple changement de quota. Une reprise au niveau FOV/ROI peut être une première étape plus simple, avec une latence de restitution plus longue.</li>
<li><strong>Négocier les diminutions.</strong> Le Hub demande un nouveau budget avec un identifiant de génération ; le module termine son unité sûre, sauvegarde, réduit/libère buffers et pool, puis confirme. Le Hub vérifie la libération effective avant d’admettre un nouveau job et d’appliquer une limite inférieure. Absence de réponse, acquittement périmé ou mémoire encore occupée : aucune capacité n’est considérée libérée. Un redémarrage contrôlé depuis checkpoint reste le repli pour les bibliothèques qui conservent leurs allocations.</li>
<li><strong>Distinguer les étapes CPU et GPU.</strong> La lecture n’a pas le même besoin que l’inférence. Une réservation par étape pourrait éviter de monopoliser la VRAM pendant une longue extraction, mais exige une admission à chaque transition et une sauvegarde d’état avant attente. Ce protocole doit précéder toute libération anticipée de réservation GPU.</li>
<li><strong>Tester hors production puis sur une petite charge dédiée.</strong> Comparer débit ROI/heure, temps moyen arrivée→fin, attente p95 et maximale, partage entre utilisateurs, erreurs/OOM, swap, débit NAS et GPU. Une RAM davantage remplie n’est pas à elle seule une réussite. Ne retenir une politique que si le gain réel et la qualité de service sont établis.</li>
</ol>
<p>Le backfill temporel permet aussi d’exécuter des petits jobs qui n’allongent pas l’attente d’un job prioritaire ; il nécessite des estimations de durée raisonnables. La protection actuelle de capacité ne remplace pas cette prévision temporelle. La suspension d’un processus ne rend pas sa RAM : elle ne résout pas ce problème.</p>

<h2>Validation et limites</h2>
<ul><li>10 tests unitaires du simulateur réussis : plafonds des quatre ressources, arrivées, récupération seulement au checkpoint, ancien code préservé, plafond CPU, équité utilisateur et admission.</li>
<li>19 tests ciblés du Hub et sondes locales réussis ; un test existant exclu après diagnostic : sa fixture ne fournit pas params_json, désormais lu par l’attribution OOM. La suite initiale a donc eu un échec de fixture, et non une validation complète. Une sonde avec une fixture complète confirme l’attribution au budget réel du job. Aucun fichier de production ou test historique n’a été modifié.</li>
<li>Deux tests MATLAB existants du streaming ROI réussis ; six comparaisons synthétiques de pixels réussies.</li>
<li>Pas de benchmark de charge sur les serveurs, pas de déploiement, pas de revendication de performance NAS ou de réduction réelle des durées. Les coûts de démarrage MATLAB/pool, libération des bibliothèques et récupération des modèles restent à mesurer sur une charge dédiée.</li></ul>

<h2>Sources et preuves reproductibles</h2>
<p>Les mécanismes étudiés s’appuient sur <a href="https://www.usenix.org/conference/nsdi11/dominant-resource-fairness-fair-allocation-multiple-resource-types">Dominant Resource Fairness, NSDI 2011</a> pour le partage multiressource, et la documentation officielle Slurm sur le <a href="https://slurm.schedmd.com/sched_config.html">backfill</a> et la <a href="https://slurm.schedmd.com/preempt.html">préemption</a>. La proposition coopérative ci-dessus est une adaptation pour DetecDiv, pas une fonction déjà disponible du Hub.</p>
<p><a href="simulator.py">Simulateur</a> · <a href="simulation_results.json">Résultats complets</a> · <a href="test_simulator.py">Tests des invariants</a> · <a href="test_hub_policy_probe.py">Sondes Hub isolées</a> · <a href="storage_probe.m">Essai MATLAB</a> · <a href="storage_results.json">Mesures stockage</a> · <a href="storage_probe.log">Journal MATLAB</a> · <a href="live-before.json">Observation avant</a> · <a href="live-after.json">Observation après</a>.</p>
<p>Reproduction locale : <code>python simulator.py --out simulation_results.json --seeds 12</code>, puis <code>python -m unittest discover -s . -p test_simulator.py -v</code>. Les sondes Hub utilisent pytest et des doublures locales ; le script MATLAB crée uniquement ses propres données synthétiques.</p>
</html>'''
(ROOT / 'report.html').write_text(html, encoding='utf-8')
print(json.dumps({'report': str(ROOT / 'report.html'), 'simulations': len(sim['results']),
                  'production_invariants_unchanged': True, 'jobs': len(bjobs)}))
