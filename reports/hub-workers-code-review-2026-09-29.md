# Hub et workers — état du 29 septembre 2026

**Observation :** 19:24–19:27 CEST, par inspections en lecture seule des dépôts,
de PostgreSQL, de l'API, des services Linux et de la tâche Windows. Aucun job
scientifique, test, redémarrage ou changement de configuration n'a été lancé
pour ce bilan. Les états de file et les mesures de ressources sont ponctuels.

## Décision sur l'ancien drain

**Ne pas lancer la commande `sudo systemd-run detecdiv-pathfix-rollout` donnée
précédemment.** Le déploiement a changé de méthode depuis cette proposition :
les jobs MATLAB sont maintenant épinglés à un commit, préparé dans un checkout
protégé distinct sur chaque hôte. Un nouveau commit de traitement ne demande
plus de remplacer le checkout partagé ni de redémarrer tous les workers.

L'activation initiale de ce mécanisme reste incomplète sur Linux. Cinq des six
pollers portent l'empreinte attendue `7389e458c980`. `@3` porte encore
`a76583656903` parce qu'il exécute le job historique
`a4bdf595-28be-4e2b-b138-91b243a7bac2`, démarré à 11:28 CEST et toujours
avec un heartbeat récent. `matlab_code_isolation_ready` reste absent sur la
cible Linux : les nouvelles demandes MATLAB ciblant explicitement Linux sont
refusées jusqu'à la fin de l'activation. La cible Windows annonce déjà
`matlab_code_isolation_ready=true` et `matlab_code_shared_cache_ready=true`.

Le helper `scripts/finish_matlab_code_activation.py` était actif sur la station
d'administration (PID 2452 à l'inspection). Il attend la fin du job de `@3`,
réserve brièvement l'admission, recharge uniquement ce poller devenu inactif,
vérifie les six empreintes puis active la cible Linux. Il utilise le compte
du poller pour lui envoyer un signal et ne requiert pas le sudo demandé pour
l'ancien plan. Garder la station et ce processus actifs, puis vérifier l'état
final dans la base. **Ne pas démarrer un second helper ni redémarrer `@3`
pendant son job.** Aucun drain Linux n'était actif au relevé.

## Changements de code récents

| Changement | Effet observé ou limite |
| --- | --- |
| `d7eaa6b` Hub et `45398914` DetecDiv | Mappings fiables entre chemins client, canonique et worker ; le correctif UNC est inclus dans la version publiée actuelle. |
| `6afec5b`, `eee5902` | Le commit MATLAB est fixé à la soumission et l'admission vérifie que la cible sait lancer des versions isolées. Les anciens jobs gardent leur contexte. |
| `b574d69`, `f7126ca`, `a1295ad` | Un checkout protégé est réutilisé par SHA et par hôte ; chaque tentative a un espace de travail persistant. Les ACL Windows ont été ajustées pour garder la synchronisation Git possible. |
| `83aed4c` | L'état des workers expose l'usage GPU du périphérique ; cette mesure ne se confond pas avec la réservation d'un job. |
| DetecDiv `6c6e414a` | L'extraction des ROIs a été modifiée pour tenir compte de la limite mémoire par job. Ce commit est publié, mais aucune exécution réussie sous ce commit n'a été établie par cette inspection. |

Le défaut publié dans `system_settings.matlab_code_release` est désormais
`6c6e414a4a3b52a905f3aab7639cd0d7a935f7d5` (mis à jour après la note
de déploiement qui mentionne `e33264d9`). Ce commit descend de `45398914`,
donc contient le correctif UNC. Son checkout de release existe sur Linux et
Windows. Le HEAD du checkout Linux historique reste `36bfde6c` ; cela est
compatible avec le nouveau mécanisme tant que le job ancien utilise ce code.
Le HEAD Windows observé était `e33264d9` : un job versionné utilise le commit
épinglé, pas nécessairement le HEAD de ce checkout source.

## Service, file et ressources

| Élément | Constat |
| --- | --- |
| API / PostgreSQL | Conteneurs en cours, zéro redémarrage Docker observé ; `/health` renvoie `ok` pour l'API et la base, empreinte API `0d93ee7f38ba`. |
| Workers Linux | Manager et six unités `detecdiv-worker@1…6` actifs ; cinq inactifs à l'empreinte attendue, `@3` occupé et sur l'ancien code. Heartbeats récents. |
| Worker Windows | Tâche planifiée active, worker inactif, heartbeat récent, empreinte `463c764ab270`, version MATLAB isolée prête. |
| File | Un pipeline Linux en cours, zéro job en attente au relevé. `drain_new_jobs=false` sur Linux. |
| RAM / CPU Linux | Hôte 125 Gio RAM, environ 100 Gio disponibles ; slice workers limitée à 96 Gio RAM et 6 Gio swap. Usage de la slice ≈22,3 Gio. `@3` consomme ≈21,1 Gio, avec limite 24 Gio, quota 8 cœurs et zéro swap consommé. Les cinq pollers inactifs ont une limite de 512 Mio chacun. |
| GPU Linux | NVIDIA : 13,3 Gio utilisés sur 24 Gio, charge GPU mesurée 0 % au relevé. Le job actif ne réserve aucune VRAM Hub ; il faut distinguer usage physique et réservations lors de l'admission. |
| Stockage | `/data` à 96 %, environ 3,7 To libres ; racine Linux à 90 %, environ 70 Go libres ; disque système VM à 59 %. `/health` signale l'alerte `/data` même si la base et l'API sont `ok`. |

Les admissions RAM/CPU sont sérialisées par verrou sur la cible. Un job reçoit
une réservation, puis son unité systemd est redimensionnée en place ; la slice
impose le plafond partagé. Le manager garde six pollers de base et peut monter
jusqu'au budget de 36 cœurs. L'état instantané montre ces plafonds appliqués
au job actif ; il ne prouve pas que tous les profils de jobs sont correctement
calibrés. Les mesures GPU et RAM restent à suivre lorsque les traitements
Cellpose et l'extraction de ROIs tournent ensemble.

## Résultats récents et stabilité

Sur les jobs créés dans les 24 heures avant le relevé : 96 ingestions
Micro-Manager terminées, 2 pipelines terminés, 16 pipelines en échec,
1 pipeline en cours et 4 archivages en échec. Les deux ingestions examinées
dans le journal n'avaient aucun candidat ; leur succès n'évalue donc pas le
traitement d'une acquisition réelle. Les 16 échecs de pipeline ont plusieurs
causes visibles, notamment CellposeSAM, conflit OpenMP et extraction de ROIs.
Les quatre archivages récents échouent sur des sources `/data` absentes ; rien
dans ce relevé ne les rattache au déploiement du Hub. Un `401` sur un heartbeat
de lease projet apparaît dans les logs API ; il mérite suivi s'il se répète.

**Conclusion de stabilité :** l'infrastructure API/DB et les pollers inspectés
répondent, et le job ancien a un heartbeat frais. La fiabilité des pipelines
scientifiques ne peut pas être qualifiée de stable avec 16 échecs sur 18 jobs
terminés en 24 heures. Il faut trier ces échecs par code MATLAB, environnement
Python et données, sans les attribuer indistinctement au Hub. La saturation
progressive de `/data` est le risque opérationnel immédiat mesuré.

## Dépôts et publication

Au relevé, `origin/master` (GitHub) et `gitlab/master` pointaient tous deux
vers `ad2e478`. Pour DetecDiv, les branches `unstable` de GitHub et GitLab
pointaient toutes deux vers `6c6e414a`. Cela prouve la synchronisation des
commits observés, pas leur activation automatique : le Hub API est une image
Docker distincte et les workers sont des copies opérationnelles distinctes.
Une release MATLAB doit être préparée sur les deux hôtes puis publiée par
`PUT /matlab-code/release`. Un push Git, à lui seul, ne change ni les jobs déjà
épinglés ni les workers en mémoire.

Le dépôt local contient aussi des changements non committés dans
`worker/run_worker.py`, `docs/worker_resource_scheduling.md` et des scripts SQL
sans rapport avec ce rapport. Ils ne sont pas inclus dans cette publication.

## Suivi recommandé

1. Laisser finir le job historique de `@3` et vérifier que le helper actif
   marque Linux prêt, sans redémarrer le job.
2. Suivre `/data` à 96 % et planifier le nettoyage ou l'extension avant que
   l'espace libre devienne bloquant.
3. Trier les échecs Cellpose/OpenMP et l'échec d'extraction ROI ; confronter
   les prochains jobs au nouveau commit `6c6e414a` et à leur pic RAM/VRAM.
4. Corriger ou retirer les quatre demandes d'archivage dont les sources sont
   absentes, après vérification de leurs emplacements catalogués.
5. Vérifier séparément la mise à jour du client MATLAB d'Abhilasha ; ce bilan
   établit la version des workers, pas celle de son poste.
