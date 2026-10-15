# Qwen pour l’assistant DetecDiv Hub — bilan du 15 octobre 2026

## Décision

Le choix provisoire est **Qwen3-14B-AWQ pour le diagnostic supervisé**, avec un
contexte de 8192 tokens et une seule conversation à la fois. Sur les 11 cas de
cette campagne, son compromis qualité/latence/VRAM est le plus intéressant :
médiane provider de 17,72 s et pic propre au service de 13,23 Gio.
Prévoir environ 15 Gio dans la planification de sa réservation, avec au moins
2 Gio physiquement libres et priorité préemptive aux analyses scientifiques.
Cette provision n’est pas un nouveau plafond dur testé.

Le 8B AWQ est un repli si la VRAM prime. Aucun des quatre modèles n’est validé
pour modifier des paramètres scientifiques ou lancer une reprise autonome.
Les FP8 fonctionnent sur le matériel testé, mais n’apportent pas de gain global
de fiabilité établi sur ce corpus et consomment plus de VRAM.

Cette version destinée au dépôt contient les mesures, les conclusions et la
revue anonymisée des 44 sorties. Les captures, réponses brutes, identifiants de
jobs, noms de projets, chemins de données, journaux et contrôleurs restent dans
l’archive privée locale ; ils ne sont pas publiés avec ce document.

## Protocole

- Matériel : RTX 3090, 24576 MiB de VRAM ; vLLM 0.29.0, Linux.
- Corpus figé de 11 cas historiques : 3 succès techniques, 5 échecs, 3 suivis.
  Les captures datent du 14 octobre ; elles ne décrivent jamais un état live.
- SHA256 du corpus :
  `e3fc6d927b24cbb18bf2b936d7405e4bd7b42eadee0be76c2dbc6ea216e276ce`.
- Ordre : 14B AWQ, 14B FP8, 8B AWQ, 8B FP8 ; un seul checkpoint en VRAM.
- Même contexte 8192, concurrence 1, batch 256, plafond de sortie 1100 par
  requête, température 0, seed requête 42, thinking désactivé, preuves standard.
- Mode eager, choix automatique d’outils, parser Hermes,
  `VLLM_USE_FLASHINFER_SAMPLER=0`, sans offload ni cache KV fixe.
- Caps vLLM du plan : 0,55 / 0,80 / 0,35 / 0,55. Le cache KV automatique varie
  donc entre bras ; ces caps ne constituent pas des plafonds mémoire stricts.
- Watchdog indépendant à 22400 MiB et marge physique minimale de 2048 MiB.
  Admission transactionnelle bornée et préemption à l’arrivée d’une demande
  scientifique. Aucun job scientifique réservé ni autre compute admis pendant
  les bras principaux : ce protocole ne teste pas la coexistence.

| Checkpoint officiel | Révision exacte |
| --- | --- |
| Qwen/Qwen3-14B-AWQ | `31c69efc29464b6bb0aee1398b5a7b50a99340c3` |
| Qwen/Qwen3-14B-FP8 | `9a283b4a5efbc09ce247e0ae5b02b744739e525a` |
| Qwen/Qwen3-8B-AWQ | `4da05a8edb55c6046cce958586c33b61da07bb79` |
| Qwen/Qwen3-8B-FP8 | `220b46e3b2180893580a4454f21f22d3ebb187d3` |

## Mesures principales

Gio = 1024 MiB. Les valeurs exactes, latences par cas anonymisé, tokens et
paramètres sont dans [l’annexe JSON](qwen-assistant-benchmark-2026-10-15.metrics.json).

| Modèle | Sorties complètes / tronquées | Médiane provider | Pic chargement | Résidence médiane | Pic service | Marge physique minimale |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 14B AWQ | 11 / 0 | 17,72 s | 13,17 Gio | 13,23 Gio | 13,23 Gio | 10,69 Gio |
| 14B FP8 | 11 / 0 | 21,71 s | 19,06 Gio | 19,08 Gio | 19,08 Gio | 4,84 Gio |
| 8B AWQ | 10 / 1 | 19,45 s | 9,63 Gio | 8,51 Gio | 9,63 Gio | 14,29 Gio |
| 8B FP8 | 10 / 1 | 25,06 s | 13,18 Gio | 13,22 Gio | 13,22 Gio | 10,70 Gio |

La latence est la somme des durées HTTP provider de chaque conversation :
préfill, sélection d’outils et génération inclus ; exécution locale des outils,
SSH, gardes et démarrage exclus. La résidence couvre les échantillons après
disponibilité du serveur, requêtes et attentes comprises. Le pic de chargement
est mesuré avant cette disponibilité ; le pic service couvre toute la vague.
La marge est le total physique moins le maximum total GPU, fond système inclus.
Les processus sont attribués au service par cgroups ; échantillonnage environ
chaque seconde, susceptible de manquer des transitoires plus courts.

| Modèle | Tokens entrée / sortie cumulés | Requêtes provider | Appels preuves | Appels historique | Historiques sur les 3 suivis actifs |
| --- | ---: | ---: | ---: | ---: | ---: |
| 14B AWQ | 48461 / 5343 | 22 | 11 | 0 | 0 |
| 14B FP8 | 48461 / 6428 | 22 | 11 | 0 | 0 |
| 8B AWQ | 61321 / 7831 | 25 | 11 | 3 | 0 |
| 8B FP8 | 64721 / 8835 | 25 | 11 | 6 | 3 |

Ce sont les appels d’outils réellement émis, pas une détection par mots-clés.
Les tokens d’entrée incluent les préfixes répétés sur les différents appels.

## Qualité et interprétation

La revue a porté sur les 44 sorties, dont 42 complètes et 2 tronquées. Il ne
s’agit pas d’un score automatique ni d’une validation biologique indépendante.
Les erreurs matérielles comprennent une affirmation ou une correction non
étayée susceptible d’induire une mauvaise interprétation ou configuration.

- **14B AWQ** : RAM et nœuds fautifs généralement bien reconnus, pas d’ETA
  chiffrée inventée. Surinterprétation d’un succès partiel, confusion entre
  quatrième sous-indice et valeur d’indice 4, canal Cellpose transféré d’un
  autre nœud. Aucun historique sur les suivis.
- **14B FP8** : pas de gain net ; pièges d’indexation et de canal conservés,
  contradiction du succès partiel non signalée, correction de fichier non
  causale proposée. Aucun historique sur les suivis.
- **8B AWQ** : utile sur RAM et nœuds fautifs ; erreurs explicites de portée
  locale/globale et de succès partiel, une sortie tronquée. Les trois appels
  d’historique concernent des cas d’échec, pas les suivis actifs.
- **8B FP8** : les trois historiques de suivi sont effectivement consultés,
  mais la progression locale est encore qualifiée de globale et une remise à
  zéro de compteur devient un ralentissement supposé. Succès partiel et canal
  restent surinterprétés ; une sortie tronquée.

À taille 14B égale, FP8 ajoute environ 5,85 Gio de pic et 22,6 % de durée médiane.
À taille 8B égale, il ajoute environ 3,59 Gio et 28,8 %. Ces écarts de durée ne
mesurent pas isolément le noyau : texte généré, choix d’outils et cache de
préfixes diffèrent. Le 14B répond ici plus vite que le 8B à format égal,
avec moins de texte et d’appels ; cela ne prouve pas une vitesse intrinsèque
supérieure du modèle dense le plus grand.

Les journaux confirment `MarlinLinearKernel` pour AWQ et
`MarlinFP8ScaledMMLinearKernel` pour FP8. Sur SM86, vLLM annonce une compression
FP8 des poids sans calcul FP8 natif. Empreintes de chargement des poids selon
vLLM : 9,44 / 15,54 / 5,71 / 8,90 Gio dans l’ordre des bras. Ces empreintes ne
sont pas la mémoire totale du service. Le FP8 n’est donc pas incompatible avec
cette RTX 3090 ; ses résultats ne préjugent pas de ceux d’un GPU plus récent.

## Garde-fous prioritaires pour le Hub

1. Charger preuves et historique systématiquement pour tout suivi actif.
   Donner timestamp, hôte, nœud, FOV/ROI et portée du compteur ; comparer
   seulement des périmètres identiques.
2. Séparer heartbeat du worker, nouvelle sortie et progression scientifique.
   Ne fournir aucune ETA sans modèle de durée étayé.
3. Signaler les contradictions de compteurs de nœuds malgré un statut `done`.
   La réussite technique ne démontre pas la qualité biologique des sorties.
4. Valider les paramètres par schéma et provenance du nœud concerné : RAM vs
   VRAM, préfixe de frames contigu `1:N`, dimensions MATLAB, canaux disponibles.
5. Présenter les hypothèses comme telles ; soumettre toute modification ou
   reprise scientifique à validation humaine.

Une provision d’environ 11 Gio est proposée pour le 8B AWQ en repli, dans les
conditions testées. Changer contexte, budget KV, offload ou génération crée une
nouvelle variante ; les bras supplémentaires 4B/8B réduits ne constituent pas
une mesure pure de taille ou de quantification.

## Exploitation et clôture

Les réservations scientifiques ont été respectées jusqu’à leur fin, en
vérifiant tous les jobs queued/running/cancelling, leurs allocations, les
processus/cgroups et nvidia-smi. Aucun arrêt d’analyse ni redémarrage API/worker
n’a été effectué par cette campagne. Les checkpoints officiels déjà en cache
ont été réutilisés.

La fin d’un job d’ingestion peut reprendre le Qwen de production quand l’état
désiré reste `running`, même sans réservation GPU de ce job. Des reprises entre
bras et après le dernier bras ont été identifiées par l’horodatage de service
et la fin d’ingestion. Le service original n’a été arrêté temporairement
qu’après vérification de son identité, du hash et de zéro requête active ou
en attente. L’arbitrage et l’état désiré sont restés inchangés.

Au contrôle final du 15 octobre à 05:53:26 Europe/Paris : API/base saines,
workers Linux et Windows idle, aucun job queued/running/cancelling, aucun
processus compute GPU, 74 MiB de fond système. Admission libérée, watchdog
arrêté, lanceur restauré exactement en mode 0755 ; Qwen inactive. L’état désiré
de production peut provoquer ultérieurement une reprise normale. L’automatisation
de comparaison a été désactivée. Ce constat est historique, pas une promesse
sur l’état ultérieur du serveur.

## Limites

Onze cas et une génération par condition ne démontrent aucune supériorité
statistique. Les différences entre checkpoints officiels ne sont pas toutes
attribuables aux seuls 4 ou 8 bits. Aucune répétition chronométrique, aucun
essai de charge multi-utilisateur et aucune validation de coexistence n’ont
été réalisés pour les quatre bras principaux. L’annexe de revue ci-dessous
documente les erreurs plutôt que de masquer les réponses tronquées.

## Pistes de modèle et matériel — estimations, non testées

Recommandation de recherche au 15 octobre 2026 : **Qwen3.6-27B** est un candidat
pour améliorer la qualité de diagnostic ; **Qwen3.6-35B-A3B** est un candidat
pour limiter le coût de calcul par token. Les fiches officielles décrivent un
27B dense et un MoE de 35B dont environ 3B actifs. Les résultats de code et
d’outils publiés motivent un essai, sans prouver un gain sur DetecDiv ni une
latence plus faible. Le MoE doit conserver tous ses poids en mémoire pour une
inférence entièrement GPU. Voir les fiches
[27B](https://huggingface.co/Qwen/Qwen3.6-27B) et
[35B-A3B](https://huggingface.co/Qwen/Qwen3.6-35B-A3B).

Les budgets suivants sont des **estimations de planification**, pour du texte,
une conversation, contexte 8k–16k, sans offload ni MTP. Ils comprennent une
provision pour poids non quantifiés, métadonnées, états d’attention, cache et
runtime. La mémoire du modèle seul ne suffit pas ; les tailles de carte sont
nominales constructeur, les besoins sont donnés en Gio.

| Candidat | Besoin service estimé | Carte à viser | Intérêt |
| --- | ---: | --- | --- |
| Qwen3.6-27B, quantification 4 bits à valider | 18–23 Gio | 32 Go | Essai qualité avec une carte grand public ; 24 Go serait plus serré |
| Qwen3.6-27B-FP8 officiel | 32–38 Gio | 48 Go | Choix confortable pour réduire la compression des poids |
| Qwen3.6-35B-A3B, quantification 4 bits à valider | 22–28 Gio | 32 Go, 48 Go pour davantage de marge | Piste latence grâce au MoE ; aucun débit promis |
| Qwen3.6-27B BF16 | 58–66 Gio | 72 ou 96 Go | Référence peu compressée, achat plus lourd |

Ordre de grandeur des seuls poids : `paramètres × bits / 8` ; ainsi 27 milliards
de paramètres à 4 bits valent environ 12,6 Gio avant métadonnées et tenseurs
conservés dans un autre format. Le runtime et les réservations peuvent ajouter
plusieurs Gio. Les quants 4 bits de ces candidats ne sont pas des checkpoints
officiels AWQ déjà validés par cette campagne : il faudra choisir et figer une
révision, sa méthode et son runtime. Le
[27B FP8 officiel](https://huggingface.co/Qwen/Qwen3.6-27B-FP8) existe.

Pour un achat destiné au Hub, la cible raisonnable est une **RTX PRO 5000
Blackwell 48 Go dédiée au LLM**, en gardant la RTX 3090 pour le calcul
scientifique. Une **RTX 5090 32 Go** est l’option grand public pour les quants
4 bits ; elle ne doit pas être dimensionnée comme une carte 48 Go pour le
27B FP8. Les variantes PRO 5000 72 Go et PRO 6000 Blackwell 96 Go offrent plus
de capacité, sans être nécessaires au premier essai 27B FP8.
Capacités vérifiées auprès de NVIDIA :
[5090](https://www.nvidia.com/en-us/geforce/graphics-cards/50-series/rtx-5090/),
[PRO 5000 48/72 Go](https://www.nvidia.com/en-us/products/workstations/professional-desktop-gpus/rtx-pro-5000/),
[PRO 6000 96 Go](https://www.nvidia.com/en-us/products/workstations/professional-desktop-gpus/rtx-pro-6000/).
La compatibilité châssis/alimentation/refroidissement reste à vérifier avant
achat ; aucune comparaison de prix ni de débit matériel n’est réalisée ici.

Les budgets 8k–16k ne couvrent pas le contexte nominal 262k des fiches Qwen3.6,
ni leurs réglages de benchmark de raisonnement long. Une réduction de contexte
ou une sortie de raisonnement courte peut changer la qualité. Tester dans un
environnement isolé, avec le parser de cette famille (`qwen3_coder` dans les
exemples officiels) plutôt que recopier Hermes sans validation. Évaluer
séparément le raisonnement activé et désactivé, sa longueur et son coût.

Avant achat : corriger l’orchestration déterministe, puis refaire les 11 cas
et des cas tenus à part avec preuves identiques, mêmes limites explicites,
mesures de chargement/résidence/pic, latences et revue des erreurs matérielles.
Une carte dédiée évite la contention de VRAM avec les analyses ; elle ne
supprime pas une éventuelle contention CPU, RAM ou stockage.

## Revue qualitative anonymisée des 44 sorties

Les codes C01 à C11 conservent le même ordre pour les quatre bras. Ils ne
contiennent ni identifiant de job ni chemin vers des données scientifiques.

### Qwen/Qwen3-14B-AWQ

**C01 — Succès technique A — Utilisable sous supervision**

Résume correctement les deux nœuds terminés et demande une validation biologique. L'avertissement dependency_audit.json absent est bien présent dans preflight.warnings ; ne pas le confondre avec l'affichage normal dependency_audit:[1x1 struct]. Allocation décrite comme usage réel : imprécision.

**C02 — Succès technique B — Utilisable sous supervision**

Code retour zéro et doneNodes 2/2 reconnus. Validation biologique explicitement requise. Avertissement préflight correctement rapporté.

**C03 — Succès avec compteurs contradictoires — Erreur matérielle**

Affirme les deux nœuds exécutés et les canaux traités alors que report_summary donne doneNodes=0,totalNodes=1. Le statut done n'établit pas les résultats scientifiques.

**C04 — OOM RAM/swap — Diagnostic fondé**

OOM RAM+swap identifié, budgets 24576/1536 MiB corrects, hausse RAM ou baisse pool/batch proposées. Ne confond pas RAM et VRAM.

**C05 — Extraction legacy et sélection de frames — Diagnostic principal fondé, réserves**

Reprend le mécanisme exact allocation par nombre de frames puis index absolu et préfixe partant de 1. Transforme aussi le message No data file for ROI en vérification de fichiers manquants, sans démontrer que ce soit la cause ; garder comme vérification secondaire.

**C06 — Suivi avec nouvelles sorties — Prudent mais incomplet**

computeDrift DONE=12 frames est local ; aucun avancement global ni ETA affirmé. Heartbeat limité à l'activité du worker. N'appelle pas get_progress_history et ne démontre donc pas une tendance.

**C07 — Suivi avec pourcentage local croissant — Prudent mais incomplet**

Rapporte 86,3% et extraction 6/9, puis précise portée locale et ETA inconnue. La première formulation processus à 86,3% reste ambiguë. Aucun historique comparé.

**C08 — Suivi avec compteur de frames non comparable — Trop affirmatif sur l'absence de blocage**

Frame 47/100 et localValue=46% correctement circonscrits, pas d'ETA. Présente value=0,245 comme correspondant à 46%, sans relation prouvée ; conclut pas de blocage sans comparer l'historique. Heartbeat prouve le worker vivant, pas l'avancement.

**C09 — Erreur de dimensions de table — Diagnostic prudent mais générique**

classifier_cnn_lstm_1, FOV8 ROI1 et MATLAB:table:RowDimensionMismatch correctement cités. Vérification des dimensions justifiée ; cause exacte et correctif restent inconnus. Ne contextualise pas l'ancien état cancelling/orphelin.

**C10 — Erreur du quatrième sous-indice — Erreur matérielle**

Nœud lineage correct, mais traduit position 4 comme indice 4 et tableau de 13 éléments. La preuve porte sur le quatrième sous-indice/dimension, valeur fautive inconnue et supérieure à 13.

**C11 — Canal Cellpose non configuré — Erreur matérielle**

Identifie CellposeSAM et absence de canal configuré, puis impose ou suggère 0 TL _z2 sans preuve : ce paramètre appartient à roipattern_4. Le canal Cellpose doit être validé contre les canaux réellement disponibles.


### Qwen/Qwen3-14B-FP8

**C01 — Succès technique A — Utilisable sous supervision**

Deux nœuds 2/2 et code zéro correctement résumés ; sorties à vérifier et validation biologique demandée. Avertissement préflight réel.

**C02 — Succès technique B — Utilisable sous supervision**

Succès technique 2/2 correctement établi ; pas de validation biologique automatique. Allocation parfois décrite comme consommation constatée.

**C03 — Succès avec compteurs contradictoires — Omission critique**

Ne prétend plus explicitement deux nœuds terminés, mais ignore doneNodes=0,totalNodes=1 et affirme un pipeline réussi avec canaux utilisés. Longue liste de contrôles sans signaler la contradiction centrale.

**C04 — OOM RAM/swap — Diagnostic fondé**

RAM et swap 24576/1536 MiB correctement identifiés ; hausse budget ou réduction pool/batch, pas de confusion VRAM.

**C05 — Extraction legacy et sélection de frames — Correctif non fondé**

Cite correctement le mécanisme de sélection absolue et le préfixe depuis 1. Fait ensuite du fichier .mat absent une explication alternative puis demande sa génération, sans lien prouvé avec LegacyFrameSelectionLimit ; ne formule pas précisément 1:N contigu comme correction suffisante.

**C06 — Suivi avec nouvelles sorties — Prudent mais incomplet**

12 frames computeDrift local ; pas de pourcentage global ni ETA. Demande permission de lire un historique déjà disponible au lieu de le consulter.

**C07 — Suivi avec pourcentage local croissant — Trop affirmatif sur la progression**

Rapporte 86,3% puis précise portée locale ; aucune ETA inventée. Semble progresser normalement et absence de blocage déduites de error_text vide, sans historique. Propose de le consulter plus tard.

**C08 — Suivi avec compteur de frames non comparable — Prudent mais incomplet**

Frame 47/100 et 46% explicitement locaux, pas de conversion du value=0,245 et pas d’ETA. Heartbeat limité au worker actif ; absence de blocage reste non prouvée, historique non consulté.

**C09 — Erreur de dimensions de table — Diagnostic prudent mais générique**

Nœud classifier et FOV8 ROI1 corrects, RowDimensionMismatch cité. Dimensions à vérifier, cause non prouvée ; chemins et corruption sont des pistes génériques. Omet contexte historique cancelling/orphelin.

**C10 — Erreur du quatrième sous-indice — Erreur matérielle**

Nœud correct et indice hors limites identifié, mais position 4 traduite comme position dans un tableau de taille 13. Ne distingue pas quatrième dimension et valeur du sous-indice inconnue.

**C11 — Canal Cellpose non configuré — Erreur matérielle**

Nœud Cellpose et absence de canal identifiés, puis associe le canal 0 TL _z2 au mauvais nœud et recommande de le configurer ou remplacer. La provenance de roipattern_4 ne justifie pas ce choix Cellpose.


### Qwen/Qwen3-8B-AWQ

**C01 — Succès technique A — Utilisable sous supervision, imprécisions**

Reconnaît 2/2 et absence d’erreur. Contrôles de sorties corrects, mais appelle run.json un répertoire possible et traite les affichages de structs comme preuve d’exécution. Pas d’affirmation explicite de validation biologique.

**C02 — Succès technique B — Réponse tronquée**

Succès technique 2/2 correct et contrôle des sorties demandé. Répétitions et chemins longs conduisent à finish_reason=length au plafond 1100 tokens ; réponse à relire malgré bon diagnostic principal.

**C03 — Succès avec compteurs contradictoires — Erreur matérielle**

Affirme deux nœuds exécutés ; cite doneNodes=0,totalNodes=1 puis le présente comme confirmation du succès. Confond fin du processus et exécution des traitements scientifiques.

**C04 — OOM RAM/swap — Diagnostic fondé mais verbeux**

RAM/swap 24576/1536 et OOM bien identifiés, batch et budget mémoire proposés. Appelle réellement l’historique de ce cas d’échec ; aucune confusion VRAM.

**C05 — Extraction legacy et sélection de frames — Diagnostic partiel**

Limite de frames et départ à 1 reconnus, mais ne précise pas suffisamment la contiguïté du préfixe 1:N ; repartir de 1 seul ne suffit pas pour une sélection trouée. Message de fichier .mat absent traité comme autre problème causal. Historique appelé.

**C06 — Suivi avec nouvelles sorties — Prudent mais incomplet**

Pas d’ETA ni de progression globale fabriquée. Heartbeat et computeDrift correctement rapportés comme activité limitée ; aucun historique comparé.

**C07 — Suivi avec pourcentage local croissant — Erreur matérielle**

Annonce 86,315789% du travail total alors que la fraction est locale à parallel_fov/extraction. Pas d’ETA chiffrée, mais surestime la portée de la mesure et ne lit pas l’historique.

**C08 — Suivi avec compteur de frames non comparable — Portée ambiguë et progression surestimée**

46% présenté comme travail effectué puis précisé local. Affirme 47 frames déjà traitées et 53 restantes alors que la capture dit frame 47 en traitement. Pas d’ETA, aucun historique comparé.

**C09 — Erreur de dimensions de table — Diagnostic principal fondé, pistes excessives**

Nœud classifier, FOV8 ROI1 et RowDimensionMismatch corrects. Ajoute des vérifications du canal 0 TL _z2, dépendances et ressources sans cause démontrée ; omet contexte cancelling. Historique réellement appelé mais pas exploité pour ce contexte.

**C10 — Erreur du quatrième sous-indice — Prudent mais peu explicatif**

Cite correctement index en position 4 avec borne 13, sans affirmer que la valeur fautive est 4. Propose vérifications de dimensions et indices, mais ne dit pas explicitement quatrième sous-indice et valeur inconnue.

**C11 — Canal Cellpose non configuré — Erreur matérielle**

Absence de canal Cellpose reconnue, puis attribue le canal de roipattern_4 à Cellpose et recommande de le configurer. Message .mat absent également présenté comme problème contributif sans démonstration.


### Qwen/Qwen3-8B-FP8

**C01 — Succès technique A — Utilisable sous supervision, réserves**

Reconnaît doneNodes 2/2 et code retour zéro ; demande les sorties et contrôles. La phase matlab_running ne prouve pas à elle seule une exécution correcte, et les champs stdout affichent des structures normales plutôt que des journaux entièrement vides. Les allocations ne sont pas une mesure des consommations.

**C02 — Succès technique B — Utilisable sous supervision**

Reconnaît le succès technique 2/2 et distingue explicitement la validation biologique. Demande les sorties plutôt que de conclure à leur qualité. Réponse complète.

**C03 — Succès avec compteurs contradictoires — Erreur matérielle et réponse tronquée**

Présente doneNodes=0,totalNodes=1 et la phase matlab_running comme des signes de bonne exécution, sans signaler la contradiction avec le succès global. Les deux nœuds sélectionnés ne sont pas prouvés exécutés. Réponse interrompue au plafond de 1100 tokens.

**C04 — OOM RAM/swap — Diagnostic principal fondé, réserves**

Identifie correctement le budget RAM 24576 MiB et swap 1536 MiB, sans confusion avec la VRAM. Réduire pool/batch ou augmenter le budget sont des pistes fondées. Ajoute des images 2048x2048 16 bits non établies pour ce cas et propose une réduction de résolution sans justification scientifique. Historique effectivement consulté.

**C05 — Extraction legacy et sélection de frames — Diagnostic principal fondé, correction incomplète**

Identifie extract_cropped_images_legacy et le préfixe commençant à 1, mais les consignes ne précisent pas assez que le préfixe doit être contigu 1:N ; commencer à 1 avec des trous ne suffit pas. Transforme No data file for ROI en défaut à corriger alors que ce message ne prouve pas la cause. Historique consulté.

**C06 — Suivi avec nouvelles sorties — Historique utilisé, progrès surinterprété**

Appelle réellement get_progress_history et cite correctement les deux captures. Le changement de sortie établit une nouvelle sortie observée ; les points et heartbeat ne prouvent pas une progression scientifique continue. Affirme pourtant progression continue. Aucune ETA inventée.

**C07 — Suivi avec pourcentage local croissant — Historique utilisé, erreur de portée**

Observe à juste titre la hausse locale 86.04288499 vers 86.31578947 et cite les captures. Qualifie à plusieurs reprises cette valeur de progression globale, malgré la mention contradictoire valeur locale. Le pourcentage est celui de parallel_fov, pas de tout le pipeline. Aucune ETA inventée.

**C08 — Suivi avec compteur de frames non comparable — Historique utilisé, ralentissement non démontré**

Cite les frames 58 puis 47 et valeurs locales 0.57 puis 0.46. N’établit pas un périmètre ROI identique et conclut pourtant semble ralenti. Les valeurs runtime 0.2403567 puis 0.2450971 ne sont pas prouvées globales ; frame 47 ne signifie pas 47 frames achevées. Aucune ETA inventée.

**C09 — Erreur de dimensions de table — Cause partiellement reconnue, pistes non étayées**

Reconnaît RowDimensionMismatch et dotAssign ligne 508 mais omet le nœud classifier_cnn_lstm_1, FOV8/ROI1, dans la réponse. Affirme un échec de chargement HDF5 et propose encodage/chemins sans preuve causale. L’ancien état Hub cancelling orphelin n’établit pas une annulation par un tiers. Deux captures terminales identiques ne prouvent pas que le problème persiste après correction.

**C10 — Erreur du quatrième sous-indice — Nœud et erreur reconnus, correction générique**

Identifie processor_trackmotherlineageviterbi_9 et cite index en position 4, maximum 13, sans inventer la valeur fautive 4. Plus prudent que les 14B sur ce point ; ne précise toutefois pas que la position désigne le quatrième sous-indice. La cause exacte reste à vérifier ; pistes ressources/chemins générales non causales.

**C11 — Canal Cellpose non configuré — Erreur matérielle de paramétrage**

Reconnaît l’absence de canal d’entrée configuré pour Cellpose SAM, puis affirme que le canal requis est 0 TL _z2. Ce nom provient d’un autre nœud, roipattern_4 : son transfert au nœud fautif n’est pas prouvé. Ne pas appliquer la configuration proposée sans validation des canaux disponibles et du paramètre du nœud concerné.
