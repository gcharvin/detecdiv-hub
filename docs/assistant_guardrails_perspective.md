# Perspective pour un agent : assistance fiable au lancement et au dépannage

Date : 15 octobre 2026. Statut : orientations de conception et ordre de travail
proposé, pas fonctionnalités livrées. Cette note ne constitue pas une demande
d’implémentation ni une autorisation de déploiement.

Le Qwen3-14B-AWQ peut accompagner un utilisateur si le Hub établit les faits,
valide les configurations et contrôle les actions. Le modèle prend en charge
la conversation, les questions manquantes et les explications. Une réponse
erronée du modèle ne doit pas suffire à déclencher un lancement invalide ou une
modification scientifique.

## Contexte et points de départ

Les campagnes locales sur les mêmes 11 cas ont montré des erreurs persistantes :
succès technique surinterprété, heartbeat assimilé au progrès, portée locale
d’un pourcentage perdue, correction scientifique proposée sans cause établie.
Le 14B AWQ reste le compromis retenu pour le diagnostic supervisé ; aucun modèle
testé n’est validé pour une reprise autonome.

Sources de cette perspective :

- [Bilan Qwen3 8B et 14B AWQ et FP8](reports/qwen-assistant-benchmark-2026-10-15.md).
- [Comparaison Qwen3.6 27B et Qwen3 14B](reports/qwen36-27b-vs-qwen3-14b-2026-10-15.md).
- [Configuration et limites actuelles de l’assistant](qwen_assistant.md).

Dans l’état du code examiné, `api/services/qwen_assistant.py` fournit un chat
sans outils Hub. Les outils d’evidence et d’historique du banc d’évaluation ne
sont pas encore des outils publics de l’assistant. Le préflight existant dans
`api/services/pipeline_runs.py`, exposé par `/pipeline-runs/preflight`, contrôle
notamment droits, références, cible et verrous. Il ne valide pas toute la
sémantique des paramètres scientifiques. Étendre ces surfaces existantes.

Avant toute implémentation, relire `AGENTS.md`, `CURRENT_DEPLOYMENT.md` et les
modules concernés : cette description peut évoluer. Le changement envisagé
traverse API, interface et worker ; une migration n’est nécessaire que si de
nouvelles données persistantes le demandent.

## Répartition des responsabilités

| Composant | Responsabilité |
| --- | --- |
| Modèle | Comprendre la demande, demander les précisions, expliquer les faits et les options autorisées. |
| API Hub | Appliquer les droits, résoudre les identifiants, préparer les faits et brouillons, valider les contrats, journaliser les décisions. |
| Interface Hub | Afficher les faits vérifiés, les inconnues, la configuration exacte et les conséquences avant confirmation. |
| Worker | Vérifier les accès et dépendances sur la cible, arbitrer les ressources et exécuter le contrat autorisé. |
| Moteur DetecDiv | Définir la sémantique des nœuds, leurs paramètres et leurs contraintes scientifiques. |
| Utilisateur | Choisir les options scientifiques et approuver la configuration ou la correction précise présentée. |

Un texte généré, une confiance déclarée, un JSON valide ou une citation présente
ne constituent pas une validation technique. La confirmation humaine complète
les contrôles du Hub ; elle ne les remplace pas.

## Préparer un lancement guidé

1. Résoudre le projet et le pipeline parmi les objets accessibles à l’utilisateur.
   Présenter les ambiguïtés au lieu de choisir silencieusement.
2. Construire un brouillon typé : projet, version du pipeline et du code, nœuds,
   FOV/ROI/frames, canaux, paramètres, cible et ressources demandées.
3. Fournir au modèle les choix issus du catalogue et des métadonnées. Refuser
   les identifiants, paramètres ou canaux inconnus. Demander les valeurs manquantes.
4. Valider le brouillon et présenter les erreurs bloquantes, avertissements et
   vérifications non réalisées. Ne pas afficher « prêt » si un contrôle requis
   reste inconnu.
5. Afficher la configuration complète et ses différences avec les réglages
   enregistrés. Le bouton de lancement soumet exactement ce brouillon validé,
   sans modification implicite par le modèle.
6. Revérifier droits, versions et verrous au moment de la soumission. Si le
   projet, le pipeline ou le brouillon a changé, invalider l’approbation et
   présenter le nouvel état. Le worker arbitre les ressources à l’admission.
7. Empêcher une double soumission du même lancement par une clé d’idempotence.
   Conserver l’auteur réel, la configuration effective et les versions utilisées.

Les budgets proposés restent soumis aux quotas et aux capacités. Une modification
de ressources pour une reprise ne doit pas modifier silencieusement un job actif.

## Approfondir la validation

Définir un contrat versionné par nœud : noms et types des paramètres, valeurs
autorisées, dépendances, canaux attendus, contraintes de sélection et compatibilité
avec le code exécuté. La définition doit provenir du moteur DetecDiv ou d’un
manifeste qu’il exporte. Ne pas recréer sa logique scientifique dans le Hub.

Les contrôles nécessitant les fichiers réels doivent être exécutés sur la cible
par un worker : présence des entrées, accès nécessaires, métadonnées et
dépendances. Ce préflight ne doit ni lancer l’analyse, ni modifier les fichiers
scientifiques, ni acquérir une priorité supérieure aux analyses en cours.

Séparer la validation statique d’une éventuelle validation nécessitant MATLAB.
Identifier explicitement les vérifications effectuées, leur date, leur version
et leur périmètre. Un contrôle devenu périmé doit être refait.

## Dépanner à partir de faits structurés

Pour une demande sur un job précis, le Hub collecte systématiquement les preuves
autorisées : statut et dates, cible, nœud fautif, erreur exacte, paramètres
effectifs, version du code et extraits de logs pertinents. Pour une question
d’avancement, collecter aussi l’historique ; ne pas laisser ce choix au modèle.

Afficher les faits dans des cartes produites par le Hub, indépendamment du
texte généré. Associer chaque fait à sa source, son horodatage et sa portée.
Séparer faits, hypothèses et vérifications suivantes.

| Observation | Conclusion permise |
| --- | --- |
| Heartbeat récent | Le worker est joignable ; le progrès scientifique n’est pas établi. |
| Nouvelle sortie horodatée | Une nouvelle sortie a été observée sur ce périmètre. |
| Pourcentage local | Avancement de l’étape, du FOV ou de la ROI explicitement identifiés. |
| Budget RAM ou VRAM | Allocation distincte de la consommation mesurée. |
| Job terminé ou code retour zéro | Fin technique à confronter aux nœuds terminés et aux sorties attendues. |
| Deux compteurs différents | Comparaison seulement si leur périmètre et leur signification sont compatibles. |

Une stagnation apparente ne suffit pas à diagnostiquer un blocage. Sans mesure
et méthode adaptées, afficher une ETA inconnue. La réussite technique ne valide
pas la qualité biologique. Un état historique reste identifié comme historique.

## Encadrer diagnostics et corrections

Créer des fiches de diagnostic maintenues avec les versions du code. Chaque fiche
décrit les conditions de reconnaissance, preuves nécessaires, limites du
diagnostic, vérifications suivantes et corrections possibles. Une signature
d’erreur seule n’établit pas toujours sa cause.

Exemples issus de la campagne :

- **Cellpose sans canal configuré** : lire le paramètre du nœud concerné et les
  canaux réellement disponibles. Ne pas reprendre le canal d’un autre nœud.
- **OOM RAM** : confronter erreur, limites et mesures disponibles. Proposer un
  budget ou une réduction de parallélisme compatibles avec les quotas ; ne pas
  présenter une augmentation de VRAM comme le correctif d’un OOM RAM.
- **Indices MATLAB** : distinguer position du sous-indice et valeur fautive.
  Une valeur absente du log reste inconnue.
- **Affectation de table incohérente** : demander les dimensions et le contexte
  utiles. Ne pas conclure à une corruption ni proposer un effacement comme
  correction établie sans preuve.

Une cause inconnue doit pouvoir aboutir à « cause non déterminée » et à une
collecte de diagnostic ciblée. Toute collecte supplémentaire reste bornée et
soumise aux droits.

Dans la première version, les outils de l’assistant restent en lecture ou en
préparation de brouillon. Suppression d’intermédiaires, exclusion de ROI,
modification scientifique et reprise nécessitent un parcours dédié : préconditions
vérifiées, impact affiché, choix explicite de l’utilisateur et trace d’audit.
Une reprise est un nouveau job lié au précédent, avec configuration et différences
conservées ; l’historique du job d’origine reste intact.

## Droits et frontière d’exécution

- Tous les outils s’exécutent avec les droits de l’utilisateur courant, jamais
  avec un accès administrateur implicite. Vérifier aussi les références indirectes.
- Utiliser des outils étroits et des arguments validés. Le modèle ne reçoit ni
  shell, ni SQL libre, ni identifiants de service.
- Traiter les logs, noms de fichiers et documents récupérés comme des données
  non fiables, jamais comme des instructions autorisant une action.
- Limiter et filtrer le contexte fourni au modèle ; exclure secrets et données
  sans rapport avec la demande. Conserver l’inférence scientifique locale.
- Journaliser les outils appelés, sources, versions, validation, confirmation et
  résultat effectif, avec une politique de rétention et d’accès adaptée.
- Conserver l’arbitrage scientifique existant. Le modèle ne décide ni de libérer
  une réservation ni d’interrompre une analyse sur la base de GPU à 0 %.

## Disponibilité et comportement dégradé

Les cartes de faits, formulaires, validations et messages de refus restent
utilisables lorsque Qwen est arrêté pour libérer la 3090 aux analyses. Une
indisponibilité, un timeout ou une réponse tronquée ne doit provoquer aucune
action implicite ni afficher une correction comme entièrement validée.

## Ordre de mise en œuvre et critères de passage

| Étape | Livrable | Critère de passage |
| --- | --- | --- |
| 1 | Faits et historique en lecture seule, cartes indépendantes du modèle. | Droits appliqués, sources et périmètres visibles, distinctions du tableau respectées. |
| 2 | Diagnostics connus et réponses explicites aux inconnues. | Revue des cas historiques et de cas nouveaux ; aucune correction non étayée présentée comme certaine dans les cas de validation. |
| 3 | Brouillons de lancement et contrats de paramètres versionnés. | Canaux et paramètres invalides bloqués, validations incomplètes visibles, aucune soumission automatique. |
| 4 | Confirmation d’un lancement exact et reprises encadrées. | Brouillon périmé refusé, double clic sans doublon, changement concurrent détecté, provenance complète. |

Pour chaque étape, compléter les 11 cas de régression par des cas nouveaux non
utilisés pour ajuster les prompts : preuves contradictoires, historique absent,
paramètre inconnu, droits insuffisants, référence vers un autre projet, logs
contenant des instructions, réponse malformée ou tronquée, modèle indisponible,
modification du projet après validation et soumissions concurrentes.

Mesurer séparément les erreurs factuelles, les hypothèses injustifiées, les
abstentions pertinentes, les erreurs bloquées et le temps nécessaire à
l’utilisateur pour aboutir. Vérifier les invariants d’action par des tests du
Hub, indépendamment des réponses du modèle. Une revue humaine des réponses
reste nécessaire ; un taux de parsing ou un score par mots-clés ne mesure pas
la qualité du dépannage.

Commencer par les étapes 1 et 2 et la préparation de l’étape 3. N’élargir les
actions qu’après validation du parcours complet ; améliorer un prompt seul ne
suffit pas à franchir cette étape.
