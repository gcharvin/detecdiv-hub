# Plan technique - Liaison manuelle DetecDiv Hub / Labguru

## Resume

Implementer une premiere V1 simple ou une experience du hub (`experiment_project`) porte un lien URL Labguru saisi manuellement, puis reexposer ce lien dans les vues projet DetecDiv et raw dataset.

Le lien externe reste canonique au niveau `experiment_project`. Les `raw_datasets` et `detecdiv_projects` ne stockent pas leur propre lien Labguru: ils affichent celui de l'experience liee.

Cette V1 ne cree rien dans Labguru et n'appelle aucune API distante. En parallele, le modele doit rester compatible avec une future integration eLabFTW et avec l'ingestion de metadonnees `pymmcore-plus` depuis la machine d'acquisition.

## Changements d'implementation

### 1. Modele de donnees et conventions

- Reutiliser `external_publication_records` comme table unique de rattachement externe.
- Considerer `experiment_project` comme entite canonique de liaison externe.
- Utiliser `system_key='labguru'` pour la V1.
- Autoriser un seul enregistrement par couple `(experiment_project_id, system_key)`.
- Utiliser `external_url` pour l'URL Labguru saisie manuellement.
- Utiliser `external_id` comme champ optionnel, laisse vide en V1 sauf si un identifiant Labguru fiable est fourni.
- Utiliser `status='linked'` si une URL est presente, et `status='unlinked'` sinon.
- Initialiser `payload_json` a `{}` ou avec un marqueur simple comme `{"link_mode":"manual"}`.
- Ne pas stocker de lien Labguru dans `raw_datasets.metadata_json` ni dans `detecdiv_projects.metadata_json`.

### 2. API

- Continuer a exposer les liens externes complets via `GET /experiments/{id}` et son champ `publication_records`.
- Ajouter un endpoint de mise a jour simple du lien Labguru, par exemple `PATCH /experiments/{id}/publications/labguru`.
- Charge utile minimale de cet endpoint: `external_url: str | null`.
- Si `external_url` est non vide, creer ou mettre a jour l'enregistrement `labguru` avec `status='linked'`.
- Si `external_url` est vide ou `null`, conserver l'enregistrement mais passer `status='unlinked'` et vider `external_url`.
- Etendre `ProjectDetail` pour exposer le lien Labguru herite de l'experience liee.
- Etendre `RawDatasetDetail` pour exposer les experiences liees et, pour chacune, leur lien Labguru.
- Ne pas ajouter d'edition directe du lien au niveau projet ou raw dataset cote API. Ces surfaces doivent cibler l'experience liee.

### 3. UI web

- Etendre l'UI existante dans `api/static/app.js`; ne pas creer une surface parallele.
- Dans la vue experience, afficher l'etat Labguru: `Linked` ou `Unlinked`.
- Dans la vue experience, afficher l'URL cliquable si elle existe.
- Dans la vue experience, ajouter une action `Edit link` et une action `Clear link`.
- Dans la vue projet DetecDiv, afficher l'experience liee et le lien Labguru herite si disponible.
- Dans la vue projet DetecDiv, afficher `No linked experiment` si le projet n'a pas d'experience liee.
- Dans la vue raw dataset, afficher les experiences liees et leur lien Labguru.
- Si plusieurs experiences sont liees au meme raw dataset, toutes les lister explicitement.
- L'edition depuis projet ou raw dataset doit rediriger vers l'experience liee ou ouvrir un panneau d'edition ciblant l'experience.

### 4. Preparation de l'ingestion future `pymmcore-plus`

- Prevoir un schema canonique de metadonnees hub-centric, stocke d'abord dans `experiment_projects.metadata_json`.
- Autoriser une duplication partielle dans `raw_datasets.metadata_json` quand l'information appartient directement a l'acquisition.
- Ne pas coupler ce schema a Labguru; il doit rester neutre pour supporter Labguru puis eLabFTW.
- Le futur flux d'ingestion depuis la machine d'acquisition sera:
  - la machine d'acquisition envoie les metadonnees `pymmcore-plus` au hub via API;
  - le hub cree ou met a jour le `raw_dataset`;
  - le hub cree ou met a jour l'`experiment_project`;
  - le hub attache le raw dataset a l'experience;
  - le lien Labguru manuel reste separe de cette ingestion.
- Prevoir les champs canoniques suivants dans `metadata_json`:
  - `acquisition`
  - `instrument`
  - `sample`
  - `operator`
  - `timestamps`
  - `positions`
  - `channels`
  - `objective`
  - `exposure`
  - `pixel_size_um`
  - `source_system='pymmcore-plus'`
- Ne pas implementer encore l'endpoint d'ingestion `pymmcore-plus` dans la V1.

### 5. Compatibilite future eLabFTW

- Garder l'abstraction par `system_key`; ne rien coder en dur qui empecherait `system_key='elabftw'`.
- Toute logique API doit rester generique au sens "publication externe", meme si seule l'entree Labguru est exposee dans l'UI V1.
- Les futurs systemes externes devront se rattacher au meme `experiment_project` canonique.

## Interfaces et types publics

- `ExperimentProjectDetail.publication_records` reste la source complete des liens externes d'une experience.
- Ajouter un schema d'update simple pour lien externe manuel, par exemple `ExperimentPublicationLinkUpdate`.
- `ExperimentPublicationLinkUpdate` contient `external_url: str | null`.
- Ajouter un schema de synthese reutilisable, par exemple `ExternalLinkSummary`.
- `ExternalLinkSummary` contient au minimum `system_key`, `status` et `external_url`.
- `ProjectDetail` expose le lien externe herite via un petit objet d'experience liee ou une liste `experiment_publications`.
- `RawDatasetDetail` expose les experiences liees et leurs liens externes.
- La V1 expose seulement `labguru` dans l'UI d'edition.
- Les reponses API peuvent transporter toute entree de `publication_records` pour rester compatibles avec eLabFTW.

## Tests et scenarios

- Creer une experience puis ajouter un lien Labguru manuel.
- Modifier un lien Labguru existant.
- Supprimer logiquement un lien Labguru.
- Lire une experience avec ses `publication_records`.
- Lire un projet lie a une experience portant un lien Labguru.
- Lire un raw dataset lie a une ou plusieurs experiences avec et sans lien Labguru.
- Verifier qu'un utilisateur non autorise ne peut pas modifier le lien externe d'une experience.
- Verifier qu'un utilisateur autorise peut voir le lien si l'experience est lisible.
- Verifier l'affichage UI `Linked` / `Unlinked`.
- Verifier le clic vers l'URL Labguru.
- Verifier l'edition depuis la vue experience.
- Verifier l'affichage herite dans la vue projet.
- Verifier l'affichage herite dans la vue raw dataset avec plusieurs experiences.
- Verifier que la creation d'experience continue d'initialiser ses `publication_records`.
- Verifier que l'absence de lien Labguru ne casse ni les listes ni les details existants.

## Hypotheses et choix retenus

- Le besoin V1 est uniquement un lien URL saisi manuellement.
- Aucune API Labguru n'est appelee en V1.
- Le lien externe est unique et canonique au niveau `experiment_project`.
- Les projets DetecDiv et raw datasets ne possedent pas leur propre lien Labguru.
- Les futures metadonnees `pymmcore-plus` viendront depuis la machine d'acquisition vers le hub par API.
- La V1 prepare eLabFTW par generalisation du modele, sans implementer encore ses endpoints ni son UI.
