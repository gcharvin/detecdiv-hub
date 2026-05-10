# Backup & Restore

DetecDiv Hub intègre un système de backup basé sur [restic](https://restic.net/),
accessible depuis l'interface web sans intervention en ligne de commande.

## Vue d'ensemble

```
UI (projet / raw dataset)
        │  Backup now / scheduler automatique
        ▼
   Job "backup_project" ou "backup_raw_dataset"
        │  exécuté par un worker sur detecdiv-server
        ▼
   restic backup → /archive/detecdiv-backup   (repo restic)
        │  résultat stocké dans backup_snapshots (DB)
        ▼
   Snapshot visible dans l'UI → Browse / Full restore / Restore sélectif
```

## Prérequis (ops)

| Composant | Emplacement | Notes |
|-----------|------------|-------|
| Repo restic | `/archive/detecdiv-backup` | créé automatiquement au 1er backup |
| FUSE mount | `/mnt/restic-mount` | nécessaire pour le file browser |
| Service systemd | `detecdiv-restic-mount.service` sur `detecdiv-server` | `--allow-other` requis |

Pour vérifier que le mount est actif :

```bash
systemctl status detecdiv-restic-mount.service
ls /mnt/restic-mount/ids/
```

## Configuration

Dans **Admin → Backup** :

| Paramètre | Description |
|-----------|-------------|
| Repo path | Chemin du repo restic (ex. `/archive/detecdiv-backup`) |
| Passphrase | Clé de chiffrement restic |
| Mount path | Point de montage FUSE (ex. `/mnt/restic-mount`) |
| Enabled | Active le scheduler périodique |
| Intervals | Fréquence de backup (raw datasets / projets, en minutes) |

> **Note** : La configuration est stockée dans `system_settings` dans chaque base PostgreSQL.
> webserver-labo et detecdiv-server ont des bases **indépendantes** — toute modification
> via l'UI doit être répliquée manuellement sur detecdiv-server si nécessaire.

## Ce qui est sauvegardé

### Projets MATLAB

Le scope du backup dépend du type de projet :

**Projet "modern"** — le `.mat` et son répertoire adjacent portent le même nom :
```
/data/user/projects/
    demo_project.mat        ← sauvegardé
    demo_project/           ← sauvegardé (résultats, classification, etc.)
    autre_projet.mat        ← ignoré
    autre_projet/           ← ignoré
```

**Projet "legacy"** — le `.mat` est à la racine du répertoire projet, avec des sous-dossiers `posN` :
```
/data/user/recircu_SCL1_...-project.mat    ← sauvegardé
/data/user/recircu_SCL1_...-ID.txt         ← sauvegardé
/data/user/recircu_SCL1_...-ActionLog.txt  ← sauvegardé
/data/user/recircu_SCL1_...-pos8/          ← ignoré (raw dataset, sauvegardé séparément)
/data/user/recircu_SCL1_...-pos9/          ← ignoré
```

### Raw datasets

Le dossier complet de l'acquisition est sauvegardé tel quel.

## Snapshots

Après chaque backup réussi, le worker enregistre un snapshot dans la table
`backup_snapshots` (avec lien vers le projet ou raw dataset concerné).
L'UI affiche ces snapshots sans appeler restic directement.

Chaque snapshot affiche :
- Date/heure et ID court (8 caractères)
- Chemin source sauvegardé

## Restauration

### Full restore

Restaure l'intégralité du snapshot. Le champ **target directory** détermine où
les fichiers sont écrits :

- **`/`** — restaure en place (recrée le chemin d'origine, **écrase** les fichiers existants)
- `/tmp/restore-test` — restaure dans un répertoire temporaire pour vérification

restic recrée le chemin absolu complet sous le target : si le fichier original
était `/data/user/projects/demo_project.mat`, il apparaîtra à
`{target}/data/user/projects/demo_project.mat`.

### Restore sélectif (file browser)

1. Cliquer **Browse** sur un snapshot
2. Naviguer dans l'arborescence (bouton **↑ Up** pour remonter)
3. Cocher les fichiers ou dossiers à restaurer
4. Saisir le target directory (`/` pour en place) et cliquer **Restore selected**

Le file browser fonctionne via le FUSE mount : le worker fait un `os.listdir()`
sur le répertoire correspondant dans `/mnt/restic-mount/ids/{snapshot_id}/`.
Chaque navigation soumet un job au worker et poll le résultat (~1–2 s).

## Scheduler automatique

Quand `backup_enabled = true`, le scheduler tourne sur detecdiv-server et
déclenche des backups périodiques selon les intervalles configurés.
Les projets et raw datasets avec `backup_excluded = true` sont ignorés.

## Opérations manuelles (restic CLI)

```bash
# Lister les snapshots
restic -r /archive/detecdiv-backup snapshots

# Vérifier l'intégrité du repo
restic -r /archive/detecdiv-backup check

# Pruning (garder les 10 derniers snapshots par tag)
restic -r /archive/detecdiv-backup forget --prune --keep-last 10

# Monter manuellement pour inspecter
restic -r /archive/detecdiv-backup mount /mnt/restic-mount
```

La passphrase est dans la colonne `value_json` de `system_settings` (clé `backup_settings`).
