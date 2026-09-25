# Pilote Windows : accès SSH et stratégie de calcul

Machine prévue : `10.20.11.56`. Cette page prépare son accès distant et
résume la façon dont elle rejoindra les trois workers Linux existants. La
configuration du worker lui-même est détaillée dans [windows_worker.md](windows_worker.md).

## Stratégie du pilote

Le hub (API et PostgreSQL) reste sur `webserver-labo`. Les trois workers Linux
actuels restent sur `detecdiv-server`. On ajoute sur `10.20.11.56` un processus
worker Python **natif Windows** qui lit la même file PostgreSQL et lance le
MATLAB installé localement avec `-batch`. Les pipelines MATLAB pourront appeler
leur environnement Python installé sur ce PC.

Pour le premier essai :

1. Créer dans le hub une cible dédiée, par exemple
   `windows-10-20-11-56`, limitée à un job à la fois.
2. N'envoyer à cette cible que des jobs `pipeline_run` qui la désignent
   explicitement. Elle ne prend ni les jobs non attribués, ni les tâches
   périodiques, ni l'ingestion des données brutes.
3. Installer MATLAB, DetecDiv, Python et les dépendances des pipelines sur le
   PC. Donner au compte du worker l'accès aux fichiers d'entrée et de sortie ;
   traduire localement les chemins canoniques `/data/...` vers les partages UNC
   correspondants.
4. Vérifier la licence MATLAB, l'accès aux données et la connexion à PostgreSQL,
   puis lancer un petit pipeline de bout en bout. Activer ensuite la tâche
   planifiée du worker à l'ouverture de session.

SSH sert à administrer `10.20.11.56` à distance. Le worker n'a pas besoin d'un
serveur SSH pour traiter les jobs : **c'est le PC Windows qui doit joindre
PostgreSQL**. L'adresse actuelle de la VM (`192.168.122.185:5432`) peut ne pas
être routable depuis le réseau du PC ; il faudra vérifier ce trajet et, si
nécessaire, mettre en place un tunnel SSH géré via `detecdiv-server`. Le seul
fait de pouvoir se connecter en SSH *vers* Windows ne résout pas ce trajet.

## Installer le serveur SSH sur `10.20.11.56`

Sur **ce PC**, ouvrir PowerShell avec « Exécuter en tant qu'administrateur ».
Windows 10 (à partir de 1809), Windows 11 et Windows Server récents fournissent
OpenSSH Server comme fonctionnalité Windows. Vérifier d'abord son état :

```powershell
Get-WindowsCapability -Online | Where-Object Name -like 'OpenSSH*'
```

Si `OpenSSH.Server~~~~0.0.1.0` est `NotPresent`, l'installer. Si son état est
déjà `Installed`, passer directement au démarrage :

```powershell
Add-WindowsCapability -Online -Name OpenSSH.Server~~~~0.0.1.0
Start-Service sshd
Set-Service -Name sshd -StartupType Automatic
```

L'installation crée normalement une règle entrante pour TCP 22. Vérifier la
règle et ne la créer que si elle manque :

```powershell
if (!(Get-NetFirewallRule -Name 'OpenSSH-Server-In-TCP' -ErrorAction SilentlyContinue)) {
    New-NetFirewallRule -Name 'OpenSSH-Server-In-TCP' -DisplayName 'OpenSSH Server (sshd)' -Enabled True -Direction Inbound -Protocol TCP -Action Allow -LocalPort 22
}
Get-Service sshd
Get-NetFirewallRule -Name 'OpenSSH-Server-In-TCP'
```

Le service doit être `Running`. Si `Add-WindowsCapability` réclame un
redémarrage, redémarrer le PC puis refaire la vérification. Le pare-feu du
réseau du laboratoire doit également permettre l'accès TCP 22 depuis le poste
d'administration ; la règle Windows seule ne garantit pas ce trajet.

Ces commandes suivent la [procédure Microsoft OpenSSH Server](https://learn.microsoft.com/en-us/windows-server/administration/openssh/openssh_install_firstuse).

## Vérifier depuis le poste d'administration

Depuis un autre PC Windows doté du client OpenSSH, remplacer `<compte>` par le
nom du compte Windows autorisé sur `10.20.11.56` :

```powershell
Test-NetConnection 10.20.11.56 -Port 22
& "$env:WINDIR\System32\OpenSSH\ssh.exe" <compte>@10.20.11.56
```

`TcpTestSucceeded` doit valoir `True`. Pour un compte de domaine, utiliser la
forme `domaine\compte@10.20.11.56`. Lors de la première connexion, contrôler
l'empreinte du serveur avant de l'accepter, puis saisir le mot de passe du
compte. Noter le nom du compte qui servira ensuite à installer le worker ; ne
pas enregistrer son mot de passe dans le dépôt.

### Clé SSH, après la première connexion (facultatif)

Une connexion par clé évite de saisir le mot de passe à chaque intervention.
Générer la paire **sur le poste d'administration** (si une paire convenable
n'existe pas déjà), puis copier uniquement le contenu du fichier `.pub` sur le
PC cible :

```powershell
ssh-keygen -t ed25519
Get-Content "$env:USERPROFILE\.ssh\id_ed25519.pub"
```

Pour un compte Windows **administrateur**, coller cette ligne dans
`C:\ProgramData\ssh\administrators_authorized_keys` sur `10.20.11.56`, puis
restreindre les droits dans un PowerShell administrateur sur ce PC :

```powershell
$keyFile = "$env:ProgramData\ssh\administrators_authorized_keys"
notepad.exe $keyFile
icacls.exe $keyFile /inheritance:r /grant '*S-1-5-32-544:F' /grant 'SYSTEM:F'
```

Pour un compte **non administrateur**, utiliser plutôt
`%USERPROFILE%\.ssh\authorized_keys` dans son profil. La clé privée reste sur
le poste d'administration. Microsoft décrit les emplacements et ACL dans sa
[documentation sur l'authentification par clé](https://learn.microsoft.com/en-us/windows-server/administration/openssh/openssh_keymanagement).

Une session SSH Windows ouverte avec une clé peut ne pas avoir les identifiants
nécessaires pour accéder à un partage réseau. Tester les accès aux partages
avec le **compte et le mode de lancement réels du worker** (tâche planifiée à
l'ouverture de session pour ce pilote), et non uniquement depuis SSH.

## Suite une fois SSH disponible

Relever le nom du compte Windows, les chemins locaux de MATLAB et DetecDiv, le
chemin UNC des données, la version de Python et la route possible vers
PostgreSQL. Suivre ensuite [l'installation du worker Windows](windows_worker.md) :
configurer sa cible et son `.env`, exécuter `run_worker.ps1 -Check`, puis lancer
un job `pipeline_run` explicite avant l'activation de la tâche planifiée.

Ne mettre dans Git ni mot de passe PostgreSQL, ni clé SSH privée, ni `.env`.
