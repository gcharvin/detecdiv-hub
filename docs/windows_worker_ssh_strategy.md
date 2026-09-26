# Pilote Windows : accès SSH et stratégie de calcul

Machine prévue : `10.20.11.56`. Cette page prépare son accès distant et
résume la façon dont elle rejoindra les trois workers Linux existants. La
configuration du worker lui-même est détaillée dans [windows_worker.md](windows_worker.md).

## Stratégie du pilote

Le PC possède une cible dédiée `windows-10-20-11-56` et commence avec un seul
worker process. Le worker Python natif Windows interroge la même base PostgreSQL
que les workers Linux ; il lance le MATLAB local avec `-batch`, et les pipelines
peuvent appeler un environnement Python installé/configuré sur le PC.

La configuration de queue actuelle autorise les jobs non attribués et tous les
types sauf `archive_raw_dataset`, `restore_raw_dataset`, `pipeline_run` et
`legacy_matlab`. Les deux types MATLAB resteront exclus jusqu'à réparation et
validation de la licence. Les schedulers périodiques restent désactivés. Seuls
les jobs avec `execution_target_id=NULL` sont partagés : un job déjà attribué à
`detecdiv-server` n'est pas déplacé. Parmi les workers éligibles, le premier à
réserver le job le prend ; ce n'est pas un équilibrage de charge entre machines.

Ne pas autoriser sur Windows l'archivage/restauration ou l'ingestion brute tant
que les chemins, les permissions SMB et les effets sur les données n'ont pas
été testés sur cette machine. L'archivage peut supprimer les sources chaudes.

Le serveur SSH entrant sert uniquement à administrer le PC. Le worker n'en a
pas besoin pour traiter les jobs. **C'est le PC Windows qui doit joindre
PostgreSQL**. Depuis ce réseau, l'accès direct à `192.168.122.185:5432` a échoué ;
le pilote utilise donc un tunnel sortant par `detecdiv-server` vers le hub. Le
tunnel et la clé restreinte sont indépendants de la clé SSH entrante utilisée
pour administrer Windows.

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

Depuis un autre PC Windows doté du client OpenSSH, remplacer le compte si
nécessaire. Pour ce compte de domaine, cette forme a fonctionné :

```powershell
Test-NetConnection 10.20.11.56 -Port 22
& "$env:WINDIR\System32\OpenSSH\ssh.exe" -l 'GMGM\Charvin-Admin' 10.20.11.56
```

`TcpTestSucceeded` doit valoir `True`. `Test-Connection` teste ICMP et n'accepte
pas `-Port`; utiliser `Test-NetConnection` pour vérifier TCP 22. Lors de la
première connexion, contrôler l'empreinte du serveur avant de l'accepter, puis
saisir le mot de passe du compte si demandé. Ne jamais enregistrer le mot de
passe dans le dépôt.

### Clé SSH, après la première connexion (facultatif)

Une connexion par clé évite de saisir le mot de passe à chaque intervention.
Générer une paire dédiée **sur le poste d'administration** (si elle n'existe
pas déjà), puis transmettre uniquement le contenu du fichier `.pub` au PC
cible. Ne jamais écraser une clé existante :

```powershell
$key = Join-Path $env:USERPROFILE '.ssh\id_ed25519_detecdiv_windows'
if (Test-Path -LiteralPath $key) { throw "La clé existe déjà : $key" }
ssh-keygen -t ed25519 -f $key -C 'detecdiv-windows-10.20.11.56'
Get-Content "$key.pub"
& "$env:WINDIR\System32\OpenSSH\ssh-keygen.exe" -lf "$key.pub"
```

Windows OpenSSH utilise normalement
`C:\ProgramData\ssh\administrators_authorized_keys` pour les comptes du groupe
Administrators, mais un bloc `Match` peut remplacer ce chemin. **Toujours lire
la configuration effective avec `sshd -T -C` avant de choisir le fichier.**
Sur le premier PC, une règle `Match User` a été utilisée pour diriger le compte
admin vers `%USERPROFILE%\.ssh\authorized_keys`; c'est ce fichier qui a ensuite
été accepté.

Pour le chemin standard réservé aux administrateurs, ajouter la ligne publique
dans le fichier et restreindre les ACL depuis un PowerShell administrateur :

```powershell
$keyFile = "$env:ProgramData\ssh\administrators_authorized_keys"
New-Item -ItemType Directory -Path (Split-Path -Parent $keyFile) -Force | Out-Null
if (-not (Test-Path -LiteralPath $keyFile)) { New-Item -ItemType File -Path $keyFile | Out-Null }
$key = 'ssh-ed25519 AAAA... commentaire-du-poste-client'
if (@(Get-Content -LiteralPath $keyFile) -notcontains $key) {
    Add-Content -LiteralPath $keyFile -Value $key -Encoding Ascii
}
icacls.exe $keyFile /inheritance:r /grant:r '*S-1-5-32-544:F' '*S-1-5-18:F'
```

Pour le fichier par utilisateur utilisé sur le premier PC, créer le dossier et
le fichier dans le profil du compte SSH, y ajouter uniquement la ligne `.pub`,
puis appliquer des ACL explicites :

```powershell
$key = 'ssh-ed25519 AAAA... commentaire-du-poste-client'
$dir = Join-Path $env:USERPROFILE '.ssh'
$file = Join-Path $dir 'authorized_keys'
New-Item -ItemType Directory -Path $dir -Force | Out-Null
if (-not (Test-Path -LiteralPath $file)) { New-Item -ItemType File -Path $file | Out-Null }
if (@(Get-Content -LiteralPath $file) -notcontains $key) {
    Add-Content -LiteralPath $file -Value $key -Encoding Ascii
}
icacls.exe $file /inheritance:r /grant:r 'GMGM\Charvin-Admin:F' '*S-1-5-18:F' '*S-1-5-32-544:F'
```

Dans les deux exemples, remplacer le placeholder par la ligne publique réelle ;
ne jamais copier la clé privée. Le chemin retenu doit correspondre à
`authorizedkeysfile` dans la
configuration effective. Microsoft décrit les emplacements et ACL dans sa
[documentation sur l'authentification par clé](https://learn.microsoft.com/en-us/windows-server/administration/openssh/openssh_keymanagement).

Une session SSH Windows ouverte avec une clé peut ne pas avoir les identifiants
nécessaires pour accéder à un partage réseau. Tester les accès aux partages
avec le **compte et le mode de lancement réels du worker** (tâche planifiée à
l'ouverture de session pour ce pilote), et non uniquement depuis SSH.

### Diagnostic SSH observé sur le pilote

Sur le poste client, forcer Windows OpenSSH et la clé voulue, puis conserver
`-vvv` lors du diagnostic :

```powershell
$key = Join-Path $env:USERPROFILE '.ssh\id_ed25519_detecdiv_windows'
& "$env:WINDIR\System32\OpenSSH\ssh-keygen.exe" -lf "$key.pub"
& "$env:WINDIR\System32\OpenSSH\ssh.exe" -vvv -o IdentitiesOnly=yes -i $key -l 'GMGM\Charvin-Admin' 10.20.11.56
```

Sur le PC Windows, dans **PowerShell administrateur** après l'essai client :

```powershell
$sshd = Join-Path $env:WINDIR 'System32\OpenSSH\sshd.exe'
Get-Service sshd
& $sshd -t
& $sshd -T -C 'user=GMGM\Charvin-Admin,host=CG-PCDELL01-306,addr=192.168.190.2' |
    Select-String '^(authorizedkeysfile|pubkeyauthentication|passwordauthentication|loglevel)\s'
$userKey = Join-Path $env:USERPROFILE '.ssh\authorized_keys'
$adminKey = Join-Path $env:ProgramData 'ssh\administrators_authorized_keys'
foreach ($keyFile in @($userKey, $adminKey)) {
    if (Test-Path -LiteralPath $keyFile) {
        Get-Item -LiteralPath $keyFile | Select-Object FullName, Length
        icacls.exe $keyFile
        & "$env:WINDIR\System32\OpenSSH\ssh-keygen.exe" -lf $keyFile
    }
}
Get-WinEvent -FilterHashtable @{
    LogName = 'OpenSSH/Operational'
    StartTime = (Get-Date).AddMinutes(-10)
} -ErrorAction SilentlyContinue |
    Select-Object -First 15 TimeCreated, Message |
    Format-List
```

Remplacer `addr` par l'adresse `from ...` du journal si le poste client en a
changé. Interprétation des erreurs vues pendant la mise en place :

- `Invalid user GMGM` : la forme `ssh charvin-admin\@...` a mal découpé le
  compte de domaine. Utiliser `ssh -l 'GMGM\Charvin-Admin' 10.20.11.56`.
- `no matching host key type found` avec des algorithmes `sk-*` : la connexion
  s'arrête pendant la négociation des algorithmes d'hôte, avant la vérification
  de `authorized_keys`. Ce n'est pas un mot de passe ou une ACL de clé refusés.
  Utiliser le client Windows OpenSSH explicite et lire `ssh -vvv` ; ne pas
  affaiblir les algorithmes du serveur à l'aveugle. `-i`/`IdentitiesOnly` sert
  séparément à sélectionner la bonne clé utilisateur après cette négociation.
- `Connection reset` : ce message seul ne prouve pas un échec de clé. Les
  événements `Accepted publickey for GMGM\Charvin-Admin` ont été enregistrés
  alors que le client voyait encore une coupure ; la confirmation la plus
  récente date du 2026-09-26 à 09:04 depuis `192.168.190.2`. Si le client affiche
  `Authenticated ... using "publickey"`, l'authentification est terminée avec
  succès ; diagnostiquer ensuite la session, le shell ou le transport, pas
  `authorized_keys`.
- `Get-Service`, `Get-WinEvent`, `Select-Object` ou `&` non reconnu/inattendu :
  les commandes PowerShell ont été collées dans `cmd.exe`. Taper `powershell`
  pour ouvrir PowerShell, puis lancer les commandes sans les marqueurs `PS>`.
- Le fichier global des administrateurs était initialement confondu avec un
  sous-dossier `administrators\authorized_keys`. Son nom standard est le fichier
  unique `administrators_authorized_keys`. Dans tous les cas, `sshd -T -C`
  donne le chemin effectif à suivre.

Un niveau `LogLevel DEBUG3` a été ajouté temporairement sur ce PC pour le
diagnostic avec `SyslogFacility LOCAL0`. Vérifier le `sshd_config` après
l'incident et retirer ces réglages temporaires s'ils sont encore présents,
sans supprimer le bloc `Match User` qui sélectionne le fichier de clé. Une
copie `.before-debug` avait été faite après la modification du chemin de clé ;
inspecter son contenu avant toute restauration. Exécuter `sshd -t`, puis
redémarrer `sshd` après une modification.

## Suite une fois SSH disponible

Relever le nom du compte Windows, les chemins locaux de MATLAB et DetecDiv, le
chemin UNC des données, la version de Python et la route vers PostgreSQL. Suivre
ensuite [l'installation du worker Windows](windows_worker.md) : configurer sa
cible et son `.env`, lancer `run_worker.ps1 -EnvFile .env -Check`, puis faire un
petit essai compatible avec les capacités et la licence avant d'enregistrer la
tâche planifiée.

Ne mettre dans Git ni mot de passe PostgreSQL, ni clé SSH privée, ni `.env`.
