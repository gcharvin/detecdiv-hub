# Local Client Installation

## Goal

This guide covers the Windows client setup for:

- browsing the hub web UI in a browser
- using the MATLAB catalog browser against the remote hub API
- optionally tunneling the API and PostgreSQL over SSH
- mapping server storage locally through Samba

Assumptions:

- the server installation is already complete
- you have a Windows PC
- you have SSH access to the Linux server
- you can mount the server storage via Samba

## 1. Minimal client setup for the web UI

If you only want the browser UI:

1. open a browser on the PC
2. browse to `http://SERVER_OR_HOSTNAME:8000/web/`
3. log in with your `user_key` and password

If the API is not directly exposed on the LAN, use an SSH tunnel first:

```powershell
ssh -L 8000:127.0.0.1:8000 youruser@yourserver
```

Then browse:

```text
http://127.0.0.1:8000/web/
```

## 2. Optional: tunnel PostgreSQL as well

This is useful for admin/debug tasks, but not required for normal use:

```powershell
ssh -L 8000:127.0.0.1:8000 -L 5432:127.0.0.1:5432 youruser@yourserver
```

## 3. Mount the project storage over Samba

Map the server project root to a local drive letter, for example:

- server canonical root: `/srv/detecdiv/projects`
- Windows local mount: `Z:\detecdiv\projects`

Use File Explorer or:

```powershell
net use Z: \\SERVER\detecdiv-projects /persistent:yes
```

The exact share name depends on the final Samba configuration.

## 4. Install the MATLAB client repositories locally

You need:

- the main DetecDiv repo
- the catalog worktree [DetecDiv-catalog](C:/Users/charvin/Documents/MATLAB/DetecDiv-catalog)

Launch MATLAB on the catalog worktree with:

```matlab
run('C:\Users\charvin\Documents\MATLAB\DetecDiv-catalog\launch_catalog_browser.m')
```

## 5. Configure the MATLAB catalog browser for the hub

In [detecdivCatalogBrowser.m](C:/Users/charvin/Documents/MATLAB/DetecDiv-catalog/structure/GUI/detecdivCatalogBrowser.m):

- choose `Hub API`
- set `Hub URL`
- set `User`
- set `Hub Root`
- set `Local Mount`
- click `Login...` if the hub requires a password-backed session

Typical final values:

- `Hub URL = http://127.0.0.1:8000` when using an SSH tunnel
- `Hub URL = http://SERVER_OR_HOSTNAME:8000` when the API is directly reachable
- `User = your user key`
- `Hub Root = /srv/detecdiv/projects`
- `Local Mount = Z:\detecdiv\projects`

Save the config. The browser will then:

- list projects from the hub
- filter based on your user rights
- remap server paths to the local Samba mount
- load local `shallow` projects through `shallowLoad`
- keep `Local SQLite` available for a purely local catalog when needed

## 6. MATLAB-only config shortcut

You can set the same hub config from MATLAB directly:

```matlab
hub = detecdiv_hub_settings_get();
hub.baseUrl = 'http://127.0.0.1:8000';
hub.userKey = 'localdev';
hub.defaultRemoteProjectRoot = '/srv/detecdiv/projects';
hub.defaultLocalProjectRoot = 'Z:\detecdiv\projects';
hub = detecdiv_hub_upsert_path_mapping(hub, '/srv/detecdiv/projects', 'Z:\detecdiv\projects');
detecdiv_hub_settings_set(hub);
```

Open a hub session explicitly from MATLAB if needed:

```matlab
[sessionInfo, hub] = detecdiv_hub_login('localdev', 'change_me');
```

## 7. Quick client checks

In MATLAB:

```matlab
projects = detecdiv_hub_list_projects();
projectDetail = detecdiv_hub_get_project(projects(1).id);
[projectMatPath, resolutionInfo] = detecdiv_hub_resolve_project_location(projectDetail);
```

And if the mount is correct:

```matlab
[shallowObj, msg] = detecdiv_hub_load_project(projects(1).id);
```

## 8. Recommended operating model

Use the client in two modes:

- browser UI for catalog browsing, groups, notes, sharing, deletion preview
- MATLAB browser when you need to load and inspect the project object locally

The server should remain the canonical place for:

- indexing
- access control
- project deletion
- storage accounting

The client should remain the place for:

- interactive local inspection
- MATLAB-specific workflows
- local path remapping through Samba
