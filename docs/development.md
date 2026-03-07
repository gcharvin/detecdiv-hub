# Development

## Local development on a Windows PC

Recommended setup:

- local clone of this repository
- SSH access to the Linux server
- Samba access to server data if needed
- Python 3.11+

## Suggested workflow

1. edit code locally
2. run API locally if needed
3. open an SSH tunnel to the remote server for API/DB testing
4. push changes
5. pull and restart services on the server

## Useful commands

Start local API:

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -e .[dev]
uvicorn api.app:app --reload --host 127.0.0.1 --port 8000
```

Start worker locally:

```powershell
python worker\run_worker.py
```

Open SSH tunnel:

```powershell
scripts\dev_tunnel.ps1
```

