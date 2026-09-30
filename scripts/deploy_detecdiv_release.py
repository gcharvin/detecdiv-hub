"""Publish one DetecDiv commit after preparing protected worktrees on both workers.

Run from the administration workstation. This changes only the default version
for future jobs; existing job pins and shared source checkout HEADs stay put.
"""

import argparse
import base64
import json
import os
from pathlib import Path
import re
import shlex
import subprocess
import sys


HUB_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG = HUB_ROOT / "ops" / "detecdiv_release_targets.json"
SHA_PATTERN = re.compile(r"[0-9a-fA-F]{40}\Z")
SSH = Path(os.environ.get("WINDIR", r"C:\Windows")) / "System32" / "OpenSSH" / "ssh.exe"


def run(args, *, timeout=180, cwd=None, env=None):
    result = subprocess.run(args, cwd=cwd, env=env, capture_output=True,
                            text=True, timeout=timeout)
    if result.returncode:
        detail = (result.stderr or result.stdout).strip()
        raise RuntimeError(f"{args[0]} failed (exit {result.returncode}): {detail}")
    return result.stdout.strip()


def git(repo, *args):
    env = {**os.environ, "GIT_TERMINAL_PROMPT": "0", "GCM_INTERACTIVE": "never",
           "GIT_SSH_COMMAND": str(SSH)}
    return run(["git", "-C", str(repo), *args], timeout=180, env=env)


def is_ancestor(repo, older, newer):
    result = subprocess.run(["git", "-C", str(repo), "merge-base", "--is-ancestor", older, newer],
                            capture_output=True, text=True, timeout=30)
    if result.returncode not in (0, 1):
        raise RuntimeError(result.stderr.strip() or "Git ancestry check failed")
    return result.returncode == 0


def ssh_args(target):
    args = [str(SSH), "-o", "BatchMode=yes", "-o", "ConnectTimeout=15"]
    if target.get("ssh_key"):
        args += ["-o", "IdentitiesOnly=yes", "-i", str(Path(target["ssh_key"]).expanduser())]
    if target.get("ssh_user"):
        args += ["-l", target["ssh_user"]]
    return args + [target["ssh_host"]]


def remote_json(target, command, *, timeout=360):
    output = run([*ssh_args(target), command], timeout=timeout)
    try:
        return json.loads(output.splitlines()[-1])
    except (IndexError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"Unexpected response from {target['ssh_host']}: {output[-500:]}") from exc


def api_code(config, code):
    encoded = base64.b64encode(code.encode("utf-8")).decode("ascii")
    container = shlex.quote(config["api"]["container"])
    command = (f"docker exec {container} python -c "
               f"'import base64;exec(base64.b64decode(\"{encoded}\"))'")
    return remote_json(config["api"], command, timeout=90)


def api_snapshot(config, publisher):
    code = f'''
import json
from sqlalchemy import select
from api.db import SessionLocal
from api.models import ExecutionTarget, User
from api.services.matlab_code_versions import default_commit
with SessionLocal() as session:
    user = session.scalars(select(User).where(User.user_key == {publisher!r})).one_or_none()
    keys = {config['api']['target_keys']!r}
    targets = session.scalars(select(ExecutionTarget).where(ExecutionTarget.target_key.in_(keys))).all()
    print(json.dumps({{'release': default_commit(session),
        'publisher_ok': bool(user and user.is_active and user.role in ('admin', 'service')),
        'targets': {{target.target_key: {{'status': target.status,
            'ready': (target.metadata_json or {{}}).get('matlab_code_isolation_ready') is True}}
            for target in targets}}}}))
'''
    return api_code(config, code)


def require_ready(snapshot, config):
    if not snapshot["publisher_ok"]:
        raise RuntimeError("Configured publisher is not an active Hub admin/service user")
    for key in config["api"]["target_keys"]:
        target = snapshot["targets"].get(key)
        if not target or target["status"] != "online" or not target["ready"]:
            raise RuntimeError(f"Target {key} is not online with MATLAB code isolation ready")


def prepare_linux(config, commit):
    target = config["linux"]
    command = (f"cd {shlex.quote(target['hub_root'])} && "
               f".venv/bin/python -m worker.matlab_code_checkout "
               f"--repo-root {shlex.quote(target['detecdiv_root'])} --commit {commit}")
    return remote_json(target, command, timeout=600)


def prepare_windows(config, commit):
    target = config["windows"]
    # Paths are controlled by the local deployment config, never by a job payload.
    for field in ("hub_root", "detecdiv_root"):
        if any(char in target[field] for char in '&|<>"\r\n'):
            raise ValueError(f"Unsupported Windows path in {field}")
    command = (f'cd /d "{target["hub_root"]}" && '
               f'".venv\\Scripts\\python.exe" -m worker.matlab_code_checkout '
               f'--repo-root "{target["detecdiv_root"]}" --commit {commit}')
    return remote_json(target, command, timeout=900)


def verify_prepared(label, result, commit):
    if result.get("code_commit") != commit or not result.get("repo_root"):
        raise RuntimeError(f"{label} did not verify the requested release: {result}")
    path = result["repo_root"].replace("\\", "/").lower()
    if not path.endswith("/detecdiv-jobs/releases/" + commit):
        raise RuntimeError(f"{label} returned an unexpected release path: {result['repo_root']}")


def publish(config, commit, previous, publisher):
    code = f'''
import json
from sqlalchemy import select
from api.db import SessionLocal
from api.models import ExecutionTarget, SystemSetting, User
from api.routes_matlab_code import MatlabCodeRelease, publish_release
from api.services.matlab_code_versions import SETTING_KEY, default_commit
with SessionLocal() as session:
    session.get(SystemSetting, SETTING_KEY, with_for_update=True)
    current = default_commit(session)
    if current != {previous!r}:
        raise RuntimeError('Published release changed during preparation; aborting')
    user = session.scalars(select(User).where(User.user_key == {publisher!r})).one()
    if not user.is_active or user.role not in ('admin', 'service'):
        raise RuntimeError('Publisher is no longer an active admin/service user')
    keys = {config['api']['target_keys']!r}
    targets = session.scalars(select(ExecutionTarget).where(ExecutionTarget.target_key.in_(keys))).all()
    ready = {{t.target_key for t in targets if t.status == 'online' and
        (t.metadata_json or {{}}).get('matlab_code_isolation_ready') is True}}
    if set(keys) != ready:
        raise RuntimeError('A compute target lost MATLAB code isolation readiness')
    result = publish_release(MatlabCodeRelease(code_commit={commit!r}), session, user)
    print(json.dumps(result))
'''
    return api_code(config, code)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("commit", help="full 40-character DetecDiv commit SHA")
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--publisher", help="Hub admin/service user key; defaults to config")
    args = parser.parse_args()
    if not SHA_PATTERN.fullmatch(args.commit):
        parser.error("commit must be a full 40-character Git SHA")
    commit = args.commit.lower()
    config = json.loads(args.config.read_text(encoding="utf-8"))
    publisher = args.publisher or config["publisher"]
    source = Path(config["source_repo"])
    if not source.is_absolute():
        source = (HUB_ROOT / source).resolve()
    if not SSH.is_file():
        raise RuntimeError(f"Windows OpenSSH client not found: {SSH}")
    if git(source, "cat-file", "-t", commit) != "commit":
        raise RuntimeError(f"{commit} is not a commit object in {source}")
    if not is_ancestor(source, commit, config["branch"]):
        raise RuntimeError(f"{commit} is not on local {config['branch']}")

    before = api_snapshot(config, publisher)
    require_ready(before, config)
    previous = before["release"]
    print(f"Preparing DetecDiv {commit}; current Hub default {previous}", flush=True)

    for remote in config["push_remotes"]:
        git(source, "fetch", "--no-tags", remote, f"refs/heads/{config['branch']}")
        remote_head = git(source, "rev-parse", "FETCH_HEAD")
        if is_ancestor(source, commit, remote_head):
            print(f"{remote}: commit already published", flush=True)
            continue
        if not is_ancestor(source, remote_head, commit):
            raise RuntimeError(f"{remote}/{config['branch']} diverged from {commit}")
        git(source, "push", remote, f"{commit}:refs/heads/{config['branch']}")
        print(f"{remote}: pushed through {commit}", flush=True)

    linux = prepare_linux(config, commit)
    verify_prepared("Linux", linux, commit)
    print(f"Linux release: {linux['repo_root']}", flush=True)
    windows = prepare_windows(config, commit)
    verify_prepared("Windows", windows, commit)
    print(f"Windows release: {windows['repo_root']}", flush=True)

    if previous != commit:
        result = publish(config, commit, previous, publisher)
        if result.get("code_commit") != commit:
            raise RuntimeError(f"Hub did not confirm publication: {result}")
    after = api_snapshot(config, publisher)
    if after["release"] != commit:
        raise RuntimeError(f"Hub reports {after['release']} instead of {commit}")
    print(f"Published default release: {commit}", flush=True)
    print("Existing job pins are unchanged; no worker restart was requested.", flush=True)


if __name__ == "__main__":
    try:
        main()
    except (OSError, RuntimeError, ValueError, KeyError, subprocess.TimeoutExpired) as exc:
        print(f"Deployment stopped: {exc}", file=sys.stderr)
        raise SystemExit(1)
