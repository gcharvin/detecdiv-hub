import base64
import ops

# Reuse the verified admission-lock and OS-tree idle guards from the preceding
# rollout. This rollout includes its pending Windows memory files.
source = (ops.ROOT / "reports/roi-auto-memory-deployment-20261019/activate_when_idle.py").read_text()
source = source.replace("roi-auto-memory-20261019", ops.ROLLOUT)
source = source.replace("roi_memory_rollout", "force_stop_rollout")
source = source.replace("detecdiv-roi-memory-activation-20261019", "detecdiv-force-stop-activation-20261020")
source = source.replace("        compile((STAGE / 'files' / name).read_bytes(), name, 'exec')",
                        "        if name.endswith('.py'):\n            compile((STAGE / 'files' / name).read_bytes(), name, 'exec')")
source = source.replace("        temporary = dest.with_suffix('.roi-deploy-tmp')",
                        "        dest.parent.mkdir(parents=True, exist_ok=True)\n        temporary = dest.with_suffix('.force-stop-deploy-tmp')")
start = source.index("            marker = STAGE / 'manager-activation.json'")
end = source.index("        else:\n            actions.append(windows_reload", start)
source = source[:start] + source[end:]
compile(source, "activate_when_idle.py", "exec")
(ops.OUT / "activate_when_idle.py").write_text(source, encoding="utf-8")
payload = base64.b64encode(source.encode()).decode()
for label in ("linux", "windows"):
    print(label, ops.remote(label, f'''
import base64,json
from pathlib import Path
p=Path('.deploy-staging/{ops.ROLLOUT}/activate_when_idle.py')
p.write_bytes(base64.b64decode({payload!r}))
print(json.dumps({{'helper_prepared':True}}))
'''))
