import ops

code = '''
import ast,json,subprocess
from pathlib import Path
tree=ast.parse(Path('.deploy-staging/roi-auto-memory-20261019/activate_when_idle.py').read_text())
function=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='windows_reload')
assignment=next(n for n in function.body if isinstance(n,ast.Assign) and isinstance(n.targets[0],ast.Name) and n.targets[0].id=='ps')
ps=assignment.value.func.value.value.split('Set-Content -LiteralPath')[0].replace('WORKERPID','31820')
assert 'Stop-ScheduledTask' not in ps and 'Stop-Process' not in ps
result=subprocess.run(['powershell.exe','-NoProfile','-NonInteractive','-Command',ps],capture_output=True,text=True)
assert result.returncode and 'Worker has child processes; defer' in result.stderr,result.stderr
print(json.dumps({'busy_tree_guard_verified':True,'worker_pid':31820,'stop_commands_executed':False}))
'''
result = ops.remote('windows', code)
ops.save('windows-busy-tree-guard.json', result)
print(result)
