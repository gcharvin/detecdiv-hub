import base64
import json
from common import ops, OLD

runtime=json.loads((OLD/'fixture-runtime.json').read_text())
folder=ops.LINUX_ROOT+'/.deploy-staging/alex-force-stop-demo-20261020/fixture'
child='''import os,pathlib,subprocess,sys,time
folder=pathlib.Path(sys.argv[1]);role=int(sys.argv[2])
if role==0:
 subprocess.Popen([sys.executable,__file__,str(folder),'1'],start_new_session=True)
p=folder/('python-'+str(role)+'.tmp');p.write_text(str(os.getpid()));p.replace(folder/('python-'+str(role)+'.pid'))
time.sleep(1800)
'''
module=f'''function ctx = forceStopAlexDemo(ctx)
folder='{folder}';
py='{ops.LINUX_ROOT}/.venv/bin/python';
pb=java.lang.ProcessBuilder({{py,fullfile(folder,'child_tree.py'),folder,'0'}});
pb.start();
deadline=posixtime(datetime('now','TimeZone','UTC'))+1800;
ready=struct('matlab_pid',feature('getpid'),'auto_finish_unix',deadline);
fid=fopen(fullfile(folder,'ready.json'),'w');fprintf(fid,'%s',jsonencode(ready));fclose(fid);
fprintf('DEMO READY: select this run, click Force stop, and confirm. No experimental data is being processed.\\n');
fprintf('This demonstration finishes automatically after 30 minutes if it is not stopped.\\n');
pause(1800);
fprintf('Demo completed automatically.\\n');
end
'''
prepare=f'''cd('{runtime['release']}');detecdiv_setup_path(pwd,'DetecDivRoot',pwd);maxNumCompThreads(1);
root='{folder}';
project=shallow();project.fov=fov.empty;
project.io=struct('path',root,'file','alex_force_stop_demo');
projectFile=fullfile(root,'alex_force_stop_demo.json');shallowProjectExportLight(project,projectFile);
pipe=pipeline(root,'force_stop_demo',1);
p=customModule.setparam();p.entryPoint='forceStopAlexDemo';p.codeFolder=fullfile(root,'code');
pipe.nodes=struct('id','force_stop_demo','type','custom','pkg','customModule','func','customModule.process','params',p,'enabled',true);
pipelineSave(pipe,'Artifacts',false);
paths=struct('project_json',projectFile,'pipeline_json',fullfile(pipe.path,'pipeline.json'));
fid=fopen(fullfile(root,'paths.json'),'w');fprintf(fid,'%s',jsonencode(paths));fclose(fid);
disp('ALEX_DEMO_FIXTURE_READY');
'''
payload=base64.b64encode(json.dumps({'child_tree.py':child,'code/forceStopAlexDemo.m':module,'prepare.m':prepare}).encode()).decode()
v=ops.remote('linux',f'''
import base64,json,subprocess
from pathlib import Path
p=Path({folder!r});p.mkdir(parents=True,exist_ok=True)
assert not (p/'paths.json').exists(),'Fixture already prepared; inspect before retry'
assert Path({runtime['release']!r}).is_dir()
for name,data in json.loads(base64.b64decode({payload!r})).items():
 dest=p/name;dest.parent.mkdir(parents=True,exist_ok=True);dest.write_text(data,encoding='utf-8')
with (p/'prepare.log').open('w') as log:
 r=subprocess.run([{runtime['matlab_command']!r},'-batch',"run('"+str(p/'prepare.m')+"')"],stdout=log,stderr=subprocess.STDOUT,timeout=150)
assert r.returncode==0,(p/'prepare.log').read_text()[-3500:]
print(json.dumps(json.loads((p/'paths.json').read_text())))
''',timeout=180)
ops.save('fixture-paths.json',v)
print(v)
