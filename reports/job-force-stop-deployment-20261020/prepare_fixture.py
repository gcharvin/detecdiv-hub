import base64
import json
import ops

runtime=json.loads((ops.OUT/'fixture-runtime.json').read_text())
fixture=ops.LINUX_ROOT+'/.deploy-staging/'+ops.ROLLOUT+'/smoke'
child='''import os,pathlib,subprocess,sys,time
folder=pathlib.Path(sys.argv[1]);role=int(sys.argv[2])
if role==0:
 subprocess.Popen([sys.executable,__file__,str(folder),'1'],start_new_session=True)
p=folder/('python-'+str(role)+'.tmp');p.write_text(str(os.getpid()));p.replace(folder/('python-'+str(role)+'.pid'))
time.sleep(150)
'''
module=f'''function ctx = forceStopSmoke(ctx)
folder = '{fixture}';
py = '{ops.LINUX_ROOT}/.venv/bin/python';
pb = java.lang.ProcessBuilder({{py, fullfile(folder,'child_tree.py'), folder, '0'}});
pb.start();
fid=fopen(fullfile(folder,'matlab.pid'),'w'); fprintf(fid,'%d',feature('getpid')); fclose(fid);
fprintf('FORCE_STOP_SMOKE_READY: no cooperative cancellation checkpoints inside this module.\\n');
pause(180);
end
'''
prepare=f'''cd('{runtime['release']}'); detecdiv_setup_path(pwd, 'DetecDivRoot', pwd); maxNumCompThreads(1);
root='{fixture}';
project=shallow(); project.fov=fov.empty;
project.io=struct('path',root,'file','empty_smoke_project');
projectFile=fullfile(root,'empty_smoke_project.json');
shallowProjectExportLight(project,projectFile);
pipe=pipeline(root,'force_stop_smoke',1);
p=customModule.setparam();p.entryPoint='forceStopSmoke';p.codeFolder=fullfile(root,'code');
pipe.nodes=struct('id','force_stop_probe','type','custom','pkg','customModule','func','customModule.process','params',p,'enabled',true);
pipelineSave(pipe,'Artifacts',false);
v=struct('project_json',projectFile,'pipeline_json',fullfile(pipe.path,'pipeline.json'));
fid=fopen(fullfile(root,'paths.json'),'w');fprintf(fid,'%s',jsonencode(v));fclose(fid);
disp('FORCE_STOP_FIXTURE_READY');
'''
files={'child_tree.py':child,'code/forceStopSmoke.m':module,'prepare.m':prepare}
payload=base64.b64encode(json.dumps(files).encode()).decode()
result=ops.remote('linux',f'''
import base64,json,subprocess
from pathlib import Path
folder=Path({fixture!r});folder.mkdir(parents=True,exist_ok=True)
assert not (folder/'paths.json').exists(),'Fixture already exists; inspect before rerunning'
for name,data in json.loads(base64.b64decode({payload!r})).items():
 p=folder/name;p.parent.mkdir(parents=True,exist_ok=True);p.write_text(data,encoding='utf-8')
with (folder/'prepare.log').open('w') as log:
 r=subprocess.run([{runtime['matlab_command']!r},'-batch',"run('"+str(folder/'prepare.m')+"')"],stdout=log,stderr=subprocess.STDOUT,timeout=150)
assert r.returncode==0,(folder/'prepare.log').read_text()[-4000:]
print(json.dumps(json.loads((folder/'paths.json').read_text())))
''',timeout=180)
ops.save('fixture-paths.json',result)
print(result)
