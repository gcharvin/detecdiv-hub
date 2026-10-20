"""Synthetic GPU smoke test; never trains or writes classifier outputs."""
from pathlib import Path
import hashlib
import importlib.util
import importlib.metadata
import json
import os
import subprocess
import time
import traceback

stage=Path('C:/ProgramData/DetecDivHub/gpu-validation-20261016')
result={'status':'running','pid':os.getpid(),'account':subprocess.check_output(['whoami'],text=True).strip()}
try:
    import h5py
    import numpy as np
    import torch
    from cellpose import models,train,io,metrics
    result['versions']={k:importlib.metadata.version(k) for k in ['cellpose','torch','torchvision','numpy','h5py','scipy']}
    result['cuda']=torch.version.cuda
    result['device']=torch.cuda.get_device_name()
    assert torch.cuda.is_available()
    x=torch.arange(16.,device='cuda',requires_grad=True)
    loss=(x*x).sum()
    loss.backward()
    torch.cuda.synchronize()
    assert loss.item()==1240 and torch.equal(x.grad,2*x)
    result['cuda_forward_backward']='passed'
    root=Path('//10.20.11.250/DATA/Gilles/analysis/classifiers/phase_cellposesam_v001_1')
    release=root/'code/releases/45f68ffc19e6faee2d727506a6bd4fa3b41f3a82/classifier/+phaseCellposeSAM'
    protocol=json.loads((root/'protocol_frozen.json').read_text())
    spec=importlib.util.spec_from_file_location('gpu_validation_pilot',release/'py/pilot.py')
    pilot=importlib.util.module_from_spec(spec)
    spec.loader.exec_module(pilot)
    rows=pilot.verify_inputs(root,protocol)
    result['verified_dataset_frames']=len(rows)
    result['protocol_sha256']=pilot.sha(root/'protocol_frozen.json')
    result['training_started_before']=(root/'fit_v001/training_started.json').exists()
    assert not result['training_started_before']
    probe=root/'pipeline_runs/train_phase_cellposesam_v001/windows_gpu_write_probe.tmp'
    assert not probe.exists()
    try:
        with probe.open('x') as f: f.write('temporary GPU worker permission check')
        assert probe.read_text()=='temporary GPU worker permission check'
    finally:
        if probe.exists(): probe.unlink()
    result['shared_output_write']='passed'
    weights=root/protocol['baselines']['server_20260929']['file']
    model=models.CellposeModel(gpu=True,pretrained_model=str(weights))
    masks,flows,styles=model.eval(np.zeros((256,256,3),dtype=np.uint8),diameter=20)
    torch.cuda.synchronize()
    assert masks.shape==(256,256)
    result['cellposesam_gpu_inference']='passed'
    result['peak_cuda_allocated_mib']=torch.cuda.max_memory_allocated()//(1024**2)
    result['training_started_after']=(root/'fit_v001/training_started.json').exists()
    assert not result['training_started_after']
    result['status']='passed'
except BaseException:
    result['status']='failed'
    result['traceback']=traceback.format_exc()
finally:
    (stage/'gpu-smoke-result.json').write_text(json.dumps(result,indent=2)+'\n')
    print(json.dumps(result),flush=True)
if result['status']!='passed': raise SystemExit(1)
