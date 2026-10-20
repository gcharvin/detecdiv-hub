from pathlib import Path
r=Path('/data/Gilles/analysis/classifiers/phase_cellposesam_v001_1')
for name in ['pipeline_runs/train_phase_cellposesam_v001/worker_console.log','pilot_python.log','fit_v001/training_started.json']:
 p=r/name
 print(name)
 if p.exists(): print(p.read_text(errors='replace')[-3200:])
