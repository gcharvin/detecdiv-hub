from pathlib import Path
import json
r=Path('/data/Gilles/analysis/classifiers/phase_cellposesam_v001_1')
print((r/'code/classifier/+phaseCellposeSAM/train.m').read_text())
print((r/'pipeline_templates/train_phase_cellposesam_v001/pipeline.json').read_text())
print('started', (r/'fit_v001/training_started.json').exists())
