import sys,json
from pathlib import Path
OUT=Path(__file__).resolve().parent
sys.path.insert(0,str(OUT.parent/'job-force-stop-deployment-20261020'))
import ops
ops.OUT=OUT
