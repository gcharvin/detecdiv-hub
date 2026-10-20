from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
OUT = Path(__file__).resolve().parent
OLD = ROOT / "reports/job-force-stop-deployment-20261020"
sys.path.insert(0, str(OLD))
import ops

ops.OUT = OUT
