import json,hashlib
from pathlib import Path
from sqlalchemy import select
from api.db import SessionLocal
from api.models import ExecutionTarget
from api.services.matlab_code_versions import default_commit
with SessionLocal() as s:
 print(json.dumps({'default':default_commit(s),'windows':[{ 'supports_gpu':t.supports_gpu,'budget':t.metadata_json.get('gpu_vram_capacity_mb'),'health':t.metadata_json.get('worker_health')} for t in s.scalars(select(ExecutionTarget).where(ExecutionTarget.target_key=='windows-10-20-11-56'))]}))
