"""Linux fixtures for the exact publication rename primitive; no production paths."""
import importlib.util
import json
import os
from pathlib import Path
import tempfile

HERE=Path(__file__).resolve().parent
spec=importlib.util.spec_from_file_location('publication_under_test',HERE/'publish_year1_folders.py')
module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)

def run():
    passed=[]
    with tempfile.TemporaryDirectory(prefix='cases-',dir=HERE) as temporary:
        root=Path(temporary);fd=os.open(root,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
        try:
            source=root/'.incoming-ok-20261105';source.mkdir();(source/'payload').write_bytes(b'fixture-science-bytes')
            inode=(source/'payload').stat().st_ino
            module.rename_directory_noreplace(fd,source.name,'ok')
            assert not source.exists() and (root/'ok/payload').read_bytes()==b'fixture-science-bytes' and (root/'ok/payload').stat().st_ino==inode
            passed.append('success_preserves_payload_and_inode')
            for kind in ['directory','file','broken_symlink']:
                name='collision_'+kind;source=root/('.incoming-'+name+'-20261105');source.mkdir();(source/'untouched').write_bytes(b'source')
                destination=root/name
                if kind=='directory':destination.mkdir();(destination/'sentinel').write_bytes(b'destination')
                elif kind=='file':destination.write_bytes(b'destination')
                else:destination.symlink_to(root/'never-created')
                try:module.rename_directory_noreplace(fd,source.name,name)
                except OSError:pass
                else:raise AssertionError('Existing destination overwritten')
                assert (source/'untouched').read_bytes()==b'source'
                if kind=='directory':assert (destination/'sentinel').read_bytes()==b'destination'
                elif kind=='file':assert destination.read_bytes()==b'destination'
                else:assert destination.is_symlink()
                passed.append('refuses_existing_'+kind)
            outside=root/'outside';outside.mkdir();(outside/'sentinel').write_bytes(b'outside')
            (root/'.incoming-link-20261105').symlink_to(outside,target_is_directory=True)
            try:module.rename_directory_noreplace(fd,'.incoming-link-20261105','link')
            except ValueError:pass
            else:raise AssertionError('Followed source symlink')
            assert (outside/'sentinel').read_bytes()==b'outside' and not (root/'link').exists();passed.append('refuses_source_symlink')
            try:module.rename_directory_noreplace(fd,'../outside','outside')
            except ValueError:pass
            else:raise AssertionError('Accepted traversal')
            passed.append('refuses_scope_escape')
        finally:os.close(fd)
    result={'success':True,'tests':passed,'production_paths_used':False}
    (HERE/'fixture-result.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result))

if __name__=='__main__':run()
