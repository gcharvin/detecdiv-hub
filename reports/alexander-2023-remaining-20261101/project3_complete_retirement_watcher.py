"""Close the already retired source only after both post-read gates succeed.

One-shot local helper; installs no daemon and performs no filesystem deletion.
Any error after finalization starts requires receipt review, never auto-replay.
"""
from datetime import datetime,timezone
import json
import time
from project3_final_operations import HERE,RUNTIME,load
import project3_post_retirement_operations as hosts
import project3_cutover_operations as cutover
import project3_retirement_operations as retirement

STATE={'phase':'waiting_for_post_retirement_reads','filesystem_deletion_allowed':False}


def save():
    STATE['heartbeat_at']=datetime.now(timezone.utc).isoformat()
    p=RUNTIME/'project3-completion-watcher-state.tmp';p.write_text(json.dumps(STATE,indent=2)+'\n',encoding='utf-8');p.replace(RUNTIME/'project3-completion-watcher-state.json')


def main():
    assert not (RUNTIME/'project3-completion-watcher-state.json').exists()
    admission=load('project3-cutover-admission.json',private=True)
    deadline=min(datetime.fromisoformat(x['expires_at']).timestamp() for x in admission['leases'])
    assert load('project3-source-retirement-receipt-verification.json')['success']
    save();errors=0
    while time.time()<deadline-300:
        try:
            hosts.status();cutover.api_status(post=True)
            report=load('project3-post-retirement-hosts-status.json',private=True)
            content=load('project3-api-content-post-retirement-status.json',private=True)
            ls=report['linux'].get('post-linux-state.json',{})
            ws=report['windows'].get('post-windows-state.json',{})
            for s in [ls,ws,content.get('state',{})]:
                assert not s.get('phase','').startswith('failed'),'Post-retirement validation failed'
            errors=0
            if (ls.get('success') and ws.get('success') and report['windows']['task']['state']!='Running'
                    and content.get('receipt_verified') and content['state'].get('success')):
                STATE['phase']='finalizing_verified_retirement';save()
                # Deliberately outside the retryable polling block.
                break
        except AssertionError:
            raise
        except Exception as error:
            errors+=1;STATE['last_poll_error']=type(error).__name__+': '+str(error);save()
            assert errors<4,'Repeated polling failures require review'
        time.sleep(60)
    else:
        raise RuntimeError('Own lease deadline is approaching; inspect receipts before further changes')
    hosts.verify()
    retirement.finish()
    hosts.cleanup()
    STATE.update(phase='source_retirement_fully_verified',success=True,finished_at=datetime.now(timezone.utc).isoformat(),finalization='project3-source-retirement-catalog-finalization.json');save()
    print(json.dumps(STATE),flush=True)


if __name__=='__main__':
    try:main()
    except Exception as error:
        STATE.update(phase='stopped_requires_receipt_review',error=type(error).__name__+': '+str(error));save();raise
