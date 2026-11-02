import asyncio,json,time
import httpx,pytest
from engine import Lab
from traces import TTL

@pytest.fixture
def lab(tmp_path):
    value=Lab(tmp_path/'learning.sqlite')
    yield value
    value.db.close()

def test_retention_is_wall_clock_and_across_runs(lab):
    expired=lab.traces.create('old-run','debate','C01',title='Old')
    current=lab.traces.create(lab.run_id,'request','C02',title='Current')
    lab.db.execute('UPDATE traces SET expires=? WHERE id=?',(time.time()-1,expired));lab.db.commit()
    assert lab.traces.get(expired) is None
    assert expired not in [t['id'] for t in lab.traces.recent()]
    lab.running=False;lab.cleanup()
    assert lab.db.execute('SELECT count(*) FROM traces WHERE id=?',(expired,)).fetchone()[0]==0
    assert lab.traces.get(current)
    assert round(lab.traces.get(current)['expires']-lab.traces.get(current)['created'])==10800

def test_network_trace_has_actual_task_and_service_result(lab):
    for _ in range(500):lab.step()
    complete=[t for t in lab.traces.recent('request') if t['status']=='complete']
    assert complete
    trace=lab.traces.get(complete[0]['id'])
    stages=[x['stage'] for x in trace['payload']['spans']]
    assert 'link_queued' in stages and 'service_result' in stages and 'response_received' in stages
    assert trace['payload']['result']['execution'].startswith('deterministic')

@pytest.mark.asyncio
async def test_debate_uses_shared_endpoint_and_records_prompt(lab):
    calls=[]
    def response(request):
        calls.append(request)
        return httpx.Response(200,json={'choices':[{'message':{'content':json.dumps({'argument':'Below-threshold memory is not universal logical computation. Test logical gates.','evidence_id':'E1','uncertainty':'Memory alone is insufficient.'})}}],'usage':{'total_tokens':100}})
    await lab.client.aclose();lab.client=httpx.AsyncClient(transport=httpx.MockTransport(response))
    lab.mode='debate';lab.debate.schedule()
    queued,epoch,obs=lab.pending.pop('C01')
    result,error=await lab.debate.respond('C01',obs,.1)
    assert error is None and str(calls[0].url).startswith(lab.model_url)
    lab.debate.apply('C01',result,error)
    assert lab.debate.turn==1 and lab.debate.entries[0]['evidence_id']=='E1'
    trace=lab.traces.get(result['trace_id'])
    assert any(x['stage']=='inference_request' and x['messages'][0]['role']=='system' for x in trace['payload']['spans'])
    assert any(x['stage']=='inference_response' for x in trace['payload']['spans'])
    assert lab.inflight # Actual debate message is queued through the simulated network.
    await lab.client.aclose()

def test_debate_assignments_and_round_limit(lab):
    assert len({lab.debate.role(f'C{i:02d}')['team'] for i in range(1,26)})==5
    lab.debate.turn=25;assert lab.debate.observation('C01')['phase']=='Cross-examination'
    lab.debate.turn=100;lab.debate.schedule()
    assert lab.debate.finished and not lab.running

@pytest.mark.asyncio
async def test_trace_api_expiry(lab,monkeypatch):
    import server
    monkeypatch.setattr(server,'lab',lab)
    identifier=lab.traces.create(lab.run_id,'inference','C01',title='Example')
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=server.app),base_url='http://localhost:8020') as c:
        assert (await c.get('/api/traces/'+identifier)).status_code==200
        lab.db.execute('UPDATE traces SET expires=? WHERE id=?',(time.time()-1,identifier));lab.db.commit()
        assert (await c.get('/api/traces/'+identifier)).status_code==404
        assert (await c.get('/api/traces')).json()['items']==[]
        assert (await c.post('/api/mode/debate')).json()['mode']=='debate'
    await lab.client.aclose()

def test_agent_conversation_delivery_and_direction(lab):
    lab.send('C01','C06','debate',payload={'argument':'Test the logical gate error rate.','reply_to':'T1','evidence_id':'E1'})
    sent=lab.traces.conversations('C01','sent')
    assert len(sent)==1 and sent[0]['recipient']=='C06'
    assert lab.traces.conversations('C06','received')==[]
    for _ in range(5):lab.step()
    received=lab.traces.conversations('C06','received')
    assert len(received)==1 and received[0]['source']=='C01'
    assert received[0]['text']=='Test the logical gate error rate.'
    assert received[0]['status']=='delivered'

def test_conversation_loss_not_marked_delivered(lab):
    lab.config['loss']=100
    lab.send('C01','A4','status',payload={'note':'Ready'})
    for _ in range(3):lab.step()
    assert lab.traces.conversations('A4','received')==[]
    assert lab.traces.conversations('C01','sent')[0]['status']=='dropped'

def test_expired_conversations_hidden(lab):
    lab.send('C01','A4','status',payload={'note':'Temporary'})
    message=lab.traces.conversations('C01')[0]
    lab.db.execute('UPDATE traces SET expires=? WHERE id=?',(time.time()-1,message['id']));lab.db.commit()
    assert lab.traces.conversations('C01')==[]
