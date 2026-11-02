import asyncio, json
import httpx, pytest
from engine import Lab

@pytest.fixture
def lab(tmp_path):
    l=Lab(tmp_path/'test.sqlite')
    yield l
    l.db.close()

def test_delivery_roundtrip(lab):
    for _ in range(100):lab.step()
    assert lab.completed>0
    assert lab.completed<=lab.generated
    assert len(lab.agents)==32
    assert sum(a.role=='Client' for a in lab.agents.values())==25

def test_service_outage_uses_alternative(lab):
    lab.agents['A8'].up=False
    for _ in range(100):lab.step()
    assert lab.agents['A9'].received>0
    assert lab.completed>0

def test_router_outage_cannot_route_through_observer(lab):
    lab.agents['A6'].up=lab.agents['A7'].up=False
    assert lab.next_hop('A4','A8',[]) is None

def test_bounded_inference_and_stale_results(lab):
    for _ in range(500):lab.enqueue('C01')
    assert list(lab.pending)==['C01']
    epoch=lab.epoch;lab.reset()
    lab.results.append((epoch,'C01',{'action':'report_status','reason':'old'},None,0,0))
    lab.apply_results()
    assert lab.agents['C01'].decisions==0

def test_full_queue_drops(lab):
    lab.config['bandwidth']=1
    for _ in range(50):lab.send('C01','A8','request',{'id':'x','origin':'C01','created':0})
    assert lab.dropped>0

@pytest.mark.asyncio
async def test_invalid_action_never_executes(lab):
    calls=[]
    def reply(request):
        calls.append(request)
        return httpx.Response(200,json={'choices':[{'message':{'content':json.dumps({'action':'run_shell','target':'','reason':'bad'})}}]})
    await lab.client.aclose();lab.client=httpx.AsyncClient(transport=httpx.MockTransport(reply))
    decision,error=await lab.infer('C01',lab.observation('C01'))
    assert len(calls)==2 and error and decision['action']=='report_status'
    await lab.client.aclose()

@pytest.mark.asyncio
async def test_model_output_validated(lab):
    def reply(request):return httpx.Response(200,json={'choices':[{'message':{'content':'{"action":"set_service","target":"A9","reason":"Less load"}'}}]})
    await lab.client.aclose();lab.client=httpx.AsyncClient(transport=httpx.MockTransport(reply))
    decision,error=await lab.infer('A4',lab.observation('A4'))
    assert error is None and decision['target']=='A9'
    await lab.client.aclose()

@pytest.mark.asyncio
async def test_offline_service_rejected(lab):
    lab.agents['A8'].up=False
    def reply(request):return httpx.Response(200,json={'choices':[{'message':{'content':'{"action":"set_service","target":"A8","reason":"Wrong"}'}}]})
    await lab.client.aclose();lab.client=httpx.AsyncClient(transport=httpx.MockTransport(reply))
    decision,error=await lab.infer('A4',lab.observation('A4'))
    assert error and decision['action']=='report_status'
    await lab.client.aclose()

@pytest.mark.asyncio
async def test_pause_buffers_model_result(lab):
    lab.results.append((lab.epoch,'A4',{'action':'set_service','target':'A9','reason':'Test'},None,0,.1))
    await lab.launch()
    await asyncio.sleep(.6)
    assert lab.agents['A4'].preferred_service=='A8'
    lab.step()
    assert lab.agents['A4'].preferred_service=='A9'
    # Close tasks and HTTP client here; fixture closes the database.
    for task in lab.workers+[lab.loop_task]:task.cancel()
    await asyncio.gather(*lab.workers,lab.loop_task,return_exceptions=True)
    await lab.client.aclose()

@pytest.mark.asyncio
async def test_control_endpoints_write_events(lab,monkeypatch):
    import server
    monkeypatch.setattr(server,'lab',lab)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=server.app),base_url='http://localhost:8020') as c:
        r=await c.post('/api/config',json={**lab.config,'concurrency':2})
        assert r.status_code==200
        r=await c.post('/api/toggle/agent/A8')
        assert r.status_code==200 and not lab.agents['A8'].up
        r=await c.get('/api/export')
        assert len(r.json()['events'])>=3
        r=await c.post('/api/config',json={**lab.config,'concurrency':10})
        assert r.status_code==422
        r=await c.post('/api/control/reset',headers={'Origin':'https://example.com'})
        assert r.status_code==403
    await lab.client.aclose()
