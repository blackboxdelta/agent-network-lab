import asyncio, json
from contextlib import asynccontextmanager
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
from engine import Lab, ROOT

lab=Lab()
@asynccontextmanager
async def lifespan(app):
    await lab.launch()
    yield
    await lab.close()
app=FastAPI(lifespan=lifespan)

@app.middleware('http')
async def local_only(request:Request,call_next):
    # Reject foreign browser origins on state-changing requests.
    if request.method not in ('GET','HEAD'):
        origin=request.headers.get('origin')
        if origin and origin not in ('http://127.0.0.1:8020','http://localhost:8020'):return JSONResponse({'detail':'Local origin required'},status_code=403)
    return await call_next(request)

@app.get('/')
def home():return FileResponse(ROOT/'static/index.html')
@app.get('/api/state')
async def state():return lab.snapshot()
@app.get('/api/health')
async def health():await lab.health();return dict(ready=lab.model_health=='ready',error=lab.model_error)
@app.post('/api/control/{action}')
async def control(action:str):
    if action=='start':
        if not await lab.health():raise HTTPException(503,'Model is not ready. Start it from the notebook or launcher.')
        lab.running=True;lab.log('run','Simulation running with shared vllm-metal inference.')
    elif action=='pause':lab.running=False
    elif action=='step':
        if lab.running:raise HTTPException(409,'Pause before single stepping')
        lab.step()
    elif action=='stop':
        lab.running=False;lab.epoch+=1;lab.pending.clear();lab.results.clear()
        for a in lab.agents.values():a.status='idle'
        lab.log('run','Stopped scheduling. Any in-flight inference result will be discarded; model remains available until launcher stop.')
    elif action=='reset':lab.reset()
    else:raise HTTPException(404,'Unknown command')
    return lab.snapshot()

class Config(BaseModel):
    load:int=Field(1,ge=0,le=5)
    latency:int=Field(100,ge=0,le=3000)
    bandwidth:int=Field(32,ge=1,le=1024)
    loss:int=Field(0,ge=0,le=50)
    service_delay:float=Field(.2,ge=.1,le=10)
    concurrency:int=Field(20,ge=1,le=20)
@app.post('/api/config')
async def config(value:Config):lab.config=value.model_dump();lab.log('config','Traffic and inference settings updated.',config=lab.config);return lab.config
@app.post('/api/toggle/{kind}/{identifier}')
async def toggle(kind:str,identifier:str):
    target=lab.agents.get(identifier) if kind=='agent' else lab.links.get(identifier) if kind=='link' else None
    if target is None:raise HTTPException(404,'Unknown target')
    if kind=='agent':target.up=not target.up
    else:target['up']=not target['up']
    lab.log('fault',f'{identifier} availability toggled.');return lab.snapshot()
@app.get('/api/export')
async def export():
    import time
    rows=lab.db.execute('SELECT payload FROM events WHERE run=? AND wall>? ORDER BY seq',(lab.run_id,time.time()-10800)).fetchall()
    return dict(run=lab.run_id,events=[json.loads(r[0]) for r in rows])

@app.post('/api/mode/{mode}')
async def mode(mode:str):
    if mode not in ('network','debate'):raise HTTPException(400,'Unknown mode')
    lab.mode=mode;lab.reset()
    lab.log('mode','Scientific debate selected: quantum fault tolerance.' if mode=='debate' else 'Network traffic mode selected.')
    return lab.snapshot()

@app.get('/api/conversations/{agent}')
async def conversations(agent:str,direction:str='all'):
    if agent not in lab.agents:raise HTTPException(404,'Unknown agent')
    if direction not in ('all','sent','received'):raise HTTPException(400,'Unknown direction')
    return dict(agent=agent,retention_seconds=10800,items=lab.traces.conversations(agent,direction))

@app.get('/api/traces')
async def traces(kind:str='',agent:str=''):
    return dict(retention_seconds=10800,cleanup_interval_seconds=60,items=lab.traces.recent(kind,agent))

@app.get('/api/traces/{identifier}')
async def trace(identifier:str):
    value=lab.traces.get(identifier)
    if value is None:raise HTTPException(404,'Trace expired or not found')
    return value
@app.websocket('/ws')
async def websocket(ws:WebSocket):
    origin=ws.headers.get('origin')
    if origin not in ('http://127.0.0.1:8020','http://localhost:8020'):await ws.close(code=1008);return
    await ws.accept()
    try:
        while True:await ws.send_json(lab.snapshot());await asyncio.sleep(.5)
    except WebSocketDisconnect:pass
app.mount('/static',StaticFiles(directory=ROOT/'static'),name='static')
