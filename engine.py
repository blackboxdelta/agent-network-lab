"""Twenty-five clients and seven network actors, one inference gateway, an explicitly simulated network."""
import asyncio, heapq, json, random, sqlite3, time, uuid
from collections import deque
from dataclasses import dataclass, field, asdict
from pathlib import Path
import httpx
import psutil
from traces import TraceStore, TTL
from debate import Debate

ROOT = Path(__file__).resolve().parent
CLIENTS = [f'C{i:02d}' for i in range(1,26)]
GATEWAYS = {client: ('A4' if i < 13 else 'A5') for i,client in enumerate(CLIENTS)}
ROLES = {**{client:'Client' for client in CLIENTS}, **dict(zip(['A4','A5','A6','A7','A8','A9','A10'],['Gateway','Gateway','Router','Router','Service','Service','Observer']))}
PAIRS = list(GATEWAYS.items()) + [(f'A{a}',f'A{b}') for a,b in [(4,6),(4,7),(5,6),(5,7),(6,8),(6,9),(7,8),(7,9),(10,4),(10,5),(10,6),(10,7),(10,8),(10,9)]]

@dataclass
class Agent:
    id: str
    role: str
    up: bool = True
    sent: int = 0
    received: int = 0
    decisions: int = 0
    status: str = 'idle'
    preferred_service: str = 'A8'
    preferred_hop: str = ''
    service_free: float = 0
    last_decision: dict = field(default_factory=dict)
    inbox: deque = field(default_factory=lambda: deque(maxlen=6))

class Lab:
    def __init__(self, db_path=None):
        ROOT.joinpath('data').mkdir(exist_ok=True)
        self.db = sqlite3.connect(db_path or ROOT/'data/events.sqlite')
        self.db.execute('PRAGMA journal_mode=WAL')
        self.db.execute('CREATE TABLE IF NOT EXISTS events (seq INTEGER PRIMARY KEY, run TEXT, wall REAL, sim REAL, kind TEXT, payload TEXT)')
        self.db.commit()
        self.traces=TraceStore(self.db)
        self.mode="network"
        self.client = httpx.AsyncClient(timeout=90, trust_env=False)
        self.model_url = 'http://127.0.0.1:8011/v1'
        self.model = 'qwen3-network'
        self.model_health = 'not checked'
        self.model_error = ''
        self.running = False
        self.pending = {}
        self.active = {}
        self.results = deque()
        self.workers = []
        self.reset()
        self.debate=Debate(self)

    def reset(self):
        self.running = False
        self.epoch = getattr(self,'epoch',0)+1
        self.pending.clear(); self.results.clear()
        if hasattr(self,'debate'):self.debate.reset()
        self.run_id = str(uuid.uuid4())[:8]
        self.clock = 0.; self.rng = random.Random(42)
        self.agents = {aid:Agent(aid,role) for aid,role in ROLES.items()}
        self.links = {f'{a}-{b}':dict(id=f'{a}-{b}',a=a,b=b,up=True,free={},sent=0) for a,b in PAIRS}
        self.config = dict(load=1, latency=100, bandwidth=32, loss=0, service_delay=.2, concurrency=20)
        self.events = deque(maxlen=160); self.inflight=[]; self.seq=0
        self.generated=0; self.completed=0; self.dropped=0; self.tokens=0; self.valid=0; self.invalid=0; self.failures=0
        self.latencies=deque(maxlen=1000); self.inference_times=deque(maxlen=200); self.queue_times=deque(maxlen=200)
        self.log('run','New run; simulated clock paused. All agents share one inference endpoint.')

    def log(self,kind,text,**extra):
        event=dict(kind=kind,text=text,sim=round(self.clock,3),wall=time.time(),**extra)
        self.events.append(event)
        self.db.execute('INSERT INTO events(run,wall,sim,kind,payload) VALUES(?,?,?,?,?)',(self.run_id,event['wall'],self.clock,kind,json.dumps(event)))
        self.db.commit()

    def neighbors(self,aid):
        return [l['b'] if l['a']==aid else l['a'] for l in self.links.values() if aid in (l['a'],l['b']) and l['up'] and self.agents[l['b'] if l['a']==aid else l['a']].up]

    def next_hop(self,source,target,visited):
        queue=deque([(source,[])])
        seen=set(visited)-{source}; seen.add(source)
        while queue:
            node,path=queue.popleft()
            if node==target:return path[0] if path else None
            for peer in self.neighbors(node):
                if peer=='A10' and target!='A10':continue
                if peer not in seen:seen.add(peer);queue.append((peer,path+[peer]))
        return None

    def send(self,source,target,kind='status',request=None,visited=None,payload=None):
        visited=list(visited or [])
        if payload is None and kind=='status':payload={'agent':source,'up':self.agents[source].up,'service_queue':round(max(0,self.agents[source].service_free-self.clock),2),'note':str(self.agents[source].last_decision.get('reason','Available'))[:120]}
        if payload is None and request and kind in ('request','response'):
            payload={'note':json.dumps(request.get('task') if kind=='request' else request.get('response')),'trace_id':request.get('trace_id')}
        if not visited and kind in ('status','debate','request','response') and payload:
            payload=dict(payload)
            payload['_message_trace']=self.traces.create(self.run_id,'message',source,title=f'{source} → {target} · {kind}',source=source,recipient=target,message_kind=kind,text=payload.get('argument',payload.get('note','')),reply_to=payload.get('reply_to'),evidence_id=payload.get('evidence_id'),uncertainty=payload.get('uncertainty'),model_trace_id=payload.get('trace_id'),status='in_transit')
        if payload and payload.get('_message_trace'):self.traces.append(payload['_message_trace'],'hop',sim=self.clock,source=source,target=target)
        if request and request.get('trace_id'):self.traces.append(request['trace_id'],'route',sim=self.clock,source=source,target=target,message_kind=kind)
        if not self.agents[source].up or not self.agents[target].up:
            self.drop(source,target,'node unavailable',request,payload);return
        if source==target:self.arrive(source,target,kind,request,visited,payload);return
        hop=self.next_hop(source,target,visited)
        preferred=self.agents[source].preferred_hop
        # A suggested hop is used only if it has a loop-free path to this target.
        if preferred in self.neighbors(source) and preferred not in visited:
            if preferred==target or self.next_hop(preferred,target,visited+[source]):hop=preferred
        if not hop:self.drop(source,target,'no reachable route',request,payload);return
        link=next(l for l in self.links.values() if {l['a'],l['b']}=={source,hop})
        direction=source+'>'+hop
        start=max(self.clock,link['free'].get(direction,0))
        if start-self.clock>8:self.drop(source,target,'link queue full',request,payload);return
        size=4 if kind in ('request','response') else .5 # simulated KiB, not actual HTTP bytes
        finish=start+size/self.config['bandwidth'];link['free'][direction]=finish
        due=finish+self.config['latency']/1000
        self.seq+=1
        heapq.heappush(self.inflight,(due,self.seq,dict(source=source,hop=hop,target=target,kind=kind,request=request,visited=visited+[source],link=link['id'],payload=payload)))
        if request and request.get('trace_id'):self.traces.append(request['trace_id'],'link_queued',sim=self.clock,source=source,hop=hop,link=link['id'],virtual_queue_seconds=start-self.clock,virtual_due=due,message_kind=kind)
        self.agents[source].sent+=1;link['sent']+=1
        self.log('send',f'{source} → {hop}: {kind}'+(' · '+payload.get('note','') if payload else ''),source=source,target=hop,final=target,payload=payload)

    def drop(self,source,target,reason,request=None,payload=None):
        if payload and payload.get("_message_trace"):self.traces.append(payload["_message_trace"],"dropped",status="dropped",sim=self.clock,reason=reason)
        if request and request.get("trace_id"):self.traces.append(request["trace_id"],"dropped",status="dropped",sim=self.clock,source=source,target=target,reason=reason)
        self.dropped+=1;self.log('drop',f'{source} → {target}: {reason}',source=source,target=target)

    def arrive(self,aid,target,kind,request,visited,payload=None):
        agent=self.agents[aid];agent.received+=1
        if request and request.get('trace_id'):self.traces.append(request['trace_id'],'delivered',sim=self.clock,agent=aid,message_kind=kind)
        agent.inbox.append(dict(kind=kind,source=visited[-1] if visited else aid,time=round(self.clock,2),wall=time.time(),payload=payload))
        if aid!=target:
            if kind=='request' and agent.role=='Gateway':
                preferred=agent.preferred_service
                if self.agents[preferred].up:target=preferred
            self.send(aid,target,kind,request,visited,payload);return
        if payload and payload.get('_message_trace'):self.traces.append(payload['_message_trace'],'delivered',status='delivered',sim=self.clock,recipient=aid)
        if kind=='request' and agent.role=='Service':
            start=max(agent.service_free,self.clock)
            if start-self.clock>12:self.drop(aid,request['origin'],'service queue full',request);return
            agent.service_free=start+self.config['service_delay']
            if request.get('trace_id'):self.traces.append(request['trace_id'],'service_queued',status='processing',sim=self.clock,service=aid,start=start,finish=agent.service_free,task=request.get('task'))
            self.seq+=1
            heapq.heappush(self.inflight,(agent.service_free,self.seq,dict(kind='service_complete',source=aid,request=request)))
        elif kind=='response':
            self.completed+=1;latency=self.clock-request['created'];self.latencies.append(latency)
            if request.get('trace_id'):self.traces.append(request['trace_id'],'response_received',status='complete',sim=self.clock,agent=aid,result=request.get('response'),virtual_latency=latency)
            self.log('complete',f"{request['id']} completed in {latency:.2f} simulated seconds",source=aid)

    def observation(self,aid):
        a=self.agents[aid]
        return dict(agent=aid,role=a.role,neighbors=self.neighbors(aid),services={x:dict(up=self.agents[x].up,queue_delay=round(max(0,self.agents[x].service_free-self.clock),2)) for x in ['A8','A9']},recent=list(a.inbox)[-3:],allowed_actions=['report_status','wait']+(['set_service'] if a.role=='Gateway' else [])+(['set_next_hop'] if a.role=='Router' else []))

    def enqueue(self,aid):
        if aid not in self.pending and aid not in self.active and self.agents[aid].up:
            self.pending[aid]=(time.monotonic(),self.epoch,self.observation(aid));self.agents[aid].status='queued'

    async def health(self):
        try:
            r=await self.client.get(self.model_url+'/models');r.raise_for_status()
            names=[m['id'] for m in r.json()['data']]
            if names!=[self.model]:raise ValueError(f'Expected only {self.model}, got {names}')
            self.model_health='ready';self.model_error=''
        except Exception as e:self.model_health='offline';self.model_error=str(e)[:300]
        return self.model_health=='ready'

    async def infer(self,aid,obs,queue_wait=0):
        healthy=[key for key,value in obs['services'].items() if value['up']]
        schema={'type':'object','properties':{'action':{'type':'string','enum':obs['allowed_actions']},'target':{'type':'string','enum':list(dict.fromkeys(['']+obs['neighbors']+healthy))},'reason':{'type':'string'}},'required':['action','target','reason'],'additionalProperties':False}
        messages=[{'role':'system','content':'You are a network simulation agent. Choose one allowed action from the observation. Gateways should set_service to a healthy service with the lower queue_delay. Other roles should normally report_status to share observations with neighbors; use wait only when no useful action is available. A service with up=true is available. Reply only with compact JSON: action, target, reason. Use target "" for report_status or wait. Keep reason under 12 words.'},{'role':'user','content':json.dumps(obs)}]
        trace=self.traces.create(self.run_id,'inference',aid,title=f'Network decision · {aid}',queue_seconds=queue_wait,observation=obs)
        error=''
        for attempt in range(2):
            try:
                request_body=dict(model=self.model,messages=messages,max_tokens=128,temperature=0,chat_template_kwargs={'enable_thinking':False},response_format={'type':'json_schema','json_schema':{'name':'network_action','strict':True,'schema':schema}})
                self.traces.append(trace,'inference_request',attempt=attempt+1,request=request_body)
                r=await self.client.post(self.model_url+'/chat/completions',json=request_body)
                self.traces.append(trace,'http_response',attempt=attempt+1,status_code=r.status_code,body=r.text)
                r.raise_for_status();body=r.json();self.tokens+=body.get('usage',{}).get('total_tokens',0)
                decision=json.loads(body['choices'][0]['message']['content'])
                action=decision.get('action');target=decision.get('target','')
                if action not in obs['allowed_actions']:raise ValueError('Action not allowed for this role')
                if action=='set_service' and target not in healthy:raise ValueError('Service is unavailable')
                if action=='set_next_hop' and target not in obs['neighbors']:raise ValueError('Not an adjacent peer')
                self.traces.append(trace,'validated',status='validated',result=decision)
                return decision,None
            except (httpx.HTTPError,ValueError,KeyError,TypeError) as e:
                error=str(e)[:220]
                self.traces.append(trace,'error',error=error,attempt=attempt+1)
                if not isinstance(e,httpx.HTTPError):self.invalid+=1
                if isinstance(e,httpx.HTTPError):break
                messages.append({'role':'user','content':'Invalid response. Return a valid allowed action with action, target, reason.'})
        self.traces.append(trace,'fallback',status='fallback',error=error)
        return {'action':'report_status','target':'','reason':'Deterministic fallback: inference unavailable or invalid.'},error

    async def worker(self,index):
        while True:
            if not self.pending or not self.running or index>=self.config['concurrency']:
                await asyncio.sleep(.1);continue
            aid=next(iter(self.pending));queued,epoch,obs=self.pending.pop(aid)
            self.active[aid]=time.monotonic();self.agents[aid].status='thinking'
            wait=time.monotonic()-queued;started=time.monotonic()
            try:
                result,error=await (self.debate.respond(aid,obs,wait) if obs.get('mode')=='debate' else self.infer(aid,obs,wait))
                result['_completed_wall']=time.time()
                self.results.append((epoch,aid,result,error,wait,time.monotonic()-started))
                if epoch==self.epoch:self.agents[aid].status='buffered'
            finally:self.active.pop(aid,None)

    def apply_results(self):
        while self.results:
            epoch,aid,result,error,wait,duration=self.results.popleft()
            if epoch!=self.epoch:continue
            a=self.agents[aid];a.status='idle'
            if not a.up:
                if result.get('action')=='debate_message':self.debate.outstanding=False;self.debate.turn+=1
                continue
            if (result['action']=='set_service' and not self.agents[result['target']].up) or (result['action']=='set_next_hop' and result['target'] not in self.neighbors(aid)):
                self.log('stale',f'{aid}: topology changed before decision applied; discarded.',source=aid)
                continue
            a.last_decision={**result,'fallback':bool(error)};a.decisions+=1
            self.queue_times.append(wait);self.inference_times.append(duration)
            if error:self.failures+=1
            else:self.valid+=1
            action=result['action'];target=result.get('target','')
            if action=='debate_message':
                self.debate.apply(aid,result,error)
                continue
            if action=='set_service' and self.agents[target].up:a.preferred_service=target
            elif action=='set_next_hop' and target in self.neighbors(aid):a.preferred_hop=target
            elif action=='report_status':
                for peer in self.neighbors(aid):self.send(aid,peer,'status')
            self.log('fallback' if error else 'decision',f"{aid}: {action} {target} · {result.get('reason','')}",source=aid,error=error)

    def step(self):
        self.apply_results();self.clock+=.5
        if self.mode=='network' and int(self.clock*2)%6==0:
            for aid in CLIENTS:
                for _ in range(self.config['load']):
                    if self.agents[aid].up:
                        self.generated+=1
                        task={'operation':'sum','numbers':[int(aid[1:]),self.generated%10]} if self.generated%3==0 else {'operation':'uppercase','text':f'hello from {aid}'} if self.generated%3==1 else {'operation':'health_check'}
                        trace=self.traces.create(self.run_id,'request',aid,title=f'R{self.generated} · {task["operation"]}',task=task)
                        request=dict(id=f'R{self.generated}',origin=aid,created=self.clock,trace_id=trace,task=task,wall=time.time())
                        gateway=GATEWAYS[aid]
                        target=self.agents[gateway].preferred_service
                        if not self.agents[target].up:target='A9' if target=='A8' else 'A8'
                        self.send(aid,target,'request',request)
        processed=0
        while self.inflight and self.inflight[0][0]<=self.clock and processed<500:
            due,_,event=heapq.heappop(self.inflight);processed+=1
            # Quantized virtual time: deliveries are processed on the next 0.5s tick.
            if event['kind']=='service_complete':
                if self.agents[event['source']].up:
                    req=event['request'];task=req.get('task',{'operation':'health_check'})
                    value=sum(task['numbers']) if task['operation']=='sum' else task['text'].upper() if task['operation']=='uppercase' else 'healthy'
                    req['response']={'service':event['source'],'operation':task['operation'],'value':value,'execution':'deterministic simulated service; not model-generated'}
                    if req.get('trace_id'):self.traces.append(req['trace_id'],'service_result',sim=self.clock,service=event['source'],result=req['response'])
                    self.send(event['source'],req['origin'],'response',req)
                else:self.drop(event['source'],event['request']['origin'],'service failed before completion',event['request'])
                continue
            if not self.links[event['link']]['up'] or not self.agents[event['hop']].up or self.rng.random()<self.config['loss']/100:
                self.drop(event['source'],event['hop'],'link failure, loss or node outage',event.get('request'),event.get('payload'));continue
            self.arrive(event['hop'],event['target'],event['kind'],event['request'],event['visited'],event.get('payload'))
        # One actor per virtual tick; all 32 actors share the same bounded inference queue.
        if self.mode=='network':self.enqueue(list(self.agents)[int(self.clock*2-1)%len(self.agents)])
        else:self.debate.schedule()

    async def loop(self):
        while True:
            if time.time()-self.traces.last_cleanup>=60:self.cleanup()
            if self.running:self.step()
            await asyncio.sleep(.5)

    def cleanup(self):
        now=time.time();self.traces.purge(now)
        self.events=deque((e for e in self.events if e['wall']>now-TTL),maxlen=160)
        for a in self.agents.values():
            a.inbox=deque((e for e in a.inbox if e.get('wall',now)>now-TTL),maxlen=6)
            if a.last_decision.get('_completed_wall',now)<=now-TTL:a.last_decision={}
        for aid,(queued,epoch,obs) in list(self.pending.items()):
            if time.monotonic()-queued>TTL:self.pending.pop(aid,None)
        self.results=deque(x for x in self.results if x[2].get('_completed_wall',now)>now-TTL)
        self.inflight=[x for x in self.inflight if (x[2].get('request') or {}).get('wall',now)>now-TTL] if self.inflight else []
        heapq.heapify(self.inflight)
        if self.mode=='debate' and not self.pending and not self.active and not self.results:self.debate.outstanding=False

    async def launch(self):
        self.workers=[asyncio.create_task(self.worker(i)) for i in range(20)]
        self.loop_task=asyncio.create_task(self.loop())

    async def close(self):
        for task in self.workers+[self.loop_task]:task.cancel()
        await asyncio.gather(*self.workers,self.loop_task,return_exceptions=True)
        await self.client.aclose();self.db.close()

    def snapshot(self):
        cutoff=time.time()-TTL
        self.events=deque((e for e in self.events if e['wall']>cutoff),maxlen=160)
        for a in self.agents.values():
            a.inbox=deque((e for e in a.inbox if e.get('wall',time.time())>cutoff),maxlen=6)
            if a.last_decision.get('_completed_wall',time.time())<=cutoff:a.last_decision={}
        rss=0
        try:
            processes=json.loads((ROOT/'data/processes.json').read_text())
            parent=psutil.Process(processes['model']['pid'])
            rss=sum(p.memory_info().rss for p in [parent]+parent.children(recursive=True) if p.is_running())
        except (OSError,KeyError,ValueError,psutil.Error):pass
        mean=lambda xs:round(sum(xs)/len(xs),3) if xs else 0
        return dict(run=self.run_id,running=self.running,clock=self.clock,mode=self.mode,debate=self.debate.snapshot(),retention_seconds=TTL,model=self.model,health=self.model_health,error=self.model_error,config=self.config,
            agents=[{**{k:v for k,v in asdict(a).items() if k!='inbox'},'inbox':list(a.inbox),'service_queue':round(max(0,a.service_free-self.clock),2)} for a in self.agents.values()],
            links=[{**l,'backlog':round(max([0]+[v-self.clock for v in l['free'].values()]),2)} for l in self.links.values()],
            events=list(self.events)[-80:],pending=list(self.pending),active=list(self.active),buffered=len(self.results),
            metrics=dict(server_rss_mb=round(rss/1024**2),invalid=self.invalid,generated=self.generated,completed=self.completed,dropped=self.dropped,inflight=len(self.inflight),tokens=self.tokens,valid=self.valid,fallbacks=self.failures,network_latency=mean(self.latencies),inference_latency=mean(self.inference_times),queue_latency=mean(self.queue_times)))
