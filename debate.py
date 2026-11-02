"""Evidence-bounded discussion, not a scientific fact-checking engine."""
import json,time
from collections import deque

TOPIC='Which path offers the most credible route to fault-tolerant quantum computing, and what experiment would distinguish the alternatives?'
CARDS=[
 {'id':'E1','title':'Surface-code memory below threshold','url':'https://www.nature.com/articles/s41586-024-08449-y','fact':'The Willow study reports below-threshold surface-code memories, including distance 7 and a distance-5 memory with a real-time decoder. This is evidence about memory; it does not alone demonstrate useful universal fault-tolerant algorithms.'},
 {'id':'E2','title':'High-threshold, low-overhead quantum memory','url':'https://www.nature.com/articles/s41586-024-07107-7','fact':'This study proposes a fault-tolerant memory protocol using bivariate bicycle quantum low-density parity-check (LDPC) codes. Its overhead and threshold results depend on the specified circuit noise model and connectivity assumptions; they are not an end-to-end useful quantum computer demonstration.'},
 {'id':'E3','title':'Logical processor using atom arrays','url':'https://www.nature.com/articles/s41586-023-06927-3','fact':'The reconfigurable neutral-atom study demonstrates logical operations using multiple encodings. Compare each reported operation and its error handling; do not equate a reported logical-qubit count with arbitrary long fault-tolerant computation.'},
 {'id':'E4','title':'Learning error decoding','url':'https://www.nature.com/articles/s41586-024-08148-8','fact':'This study develops a learned neural-network decoder for surface-code error correction. Decoder accuracy is one question; deployment latency, scaling and robustness must also be evaluated.'}
]
TEAMS=[('Surface codes','Argue for surface codes, but identify a limitation.','E1'),('Quantum LDPC','Argue for quantum LDPC, but expose connectivity assumptions.','E2'),('Neutral atoms','Argue for neutral-atom architectures, without overstating logical-qubit counts.','E3'),('Decoders & control','Examine decoding latency, calibration, leakage and control overhead.','E4'),('Critical reviewers','Compare evidence fairly; ask what experiment could falsify a claim.','E1')]
TEAMS=TEAMS*4  # 20 teams × 5 lenses = 100 debate participants.
LENSES=['physical noise and correlations','logical memory versus logical gates','connectivity and control cost','decoding latency and scaling','resource estimates and falsifiable tests']
PHASES=['Opening claim','Cross-examination','Rebuttal','Synthesis']

class Debate:
    def __init__(self,lab):self.lab=lab;self.reset()
    def reset(self):
        self.turn=0;self.entries=deque(maxlen=150);self.outstanding=False;self.finished=False
    def role(self,aid):
        n=int(aid[1:])-1;team,brief,evidence=TEAMS[n//5]
        return dict(team=team,brief=brief,lens=LENSES[n%5],evidence=evidence)
    def observation(self,aid):
        role=self.role(aid);phase=PHASES[min(3,self.turn//25)]
        # A participant replies to a published earlier turn from a different team.
        others=[e for e in self.entries if e['team']!=role['team'] and not e.get('fallback')]
        previous=others[-1] if others else None
        recipient=previous['agent'] if previous else f'C{((int(aid[1:])-1+5)%100)+1:02d}'
        cards=[c for c in CARDS if c['id'] in {role['evidence'],'E1','E2'}][:3]
        phase_task={'Opening claim':'Make one bounded claim and propose a falsifiable test.','Cross-examination':'Challenge an assumption in the previous argument and ask one specific question.','Rebuttal':'Respond to the earlier argument; concede valid points and identify a remaining disagreement.','Synthesis':'Summarize a conditional conclusion, unresolved uncertainty and next experiment; do not declare a winner.'}[phase]
        return dict(mode='debate',agent=aid,topic=TOPIC,phase=phase,phase_task=phase_task,role=role,reply_to=previous['id'] if previous else None,recipient=recipient,previous_argument=previous['argument'] if previous else 'No prior argument yet.',evidence_cards=cards)
    def schedule(self):
        if self.outstanding or self.finished:return
        if self.turn>=100:self.finished=True;self.lab.running=False;return
        aid=f'C{self.turn%100+1:02d}'
        if not self.lab.agents[aid].up:self.turn+=1;return
        self.lab.pending[aid]=(time.monotonic(),self.lab.epoch,self.observation(aid));self.lab.agents[aid].status='queued';self.outstanding=True
    async def respond(self,aid,obs,wait):
        traces=self.lab.traces
        trace=traces.create(self.lab.run_id,'debate',aid,title=f"{obs['phase']} · {obs['role']['team']}",observation=obs,queue_seconds=wait)
        system='You are a scientific debate participant in an educational simulation. LDPC means low-density parity-check. Code distance is not called memory distance. A logical memory result is not universal fault-tolerant computation. Use only the supplied evidence cards for factual claims. Do not invent results or numbers. In 2-3 sentences, address the previous argument from your assigned lens, distinguish an observation from an assumption, and propose a discriminating test or limitation. Do not claim a scientific consensus or a winner. Return JSON with argument (under 100 words), evidence_id (one supplied ID), and uncertainty (one short caveat).'
        schema={'type':'object','properties':{'argument':{'type':'string'},'evidence_id':{'type':'string','enum':[c['id'] for c in obs['evidence_cards']]},'uncertainty':{'type':'string'}},'required':['argument','evidence_id','uncertainty'],'additionalProperties':False}
        messages=[{'role':'system','content':system},{'role':'user','content':json.dumps(obs)}]
        error='';start=time.monotonic()
        for attempt in range(2):
            body=None
            try:
                traces.append(trace,'inference_request',attempt=attempt+1,messages=messages,model=self.lab.model,max_tokens=256)
                response=await self.lab.client.post(self.lab.model_url+'/chat/completions',json=dict(model=self.lab.model,messages=messages,max_tokens=256,temperature=.4,chat_template_kwargs={'enable_thinking':False},response_format={'type':'json_schema','json_schema':{'name':'scientific_argument','strict':True,'schema':schema}}))
                response.raise_for_status();body=response.json()
                traces.append(trace,'inference_response',attempt=attempt+1,response=body)
                self.lab.tokens+=body.get('usage',{}).get('total_tokens',0)
                parsed=json.loads(body['choices'][0]['message']['content'])
                if not isinstance(parsed.get('argument'),str) or not parsed['argument'].strip() or parsed.get('evidence_id') not in [c['id'] for c in obs['evidence_cards']]:raise ValueError('Malformed argument or unknown evidence ID')
                if 'linear decoding' in parsed['argument'].lower() or 'memory distance' in parsed['argument'].lower():raise ValueError('Incorrect terminology: LDPC means low-density parity-check; use code distance.')
                result=dict(action='debate_message',target=obs['recipient'],reason=parsed['argument'],argument=parsed['argument'],evidence_id=parsed['evidence_id'],uncertainty=parsed.get('uncertainty',''),phase=obs['phase'],team=obs['role']['team'],reply_to=obs['reply_to'],trace_id=trace)
                traces.append(trace,'validated',status='validated',result=result,duration_seconds=time.monotonic()-start)
                return result,None
            except Exception as e:
                error=str(e)[:300]
                if isinstance(e,(ValueError,KeyError,TypeError)):self.lab.invalid+=1
                traces.append(trace,'error',attempt=attempt+1,error=error)
                messages.append({'role':'user','content':'Return valid JSON with argument, evidence_id and uncertainty. LDPC means low-density parity-check. Use code distance, not memory distance. Address your assigned team and lens; do not copy the earlier argument.'})
        result=dict(action='debate_message',target=obs['recipient'],reason='No valid model argument; retry this participant.',argument='No valid model argument was produced. No scientific claim is substituted.',evidence_id='',uncertainty=error,phase=obs['phase'],team=obs['role']['team'],reply_to=obs['reply_to'],trace_id=trace)
        traces.append(trace,'fallback',status='fallback',result=result);return result,error
    def apply(self,aid,result,error):
        entry=dict(id=f'T{self.turn+1}',agent=aid,wall=time.time(),**{k:result.get(k) for k in ['phase','team','argument','evidence_id','uncertainty','reply_to','trace_id']},recipient=result['target'],fallback=bool(error))
        self.entries.append(entry)
        self.lab.send(aid,result['target'],'debate',payload=entry)
        self.lab.log('debate',f"{aid} → {result['target']} · {result['phase']}: {result['argument']}",source=aid,trace_id=result['trace_id'])
        self.turn+=1;self.outstanding=False
        if self.turn>=100:self.finished=True;self.lab.running=False
    def snapshot(self):
        now=time.time();self.entries=deque((e for e in self.entries if now-e['wall']<10800),maxlen=150)
        return dict(topic=TOPIC,turn=self.turn,total_turns=100,phase=PHASES[min(3,self.turn//25)],finished=self.finished,entries=list(self.entries),evidence=CARDS,teams=[dict(name=t[0],brief=t[1]) for t in TEAMS])
