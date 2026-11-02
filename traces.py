"""Three-hour wall-clock retention. Cleanup runs even when simulation is paused."""
import json,time,uuid
TTL=3*60*60

class TraceStore:
    def __init__(self,db):
        self.db=db
        db.execute('PRAGMA secure_delete=ON')
        db.execute('CREATE TABLE IF NOT EXISTS traces(id TEXT PRIMARY KEY, run TEXT, kind TEXT, agent TEXT, created REAL, expires REAL, payload TEXT)')
        db.execute('CREATE INDEX IF NOT EXISTS trace_expiry ON traces(expires)')
        db.execute('CREATE INDEX IF NOT EXISTS event_wall ON events(wall)')
        db.commit();self.last_cleanup=0;self.purge()

    def create(self,run,kind,agent,**payload):
        now=time.time();identifier=uuid.uuid4().hex[:16]
        payload={'status':'pending','spans':[],**payload}
        self.db.execute('INSERT INTO traces VALUES(?,?,?,?,?,?,?)',(identifier,run,kind,agent,now,now+TTL,json.dumps(payload)))
        self.db.commit();return identifier

    def append(self,identifier,stage,**details):
        row=self.db.execute('SELECT payload FROM traces WHERE id=? AND expires>?',(identifier,time.time())).fetchone()
        if row is None:return
        data=json.loads(row[0]);data['spans'].append({'stage':stage,'wall':time.time(),**details})
        if 'status' in details:data['status']=details['status']
        if 'result' in details:data['result']=details['result']
        self.db.execute('UPDATE traces SET payload=? WHERE id=?',(json.dumps(data),identifier));self.db.commit()

    def get(self,identifier):
        row=self.db.execute('SELECT id,run,kind,agent,created,expires,payload FROM traces WHERE id=? AND expires>?',(identifier,time.time())).fetchone()
        if not row:return None
        return dict(zip(['id','run','kind','agent','created','expires','payload'],[*row[:6],json.loads(row[6])]))

    def recent(self,kind='',agent=''):
        query='SELECT id,run,kind,agent,created,expires,payload FROM traces WHERE expires>?';args=[time.time()]
        if kind:query+=' AND kind=?';args.append(kind)
        if agent:query+=' AND agent=?';args.append(agent)
        rows=self.db.execute(query+' ORDER BY created DESC LIMIT 100',args).fetchall()
        result=[]
        for identifier,run,kind,agent,created,expires,raw in rows:
            payload=json.loads(raw)
            result.append(dict(id=identifier,run=run,kind=kind,agent=agent,created=created,expires=expires,status=payload.get('status'),title=payload.get('title',kind)))
        return result

    def conversations(self,agent,direction='all'):
        rows=self.db.execute("SELECT id,run,kind,agent,created,expires,payload FROM traces WHERE expires>? AND kind IN ('message','debate') AND (agent=? OR json_extract(payload,'$.recipient')=? OR json_extract(payload,'$.result.target')=?) ORDER BY created DESC LIMIT 300",(time.time(),agent,agent,agent)).fetchall()
        items=[];linked=set()
        for identifier,run,kind,source,created,expires,raw in rows:
            data=json.loads(raw)
            if kind=='message' and data.get('model_trace_id'):linked.add(data['model_trace_id'])
        for identifier,run,kind,source,created,expires,raw in rows:
            data=json.loads(raw)
            if kind=='debate':
                if identifier in linked or not data.get('result'):continue
                result=data['result'];target=result.get('target');text=result.get('argument','');status='generated_only';trace=identifier
                reply=result.get('reply_to');evidence=result.get('evidence_id');caveat=result.get('uncertainty');message_kind='debate'
            else:
                target=data.get('recipient');text=data.get('text','');status=data.get('status');trace=data.get('model_trace_id') or identifier
                reply=data.get('reply_to');evidence=data.get('evidence_id');caveat=data.get('uncertainty');message_kind=data.get('message_kind')
            if direction=='sent' and source!=agent:continue
            if direction=='received' and (target!=agent or status!='delivered'):continue
            items.append(dict(id=identifier,run=run,source=source,recipient=target,text=text,status=status,kind=message_kind,created=created,expires=expires,trace_id=trace,reply_to=reply,evidence_id=evidence,uncertainty=caveat,direction='sent' if source==agent else 'received' if status=='delivered' else 'incoming_pending'))
        return items[:100]

    def purge(self,now=None):
        now=time.time() if now is None else now
        count=self.db.execute('DELETE FROM traces WHERE expires<=?',(now,)).rowcount
        count+=self.db.execute('DELETE FROM events WHERE wall<=?',(now-TTL,)).rowcount
        self.db.commit()
        self.db.execute('PRAGMA wal_checkpoint(TRUNCATE)')
        self.last_cleanup=now
        return count
