#!/usr/bin/env python3
"""Transport/bookkeeping soak against committed receiver + fake model/effect peers."""
import argparse, concurrent.futures, gc, hashlib, json, os, platform, statistics, sys, threading, time, urllib.request
from pathlib import Path
p=argparse.ArgumentParser();p.add_argument('--source',required=True);p.add_argument('--output',required=True);p.add_argument('--turns',type=int,default=80);p.add_argument('--prefix',default='File a fixture task')
a=p.parse_args()
if not 1 <= a.turns <= 200:p.error('--turns must be 1..200')
if not Path('/proc/self/status').exists():p.error('resource measurements require Linux /proc')
sys.path.insert(0,str(Path(a.source)/'tests'))
import test_command_pipeline as fixtures
case=fixtures.PipelineTest('runTest');case.setUp()
case.config['max_archive_bytes']=256*1024*1024
case.config['listen']['request_timeout_secs']=30
case.config['listen']['max_connections']=12
stop=threading.Event(); arrivals=[]; admission=[]; settlement=[]; failures=[]
def summary(xs):
 s=sorted(xs)
 return {'samples':len(s),'p50':round(statistics.median(s),3),'p95':round(s[min(len(s)-1,int(len(s)*.95))],3),'p99':round(s[min(len(s)-1,int(len(s)*.99))],3)} if s else None
def resources():
 status=Path('/proc/self/status').read_text()
 rss=int(next(l.split()[1] for l in status.splitlines() if l.startswith('VmRSS:')))
 return {'rss_kib':rss,'fds':len(os.listdir('/proc/self/fd')),'threads':threading.active_count()}
try:
 with case.http(start_worker=True) as (url,worker):
  def request(path,body=None,extra=None):
   req=urllib.request.Request(url+path,data=None if body is None else json.dumps(body).encode(),headers={'Authorization':'Bearer '+case.legacy.token,'Content-Type':'application/json',**(extra or {})})
   return urllib.request.urlopen(req,timeout=10)
  with request('/route/sessions',{'id':'benchmark'}) as r:r.read()
  ready=[threading.Event(),threading.Event()]
  def observe(index):
   try:
    with request('/route/sessions/benchmark/events?seconds=25',extra={'Accept':'text/event-stream'}) as stream:
     kind='';ready[index].set()
     for raw in stream:
      if stop.is_set():break
      line=raw.decode().strip()
      if line.startswith('event: '):kind=line[7:]
      elif line.startswith('data: ') and kind=='turn':
       event=json.loads(line[6:]);arrivals.append(max(0,(time.time()-event['at'])*1000))
   except Exception as exc:failures.append(type(exc).__name__)
  watchers=[threading.Thread(target=observe,args=(i,)) for i in range(2)]
  for t in watchers:t.start()
  assert all(e.wait(5) for e in ready)
  before=resources()
  def turn(index):
   start=time.perf_counter()
   with request('/route/sessions/benchmark/turns',{'client_turn_id':f'bench-{index}','text':f'{a.prefix} {index}','mode':'dry_run'}) as r:state=json.load(r)
   admission.append((time.perf_counter()-start)*1000)
   deadline=time.monotonic()+10
   while not state['done']:
    if time.monotonic()>deadline:raise AssertionError('fixture deadline')
    time.sleep(.02)
    with request('/route/turns/'+state['id']) as r:state=json.load(r)
   assert state['phase']=='dry_run'
   settlement.append((time.perf_counter()-start)*1000)
  with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:list(pool.map(turn,range(a.turns)))
  with case.archive.db() as db:expected=db.execute('SELECT count(*) FROM triage_events').fetchone()[0]*2
  deadline=time.monotonic()+3
  while len(arrivals)<expected and time.monotonic()<deadline:time.sleep(.02)
  stop.set()
  for t in watchers:t.join(3)
  assert len(arrivals)==expected, 'observer window expired or an event was lost'
  assert not any(t.is_alive() for t in watchers)
  gc.collect();after=resources()
  assert not failures,failures
  assert not case.sender.calls and not case.voice.calls
  result={'scope':'isolated HTTP/archive/SSE; fake model and action peers; no production calls','turns':a.turns,'input_prefix':a.prefix,'concurrent_submitters':4,'observers':2,
   'admission_ms':summary(admission),'fixture_settlement_ms':summary(settlement),'commit_to_sse_ms':summary(arrivals),
   'resources_before':before,'resources_after_observers_closed':after,'rss_delta_kib':after['rss_kib']-before['rss_kib'],
   'observed_events':len(arrivals),'expected_events':expected,'effects':0,'failures':failures}
  assert after['rss_kib']-before['rss_kib']<32768
  assert after['fds']<=before['fds']+4
 result['cleanup']='HTTP endpoint/pool/watchers closed; temporary state removed'
 result['runtime']={'python':platform.python_version(),'platform':platform.platform(),
   'cpu':next((l.split(':',1)[1].strip() for l in Path('/proc/cpuinfo').read_text().splitlines() if l.startswith('model name')),'unknown')}
 result['source_sha256']={p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(Path(a.source).glob('*.py'))}
 Path(a.output).write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result,indent=2))
finally:stop.set();case.doCleanups()
