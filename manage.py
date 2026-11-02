"""Own and stop only the two local services launched by this project."""
import argparse, contextlib, fcntl, json, os, signal, subprocess, sys, time, urllib.request
import psutil
from pathlib import Path
ROOT=Path(__file__).resolve().parent
STATE=ROOT/'data/processes.json'
MODEL='mlx-community/Qwen3-0.6B-4bit'
REVISION='73e3e38d981303bc594367cd910ea6eb48349da8'

def alive(pid):
    try:os.kill(pid,0);return True
    except ProcessLookupError:return False

def status():
    processes=json.loads(STATE.read_text()) if STATE.exists() else {}
    return {k:{**v,'alive':alive(v['pid'])} for k,v in processes.items()}

def probe(port,path):
    try:
        with urllib.request.urlopen(f'http://127.0.0.1:{port}{path}',timeout=3) as r:return r.status==200
    except Exception:return False

def start():
    (ROOT/'data').mkdir(exist_ok=True);(ROOT/'logs').mkdir(exist_ok=True)
    with open(ROOT/'data/launcher.lock','w') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX)
        processes=status()
        if not processes.get('model',{}).get('alive'):
            if probe(8011,'/v1/models'):raise RuntimeError('Port 8011 already serves an unowned model; refusing to launch another.')
            import shutil
            exe=shutil.which('vllm')
            if not exe:raise RuntimeError('Install the official vllm-metal Homebrew package first. See README.')
            env={**os.environ,'HF_HOME':str(ROOT/'data/huggingface'),'VLLM_MLX_DEVICE':'gpu','VLLM_METAL_MULTIMODAL_MODE':'text-only'}
            cmd=[exe,'serve',MODEL,'--revision',REVISION,'--host','127.0.0.1','--port','8011','--served-model-name','qwen3-network','--max-model-len','2048','--max-num-seqs','20','--kv-cache-memory-bytes','1073741824','--gpu-memory-utilization','0.15','--generation-config','vllm']
            with open(ROOT/'logs/model.log','ab') as log:
                p=subprocess.Popen(cmd,stdout=log,stderr=log,env=env,cwd=ROOT,start_new_session=True)
            processes['model']={'pid':p.pid,'command':cmd,'started':time.time()}
            STATE.write_text(json.dumps(processes,indent=2))
        if not processes.get('app',{}).get('alive'):
            if probe(8020,'/api/state'):raise RuntimeError('Port 8020 already in use by another app.')
            python=ROOT/'.venv/bin/python'
            cmd=[str(python),'-m','uvicorn','server:app','--host','127.0.0.1','--port','8020']
            with open(ROOT/'logs/app.log','ab') as log:p=subprocess.Popen(cmd,stdout=log,stderr=log,cwd=ROOT,start_new_session=True)
            processes['app']={'pid':p.pid,'command':cmd,'started':time.time()}
            STATE.write_text(json.dumps(processes,indent=2))
    print('Dashboard: http://127.0.0.1:8020\nModel loading/download log: '+str(ROOT/'logs/model.log'))
    return status()

def stop():
    with open(ROOT/'data/launcher.lock','a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX)
        for name,item in status().items():
            if item['alive']:
                # Check PID identity before stopping the owned process group.
                command=subprocess.check_output(['ps','-p',str(item['pid']),'-o','command='],text=True).strip()
                expected='uvicorn server:app' if name=='app' else 'vllm serve'
                if expected not in command:raise RuntimeError(f'PID {item["pid"]} no longer matches {name}; not stopped.')
                tracked=[]
                with contextlib.suppress(psutil.NoSuchProcess):
                    parent=psutil.Process(item['pid']);tracked=[parent]+parent.children(recursive=True)
                with contextlib.suppress(ProcessLookupError):os.killpg(item['pid'],signal.SIGTERM)
                _,remaining=psutil.wait_procs(tracked,timeout=8)
                for process in remaining:
                    with contextlib.suppress(psutil.NoSuchProcess):process.kill()
                psutil.wait_procs(remaining,timeout=3)
        if STATE.exists():STATE.unlink()
    print('Stopped project-owned app and inference server.')

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('action',choices=['start','stop','status']);a=p.parse_args()
    result={'start':start,'stop':stop,'status':status}[a.action]()
    if result:print(json.dumps(result,indent=2))
