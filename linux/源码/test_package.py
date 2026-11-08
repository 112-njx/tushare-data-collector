"""Explicit offline acceptance of the compiled tarball, not unittest-discovered tests."""
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tarfile
import tempfile
import time
import urllib.request

def verify(archive):
    with tempfile.TemporaryDirectory() as temp:
        with tarfile.open(archive,'r:gz') as tar:tar.extractall(temp,filter='data')
        root=Path(temp)/'数据采集器_linux_Ubuntu24_x86_64';binary=root/'Tushare数据采集器_linux/采集器'
        env=dict(os.environ,COLLECTOR_HOME=str(root),COLLECTOR_PORT='18895',COLLECTOR_BIND='127.0.0.1')
        result=subprocess.run([binary,'--configure'],input='offline-primary-for-package-test\noffline-backup-for-package-test\n',text=True,capture_output=True,env=env,timeout=60)
        if result.returncode:raise AssertionError('compiled configure failed: '+result.stderr)
        with sqlite3.connect(root/'状态/采集状态.sqlite') as db:
            db.row_factory=sqlite3.Row
            assert db.execute('SELECT COUNT(*) FROM requests').fetchone()[0]==0
            if (root/'基线/本机采集基线.json').exists():
                baseline=json.loads((root/'基线/本机采集基线.json').read_text())
                for old in baseline['jobs']:
                    if old['records']:
                        now=db.execute('SELECT * FROM jobs WHERE id=?',(old['id'],)).fetchone()
                        assert json.loads(now['cursor'])==old['cursor'],old['id']+' cursor reset'
                        assert now['records']==old['records'] and now['batches']==old['batches']
            db.execute('INSERT OR REPLACE INTO settings VALUES(?,?)',('resume_requested','false'))
        process=subprocess.Popen([binary,'--no-browser'],env=env,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
        try:
            info=root/'状态/当前服务.json';until=time.monotonic()+15
            while not info.exists() and process.poll() is None and time.monotonic()<until:time.sleep(.1)
            if not info.exists():raise AssertionError('compiled service failed: '+process.communicate(timeout=5)[1])
            token=json.loads(info.read_text())['url'].split('#token=')[1]
            opener=urllib.request.build_opener(urllib.request.ProxyHandler({}))
            def call(path,body=None):
                request=urllib.request.Request('http://127.0.0.1:18895/api/'+path,data=json.dumps(body).encode() if body is not None else None,headers={'X-App-Token':token,'Content-Type':'application/json'})
                with opener.open(request,timeout=10) as response:return json.load(response)
            status=call('status');assert not status['active'] and not status['current']
            assert status['configured'] and status['backup_configured']
            assert 'offline-primary-for-package-test' not in json.dumps(status)
            if status['baseline']:
                with sqlite3.connect(root/'状态/采集状态.sqlite') as db:
                    original=db.execute("SELECT status FROM jobs WHERE id='stock_basic'").fetchone()
                if original and original[0]=='complete':assert len(call('universe?api=stock_basic'))>0
            call('shutdown',{});process.wait(15);assert process.returncode==0
            with sqlite3.connect(root/'状态/采集状态.sqlite') as db:assert db.execute('SELECT COUNT(*) FROM requests').fetchone()[0]==0
            assert not list((root/'mydb').rglob('*.json'))
            print(json.dumps(dict(compiled_configure=True,baseline_checkpoint_preserved=bool(status['baseline']),catalog_cache_query=True,service_http=True,graceful_shutdown=True,remote_requests=0),ensure_ascii=False))
        finally:
            if process.poll() is None:process.terminate();process.wait(110)

if __name__=='__main__':verify(sys.argv[1])
