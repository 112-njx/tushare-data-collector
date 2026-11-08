import json
import logging
import os
from pathlib import Path
import secrets
import signal
import socket
import sys
import threading
import urllib.parse
import urllib.request
import webbrowser
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
from core import secret,proxy_address,PROXY_URL
from linux_engine import Engine,Store
from credentials import access_token
from ui import HTML

ROOT=Path(os.environ.get('COLLECTOR_HOME',Path(sys.executable).resolve().parents[1] if getattr(sys,'frozen',False) else Path(__file__).resolve().parents[1]))
os.environ.setdefault('COLLECTOR_HOME',str(ROOT))
ASSETS=Path(getattr(sys,'_MEIPASS',Path(__file__).parent))
PORT=int(os.environ.get('COLLECTOR_PORT','18794'))

class Server(ThreadingHTTPServer):
    # fcntl service lock prevents duplicate collectors; reuse permits quick Linux restarts.
    allow_reuse_address=os.name=='posix'
    daemon_threads=True
    def server_bind(self):
        if hasattr(socket,'SO_EXCLUSIVEADDRUSE'):self.socket.setsockopt(socket.SOL_SOCKET,socket.SO_EXCLUSIVEADDRUSE,1)
        super().server_bind()

def main(open_browser=True):
    info_path=ROOT/'状态/当前服务.json'
    if open_browser and info_path.exists():
        try:
            url=json.loads(info_path.read_text(encoding='utf-8'))['url']
            parsed=urllib.parse.urlsplit(url)
            if parsed.scheme=='http' and parsed.netloc==f'127.0.0.1:{PORT}':
                key=urllib.parse.parse_qs(parsed.fragment)['token'][0]
                req=urllib.request.Request(f'http://127.0.0.1:{PORT}/api/status',headers={'X-App-Token':key})
                with urllib.request.build_opener(urllib.request.ProxyHandler({})).open(req,timeout=2) as response:
                    if response.status==200:webbrowser.open(url);return
        except Exception:pass
    root=ROOT/'状态';root.mkdir(parents=True,exist_ok=True,mode=0o700)
    import fcntl
    lock=open(root/'服务.lock','a')
    try:fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    except BlockingIOError:raise ValueError('采集服务已经运行，禁止启动重复实例')
    store=Store(root)
    catalog=json.loads((ASSETS/'catalog.json').read_text(encoding='utf-8'))
    from baseline import load
    load(store,catalog['entries'],ROOT)
    engine=Engine(store,catalog['entries'],data_root=ROOT/'mydb'/'Tushare');token=access_token()
    hosts={f'127.0.0.1:{PORT}',f'localhost:{PORT}'}
    class Handler(BaseHTTPRequestHandler):
        def log_message(self,*args):pass
        def send(self,value,code=200,html=False):
            b=value.encode() if html else json.dumps(value,ensure_ascii=False).encode()
            self.send_response(code);self.send_header('Content-Type','text/html; charset=utf-8' if html else 'application/json; charset=utf-8');self.send_header('Cache-Control','no-store');self.send_header('X-Content-Type-Options','nosniff');self.send_header('Content-Length',str(len(b)));self.end_headers();self.wfile.write(b)
        def auth(self):return self.headers.get('Host') in hosts and secrets.compare_digest(self.headers.get('X-App-Token',''),token)
        def do_GET(self):
            if self.headers.get('Host') not in hosts:self.send({'error':'无效访问地址'},403);return
            if self.path=='/':self.send(HTML,html=True);return
            if not self.auth():self.send({'error':'访问令牌无效，请使用启动时打开的完整地址'},403);return
            try:
                if self.path=='/api/status':self.send(engine.status())
                elif self.path=='/api/catalog':self.send(catalog)
                elif self.path.startswith('/api/preview?'):
                    key=urllib.parse.parse_qs(urllib.parse.urlsplit(self.path).query).get('key',[''])[0]
                    rows=store.rows('SELECT path FROM files WHERE key=?',(key,))
                    if not rows:raise ValueError('归档不存在')
                    p=Path(rows[0]['path']).resolve()
                    if engine.data_root.resolve() not in p.parents:raise ValueError('归档路径越界')
                    value=json.loads(p.read_text(encoding='utf-8'));value['items']=value['items'][:50];self.send(value)
                elif self.path.startswith('/api/universe?'):
                    api=urllib.parse.parse_qs(urllib.parse.urlsplit(self.path).query).get('api',['stock_basic'])[0]
                    if api not in ('stock_basic','fund_basic','etf_basic','fut_basic','opt_basic','index_basic'):raise ValueError('证券目录类型无效')
                    self.send(engine.catalog_values(api,'ts_code'))
                else:self.send({'error':'接口不存在'},404)
            except Exception as e:self.send({'error':str(e)[:900]},400)
        def do_POST(self):
            if not self.auth():self.send({'error':'访问令牌无效'},403);return
            if self.headers.get('Origin') and self.headers['Origin'] not in {'http://'+h for h in hosts}:self.send({'error':'请求来源无效'},403);return
            try:
                n=int(self.headers.get('Content-Length',0))
                if not 0<n<=1024*1024:raise ValueError('请求大小无效')
                self.connection.settimeout(10);v=json.loads(self.rfile.read(n))
                if not isinstance(v,dict):raise ValueError('参数结构无效')
                action=self.path.removeprefix('/api/')
                with engine.guard:
                    if action=='pause':engine.pause()
                    elif action=='shutdown':engine.pause();engine.closing=True;engine.wake.set()
                    else:
                        engine.paused()
                        if action=='account':
                            t=v.get('token','').strip()
                            backup=v.get('backup_token','').strip()
                            address=proxy_address(v.get('url',PROXY_URL));default=v.get('default_channel','main')
                            if default not in ('main','backup'):raise ValueError('默认通道无效')
                            if t and not 16<=len(t)<=1024:raise ValueError('主Token格式不完整')
                            if backup and not 16<=len(backup)<=1024:raise ValueError('备用Token格式不完整')
                            if not t and not backup and not any(store.get(k) for k in ('account','backup_account')):raise ValueError('首次配置至少填写一个Token')
                            encoded=secret(t) if t else None;encoded_backup=secret(backup) if backup else None
                            if default=='backup' and not backup and not store.get('backup_account'):raise ValueError('请先填写备用Token')
                            with store.db() as db:
                                if encoded:db.execute('INSERT OR REPLACE INTO settings VALUES(?,?)',('account',json.dumps(encoded)))
                                if encoded_backup:db.execute('INSERT OR REPLACE INTO settings VALUES(?,?)',('backup_account',json.dumps(encoded_backup)))
                                db.execute('INSERT OR REPLACE INTO settings VALUES(?,?)',('connection',json.dumps(dict(url=address,default_channel=default))))
                            engine.credentials_changed(bool(t),bool(backup))
                            store.log('代理地址及主/备用Token已加密配置；未发起请求，不回退官方直连')
                        elif action=='rate':
                            rpm=v.get('rpm')
                            if type(rpm) is not int or not 1<=rpm<=400:raise ValueError('全局上限为1至400次/分钟')
                            store.set('rpm',rpm)
                        elif action=='bulk':
                            from bulk import build
                            result=build(engine,v['start'],v['end'],False,v.get('include_overlap',True))
                            self.send({'message':'全量计划已生成，保持暂停；请查看覆盖报告后点击开始','report':result});return
                        elif action=='switch-token':engine.switch_token(v.get('channel'))
                        elif action=='plan':engine.plan(v['id'],v['params'],v['start'],v['end'],v['mode'],v['key'],v.get('allow_empty',False),v.get('confirmed',False),channel=v.get('channel','auto'))
                        elif action=='revise':engine.plan(v['id'],v['params'],v['start'],v['end'],v['mode'],v['key'],v.get('allow_empty',False),v.get('confirmed',False),revise=True,replay=v.get('replay',False),channel=v.get('channel','auto'))
                        elif action=='start':engine.start(v.get('id'))
                        elif action=='update':engine.update(v['id'])
                        else:raise ValueError('操作不存在')
                self.send({'message':'操作已完成；采集请求仍遵守全局和接口限频'})
            except Exception as e:self.send({'error':str(e)[:900]},400)
    server=Server((os.environ.get('COLLECTOR_BIND','127.0.0.1'),PORT),Handler);server.timeout=.5
    url=f'http://127.0.0.1:{PORT}/#token={token}';info=root/'当前服务.json'
    info.write_text(json.dumps({'pid':os.getpid(),'url':url},ensure_ascii=False),encoding='utf-8');info.chmod(0o600)
    def stop(*_):engine.active=False;engine.closing=True;engine.wake.set()
    signal.signal(signal.SIGINT,stop);signal.signal(signal.SIGTERM,stop)
    engine.resume_on_boot();engine.thread.start();store.log('Ubuntu采集服务已启动；按持久化运行意愿恢复，双通道协同，独立权限接口已排除')
    if open_browser:webbrowser.open(url)
    try:
        while not engine.closing or engine.current:server.handle_request()
    finally:
        stop();engine.thread.join(100);server.server_close();info.unlink(missing_ok=True);lock.close()

if __name__=='__main__':
    os.umask(0o077)
    if '--version' in sys.argv:print('Tushare Linux Collector 1.0 · Ubuntu 24.04 x86_64')
    elif '--configure' in sys.argv:
        from manage import configure
        configure(ROOT)
    elif '--url' in sys.argv:print((ROOT/'状态/当前服务.json').read_text(encoding='utf-8'))
    else:main('--no-browser' not in sys.argv)
