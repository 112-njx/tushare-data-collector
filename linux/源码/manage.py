import getpass
import json
import os
from pathlib import Path
from core import secret,PROXY_URL,day
from linux_engine import Store,Engine
from bulk import build

def configure(root):
    root=Path(root);os.environ['COLLECTOR_HOME']=str(root)
    import fcntl
    (root/'状态').mkdir(parents=True,exist_ok=True,mode=0o700)
    lock=open(root/'状态/服务.lock','a')
    try:fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    except BlockingIOError:raise SystemExit('请先停止服务再配置，避免并发修改断点')
    store=Store(root/'状态')
    primary=getpass.getpass('主Token（留空保留已有配置）：').strip()
    backup=getpass.getpass('备用Token（留空保留已有配置）：').strip()
    for name,value in [('account',primary),('backup_account',backup)]:
        if value:
            if not 16<=len(value)<=1024:raise SystemExit('Token长度不合法，未启动采集')
            store.set(name,secret(value))
    if not any(store.get(k) for k in ('account','backup_account')):raise SystemExit('至少需要一个有效Token')
    store.set('connection',dict(url=PROXY_URL,default_channel='main'));store.set('channel_health',{})
    source=Path(__file__).parent/'catalog.json'
    if not source.exists():
        import sys
        source=Path(sys._MEIPASS)/'catalog.json'
    entries=json.loads(source.read_text(encoding='utf-8'))['entries']
    from baseline import load
    load(store,entries,root)
    engine=Engine(store,entries,data_root=root/'mydb/Tushare')
    report=build(engine,'19900101',day(),False,True)
    store.set('resume_requested',True);store.set('resume_selected',None)
    print('账号已加密保存；未请求数据。服务下次启动自动续采。计划：'+json.dumps(report['counts'],ensure_ascii=False))
