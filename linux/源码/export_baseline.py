"""Windows-only packaging helper. Only whitelisted checkpoint columns leave the local DB."""
import argparse
import datetime as dt
import hashlib
import json
import sqlite3
import urllib.request
from pathlib import Path
from baseline import canonical
CACHE_SOURCES={'stock_basic','fund_basic','etf_basic','cb_basic','index_basic','fut_basic','opt_basic','hk_basic','us_basic','ths_index','dc_index','tdx_index','factor_list','sge_basic','fx_obasic'}

def export(windows_root,output):
    windows_root=Path(windows_root);output=Path(output)
    info=windows_root/'状态/当前服务.json'
    if info.exists():
        v=json.loads(info.read_text(encoding='utf-8'));key=v['url'].split('#token=')[1]
        req=urllib.request.Request('http://127.0.0.1:18793/api/status',headers={'X-App-Token':key})
        try:
            with urllib.request.build_opener(urllib.request.ProxyHandler({})).open(req,timeout=3) as response:s=json.load(response)
        except OSError:raise ValueError('本机服务状态不明，先停止Windows采集器后再导出基线') from None
        if s['active'] or s['current']:raise ValueError('请先暂停Windows采集并等待当前请求结束，再导出基线')
    output.mkdir(parents=True,exist_ok=True)
    with sqlite3.connect('file:'+str(windows_root/'状态/采集状态.sqlite')+'?mode=ro',uri=True) as db:
        db.row_factory=sqlite3.Row;db.execute('BEGIN')
        jobs=[dict(r) for r in db.execute('SELECT id,title,group_name,plan,cursor,status,records,batches,priority FROM jobs ORDER BY priority,rowid')]
        files=[dict(r) for r in db.execute('SELECT key,job,path,records FROM files')]
        summaries=[dict(r) for r in db.execute('SELECT * FROM history_summary')]
        checks=[dict(r) for r in db.execute('SELECT * FROM history_checks')]
    for row in jobs:row['plan']=json.loads(row['plan']);row['cursor']=json.loads(row['cursor'])
    data_root=(windows_root.parent/'mydb/Tushare').resolve();metadata=[];caches=[]
    for f in files:
        path=Path(f['path']).resolve()
        if data_root not in path.parents:raise ValueError('本机归档路径越界，不能导出基线')
        content=path.read_bytes();d=json.loads(content)
        if d.get('dataset')!=f['job'] or len(d.get('items',[]))!=f['records']:raise ValueError('本机归档与已提交索引不一致')
        digest=hashlib.sha256(content).hexdigest();metadata.append(dict(key=f['key'],job=f['job'],records=f['records'],sha256=digest))
        if f['job'] in CACHE_SOURCES and f['records']:
            relative=Path('目录缓存')/f['job']/(f['key']+'.json');dest=output/relative;dest.parent.mkdir(parents=True,exist_ok=True);dest.write_bytes(content)
            caches.append(dict(job=f['job'],relative_path=relative.as_posix(),sha256=digest))
    value=dict(schema=1,created_at=dt.datetime.now().isoformat(timespec='seconds'),jobs=jobs,files=metadata,catalog_files=caches,history_summary=summaries,history_checks=checks)
    value['manifest_id']=hashlib.sha256(canonical(value)).hexdigest()
    (output/'本机采集基线.json').write_text(json.dumps(value,ensure_ascii=False,indent=2),encoding='utf-8')
    return dict(jobs=len(jobs),files=len(metadata),records=sum(f['records'] for f in metadata),catalog_files=len(caches),id=value['manifest_id'])

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--windows-root',required=True);p.add_argument('--output',required=True);args=p.parse_args()
    print(json.dumps(export(args.windows_root,args.output),ensure_ascii=False))
