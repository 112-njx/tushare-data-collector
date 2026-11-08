"""Export validated coverage metadata without credentials or historical data."""
import argparse
import hashlib
import json
import sqlite3
from pathlib import Path
from baseline import canonical

def export(windows_root,standalone_root,output):
    windows_root=Path(windows_root);standalone_root=Path(standalone_root)
    pages=[];intervals=[];scopes={}
    catalog=json.loads((Path(__file__).parent/'catalog.json').read_text(encoding='utf-8'))['entries'];caps={e['id']:e['cap'] for e in catalog}
    data_root=(windows_root.parent/'mydb/Tushare').resolve()
    def archive(api,path,count):
        path=Path(path).resolve()
        if data_root not in path.parents:raise ValueError('本地归档路径越界')
        content=path.read_bytes();value=json.loads(content)
        if value.get('dataset')!=api or len(value.get('items',[]))!=count:raise ValueError('本地归档索引不一致：'+api)
        if count:
            digest=hashlib.sha256(json.dumps({'fields':value['fields'],'items':value['items']},ensure_ascii=False,sort_keys=True).encode()).hexdigest()
            pages.append(dict(api=api,params=value['params'],n=count,digest=digest,sha256=hashlib.sha256(content).hexdigest()))
    with sqlite3.connect('file:'+str(windows_root/'状态/采集状态.sqlite')+'?mode=ro',uri=True) as db:
        db.row_factory=sqlite3.Row;db.execute('BEGIN')
        for job in db.execute('SELECT id,plan FROM jobs'):
            plan=json.loads(job['plan']);scopes[job['id']]=plan.get('resolved_params',plan.get('params',{}))
        for f in db.execute('SELECT job,path,records FROM files'):archive(f['job'],f['path'],f['records'])
        for h in db.execute('SELECT * FROM history_checks'):
            if h['records']<caps.get(h['job'],0):intervals.append(dict(api=h['job'],variant=h['variant'],start=h['start'],end=h['end']))
    with sqlite3.connect('file:'+str(standalone_root/'采集进度.sqlite')+'?mode=ro',uri=True) as db:
        db.row_factory=sqlite3.Row;db.execute('BEGIN')
        for row in db.execute("SELECT * FROM requests WHERE status IN ('saved','empty')"):
            scopes[row['api']]={}
            if row['status']=='saved':archive(row['api'],row['path'],row['rows'])
            if row['rows']<caps[row['api']]:intervals.append(dict(api=row['api'],variant=0,start=row['trade_date'],end=row['trade_date']))
    value=dict(schema=1,pages=pages,intervals=intervals,scopes=scopes);value['manifest_id']=hashlib.sha256(canonical(value)).hexdigest()
    output=Path(output);output.parent.mkdir(parents=True,exist_ok=True);output.write_text(json.dumps(value,ensure_ascii=False),encoding='utf-8')
    return dict(pages=len(pages),intervals=len(intervals),id=value['manifest_id'])

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--windows-root',required=True);p.add_argument('--standalone-root',required=True);p.add_argument('--output',required=True);a=p.parse_args()
    print(json.dumps(export(a.windows_root,a.standalone_root,a.output)))
