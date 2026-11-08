"""Import a sanitized Windows checkpoint once, without credentials or old data archives."""
import hashlib
import json
from pathlib import Path

def canonical(value):return json.dumps(value,ensure_ascii=False,sort_keys=True,separators=(',',':')).encode()

def load(store,catalog,root):
    root=Path(root);folder=root/'基线';manifest=folder/'本机采集基线.json'
    if not manifest.exists():return None
    value=json.loads(manifest.read_text(encoding='utf-8'));ident=value.pop('manifest_id')
    if hashlib.sha256(canonical(value)).hexdigest()!=ident:raise ValueError('本机基线校验失败，禁止启动采集')
    imported=store.get('baseline')
    if imported:
        if imported['id']!=ident:raise ValueError('云端已有不同基线，禁止覆盖现有进度；请先核对迁移方案')
        return imported
    if store.rows('SELECT id FROM jobs LIMIT 1'):raise ValueError('目标已经有任务，禁止用本机基线覆盖云端进度')
    entries={e['id']:e for e in catalog};caches=[]
    for item in value['catalog_files']:
        path=(folder/item['relative_path']).resolve()
        if folder.resolve() not in path.parents:raise ValueError('基线缓存路径越界')
        if hashlib.sha256(path.read_bytes()).hexdigest()!=item['sha256']:raise ValueError('目录基线文件校验失败')
        caches.append((item['job'],str(path),item['sha256']))
    with store.db() as db:
        db.execute('CREATE TABLE IF NOT EXISTS baseline_catalog(job TEXT,path TEXT PRIMARY KEY,sha256 TEXT)')
        db.execute('CREATE TABLE IF NOT EXISTS baseline_files(key TEXT PRIMARY KEY,job TEXT,records INTEGER,sha256 TEXT)')
        for row in value['jobs']:
            if row['id'] not in entries:raise ValueError('本机基线接口不在当前目录：'+row['id'])
            p=row['plan'];p['channel']='auto'
            status=row['status'];error=''
            if entries[row['id']].get('independent'):status='excluded';error='Linux版固定排除独立权限项目'
            elif status in ('failed','retry'):status='pending';error='沿用本机断点，云端重新检查原失败批次'
            db.execute('INSERT INTO jobs(id,title,group_name,plan,cursor,status,error,records,batches,priority) VALUES(?,?,?,?,?,?,?,?,?,?)',(row['id'],row['title'],row['group_name'],json.dumps(p),json.dumps(row['cursor']),status,error,row['records'],row['batches'],row['priority']))
        db.executemany('INSERT INTO baseline_catalog VALUES(?,?,?)',caches)
        db.executemany('INSERT INTO baseline_files VALUES(?,?,?,?)',[(f['key'],f['job'],f['records'],f['sha256']) for f in value['files']])
        for h in value.get('history_summary',[]):
            db.execute('INSERT INTO history_summary VALUES(?,?,?,?,?,?,?)',tuple(h[k] for k in ('job','variant','first_start','first_end','first_date','empty_streak','empty_count')))
        for h in value.get('history_checks',[]):
            db.execute('INSERT INTO history_checks VALUES(?,?,?,?,?,?)',tuple(h[k] for k in ('job','variant','start','end','records','time')))
        summary=dict(id=ident,created_at=value['created_at'],files=len(value['files']),records=sum(f['records'] for f in value['files']),jobs=len(value['jobs']))
        db.execute('INSERT OR REPLACE INTO settings VALUES(?,?)',('baseline',json.dumps(summary)))
    store.log('已继承本机采集基线：'+str(summary['jobs'])+'项任务；不重新请求已经提交的数据批次')
    return summary
