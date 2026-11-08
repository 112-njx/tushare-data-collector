"""Provider routing and immutable, secret-free local coverage import."""
import hashlib
import json
from pathlib import Path
from bulk import FOUNDATIONS
from baseline import canonical

DATAHUB_URL='https://datahubco.com/api/tushare'
FINANCIAL={'income','balancesheet','cashflow','fina_indicator','fina_audit','fina_mainbz','forecast','express','disclosure_date','dividend','hk_income','hk_balancesheet','hk_cashflow','hk_fina_indicator','us_income','us_balancesheet','us_cashflow','us_fina_indicator'}

def financial(entry):
    return entry['id'].removesuffix('_vip') in FINANCIAL or '财务数据' in entry.get('group','')

def routed_ids(catalog):
    ordered=sorted(catalog,key=lambda e:(FOUNDATIONS.index(e['id']) if e['id'] in FOUNDATIONS else 100,e['group'],e['id']))
    starts=[i for i,e in enumerate(ordered) if e['id'] in ('top_inst','top_list')]
    return {e['id'] for e in ordered[min(starts):]} if starts else set()

def page_key(params):
    params=dict(params)
    if not params.get('offset'):params.pop('offset',None);params.pop('limit',None)
    return canonical(params).decode()

def import_coverage(store,root):
    path=Path(root)/'基线'/'本地增量覆盖.json'
    with store.db() as db:
        db.execute('CREATE TABLE IF NOT EXISTS local_pages(api TEXT,params TEXT,n INTEGER,digest TEXT,PRIMARY KEY(api,params))')
        db.execute('CREATE TABLE IF NOT EXISTS local_intervals(api TEXT,variant INTEGER,start TEXT,end TEXT,PRIMARY KEY(api,variant,start,end))')
        db.execute('CREATE TABLE IF NOT EXISTS coverage_imports(id TEXT PRIMARY KEY)')
        db.execute('CREATE TABLE IF NOT EXISTS coverage_scopes(api TEXT PRIMARY KEY,scope TEXT)')
    if not path.exists():return
    value=json.loads(path.read_text(encoding='utf-8'));ident=value.pop('manifest_id')
    if hashlib.sha256(canonical(value)).hexdigest()!=ident:raise ValueError('本地增量覆盖清单校验失败')
    with store.db() as db:
        if db.execute('SELECT 1 FROM coverage_imports WHERE id=?',(ident,)).fetchone():return
        for p in value['pages']:
            db.execute('INSERT OR REPLACE INTO local_pages VALUES(?,?,?,?)',(p['api'],page_key(p['params']),p['n'],p['digest']))
        for p in value['intervals']:
            db.execute('INSERT OR IGNORE INTO local_intervals VALUES(?,?,?,?)',(p['api'],p['variant'],p['start'],p['end']))
        for api,scope in value.get('scopes',{}).items():db.execute('INSERT OR REPLACE INTO coverage_scopes VALUES(?,?)',(api,canonical(scope).decode()))
        db.execute('INSERT INTO coverage_imports VALUES(?)',(ident,))
    store.log('已合并本地已提交覆盖清单，保留云端原断点：'+str(len(value['pages']))+'页；'+str(len(value['intervals']))+'区间')
