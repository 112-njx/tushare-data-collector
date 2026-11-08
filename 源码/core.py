import base64
import calendar
import ctypes
import datetime as dt
import hashlib
import http.client
import itertools
import json
import os
from pathlib import Path
import re
import sqlite3
import threading
import time
import urllib.request
import urllib.error
import urllib.parse
from dates import align,extend_snapshot,options,spec,request_dates
from history import preferred_schema,default_window,bounds,next_day,earliest

DATA_ROOT=Path('D:/Free_stockdb/stockdb/mydb/Tushare')
PROXY_URL='http://tus.sxdata.top/tu_share/'
MAX_ATTEMPTS=3
RETRY_SECONDS=60
class RemoteError(RuntimeError):
    def __init__(self,message,retryable=False,retry_after=0,ip_limited=False):
        super().__init__(message);self.retryable=retryable;self.retry_after=retry_after;self.ip_limited=ip_limited
class CredentialError(RemoteError):
    def __init__(self,message,kind='token'):super().__init__(message,False);self.kind=kind
class DependencyPending(ValueError):pass
class TransientDataError(ValueError):
    retryable=True
class EmptyDataError(TransientDataError):pass
class NarrowWindow(ValueError):
    def __init__(self,days):super().__init__('返回达到上限，缩小时间区间后重新核对');self.days=days
def proxy_address(value):
    if value!=PROXY_URL:raise ValueError('当前版本只允许发货信息指定的代理地址，不会回退官方或旧供应商')
    return value
DENIED={'daily','weekly','monthly','pro_bar','adj_factor','moneyflow','stk_mins','rt_min','rt_k','rt_min_daily','stk_week_month_adj','stk_weekly_monthly','bak_daily','p_save','p_delete','p_list','p_get'}
def stamp():return dt.datetime.now().isoformat(timespec='seconds')
def day():return dt.date.today().strftime('%Y%m%d')
def safe(s):return re.sub(r'[^\w.\-]','_',s)[:90]

class Blob(ctypes.Structure):
    _fields_=[('size',ctypes.c_ulong),('data',ctypes.POINTER(ctypes.c_ubyte))]
def secret(value,decode=False):
    if os.name!='nt':raise ValueError('账号存储使用Windows账户加密，请在Windows配置Token')
    data=base64.b64decode(value) if decode else value.encode()
    buf=ctypes.create_string_buffer(data);src=Blob(len(data),ctypes.cast(buf,ctypes.POINTER(ctypes.c_ubyte)));dst=Blob()
    fn=ctypes.windll.crypt32.CryptUnprotectData if decode else ctypes.windll.crypt32.CryptProtectData
    if not fn(ctypes.byref(src),None,None,None,None,1,ctypes.byref(dst)):raise ctypes.WinError()
    try:
        result=ctypes.string_at(dst.data,dst.size)
        return result.decode() if decode else base64.b64encode(result).decode()
    finally:ctypes.windll.kernel32.LocalFree(dst.data)

class Store:
    def __init__(self,root):
        self.root=Path(root);self.root.mkdir(parents=True,exist_ok=True);self.path=self.root/'采集状态.sqlite';self.lock=threading.RLock()
        with self.db() as db:
            db.executescript('CREATE TABLE IF NOT EXISTS settings(key TEXT PRIMARY KEY,value TEXT); CREATE TABLE IF NOT EXISTS jobs(id TEXT PRIMARY KEY,title TEXT,group_name TEXT,plan TEXT,cursor TEXT,status TEXT,error TEXT DEFAULT \'\',records INTEGER DEFAULT 0,batches INTEGER DEFAULT 0); CREATE TABLE IF NOT EXISTS events(id INTEGER PRIMARY KEY,time TEXT,message TEXT); CREATE TABLE IF NOT EXISTS requests(time REAL,api TEXT); CREATE TABLE IF NOT EXISTS files(key TEXT PRIMARY KEY,job TEXT,path TEXT,records INTEGER,time TEXT);')
            db.execute('CREATE TABLE IF NOT EXISTS revisions(id INTEGER PRIMARY KEY,job TEXT,time TEXT,previous_plan TEXT,previous_cursor TEXT,previous_status TEXT)')
            db.execute('CREATE TABLE IF NOT EXISTS retries(job TEXT PRIMARY KEY,request_key TEXT,attempts INTEGER,not_before REAL,last_error TEXT)')
            if 'priority' not in {r[1] for r in db.execute('PRAGMA table_info(jobs)')}:db.execute('ALTER TABLE jobs ADD COLUMN priority INTEGER NOT NULL DEFAULT 100')
            db.execute('CREATE TABLE IF NOT EXISTS history_checks(job TEXT,variant INTEGER,start TEXT,end TEXT,records INTEGER,time TEXT)')
            db.execute('CREATE TABLE IF NOT EXISTS history_summary(job TEXT,variant INTEGER,first_start TEXT,first_end TEXT,first_date TEXT,empty_streak INTEGER DEFAULT 0,empty_count INTEGER DEFAULT 0,PRIMARY KEY(job,variant))')
        self.set('rpm',min(400,self.get('rpm',400)))
    def db(self):
        from contextlib import contextmanager
        @contextmanager
        def connect():
            db=sqlite3.connect(self.path,timeout=20);db.row_factory=sqlite3.Row
            try:
                with db:yield db
            finally:db.close()
        return connect()
    def rows(self,sql,args=()):
        with self.db() as db:return [dict(r) for r in db.execute(sql,args)]
    def get(self,key,default=None):
        r=self.rows('SELECT value FROM settings WHERE key=?',(key,));return json.loads(r[0]['value']) if r else default
    def set(self,key,value):
        with self.db() as db:db.execute('INSERT OR REPLACE INTO settings VALUES(?,?)',(key,json.dumps(value,ensure_ascii=False)))
    def log(self,message):
        with self.db() as db:
            db.execute('INSERT INTO events(time,message) VALUES(?,?)',(stamp(),message))
            db.execute('DELETE FROM events WHERE id<(SELECT MAX(id)-3000 FROM events)')

def next_date(value,mode):
    d=dt.datetime.strptime(value,'%Y%m%d').date()
    if mode in ('month','quarter'):
        m=d.month+(3 if mode=='quarter' else 1);y=d.year+(m-1)//12;m=(m-1)%12+1
        d=dt.date(y,m,calendar.monthrange(y,m)[1]) if mode=='quarter' else dt.date(y,m,1)
    elif mode=='year':d=dt.date(d.year+1,1,1)
    else:d+=dt.timedelta(days=1)
    return d.strftime('%Y%m%d')

class Engine:
    def __init__(self,store,catalog,transport=None,data_root=DATA_ROOT):
        self.store=store;self.catalog={e['id']:e for e in catalog};self.transport=transport or self.http
        self.data_root=Path(data_root);self.guard=threading.RLock();self.active=False;self.current=None;self.closing=False;self.selected=None;self.next_job=None;self.wake=threading.Event()
        self.thread=threading.Thread(target=self.loop,daemon=True)
        self.upgrade_history()
    def upgrade_history(self):
        for j in self.store.rows('SELECT * FROM jobs'):
            p=json.loads(j['plan']);c=json.loads(j['cursor'])
            if j['id'] not in self.catalog or p.get('bulk_placeholder') or p.get('history_version')==1 or p.get('mode')=='snapshot':continue
            before=j['plan'];p['history_version']=1
            # Keep already fetched dates and unfinished pages intact. Only bulk
            # plans at a date boundary may adopt larger range requests.
            schema=preferred_schema(self.catalog[j['id']]) if p.get('bulk') else spec(self.catalog[j['id']],p['mode'],p['key'])
            if not c.get('offset') and not c.get('previous') and schema['mode']=='range':p.update(mode='range',key=schema['key'],schema=schema)
            with self.store.db() as db:
                db.execute('INSERT INTO revisions(job,time,previous_plan,previous_cursor,previous_status) VALUES(?,?,?,?,?)',(j['id'],stamp(),before,j['cursor'],j['status']))
                db.execute('UPDATE jobs SET plan=? WHERE id=?',(json.dumps(p),j['id']))
    def http(self,api,params,fields):
        connection=self.store.get('connection',{})
        address=proxy_address(connection.get('url',PROXY_URL))
        channel=self.channel_for(api)
        encoded=self.store.get('backup_account' if channel=='backup' else 'account')
        if not encoded:raise ValueError('尚未配置所选代理通道的Token')
        # This proxy rejects both the extended documentation fields and an empty
        # projection. Use its verified directory columns; other datasets stay strict.
        projection='ts_code,name' if api=='stock_basic' else ','.join(fields)
        payload=dict(api_name=api,token=secret(encoded,True),params=params,fields=projection)
        req=urllib.request.Request(address,data=json.dumps(payload).encode(),headers={'Content-Type':'application/json'})
        opener=urllib.request.build_opener(urllib.request.ProxyHandler({}),NoRedirect())
        try:
            with opener.open(req,timeout=90) as r:result=json.load(r)
        except urllib.error.HTTPError as exc:
            code=exc.code;retry_header=exc.headers.get('Retry-After','') if exc.headers else '';exc.close()
            if code in (401,403):raise CredentialError('代理拒绝访问，HTTP '+str(code),'token' if code==401 else 'permission') from None
            delay=0
            if code==429:
                try:delay=max(0,float(retry_header))
                except ValueError:
                    try:
                        from email.utils import parsedate_to_datetime
                        delay=max(0,parsedate_to_datetime(retry_header).timestamp()-time.time())
                    except (ValueError,TypeError,OverflowError):pass
            raise RemoteError('代理HTTP错误：'+str(code),code in (408,429) or code>=500,delay,code==429) from None
        except (urllib.error.URLError,TimeoutError,ConnectionError,OSError,http.client.HTTPException,UnicodeDecodeError,json.JSONDecodeError) as exc:raise RemoteError('代理连接、超时或响应解析失败：'+type(exc).__name__,True) from None
        if not isinstance(result,dict):raise RemoteError('代理返回结构异常',True)
        if result.get('code')!=0:
            message=self.redact(str(result.get('msg','未知响应错误')))[:600]
            # Tushare appends a permission-document link to ordinary quota errors.
            # Strip only that referral, preserving genuine denials before it.
            classification=re.sub(r'权限(?:的)?(?:具体)?详情.*$','',message,flags=re.S)
            if re.search(r'\binvalid_params\b',classification,re.I):
                raise RemoteError('代理请求参数或字段不兼容：'+message,False)
            blocked=bool(re.search(r'封禁|封号|token.{0,12}(?:无效|错误|过期)|(?:invalid|expired)[_\s-]*token|token[_\s-]*(?:invalid|expired)|权限|无权|认证|授权|禁止|unauthorized|forbidden',classification,re.I))
            transient=bool(re.search(r'频率|限流|每分钟|太频繁|too many|rate.limit|超时|timeout|系统.*错误|内部.*错误|服务.*异常|稍后|繁忙|temporar|unavailable',message,re.I))
            if blocked and not re.search(r'封禁|封号',classification):
                kind='token' if re.search(r'token|令牌|认证|invalid|unauthorized',classification,re.I) else 'permission'
                raise CredentialError('代理返回错误：'+message,kind)
            wait=re.search(r'(?:等待|wait\s*)(\d+(?:\.\d+)?)\s*(?:秒|seconds?|s\b)',message,re.I)
            delay=float(wait.group(1)) if wait else 0
            ip_limited=bool(re.search(r'ip_rate_limited|来源IP.*(?:限制|额度)',classification,re.I))
            raise RemoteError('代理返回错误：'+message,(transient or ip_limited) and not blocked,delay,ip_limited)
        return result.get('data')
    def channel_for(self,api):
        rows=self.store.rows('SELECT plan FROM jobs WHERE id=?',(api,))
        configured=json.loads(rows[0]['plan']).get('channel','auto') if rows else 'auto'
        if configured=='auto':configured='main' if self.catalog[api].get('independent') else self.store.get('connection',{}).get('default_channel','main')
        if configured not in ('main','backup'):raise ValueError('代理通道配置无效')
        if configured=='backup' and self.catalog[api].get('independent'):raise ValueError('独立/特权接口请使用主通道，不自动切换或绕过权限')
        return configured
    def redact(self,message):
        for name in ('account','backup_account'):
            encoded=self.store.get(name)
            if encoded:
                try:message=message.replace(secret(encoded,True),'[已隐藏Token]')
                except Exception:pass
        return message
    def paused(self):
        if self.active or self.current:raise ValueError('请先暂停，并等待当前请求保存或超时')
        if self.closing:raise ValueError('服务正在关闭')
    def switch_token(self,channel):
        with self.guard:
            self.paused()
            if channel not in ('main','backup'):raise ValueError('通道无效')
            encoded=self.store.get('backup_account' if channel=='backup' else 'account')
            if not encoded:raise ValueError('所选Token尚未配置，请先在账号设置保存')
            secret(encoded,True) # Fail before changing any plan if Windows cannot decrypt it.
            issue=self.store.get('credential_issue');target=issue.get('job') if issue else self.selected
            rows=self.store.rows('SELECT * FROM jobs WHERE id=?',(target,)) if target else []
            if rows and channel=='backup' and self.catalog[target].get('independent'):raise ValueError('此项目需要主通道权限，请更换有效主Token')
            connection=self.store.get('connection',dict(url=PROXY_URL));connection['default_channel']=channel
            with self.store.db() as db:
                db.execute('INSERT OR REPLACE INTO settings VALUES(?,?)',('connection',json.dumps(connection)))
                if rows:
                    p=json.loads(rows[0]['plan']);p['channel']=channel
                    db.execute("UPDATE jobs SET plan=?,status=CASE WHEN status='failed' THEN 'pending' ELSE status END,error='' WHERE id=?",(json.dumps(p),target))
                    db.execute('DELETE FROM retries WHERE job=?',(target,))
                db.execute('DELETE FROM settings WHERE key=?',('credential_issue',))
            self.store.log('已手动切换至'+('主' if channel=='main' else '备用')+'Token；保持暂停，断点及全局冷却时间保留，请点击继续')
    def catalog_values(self,source,field):
        jobs=self.store.rows('SELECT status FROM jobs WHERE id=?',(source,))
        if not jobs or jobs[0]['status']!='complete':raise DependencyPending('等待目录完成：'+source)
        values=set()
        for item in self.store.rows('SELECT path FROM files WHERE job=? ORDER BY time',(source,)):
            path=Path(item['path']).resolve()
            if self.data_root.resolve() not in path.parents:raise ValueError('目录归档路径越界')
            data=json.loads(path.read_text(encoding='utf-8'))
            if field not in data['fields']:raise ValueError('目录缺少字段：'+source+'.'+field)
            index=data['fields'].index(field);values.update(str(r[index]) for r in data['items'] if r[index] is not None and str(r[index]))
        if not values:raise DependencyPending('目录没有可用代码，需核对：'+source)
        return sorted(values)
    def plan(self,ident,params,start,end,mode,key,allow_empty=False,confirmed=False,revise=False,replay=False,channel='auto'):
        with self.guard:
            self.paused();e=self.catalog.get(ident)
            if not e or e.get('excluded') or ident in DENIED:raise ValueError('此项目已排除或不存在')
            if (e.get('independent') or e.get('overlap') or e.get('retired')) and not confirmed:raise ValueError('请先确认独立权限或本地不重复范围')
            if not isinstance(params,dict):raise ValueError('请求参数必须为对象')
            if channel not in ('auto','main','backup'):raise ValueError('代理通道无效')
            if channel=='backup' and e.get('independent'):raise ValueError('独立/特权接口应选择主通道')
            if set(params)&{'token','api_name','fields','limit','offset'}:raise ValueError('参数包含保留字段')
            schema=spec(e,mode,key)
            for v in (start,end):dt.datetime.strptime(v,'%Y%m%d')
            if start>end or end>day():raise ValueError('日期范围无效，结束不能晚于今天')
            datefields={schema['key'],schema.get('end_key')} if mode!='snapshot' else set()
            for field in e['inputs']:
                alternative=ident in {'daily_basic','share_float'} and field['name']=='ts_code' and key in {'trade_date','ann_date'} and mode!='snapshot'
                if field['required'] and field['name'] not in datefields and not params.get(field['name']) and not alternative:raise ValueError('缺少必填字段：'+field['name'])
            start=align(start,mode)
            if start>end:raise ValueError('所选范围内没有完整报告期，请调整范围')
            values=list(params.values());count=1
            for v in values:count*=len(v) if isinstance(v,list) else 1
            if not 1<=count<=20000:raise ValueError('参数组合数量应为1至20000')
            plan=dict(params=params,start=start,end=end,mode=mode,key=key,schema=schema,allow_empty=bool(allow_empty),cap=e['cap'],offset=e.get('offset',False),channel=channel)
            if mode!='snapshot':plan['history_version']=1
            with self.store.db() as db:
                old=db.execute('SELECT * FROM jobs WHERE id=?',(ident,)).fetchone()
                cursor=dict(date=start,offset=0,variant=0,previous='')
                if old:
                    if not revise:raise ValueError('已有计划，请在任务行点击“编辑计划”修订，或直接续采')
                    before=json.loads(old['plan'])
                    if before.get('bulk_placeholder'):
                        before=dict(plan);old=dict(old);old['status']='pending';old['cursor']=json.dumps(cursor)
                    changed=any(before.get(k)!=plan[k] for k in ('params','start','mode','key')) or end<before['end']
                    if changed and not replay:raise ValueError('范围或参数已改变，请勾选“从新范围重新核对”；历史归档不会删除')
                    if not changed and not replay:cursor=json.loads(old['cursor'])
                    status='pending' if changed or replay or old['status']!='complete' or end>before['end'] else 'complete'
                    if status=='pending' and old['status']=='complete' and not changed and not replay:cursor=dict(date=before['end'],offset=0,variant=0,previous='')
                    db.execute('INSERT INTO revisions(job,time,previous_plan,previous_cursor,previous_status) VALUES(?,?,?,?,?)',(ident,stamp(),old['plan'],old['cursor'],old['status']))
                    db.execute("UPDATE jobs SET plan=?,cursor=?,status=?,error='' WHERE id=?",(json.dumps(plan),json.dumps(cursor),status,ident))
                    db.execute('DELETE FROM retries WHERE job=?',(ident,))
                    if changed or replay:
                        db.execute('DELETE FROM history_summary WHERE job=?',(ident,))
                else:
                    if revise:raise ValueError('待编辑计划不存在')
                    db.execute('INSERT INTO jobs(id,title,group_name,plan,cursor,status) VALUES(?,?,?,?,?,?)',(ident,e['title'],e['group'],json.dumps(plan),json.dumps(cursor),'pending'))
            self.store.log(('修订计划（保留归档和修订前断点）：' if revise else '建立计划：')+e['title']+'；默认暂停')
    def update(self,ident,end=None):
        self.paused();rows=self.store.rows('SELECT * FROM jobs WHERE id=?',(ident,))
        if not rows:raise ValueError('请先建立计划')
        j=rows[0];p=json.loads(j['plan']);c=json.loads(j['cursor']);converted=False
        end=day() if end is None else end
        dt.datetime.strptime(end,'%Y%m%d')
        if end<p['end'] or end>day():raise ValueError('更新截止日期不能早于原范围或晚于今天')
        if p['mode']=='snapshot':converted=extend_snapshot(self.catalog[ident],p)
        if converted and j['status']!='complete':raise ValueError('含固定日期的快照尚未完成，请编辑计划切换日期分片并重新核对')
        schema=spec(self.catalog[ident],p['mode'],p['key']);p['schema']=schema
        if j['status']=='complete':
            p.pop('resolved_params',None)
            start=(dt.datetime.strptime(p['end'],'%Y%m%d').date()-dt.timedelta(days=7)).strftime('%Y%m%d')
            c=dict(date=max(p['start'],start),offset=0,variant=0,previous='')
            if p['mode']=='quarter':c['date']=p['start'] # Reports may be revised: explicit full-period recheck.
            c['date']=align(c['date'],p['mode'])
        p['end']=end
        with self.store.db() as db:
            db.execute('INSERT INTO revisions(job,time,previous_plan,previous_cursor,previous_status) VALUES(?,?,?,?,?)',(ident,stamp(),j['plan'],j['cursor'],j['status']))
            db.execute("UPDATE jobs SET plan=?,cursor=?,status='pending',error='' WHERE id=?",(json.dumps(p),json.dumps(c),ident))
        self.store.log('更新计划范围至'+end+'：'+j['title']+'；保留未完成断点，已完成项目回查近期数据')
    def start(self,ident=None):
        with self.guard:
            self.paused()
            if not self.store.get('account'):raise ValueError('请先配置Token；保存Token不会发起请求')
            if ident:
                rows=self.store.rows('SELECT status FROM jobs WHERE id=?',(ident,))
                if not rows:raise ValueError('请先建立计划')
                if rows[0]['status']=='complete':raise ValueError('当前范围已完成，请先更新至今天')
                with self.store.db() as db:
                    if rows[0]['status']=='failed':db.execute('DELETE FROM retries WHERE job=?',(ident,))
                    db.execute("UPDATE jobs SET status='pending',error='' WHERE id=?",(ident,))
            elif not self.store.rows("SELECT id FROM jobs WHERE status IN ('pending','waiting')"):raise ValueError('没有待采集任务')
            self.selected=ident;self.next_job=ident;self.active=True;self.wake.set();self.store.log('开始单项续采：'+ident if ident else '开始按顺序采集待执行任务')
    def pause(self):
        with self.guard:self.active=False;self.wake.set()
        self.store.log('请求暂停；当前已发请求等待保存或超时，不派发新请求')
    def due(self,api,rpm):
        now=time.time();history=self.store.rows('SELECT time,api FROM requests WHERE time>? ORDER BY time',(now-60,))
        due=max(now,self.store.get('next_due',0));global_limit=min(400,self.store.get('rpm',400))
        if len(history)>=global_limit:due=max(due,history[-global_limit]['time']+60.1)
        same=[r for r in history if r['api']==api]
        if len(same)>=rpm:due=max(due,same[-rpm]['time']+60.1)
        return due
    def request_params(self,j):
        p=json.loads(j['plan']);c=json.loads(j['cursor'])
        if p.get('bulk_placeholder'):raise ValueError('此项目尚无可执行方案，请先编辑计划补足条件')
        values=p.get('resolved_params')
        if values is None:
            values={k:self.catalog_values(v['$catalog'],v.get('field','ts_code')) if isinstance(v,dict) and '$catalog' in v else v for k,v in p['params'].items()}
            if values!=p['params']:
                p['resolved_params']=values
                with self.store.db() as db:db.execute('UPDATE jobs SET plan=? WHERE id=?',(json.dumps(p),j['id']))
        names=list(values);variants=itertools.product(*[v if isinstance(v,list) else [v] for v in values.values()])
        variant=next(itertools.islice(variants,c['variant'],None),None)
        if variant is None:return None,p,c
        args=dict(zip(names,variant));mode=p['mode'];date=c['date']
        if mode!='snapshot' and date>p['end']:return None,p,c
        request_dates(args,spec(self.catalog[j['id']],mode,p['key']),date)
        if p.get('history_version') and mode=='range' and p['schema']['format']=='day':
            days=c.get('window_days',default_window(p))
            args[p['schema']['end_key']]=min(p['end'],(dt.datetime.strptime(date,'%Y%m%d')+dt.timedelta(days=days-1)).strftime('%Y%m%d'))
        if p['offset']:args.update(limit=p['cap'],offset=c['offset'])
        return args,p,c
    def accept(self,j,args,p,c,data):
        if not isinstance(data,dict) or not isinstance(data.get('fields'),list) or not isinstance(data.get('items'),list):raise TransientDataError('代理未返回有效的数据结构，保留断点')
        fields=data['fields'];items=data['items']
        if len(fields)!=len(set(fields)) or any(not isinstance(f,str) for f in fields):raise ValueError('返回字段重复或非法')
        if any(not isinstance(r,list) or len(r)!=len(fields) for r in items):raise ValueError('返回行列不一致')
        if not items and not p['allow_empty']:raise EmptyDataError('成功返回空数据；计划未允许空结果，保留断点')
        digest=hashlib.sha256(json.dumps(data,ensure_ascii=False,sort_keys=True).encode()).hexdigest()
        if items and c.get('previous')==digest:raise ValueError('连续分页返回相同数据，可能未支持offset；停止，断点不推进')
        full=len(items)>=p['cap']
        if full and not p['offset']:
            if p.get('history_version') and p['mode']=='range' and p['schema']['format']=='day':
                span=(dt.datetime.strptime(args[p['schema']['end_key']],'%Y%m%d')-dt.datetime.strptime(c['date'],'%Y%m%d')).days+1
                if span>1:raise NarrowWindow(max(1,span//2))
            raise ValueError('返回达到文档保守上限，无法证明完整；请缩小范围或确认分页支持，保留断点')
        cursor=dict(c)
        if full:cursor.update(offset=c['offset']+len(items),previous=digest)
        else:
            last=args[p['schema']['end_key']] if p.get('history_version') and p['mode']=='range' and p['schema']['format']=='day' else c['date']
            cursor.update(offset=0,previous='',date=next_date(last,p['mode']))
            cursor.pop('window_days',None)
            if p['mode']=='snapshot' or cursor['date']>p['end']:cursor.update(variant=c['variant']+1,date=p['start'])
        total=1
        for v in p.get('resolved_params',p['params']).values():total*=len(v) if isinstance(v,list) else 1
        status='complete' if cursor['variant']>=total else 'pending'
        history=None
        if p.get('history_version') and p['mode']!='snapshot':
            start,end=bounds(p,c,args);first=earliest(j['id'],fields,items,start,end)
            rows=self.store.rows('SELECT * FROM history_summary WHERE job=? AND variant=?',(j['id'],c['variant']))
            history=rows[0] if rows else dict(first_start=None,first_end=None,first_date=None,empty_streak=0,empty_count=0)
            if items:
                if history['first_start'] is None:history.update(first_start=start,first_end=end,first_date=first)
                history['empty_streak']=0
            else:
                history['empty_streak']+=1;history['empty_count']+=1
                threshold=40 if default_window(p)>1 else 32
                if (cursor['variant']!=c['variant'] and history['first_start'] is None) or history['empty_streak']>=threshold:
                    raise ValueError('历史覆盖待核验：连续空结果或整个范围没有记录；保留当前断点，不标记完成。请核对代理历史范围/请求条件')
            if not items:
                with self.store.db() as db:
                    self.save_history(db,j,c,start,end,0,history)
                    db.execute("UPDATE jobs SET cursor=?,status=?,error='' WHERE id=?",(json.dumps(cursor),status,j['id']))
                self.store.log('历史区间无记录（仅保存核验断点，不生成空数据文件）：'+j['title']+'；'+start+'至'+end)
                return status
        if not items:
            # Empty snapshots are checks, not data files.
            with self.store.db() as db:
                db.execute('INSERT INTO history_checks VALUES(?,?,?,?,?,?)',(j['id'],c['variant'],c['date'],c['date'],0,stamp()))
                db.execute("UPDATE jobs SET cursor=?,status=?,error='' WHERE id=?",(json.dumps(cursor),status,j['id']))
            self.store.log('当前快照无记录（仅保存核验断点，不生成空数据文件）：'+j['title'])
            return status
        key=hashlib.sha256(json.dumps([j['id'],args,digest],sort_keys=True,ensure_ascii=False).encode()).hexdigest()
        scope='_'.join(safe(str(args[k])) for k in ('ts_code','trade_date','ann_date','period','start_date','date','month','year') if k in args) or '全量快照'
        if p.get('history_version') and p['mode']=='range' and p['schema']['format']=='day':scope+='至'+args[p['schema']['end_key']]
        folder=self.data_root/safe(j['group_name'])/safe(j['title']);folder.mkdir(parents=True,exist_ok=True)
        path=folder/(scope+'_第'+str(c['offset'])+'行_'+key[:12]+'.json')
        record=dict(source='Tushare Pro',dataset=j['id'],title=j['title'],params=args,fetched_at=stamp(),fields=fields,items=items,empty=not items)
        temp=path.with_suffix('.tmp')
        with temp.open('w',encoding='utf-8') as stream:json.dump(record,stream,ensure_ascii=False,allow_nan=False);stream.flush();os.fsync(stream.fileno())
        temp.replace(path)
        with self.store.db() as db:
            if history is not None:self.save_history(db,j,c,start,end,len(items),history)
            exists=db.execute('SELECT 1 FROM files WHERE key=?',(key,)).fetchone()
            db.execute('INSERT OR REPLACE INTO files VALUES(?,?,?,?,?)',(key,j['id'],str(path),len(items),stamp()))
            db.execute("UPDATE jobs SET cursor=?,status=?,error='',records=records+?,batches=batches+? WHERE id=?",(json.dumps(cursor),status,0 if exists else len(items),0 if exists else 1,j['id']))
        self.store.log('已保存：'+j['title']+'；'+str(len(items))+'条；'+str(path))
        return status
    def save_history(self,db,j,c,start,end,count,h):
        db.execute('INSERT INTO history_checks VALUES(?,?,?,?,?,?)',(j['id'],c['variant'],start,end,count,stamp()))
        db.execute('INSERT OR REPLACE INTO history_summary VALUES(?,?,?,?,?,?,?)',(j['id'],c['variant'],h['first_start'],h['first_end'],h['first_date'],h['empty_streak'],h['empty_count']))
    def loop(self):
        while not self.closing:
            try:
                if not self.active:self.wake.wait(.3);self.wake.clear();continue
                args=None
                with self.guard:
                    candidates=self.store.rows('SELECT * FROM jobs WHERE id=?',(self.selected,)) if self.selected else self.store.rows("SELECT * FROM jobs WHERE status IN ('pending','failed','waiting') ORDER BY priority,rowid")
                    jobs=[]
                    for candidate in candidates:
                        if candidate['status']=='failed':jobs=[candidate];break
                        try:self.request_params(candidate)
                        except DependencyPending as exc:
                            with self.store.db() as db:db.execute("UPDATE jobs SET status='waiting',error=? WHERE id=?",(str(exc),candidate['id']))
                            if self.selected:self.pause();self.store.log(str(exc)+'；先采集该目录后再继续')
                            continue
                        if candidate['status']=='waiting':
                            with self.store.db() as db:db.execute("UPDATE jobs SET status='pending',error='' WHERE id=?",(candidate['id'],))
                            candidate['status']='pending'
                        jobs=[candidate];break
                    if not jobs or jobs[0]['status']=='complete':self.pause();continue
                    j=jobs[0];e=self.catalog[j['id']];self.next_job=j['id']
                    if j['status']!='pending':raise ValueError('队首失败项目未解决，请单独选择其他项目或重试此项')
                    if e.get('excluded') or j['id'] in DENIED:raise ValueError('项目属于排除的本地重复数据')
                    args,p,c=self.request_params(j)
                    if args is None:raise ValueError('断点与计划范围不一致，请人工核对')
                    request_key=hashlib.sha256(json.dumps([j['id'],args,e['fields'],p.get('channel','auto')],sort_keys=True).encode()).hexdigest()
                    retry=self.store.rows('SELECT * FROM retries WHERE job=? AND request_key=?',(j['id'],request_key))
                    attempts=retry[0]['attempts'] if retry else 0
                    if attempts>=MAX_ATTEMPTS:
                        with self.store.db() as db:db.execute("UPDATE jobs SET status='failed',error=? WHERE id=?",('此请求已达到3次尝试上限，请人工核对后再重试',j['id']))
                        raise ValueError('此请求已达到3次尝试上限，请人工核对后再重试')
                    due=self.due(j['id'],min(400,e.get('rpm',30)))
                    if retry:due=max(due,retry[0]['not_before'])
                    if due>time.time():self.wake.wait(min(.2,due-time.time()));self.wake.clear();continue
                    if not self.active:continue
                    now=time.time()
                    with self.store.db() as db:
                        db.execute('INSERT INTO requests VALUES(?,?)',(now,j['id']))
                        db.execute('DELETE FROM requests WHERE time<?',(now-86400,))
                        db.execute('INSERT OR REPLACE INTO retries VALUES(?,?,?,?,?)',(j['id'],request_key,attempts+1,now+RETRY_SECONDS,''))
                    self.store.set('next_due',now+max(60/min(400,self.store.get('rpm',400)),self.store.get('proxy_gap',0)))
                    self.current=j['id']
                self.store.log('正在获取：'+j['title']+'；第'+str(attempts+1)+'/3次尝试；'+json.dumps(args,ensure_ascii=False))
                try:
                    data=self.transport(j['id'],args,e['fields']);status=self.accept(j,args,p,c,data)
                    with self.store.db() as db:db.execute('DELETE FROM retries WHERE job=?',(j['id'],))
                    if status=='complete':
                        self.store.log('项目已完成当前范围：'+j['title'])
                        if self.selected:self.pause()
                except NarrowWindow as exc:
                    cursor=dict(c);cursor['window_days']=exc.days
                    with self.store.db() as db:
                        db.execute('UPDATE jobs SET cursor=? WHERE id=?',(json.dumps(cursor),j['id']))
                        db.execute('DELETE FROM retries WHERE job=?',(j['id'],))
                    self.store.log('区间达到返回上限，未保存或推进日期；缩为'+str(exc.days)+'天后按限频重新获取：'+j['title'])
                except Exception as exc:
                    message=self.redact(str(exc))[:900]
                    transient=getattr(exc,'retryable',False)
                    delay=max(RETRY_SECONDS,getattr(exc,'retry_after',0))
                    if getattr(exc,'ip_limited',False):
                        self.store.set('proxy_gap',max(self.store.get('proxy_gap',0),delay))
                        self.store.log('代理IP限流：全队列请求间隔已自动提高至至少'+str(delay)+'秒，重启后保留')
                    if transient:self.store.set('next_due',max(self.store.get('next_due',0),time.time()+delay))
                    if transient and attempts+1<MAX_ATTEMPTS:
                        retry_at=time.time()+delay
                        with self.store.db() as db:
                            db.execute('UPDATE retries SET not_before=?,last_error=? WHERE job=?',(retry_at,message,j['id']))
                            db.execute("UPDATE jobs SET error=? WHERE id=?",('等待重试：'+message,j['id']))
                        self.store.set('next_due',max(self.store.get('next_due',0),retry_at))
                        self.store.log('第'+str(attempts+1)+'/3次失败，至少等待'+str(delay)+'秒后有限重试；暂停/关闭可中止等待：'+message)
                    else:
                        with self.store.db() as db:
                            db.execute("UPDATE jobs SET status='failed',error=? WHERE id=?",(message,j['id']))
                            db.execute('UPDATE retries SET last_error=? WHERE job=?',(message,j['id']))
                        if isinstance(exc,CredentialError):self.store.set('credential_issue',dict(job=j['id'],title=j['title'],channel=self.channel_for(j['id']),kind=exc.kind,message=message,time=stamp()))
                        self.pause();self.store.log(('3次尝试后仍失败' if attempts+1>=MAX_ATTEMPTS else '不可恢复的请求、校验或保存错误')+'，整个队列已暂停：'+message)
                finally:
                    with self.guard:self.current=None
            except Exception as exc:
                with self.guard:self.active=False;self.current=None
                try:self.store.log('调度异常，已暂停：'+str(exc)[:900])
                except Exception:pass
                self.wake.wait(.5);self.wake.clear()
    def status(self):
        with self.guard:
            return dict(active=self.active,current=self.current,closing=self.closing,selected=self.selected,next_job=self.next_job,
                        configured=bool(self.store.get('account')),backup_configured=bool(self.store.get('backup_account')),
                        connection=self.store.get('connection',{'url':PROXY_URL,'default_channel':'main'}),
                        credential_issue=self.store.get('credential_issue'),bulk_report=self.store.get('bulk_report'),
                        history=self.store.rows('SELECT * FROM history_summary ORDER BY job,variant'),
                        retry_state=self.store.rows('SELECT job,attempts,not_before,last_error FROM retries WHERE last_error<>\'\''),
                        rpm=self.store.get('rpm',400),proxy_gap=self.store.get('proxy_gap',0),jobs=self.store.rows('SELECT * FROM jobs ORDER BY priority,rowid'),
                        events=self.store.rows('SELECT time,message FROM events ORDER BY id DESC LIMIT 80'),
                        files=self.store.rows('SELECT * FROM files ORDER BY time DESC LIMIT 100'),
                        data_root=str(self.data_root),next_due=self.store.get('next_due',0))

class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self,*args,**kwargs):raise ValueError('拒绝重定向，避免Token发往其他地址')
