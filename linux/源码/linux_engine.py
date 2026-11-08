"""Two-token collector. Only a job's owner may move its committed checkpoint."""
import hashlib
import json
import os
import threading
import time
from pathlib import Path
from core import Engine as BaseEngine,Store as BaseStore,CredentialError,RemoteError,DependencyPending,NarrowWindow,DENIED,DATA_ROOT,stamp,secret

from history import latest,floor,previous,reverse_dates

RETRY_SECONDS=30
class DirectoryUnavailable(ValueError):pass

class Store(BaseStore):
    def __init__(self,root):
        super().__init__(root)
        with self.db() as db:
            db.execute('PRAGMA journal_mode=WAL')
            db.execute('CREATE TABLE IF NOT EXISTS job_delays(job TEXT PRIMARY KEY,not_before REAL)')
            db.execute('CREATE TABLE IF NOT EXISTS job_channels(job TEXT,channel TEXT,PRIMARY KEY(job,channel))')
            if 'block_reason' not in {r[1] for r in db.execute('PRAGMA table_info(jobs)')}:
                db.execute("ALTER TABLE jobs ADD COLUMN block_reason TEXT NOT NULL DEFAULT ''")
                db.execute("UPDATE jobs SET block_reason=CASE WHEN error LIKE '%；前置目录不可用：%' THEN 'dependency' ELSE 'validation' END WHERE status='blocked'")
        if os.name=='posix':self.root.chmod(0o700);self.path.chmod(0o600)

class Engine(BaseEngine):
    def __init__(self,store,catalog,transport=None,data_root=DATA_ROOT):
        super().__init__(store,catalog,transport,data_root)
        self.local=threading.local();self.busy={};self.workers=[]
        self.thread=threading.Thread(target=self.supervise,name='采集调度器',daemon=True)
        self.phase='paused';self.calendar_cache=None
        for job in store.rows('SELECT * FROM jobs'):self.reverse_job(job)
        # Old or manually imported plans cannot override the hard exclusion.
        with store.db() as db:
            for entry in self.catalog.values():
                if entry.get('independent'):
                    db.execute("UPDATE jobs SET status='excluded',error='Linux版排除独立权限接口' WHERE id=?",(entry['id'],))
    def reverse_job(self,j,fresh=False):
        p=json.loads(j['plan']);c=json.loads(j['cursor'])
        if p.get('bulk_placeholder') or p.get('mode')=='snapshot' or p.get('direction')=='desc' or j['status'] in ('complete','excluded'):return j
        # Finish any partially saved ascending page window before changing direction.
        if c.get('offset'):return j
        before=j['plan'];old_cursor=j['cursor']
        p['direction']='desc';p['history_version']=1
        if not fresh:p['ascending_prefix']={'variant':c['variant'],'date':c['date']}
        c.update(date=latest(p),offset=0,previous='');c.pop('window_days',None)
        status=j['status'];error=j['error']
        if status=='blocked' and '历史覆盖待核验' in error:status='pending';error=''
        with self.store.db() as db:
            db.execute('INSERT INTO revisions(job,time,previous_plan,previous_cursor,previous_status) VALUES(?,?,?,?,?)',(j['id'],stamp(),before,old_cursor,j['status']))
            db.execute('UPDATE jobs SET plan=?,cursor=?,status=?,error=? WHERE id=?',(json.dumps(p),json.dumps(c),status,error,j['id']))
            db.execute('UPDATE history_summary SET empty_streak=0 WHERE job=?',(j['id'],))
            db.execute('DELETE FROM retries WHERE job=?',(j['id'],))
        self.store.log('历史查询切换为最新向前倒查，保留已提交区间：'+j['title']+'；下一日期 '+c['date'])
        return self.store.rows('SELECT * FROM jobs WHERE id=?',(j['id'],))[0]
    def update(self,ident,end=None):
        self.paused()
        j=self.store.rows('SELECT * FROM jobs WHERE id=?',(ident,))
        if j and json.loads(j[0]['plan']).get('direction')=='desc' and j[0]['status']!='complete':
            raise ValueError('倒序历史尚未走完，请先续采原范围；完成后再更新至今天，避免覆盖未完成断点')
        super().update(ident,end)
        row=self.store.rows('SELECT * FROM jobs WHERE id=?',(ident,))[0];p=json.loads(row['plan'])
        if p.get('mode')!='snapshot':
            c=json.loads(row['cursor']);p.pop('direction',None);p.pop('ascending_prefix',None)
            completed=bool(j and j[0]['status']=='complete')
            if completed:
                prior=json.loads(j[0]['plan'])
                p['incremental_scope']={'date':c['date'],'params':prior.get('resolved_params',prior['params'])}
            with self.store.db() as db:db.execute('UPDATE jobs SET plan=? WHERE id=?',(json.dumps(p),ident))
            self.reverse_job(self.store.rows('SELECT * FROM jobs WHERE id=?',(ident,))[0],fresh=completed)
        with self.store.db() as db:db.execute("UPDATE jobs SET block_reason='' WHERE id=?",(ident,))
    def closed_days(self):
        files=self.store.rows("SELECT path,time FROM files WHERE job='trade_cal'")
        signature=tuple((f['path'],f['time']) for f in files)
        if self.calendar_cache and self.calendar_cache[0]==signature:return self.calendar_cache[1]
        result={}
        for f in files:
            path=Path(f['path'])
            if self.data_root.resolve() not in path.resolve().parents:continue
            try:
                data=json.loads(path.read_text(encoding='utf-8'));fields=data['fields']
                if not {'cal_date','is_open'}<=set(fields):continue
                for row in data['items']:
                    item=dict(zip(fields,row));exchange=item.get('exchange') or data.get('params',{}).get('exchange')
                    if exchange:
                        key=(exchange,str(item['cal_date']));result[key]=result.get(key,True) and str(item['is_open'])=='0'
            except (OSError,ValueError,KeyError):continue
        self.calendar_cache=(signature,result);return result
    def historical_boundary(self,j,args,plan):
        entry=self.catalog[j['id']]
        if entry.get('history_start'):return entry['history_start']
        # Listing dates bound trading records, not pre-IPO financial/event records.
        field='list_date' if plan['key']=='trade_date' else 'found_date' if plan['key']=='nav_date' else None
        reference=plan['params'].get('ts_code')
        if not field or not isinstance(reference,dict) or '$catalog' not in reference:return None
        source=reference['$catalog'];files=[(Path(f['path']),None) for f in self.store.rows('SELECT path FROM files WHERE job=?',(source,))]
        if self.store.rows("SELECT name FROM sqlite_master WHERE type='table' AND name='baseline_catalog'"):
            files.extend((Path(f['path']),f['sha256']) for f in self.store.rows('SELECT path,sha256 FROM baseline_catalog WHERE job=?',(source,)))
        found=[]
        for path,digest in files:
            allowed=self.store.root.parent/'基线' if digest else self.data_root
            if allowed.resolve() not in path.resolve().parents:raise ValueError('历史边界目录路径越界')
            content=path.read_bytes()
            if digest and hashlib.sha256(content).hexdigest()!=digest:raise ValueError('历史边界目录校验失败')
            data=json.loads(content);fields=data['fields']
            if not {field,'ts_code'}<=set(fields):continue
            for row in data['items']:
                if row[fields.index('ts_code')]==args.get('ts_code'):
                    value=str(row[fields.index(field)])
                    if len(value)==8 and value.isdigit():found.append(value)
        return min(found) if found else None
    def request_params(self,j):
        p=json.loads(j['plan']);c=json.loads(j['cursor'])
        if p.get('direction')=='desc' and p['mode']=='day' and p['key']=='trade_date' and not c.get('offset'):
            closed=self.closed_days();exchanges=[];params=p.get('resolved_params',p['params']);code=params.get('ts_code')
            if isinstance(params.get('exchange'),str):exchanges=[params['exchange']]
            elif isinstance(code,str) and code.endswith(('.SH','.SZ','.BJ')):exchanges=[{'.SH':'SSE','.SZ':'SZSE','.BJ':'BSE'}[code[-3:]]]
            elif self.catalog[j['id']].get('group','').startswith('股票'):exchanges=['SSE','SZSE','BSE']
            count=0
            while exchanges and c['date']>floor(p,c) and all(closed.get((e,c['date'])) is True for e in exchanges):
                c['date']=previous(c['date']);count+=1
            if count:
                with self.store.db() as db:db.execute('UPDATE jobs SET cursor=? WHERE id=?',(json.dumps(c),j['id']))
                j=dict(j,cursor=json.dumps(c));self.store.log('依据已保存交易日历跳过'+str(count)+'个休市日期：'+j['title'])
        args,p,c=super().request_params(j)
        if args is not None and p.get('direction')=='desc':
            boundary=self.historical_boundary(j,args,p)
            if boundary:
                import datetime as dt
                dt.datetime.strptime(boundary,'%Y%m%d')
                p=dict(p,verified_history_start=boundary)
                if c['date']<floor(p,c):raise ValueError('所选未采集范围早于已核验的历史起点，请核对计划范围：'+boundary)
                reverse_dates(args,p,c)
        return args,p,c
    def plan(self,ident,*args,**kwargs):
        if self.catalog.get(ident,{}).get('independent'):raise ValueError('Linux版不采集需要独立权限的接口')
        before=self.store.rows('SELECT * FROM jobs WHERE id=?',(ident,))
        if before:
            prior=json.loads(before[0]['plan']);new_end=kwargs.get('end',args[2] if len(args)>2 else prior.get('end'))
            if prior.get('direction')=='desc' and before[0]['status']!='complete' and new_end!=prior.get('end') and not kwargs.get('replay'):
                raise ValueError('倒序历史尚未完成，修改截止日期需明确选择重新核对；直接续采可保留原范围断点')
        result=super().plan(ident,*args,**kwargs)
        with self.store.db() as db:
            p=json.loads(db.execute('SELECT plan FROM jobs WHERE id=?',(ident,)).fetchone()['plan'])
            p['dependencies']=self.dependencies(p)
            db.execute("UPDATE jobs SET plan=?,block_reason='' WHERE id=?",(json.dumps(p),ident))
        row=self.store.rows('SELECT * FROM jobs WHERE id=?',(ident,))[0]
        current=json.loads(row['plan']);prior=json.loads(before[0]['plan']) if before else {}
        replay=not before or kwargs.get('replay') or prior.get('bulk_placeholder') or any(prior.get(k)!=current.get(k) for k in ('params','start','mode','key')) or current.get('end','')<prior.get('end','')
        if before and prior.get('direction')=='desc' and row['cursor']==before[0]['cursor'] and not replay:
            old=json.loads(before[0]['plan']);p=json.loads(row['plan']);p['direction']='desc'
            for name in ('ascending_prefix','incremental_scope'):
                if name in old:p[name]=old[name]
            with self.store.db() as db:db.execute('UPDATE jobs SET plan=? WHERE id=?',(json.dumps(p),ident))
        else:
            completed_increment=bool(before and before[0]['status']=='complete' and row['status']=='pending' and not replay and current.get('mode')!='snapshot')
            if completed_increment:
                current['incremental_scope']={'date':json.loads(row['cursor'])['date'],'params':prior.get('resolved_params',prior['params'])}
                with self.store.db() as db:db.execute('UPDATE jobs SET plan=? WHERE id=?',(json.dumps(current),ident))
                row=self.store.rows('SELECT * FROM jobs WHERE id=?',(ident,))[0]
            self.reverse_job(row,fresh=bool(replay or completed_increment))
        return result
    @staticmethod
    def dependencies(plan):
        return sorted(set(plan.get('dependencies',[]))|{v['$catalog'] for v in plan.get('params',{}).values() if isinstance(v,dict) and '$catalog' in v})
    def block_dependency(self,j,message):
        if j['status']=='blocked' and j.get('block_reason')=='dependency' and j['error']==message:return
        with self.store.db() as db:db.execute("UPDATE jobs SET status='blocked',error=?,block_reason='dependency' WHERE id=?",(message,j['id']))
        self.store.log('目录依赖受阻，其他项目继续：'+j['title']+'；'+message)
    def catalog_values(self,source,field):
        rows=self.store.rows('SELECT status FROM jobs WHERE id=?',(source,))
        if not rows or rows[0]['status']!='complete':raise DependencyPending('等待目录完成：'+source)
        files=[(Path(f['path']),None) for f in self.store.rows('SELECT path FROM files WHERE job=?',(source,))]
        if self.store.rows("SELECT name FROM sqlite_master WHERE type='table' AND name='baseline_catalog'"):
            files.extend((Path(f['path']),f['sha256']) for f in self.store.rows('SELECT path,sha256 FROM baseline_catalog WHERE job=?',(source,)))
        values=set();baseline_root=self.store.root.parent/'基线'
        for path,digest in files:
            allowed=baseline_root.resolve() if digest else self.data_root.resolve()
            if allowed not in path.resolve().parents:raise ValueError('目录缓存路径越界')
            content=path.read_bytes()
            if digest and hashlib.sha256(content).hexdigest()!=digest:raise ValueError('本机基线目录缓存被修改，需核验')
            d=json.loads(content)
            if field not in d['fields']:raise ValueError('目录缺少字段：'+source+'.'+field)
            index=d['fields'].index(field);values.update(str(r[index]) for r in d['items'] if r[index] is not None and str(r[index]))
        if not values:raise DirectoryUnavailable('目录已完成但没有可用代码，关联任务受阻：'+source)
        return sorted(values)
    def channel_for(self,api):
        if self.catalog[api].get('independent'):raise ValueError('独立权限接口禁止派发')
        return getattr(self.local,'channel',None) or super().channel_for(api)
    def http(self,api,params,fields):
        channel=self.channel_for(api)
        try:secret(self.store.get('account' if channel=='main' else 'backup_account',''),True)
        except Exception:raise CredentialError('本机Token无法解密，请恢复密钥或重新配置凭据') from None
        return super().http(api,params,fields)
    def credentials_changed(self,main=False,backup=False):
        health=self.store.get('channel_health',{})
        for channel,changed in [('main',main),('backup',backup)]:
            if changed:
                health.pop(channel,None)
                with self.store.db() as db:
                    db.execute("UPDATE jobs SET status='pending',error='' WHERE status='unavailable' AND id IN (SELECT job FROM job_channels WHERE channel=?)",(channel,))
                    db.execute('DELETE FROM job_channels WHERE channel=?',(channel,))
        self.store.set('channel_health',health)
    def switch_token(self,channel):
        with self.guard:
            self.paused()
            if channel not in ('main','backup'):raise ValueError('通道无效')
            value=self.store.get('account' if channel=='main' else 'backup_account')
            if not value:raise ValueError('该Token尚未配置')
            secret(value,True);self.credentials_changed(channel=='main',channel=='backup')
            self.store.log('已手动重新启用'+channel+'通道；双Token协同模式，保持暂停')
    def start(self,ident=None):
        with self.guard:
            self.paused()
            if not any(self.store.get(k) for k in ('account','backup_account')):raise ValueError('请先配置至少一个Token')
            if ident:
                rows=self.store.rows('SELECT * FROM jobs WHERE id=?',(ident,))
                if not rows or self.catalog[ident].get('independent') or json.loads(rows[0]['plan']).get('bulk_placeholder'):raise ValueError('任务不存在或被排除，请先建立可执行计划')
                if rows[0]['status']=='complete':raise ValueError('请先更新已完成项目的日期范围')
                with self.store.db() as db:
                    db.execute("UPDATE jobs SET status='pending',error='',block_reason='' WHERE id=?",(ident,))
                    db.execute('DELETE FROM retries WHERE job=?',(ident,))
                    db.execute('DELETE FROM job_channels WHERE job=?',(ident,))
            self.selected=ident;self.active=True;self.phase='running'
            self.store.set('resume_selected',ident)
            self.store.set('resume_requested',True);self.wake.set();self.store.log('开始双Token持续采集'+('，单项：'+ident if ident else '，失败任务不阻塞其他项目'))
    def pause(self):
        with self.guard:
            self.active=False;self.phase='paused';self.store.set('resume_requested',False);self.wake.set()
        self.store.log('请求暂停；等待两个通道当前请求保存或超时，不派发新请求')
    def resume_on_boot(self):
        if self.store.get('resume_requested',False):
            try:self.start(self.store.get('resume_selected'))
            except ValueError as exc:self.store.log('启动后等待配置：'+str(exc))
    def supervise(self):
        self.workers=[threading.Thread(target=self.worker,args=(c,),name='采集-'+c,daemon=True) for c in ('main','backup')]
        for worker in self.workers:worker.start()
        for worker in self.workers:worker.join()
    def channel_ready(self,channel):
        return bool(self.store.get('account' if channel=='main' else 'backup_account')) and channel not in self.store.get('channel_health',{})
    def prepare(self,channel):
        """Called under guard: claim and quota reservation are a single critical section."""
        if not self.active or self.closing or not self.channel_ready(channel):return None
        rows=self.store.rows("SELECT * FROM jobs WHERE status IN ('pending','retry','waiting','failed','blocked') ORDER BY priority,rowid")
        for j in rows:
            ident=j['id']
            if self.selected and ident!=self.selected or ident in self.busy.values():continue
            j=self.reverse_job(j)
            entry=self.catalog[ident];p=json.loads(j['plan'])
            if j['status']=='blocked':
                if j.get('block_reason')!='dependency':continue
                deps=self.dependencies(p)
                states={r['id']:r for r in self.store.rows('SELECT id,status,records FROM jobs')}
                if not deps or not all(d in states and states[d]['status']=='complete' and states[d]['records']>0 for d in deps):continue
            if entry.get('independent') or entry.get('excluded') or ident in DENIED:
                with self.store.db() as db:db.execute("UPDATE jobs SET status='excluded',error='排除独立权限或本地重复接口' WHERE id=?",(ident,))
                continue
            if p.get('channel','auto') not in ('auto',channel):continue
            if self.store.rows('SELECT 1 FROM job_channels WHERE job=? AND channel=?',(ident,channel)):continue
            deferred=self.store.rows('SELECT not_before FROM job_delays WHERE job=?',(ident,))
            if deferred and deferred[0]['not_before']>time.time():continue
            try:args,p,c=self.request_params(j)
            except DirectoryUnavailable as exc:
                self.block_dependency(j,str(exc));continue
            except DependencyPending as exc:
                dependencies=self.dependencies(p)
                states={r['id']:r['status'] for r in self.store.rows('SELECT id,status FROM jobs')}
                dead=[d for d in dependencies if d not in states or states[d] in ('excluded','unavailable','blocked')]
                if dead:self.block_dependency(j,str(exc)+'；前置目录不可用：'+','.join(dead))
                else:
                    with self.store.db() as db:db.execute("UPDATE jobs SET status='waiting',error=?,block_reason='' WHERE id=?",(str(exc),ident))
                continue
            except (ValueError,KeyError,OSError) as exc:
                self.fail(j,exc);continue
            if args is None:
                self.fail(j,ValueError('断点与计划范围不一致，需核验'));continue
            if j['status']=='blocked':
                # A repaired dependency is runnable even while its quota is cooling.
                # settle() must keep the queue alive until that future request is due.
                with self.store.db() as db:db.execute("UPDATE jobs SET status='pending',error='',block_reason='' WHERE id=?",(ident,))
            if self.store.get('next_due',0)>time.time():return None
            fingerprint=hashlib.sha256(json.dumps([ident,args,entry['fields']],sort_keys=True).encode()).hexdigest()
            retry=self.store.rows('SELECT * FROM retries WHERE job=? AND request_key=?',(ident,fingerprint))
            attempts=retry[0]['attempts'] if retry else 0
            due=self.due(ident,min(400,entry.get('rpm',30)))
            if retry:due=max(due,retry[0]['not_before'])
            if due>time.time():continue
            now=time.time()
            with self.store.db() as db:
                db.execute('INSERT INTO requests VALUES(?,?)',(now,ident))
                db.execute('DELETE FROM requests WHERE time<?',(now-86400,))
                db.execute('INSERT OR REPLACE INTO retries VALUES(?,?,?,?,?)',(ident,fingerprint,attempts+1,now+RETRY_SECONDS,''))
                db.execute("UPDATE jobs SET status='pending',error='',block_reason='' WHERE id=?",(ident,))
            self.store.set('next_due',now+max(60/min(400,self.store.get('rpm',400)),self.store.get('proxy_gap',0)))
            self.busy[channel]=ident;self.current=','.join(self.busy.values());self.next_job=ident
            return j,args,p,c,attempts+1
        self.settle()
        return None
    def settle(self):
        if self.busy:return
        jobs=self.store.rows('SELECT * FROM jobs')
        pending=[j for j in jobs if (not self.selected or j['id']==self.selected) and j['status'] in ('pending','retry','waiting','failed')]
        if pending:
            self.phase='waiting' if any(self.channel_ready(c) for c in ('main','backup')) else 'waiting_credentials'
            return
        self.active=False;self.phase='finished';self.store.set('resume_requested',False)
        self.store.log('本轮已结束：可执行项目已走完，排除/不可用/覆盖待核验项目另列；不代表上游绝对完整')
    def fail(self,j,exc):
        message=self.redact(str(exc))[:900];transient=getattr(exc,'retryable',False)
        delay=max(RETRY_SECONDS,getattr(exc,'retry_after',0))
        channel=getattr(self.local,'channel',None)
        if isinstance(exc,CredentialError) and exc.kind=='token':
            health=self.store.get('channel_health',{});health[channel]=dict(message=message,time=stamp());self.store.set('channel_health',health)
            transient=True
        elif isinstance(exc,CredentialError) and exc.kind=='permission':
            with self.store.db() as db:db.execute('INSERT OR IGNORE INTO job_channels VALUES(?,?)',(j['id'],channel))
            denied={r['channel'] for r in self.store.rows('SELECT channel FROM job_channels WHERE job=?',(j['id'],))}
            preferred=json.loads(j['plan']).get('channel','auto')
            transient=any(c not in denied and self.channel_ready(c) and preferred in ('auto',c) for c in ('main','backup'))
        if getattr(exc,'ip_limited',False):
            self.store.set('proxy_gap',max(self.store.get('proxy_gap',0),delay))
            self.store.set('next_due',max(self.store.get('next_due',0),time.time()+delay))
        if isinstance(exc,OSError):transient=True
        status='retry' if transient else 'blocked' if isinstance(exc,ValueError) else 'unavailable'
        with self.store.db() as db:
            db.execute('UPDATE jobs SET status=?,error=?,block_reason=? WHERE id=?',(status,message,'validation' if status=='blocked' else '',j['id']))
            db.execute('UPDATE retries SET not_before=?,last_error=? WHERE job=?',(time.time()+delay,message,j['id']))
            if transient:db.execute('INSERT OR REPLACE INTO job_delays VALUES(?,?)',(j['id'],time.time()+delay))
        self.store.log(('任务等待至少'+str(delay)+'秒重试' if transient else '任务已隔离为'+status)+'，其他可执行项目继续：'+j['title']+'；'+message)
    def worker(self,channel):
        self.local.channel=channel
        while not self.closing:
            work=None
            try:
                with self.guard:work=self.prepare(channel)
                if work is None:self.wake.wait(.2);self.wake.clear();continue
                j,args,p,c,attempts=work
                self.store.log('正在获取：'+j['title']+'；'+channel+'通道；累计尝试'+str(attempts)+'；'+json.dumps(args,ensure_ascii=False))
                try:
                    data=self.transport(j['id'],args,self.catalog[j['id']]['fields'])
                    result=self.accept(j,args,p,c,data)
                    with self.guard:
                        with self.store.db() as db:
                            db.execute('DELETE FROM retries WHERE job=?',(j['id'],))
                            db.execute('DELETE FROM job_delays WHERE job=?',(j['id'],))
                        if result=='complete':self.store.log('项目已完成当前范围：'+j['title'])
                except NarrowWindow as exc:
                    cursor=dict(c);cursor['window_days']=exc.days
                    with self.guard:
                        with self.store.db() as db:
                            db.execute('UPDATE jobs SET cursor=? WHERE id=?',(json.dumps(cursor),j['id']))
                            db.execute('DELETE FROM retries WHERE job=?',(j['id'],))
                    self.store.log('缩小区间至'+str(exc.days)+'天，按限频继续：'+j['title'])
                except Exception as exc:
                    with self.guard:self.fail(j,exc)
            except Exception as exc:
                # Never spin on a local scheduling failure; keep service observable.
                try:self.store.log('调度异常，30秒后重新检查：'+self.redact(str(exc))[:500])
                except Exception:pass
                self.wake.wait(RETRY_SECONDS);self.wake.clear()
            finally:
                if work:
                    with self.guard:self.busy.pop(channel,None);self.current=','.join(self.busy.values()) or None
    def status(self):
        with self.guard:
            value=super().status()
            if self.active and not self.busy and not any(self.channel_ready(c) for c in ('main','backup')):self.phase='waiting_credentials'
            value.update(platform='Ubuntu 24.04 x86_64',workers=dict(self.busy),phase=self.phase,retry_seconds=RETRY_SECONDS,channel_health=self.store.get('channel_health',{}),resume_requested=self.store.get('resume_requested',False),baseline=self.store.get('baseline'))
            return value
