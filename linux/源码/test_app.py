import copy
import datetime as dt
import json
import tempfile
import time
import unittest
from pathlib import Path
from core import Engine,Store,secret,RemoteError,PROXY_URL

ENTRY=dict(id='demo',title='测试数据',group='测试分类',inputs=[dict(name='trade_date',required=True)],fields=['id'],cap=2,offset=True,rpm=400)
class Tests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.root=Path(self.temp.name);self.store=Store(self.root/'状态')
        self.engine=Engine(self.store,copy.deepcopy([ENTRY,dict(ENTRY,id='other')]),data_root=self.root/'数据')
    def tearDown(self):
        self.engine.active=False;self.engine.closing=True;self.engine.wake.set()
        if self.engine.thread.is_alive():self.engine.thread.join(4)
        self.temp.cleanup()
    def plan(self,api='demo',empty=False):self.engine.plan(api,{},'20200101','20200102','day','trade_date',empty)
    def job(self,api='demo'):return self.store.rows('SELECT * FROM jobs WHERE id=?',(api,))[0]
    def accept(self,data):
        j=self.job();args,p,c=self.engine.request_params(j);return self.engine.accept(j,args,p,c,data)
    def test_account_encryption(self):
        raw='test-token-never-used-online';encoded=secret(raw);self.assertNotIn(raw,encoded);self.assertEqual(secret(encoded,True),raw)
    def test_no_token_no_start(self):
        self.plan()
        with self.assertRaisesRegex(ValueError,'Token'):self.engine.start('demo')
    def test_failed_first_can_select_other_without_mutating_first(self):
        self.plan();self.plan('other');self.store.set('account','fake')
        with self.store.db() as db:db.execute("UPDATE jobs SET status='failed',error='upstream' WHERE id='demo'")
        old=self.job();self.store.set('next_due',time.time()+50);due=self.store.get('next_due')
        self.engine.start('other');self.assertEqual(self.engine.selected,'other');self.assertEqual(self.job(),old);self.assertEqual(self.store.get('next_due'),due)
    def test_plan_requires_paused_and_drained(self):
        self.engine.current='demo'
        with self.assertRaises(ValueError):self.plan()
    def test_plan_cannot_overwrite_checkpoint(self):
        self.plan()
        with self.assertRaises(ValueError):self.plan()
    def test_wrong_response_does_not_advance(self):
        self.plan();old=self.job()
        with self.assertRaises(ValueError):self.accept('upstream error')
        self.assertEqual(self.job(),old)
    def test_empty_stops_unless_explicitly_allowed(self):
        self.plan()
        with self.assertRaisesRegex(ValueError,'空数据'):self.accept(dict(fields=['id'],items=[]))
        self.assertEqual(json.loads(self.job()['cursor'])['date'],'20200101')
    def test_allowed_history_empty_records_check_without_empty_archive(self):
        self.plan(empty=True);self.accept(dict(fields=['id'],items=[]))
        self.assertEqual(json.loads(self.job()['cursor'])['date'],'20200102');self.assertEqual(len(self.store.rows('SELECT * FROM files')),0)
        self.assertEqual(len(self.store.rows('SELECT * FROM history_checks')),1)
    def test_paginated_and_duplicate_page_rejected(self):
        self.plan();data=dict(fields=['id'],items=[[1],[2]]);self.accept(data)
        self.assertEqual(json.loads(self.job()['cursor'])['offset'],2)
        with self.assertRaisesRegex(ValueError,'相同'):self.accept(data)
        self.accept(dict(fields=['id'],items=[[3]]));self.assertEqual(json.loads(self.job()['cursor'])['date'],'20200102')
    def test_full_page_without_pagination_stops(self):
        self.plan();j=self.job();a,p,c=self.engine.request_params(j);p['offset']=False
        with self.assertRaisesRegex(ValueError,'上限'):self.engine.accept(j,a,p,c,dict(fields=['id'],items=[[1],[2]]))
        self.assertEqual(self.job()['records'],0)
    def test_disk_error_keeps_checkpoint(self):
        from unittest.mock import patch
        self.plan();old=self.job()
        with patch('pathlib.Path.mkdir',side_effect=OSError('disk full')):
            with self.assertRaises(OSError):self.accept(dict(fields=['id'],items=[[1]]))
        self.assertEqual(self.job(),old)
    def test_persisted_cooldown_global_and_per_api(self):
        now=time.time()
        with self.store.db() as db:db.executemany('INSERT INTO requests VALUES(?,?)',[(now,'demo')]*400)
        self.assertGreaterEqual(self.engine.due('other',400),now+60)
        self.assertGreaterEqual(Engine(Store(self.root/'状态'),[ENTRY]).due('demo',30),now+60)
    def test_single_completion_does_not_start_other(self):
        self.plan();self.plan('other');self.store.set('account','fake');calls=[]
        def fetch(api,*args):calls.append(api);return dict(fields=['id'],items=[[1]])
        self.engine.transport=fetch;self.engine.start('demo');self.engine.thread.start()
        end=time.time()+4
        while self.engine.active and time.time()<end:time.sleep(.03)
        self.assertFalse(self.engine.active);self.assertEqual(calls,['demo','demo']);self.assertEqual(self.job('other')['batches'],0)
    def test_one_failure_pauses_entire_queue(self):
        self.plan();self.plan('other');self.store.set('account','fake');calls=[]
        def fetch(api,*args):calls.append(api);raise RuntimeError('permission denied')
        self.engine.transport=fetch;self.engine.start('demo');self.engine.thread.start()
        end=time.time()+3
        while self.engine.active and time.time()<end:time.sleep(.03)
        self.assertFalse(self.engine.active);self.assertEqual(calls,['demo']);self.assertEqual(self.job()['status'],'failed')
    def test_update_retains_pending_checkpoint(self):
        self.plan();old=self.job()['cursor'];self.engine.update('demo');self.assertEqual(self.job()['cursor'],old);self.assertEqual(json.loads(self.job()['plan'])['end'],dt.date.today().strftime('%Y%m%d'))
    def test_batch_parameters_create_cartesian_plan(self):
        e=dict(ENTRY,inputs=[dict(name='ts_code',required=True)]);self.engine.catalog['demo']=e
        self.engine.plan('demo',dict(ts_code=['000001.SZ','600000.SH']),'20200101','20200101','snapshot','')
        args,p,c=self.engine.request_params(self.job());self.assertEqual(args['ts_code'],'000001.SZ')
        self.accept(dict(fields=['id'],items=[[1]]));args,p,c=self.engine.request_params(self.job());self.assertEqual(args['ts_code'],'600000.SH')
    def test_reserved_token_cannot_enter_params(self):
        with self.assertRaises(ValueError):self.engine.plan('demo',{'token':'sensitive'},'20200101','20200101','snapshot','')
    def test_revise_empty_policy_preserves_checkpoint_archives_and_cooldown(self):
        self.plan();self.accept(dict(fields=['id'],items=[[1]]));old=self.job();self.store.set('next_due',time.time()+60);due=self.store.get('next_due')
        with self.store.db() as db:db.execute("UPDATE jobs SET status='failed',error='empty response' WHERE id='demo'")
        self.engine.plan('demo',{},'20200101','20200102','day','trade_date',True,revise=True)
        changed=self.job();self.assertEqual(changed['cursor'],old['cursor']);self.assertEqual(changed['records'],1)
        self.assertTrue(json.loads(changed['plan'])['allow_empty']);self.assertEqual(changed['status'],'pending');self.assertEqual(self.store.get('next_due'),due)
        self.assertEqual(len(self.store.rows('SELECT * FROM files')),1);self.assertEqual(len(self.store.rows('SELECT * FROM revisions')),1)
    def test_revise_scope_requires_explicit_replay_and_keeps_saved_files(self):
        self.plan();self.accept(dict(fields=['id'],items=[[1]]));old=self.job()
        with self.assertRaisesRegex(ValueError,'重新核对'):self.engine.plan('demo',{'ts_code':'000001.SZ'},'20200101','20200102','day','trade_date',revise=True)
        self.assertEqual(self.job(),old)
        self.engine.plan('demo',{'ts_code':'000001.SZ'},'20200101','20200102','day','trade_date',revise=True,replay=True)
        self.assertEqual(json.loads(self.job()['cursor'])['date'],'20200101');self.assertEqual(self.job()['records'],1)
        archive=self.store.rows('SELECT * FROM files')[0];self.assertTrue(Path(archive['path']).exists())
        self.assertEqual(self.store.rows('SELECT * FROM revisions')[0]['previous_cursor'],old['cursor'])
    def test_revise_rejects_inflight_and_unknown_plan(self):
        self.plan();self.engine.current='demo'
        with self.assertRaises(ValueError):self.engine.plan('demo',{},'20200101','20200102','day','trade_date',True,revise=True)
        self.engine.current=None
        with self.assertRaisesRegex(ValueError,'不存在'):self.engine.plan('other',{},'20200101','20200102','day','trade_date',revise=True)
    def real_catalog(self):
        entries=json.loads((Path(__file__).parent/'catalog.json').read_text(encoding='utf-8'))['entries']
        self.engine.catalog={e['id']:e for e in entries}
        return self.engine.catalog
    def test_output_required_columns_do_not_replace_inputs(self):
        from prepare_catalog import Page,load
        from unittest.mock import patch
        p=Page();p.feed('<p>接口：index_basic</p><h3>输入参数</h3><table><tr><th>名称</th><th>类型</th><th>必选</th><th>描述</th></tr><tr><td>market</td><td>str</td><td>N</td><td>市场</td></tr></table><h3>输出参数</h3><table><tr><th>名称</th><th>类型</th><th>必选</th><th>描述</th></tr><tr><td>ts_code</td><td>str</td><td>Y</td><td>代码</td></tr></table>')
        with patch('prepare_catalog.page',return_value=p):entry=load(('/document/2?doc_id=94','指数目录',['指数']))
        self.assertEqual([f['name'] for f in entry['inputs']],['market']);self.assertEqual(entry['fields'],['ts_code'])
    def test_real_index_and_margin_schemas_are_valid(self):
        entries=self.real_catalog();self.engine.plan('index_basic',{'market':'SSE'},'20200101','20200101','snapshot','')
        self.engine.plan('index_weight',{'index_code':'000300.SH'},'20200101','20200102','day','trade_date')
        self.engine.plan('margin_detail',{},'20200101','20200102','day','trade_date')
        self.assertIn('weight',entries['index_weight']['fields']);self.assertNotIn('weight',[f['name'] for f in entries['index_weight']['inputs']])
    def test_datetime_request_covers_entire_day(self):
        self.real_catalog();self.engine.plan('news',{'src':'sina'},'20200101','20200102','range','start_date',True,True)
        args,_,_=self.engine.request_params(self.job('news'))
        self.assertEqual(args['start_date'],'2020-01-01 00:00:00');self.assertEqual(args['end_date'],'2020-01-01 23:59:59')
    def test_monthly_and_quarterly_parameter_formats(self):
        self.real_catalog();self.engine.plan('cn_cpi',{},'20200101','20201231','month','m')
        self.assertEqual(self.engine.request_params(self.job('cn_cpi'))[0]['m'],'202001')
        self.engine.plan('cn_gdp',{},'20200101','20201231','quarter','q')
        self.assertEqual(self.engine.request_params(self.job('cn_gdp'))[0]['q'],'2020Q1')
        self.engine.plan('income_vip',{},'20200101','20201231','quarter','period')
        self.assertEqual(self.engine.request_params(self.job('income_vip'))[0]['period'],'20200331')
    def test_incompatible_date_modes_rejected_before_persisting(self):
        self.real_catalog()
        for ident,mode,key in [('broker_recommend','day','month'),('cn_gdp','month','q'),('top_list','range','start_date')]:
            with self.assertRaisesRegex(ValueError,'不兼容'):self.engine.plan(ident,{},'20200101','20201231',mode,key)
        self.assertEqual(self.store.rows('SELECT * FROM jobs'),[])
    def test_snapshot_must_supply_required_date(self):
        with self.assertRaisesRegex(ValueError,'必填'):self.engine.plan('demo',{},'20200101','20200102','snapshot','')
    def test_completed_fixed_month_range_converts_to_real_incremental(self):
        self.real_catalog();self.engine.plan('cn_cpi',{'start_m':'202001','end_m':'202012'},'20200101','20201231','snapshot','')
        with self.store.db() as db:db.execute("UPDATE jobs SET status='complete' WHERE id='cn_cpi'")
        self.engine.update('cn_cpi');j=self.job('cn_cpi');p=json.loads(j['plan']);c=json.loads(j['cursor'])
        self.assertEqual(p['mode'],'month');self.assertNotIn('end_m',p['params']);self.assertEqual(c['date'],'20201201')
        c['date']=dt.date.today().strftime('%Y%m')+'01';j['cursor']=json.dumps(c)
        args,_,_=self.engine.request_params(j);self.assertEqual(args['end_m'],dt.date.today().strftime('%Y%m'))
    def test_incomplete_fixed_snapshot_cannot_silently_lose_offset(self):
        self.real_catalog();self.engine.plan('cn_cpi',{'start_m':'202001','end_m':'202012'},'20200101','20201231','snapshot','');old=self.job('cn_cpi')
        with self.assertRaisesRegex(ValueError,'尚未完成'):self.engine.update('cn_cpi')
        self.assertEqual(self.job('cn_cpi'),old)
    def test_legacy_checkpoint_without_schema_remains_readable(self):
        self.plan();j=self.job();p=json.loads(j['plan']);p.pop('schema');j['plan']=json.dumps(p)
        self.assertEqual(self.engine.request_params(j)[0]['trade_date'],'20200101')
    def run_until_paused(self):
        self.engine.thread.start();deadline=time.time()+4
        while (self.engine.active or self.engine.current) and time.time()<deadline:time.sleep(.02)
        self.assertFalse(self.engine.active);self.assertIsNone(self.engine.current)
    def test_retry_limit_is_three_total_attempts(self):
        from unittest.mock import patch
        self.plan();self.store.set('account','fake');calls=[]
        def fetch(*args):calls.append(args[0]);raise RemoteError('系统临时错误',True)
        self.engine.transport=fetch
        with patch('core.RETRY_SECONDS',0):self.engine.start('demo');self.run_until_paused()
        self.assertEqual(calls,['demo']*3);self.assertEqual(self.job()['status'],'failed');self.assertEqual(self.job()['batches'],0)
        self.assertEqual(self.store.rows('SELECT attempts FROM retries')[0]['attempts'],3)
        self.assertEqual(len(self.store.rows('SELECT * FROM requests')),3)
    def test_empty_retry_then_success_keeps_request_and_checkpoint(self):
        from unittest.mock import patch
        self.engine.plan('demo',{},'20200101','20200101','day','trade_date');self.store.set('account','fake');calls=[]
        def fetch(*args):calls.append(args[1]);return dict(fields=['id'],items=[] if len(calls)==1 else [[1]])
        self.engine.transport=fetch
        with patch('core.RETRY_SECONDS',0):self.engine.start('demo');self.run_until_paused()
        self.assertEqual(len(calls),2);self.assertEqual(calls[0],calls[1]);self.assertEqual(self.job()['records'],1);self.assertEqual(self.job()['status'],'complete')
        self.assertEqual(self.store.rows('SELECT * FROM retries'),[])
    def test_pause_and_restart_preserve_retry_delay_and_attempt_count(self):
        from types import SimpleNamespace
        from unittest.mock import patch
        self.plan();self.store.set('account','fake');calls=[]
        def fetch(*args):calls.append(1);raise RemoteError('频率超限',True)
        self.engine.transport=fetch;fixed=time.time()
        with patch('core.time',SimpleNamespace(time=lambda:fixed)):
            self.engine.start('demo');self.engine.thread.start();deadline=time.monotonic()+5
            retry=[]
            while time.monotonic()<deadline:
                retry=self.store.rows("SELECT * FROM retries WHERE last_error<>''")
                if retry:break
                time.sleep(.02)
            self.engine.pause();self.assertTrue(retry);self.assertEqual(len(calls),1)
            self.assertEqual(retry[0]['attempts'],1);self.assertGreaterEqual(retry[0]['not_before'],fixed+60)
            other=Engine(Store(self.root/'状态'),[ENTRY]);self.assertFalse(other.active)
            self.assertGreaterEqual(other.due('demo',400),retry[0]['not_before'])
    def test_permission_error_never_retries_or_switches_channel(self):
        self.plan();self.store.set('account','fake');calls=[]
        def fetch(*args):calls.append(1);raise RemoteError('没有权限',False)
        self.engine.transport=fetch;self.engine.start('demo');self.run_until_paused();self.assertEqual(len(calls),1)
    def test_proxy_transport_uses_configured_channel_and_redacts_tokens(self):
        from unittest.mock import Mock,patch
        self.store.set('account',secret('test-primary-token'));self.store.set('backup_account',secret('test-backup-token'))
        self.store.set('connection',dict(url=PROXY_URL,default_channel='backup'))
        class Response:
            def __enter__(self):return self
            def __exit__(self,*args):pass
            def read(self):return b'{"code":0,"data":{"fields":["id"],"items":[[1]]}}'
        opener=Mock();opener.open.return_value=Response()
        with patch('urllib.request.build_opener',return_value=opener):self.engine.http('demo',{},['id'])
        req=opener.open.call_args.args[0];self.assertEqual(req.full_url,PROXY_URL);self.assertEqual(json.loads(req.data)['token'],'test-backup-token')
        self.engine.catalog['demo']['independent']=True
        self.assertEqual(self.engine.channel_for('demo'),'main')
        self.assertNotIn('test-primary-token',self.engine.redact('error test-primary-token test-backup-token'))
        self.assertNotIn('test-backup-token',json.dumps(self.engine.status()))
    def test_proxy_transport_rejects_official_or_other_endpoints(self):
        self.store.set('connection',dict(url='https://api.tushare.pro',default_channel='main'))
        with self.assertRaisesRegex(ValueError,'代理地址'):self.engine.http('demo',{},['id'])
    def test_http_rate_error_is_transient(self):
        from unittest.mock import Mock,patch
        import urllib.error
        self.store.set('account',secret('test-primary-token'));opener=Mock();opener.open.side_effect=urllib.error.HTTPError(PROXY_URL,429,'rate',{},None)
        with patch('urllib.request.build_opener',return_value=opener):
            with self.assertRaises(RemoteError) as caught:self.engine.http('demo',{},['id'])
        self.assertTrue(caught.exception.retryable)
    def test_missing_data_retries_then_pauses_without_progress(self):
        from unittest.mock import patch
        self.plan();self.store.set('account','fake');calls=[]
        def fetch(*args):calls.append(1);return None
        self.engine.transport=fetch
        with patch('core.RETRY_SECONDS',0):self.engine.start('demo');self.run_until_paused()
        self.assertEqual(len(calls),3);self.assertEqual(self.job()['records'],0);self.assertEqual(json.loads(self.job()['cursor'])['date'],'20200101')
    def response_error(self,payload=None,read_error=None):
        from unittest.mock import Mock,patch
        self.store.set('account',secret('offline-test-token'))
        class Response:
            def __enter__(self):return self
            def __exit__(self,*args):pass
            def read(self):
                if read_error:raise read_error
                return payload if isinstance(payload,bytes) else json.dumps(payload).encode()
        opener=Mock();opener.open.return_value=Response()
        with patch('urllib.request.build_opener',return_value=opener):
            with self.assertRaises(RemoteError) as caught:self.engine.http('demo',{},['id'])
        return caught.exception
    def test_invalid_params_is_not_a_credential_error(self):
        from core import CredentialError
        error=self.response_error(dict(code=-1,msg='[invalid_params] 业务参数缺失或格式不合法；'))
        self.assertFalse(isinstance(error,CredentialError));self.assertFalse(error.retryable)
    def test_invalid_token_is_still_a_credential_error(self):
        from core import CredentialError
        for message in ('[invalid_token] token invalid','invalid token','Token无效'):
            self.assertIsInstance(self.response_error(dict(code=-1,msg=message)),CredentialError)
    def test_stock_basic_uses_verified_directory_fields(self):
        from unittest.mock import Mock,patch
        self.real_catalog();self.store.set('account',secret('offline-token'))
        class Response:
            def __enter__(self):return self
            def __exit__(self,*args):pass
            def read(self):return b'{"code":0,"data":{"fields":["ts_code","name"],"items":[["000001.SZ","demo"]]}}'
        opener=Mock();opener.open.return_value=Response()
        with patch('urllib.request.build_opener',return_value=opener):self.engine.http('stock_basic',{'list_status':'L'},self.engine.catalog['stock_basic']['fields'])
        payload=json.loads(opener.open.call_args.args[0].data)
        self.assertEqual(payload['fields'],'ts_code,name');self.assertEqual(payload['params'],{'list_status':'L'})
        self.assertEqual(opener.open.call_count,1)
    def test_ip_limit_wait_hint_is_preserved(self):
        error=self.response_error(dict(code=-1,msg='[ip_rate_limited] 来源IP额度已达到限制；建议等待180秒后重试；'))
        self.assertTrue(error.retryable);self.assertTrue(error.ip_limited);self.assertEqual(error.retry_after,180)
    def test_ip_limit_persists_longer_cooldown_and_spacing(self):
        self.plan();self.store.set('account','fake')
        def fetch(*args):raise RemoteError('IP限流，等待180秒',True,180,True)
        self.engine.transport=fetch;self.engine.start('demo');self.engine.thread.start();deadline=time.time()+3
        while time.time()<deadline:
            rows=self.store.rows("SELECT * FROM retries WHERE last_error<>''")
            if rows:break
            time.sleep(.02)
        self.engine.pause();self.assertTrue(rows)
        self.assertGreaterEqual(rows[0]['not_before'],self.store.rows('SELECT time FROM requests ORDER BY time LIMIT 1')[0]['time']+180)
        other=Engine(Store(self.root/'状态'),[ENTRY])
        self.assertEqual(other.store.get('proxy_gap'),180)
        self.assertGreaterEqual(other.due('demo',400),rows[0]['not_before'])
        self.assertEqual(self.job()['records'],0)
    def test_http_retry_after_header_is_respected(self):
        from unittest.mock import Mock,patch
        import urllib.error
        self.store.set('account',secret('offline-token'));opener=Mock()
        opener.open.side_effect=urllib.error.HTTPError(PROXY_URL,429,'rate',{'Retry-After':'240'},None)
        with patch('urllib.request.build_opener',return_value=opener):
            with self.assertRaises(RemoteError) as caught:self.engine.http('demo',{},['id'])
        self.assertTrue(caught.exception.ip_limited);self.assertEqual(caught.exception.retry_after,240)
    def test_business_rate_error_with_permission_document_link_retries(self):
        message='抱歉，您每分钟最多访问该接口200次，权限的具体详情访问：https://tushare.pro/document/1?doc_id=108'
        error=self.response_error(dict(code=-2002,msg=message,data=None))
        self.assertTrue(error.retryable);self.assertIn('权限的具体详情',str(error))
    def test_real_permission_denial_is_not_hidden_by_rate_text_or_doc_link(self):
        for message in ('没有权限调用此接口，权限的具体详情访问：https://tushare.pro/document/1',
                        'Token无效，请勿每分钟重复请求，权限的具体详情访问：https://tushare.pro/document/1',
                        '账号已封禁，每分钟最多200次'):
            with self.subTest(message=message):self.assertFalse(self.response_error(dict(code=2002,msg=message,data=None)).retryable)
    def test_truncated_and_bad_http_response_are_retryable(self):
        import http.client
        for error in (http.client.IncompleteRead(b'{"code":0,',100),http.client.BadStatusLine('truncated response')):
            with self.subTest(error=type(error).__name__):self.assertTrue(self.response_error(read_error=error).retryable)
    def test_invalid_response_encoding_is_retryable(self):
        error=self.response_error(payload=b'\xff');self.assertTrue(error.retryable);self.assertIn('UnicodeDecodeError',str(error))
    def test_business_rate_error_runs_bounded_retry_then_pauses(self):
        from unittest.mock import Mock,patch
        self.plan();old=self.job()['cursor'];self.store.set('account',secret('offline-test-token'))
        message='抱歉，您每分钟最多访问该接口200次，权限的具体详情访问：https://tushare.pro/document/1?doc_id=108'
        class Response:
            def __enter__(self):return self
            def __exit__(self,*args):pass
            def read(self):return json.dumps(dict(code=-2002,msg=message,data=None)).encode()
        opener=Mock();opener.open.side_effect=lambda *args,**kwargs:Response()
        with patch('urllib.request.build_opener',return_value=opener),patch('core.RETRY_SECONDS',0):
            self.engine.start('demo');self.run_until_paused()
        self.assertEqual(opener.open.call_count,3);self.assertEqual(self.job()['status'],'failed');self.assertEqual(self.job()['cursor'],old)
    def test_bulk_catalog_is_complete_explained_and_idempotent(self):
        from bulk import build
        self.real_catalog();report=build(self.engine,'20200101','20201231')
        self.assertEqual(len(report['items']),len(self.engine.catalog));self.assertEqual(sum(report['counts'].values()),len(self.engine.catalog))
        self.assertEqual(len(self.store.rows('SELECT * FROM jobs')),len(self.engine.catalog));self.assertFalse(self.engine.active)
        self.assertEqual(self.store.rows('SELECT * FROM requests'),[])
        self.assertEqual(self.job('daily')['status'],'excluded');self.assertEqual(self.job('news')['status'],'excluded')
        self.assertEqual(self.job('top10_holders')['status'],'waiting')
        with self.store.db() as db:db.execute("UPDATE jobs SET cursor=? WHERE id='income_vip'",(json.dumps(dict(date='20200630',offset=1000,variant=0,previous='fingerprint')),))
        cursor=self.job('income_vip')['cursor'];build(self.engine,'20200101','20201231')
        self.assertEqual(self.job('income_vip')['cursor'],cursor)
    def test_bulk_expands_existing_single_plan_without_deleting_archives(self):
        from bulk import build
        self.plan();self.accept(dict(fields=['id'],items=[[1]]));files=self.store.rows('SELECT * FROM files')
        build(self.engine,'20190101','20201231');p=json.loads(self.job()['plan'])
        self.assertTrue(p['bulk']);self.assertEqual(p['start'],'20190101');self.assertEqual(self.store.rows('SELECT * FROM files'),files)
        self.assertEqual(len(self.store.rows('SELECT * FROM revisions')),1)
    def test_bulk_directory_dependency_is_resolved_before_collection(self):
        from bulk import build
        stock=dict(id='stock_basic',title='证券目录',group='基础',inputs=[],fields=['ts_code'],cap=1000,offset=False,rpm=400)
        detail=dict(id='detail',title='证券明细',group='股票数据 / 参考数据',inputs=[dict(name='ts_code',required=True,type='str')],fields=['id'],cap=1000,offset=False,rpm=400)
        self.engine.catalog={e['id']:e for e in (stock,detail)};build(self.engine,'20200101','20200101');self.store.set('account','fake');calls=[]
        def fetch(api,args,fields):
            calls.append((api,dict(args)))
            return dict(fields=['ts_code'],items=[[args['list_status']+'.SZ']]) if api=='stock_basic' else dict(fields=['id'],items=[[1]])
        self.engine.transport=fetch;self.engine.start();self.run_until_paused()
        self.assertEqual([a for a,_ in calls],['stock_basic']*3+['detail']*3)
        self.assertEqual({v['ts_code'] for api,v in calls if api=='detail'},{'L.SZ','D.SZ','P.SZ'})
        self.assertEqual(self.job('detail')['status'],'complete')
    def test_bulk_extension_respects_selected_end_and_checkpoint(self):
        from bulk import build
        build(self.engine,'20200101','20200102')
        self.accept(dict(fields=['id'],items=[[1]]))
        cursor=self.job()['cursor'];files=self.store.rows('SELECT * FROM files')
        build(self.engine,'20200101','20200103')
        self.assertEqual(json.loads(self.job()['plan'])['end'],'20200103')
        self.assertEqual(self.job()['cursor'],cursor)
        self.assertEqual(self.store.rows('SELECT * FROM files'),files)
        with self.store.db() as db:db.execute("UPDATE jobs SET status='complete' WHERE id='demo'")
        build(self.engine,'20200101','20200104')
        self.assertEqual(json.loads(self.job()['plan'])['end'],'20200104')
        self.assertEqual(self.job()['status'],'pending')
        self.assertLessEqual(self.engine.request_params(self.job())[0]['trade_date'],'20200104')
    @unittest.skip('Linux版固定排除港股目录等独立权限接口，由LinuxTests验证硬排除')
    def test_bulk_hk_directory_includes_all_listing_statuses(self):
        from bulk import build
        self.real_catalog();build(self.engine,'20200101','20201231',include_independent=True,include_overlap=True)
        p=json.loads(self.job('hk_basic')['plan'])
        self.assertEqual(p['params']['list_status'],['L','D','P'])
        for index,status in enumerate(['L','D','P']):
            j=self.job('hk_basic');c=json.loads(j['cursor']);c['variant']=index;j['cursor']=json.dumps(c)
            self.assertEqual(self.engine.request_params(j)[0]['list_status'],status)
        self.assertEqual(json.loads(self.job('hk_income')['plan'])['params']['ts_code']['$catalog'],'hk_basic')
    def test_bulk_trade_calendar_covers_documented_exchanges(self):
        from bulk import build
        self.real_catalog();build(self.engine,'20200101','20200101')
        self.assertEqual(set(json.loads(self.job('trade_cal')['plan'])['params']['exchange']),{'SSE','SZSE','BSE','CFFEX','SHFE','CZCE','DCE','INE'})
    def test_bulk_upgrades_existing_directory_template_once(self):
        from bulk import build
        self.real_catalog();build(self.engine,'20200101','20200101')
        j=self.job('trade_cal');p=json.loads(j['plan']);p['params']['exchange']=['SSE','SZSE','BSE']
        with self.store.db() as db:db.execute('UPDATE jobs SET plan=? WHERE id=?',(json.dumps(p),'trade_cal'))
        args,p,c=self.engine.request_params(self.job('trade_cal'))
        self.engine.accept(self.job('trade_cal'),args,p,c,dict(fields=['exchange','cal_date'],items=[['SSE','20200101']]))
        files=self.store.rows('SELECT * FROM files');old=self.job('trade_cal')['cursor']
        build(self.engine,'20200101','20200101')
        self.assertIn('CFFEX',json.loads(self.job('trade_cal')['plan'])['params']['exchange'])
        self.assertEqual(self.store.rows('SELECT * FROM files'),files)
        revisions=self.store.rows('SELECT * FROM revisions WHERE job=?',('trade_cal',))
        self.assertEqual(len(revisions),1);self.assertEqual(revisions[0]['previous_cursor'],old)
        cursor=self.job('trade_cal')['cursor'];build(self.engine,'20200101','20200101')
        self.assertEqual(self.job('trade_cal')['cursor'],cursor)
        self.assertEqual(len(self.store.rows('SELECT * FROM revisions WHERE job=?',('trade_cal',))),1)
    def test_expanded_hk_directory_rechecks_completed_dependents(self):
        from bulk import build
        source=dict(id='hk_basic',title='港股目录',group='港股数据',inputs=[],fields=['ts_code'],cap=1000,offset=False,rpm=400)
        detail=dict(id='hk_income',title='港股财务',group='港股数据',inputs=[dict(name='ts_code',required=True)],fields=['id'],cap=1000,offset=False,rpm=400)
        self.engine.catalog={e['id']:e for e in (source,detail)}
        build(self.engine,'20200101','20200101')
        p=json.loads(self.job('hk_basic')['plan']);p['params']={}
        q=json.loads(self.job('hk_income')['plan']);q['resolved_params']={'ts_code':['OLD.HK']}
        with self.store.db() as db:
            db.execute("UPDATE jobs SET plan=?,status='complete' WHERE id='hk_basic'",(json.dumps(p),))
            db.execute("UPDATE jobs SET plan=?,status='complete' WHERE id='hk_income'",(json.dumps(q),))
        build(self.engine,'20200101','20200101')
        self.assertEqual(self.job('hk_income')['status'],'pending')
        self.assertNotIn('resolved_params',json.loads(self.job('hk_income')['plan']))
        self.assertEqual(json.loads(self.job('hk_income')['cursor'])['variant'],0)
    def test_empty_snapshot_never_creates_data_file(self):
        self.engine.catalog['demo']['inputs']=[]
        self.engine.plan('demo',{},'20200101','20200101','snapshot','',True)
        self.accept(dict(fields=['id'],items=[]))
        self.assertEqual(self.job()['status'],'complete');self.assertEqual(self.job()['batches'],0)
        self.assertEqual(list((self.root/'数据').rglob('*.json')),[])
        self.assertEqual(self.store.rows('SELECT * FROM files'),[])
        self.assertEqual(self.store.rows('SELECT * FROM history_checks')[0]['records'],0)
    def calendar_plan(self,start='19900101',end='19921231'):
        self.real_catalog();self.engine.plan('trade_cal',{'exchange':'SSE'},start,end,'range','start_date',True)
    def calendar_accept(self,dates):
        j=self.job('trade_cal');a,p,c=self.engine.request_params(j)
        return self.engine.accept(j,a,p,c,dict(fields=['cal_date'],items=[[d] for d in dates]))
    def test_history_scans_empty_year_then_saves_earliest_date(self):
        self.calendar_plan()
        args,_,_=self.engine.request_params(self.job('trade_cal'))
        self.assertEqual((args['start_date'],args['end_date']),('19900101','19901231'))
        self.calendar_accept([])
        self.assertEqual(self.store.rows('SELECT * FROM files'),[])
        self.assertEqual(json.loads(self.job('trade_cal')['cursor'])['date'],'19910101')
        self.calendar_accept(['19910103','19910101','19910102'])
        summary=self.store.rows('SELECT * FROM history_summary')[0]
        self.assertEqual(summary['first_date'],'19910101');self.assertEqual(summary['empty_count'],1)
        self.assertEqual(self.job('trade_cal')['records'],3)
    def test_history_whole_range_empty_does_not_complete(self):
        self.calendar_plan('19900101','19901231');old=self.job('trade_cal')['cursor']
        with self.assertRaisesRegex(ValueError,'覆盖待核验'):self.calendar_accept([])
        self.assertEqual(self.job('trade_cal')['cursor'],old);self.assertEqual(self.job('trade_cal')['status'],'pending')
    def test_history_calendar_rejects_unfiltered_dates(self):
        self.calendar_plan();old=self.job('trade_cal')['cursor']
        with self.assertRaisesRegex(ValueError,'超出'):self.calendar_accept(['20261106'])
        self.assertEqual(self.job('trade_cal')['cursor'],old)
    def test_history_range_cap_narrows_before_saving_or_advancing(self):
        from core import NarrowWindow
        self.calendar_plan();self.engine.catalog['trade_cal']['cap']=2
        with self.store.db() as db:
            p=json.loads(self.job('trade_cal')['plan']);p['cap']=2;db.execute("UPDATE jobs SET plan=? WHERE id='trade_cal'",(json.dumps(p),))
        old=self.job('trade_cal')['cursor']
        with self.assertRaises(NarrowWindow) as caught:self.calendar_accept(['19900101','19900102'])
        self.assertEqual(caught.exception.days,182);self.assertEqual(self.job('trade_cal')['cursor'],old)
        self.assertEqual(self.store.rows('SELECT * FROM files'),[])
    def test_history_loop_narrows_then_collects_all_dates(self):
        self.engine.catalog['demo']['inputs']=[dict(name='start_date',required=False),dict(name='end_date',required=False)]
        self.engine.plan('demo',{},'20200101','20200104','range','start_date',True);self.store.set('account','fake');calls=[]
        def fetch(api,args,fields):
            calls.append(dict(args));span=(dt.datetime.strptime(args['end_date'],'%Y%m%d')-dt.datetime.strptime(args['start_date'],'%Y%m%d')).days+1
            return dict(fields=['id'],items=[[args['start_date']],[args['end_date']]] if span>1 else [[args['start_date']]])
        self.engine.transport=fetch
        with self.store.db() as db:
            p=json.loads(self.job()['plan']);p['offset']=False;db.execute("UPDATE jobs SET plan=? WHERE id='demo'",(json.dumps(p),))
        self.engine.start('demo');self.run_until_paused()
        self.assertEqual(self.job()['status'],'complete');self.assertEqual(self.job()['records'],4)
        self.assertEqual({a['start_date'] for a in calls if a['start_date']==a['end_date']},{'20200101','20200102','20200103','20200104'})
    def test_history_empty_search_survives_restart(self):
        self.calendar_plan();self.calendar_accept([])
        other=Engine(Store(self.root/'状态'),list(self.engine.catalog.values()),data_root=self.root/'数据')
        self.assertEqual(json.loads(other.store.rows("SELECT cursor FROM jobs WHERE id='trade_cal'")[0]['cursor'])['date'],'19910101')
        self.assertEqual(other.store.rows('SELECT * FROM history_summary')[0]['empty_count'],1)
    def test_history_upgrade_preserves_legacy_cursor_and_archives(self):
        self.real_catalog();self.engine.plan('trade_cal',{'exchange':'SSE'},'19900101','19921231','range','start_date',True)
        p=json.loads(self.job('trade_cal')['plan']);p.pop('history_version');p['bulk']=True
        c=json.loads(self.job('trade_cal')['cursor']);c['date']='19900106'
        with self.store.db() as db:db.execute("UPDATE jobs SET plan=?,cursor=? WHERE id='trade_cal'",(json.dumps(p),json.dumps(c)))
        self.engine.upgrade_history();a,p,c=self.engine.request_params(self.job('trade_cal'))
        self.assertEqual(a['start_date'],'19900106');self.assertEqual(a['end_date'],'19910105')
        self.assertEqual(len(self.store.rows('SELECT * FROM revisions')),1)
        self.engine.upgrade_history();self.assertEqual(len(self.store.rows('SELECT * FROM revisions')),1)
    def test_history_daily_empty_guard_keeps_last_checkpoint(self):
        self.engine.plan('demo',{},'20200101','20201231','day','trade_date',True)
        for _ in range(31):self.accept(dict(fields=['id'],items=[]))
        old=self.job()['cursor']
        with self.assertRaisesRegex(ValueError,'覆盖待核验'):self.accept(dict(fields=['id'],items=[]))
        self.assertEqual(self.job()['cursor'],old);self.assertEqual(len(self.store.rows('SELECT * FROM history_checks')),31)
    def test_bulk_requires_pause(self):
        from bulk import build
        self.engine.active=True
        with self.assertRaises(ValueError):build(self.engine,'20200101','20201231')
        self.engine.active=False;self.assertEqual(self.store.rows('SELECT * FROM jobs'),[])
    def test_token_failure_pauses_and_quick_switch_preserves_progress(self):
        from core import CredentialError
        self.plan();self.store.set('account',secret('test-main-token'));self.store.set('backup_account',secret('test-backup-token'));old=self.job()['cursor'];calls=[]
        def fetch(*args):calls.append(1);raise CredentialError('Token无效')
        self.engine.transport=fetch;self.engine.start('demo');self.run_until_paused()
        self.assertEqual(len(calls),1);self.assertEqual(self.store.get('credential_issue')['kind'],'token')
        self.store.set('next_due',time.time()+60);due=self.store.get('next_due');self.engine.switch_token('backup')
        self.assertFalse(self.engine.active);self.assertEqual(self.job()['cursor'],old);self.assertEqual(self.job()['status'],'pending')
        self.assertEqual(self.engine.channel_for('demo'),'backup');self.assertEqual(self.store.get('next_due'),due)
        self.assertIsNone(self.store.get('credential_issue'));self.assertEqual(len(calls),1)
    def test_token_switch_rejects_missing_backup_or_busy_and_privileged(self):
        self.plan()
        with self.assertRaises(ValueError):self.engine.switch_token('backup')
        self.store.set('backup_account',secret('test-backup-token'));self.engine.active=True
        with self.assertRaises(ValueError):self.engine.switch_token('backup')
        self.engine.active=False;self.engine.selected='demo';self.engine.catalog['demo']['independent']=True
        with self.assertRaisesRegex(ValueError,'主通道'):self.engine.switch_token('backup')

if __name__=='__main__':unittest.main()
