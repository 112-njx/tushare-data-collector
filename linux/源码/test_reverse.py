import copy
import datetime as dt
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from core import Engine as Ascending, NarrowWindow, RemoteError
from linux_engine import Engine,Store
from history import latest

ENTRY=dict(id='a',title='倒序测试',group='股票数据',inputs=[dict(name='start_date',required=False),dict(name='end_date',required=False),dict(name='trade_date',required=False)],fields=['date'],cap=2,offset=True,rpm=400)
class ReverseTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.root=Path(self.temp.name)
        self.env=patch.dict(os.environ,{'COLLECTOR_HOME':str(self.root)});self.env.start()
        self.store=Store(self.root/'状态');self.engine=Engine(self.store,[copy.deepcopy(ENTRY)],data_root=self.root/'mydb/Tushare')
    def tearDown(self):self.env.stop();self.temp.cleanup()
    def job(self):return self.store.rows("SELECT * FROM jobs WHERE id='a'")[0]
    def plan(self,start='20200101',end='20220101',mode='range',key='start_date',params=None):
        self.engine.plan('a',params or {},start,end,mode,key,True)
    def accept(self,items):
        j=self.job();a,p,c=self.engine.request_params(j)
        result=self.engine.accept(j,a,p,c,dict(fields=['date'],items=[[v] for v in items]))
        return a,result
    def test_reverse_empty_year_then_older_data_and_no_empty_files(self):
        self.plan();a,_=self.accept([])
        self.assertEqual((a['start_date'],a['end_date']),('20210102','20220101'))
        self.assertEqual(json.loads(self.job()['cursor'])['date'],'20210101')
        self.assertEqual(self.store.rows('SELECT * FROM files'),[])
        self.accept(['20210101'])
        self.assertEqual(self.job()['records'],1)
    def test_pages_keep_same_range_until_final_page_then_move_back(self):
        self.plan();a,_=self.accept(['x','y']);b,p,c=self.engine.request_params(self.job())
        self.assertEqual((a['start_date'],a['end_date']),(b['start_date'],b['end_date']));self.assertEqual(b['offset'],2)
        self.accept(['z']);self.assertLess(json.loads(self.job()['cursor'])['date'],a['start_date'])
    def test_narrow_reverse_window_keeps_upper_edge_and_covers_every_day(self):
        self.plan('20200101','20200104')
        with self.store.db() as db:
            p=json.loads(self.job()['plan']);p['offset']=False
            db.execute('UPDATE jobs SET plan=? WHERE id=?',(json.dumps(p),'a'))
        dates=[]
        for _ in range(20):
            j=self.job();a,p,c=self.engine.request_params(j)
            try:
                items=[['x'],['y']] if a['start_date']!=a['end_date'] else [[a['start_date']]]
                self.engine.accept(j,a,p,c,dict(fields=['date'],items=items))
                dates.append(a['start_date'])
                if self.job()['status']=='complete':break
            except NarrowWindow as e:
                c['window_days']=e.days
                with self.store.db() as db:db.execute('UPDATE jobs SET cursor=? WHERE id=?',(json.dumps(c),'a'))
        self.assertEqual(dates,['20200104','20200103','20200102','20200101'])
    def test_unknown_empty_history_blocks_instead_of_completing(self):
        self.plan('19900101','20260101')
        for _ in range(4):self.accept([])
        before=self.job()['cursor']
        with self.assertRaisesRegex(ValueError,'覆盖待核验'):self.accept([])
        self.assertEqual(self.job()['cursor'],before)
    def test_daily_empty_does_not_stop_at_weekend_or_month_boundary(self):
        self.plan('20200101','20201231','day','trade_date')
        for _ in range(40):self.accept([])
        self.assertEqual(self.job()['status'],'pending')
        self.assertEqual(len(self.store.rows('SELECT * FROM files')),0)
    def test_nonempty_completed_variant_then_next_starts_latest(self):
        self.plan('20200101','20200102','day','trade_date',{'ts_code':['a','b']})
        self.accept(['20200102']);self.accept([])
        a,_,c=self.engine.request_params(self.job())
        self.assertEqual((a['ts_code'],a['trade_date'],c['variant']),('b','20200102',1))
    def test_migration_skips_committed_ascending_prefix_and_survives_restart(self):
        old=Ascending(self.store,[copy.deepcopy(ENTRY)],data_root=self.engine.data_root)
        old.plan('a',{},'20200101','20220101','range','start_date',True)
        j=self.job();a,p,c=old.request_params(j);old.accept(j,a,p,c,dict(fields=['date'],items=[['20200101']]))
        self.engine=Engine(self.store,[copy.deepcopy(ENTRY)],data_root=self.engine.data_root)
        p=json.loads(self.job()['plan']);c=json.loads(self.job()['cursor'])
        self.assertEqual(p['ascending_prefix']['date'],'20201231');self.assertEqual(c['date'],'20220101')
        before=self.job()['cursor'];Engine(self.store,[copy.deepcopy(ENTRY)],data_root=self.engine.data_root)
        self.assertEqual(self.job()['cursor'],before)
        self.accept(['x']);a,_=self.accept(['y']);self.assertEqual(a['start_date'],'20201231')
        self.assertEqual(self.job()['status'],'complete')
    def test_migration_keeps_partial_forward_page_before_reversing(self):
        old=Ascending(self.store,[copy.deepcopy(ENTRY)],data_root=self.engine.data_root)
        old.plan('a',{},'20200101','20220101','range','start_date',True)
        j=self.job();a,p,c=old.request_params(j);old.accept(j,a,p,c,dict(fields=['date'],items=[['x'],['y']]))
        self.engine=Engine(self.store,[copy.deepcopy(ENTRY)],data_root=self.engine.data_root)
        a,p,c=self.engine.request_params(self.job());self.assertEqual(a['offset'],2);self.assertNotIn('direction',p)
        self.accept(['z']);self.engine.reverse_job(self.job())
        a,p,c=self.engine.request_params(self.job());self.assertEqual(p['direction'],'desc');self.assertEqual(a['end_date'],'20220101')
    def test_snapshot_unchanged(self):
        self.plan(mode='snapshot',key='');self.assertNotIn('direction',json.loads(self.job()['plan']))
        self.accept(['x']);self.assertEqual(self.job()['status'],'complete')
    def test_verified_calendar_closure_only_skips_known_dates(self):
        self.plan('20200101','20200105','day','trade_date',{'ts_code':'000001.SZ'})
        self.engine.calendar_cache=((),{('SZSE','20200105'):True,('SZSE','20200104'):True})
        a,_,_=self.engine.request_params(self.job());self.assertEqual(a['trade_date'],'20200103')
    def test_quarter_latest_never_exceeds_end(self):
        self.assertEqual(latest(dict(mode='quarter',end='20261108')),'20260930')
    def test_error_keeps_reverse_checkpoint(self):
        self.plan();before=self.job()['cursor'];self.engine.local.channel='main'
        self.engine.fail(self.job(),RemoteError('rate limited',True))
        self.assertEqual(self.job()['cursor'],before);self.assertEqual(self.job()['status'],'retry')
    def test_update_completed_only_queries_new_suffix_descending(self):
        self.plan('20200101','20200102','day','trade_date');self.accept(['2']);self.accept(['1'])
        self.engine.update('a','20200104');a,p,c=self.engine.request_params(self.job())
        self.assertEqual(a['trade_date'],'20200104');self.assertEqual(p['direction'],'desc')
    def test_revising_policy_does_not_flip_existing_reverse_cursor(self):
        self.plan();self.accept(['x']);before=self.job()['cursor']
        self.engine.plan('a',{},'20200101','20220101','range','start_date',True,revise=True)
        self.assertEqual(self.job()['cursor'],before);self.assertEqual(json.loads(self.job()['plan'])['direction'],'desc')
    def test_explicit_verified_boundary_allows_empty_to_reach_real_floor(self):
        self.engine.catalog['a']['history_start']='20200103'
        self.plan('19900101','20200105','day','trade_date')
        self.accept([]);self.accept([]);self.accept([])
        self.assertEqual(self.job()['status'],'complete')
        self.assertEqual(len(self.store.rows('SELECT * FROM history_checks')),3)
    def test_listing_floor_only_applies_to_matching_security_trading_dates(self):
        from core import stamp
        directory=self.engine.data_root/'目录.json';directory.parent.mkdir(parents=True,exist_ok=True)
        directory.write_text(json.dumps(dict(fields=['ts_code','list_date'],items=[['a','20200103']])))
        with self.store.db() as db:db.execute('INSERT INTO files VALUES(?,?,?,?,?)',('directory','stock_basic',str(directory),1,stamp()))
        self.engine.catalog['stock_basic']=dict(ENTRY,id='stock_basic')
        # Existing completed directory, so dependency resolution uses saved rows.
        with self.store.db() as db:db.execute("INSERT INTO jobs(id,title,group_name,plan,cursor,status,records) VALUES('stock_basic','目录','基础','{}','{}','complete',1)")
        self.plan('19900101','20200105','day','trade_date',{'ts_code':{'$catalog':'stock_basic','field':'ts_code'}})
        for _ in range(3):self.accept([])
        self.assertEqual(self.job()['status'],'complete')
    def test_global_cooldown_does_not_choose_later_task_mid_scan(self):
        import time
        self.plan();self.engine.active=True;self.store.set('next_due',time.time()+30)
        self.engine.channel_ready=lambda c:True
        with patch.object(self.engine,'due',side_effect=AssertionError('should not inspect a later quota during global wait')):
            self.assertIsNone(self.engine.prepare('main'))
        self.assertEqual(self.store.rows('SELECT * FROM requests'),[])
    def test_console_port_reopens_after_response_but_live_listener_stays_exclusive(self):
        import socket
        import threading
        from http.server import BaseHTTPRequestHandler
        from app import Server
        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):self.send_response(200);self.end_headers();self.wfile.write(b'ok')
            def log_message(self,*a):pass
        server=Server(('127.0.0.1',0),Handler);address=server.server_address
        worker=threading.Thread(target=server.handle_request);worker.start()
        with socket.create_connection(address,timeout=2) as client:
            client.sendall(b'GET / HTTP/1.0\r\nHost: localhost\r\n\r\n')
            while client.recv(4096):pass
        worker.join(2);server.server_close()
        reopened=Server(address,Handler)
        try:
            with self.assertRaises(OSError):Server(address,Handler)
        finally:reopened.server_close()
    def test_revise_completed_end_retains_prefix(self):
        self.plan('20200101','20200102','day','trade_date');self.accept(['2']);self.accept(['1'])
        self.engine.plan('a',{},'20200101','20200104','day','trade_date',True,revise=True)
        args,p,c=self.engine.request_params(self.job())
        self.assertEqual(args['trade_date'],'20200104');self.assertEqual(p['incremental_scope']['date'],'20200102')
    def test_editing_unfinished_end_requires_explicit_replay(self):
        self.plan('20200101','20200102','day','trade_date');before=self.job()['cursor']
        with self.assertRaisesRegex(ValueError,'尚未完成'):self.engine.plan('a',{},'20200101','20200104','day','trade_date',True,revise=True)
        self.assertEqual(self.job()['cursor'],before)
    def test_quarter_tokens_include_earliest_quarter_across_year_boundary(self):
        self.engine.catalog['a']['inputs']=[dict(name='q',required=False)]
        self.plan('20191001','20260930','quarter','q')
        calls=[]
        while self.job()['status']!='complete':calls.append(self.accept(['record'])[0]['q'])
        expected=[f'{y}Q{q}' for y in range(2026,2018,-1) for q in (4,3,2,1) if '2019Q4'<=f'{y}Q{q}'<='2026Q3']
        self.assertEqual(calls,expected)
    def test_quarter_range_and_period_keep_aligned_checkpoints(self):
        from history import reverse_advance
        for schema,args in [(dict(key='start_q'),dict(start_q='2026Q2')),(dict(key='period'),dict(period='20260630'))]:
            p=dict(mode='quarter',schema=schema,start='20260331',end='20260930',params={})
            self.assertEqual(reverse_advance(p,dict(date='20260630',variant=0),args)['date'],'20260331')
    def test_completed_multi_symbol_update_limits_all_existing_symbols(self):
        self.plan('20200101','20200120','day','trade_date',{'ts_code':['A','B']})
        while self.job()['status']!='complete':self.accept(['record'])
        self.engine.update('a','20200122');calls=[]
        while self.job()['status']!='complete':calls.append(self.accept(['record'])[0])
        for code in ('A','B'):
            dates=[a['trade_date'] for a in calls if a['ts_code']==code]
            self.assertEqual((min(dates),max(dates),len(dates)),('20200113','20200122',10))
    def test_revised_completed_multi_category_plan_applies_same_floor(self):
        params={'market':['A','B'],'kind':['X','Y']}
        self.plan('20200101','20200120','day','trade_date',params)
        while self.job()['status']!='complete':self.accept(['record'])
        self.engine.plan('a',params,'20200101','20200122','day','trade_date',True,revise=True)
        calls=[]
        while self.job()['status']!='complete':calls.append(self.accept(['record'])[0])
        self.assertEqual(len(calls),12)
        self.assertTrue(all(a['trade_date']>='20200120' for a in calls))
    def test_incremental_scope_survives_restart_and_policy_edit(self):
        self.plan('20200101','20200120','day','trade_date',{'ts_code':['A','B']})
        while self.job()['status']!='complete':self.accept(['record'])
        self.engine.update('a','20200122');self.accept(['record'])
        before=self.job()['cursor'];scope=json.loads(self.job()['plan'])['incremental_scope']
        self.engine=Engine(self.store,[copy.deepcopy(ENTRY)],data_root=self.engine.data_root)
        self.engine.plan('a',{'ts_code':['A','B']},'20200101','20200122','day','trade_date',True,revise=True)
        self.assertEqual(self.job()['cursor'],before);self.assertEqual(json.loads(self.job()['plan'])['incremental_scope'],scope)
    def test_new_parameter_combination_does_not_inherit_old_incremental_floor(self):
        from history import floor
        p=dict(start='20200101',params={'ts_code':['A','B']},incremental_scope={'date':'20200113','params':{'ts_code':['B']}})
        self.assertEqual(floor(p,dict(variant=0)),'20200101')
        self.assertEqual(floor(p,dict(variant=1)),'20200113')
if __name__=='__main__':unittest.main()
