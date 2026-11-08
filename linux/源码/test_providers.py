import copy,json,os,tempfile,unittest,time
from pathlib import Path
from unittest.mock import patch
from core import secret
from linux_engine import Engine,Store
from providers import page_key,routed_ids,financial

class ProviderTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.root=Path(self.temp.name)
        self.env=patch.dict(os.environ,{'COLLECTOR_HOME':str(self.root)});self.env.start()
        common=dict(inputs=[dict(name='trade_date',required=True)],fields=['id'],cap=10000,offset=True,rpm=1000,group='股票数据 / 打板专题数据')
        self.catalog=[dict(common,id=x,title=x) for x in ('top_inst','top_list')]+[dict(common,id='stock_basic',title='目录',group='股票数据 / 基础数据',inputs=[]),dict(common,id='income_vip',title='利润表',group='股票数据 / 财务数据')]
        self.store=Store(self.root/'状态');self.engine=Engine(self.store,copy.deepcopy(self.catalog),data_root=self.root/'mydb/Tushare')
        for key in ('account','backup_account','datahub_account'):self.store.set(key,secret('offline-'+key))
    def tearDown(self):self.env.stop();self.temp.cleanup()
    def test_route_and_financial_hard_exclusion(self):
        self.assertIn('top_inst',self.engine.datahub_ids);self.assertNotIn('stock_basic',self.engine.datahub_ids)
        self.assertEqual(self.engine.channel_for('top_inst'),'datahub')
        with self.assertRaisesRegex(ValueError,'财务'):self.engine.plan('income_vip',{},'20260101','20260101','day','trade_date',True)
    def test_provider_cooling_is_independent(self):
        self.store.set('next_due',time.time()+100)
        self.assertLess(self.engine.due('top_inst',1000),time.time()+1)
        self.store.set('datahub_next_due',time.time()+200)
        self.assertGreater(self.engine.due('stock_basic',400),time.time()+99)
    def test_local_date_skips_without_transport(self):
        self.engine.plan('top_list',{},'20260101','20260103','day','trade_date',True)
        with self.store.db() as db:
            db.execute('INSERT INTO local_intervals VALUES(?,?,?,?)',('top_list',0,'20260103','20260103'))
            db.execute('INSERT INTO coverage_scopes VALUES(?,?)',('top_list','{}'))
        j=self.store.rows("SELECT * FROM jobs WHERE id='top_list'")[0]
        args,p,c=self.engine.request_params(j)
        self.assertEqual(args['trade_date'],'20260102')
    def test_local_snapshot_skip_finishes(self):
        self.engine.plan('stock_basic',{},'20260101','20260101','snapshot','',True)
        with self.store.db() as db:db.execute('INSERT INTO local_pages VALUES(?,?,?,?)',('stock_basic',page_key({}),10,'digest'))
        j=self.store.rows("SELECT * FROM jobs WHERE id='stock_basic'")[0]
        self.assertIsNone(self.engine.request_params(j)[0])
        self.assertEqual(self.store.rows("SELECT status FROM jobs WHERE id='stock_basic'")[0]['status'],'complete')
    def test_interval_with_different_parameters_does_not_skip(self):
        self.engine.plan('top_list',{},'20260101','20260103','day','trade_date',True)
        with self.store.db() as db:
            db.execute('INSERT INTO local_intervals VALUES(?,?,?,?)',('top_list',0,'20260103','20260103'))
            db.execute('INSERT INTO coverage_scopes VALUES(?,?)',('top_list','{"exchange":"SSE"}'))
        j=self.store.rows("SELECT * FROM jobs WHERE id='top_list'")[0]
        self.assertEqual(self.engine.request_params(j)[0]['trade_date'],'20260103')
    def test_logs_and_credentials_are_separate(self):
        import threading
        t=threading.Thread(name='采集-datahub',target=lambda:self.store.log('独立消息'));t.start();t.join()
        self.assertEqual(self.store.rows("SELECT channel FROM events WHERE message='独立消息'")[0]['channel'],'datahub')
        self.assertNotIn('offline-datahub_account',self.engine.redact('offline-datahub_account'))

if __name__=='__main__':unittest.main()
