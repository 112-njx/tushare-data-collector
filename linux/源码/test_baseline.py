import copy
import hashlib
import json
import os
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import patch
from core import Engine as BaseEngine,secret
from linux_engine import Store,Engine
from export_baseline import export
from baseline import load,canonical

ENTRY=dict(id='stock_basic',title='股票目录',group='基础',inputs=[],fields=['ts_code'],cap=2,offset=True,rpm=400)

class BaselineTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.root=Path(self.temp.name)
        self.env=patch.dict(os.environ,{'COLLECTOR_HOME':str(self.root/'cloud')});self.env.start()
        self.windows=self.root/'windows';self.catalog=[copy.deepcopy(ENTRY)]
        self.source=Store(self.windows/'状态');self.source.set('account','private-account-must-not-export')
        self.local=BaseEngine(self.source,self.catalog,data_root=self.root/'mydb/Tushare')
        self.local.plan('stock_basic',{},'20200101','20200101','snapshot','',True)
        self.cloud=self.root/'cloud';self.folder=self.cloud/'基线'
    def tearDown(self):self.env.stop();self.temp.cleanup()
    def accept_local(self,codes):
        j=self.source.rows('SELECT * FROM jobs')[0];a,p,c=self.local.request_params(j)
        self.local.accept(j,a,p,c,dict(fields=['ts_code'],items=[[v] for v in codes]))
    def test_exports_only_whitelisted_state_without_credentials(self):
        self.accept_local(['000001.SZ']);export(self.windows,self.folder)
        manifest=(self.folder/'本机采集基线.json').read_text(encoding='utf-8')
        self.assertNotIn('private-account-must-not-export',manifest);self.assertNotIn('settings',manifest)
        self.assertEqual(len(list(self.folder.rglob('*.sqlite'))),0)
    def test_completed_directory_is_not_requested_again(self):
        self.accept_local(['000001.SZ']);export(self.windows,self.folder)
        store=Store(self.cloud/'状态');load(store,self.catalog,self.cloud)
        engine=Engine(store,self.catalog,data_root=self.cloud/'mydb/Tushare')
        self.assertEqual(engine.catalog_values('stock_basic','ts_code'),['000001.SZ'])
        self.assertEqual(store.rows('SELECT * FROM jobs')[0]['status'],'complete')
        self.assertEqual(store.rows('SELECT * FROM files'),[])
        self.assertEqual(store.rows('SELECT * FROM requests'),[])
    def test_partial_page_resumes_after_local_offset_and_only_archives_increment(self):
        self.accept_local(['000001.SZ','000002.SZ']);export(self.windows,self.folder)
        store=Store(self.cloud/'状态');load(store,self.catalog,self.cloud);store.set('account',secret('offline-cloud-token'))
        engine=Engine(store,self.catalog,data_root=self.cloud/'mydb/Tushare');calls=[]
        def fetch(api,args,fields):calls.append(dict(args));return dict(fields=['ts_code'],items=[['000003.SZ']])
        engine.transport=fetch;engine.start();engine.thread.start();deadline=time.monotonic()+5
        try:
            while (engine.active or engine.current) and time.monotonic()<deadline:time.sleep(.02)
            self.assertEqual([v['offset'] for v in calls],[2])
            self.assertEqual(store.rows('SELECT * FROM jobs')[0]['records'],3)
            self.assertEqual(sum(f['records'] for f in store.rows('SELECT * FROM files')),1)
            self.assertEqual(engine.catalog_values('stock_basic','ts_code'),['000001.SZ','000002.SZ','000003.SZ'])
        finally:engine.closing=True;engine.wake.set();engine.thread.join(5)
    def test_reimport_never_overwrites_cloud_progress(self):
        self.accept_local(['000001.SZ']);export(self.windows,self.folder)
        store=Store(self.cloud/'状态');load(store,self.catalog,self.cloud)
        with store.db() as db:db.execute("UPDATE jobs SET records=99 WHERE id='stock_basic'")
        load(store,self.catalog,self.cloud)
        self.assertEqual(store.rows('SELECT records FROM jobs')[0]['records'],99)
    def test_corrupt_cache_rejects_before_import(self):
        self.accept_local(['000001.SZ']);export(self.windows,self.folder)
        f=next((self.folder/'目录缓存').rglob('*.json'));f.write_text('{}')
        store=Store(self.cloud/'状态')
        with self.assertRaisesRegex(ValueError,'校验失败'):load(store,self.catalog,self.cloud)
        self.assertEqual(store.rows('SELECT * FROM jobs'),[])
    def test_different_baseline_cannot_replace_active_cloud(self):
        self.accept_local(['000001.SZ']);export(self.windows,self.folder)
        store=Store(self.cloud/'状态');load(store,self.catalog,self.cloud)
        p=self.folder/'本机采集基线.json';v=json.loads(p.read_text());v.pop('manifest_id');v['created_at']='changed';v['manifest_id']=hashlib.sha256(canonical(v)).hexdigest();p.write_text(json.dumps(v))
        with self.assertRaisesRegex(ValueError,'不同基线'):load(store,self.catalog,self.cloud)

if __name__=='__main__':unittest.main()
