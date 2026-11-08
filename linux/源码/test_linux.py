import json
import os
import tempfile
import threading
import time
import unittest
import copy
from pathlib import Path
from unittest.mock import patch
from core import secret,CredentialError,RemoteError
from linux_engine import Engine,Store

ENTRY=dict(id='a',title='测试A',group='测试',inputs=[],fields=['id'],cap=2,offset=True,rpm=400)

class LinuxTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.root=Path(self.temp.name)
        self.env=patch.dict(os.environ,{'COLLECTOR_HOME':str(self.root)});self.env.start()
        self.store=Store(self.root/'状态');self.engine=Engine(self.store,copy.deepcopy([ENTRY,dict(ENTRY,id='b',title='测试B')]),data_root=self.root/'mydb/Tushare')
        self.store.set('account',secret('offline-primary-token'));self.store.set('backup_account',secret('offline-backup-token'))
    def tearDown(self):
        self.engine.active=False;self.engine.closing=True;self.engine.wake.set()
        if self.engine.thread.is_alive():self.engine.thread.join(5)
        self.env.stop();self.temp.cleanup()
    def plan(self,ident='a'):self.engine.plan(ident,{},'20200101','20200101','snapshot','',True)
    def job(self,ident='a'):return self.store.rows('SELECT * FROM jobs WHERE id=?',(ident,))[0]
    def wait(self,condition,timeout=5):
        until=time.time()+timeout
        while not condition() and time.time()<until:time.sleep(.02)
        self.assertTrue(condition())
    def run_all(self):
        self.engine.start();self.engine.thread.start();self.wait(lambda:not self.engine.active and not self.engine.current)
    def test_two_tokens_have_concurrent_different_jobs(self):
        self.plan();self.plan('b');barrier=threading.Barrier(2);calls=[]
        def fetch(api,*args):
            calls.append((api,self.engine.local.channel));barrier.wait(2)
            return dict(fields=['id'],items=[[api]])
        self.engine.transport=fetch;self.run_all()
        self.assertEqual({c for _,c in calls},{'main','backup'})
        self.assertEqual({j for j,_ in calls},{'a','b'})
        self.assertTrue(all(j['status']=='complete' for j in self.store.rows('SELECT * FROM jobs')))
    def test_same_job_checkpoint_has_only_one_owner(self):
        self.plan();active=0;maximum=0;lock=threading.Lock()
        def fetch(api,args,fields):
            nonlocal active,maximum
            with lock:active+=1;maximum=max(maximum,active)
            time.sleep(.05)
            with lock:active-=1
            return dict(fields=['id'],items=[[1],[2]] if args['offset']==0 else [[3]])
        self.engine.transport=fetch;self.run_all()
        self.assertEqual(maximum,1);self.assertEqual(self.job()['records'],3)
    def test_permission_error_does_not_stop_other_jobs(self):
        self.plan();self.plan('b')
        def fetch(api,*args):
            if api=='a':raise CredentialError('没有权限','permission')
            return dict(fields=['id'],items=[[1]])
        self.engine.transport=fetch
        with patch('linux_engine.RETRY_SECONDS',0):self.run_all()
        self.assertEqual(self.job()['status'],'unavailable');self.assertEqual(self.job('b')['status'],'complete')
        self.assertEqual(self.store.get('channel_health',{}),{})
    def test_transient_retry_continues_past_three_attempts(self):
        self.plan();calls=[]
        def fetch(*args):
            calls.append(1)
            if len(calls)<=4:raise RemoteError('临时故障',True)
            return dict(fields=['id'],items=[[1]])
        self.engine.transport=fetch
        with patch('linux_engine.RETRY_SECONDS',0):self.run_all()
        self.assertEqual(len(calls),5);self.assertEqual(self.job()['status'],'complete')
    def test_waiting_retry_does_not_block_other_job(self):
        self.plan();self.plan('b');calls=[]
        def fetch(api,*args):
            calls.append(api)
            if api=='a' and calls.count('a')==1:raise RemoteError('临时故障',True)
            return dict(fields=['id'],items=[[1]])
        self.engine.transport=fetch
        with patch('linux_engine.RETRY_SECONDS',.5):self.run_all()
        self.assertEqual(calls,['a','b','a'])
    def test_two_invalid_tokens_wait_without_request_storm(self):
        self.plan();self.plan('b');calls=[]
        def fetch(*args):calls.append(self.engine.local.channel);raise CredentialError('Token无效')
        self.engine.transport=fetch;self.engine.start();self.engine.thread.start()
        self.wait(lambda:len(self.store.get('channel_health',{}))==2)
        self.assertEqual(set(calls),{'main','backup'});self.assertEqual(len(calls),2)
        self.assertTrue(self.engine.active);self.assertEqual(self.engine.status()['phase'],'waiting_credentials')
        self.assertTrue(all(j['status']=='retry' for j in self.store.rows('SELECT * FROM jobs')))
    def test_independent_cannot_be_enabled_manually(self):
        self.engine.catalog['a']['independent']=True
        with self.assertRaisesRegex(ValueError,'独立权限'):self.plan()
    def test_one_channel_permission_failure_does_not_hide_available_data(self):
        self.plan();self.engine.local.channel='backup'
        with patch('linux_engine.RETRY_SECONDS',0):self.engine.fail(self.job(),CredentialError('备用无权限','permission'))
        calls=[]
        def fetch(*args):calls.append(self.engine.local.channel);return dict(fields=['id'],items=[[1]])
        self.engine.transport=fetch;self.run_all()
        self.assertEqual(calls,['main']);self.assertEqual(self.job()['status'],'complete')
    def test_bulk_excludes_independent_even_when_checkbox_true(self):
        from bulk import build
        self.engine.catalog['a']['independent']=True
        build(self.engine,'20200101','20201231',True,True)
        self.assertEqual(self.job()['status'],'excluded')
        self.assertEqual(self.store.rows('SELECT * FROM requests'),[])
    def test_failed_directory_does_not_block_unrelated_job(self):
        self.plan();self.plan('b')
        detail=dict(ENTRY,id='detail',inputs=[dict(name='ts_code',required=True)])
        self.engine.catalog['detail']=detail
        self.engine.plan('detail',{'ts_code':{'$catalog':'a','field':'ts_code'}},'20200101','20200101','snapshot','',True)
        j=self.job('detail');p=json.loads(j['plan']);p['dependencies']=['a']
        with self.store.db() as db:db.execute("UPDATE jobs SET plan=? WHERE id='detail'",(json.dumps(p),))
        def fetch(api,*args):
            if api=='a':raise RemoteError('不支持的接口',False)
            self.assertEqual(api,'b');return dict(fields=['id'],items=[[1]])
        self.engine.transport=fetch;self.run_all()
        self.assertEqual(self.job('detail')['status'],'blocked');self.assertEqual(self.job('b')['status'],'complete')
    def test_retry_and_ip_cooldown_survive_restart(self):
        self.plan()
        def fetch(*args):raise RemoteError('等待120秒',True,120,True)
        self.engine.transport=fetch;self.engine.start();self.engine.thread.start()
        self.wait(lambda:bool(self.store.rows("SELECT * FROM retries WHERE last_error<>''")))
        self.engine.pause();self.wait(lambda:not self.engine.current)
        other=Engine(Store(self.root/'状态'),[ENTRY],data_root=self.root/'mydb/Tushare')
        self.assertGreater(other.due('a',400),time.time()+115);self.assertEqual(other.store.get('proxy_gap'),120)
        self.assertEqual(self.job()['records'],0)
    def test_manual_pause_survives_restart(self):
        self.plan();self.engine.start();self.engine.pause()
        other=Engine(Store(self.root/'状态'),[ENTRY]);other.resume_on_boot();self.assertFalse(other.active)
    def test_running_intent_resumes_on_boot(self):
        self.plan();self.engine.start('a')
        other=Engine(Store(self.root/'状态'),[ENTRY]);other.resume_on_boot()
        self.assertTrue(other.active);self.assertEqual(other.selected,'a');other.pause()
    def test_pause_drains_without_dispatching_new_job(self):
        self.plan();self.plan('b');started=threading.Event();release=threading.Event();calls=[]
        self.store.set('backup_account',None)
        def fetch(api,*args):calls.append(api);started.set();release.wait(2);return dict(fields=['id'],items=[[1]])
        self.engine.transport=fetch;self.engine.start();self.engine.thread.start();self.assertTrue(started.wait(2))
        self.engine.pause();release.set();self.wait(lambda:not self.engine.current)
        self.assertEqual(calls,['a']);self.assertEqual(self.job()['status'],'complete');self.assertEqual(self.job('b')['status'],'pending')
    def test_credentials_are_encrypted_and_private(self):
        encoded=self.store.get('account');self.assertNotIn('offline-primary-token',encoded)
        self.assertEqual(secret(encoded,True),'offline-primary-token')
        if os.name=='posix':self.assertEqual((self.root/'状态/凭据密钥.key').stat().st_mode&0o777,0o600)
        self.assertNotIn('offline-primary-token',json.dumps(self.engine.status()))
    def test_invalid_local_key_is_credential_error(self):
        self.plan();(self.root/'状态/凭据密钥.key').unlink()
        self.engine.local.channel='main'
        with self.assertRaises(CredentialError):self.engine.http('a',{},['id'])
    def test_jobs_share_global_request_window(self):
        now=time.time()
        with self.store.db() as db:db.executemany('INSERT INTO requests VALUES(?,?)',[(now,'a')]*400)
        self.assertGreater(self.engine.due('b',400),now+59)

if __name__=='__main__':unittest.main()
