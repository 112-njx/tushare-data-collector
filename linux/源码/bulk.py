"""Local full-plan generation. No API calls during planning."""
import json
from dates import options,align
from history import preferred_schema

FOUNDATIONS=['stock_basic','trade_cal','fund_basic','etf_basic','cb_basic','index_basic','index_classify','fut_basic','opt_basic','hk_basic','us_basic','sge_basic','fx_obasic','ths_index','dc_index','tdx_index','factor_list']
FUT_EXCHANGES=['CFFEX','DCE','CZCE','SHFE','INE','GFEX']
DEFAULTS={
 'stock_basic':{'list_status':['L','D','P']},
 'trade_cal':{'exchange':['SSE','SZSE','BSE','CFFEX','SHFE','CZCE','DCE','INE']},
 'hk_basic':{'list_status':['L','D','P']},
 'fund_basic':{'market':['E','O'],'status':['D','I','L']},
 'index_basic':{'market':['SSE','SZSE','CSI','SW','CICC','MSCI','OTH']},
 'fut_basic':{'exchange':FUT_EXCHANGES},
 'opt_basic':{'exchange':['SSE','SZSE']+FUT_EXCHANGES},
 'stock_company':{'exchange':['SSE','SZSE','BSE']},
 'stock_hsgt':{'type':['HK_SZ','HK_SH','SH','SZ']},
 'dc_index':{'idx_type':['行业板块','概念板块','地域板块']},
 'ths_index':{'exchange':'A','type':['N','I','R','S','ST','TH','BB']},
 'index_member_all':{'is_new':['Y','N']},'ci_index_member':{'is_new':['Y','N']},
 'fut_weekly_monthly':{'freq':['week','month']},
 'news':{'src':['sina','wallstreetcn','10jqka','eastmoney','yuncaijing','fenghuang','jinrongjie','cls','yicai']},
}
REFERENCES={
 'index_weight':('index_code','index_basic','ts_code'),
 'ths_member':('ts_code','ths_index','ts_code'), 'ths_daily':('ts_code','ths_index','ts_code'),
 'dc_member':('ts_code','dc_index','ts_code'), 'tdx_member':('ts_code','tdx_index','ts_code'),
 'factor_value':('factor_name','factor_list','factor_name'),
}

def profile(entry,catalog):
    ident=entry['id'];params=dict(DEFAULTS.get(ident,{}));schema=preferred_schema(entry)
    if ident in REFERENCES:
        target,source,field=REFERENCES[ident];params[target]={'$catalog':source,'field':field}
    generated={schema['key'],schema.get('end_key')} if schema['mode']!='snapshot' else set()
    for field in entry['inputs']:
        name=field['name']
        if not field['required'] or name in generated or name in params:continue
        if name=='ts_code' and ident in {'daily_basic','share_float'} and schema['key'] in {'trade_date','ann_date'}:continue
        if name=='ts_code':
            source='cb_basic' if entry['group']=='债券专题' else 'hk_basic' if entry['group']=='港股数据' else 'us_basic' if entry['group']=='美股数据' else 'fund_basic' if entry['group']=='公募基金' else 'etf_basic' if entry['group']=='ETF专题' else 'index_basic' if entry['group']=='指数专题' else 'opt_basic' if entry['group']=='期权数据' else 'fut_basic' if entry['group']=='期货数据' else 'stock_basic'
            params[name]={'$catalog':source,'field':'ts_code'}
        else:return params,schema,'无法安全自动补足必填字段：'+name
    if ident=='fut_weekly_detail':return params,schema,'周序号接口尚无可靠历史分片规则，需高级配置确认'
    return params,schema,''

def build(engine,start,end,include_independent=False,include_overlap=False,preserve_existing=False):
    from core import DENIED,stamp
    import datetime as dt
    engine.paused()
    for value in (start,end):dt.datetime.strptime(value,'%Y%m%d')
    if start>end or end>dt.date.today().strftime('%Y%m%d'):raise ValueError('全量历史范围无效')
    report=[];changed_sources=set()
    entries=sorted(engine.catalog.values(),key=lambda e:(FOUNDATIONS.index(e['id']) if e['id'] in FOUNDATIONS else 100,e['group'],e['id']))
    for entry in entries:
        ident=entry['id'];old=engine.store.rows('SELECT * FROM jobs WHERE id=?',(ident,));reason='';state='pending'
        if preserve_existing and old and not json.loads(old[0]['plan']).get('bulk_placeholder'):
            report.append(dict(id=ident,title=entry['title'],status='kept',reason='保留已有计划范围、参数、状态及断点；配置账号不重放或自动延长历史'))
            continue
        if entry.get('excluded') or ident in DENIED:state='excluded';reason='本地重复数据或非采集接口'
        elif ident+'_vip' in engine.catalog:state='excluded';reason='由同字段全市场VIP接口覆盖，避免重复拉取'
        elif entry.get('independent'):state='excluded';reason='Linux版自动排除独立权限接口，不发送请求'
        elif entry.get('overlap') and not include_overlap:state='blocked';reason='其他资产行情或资金流与本地覆盖关系待确认'
        params,schema,unsupported=profile(entry,engine.catalog)
        if not reason and unsupported:state='blocked';reason=unsupported
        priority=FOUNDATIONS.index(ident) if ident in FOUNDATIONS else 100
        if reason:
            if not old:
                with engine.store.db() as db:db.execute('INSERT INTO jobs(id,title,group_name,plan,cursor,status,error,priority) VALUES(?,?,?,?,?,?,?,?)',(ident,entry['title'],entry['group'],json.dumps({'bulk_placeholder':True}),json.dumps({}),state,reason,priority))
            elif old[0]['status'] not in ('excluded','blocked'):
                # Preserve existing explicit plans, including their failures and scopes.
                report.append(dict(id=ident,title=entry['title'],status='kept',reason='保留已有单项计划；'+reason));continue
        else:
            try:
                placeholder=bool(old and json.loads(old[0]['plan']).get('bulk_placeholder'))
                dependencies=sorted({v['$catalog'] for v in params.values() if isinstance(v,dict) and '$catalog' in v})
                if not old or placeholder:
                    if placeholder:
                        # Replace only an unexecuted explanatory placeholder, never real progress.
                        with engine.store.db() as db:db.execute('DELETE FROM jobs WHERE id=? AND batches=0',(ident,))
                    engine.plan(ident,params,start,end,schema['mode'],schema['key'],allow_empty=True,confirmed=True)
                    with engine.store.db() as db:
                        row=db.execute('SELECT plan FROM jobs WHERE id=?',(ident,)).fetchone();p=json.loads(row['plan']);p.update(bulk=True,dependencies=dependencies)
                        db.execute('UPDATE jobs SET plan=?,priority=?,status=?,error=? WHERE id=?',(json.dumps(p),priority,'waiting' if dependencies else 'pending','等待证券/分类目录：'+','.join(dependencies) if dependencies else '',ident))
                    state='waiting' if dependencies else 'pending';reason='目录完成后自动补齐参数' if dependencies else '历史分片已自动设置；允许正常成功空记录'
                else:
                    p=json.loads(old[0]['plan'])
                    template_changed=p.get('bulk') and (p['params']!=params or bool(changed_sources.intersection(dependencies)))
                    if not p.get('bulk') or p['start']>align(start,schema['mode']) or template_changed:
                        engine.plan(ident,params,min(start,p['start']),max(end,p['end']),schema['mode'],schema['key'],True,True,revise=True,replay=True,channel=p.get('channel','auto'))
                        with engine.store.db() as db:
                            updated=json.loads(db.execute('SELECT plan FROM jobs WHERE id=?',(ident,)).fetchone()['plan']);updated.update(bulk=True,dependencies=dependencies)
                            db.execute('UPDATE jobs SET plan=? WHERE id=?',(json.dumps(updated),ident))
                        state='pending';reason='补齐查询范围或目录条件，保留旧归档及修订前断点'
                        if template_changed:changed_sources.add(ident)
                    elif p.get('bulk') and p['end']<end:engine.update(ident,end=end);state='pending';reason='范围延长至所选截止日期，沿用断点'
                    else:state='kept';reason='已有计划和断点保留'+('；原单项范围不等于全量范围，请点编辑计划确认' if not p.get('bulk') else '')
                    with engine.store.db() as db:db.execute('UPDATE jobs SET priority=? WHERE id=?',(priority,ident))
            except (ValueError,KeyError) as exc:
                state='blocked';reason=str(exc)
                if not engine.store.rows('SELECT id FROM jobs WHERE id=?',(ident,)):
                    with engine.store.db() as db:db.execute('INSERT INTO jobs(id,title,group_name,plan,cursor,status,error,priority) VALUES(?,?,?,?,?,?,?,?)',(ident,entry['title'],entry['group'],json.dumps({'bulk_placeholder':True}),json.dumps({}),state,reason,priority))
        report.append(dict(id=ident,title=entry['title'],status=state,reason=reason))
    result=dict(created_at=stamp(),start=start,end=end,items=report,counts={s:sum(r['status']==s for r in report) for s in ('pending','waiting','blocked','excluded','kept')})
    engine.store.set('bulk_report',result);engine.store.log('全量计划已生成，保持暂停；'+json.dumps(result['counts'],ensure_ascii=False))
    return result
