"""Read official documentation only. Never calls any data API."""
import concurrent.futures
import datetime
import json
import re
import urllib.request
from html.parser import HTMLParser
from pathlib import Path
from dates import options

ROOT=Path(__file__).resolve().parent
class Page(HTMLParser):
    def __init__(self):
        super().__init__();self.text=[];self.tables=[];self.table_sections=[];self.table=None;self.row=None;self.cell=None;self.links=[];self.link=None;self.parents=[];self.path=[];self.section='';self.table_section=''
    def handle_starttag(self,tag,attrs):
        a=dict(attrs)
        if tag=='li':self.parents.append(None)
        if tag=='a':self.link=[a.get('href',''),[],list(filter(None,self.parents))]
        if tag=='table':
            self.table=[];self.table_section=self.section
        if tag=='tr':self.row=[]
        if tag in ('td','th'):self.cell=[]
    def handle_data(self,data):
        self.text.append(data)
        label=re.sub(r'\s','',data)
        if label in ('输入参数','输入说明'):self.section='input'
        elif label in ('输出参数','输出说明'):self.section='output'
        if self.link is not None:self.link[1].append(data)
        if self.cell is not None:self.cell.append(data)
    def handle_endtag(self,tag):
        if tag=='li' and self.parents:self.parents.pop()
        if tag=='a' and self.link is not None:
            href,parts,parents=self.link;title=''.join(parts).strip()
            self.links.append((href,title,parents))
            if self.parents:self.parents[-1]=title
            self.link=None
        if tag in ('td','th') and self.cell is not None:
            if self.row is not None:self.row.append(''.join(self.cell).strip())
            self.cell=None
        if tag=='tr' and self.row is not None:
            if self.table is not None:self.table.append(self.row)
            self.row=None
        if tag=='table' and self.table is not None:
            self.tables.append(self.table);self.table_sections.append(self.table_section);self.table=None

def page(url):
    p=Page()
    with urllib.request.urlopen(url,timeout=25) as r:p.feed(r.read().decode('utf-8'))
    return p

def load(link):
    href,title,parents=link;url='https://tushare.pro'+href
    p=page(url);text=' '.join(p.text)
    m=re.search(r'接口[：:]\s*([a-z][a-z0-9_]+)',text)
    if not m:return None
    api=m[1];inputs=[];outputs=[]
    for t,section in zip(p.tables,p.table_sections):
        if not t:continue
        h=' '.join(t[0])
        if '名称' not in h or '类型' not in h:continue
        if section=='input' and ('必选' in h or '必填' in h):
            inputs=[dict(name=r[0],type=r[1],required=r[2].upper()=='Y',description=' '.join(r[3:])) for r in t[1:] if len(r)>=4]
        elif section=='output':
            outputs.extend(r[0] for r in t[1:] if r and re.fullmatch('[a-zA-Z0-9_]+',r[0]))
    outputs=list(dict.fromkeys(outputs))
    names={f['name'] for f in inputs};mode='snapshot';key=''
    for k in ('trade_date','ann_date','date','period','month','year'):
        if k in names:mode={'period':'quarter','month':'month','year':'year'}.get(k,'day');key=k;break
    if mode=='snapshot' and {'start_date','end_date'}<=names:mode='range';key='start_date'
    group=' / '.join(parents[-2:]) or '其他'
    excluded=api in {'daily','weekly','monthly','pro_bar','adj_factor','moneyflow','stk_mins','rt_min','rt_k','rt_min_daily','stk_week_month_adj','stk_weekly_monthly','bak_daily','p_save','p_delete','p_list','p_get'}
    # Price overlap needs verification before the user explicitly enables it.
    overlap=(any(w in title for w in ('行情','复权因子')) and api not in {'ths_daily','dc_daily','tdx_daily'}) or api in {'moneyflow_ths','moneyflow_dc'}
    independent=bool(re.search(r'需单独|单独开通|单独开权限|单独申请权限|独立权限|单独2000|单独购买',text[text.find('接口：'):]))
    if parents and parents[0] in ('港股数据','美股数据','大模型语料','量化因子库'):independent=True
    retired='停）' in title or '（停' in title
    limits=re.findall(r'每分钟(?:可以)?(?:请求|调取)?\s*(\d+)\s*次',text)
    rpm=min([400]+[int(v) for v in limits if int(v)>0])
    caps=re.findall(r'(?:单次|每次)[^。\n]{0,35}?(\d{3,6})\s*(?:行|条)',text)
    cap=min([int(v) for v in caps]) if caps else 1000
    note='本地重复，禁止采集' if excluded else '独立权限待确认' if independent else '本地行情覆盖待核对' if overlap else '接口已停更，仅供手工历史查询' if retired else '15000积分范围候选，实际以账号响应为准'
    entry=dict(id=api,title=title,group=group,url=url,inputs=inputs,fields=outputs,mode=mode,date_key=key,rpm=rpm,cap=cap,offset='offset' in names,excluded=excluded,independent=independent,overlap=overlap,retired=retired,note=note,vip=(api+'_vip') in text)
    entry['date_options']=options(entry);entry.update(mode=entry['date_options'][0]['mode'],date_key=entry['date_options'][0]['key'])
    return entry

if __name__=='__main__':
    import sys
    if '--schemas-only' in sys.argv:
        path=ROOT/'catalog.json';data=json.loads(path.read_text(encoding='utf-8'))
        for entry in data['entries']:entry['date_options']=options(entry)
        path.write_text(json.dumps(data,ensure_ascii=False,indent=2),encoding='utf-8')
        print('Date schemas refreshed locally; remote requests: 0');raise SystemExit(0)
    nav=page('https://tushare.pro/document/2');seen=set();links=[]
    for item in nav.links:
        if re.fullmatch(r'/document/2\?doc_id=\d+',item[0]) and item[0] not in seen:seen.add(item[0]);links.append(item)
    entries=[];errors=[]
    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool:
        futures={pool.submit(load,x):x for x in links}
        for future in concurrent.futures.as_completed(futures):
            try:
                v=future.result()
                if v:entries.append(v)
            except Exception as e:errors.append([futures[future][0],str(e)])
    unique={e['id']:e for e in entries if e['id'] not in {'query','pro_api'}}
    import copy
    for e in list(unique.values()):
        if e.get('vip') and 'period' in {f['name'] for f in e['inputs']}:
            v=copy.deepcopy(e);v.update(id=e['id']+'_vip',title=e['title']+'（全市场VIP）',mode='quarter',date_key='period',offset=True)
            for f in v['inputs']:
                if f['name']=='ts_code':f['required']=False
            unique[v['id']]=v
    data=dict(updated=datetime.date.today().isoformat(),source='https://tushare.pro/document/2',entries=sorted(unique.values(),key=lambda e:(e['group'],e['title'])),errors=errors)
    (ROOT/'catalog.json').write_text(json.dumps(data,ensure_ascii=False,indent=2),encoding='utf-8')
    print('documentation entries',len(unique),'failures',len(errors))
