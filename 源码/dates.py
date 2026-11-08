"""Shared interface-specific date schema for the planner and request builder."""
import calendar
import datetime as dt
import re

def options(entry):
    fields={f['name']:f for f in entry['inputs']};result=[]
    for key in ('trade_date','ann_date','nav_date','date','f_ann_date','period','month','m','year','y','quarter','q'):
        if key not in fields:continue
        mode='quarter' if key in ('period','quarter','q') else 'month' if key in ('month','m') else 'year' if key in ('year','y') else 'day'
        fmt='quarter' if key in ('quarter','q') else 'day' if key=='period' else 'datetime' if fields[key].get('type','str').lower()=='datetime' else mode
        result.append(dict(mode=mode,key=key,format=fmt))
    for start,end,mode,fmt in (('start_date','end_date','range','day'),('start_m','end_m','month','month'),('start_q','end_q','quarter','quarter'),('start_y','end_y','year','year')):
        if {start,end}<=fields.keys():
            if fields[start].get('type','str').lower()=='datetime':fmt='datetime'
            result.append(dict(mode=mode,key=start,end_key=end,format=fmt))
    result.append(dict(mode='snapshot',key='',format='snapshot'))
    return result

def spec(entry,mode,key):
    for value in options(entry):
        if value['mode']==mode and value['key']==key:return value
    raise ValueError('分片方式与日期字段不兼容，请按接口日期格式重新选择')

def align(value,mode):
    d=dt.datetime.strptime(value,'%Y%m%d').date()
    if mode=='month':return value[:6]+'01'
    if mode=='year':return value[:4]+'0101'
    if mode=='quarter':
        m=(d.month-1)//3*3+3
        return f'{d.year}{m:02d}{calendar.monthrange(d.year,m)[1]:02d}'
    return value

def render(value,fmt,end=False):
    d=dt.datetime.strptime(value,'%Y%m%d')
    if fmt=='datetime':return d.strftime('%Y-%m-%d')+(' 23:59:59' if end else ' 00:00:00')
    if fmt=='quarter':return f'{d.year}Q{(d.month-1)//3+1}'
    if fmt=='month':return value[:6]
    if fmt=='year':return value[:4]
    return value

def request_dates(params,schema,value):
    if schema['mode']=='snapshot':return
    params[schema['key']]=render(value,schema['format'])
    if schema.get('end_key'):params[schema['end_key']]=render(value,schema['format'],True)

def parse_bound(value,end=False):
    value=str(value)
    if re.fullmatch(r'\d{4}Q[1-4]',value,re.I):
        year=int(value[:4]);m=(int(value[-1])-1)*3+(3 if end else 1)
        return f'{year}{m:02d}{calendar.monthrange(year,m)[1] if end else 1:02d}'
    digits=re.sub(r'\D','',value)
    if len(digits)==4:return digits+('1231' if end else '0101')
    if len(digits)==6:
        y,m=int(digits[:4]),int(digits[4:]);return digits+(f'{calendar.monthrange(y,m)[1]:02d}' if end else '01')
    if len(digits)>=8:
        dt.datetime.strptime(digits[:8],'%Y%m%d');return digits[:8]
    raise ValueError('无法识别日期边界：'+value)

def extend_snapshot(entry,plan):
    """Convert a fixed temporal snapshot into a real incremental date plan."""
    params=plan['params']
    for schema in options(entry):
        if schema['mode']=='snapshot':continue
        key=schema['key'];end=schema.get('end_key')
        if key not in params:continue
        if isinstance(params[key],list) or (end and isinstance(params.get(end),list)):
            raise ValueError('固定日期数组不能自动更新，请编辑计划改为日期分片')
        start=parse_bound(params[key]);previous_end=parse_bound(params.get(end,params[key]),True)
        params.pop(key,None)
        if end:params.pop(end,None)
        temporal={s['key'] for s in options(entry) if s['key']}|{s['end_key'] for s in options(entry) if s.get('end_key')}
        if temporal.intersection(params):raise ValueError('快照存在多个日期过滤条件，请编辑计划明确更新范围')
        plan.update(mode=schema['mode'],key=key,schema=schema,start=align(start,schema['mode']),end=previous_end)
        return True
    if any(re.search(r'date|period|month|year|quarter|^m$|^q$|start_m|end_m|start_q|end_q',k) for k in params):
        raise ValueError('快照包含固定日期过滤，无法安全更新至今天；请编辑计划改为日期分片')
    return False
