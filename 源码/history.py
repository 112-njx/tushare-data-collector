"""Chronological history discovery; never assume empty periods are monotonic."""
import datetime as dt
from dates import options

CALENDARS={'trade_cal','hk_tradecal','us_tradecal','fut_trade_cal'}

def preferred_schema(entry):
    schemas=options(entry)
    if entry['id'] in {'daily_basic','share_float'}:return schemas[0]
    for s in schemas:
        if s['mode']!='range' or s['format']!='day':continue
        generated={s['key'],s.get('end_key')}
        if not any(f['required'] and f['name'] in {'trade_date','ann_date','date','period','nav_date'}-generated for f in entry['inputs']):return s
    return schemas[0]

def default_window(plan):
    return 365 if plan['mode']=='range' and plan['schema']['format']=='day' else 1

def bounds(plan,cursor,args):
    start=cursor['date'];end=start
    if plan['mode']=='range' and plan['schema']['format']=='day':end=args[plan['schema']['end_key']]
    return start,end

def next_day(value):return (dt.datetime.strptime(value,'%Y%m%d')+dt.timedelta(days=1)).strftime('%Y%m%d')

def earliest(api,fields,items,start,end):
    if api not in CALENDARS:return None
    if 'cal_date' not in fields:raise ValueError('交易日历缺少cal_date，无法确认历史覆盖')
    index=fields.index('cal_date');dates=[]
    for row in items:
        value=str(row[index]);dt.datetime.strptime(value,'%Y%m%d')
        if not start<=value<=end:raise ValueError('交易日历返回日期超出请求区间，保留断点')
        dates.append(value)
    return min(dates) if dates else None
