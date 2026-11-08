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
    if plan.get('direction')=='desc':
        from dates import parse_bound
        return parse_bound(args[plan['schema']['key']]),parse_bound(args.get(plan['schema'].get('end_key'),args[plan['schema']['key']]),True)
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


def previous(value,mode='day'):
    import calendar
    d=dt.datetime.strptime(value,'%Y%m%d').date()
    if mode in ('month','quarter'):
        m=d.month-(3 if mode=='quarter' else 1);y=d.year+(m-1)//12;m=(m-1)%12+1
        d=dt.date(y,m,calendar.monthrange(y,m)[1] if mode=='quarter' else 1)
    elif mode=='year':d=dt.date(d.year-1,1,1)
    else:d-=dt.timedelta(days=1)
    return d.strftime('%Y%m%d')

def latest(plan):
    from dates import align
    value=align(plan['end'],plan['mode'])
    if value>plan['end']:value=previous(value,plan['mode'])
    return value

def floor(plan,cursor):
    lower=plan['start']
    migration=plan.get('ascending_prefix')
    if migration and cursor['variant']==migration['variant']:lower=max(lower,migration['date'])
    incremental=plan.get('incremental_scope')
    if incremental:
        # Apply the incremental floor to every previously completed combination.
        # Newly added securities/categories still need their full history.
        values=plan.get('resolved_params',plan['params']);index=cursor['variant'];combo={}
        for key,value in reversed(list(values.items())):
            choices=value if isinstance(value,list) else [value]
            if not choices:return lower
            combo[key]=choices[index%len(choices)];index//=len(choices)
        old=incremental['params']
        if set(combo)==set(old) and all(v in (old[k] if isinstance(old[k],list) else [old[k]]) for k,v in combo.items()):lower=max(lower,incremental['date'])
    return max(lower,plan.get('verified_history_start',lower))

def reverse_dates(args,plan,cursor):
    from dates import render
    schema=plan['schema'];end=cursor['date'];start=end
    if plan['mode']=='range' and schema['format'] in ('day','datetime'):
        days=cursor.get('window_days',365)
        start=max(floor(plan,cursor),(dt.datetime.strptime(end,'%Y%m%d')-dt.timedelta(days=days-1)).strftime('%Y%m%d'))
    args[schema['key']]=render(start,schema['format'])
    if schema.get('end_key'):args[schema['end_key']]=render(end,schema['format'],True)

def reverse_advance(plan,cursor,args):
    from dates import parse_bound
    result=dict(cursor)
    first=parse_bound(args[plan['schema']['key']])
    if plan['mode']=='quarter':
        from dates import align
        first=align(first,'quarter')
    result.update(offset=0,previous='',date=previous(first,plan['mode']))
    result.pop('window_days',None)
    if result['date']<floor(plan,cursor):result.update(variant=cursor['variant']+1,date=latest(plan))
    return result

def empty_limit(plan):
    # Five years of empty history merits verification, never inferred completion.
    return {'day':1826,'month':60,'quarter':20,'year':5}.get(plan['mode'],5)
