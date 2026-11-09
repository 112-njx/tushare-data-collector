"""Request shapes supported by the collector's original proxy."""

USER_EXCLUDED = frozenset({
    'top_inst', 'top_list', 'broker_recommend', 'trade_cal', 'dc_index',
})

PROJECTIONS = {
    'fund_basic': 'ts_code,name,market,status',
    'etf_basic': 'ts_code,extname,index_code,index_name,exchange,mgr_name',
    'cb_basic': 'ts_code,bond_short_name,stk_code,list_date',
    'sge_basic': 'ts_code,ts_name',
    'fut_basic': 'ts_code,symbol,exchange,name',
    'opt_basic': 'ts_code,name,exchange,opt_code',
    'fund_factor_pro': 'ts_code,trade_date,close',
}

PAGED_DIRECTORIES = {'fund_basic', 'fut_basic', 'opt_basic', 'index_basic'}
PROFILE_IDS = PROJECTIONS.keys() | PAGED_DIRECTORIES


def configure_catalog(entries):
    result = []
    for entry in entries:
        item = dict(entry)
        ident = item['id']
        if ident in USER_EXCLUDED:
            item['excluded'] = True
        if ident in PROJECTIONS:
            item['fields'] = PROJECTIONS[ident].split(',')
        if ident in PAGED_DIRECTORIES:
            item.update(cap=1000, offset=True)
        if ident == 'etf_basic':
            item['cap'] = min(item['cap'], 2000)
        if ident == 'cb_basic':
            # The proxy returned 1,179 rows with this projection, below the documented cap.
            item['cap'] = 2000
        result.append(item)
    return result


def profile_params(ident, params):
    params = dict(params)
    if ident == 'etf_basic':
        params.setdefault('list_status', ['L', 'D', 'P'])
    if ident == 'fut_basic':
        params.setdefault('fut_type', ['1', '2'])
    if ident in ('namechange', 'fund_factor_pro'):
        source = 'stock_basic' if ident == 'namechange' else 'etf_basic'
        params.setdefault('ts_code', {'$catalog': source, 'field': 'ts_code'})
    return params
