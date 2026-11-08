"""DataHubCo transport: no credential fallback to the original provider."""
import json
import re
import urllib.request
import urllib.error
from core import secret,RemoteError,CredentialError,NoRedirect
from providers import DATAHUB_URL

def fetch(engine,api,params,fields):
    encoded=engine.store.get('datahub_account')
    if not encoded:raise CredentialError('DataHubCo凭据未配置')
    try:token=secret(encoded,True)
    except Exception:raise CredentialError('DataHubCo凭据无法解密') from None
    req=urllib.request.Request(DATAHUB_URL,data=json.dumps(dict(api_name=api,token=token,params=params,fields=','.join(fields))).encode(),headers={'Content-Type':'application/json'})
    try:
        with urllib.request.build_opener(urllib.request.ProxyHandler({}),NoRedirect()).open(req,timeout=90) as response:result=json.load(response)
    except urllib.error.HTTPError as exc:
        status=exc.code
        try:hint=json.loads(exc.read(8192));delay=float(hint.get('help',{}).get('retry_after_seconds',0))
        except Exception:delay=0
        exc.close()
        if status in (401,403):raise CredentialError('DataHubCo拒绝访问：HTTP '+str(status),'token' if status==401 else 'permission') from None
        error=RemoteError('DataHubCo HTTP '+str(status),status in (408,429) or status>=500,delay)
        error.provider_limited=status==429
        raise error from None
    except (OSError,ValueError) as exc:raise RemoteError('DataHubCo连接、超时或JSON解析异常：'+type(exc).__name__,True) from None
    if not isinstance(result,dict) or 'code' not in result:raise RemoteError('DataHubCo响应缺少业务状态',True)
    if result['code']!=0:
        message=engine.redact(str(result.get('msg','未知错误')))[:600]
        if re.search('token|令牌|认证',message,re.I):raise CredentialError(message)
        if re.search('权限|无权|forbidden',message,re.I):raise CredentialError(message,'permission')
        transient=bool(re.search('频率|限流|timeout|temporar|capacity|unavailable|繁忙|超时',message,re.I))
        raise RemoteError('DataHubCo：'+message,transient)
    return result.get('data')

LOG_HTML='''<!doctype html><html lang="zh-CN"><meta charset="utf-8"><title>DataHubCo独立采集日志</title><style>body{background:#101c2d;color:#dde8f5;font:16px sans-serif;margin:30px}pre{white-space:pre-wrap}a{color:#6bd8cd}</style><h1>DataHubCo独立采集日志</h1><p>龙虎榜及后续项目 · 限频、冷却和凭据独立于原有双Token</p><p id="state"></p><pre id="logs"></pre><script>const token=new URLSearchParams(location.hash.slice(1)).get('token');history.replaceState(null,'',location.pathname);async function poll(){try{const r=await fetch('/api/datahub-logs',{headers:{'X-App-Token':token}});const s=await r.json();if(!r.ok)throw Error(s.error);document.getElementById('state').textContent=s.state;document.getElementById('logs').textContent=s.events.map(e=>e.time+' '+e.message).join('\\n')}catch(e){document.getElementById('state').textContent=e.message}}poll();setInterval(poll,3000)</script></html>'''
