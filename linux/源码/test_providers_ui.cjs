const {chromium}=require('playwright');
const {execFileSync}=require('child_process');
const fs=require('fs'),path=require('path');
const html=name=>execFileSync('py',['-X','utf8','-c',`import sys;sys.path.insert(0,sys.argv[1]);from ${name==='ui'?'ui import HTML':'datahub import LOG_HTML as HTML'};print(HTML)`,__dirname],{encoding:'utf8'});
(async()=>{
 const root=path.resolve(__dirname,'..'),main=html('ui'),logs=html('datahub');
 const browser=await chromium.launch({headless:true,executablePath:'C:/Program Files (x86)/Microsoft/Edge/Application/msedge.exe',env:{...process.env,TEMP:path.join(root,'构建临时'),TMP:path.join(root,'构建临时')}});
 try{
  const context=await browser.newContext(),errors=[];context.on('page',p=>p.on('pageerror',e=>errors.push(e.message)));
  await context.route('**/*',async route=>{
   const u=new URL(route.request().url());let body,type='application/json';
   if(u.pathname==='/'){body=main;type='text/html'}
   else if(u.pathname==='/datahub-logs'){body=logs;type='text/html'}
   else if(u.pathname==='/api/catalog')body=fs.readFileSync(path.join(__dirname,'catalog.json'),'utf8');
   else if(u.pathname==='/api/datahub-logs')body=JSON.stringify({state:'独立通道等待',events:[{time:'离线',message:'独立日志测试'}]});
   else if(u.pathname==='/api/status'){
    const jobs=[['top_inst','pending',''],['done','complete',''],['excluded','excluded',''],
      ['unknown','unavailable','[unknown_api]'],['policy','unavailable','[api_or_ip_policy]'],
      ['invalid','unavailable','[invalid_params]'],['coverage','blocked','历史覆盖待核验']]
      .map(([id,status,error])=>({id,title:id,status,error,plan:'{}',cursor:'{}',records:0,batches:0,provider:'datahub',local_records:100,local_batches:2}));
    body=JSON.stringify({active:false,current:null,closing:false,jobs,events:[],files:[],history:[],retry_state:[],workers:{},data_root:'/opt/collector/mydb',rpm:400,next_due:0,connection:{},channel_health:{},datahub_configured:true,datahub_rpm:800,datahub_next_due:0});
   }
   else throw Error('非预期请求 '+u.pathname);
   await route.fulfill({contentType:type,body});
  });
  const p=await context.newPage();await p.goto('http://collector.test/#token=offline');
  await p.waitForFunction(()=>document.querySelector('#tasks').textContent.includes('本地已保存 2批 / 100行'));
  if(await p.locator('#datahubToken').getAttribute('type')!=='password')throw Error('凭据框未隐藏');
  if(await p.locator('#datahubRpm').getAttribute('max')!=='1000')throw Error('限频上限错误');
  await p.locator('#hideSettledTasks').check();
  await p.waitForFunction(()=>document.querySelectorAll('#tasks tr:not([hidden])').length===3);
  await p.reload();
  await p.waitForFunction(()=>document.querySelector('#taskVisibility').textContent.includes('3 / 7'));
  await p.locator('#hideSettledTasks').uncheck();
  await p.waitForFunction(()=>document.querySelectorAll('#tasks tr:not([hidden])').length===7);
  const popup=context.waitForEvent('page');await p.locator('#datahubLogs').click();const logPage=await popup;await logPage.waitForSelector('#logs');await logPage.waitForFunction(()=>document.querySelector('#logs').textContent.includes('独立日志测试'));
  if(errors.length)throw Error(errors.join(';'));
  await p.screenshot({path:path.join(root,'构建临时/DataHubCo页面验收.png'),fullPage:true});
  console.log(JSON.stringify({providerLabel:true,localProgress:true,separateLogs:true,remoteRequests:0,jsErrors:errors}));
 }finally{await browser.close()}
})().catch(e=>{console.error(e);process.exitCode=1});
