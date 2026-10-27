#!/usr/bin/env node
// business-ops: a deterministic local job queue for recurring business operations.
// Only fixed adapters execute; no model, no tool selection and no email sender live in this worker.
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import { fileURLToPath } from 'node:url';
import { execFile, execFileSync } from 'node:child_process';
import { promisify } from 'node:util';

const HERE = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const execute = promisify(execFile);
const ID = /^[a-f0-9]{24}$/;
const KINDS = {'agentproof':'site-audit','baylift':'csv-audit','native-supply':'fulfillment-check','sales-pipeline':'pipeline-prep','arsenal-index':'snapshot-report'};
const supported = business => typeof business === 'string' && Object.hasOwn(KINDS,business);
const digest = value => crypto.createHash('sha256').update(value).digest('hex');
const clock = () => new Date().toISOString();
const SITE_MAX_ATTEMPTS = 3;
const SITE_BACKOFF_MS = [15*60_000,60*60_000];
// Optional weekly self-check against a site you own (set BUSINESS_OPS_CANARY_URL to an https origin).
const CANARY_URL = process.env.BUSINESS_OPS_CANARY_URL || null;
const PYTHON = process.env.BUSINESS_OPS_PYTHON || 'python3';
export function classifySiteFailure(report) {
  if(!report || report.schemaVersion!==1 || report.product!=='AgentProof' || !['partial','blocked'].includes(report.status) || !Array.isArray(report.pages) || !report.pages.length || report.pages.some(p=>!p||typeof p.url!=='string') || !Array.isArray(report.actions)||report.actions.some(a=>!a||typeof a.kind!=='string')) return null;
  const failed=report.pages.filter(p=>p.outcome!=='fetched' && !(String(p.url).endsWith('/robots.txt')&&p.status===404));
  if(!failed.length) return null;
  const temporary=p=>(p.outcome==='http-error'&&[408,429,500,502,503,504].includes(p.status)) ||
    (p.outcome==='unavailable'&&typeof p.error==='string'&&(/^(DNS timeout|Request timeout|socket hang up)$/.test(p.error)||/\b(EAI_AGAIN|ECONNRESET|ETIMEDOUT|ECONNREFUSED)\b/.test(p.error)));
  if(!failed.every(temporary)) return null;
  // Only the two coverage exceptions caused by failed fetches qualify. Parser,
  // policy and content exceptions must not disappear behind a retry loop.
  const derived=new Set(['Robots policy could not be established. Resolve this before continuing.','Homepage did not yield permitted HTML. Review the receipt; this is not a negative business score.']);
  if(report.actions.some(a=>a.kind==='exception'&&!derived.has(a.reason))) return null;
  return {classification:'transient-fetch',reason:failed.map(p=>p.status?`HTTP ${p.status}`:p.error).join('; ')};
}
export const opsRoot = (root = HERE) => path.join(root, 'data/business-ops');
function dirs(root) { for (const dir of ['jobs','outputs','inputs','receipts','monitors']) fs.mkdirSync(path.join(opsRoot(root),dir), {recursive:true,mode:0o700}); }
function atomic(file, value) {
  const tmp = `${file}.${crypto.randomUUID()}.tmp`;
  fs.writeFileSync(tmp, JSON.stringify(value,null,2)+'\n', {mode:0o600});
  fs.renameSync(tmp,file);
}
function json(file) { return JSON.parse(fs.readFileSync(file,'utf8')); }
const jobPath = (root,id) => { if (!ID.test(id)) throw Error('Invalid job ID.'); return path.join(opsRoot(root),'jobs',id+'.json'); };
function receipt(root,job,event) { fs.appendFileSync(path.join(opsRoot(root),'receipts',job.id+'.jsonl'),JSON.stringify({at:clock(),event,status:job.status,attempt:job.attempt||0,artifact:job.artifact||null})+'\n',{mode:0o600}); }

// Real registry first; the committed sample keeps a fresh clone runnable.
function dataFile(root,name) { const real=path.join(root,'data',name+'.json'); const sample=path.join(root,'data',name+'.sample.json'); return !fs.existsSync(real)&&fs.existsSync(sample)?sample:real; }
export function readPortfolio(root = HERE) {
  const data = json(dataFile(root,'business-operations'));
  if (data.version !== 1 || !Array.isArray(data.businesses) || !Array.isArray(data.recurring)) throw Error('Business operations registry needs repair.');
  return data;
}
export function paused(root = HERE) {
  return ['STOP','data/business-ops/STOP'].some(p=>fs.existsSync(path.join(root,p)));
}
export function setPaused(value, root = HERE) {
  dirs(root); const file=path.join(opsRoot(root),'STOP');
  if (value) fs.writeFileSync(file,`Paused by owner ${clock()}\n`,{mode:0o600});
  else if (fs.existsSync(file)) fs.unlinkSync(file);
  return paused(root);
}
function normalizeUrl(value) {
  if(typeof value!=='string'||value.length>2048) throw Error('Provide a public website origin.');
  const u = new URL(value);
  if (u.protocol!=='https:' || u.username || u.password || u.port || u.search || u.hash || u.pathname!=='/') throw Error('Use a public HTTPS website origin without credentials, a custom port, path, query or fragment.');
  if (!u.hostname.includes('.')||u.hostname.endsWith('.')||/^(localhost|.*\.localhost|.*\.local|.*\.internal|.*\.test|.*\.invalid|127\..*|0\..*|10\..*|192\.168\..*|192\.0\..*|169\.254\..*|172\.(1[6-9]|2\d|3[01])\..*|\[.*\])$/i.test(u.hostname)) throw Error('Public websites only.');
  return u.href;
}
function validateJob(job) {
  if(!job||!ID.test(job.id)||!supported(job.business)||job.kind!==KINDS[job.business]||
    !['queued','running','ready','review','failed'].includes(job.status)||
    typeof job.createdAt!=='string'||!Number.isFinite(Date.parse(job.createdAt))||
    !Number.isSafeInteger(job.attempt)||job.attempt<0||!job.payload||typeof job.payload!=='object') throw Error('Invalid job shape.');
  if(job.nextAttemptAt!=null&&(job.kind!=='site-audit'||typeof job.nextAttemptAt!=='string'||!Number.isFinite(Date.parse(job.nextAttemptAt)))) throw Error('Invalid retry schedule.');
  let fingerprint=job.createdAt.slice(0,10);
  if(job.kind==='site-audit') fingerprint=normalizeUrl(job.payload.url)+'|'+fingerprint;
  if(job.kind==='csv-audit') {
    if(job.payload.declinedOnly!==true||!(/^[a-f0-9]{64}$/).test(job.payload.csvHash)) throw Error('Invalid CSV intake attestation.');
    fingerprint=job.payload.csvHash;
  }
  if(job.id!==digest('ops-v1|'+job.business+'|'+fingerprint).slice(0,24)) throw Error('Job identity does not match its source input.');
  return job;
}
export function enqueue(input, {root=HERE, now=clock()}={}) {
  if (!input || !supported(input.business)) throw Error('Choose a supported business.');
  if(typeof now!=='string'||!Number.isFinite(Date.parse(now))) throw Error('Invalid operation date.');
  const kind=KINDS[input.business];
  let payload={}; let fingerprint;
  if (kind==='site-audit') { payload={url:normalizeUrl(input.url)}; fingerprint=payload.url+'|'+now.slice(0,10); }
  else if (kind==='csv-audit') {
    if (input.declinedOnly !== true) throw Error('Confirm this is a declined/deferred-work export you are authorized to process.');
    if (typeof input.csv !== 'string' || !input.csv.trim() || Buffer.byteLength(input.csv)>5_000_000 || input.csv.includes('\0')) throw Error('Provide a nonempty CSV smaller than 5 MB.');
    fingerprint=digest(input.csv); payload={csvHash:fingerprint,declinedOnly:true};
  } else fingerprint=now.slice(0,10);
  const id=digest('ops-v1|'+input.business+'|'+fingerprint).slice(0,24);
  const file=jobPath(root,id); dirs(root);
  if (fs.existsSync(file)) return {job:validateJob(json(file)),reused:true};
  if (kind==='csv-audit') {
    const inputPath=path.join(opsRoot(root),'inputs',id+'.csv');
    try {fs.writeFileSync(inputPath,input.csv,{flag:'wx',mode:0o600});} catch(e) {if(e.code!=='EEXIST'||digest(fs.readFileSync(inputPath))!==payload.csvHash) throw Error('Input integrity check failed.');}
  }
  const job={id,business:input.business,kind,label:String(input.label||input.business+' operation').trim().slice(0,160),payload,status:'queued',createdAt:now,attempt:0,finishedAt:null,artifact:null,summary:'Waiting for the operations worker.',issues:[]};
  // Publish complete JSON atomically with no replacement, including concurrent intake.
  const temp=file+'.'+crypto.randomUUID()+'.tmp';
  fs.writeFileSync(temp,JSON.stringify(job,null,2)+'\n',{mode:0o600});
  try {fs.linkSync(temp,file);} catch(e) {if(e.code!=='EEXIST') throw e; return {job:validateJob(json(file)),reused:true};} finally {fs.unlinkSync(temp);}
  receipt(root,job,'queued'); return {job,reused:false};
}
export function readOperations(root=HERE, {limit=100}={}) {
  const portfolio=readPortfolio(root); const base=opsRoot(root); const jobs=[]; const errors=[];
  if(fs.existsSync(path.join(base,'jobs'))) for(const file of fs.readdirSync(path.join(base,'jobs')).filter(f=>/^[a-f0-9]{24}\.json$/.test(f))) {
    try { const target=path.join(base,'jobs',file); if(!fs.lstatSync(target).isFile()) throw Error('Not a regular job file.'); const job=validateJob(json(target)); if(file!==job.id+'.json') throw Error('shape'); jobs.push(job); } catch {errors.push('Unreadable or invalid job '+file);}
  }
  jobs.sort((a,b)=>b.createdAt.localeCompare(a.createdAt));
  const counts=Object.fromEntries(['queued','running','ready','review','failed'].map(s=>[s,jobs.filter(j=>j.status===s).length]));
  let heartbeat=null;
  try {heartbeat=json(path.join(base,'worker.json'));} catch(e) {if(e.code!=='ENOENT') errors.push('Worker status unreadable.');}
  const stale=!heartbeat || !Number.isFinite(Date.parse(heartbeat.at)) || Date.now()-Date.parse(heartbeat.at)>35*60_000;
  let assessment=null;
  try {assessment=json(dataFile(root,'business-portfolio-review'));} catch(e) {if(e.code!=='ENOENT') errors.push('Portfolio assessment unreadable.');}
  let monitors=[];
  try {monitors=readMonitors(root);} catch {errors.push('Website monitor settings need repair.');}
  const sites=jobs.filter(j=>j.kind==='site-audit');
  const readiness={agentproof:{ready:sites.filter(j=>j.status==='ready').length,waiting:sites.filter(j=>j.status==='queued'&&!j.nextAttemptAt).length,retrying:sites.filter(j=>j.status==='queued'&&j.nextAttemptAt).length,needsAttention:sites.filter(j=>['review','failed'].includes(j.status)).length,maxAutomaticAttempts:SITE_MAX_ATTEMPTS,customerDeliveryVerified:false}};
  return {portfolio,assessment,monitors,readiness,jobs:limit===null?jobs:jobs.slice(0,limit),counts,paused:paused(root),heartbeat,workerStale:stale,errors,total:jobs.length};
}
export function readMonitors(root=HERE) {
  const dir=path.join(opsRoot(root),'monitors');
  if(!fs.existsSync(dir)) return [];
  return fs.readdirSync(dir).filter(f=>f.endsWith('.json')).map(f=>{
    const m=json(path.join(dir,f));
    if(!ID.test(m.id)||f!==m.id+'.json'||m.id!==digest('monitor|'+normalizeUrl(m.url)).slice(0,24)||typeof m.enabled!=='boolean'||!Number.isFinite(Date.parse(m.createdAt))) throw Error('Invalid website monitor.');
    return m;
  });
}
export function saveMonitor(input,{root=HERE,now=clock()}={}) {
  if(input.enabled!==undefined&&typeof input.enabled!=='boolean') throw Error('Monitor enabled must be a boolean.');
  const url=normalizeUrl(input.url);const id=digest('monitor|'+url).slice(0,24);dirs(root);
  const file=path.join(opsRoot(root),'monitors',id+'.json');
  const old=fs.existsSync(file)?json(file):null;
  const value={id,url,label:String(input.label||new URL(url).hostname+' weekly report').slice(0,160),enabled:input.enabled!==false,createdAt:old?.createdAt||now};
  atomic(file,value);return value;
}
export function ensureCanaryMonitor({root=HERE,now=clock(),url=CANARY_URL}={}) {
  if(!url) return null;
  const canonical=new URL(url).href;
  const found=readMonitors(root).find(m=>m.url===canonical);
  // An existing monitor, including the owner's disabled setting, is authoritative.
  return found||saveMonitor({url:canonical,label:'Weekly site-audit canary'},{root,now});
}
export function operationAlerts(root=HERE) {
  const data=readOperations(root,{limit:null});const alerts=[];
  if(!data.paused&&data.workerStale) alerts.push({id:'worker',title:'Business operations worker needs a check',detail:'No recent worker receipt. Open Operations to check or resume the worker.'});
  for(const error of data.errors) alerts.push({id:'source-'+digest(error).slice(0,10),title:'Business operations source error',detail:error});
  const latest=new Map();
  for(const job of data.jobs) {
    // Daily checks replace old checks; each CSV/customer URL still keeps its own exception.
    const key=['sales-pipeline','native-supply','arsenal-index'].includes(job.business)?job.business:job.kind==='site-audit'?job.payload.url:job.id;
    if(!latest.has(key)) latest.set(key,job);
  }
  for(const job of latest.values()) if(['review','failed'].includes(job.status)) alerts.push({id:job.id+'-'+job.attempt,title:job.label+' needs attention',detail:job.summary+(job.issues[0]?' '+job.issues[0]:'')});
  return alerts;
}
export function retryJob(id, root=HERE) {
  const job=validateJob(json(jobPath(root,id)));
  if(!['failed','review'].includes(job.status)) throw Error('Only failed or review jobs can be retried.');
  // Worker owns state; UI writes only a request so an active job cannot be clobbered.
  fs.writeFileSync(path.join(opsRoot(root),'jobs',id+'.retry'),clock(),{mode:0o600});
  return {requested:true};
}
export function artifactText(id, root=HERE) {
  const job=json(jobPath(root,id)); if(!job.artifact) throw Error('No report is available yet.');
  const output=path.join(opsRoot(root),'outputs');
  const resolved=fs.realpathSync(path.resolve(root,job.artifact));
  if(!resolved.startsWith(fs.realpathSync(output)+path.sep)||!resolved.endsWith('.json')) throw Error('Invalid artifact path.');
  if(fs.statSync(resolved).size>4_000_000) throw Error('Report exceeds viewing limit.');
  const bytes=fs.readFileSync(resolved);
  if(digest(bytes)!==job.artifactHash) throw Error('Report changed after verification. Rerun the operation.');
  return bytes.toString('utf8');
}
function strictLedger(file) {
  if(!fs.existsSync(file)) throw Error('Send ledger is unavailable; sent/replied exclusions are unknown.');
  return fs.readFileSync(file,'utf8').split('\n').filter(l=>l.trim()).map(l=>JSON.parse(l));
}
async function pipelineReport(root,out) {
  // Sales records live in BUSINESS_OPS_PIPELINE_DIR (default <root>/data). See data/example/pipeline/ for the shapes.
  const dir=process.env.BUSINESS_OPS_PIPELINE_DIR||path.join(root,'data');
  const leads=json(path.join(dir,'leads.json')).leads;
  const deals=json(path.join(dir,'pipeline.json')).deals;
  const followups=json(path.join(dir,'followups.json'));
  const config=json(path.join(dir,'outreach.json'));
  const suppressed=json(path.join(dir,'suppression.json')).entries;
  let ledger=null; let ledgerError=null;
  try {ledger=strictLedger(path.join(dir,'send-ledger.jsonl'));} catch(e) {ledgerError=e.message;}
  if(![leads,deals,suppressed].every(Array.isArray)||!Array.isArray(followups.drafts)) throw Error('Pipeline source data is incomplete.');
  // Optional external send guard: a script that prints {"allowed_now":N,"blockers":[...]} (exit 0, or 2 when blocked).
  let guard={allowed_now:null,blockers:['No send guard configured (BUSINESS_OPS_SEND_GUARD); outreach eligibility is unknown.']};
  if(process.env.BUSINESS_OPS_SEND_GUARD) {
    let stdout;
    try {({stdout}=await execute(process.execPath,[process.env.BUSINESS_OPS_SEND_GUARD,'check'],{cwd:root,timeout:10_000,maxBuffer:200_000}));} catch(e) {if(e.code===2&&e.stdout)stdout=e.stdout;else throw e;}
    guard=JSON.parse(stdout);
    if(!Number.isSafeInteger(guard.allowed_now)||guard.allowed_now<0||!Array.isArray(guard.blockers)) throw Error('Send guard result is incomplete.');
  }
  const issues=[...(ledgerError?[ledgerError]:[]),...(guard.blockers||[])];
  const normalize=x=>String(x||'').trim().toLowerCase();
  const exclude=new Set([...suppressed.map(x=>normalize(x.email)),...(ledger||[]).filter(x=>['send','reply'].includes(x.event)).map(x=>normalize(x.to))]);
  const drafts=leads.filter(x=>x.status==='drafted');
  const handoff=drafts.filter(x=>!exclude.has(normalize(x.email))).map(x=>({name:x.name||x.organization||x.org,emailAvailable:Boolean(x.email),source:x.url||x.website||null,status:'Reverify contact and current need in your existing sender before sending.'}));
  const report={schemaVersion:1,product:'pipeline-prep',generatedAt:clock(),status:issues.length?'needs_review':'ready',summary:`${drafts.length} drafted leads; ${deals.filter(d=>!['won','lost'].includes(d.stage)).length} open deals; ${guard.allowed_now===null?'send allowance unknown':guard.allowed_now+' emails currently allowed by the send guard'}.`,issues,
    counts:{drafted: drafts.length,localSendReceipts:ledger?ledger.filter(x=>x.event==='send').length:null,localReplyReceipts:ledger?ledger.filter(x=>x.event==='reply').length:null},
    bookingUrl:config.book_url||null,leadHandoff:handoff,
    meetingPreparation:deals.filter(d=>!['won','lost'].includes(d.stage)).map(d=>({name:d.name,stage:d.stage,lastMoved:d.moved||null,recordedContext:d.note||'',draft:followups.drafts.find(f=>f.card===d.name)?.body||null,nextStep:'Verify the latest customer thread, attend the meeting and record the agreed scope.'})),
    limitations:['Source is the local pipeline and ledgers; no inbox sync or current customer confirmation was performed.','Missing send history means candidate eligibility is unknown; this handoff is preparation only.','Deal amounts are recorded proposals, never proof of payment.','Existing sender owns all outreach; no second sender or new permissions created.']};
  fs.mkdirSync(out,{recursive:true,mode:0o700}); atomic(path.join(out,'report.json'),report); return {file:path.join(out,'report.json'),status:issues.length?'review':'ready',summary:report.summary,issues};
}
async function runAdapter(job,{root,developerRoot,out}) {
  validateJob(job);
  if(job.kind==='pipeline-prep') return pipelineReport(root,out);
  // Each adapter is a separate project checked out next to this one (or pointed to by env).
  const dir=(env,name)=>process.env[env]||path.join(developerRoot,name);
  let args,repo;let executable=process.execPath;
  if(job.kind==='csv-audit') {
    repo=dir('BUSINESS_OPS_BAYLIFT_DIR','BayLift');
    const input=path.join(opsRoot(root),'inputs',job.id+'.csv');
    if(!fs.lstatSync(input).isFile()||fs.statSync(input).size>5_000_000) throw Error('Stored CSV is not a regular input within the intake limit.');
    if(digest(fs.readFileSync(input))!==job.payload.csvHash) throw Error('Stored CSV changed since intake.');
    args=[path.join(repo,'scripts/audit-file.mjs'),input,out,'--declined-only'];
  } else if(job.kind==='site-audit') {repo=dir('BUSINESS_OPS_AGENTPROOF_DIR','AgentProof');args=[path.join(repo,'scripts/evidence-audit.mjs'),job.payload.url,out];}
  else if(job.kind==='fulfillment-check') {repo=dir('BUSINESS_OPS_NATIVE_SUPPLY_DIR','native-supply');args=[path.join(repo,'scripts/fulfillment-check.mjs'),out];}
  else if(job.kind==='snapshot-report') {repo=dir('BUSINESS_OPS_ARSENAL_DIR','arsenal-index');executable=PYTHON;args=[path.join(repo,'data/operations_report.py'),'--out',out];}
  else throw Error('Unknown adapter.');
  let result; let exitCode=0;
  try {result=await execute(executable,args,{cwd:repo,timeout:120_000,maxBuffer:300_000,env:{PATH:process.env.PATH||'/usr/local/bin:/usr/bin:/bin'}});} catch(e) {if(e.code===2) {result={stdout:e.stdout||''};exitCode=2;}else throw Error(e.killed?'Operation timed out.':`Operation failed; inspect ${job.business}'s local adapter before retrying.`);}
  let file=path.join(out,'report.json');
  if(job.kind==='csv-audit') {
    const resultData=JSON.parse(result.stdout.trim());
    file=path.join(resultData.outputDirectory,'audit.json');
    if(!path.resolve(file).startsWith(path.resolve(out)+path.sep)) throw Error('Adapter returned an invalid report path.');
  }
  const report=json(file);
  if(report.schemaVersion!==1) throw Error('Adapter report schema is unsupported.');
  let ready=false, summary, issues=[];
  if(job.kind==='csv-audit') {
    if(!['ready','needs_review'].includes(report.deliveryStatus)||report.sourceSha256!==job.payload.csvHash||!Array.isArray(report.issues)) throw Error('BayLift report does not match the supplied CSV.');
    ready=report.deliveryStatus==='ready'&&report.issues.length===0;
    summary=report.summary ? `${report.summary.jobCount} supplied jobs reconciled; declared declined-work value is not recovered revenue.` : 'CSV requires source correction before a report can be prepared.';
    issues=report.issues;
  } else if(job.kind==='site-audit') {
    if(report.product!=='AgentProof'||!['complete','partial','blocked'].includes(report.status)||report.siteUrl!==new URL(job.payload.url).origin||!Array.isArray(report.pages)||!Array.isArray(report.actions)) throw Error('AgentProof returned an invalid evidence report.');
    ready=report.status==='complete';
    issues=report.actions.filter(a=>a.kind==='exception').map(a=>a.reason);
    if(!ready) issues.unshift('Public-site coverage is '+report.status+'. Inspect fetch receipts.');
    summary=`${report.pages.length} page receipts; bounded public-site sample ${report.status}.`;
  } else if(job.kind==='snapshot-report') {
    if(report.businessId!=='arsenal-index'||!['ready','needs_review'].includes(report.status)||!Array.isArray(report.issues)||typeof report.summary!=='string') throw Error('Arsenal Index returned an invalid snapshot report.');
    ready=report.status==='ready'&&report.issues.length===0;issues=report.issues;summary=report.summary;
  } else {
    if(report.businessId!=='native-supply'||!['artifacts_ready','blocked'].includes(report.status)||!Array.isArray(report.products)) throw Error('Native Supply returned an invalid artifact report.');
    ready=report.status==='artifacts_ready';
    issues=report.products.filter(p=>['missing','blocked'].includes(p.status)).map(p=>`${p.slug}: ${p.reason}`);
    summary=`${report.counts?.artifactReady??0}/${report.counts?.downloadable??0} local download archives prepared; release, payment and customer delivery remain unverified.`;
  }
  if(exitCode!==0&&ready) throw Error('Adapter exit status contradicts its completion report.');
  return {file,status:ready?'ready':'review',summary,issues:issues.slice(0,20).map(x=>typeof x==='string'?x:JSON.stringify(x)),retry:job.kind==='site-audit'&&!ready?classifySiteFailure(report):null};
}
async function workUnlocked({root=HERE,developerRoot=process.env.BUSINESS_OPS_ADAPTERS_ROOT||path.dirname(root),adapter=runAdapter,now=clock()}={}) {
  dirs(root);
  if(paused(root)) return {status:'paused',processed:0};
  if(path.resolve(root)===HERE) ensureCanaryMonitor({root,now});
  const before=readOperations(root); if(before.errors.length) throw Error(before.errors.join(' '));
  for(const r of before.portfolio.recurring) {
    if(!['native-supply','sales-pipeline','arsenal-index'].includes(r.business)||r.cadence!=='daily'||KINDS[r.business]!==r.kind) throw Error('Unsupported recurring operation.');
    enqueue({business:r.business,label:r.label},{root,now});
  }
  for(const monitor of before.monitors.filter(m=>m.enabled)) {
    const week=Math.floor((Date.parse(now)-Date.parse(monitor.createdAt))/(7*86400000));
    if(week<0) continue;
    const occurrence=new Date(Date.parse(monitor.createdAt)+week*7*86400000).toISOString();
    enqueue({business:'agentproof',url:monitor.url,label:monitor.label},{root,now:occurrence});
  }
  const jobs=readOperations(root,{limit:null}).jobs.slice().reverse(); let processed=0;
  for(let job of jobs) {
    if(paused(root)) break;
    const file=jobPath(root,job.id); const retry=path.join(opsRoot(root),'jobs',job.id+'.retry');
    if(job.status==='running') {job={...job,status:'failed',finishedAt:clock(),summary:'Previous worker exited before verification. Retry from Operations.',issues:['Interrupted operation; no delivery was established.']};atomic(file,job);receipt(root,job,'interrupted');}
    if(fs.existsSync(retry)) {if(['failed','review'].includes(job.status)) job={...job,status:'queued',nextAttemptAt:null};fs.unlinkSync(retry);}
    if(job.status!=='queued') continue;
    if(job.nextAttemptAt && Date.parse(job.nextAttemptAt)>Date.parse(now)) continue;
    if(processed>=8) {atomic(file,job);continue;}
    job={...job,status:'running',startedAt:clock(),attempt:job.attempt+1,nextAttemptAt:null,retry:null,artifact:null,artifactHash:null,issues:[],summary:'Processing source input.'};atomic(file,job);receipt(root,job,'started');
    const out=path.join(opsRoot(root),'outputs',job.id+'-'+job.attempt);
    try {
      const result=await adapter(job,{root,developerRoot,out});
      if(!['ready','review'].includes(result.status)||!result.file||typeof result.summary!=='string'||(result.issues!==undefined&&!Array.isArray(result.issues))) throw Error('Adapter returned no verified report.');
      const actual=fs.realpathSync(result.file);
      if(!actual.startsWith(fs.realpathSync(out)+path.sep)||!actual.endsWith('.json')||!fs.statSync(actual).isFile()||fs.statSync(actual).size>4_000_000) throw Error('Report is outside this attempt or exceeds the artifact limit.');
      const content=fs.readFileSync(actual); JSON.parse(content);
      job={...job,status:result.status,summary:result.summary,issues:result.issues||[],artifact:path.relative(root,actual),artifactHash:digest(content),finishedAt:clock()};
      if(job.kind==='site-audit'&&result.status==='review') {
        // Reclassify the persisted evidence, never trust an adapter's retry hint.
        const retryEvidence=classifySiteFailure(JSON.parse(content));
        if(retryEvidence) {
          if(job.attempt<SITE_MAX_ATTEMPTS) {
            const nextAttemptAt=new Date(Date.parse(now)+SITE_BACKOFF_MS[job.attempt-1]).toISOString();
            job={...job,status:'queued',nextAttemptAt,retry:{...retryEvidence,maxAttempts:SITE_MAX_ATTEMPTS,exhausted:false},summary:`Temporary website fetch failure. Automatic attempt ${job.attempt+1}/${SITE_MAX_ATTEMPTS} after ${nextAttemptAt}.`,issues:job.issues};
          } else job={...job,retry:{...retryEvidence,maxAttempts:SITE_MAX_ATTEMPTS,exhausted:true},summary:`Website still unavailable after ${SITE_MAX_ATTEMPTS} attempts. Human review required.`};
        }
      }
    } catch(e) {job={...job,status:'failed',summary:e.message,issues:['Operation did not complete. No customer delivery or payment recorded.'],finishedAt:clock()};}
    atomic(file,job);receipt(root,job,job.nextAttemptAt?'retry-scheduled':'finished');processed++;
  }
  const result={at:clock(),status:'ok',processed};atomic(path.join(opsRoot(root),'worker.json'),result);return result;
}
export async function work({root=HERE,...options}={}) {
  dirs(root); const lease=path.join(opsRoot(root),'worker-lease'); const helper=path.join(HERE,'scripts/run_lease.py');
  try {execFileSync(PYTHON,[helper,'acquire',lease,String(process.pid)],{stdio:'pipe'});} catch(e) {if(e.status===75)return {status:'already-running',processed:0};throw e;}
  try {return await workUnlocked({root,...options});} finally {execFileSync(PYTHON,[helper,'release',lease,String(process.pid)],{stdio:'pipe'});}
}
if(process.argv[1]&&path.resolve(process.argv[1])===fileURLToPath(import.meta.url)) {
  const root=process.env.BUSINESS_OPS_ROOT||HERE; const [command,...args]=process.argv.slice(2);
  try {
    let result;
    if(command==='tick') result=await work({root});
    else if(command==='status') result=readOperations(root);
    else if(command==='enqueue') {
      const [business,value]=args;
      result=enqueue({business,...(business==='agentproof'?{url:value}:business==='baylift'?{csv:fs.readFileSync(value,'utf8'),declinedOnly:args.includes('--declined-only')}:{}),label:business+' operation'},{root});
    } else if(command==='retry') result=retryJob(args[0],root);
    else if(command==='pause'||command==='resume') result={paused:setPaused(command==='pause',root)};
    else throw Error('Usage: business-ops.mjs status | tick | enqueue agentproof URL | enqueue baylift FILE --declined-only | enqueue native-supply | enqueue sales-pipeline | retry ID | pause | resume');
    console.log(JSON.stringify(result,null,2));
  } catch(e) {console.error(e.message);process.exitCode=1;}
}
