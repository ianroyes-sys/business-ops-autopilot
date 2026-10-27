import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import {artifactText,enqueue,opsRoot,readOperations,readPortfolio,retryJob,setPaused,work,saveMonitor,operationAlerts,classifySiteFailure,ensureCanaryMonitor} from './business-ops.mjs';

function fixture(t,{recurring=[]}={}) {
  const root=fs.mkdtempSync(path.join(os.tmpdir(),'business-ops-test-'));
  fs.mkdirSync(path.join(root,'data'));fs.writeFileSync(path.join(root,'data/business-operations.json'),JSON.stringify({version:1,businesses:[],recurring}));
  t.after(()=>fs.rmSync(root,{recursive:true,force:true}));return root;
}
const good=async(job,{out})=>{fs.mkdirSync(out,{recursive:true});const file=path.join(out,'report.json');fs.writeFileSync(file,JSON.stringify({status:'complete',input:job.payload,proof:'fixture'}));return {file,status:'ready',summary:'Fixture verified',issues:[]};};
const at='2026-09-14T20:00:00.000Z';
test('same input is one job; identity ignores label; CSV attestation and public URL scope are enforced',t=>{
  const root=fixture(t);const a=enqueue({business:'agentproof',url:'https://example.com',label:'one'},{root,now:at});
  const b=enqueue({business:'agentproof',url:'https://example.com/',label:'two'},{root,now:at});
  assert.equal(a.job.id,b.job.id);assert.equal(b.reused,true);assert.equal(b.job.label,'one');
  for(const business of ['constructor','__proto__','missing'])assert.throws(()=>enqueue({business},{root}));
  for(const url of ['http://example.com','https://localhost','https://127.0.0.1','https://user:pw@example.com','https://example.com/path','https://example.com?token=x'])assert.throws(()=>enqueue({business:'agentproof',url},{root}));
  assert.throws(()=>enqueue({business:'baylift',csv:'Total\n123'},{root}));
  const c=enqueue({business:'baylift',csv:'Total\n123',declinedOnly:true},{root});
  assert.equal(fs.readFileSync(path.join(opsRoot(root),'inputs',c.job.id+'.csv'),'utf8'),'Total\n123');
  assert.equal(fs.statSync(path.join(opsRoot(root),'inputs',c.job.id+'.csv')).mode&0o777,0o600);
});
test('successful work has immutable verified report; retry does not mean customer delivery',async t=>{
  const root=fixture(t);const {job}=enqueue({business:'agentproof',url:'https://example.com'},{root,now:at});
  assert.equal((await work({root,adapter:good,now:at})).processed,1);
  assert.equal(readOperations(root).counts.ready,1);assert.equal(JSON.parse(artifactText(job.id,root)).proof,'fixture');
  assert.equal((await work({root,adapter:good,now:at})).processed,0);
  assert.throws(()=>retryJob(job.id,root));
  const done=readOperations(root).jobs[0];fs.writeFileSync(path.join(root,done.artifact),'{}');
  assert.throws(()=>artifactText(job.id,root),/changed/);
});
test('STOP halts processing and resuming does not override global STOP',async t=>{
  const root=fixture(t);enqueue({business:'sales-pipeline'},{root,now:at});setPaused(true,root);
  assert.equal((await work({root,adapter:good,now:at})).status,'paused');assert.equal(readOperations(root).counts.queued,1);
  fs.writeFileSync(path.join(root,'STOP'),'stop');assert.equal(setPaused(false,root),true);
  assert.equal((await work({root,adapter:good,now:at})).processed,0);
});
test('a failed adapter stays failed until explicit retry; artifacts cannot escape its attempt',async t=>{
  const root=fixture(t);const {job}=enqueue({business:'sales-pipeline'},{root,now:at});
  await work({root,now:at,adapter:async()=>{throw Error('Provider unavailable');}});
  assert.equal(readOperations(root).counts.failed,1);assert.equal((await work({root,now:at,adapter:good})).processed,0);
  retryJob(job.id,root);await work({root,now:at,adapter:good});assert.equal(readOperations(root).jobs[0].attempt,2);
  const evil=enqueue({business:'agentproof',url:'https://example.com'},{root,now:at}).job;
  await work({root,now:at,adapter:async()=>({file:path.join(root,'data/business-operations.json'),status:'ready',summary:'fake'})});
  assert.equal(readOperations(root).jobs.find(j=>j.id===evil.id).status,'failed');
});
test('worker lease prevents simultaneous processing, including direct imported calls',async t=>{
  const root=fixture(t);enqueue({business:'sales-pipeline'},{root,now:at});let release;let entered;
  const ready=new Promise(r=>{entered=r;});const pending=new Promise(r=>{release=r;});
  const running=work({root,now:at,adapter:async(j,o)=>{entered();await pending;return good(j,o);}});
  await ready;const second=await work({root,now:at,adapter:good});assert.equal(second.status,'already-running');
  release();await running;assert.equal(readOperations(root).jobs[0].attempt,1);
});
test('old queued jobs remain eligible when more than 100 newer reports exist',async t=>{
  const root=fixture(t);let oldest;
  for(let i=0;i<105;i++){const now=new Date(Date.parse(at)+i*86400000).toISOString();const {job}=enqueue({business:'sales-pipeline'},{root,now});if(i===0)oldest=job.id;else fs.writeFileSync(path.join(opsRoot(root),'jobs',job.id+'.json'),JSON.stringify({...job,status:'ready'}));}
  assert.equal(readOperations(root).jobs.length,100);assert.equal((await work({root,now:at,adapter:good})).processed,1);
  assert.equal(JSON.parse(fs.readFileSync(path.join(opsRoot(root),'jobs',oldest+'.json'))).status,'ready');
});
test('daily checks and explicit weekly monitors dedupe, and disabled monitors stop recurring work',async t=>{
  const root=fixture(t,{recurring:[{business:'sales-pipeline',kind:'pipeline-prep',cadence:'daily',label:'Daily'}]});
  const monitor=saveMonitor({url:'https://example.com'},{root,now:at});
  assert.equal((await work({root,now:at,adapter:good})).processed,2);
  assert.equal((await work({root,now:at,adapter:good})).processed,0);
  assert.equal((await work({root,now:'2026-09-15T20:00:00.000Z',adapter:good})).processed,1);
  assert.equal((await work({root,now:'2026-09-21T20:00:00.000Z',adapter:good})).processed,2);
  saveMonitor({...monitor,enabled:false},{root});
  assert.equal((await work({root,now:'2026-09-28T20:00:00.000Z',adapter:good})).processed,1);
});
test('corrupt jobs stop the worker instead of looking like an empty queue',async t=>{
  const root=fixture(t);const {job}=enqueue({business:'sales-pipeline'},{root,now:at});
  fs.writeFileSync(path.join(opsRoot(root),'jobs',job.id+'.json'),JSON.stringify({...job,kind:'site-audit'}));
  assert.equal(readOperations(root).errors.length,1);await assert.rejects(work({root,now:at,adapter:good}));
  assert.ok(operationAlerts(root).some(a=>a.title==='Business operations source error'));
});
test('interrupted jobs do not become success, and daily alerts resolve after a newer good check',async t=>{
  const root=fixture(t);const {job}=enqueue({business:'sales-pipeline'},{root,now:at});
  fs.writeFileSync(path.join(opsRoot(root),'jobs',job.id+'.json'),JSON.stringify({...job,status:'running',attempt:1}));
  assert.equal((await work({root,now:at,adapter:good})).processed,0);assert.equal(readOperations(root).counts.failed,1);
  enqueue({business:'sales-pipeline'},{root,now:'2026-09-15T20:00:00.000Z'});await work({root,now:at,adapter:good});
  assert.equal(operationAlerts(root).filter(a=>a.id.startsWith(job.id)).length,0);
});
const temporaryReport={schemaVersion:1,product:'AgentProof',status:'partial',pages:[{url:'https://example.com/',status:503,outcome:'http-error'}],actions:[]};
const reviewAdapter=report=>async(job,{out})=>{
  fs.mkdirSync(out,{recursive:true});const file=path.join(out,'report.json');fs.writeFileSync(file,JSON.stringify(report));
  return {file,status:'review',summary:'Coverage incomplete',issues:['Fetch failed.'],retry:{classification:'transient-fetch'}};
};
test('only explicit temporary fetch evidence qualifies for retry',()=>{
  assert.equal(classifySiteFailure(temporaryReport).classification,'transient-fetch');
  assert.equal(classifySiteFailure({...temporaryReport,pages:[{url:'https://example.com/',status:0,outcome:'unavailable',error:'DNS timeout'}]}).classification,'transient-fetch');
  for(const page of [{status:403,outcome:'http-error'},{status:301,outcome:'redirect-not-followed'},{status:0,outcome:'robots-disallowed'},{status:0,outcome:'unavailable',error:'Target DNS includes a non-public or unsupported address'},{status:0,outcome:'unavailable',error:'getaddrinfo ENOTFOUND example.com'}]) {
    assert.equal(classifySiteFailure({...temporaryReport,pages:[{url:'https://example.com/',...page}]}),null);
  }
  assert.equal(classifySiteFailure({...temporaryReport,actions:[{kind:'exception',reason:'Parser could not interpret page'}]}),null);
  assert.equal(classifySiteFailure({...temporaryReport,pages:[]}),null);
});
test('temporary AgentProof failures back off then stop after three attempts with an exception',async t=>{
  const root=fixture(t);const {job}=enqueue({business:'agentproof',url:'https://example.com'},{root,now:at});
  const adapter=reviewAdapter(temporaryReport);
  await work({root,now:at,adapter});
  let current=readOperations(root).jobs[0];
  assert.equal(current.status,'queued');assert.equal(current.nextAttemptAt,'2026-09-14T20:15:00.000Z');
  assert.equal(readOperations(root).readiness.agentproof.retrying,1);
  assert.equal((await work({root,now:'2026-09-14T20:14:00.000Z',adapter})).processed,0);
  assert.equal(operationAlerts(root).some(a=>a.id.startsWith(job.id)),false);
  await work({root,now:'2026-09-14T20:15:00.000Z',adapter});
  current=readOperations(root).jobs[0];assert.equal(current.attempt,2);assert.equal(current.nextAttemptAt,'2026-09-14T21:15:00.000Z');
  await work({root,now:'2026-09-14T21:15:00.000Z',adapter});
  current=readOperations(root).jobs[0];assert.equal(current.status,'review');assert.equal(current.attempt,3);assert.equal(current.retry.exhausted,true);
  assert.equal((await work({root,now:'2026-09-14T23:15:00.000Z',adapter})).processed,0);
  assert.ok(operationAlerts(root).some(a=>a.id.startsWith(job.id)));
});
test('recovered temporary report becomes ready; permanent and non-AgentProof failures never auto retry',async t=>{
  const root=fixture(t);enqueue({business:'agentproof',url:'https://example.com'},{root,now:at});
  await work({root,now:at,adapter:reviewAdapter(temporaryReport)});
  await work({root,now:'2026-09-14T20:15:00.000Z',adapter:good});
  assert.equal(readOperations(root).jobs[0].status,'ready');assert.equal(readOperations(root).jobs[0].nextAttemptAt,null);
  const bad=enqueue({business:'agentproof',url:'https://permanent.example.com'},{root,now:at}).job;
  const other=enqueue({business:'native-supply'},{root,now:at}).job;
  await work({root,now:at,adapter:reviewAdapter({...temporaryReport,pages:[{url:'https://example.com/',status:403,outcome:'http-error'}]})});
  assert.equal(readOperations(root).jobs.find(j=>j.id===bad.id).status,'review');
  assert.equal(readOperations(root).jobs.find(j=>j.id===other.id).status,'review');
  assert.equal((await work({root,now:'2026-09-14T23:15:00.000Z',adapter:good})).processed,0);
});
test('owned weekly canary installs once and preserves existing disabled monitor',t=>{
  const root=fixture(t);const url='https://example.org';assert.equal(ensureCanaryMonitor({root,now:at,url:null}),null);
  const first=ensureCanaryMonitor({root,now:at,url});
  assert.equal(first.url,'https://example.org/');
  assert.equal(ensureCanaryMonitor({root,now:'2026-09-15T20:00:00.000Z',url}).id,first.id);
  saveMonitor({...first,label:'My preserved monitor',enabled:false},{root});
  const preserved=ensureCanaryMonitor({root,now:at,url});assert.equal(preserved.enabled,false);assert.equal(preserved.label,'My preserved monitor');
  assert.equal(readOperations(root).monitors.length,1);
});
test('built-in pipeline-prep adapter runs on the sample records and never treats a missing guard as permission',async t=>{
  const root=fixture(t);const previous=process.env.BUSINESS_OPS_PIPELINE_DIR;
  process.env.BUSINESS_OPS_PIPELINE_DIR=path.join(import.meta.dirname,'../data/example/pipeline');
  t.after(()=>{if(previous===undefined)delete process.env.BUSINESS_OPS_PIPELINE_DIR;else process.env.BUSINESS_OPS_PIPELINE_DIR=previous;});
  const {job}=enqueue({business:'sales-pipeline'},{root,now:at});
  await work({root,now:at});
  const done=readOperations(root).jobs[0];assert.equal(done.status,'review');assert.match(done.issues[0],/No send guard/);
  const report=JSON.parse(artifactText(job.id,root));
  assert.equal(report.counts.drafted,2);assert.equal(report.counts.localSendReceipts,1);
  assert.deepEqual(report.leadHandoff,[],'sent and suppressed leads are excluded');
  assert.equal(report.meetingPreparation.length,1);assert.match(report.meetingPreparation[0].draft,/TestFlight/);
});
test('the committed sample registry keeps a fresh clone readable',()=>{
  const p=readPortfolio(path.join(import.meta.dirname,'..'));assert.equal(p.version,1);assert.equal(p.businesses.length,5);
});
