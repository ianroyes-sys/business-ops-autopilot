import { NextRequest, NextResponse } from 'next/server';
import { spawn } from 'node:child_process';
import path from 'node:path';
import { opsGuard } from '../../../lib/guard';
import { artifactText, enqueue, readOperations, retryJob, setPaused, saveMonitor } from '../../../../scripts/business-ops.mjs';

export const dynamic = 'force-dynamic';
export const runtime = 'nodejs';
// Root that holds data/ (registry + private queue). The worker script path is resolved from it.
const root = process.env.BUSINESS_OPS_ROOT || process.cwd();
function wake() {
  const child = spawn(process.execPath, [path.join(root, 'scripts/business-ops.mjs'), 'tick'], {
    cwd: root, detached: true, stdio: 'ignore',
    env: { PATH: process.env.PATH, NODE_ENV: process.env.NODE_ENV, BUSINESS_OPS_ROOT: root },
  });
  child.on('error', () => { /* Scheduled retry and heartbeat expose worker failures. */ });
  child.unref();
}
export async function GET(req: NextRequest) {
  const denied = opsGuard(req); if (denied) return denied;
  try {
    const id = req.nextUrl.searchParams.get('report');
    if (id) return new NextResponse(artifactText(id, root), {headers:{'Content-Type':'application/json; charset=utf-8','Content-Disposition':`inline; filename="report-${id}.json"`,'Cache-Control':'private, no-store','X-Content-Type-Options':'nosniff'}});
    return NextResponse.json(readOperations(root), {headers:{'Cache-Control':'private, no-store'}});
  } catch {return NextResponse.json({error:'Operations data could not be verified. Original files are preserved.'},{status:500});}
}
export async function POST(req: NextRequest) {
  const denied = opsGuard(req); if (denied) return denied;
  try {
    if(Number(req.headers.get('content-length')||0)>5_500_000) return NextResponse.json({error:'Use a CSV smaller than 5 MB.'},{status:413});
    const raw=await req.text();
    if(Buffer.byteLength(raw)>5_500_000) return NextResponse.json({error:'Input is too large.'},{status:413});
    const body=JSON.parse(raw);
    if(body.action==='pause'||body.action==='resume') {
      const result={paused:setPaused(body.action==='pause',root)};
      if(body.action==='resume') wake();
      return NextResponse.json(result);
    }
    if(body.action==='retry') {const result=retryJob(String(body.id||''),root);wake();return NextResponse.json(result,{status:202});}
    if(body.action==='monitor') {const result=saveMonitor({url:body.url,label:body.label,enabled:body.enabled},{root});wake();return NextResponse.json(result,{status:202});}
    if(body.action==='run') {wake();return NextResponse.json({requested:true},{status:202});}
    if(body.action!=='enqueue') return NextResponse.json({error:'Unknown operation.'},{status:400});
    const result=enqueue({business:body.business,url:body.url,csv:body.csv,declinedOnly:body.declinedOnly,label:body.label},{root});
    wake();return NextResponse.json(result,{status:202});
  } catch(e) {return NextResponse.json({error:e instanceof SyntaxError?'Invalid request.':e instanceof Error?e.message:'Could not queue operation.'},{status:400});}
}
