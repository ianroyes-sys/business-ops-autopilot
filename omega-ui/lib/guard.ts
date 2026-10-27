import { NextRequest, NextResponse } from 'next/server';
// Local-only API guard: shared key (cookie or header) + same-origin writes.
export function opsGuard(req: NextRequest) {
  if (process.env.NEXT_PUBLIC_MODE === 'snapshot') return NextResponse.json({ error: 'Operations run locally.' }, { status: 503 });
  const key = process.env.BUSINESS_OPS_KEY;
  if (!key || (req.cookies.get('business_ops_key')?.value !== key && req.headers.get('x-business-ops-key') !== key)) return NextResponse.json({ error: 'unauthorized' }, { status: 401 });
  const origin = req.headers.get('origin');
  if (req.method !== 'GET' && origin && origin !== req.nextUrl.origin && origin !== `${req.nextUrl.protocol}//${req.headers.get('host')}`) return NextResponse.json({ error: 'cross-origin' }, { status: 403 });
  return null;
}
