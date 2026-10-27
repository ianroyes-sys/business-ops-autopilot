#!/usr/bin/env node
// Offline demo: a throwaway queue, three jobs, a fixture adapter, then the status packet.
// No network, no external adapters and nothing written outside a temp folder.
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { enqueue, work, readOperations, operationAlerts } from './business-ops.mjs';

const root = fs.mkdtempSync(path.join(os.tmpdir(), 'business-ops-demo-'));
fs.mkdirSync(path.join(root, 'data'));
fs.copyFileSync(new URL('../data/business-operations.sample.json', import.meta.url), path.join(root, 'data/business-operations.json'));
const csv = fs.readFileSync(new URL('../data/example/declined-work.sample.csv', import.meta.url), 'utf8');

enqueue({ business: 'agentproof', url: 'https://example.com', label: 'Example site audit' }, { root });
enqueue({ business: 'agentproof', url: 'https://example.com/', label: 'Same site again' }, { root }); // deduplicated
enqueue({ business: 'baylift', csv, declinedOnly: true, label: 'Sample declined-work export' }, { root });
enqueue({ business: 'native-supply', label: 'Catalog check' }, { root });

// Fixture adapter: pretends the site audit and CSV pass and the catalog check needs review.
const adapter = async (job, { out }) => {
  fs.mkdirSync(out, { recursive: true });
  const file = path.join(out, 'report.json');
  fs.writeFileSync(file, JSON.stringify({ schemaVersion: 1, demo: true, kind: job.kind, payload: job.payload }));
  return job.kind === 'fulfillment-check'
    ? { file, status: 'review', summary: '1 of 2 download archives missing (demo).', issues: ['demo-bundle: archive not packaged'] }
    : { file, status: 'ready', summary: `Demo ${job.kind} report verified.`, issues: [] };
};

const result = await work({ root, adapter });
const ops = readOperations(root);
console.log(`Worker processed ${result.processed} jobs in ${root}`);
for (const j of ops.jobs) console.log(`  ${j.status.padEnd(7)} ${j.label.padEnd(30)} ${j.summary}`);
console.log('Counts:', ops.counts);
console.log('Alerts:', operationAlerts(root).map(a => a.title));
