// scripts/export-and-upload-icsource.mjs
// Export Supabase -> CSV (no aggregation), upload to IC Source FTP, and log to Supabase

import fs from 'node:fs';
import path from 'node:path';
import { createRequire } from 'module';
const require = createRequire(import.meta.url);
import { createClient } from '@supabase/supabase-js';
import ftp from 'basic-ftp';
import { stringify } from 'csv-stringify/sync';

// ---------- ENV ----------
const {
  SUPABASE_URL,
  SUPABASE_ANON_KEY,        // we’re using anon for testing
  SUPABASE_SERVICE_ROLE,    // (optional) ignore for now
  ICSOURCE_FTP_USER,
  ICSOURCE_FTP_PASS,
  ICSOURCE_FTPS,            // "true" to use FTPS (explicit TLS). defaults false
  SUPABASE_EXCESS_TABLE,    // optional; defaults to 'excess_parts'
  SUPABASE_EXCESS_SCHEMA,   // optional; defaults to 'public'
  GITHUB_REPOSITORY,
  GITHUB_RUN_ID,
} = process.env;

const SCHEMA = SUPABASE_EXCESS_SCHEMA || 'public';
const TABLE  = SUPABASE_EXCESS_TABLE  || 'excess_parts';
const FTPS   = String(ICSOURCE_FTPS || '').toLowerCase() === 'true';

// ---------- UTILS ----------
function runUrl() {
  if (GITHUB_REPOSITORY && GITHUB_RUN_ID) {
    return `https://github.com/${GITHUB_REPOSITORY}/actions/runs/${GITHUB_RUN_ID}`;
  }
  return null;
}

async function sleep(ms){ return new Promise(r=>setTimeout(r, ms)); }

// ---------- Supabase clients ----------
const supabase = createClient(SUPABASE_URL, SUPABASE_ANON_KEY, {
  auth: { persistSession: false }
});

// ---------- Export to CSV (NO aggregation) ----------
async function exportCsvNoAggregation() {
  console.log('Dry run: false');
  console.log('Aggregation: OFF (each row exported)');

  const pageSize = 1000;
  let cursor = null;
  let fetched = 0;

  const rows = [];
  const seenSku = new Set();
  let totalQty = 0;

  // Ensure output dir
  await fs.promises.mkdir('out', { recursive: true });

  // Cursor-based paging on "id"
  console.log('Cursor paging on column: id (page=1000)');
  while (true) {
    let q = supabase
      .from(`${SCHEMA}.${TABLE}`)
      .select('*')
      .order('id', { ascending: true })
      .limit(pageSize);

    if (cursor != null) q = q.gt('id', cursor);
    const { data, error } = await q;

    if (error) throw new Error(`Supabase fetch error: ${error.message}`);
    if (!data || data.length === 0) break;

    for (const r of data) {
      rows.push({
        mpn: (r.part_number || '').trim(),
        qty: Number(r.quantity || 0),
        vendor: (r.vendor || '').trim(),
      });
      if (r.part_number) seenSku.add(r.part_number);
      totalQty += Number(r.quantity || 0);
    }

    fetched += data.length;
    console.log(`Fetched ${fetched}/? rows so far…`);
    cursor = data[data.length - 1].id;

    if (data.length < pageSize) break;
    await sleep(25); // be nice
  }

  const lineCount = rows.length;
  const distinctSkus = seenSku.size;

  console.log(`Export (NO aggregation): ${lineCount} lines, distinct SKUs ${distinctSkus}, total qty ${totalQty}`);

  const header = ['mpn', 'qty', 'vendor'];
  const records = rows.map(r => [r.mpn, r.qty, r.vendor]);
  const csv = stringify(records, { header: true, columns: header });
  const outFile = path.join('out', 'verified_inventory.csv');
  await fs.promises.writeFile(outFile, csv, 'utf8');

  return { outFile, lineCount, distinctSkus, totalQty };
}

// ---------- Upload to IC Source FTP ----------
async function uploadToIcSource(localFile) {
  // Server / login
  const client = new ftp.Client(0);
  client.ftp.verbose = false;

  await client.access({
    host: 'ftp.icsource.com',
    user: ICSOURCE_FTP_USER,
    password: ICSOURCE_FTP_PASS,
    secure: FTPS ? 'explicit' : false
  });

  const pwd = await client.pwd();
  console.log(`Connected. Server PWD: ${pwd}`);

  // Before-listing
  console.log('Remote listing BEFORE upload:');
  let before;
  try {
    before = await client.list();
    for (const f of before) {
      if (f.name === 'verified_inventory.csv') {
        console.log(` • ${f.name}  ${f.size}`);
      }
    }
  } catch { /* ignore */ }

  // Upload (overwrite)
  await client.uploadFrom(localFile, 'verified_inventory.csv');

  // After-listing
  console.log('Remote listing AFTER upload:');
  const after = await client.list();
  let remoteSize = 0;
  for (const f of after) {
    if (f.name === 'verified_inventory.csv') {
      remoteSize = Number(f.size || 0);
      console.log(`✅ Upload confirmed on server: verified_inventory.csv (${remoteSize} bytes)`);
      break;
    }
  }

  await client.close();
  return { remoteSize, remoteDir: pwd || '/' };
}

// ---------- Log to Supabase ----------
async function insertLog({
  run_id,
  run_url,
  dry_run = false,
  aggregate = false,
  line_count,
  distinct_skus,
  total_qty,
  file_size,
  dir = '/',
  ftps = FTPS,
  status = 'ERR',
  note = ''
}) {
  const { data, error } = await supabase.from('ic_source_uploads').insert([{
    run_id, run_url,
    dry_run, aggregate,
    line_count, distinct_skus, total_qty,
    file_size, dir, ftps,
    status, note
  }]).select('id, created_at').single();

  if (error) throw new Error(`Supabase log insert failed: ${error.message}`);
  console.log(`Log saved (id=${data.id}, at=${data.created_at})`);
  return data;
}

// ---------- Main ----------
(async () => {
  let status = 'ERR';
  let note = '';

  try {
    // 1) Export
    const { outFile, lineCount, distinctSkus, totalQty } = await exportCsvNoAggregation();

    // 2) Upload + confirm
    const { remoteSize, remoteDir } = await uploadToIcSource(outFile);

    // 3) Decide final status based on remote confirmation
    if (remoteSize > 0) {
      status = 'OK';
      note = 'OK';
    } else {
      status = 'ERR';
      note = 'Remote file size 0';
    }

    // 4) Write log
    console.log('Writing log to Supabase…');
    await insertLog({
      run_id: GITHUB_RUN_ID || null,
      run_url: runUrl(),
      dry_run: false,
      aggregate: false,
      line_count: lineCount,
      distinct_skus: distinctSkus,
      total_qty: totalQty,
      file_size: remoteSize,
      dir: remoteDir || '/',
      ftps: FTPS,
      status,
      note
    });

  } catch (err) {
    status = 'ERR';
    note = (err && err.message) ? err.message.slice(0, 200) : 'Unknown error';
    console.error('ERROR:', err?.stack || err);

    // try to record the failure anyway
    try {
      await insertLog({
        run_id: GITHUB_RUN_ID || null,
        run_url: runUrl(),
        dry_run: false,
        aggregate: false,
        line_count: 0,
        distinct_skus: 0,
        total_qty: 0,
        file_size: 0,
        dir: '/',
        ftps: FTPS,
        status,
        note
      });
    } catch (e2) {
      console.error('Failed to write error log:', e2?.message || e2);
    }
    process.exitCode = 1;
    return;
  }

  console.log('Done.');
})();









