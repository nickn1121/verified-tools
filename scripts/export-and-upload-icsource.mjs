// scripts/export-and-upload-icsource.mjs
// Export excess to CSV, (optionally) upload to IC Source via FTP, then log to Supabase.

import fs from 'node:fs';
import path from 'node:path';
import { createClient } from '@supabase/supabase-js';
import { stringify } from 'csv-stringify/sync';
import ftp from 'basic-ftp';

const {
  SUPABASE_URL,
  SUPABASE_SERVICE_ROLE,
  SUPABASE_EXCESS_TABLE = 'excess_parts',

  FTP_HOST,
  FTP_USER,
  FTP_PASS,
  FTP_SECURE = 'false',
  FTP_DIR = '/',
  OUTPUT_FILE = 'verified_inventory.csv',

  DRY_RUN = 'false',
  GITHUB_RUN_URL = '',
  GITHUB_RUN_ID = '',
} = process.env;

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE) {
  console.error('Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE env vars.');
  process.exit(1);
}

const isDryRun = String(DRY_RUN).toLowerCase() === 'true';
const outDir = 'out';
const outPath = path.join(outDir, OUTPUT_FILE);

// ---------- Helpers ----------

function sleep(ms) {
  return new Promise((res) => setTimeout(res, ms));
}

async function logWithTimeout(supabase, data, timeoutMs = 4000) {
  // Await the insert but cap it with a timeout so job can't hang on logging
  try {
    const result = await Promise.race([
      supabase.from('ic_source_uploads').insert(data).select('id').single(),
      new Promise((_, rej) => setTimeout(() => rej(new Error('log timeout')), timeoutMs)),
    ]);
    if (result && result.data && result.data.id) {
      console.log(`Log saved (id=${result.data.id})`);
    } else {
      console.log('Log insert returned without id (ok).');
    }
  } catch (e) {
    console.warn('Log insert failed (non-fatal):', e.message || e);
  }
}

async function downloadPage(supabase, lastId, pageSize) {
  // Cursor paging by id to reliably fetch large tables
  const q = supabase
    .from(SUPABASE_EXCESS_TABLE)
    .select('id, part_number, quantity', { count: 'exact' })
    .gt('id', lastId)
    .order('id', { ascending: true })
    .limit(pageSize);

  const { data, error } = await q;
  if (error) throw error;
  return data || [];
}

// ---------- Main ----------

(async () => {
  const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE, {
    auth: { persistSession: false },
  });

  fs.mkdirSync(outDir, { recursive: true });

  console.log(`Dry run: ${isDryRun}`);
  console.log('Aggregation: OFF (each row exported)');
  console.log('Cursor paging on column: id (page=1000)');

  const rows = [];
  const pageSize = 1000;
  let lastId = 0;
  let fetchedTotal = 0;

  // Fetch all rows
  while (true) {
    const page = await downloadPage(supabase, lastId, pageSize);
    if (!page.length) break;
    fetchedTotal += page.length;
    lastId = page[page.length - 1].id;

    if (fetchedTotal % 1000 === 0) {
      console.log(`Fetched ${fetchedTotal}/? rows so far…`);
    }

    for (const r of page) {
      // Send raw quantity (no aggregation, no 80% adjustment here)
      const qty = Number(r.quantity || 0);
      if (!r.part_number || qty <= 0) continue;
      rows.push([String(r.part_number).trim(), String(qty)]);
    }
  }

  const lineCount = rows.length;
  const distinctSkus = new Set(rows.map((r) => r[0])).size;
  const totalQty = rows.reduce((acc, r) => acc + Number(r[1]), 0);

  console.log(`Export (NO aggregation): ${lineCount.toLocaleString()} lines, distinct SKUs ${distinctSkus.toLocaleString()}, total qty ${totalQty.toLocaleString()}`);

  // Write CSV
  const csv = stringify(rows, { header: false });
  fs.writeFileSync(outPath, csv);
  const fileSize = fs.statSync(outPath).size;

  // Upload to FTP unless dry-run
  let ftpNote = 'OK';
  let ftpSecure = String(FTP_SECURE).toLowerCase() === 'true';
  let ftpDir = FTP_DIR || '/';

  if (!isDryRun) {
    console.log(`Connecting to ${FTP_HOST} (secure=${ftpSecure ? 'FTPS' : 'FTP'})…`);
    const client = new ftp.Client();
    client.ftp.verbose = false;

    try {
      await client.access({
        host: FTP_HOST,
        user: FTP_USER,
        password: FTP_PASS,
        secure: ftpSecure,
      });

      // Confirm present working directory
      const pwd = await client.pwd();
      console.log(`Connected. Server PWD: ${pwd}`);

      if (ftpDir && ftpDir !== '/') {
        await client.cd(ftpDir);
      }

      // Debug: list before
      try {
        const listBefore = await client.list();
        console.log('Remote listing BEFORE upload:');
        listBefore.forEach((x) => console.log(` • ${x.name}  ${x.size}`));
      } catch (_) {}

      console.log(`Uploading: ${OUTPUT_FILE}  ->  ${OUTPUT_FILE}`);
      await client.uploadFrom(outPath, OUTPUT_FILE);

      // Debug: list after
      try {
        const listAfter = await client.list();
        const found = listAfter.find((x) => x.name === OUTPUT_FILE);
        if (found) {
          console.log(`✅ Upload confirmed on server: ${OUTPUT_FILE} (${found.size} bytes)`);
        }
      } catch (_) {}

      await client.close();
    } catch (e) {
      ftpNote = `FTP failed: ${e.message || e}`;
      console.warn(ftpNote);
      try { await client.close(); } catch {}
    }
  } else {
    ftpNote = 'Dry run (no FTP upload)';
  }

  // --------- Log to Supabase (AWAITED with timeout) ---------
  const payload = {
    run_id: String(GITHUB_RUN_ID || Date.now()),
    run_url: GITHUB_RUN_URL || '',
    dry_run: isDryRun,
    aggregate: false,
    line_count: lineCount,
    distinct_skus: distinctSkus,
    total_qty: totalQty,
    file_size: fileSize,
    dir: ftpDir,
    ftps: ftpSecure,
    note: ftpNote || 'OK',
  };

  await logWithTimeout(supabase, payload, 5000);

  console.log('Done.');
})().catch((err) => {
  console.error(err);
  process.exit(1);
});






