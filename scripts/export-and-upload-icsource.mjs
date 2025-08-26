// scripts/export-and-upload-icsource.mjs
// Stream export to CSV (no in-memory accumulation), FTP upload, and log to Supabase.

import fs from 'node:fs';
import path from 'node:path';
import { createClient } from '@supabase/supabase-js';
import { stringify } from 'csv-stringify';
import ftp from 'basic-ftp';

// ---------- ENV ----------
const {
  // Supabase (service role is recommended for Actions)
  SUPABASE_URL,
  SUPABASE_SERVICE_ROLE,

  // Source table for excess parts
  SUPABASE_EXCESS_TABLE = 'excess_parts', // must have: id (int), part_number (text), quantity (int)

  // FTP target
  FTP_HOST = 'ftp.icsource.com',
  FTP_USER = '',
  FTP_PASS = '',
  FTP_SECURE = 'false',                // "true" => FTPS, "false" => plain FTP
  FTP_DIR = '/',                       // remote folder
  OUTPUT_FILE = 'verified_inventory.csv', // remote filename and local filename

  // Run flags & metadata
  DRY_RUN = 'false',                   // "true" => skip FTP upload
  GITHUB_RUN_URL = '',
  GITHUB_RUN_ID = '',
} = process.env;

// ---------- Checks ----------
if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE) {
  console.error('Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE.');
  process.exit(1);
}

if (!FTP_HOST || !FTP_USER || !FTP_PASS) {
  console.warn('FTP credentials not fully set. Upload will fail unless DRY_RUN=true.');
}

const isDryRun = String(DRY_RUN).toLowerCase() === 'true';
const useFTPS  = String(FTP_SECURE).toLowerCase() === 'true';

console.log(`Dry run: ${isDryRun}`);
console.log('Aggregation: OFF (each row exported)');
console.log(`Cursor paging on column: id (page=1000)`);

// ---------- Supabase client ----------
const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE, {
  auth: { persistSession: false },
});

// ---------- Helpers ----------
function adjustQty80(q) {
  // 80% rounded to nearest whole, never below zero
  const v = Math.round(Number(q || 0) * 0.8);
  return v > 0 ? v : 0;
}

async function writeLog(payload) {
  console.log('Writing log to Supabase…');
  const { data, error } = await supabase
    .from('ic_source_uploads')
    .insert(payload)
    .select('id, created_at')
    .single();

  if (error) {
    console.error('Supabase log insert error:', error.message, error.details || '');
    throw error;
  }
  console.log(`Log saved (id=${data.id}, at=${data.created_at})`);
}

// ---------- Export & CSV streaming ----------
async function exportToCsvStream(outPath) {
  await fs.promises.mkdir(path.dirname(outPath), { recursive: true });

  const csv = stringify({ header: false }); // no header
  const out = fs.createWriteStream(outPath);
  csv.pipe(out);

  let lastId = 0;
  let fetched = 0;

  let lineCount = 0;
  let totalQty = 0;
  const skuSet = new Set();

  while (true) {
    // page of 1000
    const { data, error } = await supabase
      .from(SUPABASE_EXCESS_TABLE)
      .select('id, part_number, quantity')
      .gt('id', lastId)
      .order('id', { ascending: true })
      .limit(1000);

    if (error) {
      console.error('Supabase fetch error:', error.message);
      throw error;
    }
    if (!data || data.length === 0) break;

    for (const row of data) {
      const mpnRaw = (row.part_number || '').trim();
      if (!mpnRaw) continue;

      const adj = adjustQty80(row.quantity);
      if (adj <= 0) continue;

      // write one CSV line: mpn,qty
      csv.write([mpnRaw, adj]);

      lineCount++;
      totalQty += adj;
      skuSet.add(mpnRaw);
    }

    lastId = data[data.length - 1].id;
    fetched += data.length;
    console.log(`Fetched ${fetched}/? rows so far…`);
  }

  // finalize stream
  await new Promise((resolve, reject) => {
    out.on('finish', resolve);
    out.on('error', reject);
    csv.on('error', reject);
    csv.end();
  });

  const stat = await fs.promises.stat(outPath);

  return {
    lineCount,
    distinctSkus: skuSet.size,
    totalQty,
    fileSize: stat.size,
  };
}

// ---------- FTP upload ----------
async function uploadViaFtp(localPath, remoteName) {
  const client = new ftp.Client();
  client.ftp.verbose = false;

  try {
    console.log(`Connecting to ${FTP_HOST} (secure=${useFTPS ? 'FTPS' : 'FTP'})…`);
    await client.access({
      host: FTP_HOST,
      user: FTP_USER,
      password: FTP_PASS,
      secure: useFTPS,
    });

    const pwd = await client.pwd();
    console.log(`Connected. Server PWD: ${pwd}`);

    console.log('Remote listing BEFORE upload:');
    const before = await client.list(FTP_DIR);
    for (const f of before) {
      if (f.isFile) console.log(` • ${f.name}  ${f.size}`);
    }

    await client.ensureDir(FTP_DIR);
    console.log(`Uploading: ${path.basename(localPath)}  ->  ${remoteName}`);
    await client.uploadFrom(localPath, remoteName);

    console.log('Remote listing AFTER upload:');
    const after = await client.list(FTP_DIR);
    let uploaded = after.find(f => f.name === remoteName);
    if (uploaded) {
      console.log(`✅ Upload confirmed on server: ${uploaded.name} (${uploaded.size} bytes)`);
      return { ok: true, size: uploaded.size, note: 'OK' };
    } else {
      console.warn('Could not find uploaded file in listing (still might be there).');
      return { ok: true, size: 0, note: 'Uploaded (size unknown)' };
    }
  } catch (err) {
    console.error('FTP error:', err.message || err);
    return { ok: false, size: 0, note: `FTP error: ${err.message || err}` };
  } finally {
    client.close();
  }
}

// ---------- Main ----------
(async () => {
  try {
    const outDir = 'out';
    const outPath = path.join(outDir, OUTPUT_FILE);

    // 1) Export to CSV (streaming)
    const { lineCount, distinctSkus, totalQty, fileSize } = await exportToCsvStream(outPath);
    console.log(
      `Export (NO aggregation): ${lineCount} lines, ` +
      `distinct SKUs ${distinctSkus}, total qty ${totalQty}`
    );

    // 2) FTP upload (unless dry run)
    let ftpNote = 'OK';
    let uploadedSize = 0;

    if (!isDryRun) {
      const result = await uploadViaFtp(outPath, OUTPUT_FILE);
      ftpNote = result.note || (result.ok ? 'OK' : 'FAILED');
      uploadedSize = result.size || 0;
    } else {
      console.log('DRY RUN — skipping FTP upload.');
      ftpNote = 'DRY RUN';
      uploadedSize = fileSize;
    }

    // 3) Log to Supabase (AWAITED — so Logs page updates)
    const payload = {
      run_id: String(GITHUB_RUN_ID || Date.now()),
      run_url: GITHUB_RUN_URL || '',
      dry_run: isDryRun,
      aggregate: false,
      line_count: lineCount,
      distinct_skus: distinctSkus,
      total_qty: totalQty,
      file_size: uploadedSize || fileSize,
      dir: FTP_DIR || '/',
      ftps: useFTPS,
      note: ftpNote || 'OK',
    };

    try {
      await writeLog(payload);
    } catch (e) {
      // keep the job green — logging issues shouldn’t block the upload
      console.warn('Logging failed (non-fatal):', e.message || e);
    }

    console.log('Done.');
  } catch (err) {
    console.error('Fatal error:', err.message || err);
    process.exit(1);
  }
})();







