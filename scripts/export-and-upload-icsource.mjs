// scripts/export-and-upload-icsource.mjs
// Stream export to CSV (no in-memory accumulation), FTP upload, and log to Supabase.
// This version ONLY inserts the columns you already have in ic_source_uploads:
// run_id, run_url, dry_run, aggregate, line_count, distinct_skus, total_qty, file_size

import fs from 'node:fs';
import path from 'node:path';
import { createClient } from '@supabase/supabase-js';
import { stringify } from 'csv-stringify';
import ftp from 'basic-ftp';

// ---------- ENV ----------
const {
  SUPABASE_URL,
  SUPABASE_SERVICE_ROLE,
  SUPABASE_EXCESS_TABLE = 'excess_parts',

  FTP_HOST = 'ftp.icsource.com',
  FTP_USER = '',
  FTP_PASS = '',
  FTP_SECURE = 'false',
  FTP_DIR = '/',
  OUTPUT_FILE = 'verified_inventory.csv',

  DRY_RUN = 'false',
  GITHUB_RUN_URL = '',
  GITHUB_RUN_ID = '',
} = process.env;

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE) {
  console.error('Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE.');
  process.exit(1);
}

const isDryRun = String(DRY_RUN).toLowerCase() === 'true';
const useFTPS  = String(FTP_SECURE).toLowerCase() === 'true';

console.log(`Dry run: ${isDryRun}`);
console.log('Aggregation: OFF (each row exported)');
console.log(`Cursor paging on column: id (page=1000)`);

// ---------- Supabase ----------
const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE, {
  auth: { persistSession: false },
});

function adjustQty80(q) {
  const v = Math.round(Number(q || 0) * 0.8);
  return v > 0 ? v : 0;
}

async function writeLogMinimal({ lineCount, distinctSkus, totalQty, fileSize }) {
  console.log('Writing log to Supabase…');
  const payload = {
    run_id: String(GITHUB_RUN_ID || Date.now()),
    run_url: GITHUB_RUN_URL || '',
    dry_run: isDryRun,
    aggregate: false,
    line_count: lineCount,
    distinct_skus: distinctSkus,
    total_qty: totalQty,
    file_size: fileSize,
  };

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

async function exportToCsvStream(outPath) {
  await fs.promises.mkdir(path.dirname(outPath), { recursive: true });

  const csv = stringify({ header: false });
  const out = fs.createWriteStream(outPath);
  csv.pipe(out);

  let lastId = 0;
  let fetched = 0;

  let lineCount = 0;
  let totalQty = 0;
  const skuSet = new Set();

  while (true) {
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
      const mpn = (row.part_number || '').trim();
      if (!mpn) continue;

      const adj = adjustQty80(row.quantity);
      if (adj <= 0) continue;

      csv.write([mpn, adj]);

      lineCount++;
      totalQty += adj;
      skuSet.add(mpn);
    }

    lastId = data[data.length - 1].id;
    fetched += data.length;
    console.log(`Fetched ${fetched}/? rows so far…`);
  }

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

async function uploadViaFtp(localPath, remoteName) {
  if (isDryRun) {
    console.log('DRY RUN — skipping FTP upload.');
    const stat = await fs.promises.stat(localPath);
    return { ok: true, size: stat.size };
  }

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
    for (const f of before) if (f.isFile) console.log(` • ${f.name}  ${f.size}`);

    await client.ensureDir(FTP_DIR);
    console.log(`Uploading: ${path.basename(localPath)}  ->  ${remoteName}`);
    await client.uploadFrom(localPath, remoteName);

    console.log('Remote listing AFTER upload:');
    const after = await client.list(FTP_DIR);
    const uploaded = after.find(f => f.name === remoteName);
    if (uploaded) {
      console.log(`✅ Upload confirmed on server: ${uploaded.name} (${uploaded.size} bytes)`);
      return { ok: true, size: uploaded.size };
    }
    console.warn('Could not find uploaded file in listing.');
    return { ok: true, size: 0 };
  } catch (err) {
    console.error('FTP error:', err.message || err);
    return { ok: false, size: 0 };
  } finally {
    client.close();
  }
}

(async () => {
  try {
    const outDir = 'out';
    const outPath = path.join(outDir, OUTPUT_FILE);

    const stats = await exportToCsvStream(outPath);
    console.log(
      `Export (NO aggregation): ${stats.lineCount} lines, ` +
      `distinct SKUs ${stats.distinctSkus}, total qty ${stats.totalQty}`
    );

    const up = await uploadViaFtp(outPath, OUTPUT_FILE);
    if (!up.ok) console.warn('Upload reported a problem; proceeding to log anyway.');
    const sizeForLog = up.size || stats.fileSize;

    await writeLogMinimal({
      lineCount: stats.lineCount,
      distinctSkus: stats.distinctSkus,
      totalQty: stats.totalQty,
      fileSize: sizeForLog,
    });

    console.log('Done.');
  } catch (err) {
    console.error('Fatal error:', err.message || err);
    process.exit(1);
  }
})();








