// scripts/export-and-upload-icsource.mjs
// ESM module

import { createClient } from '@supabase/supabase-js';
import { Client as FtpClient } from 'basic-ftp';
import fs from 'node:fs/promises';
import { createWriteStream } from 'node:fs';
import path from 'node:path';
import { stringify } from 'csv-stringify/sync';

const DRY_RUN =
  String(process.env.DRY_RUN ?? process.env.INPUT_DRY_RUN ?? 'false').toLowerCase() === 'true';

const SUPABASE_URL =
  process.env.SUPABASE_URL || 'https://ijzroisggstqkfhpjndq.supabase.co';
const SUPABASE_KEY =
  process.env.SUPABASE_ANON_KEY ||
  process.env.SUPABASE_KEY ||
  // fallback anon key for testing (remove later)
  'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImlqenJvaXNnZ3N0cWtmaHBqbmRxIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTQ0NzYzMjEsImV4cCI6MjA3MDA1MjMyMX0.ZqI2EiNNROR3Y7MflOeHlAY49N7b89oHA_VcEHVEwjc';

const TABLE = process.env.SUPABASE_EXCESS_TABLE || 'excess_parts';
const SCHEMA = process.env.SUPABASE_EXCESS_SCHEMA || 'public';

// FTP env
const FTP_HOST = process.env.ICSOURCE_FTP_HOST || 'ftp.icsource.com';
const FTP_USER = process.env.ICSOURCE_FTP_USER || '';
const FTP_PASS = process.env.ICSOURCE_FTP_PASS || '';
const FTP_SECURE = String(process.env.ICSOURCE_FTPS || 'false').toLowerCase() === 'true';
const REMOTE_DIR = process.env.ICSOURCE_DIR || '/';

const RUN_ID = process.env.GITHUB_RUN_ID || `${Date.now()}`;
const RUN_URL =
  process.env.GITHUB_REPOSITORY && RUN_ID
    ? `https://github.com/${process.env.GITHUB_REPOSITORY}/actions/runs/${RUN_ID}`
    : null;

const OUT_DIR = 'out';
const OUT_FILE = path.join(OUT_DIR, 'verified_inventory.csv');

// ---- Helpers --------------------------------------------------------------

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function fmt(num) {
  return num.toLocaleString('en-GB');
}

function nowIso() {
  return new Date().toISOString();
}

// ---- Main ---------------------------------------------------------------

(async () => {
  await fs.mkdir(OUT_DIR, { recursive: true });
  console.log('Run mkdir -p out');

  // Supabase client
  const supa = createClient(SUPABASE_URL, SUPABASE_KEY);

  // Export data (paged by id)
  const LIMIT = 1000;
  let lastId = null;
  let total = 0;
  const rows = [];
  const skuSet = new Set();
  let totalQty = 0;

  console.log(`Dry run: ${DRY_RUN}`);
  console.log('Aggregation: OFF (each row exported)');
  console.log('Cursor paging on column: id (page=1000)');

  while (true) {
    let q = supa
      .from(`${SCHEMA}.${TABLE}`)
      .select('id,part_number,quantity,vendor,created_on_utc', { head: false })
      .order('id', { ascending: true })
      .limit(LIMIT);

    if (lastId != null) q = q.gt('id', lastId);

    const { data, error } = await q;
    if (error) throw new Error(`Supabase fetch error: ${error.message}`);

    if (!data || data.length === 0) break;

    for (const r of data) {
      // Push raw row, no aggregation
      rows.push({
        part_number: r.part_number ?? '',
        quantity: Number(r.quantity ?? 0),
        vendor: r.vendor ?? '',
        created_on_utc: r.created_on_utc ?? null
      });
      skuSet.add(r.part_number ?? '');
      totalQty += Number(r.quantity ?? 0);
    }

    total += data.length;
    lastId = data[data.length - 1].id;

    console.log(`Fetched ${fmt(total)}/? rows so far…`);
    // small yield
    await sleep(10);
  }

  // Prepare CSV in-memory (fast)
  const csv = stringify(rows, {
    header: true,
    columns: [
      { key: 'part_number', header: 'part_number' },
      { key: 'quantity', header: 'quantity' },
      { key: 'vendor', header: 'vendor' },
      { key: 'created_on_utc', header: 'created_on_utc' }
    ]
  });

  await fs.writeFile(OUT_FILE, csv);
  const stats = await fs.stat(OUT_FILE);

  const lineCount = rows.length;
  const distinctSkus = skuSet.size;

  console.log(
    `Export (NO aggregation): ${fmt(lineCount)} lines, distinct SKUs ${fmt(
      distinctSkus
    )}, total qty ${fmt(totalQty)}`
  );

  let ftpOk = false;
  let ftpNote = '';

  if (!DRY_RUN) {
    console.log(`Connecting to ${FTP_HOST} (secure=${FTP_SECURE ? 'FTPS' : 'FTP'})…`);
    const client = new FtpClient();
    try {
      await client.access({
        host: FTP_HOST,
        user: FTP_USER,
        password: FTP_PASS,
        secure: FTP_SECURE,
        secureOptions: undefined
      });

      const pwd = await client.pwd();
      console.log(`Connected. Server PWD: ${pwd}`);

      // list before
      console.log('Remote listing BEFORE upload:');
      const list1 = await client.list(REMOTE_DIR);
      for (const f of list1) {
        if (f.isFile) console.log(` • ${f.name.padEnd(25)} ${f.size}`);
      }

      // upload
      const remotePath = path.posix.join(REMOTE_DIR, 'verified_inventory.csv');
      console.log(`Uploading: verified_inventory.csv  ->  ${path.posix.basename(remotePath)}`);
      await client.uploadFrom(OUT_FILE, remotePath);

      // list after
      console.log('Remote listing AFTER upload:');
      const list2 = await client.list(REMOTE_DIR);
      const uploaded = list2.find((f) => f.name === 'verified_inventory.csv');
      if (uploaded) {
        console.log(
          `✅ Upload confirmed on server: verified_inventory.csv (${uploaded.size} bytes)`
        );
        ftpOk = true;
        ftpNote = 'OK';
      } else {
        ftpOk = false;
        ftpNote = 'Not found after upload';
      }
      await client.close();
    } catch (e) {
      ftpOk = false;
      ftpNote = e?.message || 'FTP error';
      try {
        await client.close();
      } catch {}
    }
  } else {
    ftpOk = true; // treat dry run as OK for logging purposes
    ftpNote = 'DRY RUN';
  }

  // Insert a log row
  console.log('Writing log to Supabase…');
  const logPayload = {
    run_id: RUN_ID,
    run_url: RUN_URL,
    dry_run: DRY_RUN,
    aggregate: false,
    line_count: lineCount,
    distinct_skus: distinctSkus,
    total_qty: totalQty,
    file_size: stats.size,
    dir: REMOTE_DIR,
    ftps: FTP_SECURE,
    note: ftpNote
  };

  const { data: ins, error: insErr } = await supa
    .from('ic_source_uploads')
    .insert([logPayload])
    .select('id,created_at')
    .single();

  if (insErr) {
    console.log('Log insert error (non-fatal):', insErr.message);
  } else {
    console.log(`Log saved (id=${ins.id}, at=${ins.created_at})`);
  }

  console.log('Done.');
})().catch((err) => {
  console.error(err);
  process.exit(1);
});











