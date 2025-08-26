// scripts/export-and-upload-icsource.mjs
// ------------------------------------------------------
// Export excess inventory from Supabase -> CSV -> upload to IC Source (FTP)
// and write a log row into Supabase (ic_source_uploads).
// ------------------------------------------------------

import fs from 'node:fs';
import path from 'node:path';
import process from 'node:process';
import { createClient } from '@supabase/supabase-js';
import ftp from 'basic-ftp';
import { stringify } from 'csv-stringify';

// ---------- helpers ----------
const bool = (v, d = false) => {
  if (v === undefined || v === null || v === '') return d;
  const s = String(v).trim().toLowerCase();
  return ['1', 'true', 'yes', 'y', 'on'].includes(s);
};

const toNumber = (v, d = 0) => {
  const n = Number(v);
  return Number.isFinite(n) ? n : d;
};

// Build a GitHub run URL if running inside Actions
function githubRunURL() {
  const repo = process.env.GITHUB_REPOSITORY;
  const runId = process.env.GITHUB_RUN_ID;
  const server = process.env.GITHUB_SERVER_URL || 'https://github.com';
  if (repo && runId) return `${server}/${repo}/actions/runs/${runId}`;
  return null;
}

// ---------- configuration from env ----------
const SUPABASE_URL  = process.env.SUPABASE_URL;
const SUPABASE_KEY  = process.env.SUPABASE_KEY || process.env.SUPABASE_SERVICE_ROLE; // either works

// tables (accept "excess_parts" OR "public.excess_parts")
const RAW_EXCESS_TABLE = process.env.SUPABASE_EXCESS_TABLE || 'excess_parts';
let EXCESS_SCHEMA = 'public';
let EXCESS_TABLE  = RAW_EXCESS_TABLE;
if (RAW_EXCESS_TABLE.includes('.')) {
  const [s, ...rest] = RAW_EXCESS_TABLE.split('.');
  EXCESS_SCHEMA = s || 'public';
  EXCESS_TABLE  = rest.join('.') || 'excess_parts';
}

const RAW_LOGS_TABLE = process.env.SUPABASE_LOGS_TABLE || 'ic_source_uploads';
let LOGS_SCHEMA = 'public';
let LOGS_TABLE  = RAW_LOGS_TABLE;
if (RAW_LOGS_TABLE.includes('.')) {
  const [s, ...rest] = RAW_LOGS_TABLE.split('.');
  LOGS_SCHEMA = s || 'public';
  LOGS_TABLE  = rest.join('.') || 'ic_source_uploads';
}

// pagination
const PAGE_SIZE     = toNumber(process.env.PAGE_SIZE, 1000);
const PAGINATE_COL  = process.env.PAGINATE_COL || 'id';

// 80% qty adjustment (set ADJUST_RATIO=1 to disable)
const ADJUST_RATIO  = toNumber(process.env.ADJUST_RATIO, 0.80);

// dry-run (CSV only) – can be passed by GH Actions input `dry_run`
const DRY_RUN = bool(process.env.INPUT_DRY_RUN ?? process.env.DRY_RUN, false);

// IC Source FTP
const FTP_HOST = process.env.ICSOURCE_FTP_HOST || 'ftp.icsource.com';
const FTP_USER = process.env.ICSOURCE_FTP_USER;
const FTP_PASS = process.env.ICSOURCE_FTP_PASS;
const FTP_DIR  = process.env.ICSOURCE_FTP_DIR || '/';
const FTP_SECURE = bool(process.env.ICSOURCE_FTPS, false); // false in your current setup

// output file
const OUT_DIR  = 'out';
const OUT_FILE = path.join(OUT_DIR, 'verified_inventory.csv');

// ---------- sanity checks ----------
if (!SUPABASE_URL || !SUPABASE_KEY) {
  console.error('Supabase URL or KEY missing. Set SUPABASE_URL and SUPABASE_KEY (or SUPABASE_SERVICE_ROLE).');
  process.exit(1);
}

if (!DRY_RUN && (!FTP_USER || !FTP_PASS)) {
  console.error('FTP credentials missing. Set ICSOURCE_FTP_USER and ICSOURCE_FTP_PASS.');
  process.exit(1);
}

fs.mkdirSync(OUT_DIR, { recursive: true });

// ---------- supabase client ----------
const sb = createClient(SUPABASE_URL, SUPABASE_KEY, {
  auth: { persistSession: false }
});

// small helper to pick schema
const fromParts = (client, schema, table) =>
  (schema ? client.schema(schema).from(table) : client.from(table));

// ---------- export -> CSV ----------
async function exportToCSV() {
  console.log(`Dry run: ${DRY_RUN ? 'true' : 'false'}`);
  console.log(`Aggregation: OFF (each row exported)`);
  console.log(`Cursor paging on column: ${PAGINATE_COL} (page=${PAGE_SIZE})`);

  let lastId = null;
  let totalLines = 0;
  let totalQty   = 0;
  const distinct = new Set();

  // stream CSV
  const ws = fs.createWriteStream(OUT_FILE, { encoding: 'utf8' });
  const csv = stringify({
    header: true,
    columns: ['part_number', 'quantity', 'vendor'],
  });
  csv.pipe(ws);

  while (true) {
    let q = fromParts(sb, EXCESS_SCHEMA, EXCESS_TABLE)
      .select(`id, part_number, quantity, vendor`)
      .order(PAGINATE_COL, { ascending: true })
      .limit(PAGE_SIZE);

    if (lastId != null) {
      q = q.gt(PAGINATE_COL, lastId);
    }

    const { data, error } = await q;
    if (error) throw new Error(`Supabase fetch error: ${error.message}`);

    if (!data || data.length === 0) break;

    for (const row of data) {
      lastId = row[PAGINATE_COL];

      const mpn    = String(row.part_number ?? '').trim();
      const vendor = String(row.vendor ?? '').trim();
      const rawQty = toNumber(row.quantity, 0);
      const adjQty = Math.round(rawQty * ADJUST_RATIO);

      if (!mpn || adjQty <= 0) continue;

      csv.write([mpn, adjQty, vendor]);
      totalLines += 1;
      totalQty   += adjQty;
      distinct.add(mpn);
    }

    console.log(`Fetched ${lastId ?? '?'} rows so far…`);
  }

  csv.end();
  await new Promise((r) => ws.on('finish', r));

  const stats = fs.statSync(OUT_FILE);
  const result = {
    lines: totalLines,
    distinct: distinct.size,
    totalQty,
    fileSize: stats.size,
  };

  console.log(`Export (NO aggregation): ${result.lines} lines, distinct SKUs ${result.distinct}, total qty ${result.totalQty}`);
  return result;
}

// ---------- FTP upload ----------
async function uploadToICSource() {
  const client = new ftp.Client(0);
  client.ftp.verbose = false;
  try {
    console.log(`Connecting to ${FTP_HOST} (secure=${FTP_SECURE ? 'FTPS' : 'FTP'})…`);
    await client.access({
      host: FTP_HOST,
      user: FTP_USER,
      password: FTP_PASS,
      secure: FTP_SECURE,
    });

    const pwd = await client.pwd();
    console.log(`Connected. Server PWD: ${pwd}`);

    if (FTP_DIR && FTP_DIR !== '/' && FTP_DIR !== '.') {
      await client.ensureDir(FTP_DIR);
    }

    // pre list
    const before = await client.list();
    console.log('Remote listing BEFORE upload:');
    for (const f of before) {
      if (f.isFile) console.log(` • ${f.name}  ${f.size}`);
    }

    const remoteName = path.basename(OUT_FILE);
    console.log(`Uploading: ${remoteName}  ->  ${remoteName}`);
    await client.uploadFrom(OUT_FILE, remoteName);

    const after = await client.list();
    console.log('Remote listing AFTER upload:');
    let uploadedSize = null;
    for (const f of after) {
      if (f.isFile) {
        console.log(` • ${f.name}  ${f.size}`);
        if (f.name === remoteName) uploadedSize = f.size;
      }
    }

    if (uploadedSize == null) {
      throw new Error('Upload appears to be missing on remote.');
    }

    console.log(`✅ Upload confirmed on server: ${remoteName} (${uploadedSize} bytes)`);
    return { remoteDir: FTP_DIR || '/', remoteName, uploadedSize, ftps: FTP_SECURE };
  } finally {
    client.close();
  }
}

// ---------- Write log row ----------
async function writeLog({ ok, msg, exportStats, ftpInfo }) {
  const runUrl = githubRunURL();
  const runId  = process.env.GITHUB_RUN_ID || null;

  const row = {
    run_id: runId,
    run_url: runUrl,
    dry_run: DRY_RUN,
    aggregate: false,
    line_count: exportStats?.lines ?? null,
    distinct_skus: exportStats?.distinct ?? null,
    total_qty: exportStats?.totalQty ?? null,
    file_size: exportStats?.fileSize ?? null,
    remote_name: ftpInfo?.remoteName ?? null,
    remote_dir: ftpInfo?.remoteDir ?? null,
    ftps: ftpInfo?.ftps ?? false,
    success: ok,
    message: msg || (ok ? 'OK' : 'ERR'),
  };

  const { error } = await fromParts(sb, LOGS_SCHEMA, LOGS_TABLE).insert(row).select('id, created_at').single();
  if (error) throw new Error(`log insert failed: ${error.message}`);
}

// ---------- main ----------
(async () => {
  try {
    // 1) Export CSV
    const exportStats = await exportToCSV();

    // 2) Upload (unless dry run)
    let ftpInfo = null;
    if (!DRY_RUN) {
      ftpInfo = await uploadToICSource();
    } else {
      console.log('Dry run: skipping FTP upload.');
    }

    // 3) Log success
    console.log('Writing log to Supabase…');
    await writeLog({ ok: true, msg: 'OK', exportStats, ftpInfo });
    console.log('Log saved (ok).');
    console.log('Done.');
    process.exit(0);
  } catch (err) {
    console.error(err?.stack || err?.message || String(err));
    // try to log error row
    try {
      await writeLog({ ok: false, msg: String(err?.message || err), exportStats: null, ftpInfo: null });
      console.log('Error row written.');
    } catch (e2) {
      console.error('Failed to write error log:', e2?.message || e2);
    }
    process.exit(1);
  }
})();












