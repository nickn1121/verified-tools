// scripts/export-and-upload-icsource.mjs
// 1) Read from Supabase (paged)
// 2) build CSV with 80% qty
// 3) (optional) upload to IC Source FTP
// 4) insert log row into public.ic_source_uploads

import { createClient } from "@supabase/supabase-js";
import { stringify } from "csv-stringify/sync";
import fs from "node:fs";
import path from "node:path";
import ftp from "basic-ftp";

const env = (k, d = undefined) => (process.env[k] ?? d);

const SUPABASE_URL  = env("SUPABASE_URL");
const SUPABASE_KEY  = env("SUPABASE_KEY");
const EXCESS_TABLE  = (env("SUPABASE_EXCESS_TABLE","excess_parts")).replace(/^public\./,"");
const LOGS_TABLE    = (env("SUPABASE_LOGS_TABLE","ic_source_uploads")).replace(/^public\./,"");

const PAGE_SIZE     = parseInt(env("PAGE_SIZE","1000"),10);
const PAGINATE_COL  = env("PAGINATE_COL","id");
const ADJUST_RATIO  = parseFloat(env("ADJUST_RATIO","0.8"));
const DRY_RUN       = String(env("DRY_RUN","false")).toLowerCase()==="true";

const FTP_HOST = env("ICSOURCE_FTP_HOST","ftp.icsource.com");
const FTP_USER = env("ICSOURCE_FTP_USER");
const FTP_PASS = env("ICSOURCE_FTP_PASS");
const FTP_DIR  = env("ICSOURCE_FTP_DIR","/");
const FTP_SSL  = String(env("ICSOURCE_FTPS","false")).toLowerCase()==="true";

if (!SUPABASE_URL || !SUPABASE_KEY) {
  throw new Error("SUPABASE_URL / SUPABASE_KEY missing");
}

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);

const OUT_DIR  = "out";
const OUT_FILE = path.join(OUT_DIR, "verified_inventory.csv");
fs.mkdirSync(OUT_DIR, { recursive: true });

function roundInt(n){ return Math.max(0, Math.round(n)); }

async function* fetchPaged() {
  // cursor paging on PAGINATE_COL
  let last = null;
  while (true) {
    let q = supabase.from(EXCESS_TABLE).select("*").order(PAGINATE_COL, { ascending: true }).limit(PAGE_SIZE);
    if (last !== null) q = q.gt(PAGINATE_COL, last);
    const { data, error } = await q;
    if (error) throw new Error(`Supabase fetch error: ${error.message}`);
    if (!data || data.length===0) break;
    last = data[data.length-1][PAGINATE_COL];
    yield data;
  }
}

function rowToCsv(r){
  const qty = Math.max(0, r.quantity ?? 0);
  const adj = roundInt(qty * ADJUST_RATIO);
  return {
    part_number: r.part_number ?? "",
    vendor: r.vendor ?? "",
    quantity: adj,
  };
}

(async () => {
  console.log("Dry run:", DRY_RUN);
  console.log("Aggregation: OFF (each row exported)");
  console.log(`Cursor paging on column: ${PAGINATE_COL} (page=${PAGE_SIZE})`);

  let totalLines=0, totalQty=0, parts=[];
  for await (const page of fetchPaged()) {
    totalLines += page.length;
    if (totalLines % 1000 === 0) console.log(`Fetched ${totalLines}/? rows so far…`);
    for (const r of page) {
      const csvRow = rowToCsv(r);
      parts.push(csvRow);
      totalQty += csvRow.quantity;
    }
  }

  // Write CSV
  const csv = stringify(parts, { header:true, columns:["part_number","vendor","quantity"] });
  fs.writeFileSync(OUT_FILE, csv, "utf8");
  const csvBytes = fs.statSync(OUT_FILE).size;

  console.log(`Export (NO aggregation): ${parts.length} lines, distinct SKUs ${new Set(parts.map(x=>x.part_number)).size}, total qty ${totalQty}`);

  // Upload to IC Source
  let remoteName = "verified_inventory.csv";
  let remoteDir = FTP_DIR;
  let ok = true, message="OK";

  if (!DRY_RUN) {
    if (!FTP_USER || !FTP_PASS) throw new Error("FTP credentials missing");
    const client = new ftp.Client();
    client.ftp.verbose = false;
    try {
      await client.access({ host: FTP_HOST, user: FTP_USER, password: FTP_PASS, secure: FTP_SSL });
      console.log("Connected. Server PWD:", await client.pwd());
      const listBefore = await client.list(remoteDir);
      const existing = listBefore.find(f=>f.name===remoteName);
      if (existing) console.log("Remote listing BEFORE upload:\n •", existing.name, existing.size);

      await client.cd(remoteDir);
      await client.uploadFrom(OUT_FILE, remoteName);

      const listAfter = await client.list(remoteDir);
      const now = listAfter.find(f=>f.name===remoteName);
      if (!now) throw new Error("Upload verification failed (not found)");
      console.log("✅ Upload confirmed on server:", `${remoteName} (${now.size} bytes)`);
      await client.close();
    } catch (e) {
      ok=false; message = e.message || String(e);
      try { await client.close(); } catch {}
    }
  }

  // Log to Supabase
  try {
    const runUrl = process.env.GITHUB_SERVER_URL && process.env.GITHUB_REPOSITORY && process.env.GITHUB_RUN_ID
      ? `${process.env.GITHUB_SERVER_URL}/${process.env.GITHUB_REPOSITORY}/actions/runs/${process.env.GITHUB_RUN_ID}`
      : null;

    const insert = {
      run_id: process.env.GITHUB_RUN_ID || null,
      run_url: runUrl,
      dry_run: DRY_RUN,
      aggregate: false,
      line_count: parts.length,
      distinct_skus: new Set(parts.map(x=>x.part_number)).size,
      total_qty: totalQty,
      file_size: csvBytes,
      remote_name: remoteName,
      remote_dir: remoteDir,
      ftps: FTP_SSL,
      success: ok,
      message
    };

    console.log("Writing log to Supabase…");
    const { data, error } = await supabase.from(LOGS_TABLE).insert(insert).select().single();
    if (error) throw error;
    console.log(`Log saved (id=${data.id}, at=${data.created_at})`);
  } catch (e) {
    console.log(`Log insert returned without id (ok). ${e.message || e}`);
  }

  console.log("Done.");
})().catch(e => {
  console.error(e);
  process.exit(1);
});













