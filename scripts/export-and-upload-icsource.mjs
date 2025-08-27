// scripts/export-and-upload-icsource.mjs
// Build CSV with header: part_number,quantity (NO vendor), apply 80% rule,
// upload to IC Source by FTP, and log run details to Supabase.

import fs from "fs/promises";
import path from "path";
import ftp from "basic-ftp";
import { createClient } from "@supabase/supabase-js";

// ---------- ENV ----------
const SUPABASE_URL  = process.env.SUPABASE_URL;
const SUPABASE_KEY  = process.env.SUPABASE_KEY || process.env.SUPABASE_SERVICE_ROLE;
const FTP_USER      = process.env.ICSOURCE_FTP_USER;
const FTP_PASS      = process.env.ICSOURCE_FTP_PASS;
const DRY_RUN       = (process.env.DRY_RUN || "false").toLowerCase() === "true";

if (!SUPABASE_URL || !SUPABASE_KEY) {
  throw new Error("SUPABASE_URL / SUPABASE_KEY missing");
}
if (!FTP_USER || !FTP_PASS) {
  throw new Error("FTP credentials missing. Set ICSOURCE_FTP_USER and ICSOURCE_FTP_PASS.");
}

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);

// ---------- helpers ----------
const OUT_FILE = "out/verified_inventory.csv";
const REMOTE_NAME = "verified_inventory.csv";

function normPart(p) {
  return (p ?? "").toString().trim().toUpperCase();
}
function toInt(n) {
  const v = Number(n);
  return Number.isFinite(v) ? Math.max(0, Math.round(v)) : 0;
}

// Cursor-paged read from public.excess_parts (id asc)
async function* fetchAllParts(pageSize = 1000) {
  let lastId = null;
  for (;;) {
    let q = supabase
      .from("excess_parts")
      .select("id, part_number, quantity")
      .order("id", { ascending: true })
      .limit(pageSize);
    if (lastId !== null) q = q.gt("id", lastId);

    const { data, error } = await q;
    if (error) throw new Error(`Supabase fetch error: ${error.message}`);
    if (!data || data.length === 0) break;

    yield data;

    lastId = data[data.length - 1].id;
    if (data.length < pageSize) break;
  }
}

// Build CSV with header: part_number,quantity
async function buildCsv(outPath) {
  let lines = 0;
  let totalQty = 0;
  const seen = new Set();
  const rows = [];

  // <-- exact header required by IC Source
  rows.push("part_number,quantity");

  for await (const chunk of fetchAllParts(1000)) {
    for (const r of chunk) {
      const part = normPart(r.part_number);  // read part_number from DB
      if (!part) continue;

      const qRaw = toInt(r.quantity);
      const qAdj = Math.round(qRaw * 0.8);   // 80% rule
      rows.push(`${part},${qAdj}`);

      lines += 1;
      totalQty += qAdj;
      seen.add(part);
    }
    console.log(`Fetched ${lines}/? rows so far…`);
  }

  await fs.mkdir(path.dirname(outPath), { recursive: true });
  const content = rows.join("\n");
  await fs.writeFile(outPath, content, "utf8");

  return {
    lineCount: lines,
    distinctSkus: seen.size,
    totalQty,
    bytes: Buffer.byteLength(content, "utf8"),
  };
}

// FTP upload + confirm
async function uploadToIcSource(localFile, remoteName) {
  const client = new ftp.Client(20 * 1000);
  client.ftp.verbose = false;

  try {
    console.log("Connecting to ftp.icsource.com (secure=FTP)…");
    await client.access({
      host: "ftp.icsource.com",
      user: FTP_USER,
      password: FTP_PASS,
      secure: false, // IC Source uses plain FTP
    });

    const before = await client.list();
    const prev = before.find(f => f.name === remoteName);
    if (prev) console.log(` • ${remoteName}  ${prev.size}`);

    await client.uploadFrom(localFile, remoteName);

    const after = await client.list();
    const now = after.find(f => f.name === remoteName);
    if (!now) throw new Error("Upload not visible after STOR.");
    console.log(`✅ Upload confirmed on server: ${remoteName} (${now.size} bytes)`);

    return { remoteDir: "/", remoteName, size: now.size, ftps: false, success: true, message: "OK" };
  } finally {
    client.close();
  }
}

// Insert log into ic_source_uploads (rich → minimal fallback)
async function insertLog(payload) {
  try {
    const { error } = await supabase.from("ic_source_uploads").insert(payload);
    if (error) throw error;
    return;
  } catch (e1) {
    try {
      const minimal = {
        total_qty: payload.total_qty,
        file_size: payload.file_size,
        remote_name: payload.remote_name,
        remote_dir: payload.remote_dir,
        ftps: payload.ftps,
        success: payload.success,
        message: payload.message,
      };
      const { error } = await supabase.from("ic_source_uploads").insert(minimal);
      if (error) throw error;
    } catch (e2) {
      console.warn("⚠️ Log insert failed:", e2.message);
    }
  }
}

// ---------- main ----------
(async () => {
  console.log(`Dry run: ${DRY_RUN}`);
  console.log("Aggregation: OFF (each row exported)");
  await fs.mkdir(path.dirname(OUT_FILE), { recursive: true });

  const stats = await buildCsv(OUT_FILE);
  console.log(
    `Export (NO aggregation): ${stats.lineCount} lines, distinct SKUs ${stats.distinctSkus}, total qty ${stats.totalQty}`
  );

  let ftpResult = {
    remoteDir: "/",
    remoteName: REMOTE_NAME,
    size: stats.bytes,
    ftps: false,
    success: true,
    message: "DRY",
  };

  if (!DRY_RUN) {
    ftpResult = await uploadToIcSource(OUT_FILE, REMOTE_NAME);
  }

  // Link back to the job if running in Actions
  const runUrl =
    process.env.GITHUB_SERVER_URL && process.env.GITHUB_REPOSITORY && process.env.GITHUB_RUN_ID
      ? `${process.env.GITHUB_SERVER_URL}/${process.env.GITHUB_REPOSITORY}/actions/runs/${process.env.GITHUB_RUN_ID}`
      : null;

  // Log (best-effort)
  await insertLog({
    run_url: runUrl,
    dry_run: DRY_RUN,
    aggregated: false,
    line_count: stats.lineCount,
    distinct_skus: stats.distinctSkus,
    total_qty: stats.totalQty,
    file_size: ftpResult.size || stats.bytes,
    remote_name: ftpResult.remoteName,
    remote_dir: ftpResult.remoteDir,
    ftps: ftpResult.ftps,
    success: ftpResult.success,
    message: ftpResult.message,
  });

  console.log("Done.");
})().catch((e) => {
  console.error(e);
  process.exit(1);
});

















