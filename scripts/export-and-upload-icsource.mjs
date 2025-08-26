// scripts/export-and-upload-icsource.mjs
import fs from "fs";
import path from "path";
import { createClient } from "@supabase/supabase-js";
import { stringify } from "csv-stringify/sync";
import ftp from "basic-ftp";

/* ---------- Config from env ---------- */
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE = process.env.SUPABASE_SERVICE_ROLE;
const EXCESS_TABLE = process.env.SUPABASE_EXCESS_TABLE || "excess_parts";
const SUPABASE_PK = process.env.SUPABASE_PK || "id"; // primary key column for cursor paging
const PAGE = Number(process.env.SUPABASE_PAGE || "1000"); // batch size

// Upload target (IC Source)
const FTP_HOST   = process.env.ICSOURCE_FTP_HOST || "ftp.icsource.com";
const FTP_USER   = process.env.ICSOURCE_FTP_USER;
const FTP_PASS   = process.env.ICSOURCE_FTP_PASS;
const FTP_SECURE = /^(1|true|yes)$/i.test(process.env.ICSOURCE_FTPS || "false"); // FTPS toggle
const REMOTE_DIR = (process.env.ICSOURCE_REMOTE_DIR || "").trim();               // e.g. /incoming
const REMOTE_NAME = (process.env.ICSOURCE_REMOTE_NAME || "verified_inventory.csv").trim();

// Behaviour
const QTY_FACTOR = Number(process.env.ICSOURCE_QTY_FACTOR || "0.8");             // 80%
const DRY_RUN    = /^(1|true|yes)$/i.test(process.env.DRY_RUN || "false");
const AGGREGATE  = /^(1|true|yes)$/i.test(process.env.ICSOURCE_AGGREGATE || "false"); // default OFF

// Diagnostics
const FTP_DEBUG = /^(1|true|yes)$/i.test(process.env.FTP_DEBUG || "true");

// Helpers
const info = (...a) => console.log(...a);
function roundQty(q) {
  const n = Number(q) || 0;
  return Math.max(0, Math.round(n * QTY_FACTOR));
}

/* ---------- Supabase logger ---------- */
async function logToSupabase(payload) {
  try {
    const supa = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE, {
      auth: { persistSession: false }
    });
    await supa.from("ic_source_uploads").insert([payload]);
  } catch (e) {
    console.error("Log insert failed:", e?.message || e);
  }
}

/* ---------- Fetch ALL rows with cursor pagination on SUPABASE_PK ---------- */
async function fetchAllRaw() {
  const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE, {
    auth: { persistSession: false }
  });

  let all = [];
  let last = null;
  let totalCount = null;

  while (true) {
    let q = supabase
      .from(EXCESS_TABLE)
      .select(`${SUPABASE_PK}, part_number, quantity`, { count: "exact" })
      .order(SUPABASE_PK, { ascending: true })
      .limit(PAGE);

    if (last !== null) q = q.gt(SUPABASE_PK, last);

    const { data, error, count } = await q;
    if (error) throw error;
    if (totalCount === null && typeof count === "number") totalCount = count;

    if (!data || data.length === 0) break;

    all.push(...data);
    last = data[data.length - 1][SUPABASE_PK];
    info(`Fetched ${all.length}/${totalCount ?? "?"} rows so far…`);

    if (data.length < PAGE) break;
  }

  return all;
}

/* ---------- Build rows (aggregation toggle) ---------- */
function buildRows(raw) {
  const usable = raw.filter(
    r =>
      r &&
      r.part_number &&
      String(r.part_number).trim() !== "" &&
      Number(r.quantity) > 0
  );

  if (!AGGREGATE) {
    // export every row (no de-dup)
    const rows = [];
    let total = 0;
    for (const r of usable) {
      const pn = String(r.part_number).trim();
      const q  = roundQty(r.quantity);
      if (q > 0) {
        rows.push({ part_number: pn, quantity: q });
        total += q;
      }
    }
    const distinct = new Set(rows.map(r => r.part_number)).size;
    info(`Export (NO aggregation): ${rows.length} lines, distinct SKUs ${distinct}, total qty ${total}`);
    return rows;
  }

  // aggregated (group by part_number)
  const map = new Map();
  for (const r of usable) {
    const pn = String(r.part_number).trim();
    const q  = roundQty(r.quantity);
    if (q > 0) map.set(pn, (map.get(pn) || 0) + q);
  }
  const rows = Array.from(map, ([part_number, quantity]) => ({ part_number, quantity }));
  const total = rows.reduce((s, r) => s + r.quantity, 0);
  info(`Aggregated ${rows.length} SKUs (80% adj). Total qty ${total}`);
  return rows;
}

/* ---------- Write CSV ---------- */
function writeCsv(rows) {
  const csv = stringify(rows, { header: true, columns: ["part_number", "quantity"] });
  const outDir = path.join(process.cwd(), "out");
  fs.mkdirSync(outDir, { recursive: true });
  const tmp = path.join(outDir, "verified_inventory.csv");
  fs.writeFileSync(tmp, csv);
  return tmp;
}

/* ---------- Upload via FTP/FTPS (with diagnostics) ---------- */
async function uploadCsv(localPath) {
  const client = new ftp.Client(FTP_DEBUG ? 0 : 10_000);
  if (FTP_DEBUG) client.ftp.verbose = true;

  try {
    info(`Connecting to ${FTP_HOST} (secure=${FTP_SECURE ? "FTPS" : "FTP"})…`);
    await client.access({ host: FTP_HOST, user: FTP_USER, password: FTP_PASS, secure: FTP_SECURE });

    // Show current working directory (server may change on ensureDir)
    let pwd = await client.pwd();
    info(`Connected. Server PWD: ${pwd}`);

    if (REMOTE_DIR) {
      info(`Ensuring/changing to remote dir: ${REMOTE_DIR}`);
      await client.ensureDir(REMOTE_DIR);
      pwd = await client.pwd();
      info(`Server PWD after cd: ${pwd}`);
    }

    info("Remote listing BEFORE upload:");
    const before = await client.list();
    before.forEach(e => info(` • ${e.name}  ${e.size ?? ""}`));

    info(`Uploading: ${path.basename(localPath)}  ->  ${REMOTE_NAME}`);
    await client.uploadFrom(localPath, REMOTE_NAME);

    info("Remote listing AFTER upload:");
    const after = await client.list();
    after.forEach(e => info(` • ${e.name}  ${e.size ?? ""}`));

    const found = after.find(e => e.name === REMOTE_NAME);
    if (found) info(`✅ Upload confirmed on server: ${REMOTE_NAME} (${found.size ?? "unknown"} bytes)`);
    else info("⚠️ Uploaded file not found in listing (server may immediately move ingested files).");
  } finally {
    client.close();
  }
}

/* ---------- Main ---------- */
(async () => {
  try {
    console.log(`Dry run: ${DRY_RUN}`);
    console.log(`Aggregation: ${AGGREGATE ? "ON (group by part_number)" : "OFF (each row exported)"}`);
    console.log(`Cursor paging on column: ${SUPABASE_PK} (page=${PAGE})`);

    const raw    = await fetchAllRaw();
    const rows   = buildRows(raw);
    const csvPath = writeCsv(rows);

    // Build GitHub run URL for convenience in the log table
    const runUrl =
      process.env.GITHUB_SERVER_URL &&
      process.env.GITHUB_REPOSITORY &&
      process.env.GITHUB_RUN_ID
        ? `${process.env.GITHUB_SERVER_URL}/${process.env.GITHUB_REPOSITORY}/actions/runs/${process.env.GITHUB_RUN_ID}`
        : null;

    const meta = {
      run_id: process.env.GITHUB_RUN_ID || null,
      run_url: runUrl,
      dry_run: DRY_RUN,
      aggregate: AGGREGATE,
      line_count: rows.length,
      distinct_skus: new Set(rows.map(r => r.part_number)).size,
      total_qty: rows.reduce((s, r) => s + r.quantity, 0),
      file_size: fs.statSync(csvPath).size,
      remote_name: REMOTE_NAME,
      remote_dir: REMOTE_DIR || "/",
      ftps: FTP_SECURE
    };

    if (DRY_RUN) {
      await logToSupabase({ ...meta, success: true, message: "Dry run (CSV only)" });
      info(`CSV ready at: ${csvPath}`);
      return;
    }

    await uploadCsv(csvPath);
    await logToSupabase({ ...meta, success: true, message: "OK" });
  } catch (err) {
    console.error("FATAL:", err?.stack || err);
    // Best-effort failure log
    await logToSupabase({
      run_id: process.env.GITHUB_RUN_ID || null,
      run_url:
        process.env.GITHUB_SERVER_URL &&
        process.env.GITHUB_REPOSITORY &&
        process.env.GITHUB_RUN_ID
          ? `${process.env.GITHUB_SERVER_URL}/${process.env.GITHUB_REPOSITORY}/actions/runs/${process.env.GITHUB_RUN_ID}`
          : null,
      success: false,
      message: String(err?.message || err)
    });
    process.exit(1);
  }
})();




