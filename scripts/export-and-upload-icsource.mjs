// scripts/export-and-upload-icsource.mjs
import fs from "fs";
import os from "os";
import path from "path";
import { createClient } from "@supabase/supabase-js";
import { stringify } from "csv-stringify/sync";
import ftp from "basic-ftp";

/* ---------- Config from env ---------- */
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE = process.env.SUPABASE_SERVICE_ROLE;
const EXCESS_TABLE = process.env.SUPABASE_EXCESS_TABLE || "excess_parts";

const FTP_HOST   = process.env.ICSOURCE_FTP_HOST || "ftp.icsource.com";
const FTP_USER   = process.env.ICSOURCE_FTP_USER;
const FTP_PASS   = process.env.ICSOURCE_FTP_PASS;
const FTP_SECURE = /^(1|true|yes)$/i.test(process.env.ICSOURCE_FTPS || "false"); // FTPS explicit if true
const REMOTE_DIR = (process.env.ICSOURCE_REMOTE_DIR || "").trim();               // e.g. "/incoming"
const REMOTE_NAME = (process.env.ICSOURCE_REMOTE_NAME || "verified_inventory.csv").trim();
const FTP_DEBUG = /^(1|true|yes)$/i.test(process.env.FTP_DEBUG || "true");       // verbose logs while diagnosing

const QTY_FACTOR   = Number(process.env.ICSOURCE_QTY_FACTOR || "0.8");           // 80%
const DRY_RUN      = /^(1|true|yes)$/i.test(process.env.DRY_RUN || "false");
const AGGREGATE    = /^(1|true|yes)$/i.test(process.env.ICSOURCE_AGGREGATE || "false"); // << default: NO de-dup

/* ---------- Helpers ---------- */
const info = (...a) => console.log(...a);
function roundQty(q) {
  const n = Number(q) || 0;
  return Math.max(0, Math.round(n * QTY_FACTOR));
}

/* ---------- Fetch data from Supabase in pages (ALL rows) ---------- */
async function fetchAllRaw() {
  const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE, {
    auth: { persistSession: false },
  });

  const page = 5000;
  let from = 0;
  const all = [];

  while (true) {
    const to = from + page - 1;
    const { data, error, count } = await supabase
      .from(EXCESS_TABLE)
      .select("part_number,quantity", { count: "exact" })
      .range(from, to);

    if (error) throw error;
    if (!data || data.length === 0) break;

    all.push(...data);
    info(`Fetched ${all.length}/${count ?? "?"} rows so far…`);

    from += data.length;
    if (data.length < page) break;
  }
  return all;
}

/* ---------- Transform rows ---------- */
function buildRows(raw) {
  // filter usable
  const usable = raw.filter(
    r => r && r.part_number && String(r.part_number).trim() !== "" && Number(r.quantity) > 0
  );

  if (!AGGREGATE) {
    // NO de-dup: keep each row as a line, after 80% + round
    const rows = [];
    let total = 0;
    for (const r of usable) {
      const pn = String(r.part_number).trim();
      const q = roundQty(r.quantity);
      if (q > 0) {
        rows.push({ part_number: pn, quantity: q });
        total += q;
      }
    }
    const distinct = new Set(rows.map(r => r.part_number)).size;
    info(`Export (NO aggregation): ${rows.length} lines, distinct SKUs ${distinct}, total qty ${total}`);
    return rows;
  }

  // Aggregation ON: group by part_number
  const map = new Map();
  for (const r of usable) {
    const pn = String(r.part_number).trim();
    const q = roundQty(r.quantity);
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

/* ---------- Upload via FTP (with diagnostics) ---------- */
async function uploadCsv(localPath) {
  const client = new ftp.Client(FTP_DEBUG ? 0 : 10_000);
  if (FTP_DEBUG) client.ftp.verbose = true;

  try {
    info(`Connecting to ${FTP_HOST} (secure=${FTP_SECURE ? "FTPS" : "FTP"})…`);
    await client.access({ host: FTP_HOST, user: FTP_USER, password: FTP_PASS, secure: FTP_SECURE });

    const pwd = await client.pwd();
    info(`Connected. Server PWD: ${pwd}`);

    if (REMOTE_DIR) {
      info(`Ensuring/changing to remote dir: ${REMOTE_DIR}`);
      await client.ensureDir(REMOTE_DIR);
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
    if (found) {
      info(`✅ Upload confirmed on server: ${REMOTE_NAME} (${found.size ?? "unknown"} bytes)`);
    } else {
      info("⚠️ Uploaded file not found in listing (server may immediately move ingested files).");
    }
  } finally {
    client.close();
  }
}

/* ---------- Main ---------- */
(async () => {
  console.log(`Dry run: ${DRY_RUN}`);
  console.log(`Aggregation: ${AGGREGATE ? "ON (group by part_number)" : "OFF (each row exported)"}`);

  const raw = await fetchAllRaw();
  const rows = buildRows(raw);
  const csvPath = writeCsv(rows);

  if (DRY_RUN) {
    info(`CSV ready at: ${csvPath} (artifact will be attached to this run)`);
    return;
  }
  await uploadCsv(csvPath);
})();


