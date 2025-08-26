// scripts/export-and-upload-icsource.mjs
// Purpose: pull excess from Supabase, write CSV (Part Number,Quantity) with 80% qty,
// then upload to IC Source via FTP.
//
// Env vars required (set as GitHub Secrets/Variables):
// - SUPABASE_URL
// - SUPABASE_SERVICE_ROLE
// - SUPABASE_EXCESS_TABLE  (optional; defaults to "excess_parts")
// - ICSOURCE_FTP_HOST      (optional; defaults to "ftp.icsource.com")
// - ICSOURCE_FTP_USER
// - ICSOURCE_FTP_PASS
// - ICSOURCE_FTP_DIR       (optional; defaults to "/")
// - ICSOURCE_FILE_NAME     (optional; defaults to "verified_inventory.csv")
// - DRY_RUN                ("true" to skip FTP upload; useful for tests)

import fs from "node:fs/promises";
import path from "node:path";
import { createClient } from "@supabase/supabase-js";
import ftp from "basic-ftp";

const {
  SUPABASE_URL,
  SUPABASE_SERVICE_ROLE,
  SUPABASE_EXCESS_TABLE = "excess_parts",

  ICSOURCE_FTP_HOST = "ftp.icsource.com",
  ICSOURCE_FTP_USER,
  ICSOURCE_FTP_PASS,
  ICSOURCE_FTP_DIR = "/",
  ICSOURCE_FILE_NAME = "verified_inventory.csv",

  DRY_RUN = "false",
} = process.env;

// Basic validation
if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE) {
  throw new Error("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE env vars.");
}
if (!ICSOURCE_FTP_USER || !ICSOURCE_FTP_PASS) {
  console.warn("Warning: ICSOURCE_FTP_USER / ICSOURCE_FTP_PASS not set. Use DRY_RUN=true to test without upload.");
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE);

// Normalize MPN for grouping (case-insensitive, no spaces)
function normalizeMpn(mpn) {
  if (!mpn) return "";
  return String(mpn)
    .toUpperCase()
    .replace(/\s+/g, "")
    // keep common MPN chars; strip anything weird
    .replace(/[^A-Z0-9._#/+:-]/g, "");
}

// Pull table in pages and aggregate qty by normalized MPN
async function fetchAggregated() {
  const pageSize = 10000;
  let from = 0;
  let aggregated = new Map();

  for (;;) {
    const { data, error, count } = await supabase
      .from(SUPABASE_EXCESS_TABLE)
      .select("*", { count: "exact" })
      .range(from, from + pageSize - 1);

    if (error) throw error;
    if (!data || data.length === 0) break;

    for (const row of data) {
      // Try a bunch of likely column names
      const mpnRaw =
        row.part_number ?? row.PartNumber ?? row.Part_Number ?? row.mpn ?? row.MPN ?? row.PN ?? row.Pn;
      const qtyRaw = row.quantity ?? row.qty ?? row.Qty ?? row.Quantity ?? row.QTY;

      const mpn = normalizeMpn(mpnRaw);
      const qty = Number(qtyRaw ?? 0);

      if (!mpn || !Number.isFinite(qty) || qty <= 0) continue;

      aggregated.set(mpn, (aggregated.get(mpn) ?? 0) + Math.floor(qty));
    }

    from += pageSize;
    if (from >= (count ?? from)) break;
  }

  // Apply 80% rule, round to nearest whole number, never below 1 if there was stock
  return [...aggregated.entries()]
    .map(([mpn, qty]) => {
      let adjusted = Math.round(qty * 0.8);
      if (qty > 0 && adjusted < 1) adjusted = 1;
      return { mpn, qty: adjusted };
    })
    .sort((a, b) => a.mpn.localeCompare(b.mpn));
}

function buildCsv(rows) {
  const header = "Part Number,Quantity";
  const body = rows.map((r) => `${r.mpn},${r.qty}`).join("\n");
  return `${header}\n${body}\n`;
}

async function uploadViaFtp(localPath) {
  const client = new ftp.Client(30_000);
  client.ftp.verbose = false;

  try {
    // Try FTPS first
    await client.access({
      host: ICSOURCE_FTP_HOST,
      user: ICSOURCE_FTP_USER,
      password: ICSOURCE_FTP_PASS,
      secure: true,
    });
  } catch (_) {
    // Fallback to plain FTP if FTPS fails
    await client.access({
      host: ICSOURCE_FTP_HOST,
      user: ICSOURCE_FTP_USER,
      password: ICSOURCE_FTP_PASS,
      secure: false,
    });
  }

  if (ICSOURCE_FTP_DIR && ICSOURCE_FTP_DIR !== "/") {
    await client.ensureDir(ICSOURCE_FTP_DIR);
  }

  const remotePath = path.posix.join(
    ICSOURCE_FTP_DIR === "/" ? "" : ICSOURCE_FTP_DIR,
    ICSOURCE_FILE_NAME
  );

  await client.uploadFrom(localPath, remotePath);
  client.close();
  return remotePath;
}

(async () => {
  console.log(`Reading "${SUPABASE_EXCESS_TABLE}" from Supabase…`);
  const rows = await fetchAggregated();
  console.log(`Aggregated ${rows.length} SKUs (after 80% adjustment).`);

  const outDir = path.resolve("out");
  await fs.mkdir(outDir, { recursive: true });
  const localFile = path.join(outDir, ICSOURCE_FILE_NAME);

  await fs.writeFile(localFile, buildCsv(rows), "utf8");
  console.log(`CSV written: ${localFile}`);

  if (DRY_RUN === "true") {
    console.log("DRY_RUN=true → skipping FTP upload.");
    process.exit(0);
  }

  if (!ICSOURCE_FTP_USER || !ICSOURCE_FTP_PASS) {
    throw new Error("FTP credentials missing and DRY_RUN is false. Aborting upload.");
  }

  console.log("Uploading to IC Source…");
  const remote = await uploadViaFtp(localFile);
  const totalQty = rows.reduce((s, r) => s + r.qty, 0);

  console.log(`✅ Upload complete: ${remote}`);
  console.log(`Summary (80% qty): ${rows.length} SKUs, total qty ${totalQty}`);
})().catch((err) => {
  console.error("❌ Failed:", err?.message || err);
  process.exit(1);
});

