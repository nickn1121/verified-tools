// scripts/export-and-upload-icsource.mjs
import { createClient } from "@supabase/supabase-js";
import ftp from "basic-ftp";
import { stringify } from "csv-stringify/sync";
import fs from "node:fs/promises";
import path from "node:path";

// ----- env + small helpers -----
const {
  SUPABASE_URL,
  SUPABASE_SERVICE_ROLE,
  SUPABASE_EXCESS_TABLE = "excess_parts",

  DRY_RUN = "false",

  FTP_HOST = "ftp.icsource.com",
  FTP_USER,
  FTP_PASS,
  FTP_SECURE = "false",
  FTP_DIR = "/",

  OUTPUT_FILE = "verified_inventory.csv",

  GITHUB_RUN_URL = ""
} = process.env;

const isDryRun = String(DRY_RUN).toLowerCase() === "true";
const outDir = "out";
const outFile = path.join(outDir, OUTPUT_FILE);

function must(name, value) {
  if (!value) throw new Error(`Missing required env: ${name}`);
  return value;
}

// Validate required env
must("SUPABASE_URL", SUPABASE_URL);
must("SUPABASE_SERVICE_ROLE", SUPABASE_SERVICE_ROLE);
must("FTP_USER", FTP_USER);
must("FTP_PASS", FTP_PASS);

// ----- Supabase client -----
const supa = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE, {
  auth: { persistSession: false }
});

// ----- CSV columns (do NOT aggregate; 80% already applied in DB or we send as-is) -----
const HEADERS = [
  "part_number",
  "quantity",
  "vendor",
  "batch_id",
  "uploaded_by",
  "created_on_utc"
];

// adjust if your table uses different names
const SELECT = `
  id, part_number, quantity, vendor, batch_id, uploaded_by, created_on_utc
`;

// ----- main -----
(async () => {
  console.log(`Dry run: ${isDryRun}`);
  console.log(`Table: ${SUPABASE_EXCESS_TABLE}`);
  console.log(`Output: ${outFile}`);
  await fs.mkdir(outDir, { recursive: true });

  // Cursor paging on id
  let lastId = 0;
  const pageSize = 1000;
  let total = 0;

  const rows = [];

  console.log(`Cursor paging on column: id (page=${pageSize})`);
  while (true) {
    const { data, error } = await supa
      .from(SUPABASE_EXCESS_TABLE)
      .select(SELECT)
      .gt("id", lastId)
      .order("id", { ascending: true })
      .limit(pageSize);

    if (error) throw new Error(`Supabase fetch error: ${error.message}`);

    if (!data || data.length === 0) break;

    for (const r of data) rows.push(r);

    total += data.length;
    lastId = data[data.length - 1].id;

    console.log(`Fetched ${total} rows so far…`);
    if (data.length < pageSize) break;
  }

  // Build CSV (no aggregation)
  const records = rows.map((r) => [
    r.part_number ?? "",
    r.quantity ?? 0,
    r.vendor ?? "",
    r.batch_id ?? "",
    r.uploaded_by ?? "",
    r.created_on_utc ?? ""
  ]);

  const csv = stringify([HEADERS, ...records], { bom: true });
  await fs.writeFile(outFile, csv);
  console.log(`CSV written: ${outFile}`);
  console.log(
    `Export: ${records.length} lines, distinct SKUs ${new Set(
      records.map((x) => x[0])
    ).size}, total qty ${records.reduce((a, x) => a + Number(x[1] || 0), 0)}`
  );

  // ----- FTP upload -----
  if (!isDryRun) {
    console.log(`Connecting to ${FTP_HOST} (secure=${FTP_SECURE})…`);
    const client = new ftp.Client();
    client.ftp.verbose = true;
    try {
      await client.access({
        host: FTP_HOST,
        user: FTP_USER,
        password: FTP_PASS,
        secure: String(FTP_SECURE).toLowerCase() === "true"
      });

      console.log(`Remote folder (PWD): ${await client.pwd()}`);
      if (FTP_DIR && FTP_DIR !== "/") {
        await client.ensureDir(FTP_DIR);
        await client.cd(FTP_DIR);
        console.log(`Changed to remote dir: ${FTP_DIR}`);
      }

      // list before
      console.log(`Remote listing BEFORE upload:`);
      try {
        console.log(await client.list());
      } catch {}

      await client.uploadFrom(outFile, OUTPUT_FILE);
      console.log(`✅ Uploaded ${OUTPUT_FILE}`);

      // list after
      console.log(`Remote listing AFTER upload:`);
      try {
        console.log(await client.list());
      } catch {}
    } finally {
      client.close();
    }
  } else {
    console.log(`DRY_RUN is true — FTP upload skipped.`);
  }

  // ----- log to table (non-blocking) -----
  try {
    const details = {
      run_url: GITHUB_RUN_URL || null,
      dry_run: isDryRun,
      rows: records.length,
      distinct_skus: new Set(records.map((x) => x[0])).size,
      file: OUTPUT_FILE,
      ts: new Date().toISOString()
    };
    const { error: logErr } = await supa.from("auto_ic_logs").insert([details]);
    if (logErr) {
      console.warn(`(non-blocking) log insert failed: ${logErr.message}`);
    } else {
      console.log(`Log row inserted into auto_ic_logs`);
    }
  } catch (e) {
    console.warn(`(non-blocking) log insert exception: ${e.message}`);
  }

  console.log("Done.");
})().catch((err) => {
  console.error("FATAL:", err);
  process.exit(1);
});





