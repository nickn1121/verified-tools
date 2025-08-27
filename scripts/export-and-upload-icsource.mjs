// scripts/export-and-upload-icsource.mjs
// Exports ONLY part_num + quantity (80% rounded), uploads to IC Source via FTP,
// and logs the run into Supabase.

import { createClient } from '@supabase/supabase-js'
import { stringify } from 'csv-stringify/sync'
import * as fs from 'fs/promises'
import path from 'path'
import ftp from 'basic-ftp'

// -------- Env & constants --------
const SUPABASE_URL = process.env.SUPABASE_URL
const SUPABASE_KEY = process.env.SUPABASE_KEY   // Use service role in GitHub Actions
const ICSOURCE_FTP_USER = process.env.ICSOURCE_FTP_USER
const ICSOURCE_FTP_PASS = process.env.ICSOURCE_FTP_PASS
const DRY_RUN = String(process.env.DRY_RUN || '').toLowerCase() === 'true'

if (!SUPABASE_URL || !SUPABASE_KEY) {
  throw new Error('SUPABASE_URL / SUPABASE_KEY missing')
}

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY)

const TABLE = 'excess_parts'   // your source table
const PAGE = 1000               // fetch page size
const CSV_DIR = 'out'
const CSV_FILE = 'verified_inventory.csv'
const CSV_PATH = path.join(CSV_DIR, CSV_FILE)

// -------- Helpers --------
function adjustQty(q) {
  const n = Number(q) || 0
  // 80% and round to nearest whole number
  const v = Math.round(n * 0.80)
  return v < 0 ? 0 : v
}

function ghRunUrl() {
  const { GITHUB_SERVER_URL, GITHUB_REPOSITORY, GITHUB_RUN_ID } = process.env
  if (GITHUB_SERVER_URL && GITHUB_REPOSITORY && GITHUB_RUN_ID) {
    return `${GITHUB_SERVER_URL}/${GITHUB_REPOSITORY}/actions/runs/${GITHUB_RUN_ID}`
  }
  return null
}

// -------- Fetch from Supabase (cursor on id) --------
async function fetchAll() {
  let lastId = 0
  const rows = []

  while (true) {
    const { data, error } = await supabase
      .from(TABLE)
      .select('id, part_num, quantity')
      .gt('id', lastId)
      .order('id', { ascending: true })
      .limit(PAGE)

    if (error) {
      throw new Error(`Supabase fetch error: ${error.message}`)
    }
    if (!data || data.length === 0) break

    rows.push(...data)
    lastId = data[data.length - 1].id

    // Optional console progress for Actions log
    console.log(`Fetched ${rows.length}/? rows so far…`)
  }
  return rows
}

// -------- Build CSV (ONLY part_num + quantity) --------
function buildCsv(rows) {
  const out = []
  const skuSet = new Set()
  let totalQty = 0

  for (const r of rows) {
    const pn = (r.part_num || '').trim()
    // use adjusted quantity (80% rule)
    const qty = adjustQty(r.quantity)
    totalQty += qty
    skuSet.add(pn)
    // ONLY two columns in CSV: part_num, quantity
    out.push([pn, qty])
  }

  const csv = stringify(out, {
    header: true,
    columns: ['part_num', 'quantity'], // <-- no vendor
    record_delimiter: 'unix',
  })

  return {
    csv,
    lineCount: out.length,
    distinctSkus: skuSet.size,
    totalQty,
  }
}

// -------- FTP upload (if not DRY_RUN) --------
async function uploadViaFtp(localPath, remoteName) {
  const client = new ftp.Client(0)
  client.ftp.verbose = false

  await client.access({
    host: 'ftp.icsource.com',
    user: ICSOURCE_FTP_USER,
    password: ICSOURCE_FTP_PASS,
    secure: false, // IC Source is plain FTP
  })

  console.log('Connected. Server PWD:', await client.pwd())
  console.log(`Uploading: ${localPath} -> ${remoteName}`)
  await client.uploadFrom(localPath, remoteName)

  // Quick confirmation by stat (not all servers support size reliably, but we try)
  try {
    const list = await client.list()
    const found = list.find(f => f.name === remoteName)
    if (found?.size != null) {
      console.log(`✅ Upload confirmed on server: ${remoteName} (${found.size} bytes)`)
    } else {
      console.log('Upload done (size not confirmed by MLSD).')
    }
  } catch {
    // Non-fatal if MLSD isn’t supported
  }

  client.close()
}

// -------- Log to Supabase (best-effort) --------
async function logRun({
  success,
  message,
  lineCount,
  distinctSkus,
  totalQty,
  fileSize,
}) {
  try {
    const payload = {
      run_id: process.env.GITHUB_RUN_ID || null,
      run_url: ghRunUrl(),
      dry_run: DRY_RUN,
      aggregated: false,
      line_count: lineCount ?? null,
      distinct_skus: distinctSkus ?? null,
      total_qty: totalQty ?? null,
      file_size: fileSize ?? null,
      remote_name: CSV_FILE,
      remote_dir: '/',
      ftps: false,
      success: success ?? null,
      message: message ?? null,
    }

    const { data, error } = await supabase
      .from('ic_source_uploads')
      .insert([payload])
      .select('id, created_at')
      .single()

    if (error) {
      console.warn('⚠️ Log insert failed:', error.message)
      return
    }
    console.log(`Log saved (id=${data.id}, at=${data.created_at})`)
  } catch (e) {
    console.warn('⚠️ Log insert threw:', e.message)
  }
}

// -------- Main --------
;(async () => {
  await fs.mkdir(CSV_DIR, { recursive: true })

  // 1) Fetch rows
  const rows = await fetchAll()

  // 2) Build CSV with ONLY part_num + quantity
  const res = buildCsv(rows)
  await fs.writeFile(CSV_PATH, res.csv)
  const stat = await fs.stat(CSV_PATH)

  console.log('Export (NO aggregation):',
    `${res.lineCount} lines, distinct SKUs ${res.distinctSkus}, total qty ${res.totalQty}`)

  let success = true
  let message = 'OK'

  // 3) Upload via FTP (unless DRY_RUN)
  if (!DRY_RUN) {
    if (!ICSOURCE_FTP_USER || !ICSOURCE_FTP_PASS) {
      success = false
      message = 'FTP credentials missing'
      console.error('FTP credentials missing. Set ICSOURCE_FTP_USER and ICSOURCE_FTP_PASS.')
    } else {
      try {
        await uploadViaFtp(CSV_PATH, CSV_FILE)
      } catch (e) {
        success = false
        message = `FTP error: ${e.message}`
        console.error(message)
      }
    }
  } else {
    console.log('Dry run: FTP upload skipped')
  }

  // 4) Log run (best-effort)
  await logRun({
    success,
    message,
    lineCount: res.lineCount,
    distinctSkus: res.distinctSkus,
    totalQty: res.totalQty,
    fileSize: stat.size,
  })

  console.log('Done.')
})().catch(async (err) => {
  console.error(err?.stack || err?.message || String(err))
  // Attempt to log failure with minimal info
  await logRun({ success: false, message: err?.message || 'Unhandled error' })
  process.exit(1)
})














