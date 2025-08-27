// scripts/export-and-upload-icsource.mjs
// EXPORTS ONLY: part_num,quantity  (80% rounded)
// Uploads to IC Source (FTP) and logs a simple row to ic_source_uploads.

import { createClient } from '@supabase/supabase-js'
import { stringify } from 'csv-stringify/sync'
import * as fs from 'fs/promises'
import path from 'path'
import ftp from 'basic-ftp'

// ---------- ENV ----------
const SUPABASE_URL  = process.env.SUPABASE_URL
// Allow either SUPABASE_KEY or SUPABASE_SERVICE_ROLE as the key var name.
const SUPABASE_KEY  = process.env.SUPABASE_KEY || process.env.SUPABASE_SERVICE_ROLE
const FTP_USER      = process.env.ICSOURCE_FTP_USER
const FTP_PASS      = process.env.ICSOURCE_FTP_PASS
const DRY_RUN       = String(process.env.DRY_RUN || '').toLowerCase() === 'true'

if (!SUPABASE_URL || !SUPABASE_KEY) {
  throw new Error('SUPABASE_URL / SUPABASE_KEY missing')
}

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY)

// ---------- CONSTANTS ----------
const TABLE         = 'excess_parts'
const PAGE          = 1000
const CSV_DIR       = 'out'
const CSV_NAME      = 'verified_inventory.csv'
const CSV_PATH      = path.join(CSV_DIR, CSV_NAME)

// ---------- HELPERS ----------
function adj80(n) {
  const v = Math.round((Number(n) || 0) * 0.80)
  return v < 0 ? 0 : v
}

async function fetchAll() {
  // *** This is the same selection as before, just without 'vendor' ***
  // If these column names ever change in the DB, update them here only.
  let lastId = 0
  const rows = []
  while (true) {
    const { data, error } = await supabase
      .from(TABLE)
      .select('id, part_num, quantity')
      .gt('id', lastId)
      .order('id', { ascending: true })
      .limit(PAGE)

    if (error) throw new Error(`Supabase fetch error: ${error.message}`)
    if (!data || data.length === 0) break

    rows.push(...data)
    lastId = data[data.length - 1].id
    console.log(`Fetched ${rows.length}/? rows so far…`)
  }
  return rows
}

function buildCsv(rows) {
  // ONLY two columns in the CSV (no vendor):
  const out = []
  const uniq = new Set()
  let totalQty = 0

  for (const r of rows) {
    const pn = (r.part_num || '').toString().trim()
    const qty = adj80(r.quantity)
    uniq.add(pn)
    totalQty += qty
    out.push([pn, qty])
  }

  const csv = stringify(out, {
    header: true,
    columns: ['part_num', 'quantity'],
    record_delimiter: 'unix',
  })

  return { csv, lineCount: out.length, distinctSkus: uniq.size, totalQty }
}

async function uploadFtp(localPath, remoteName) {
  const client = new ftp.Client(0)
  client.ftp.verbose = false

  await client.access({
    host: 'ftp.icsource.com',
    user: FTP_USER,
    password: FTP_PASS,
    secure: false, // same as before
  })

  console.log('Connected. Server PWD:', await client.pwd())

  // Optional: show what’s already there (handy for sanity checks)
  try {
    const listing = await client.list()
    const found = listing.find(x => x.name === remoteName)
    if (found) {
      console.log('Remote listing BEFORE upload:')
      console.log(` • ${found.name}  ${found.size}`)
    }
  } catch { /* non-fatal */ }

  console.log(`Uploading: ${localPath}  ->  ${remoteName}`)
  await client.uploadFrom(localPath, remoteName)

  try {
    const listing = await client.list()
    const f = listing.find(x => x.name === remoteName)
    if (f?.size != null) {
      console.log(`✅ Upload confirmed on server: ${remoteName} (${f.size} bytes)`)
    } else {
      console.log('Upload done (size not reported by server).')
    }
  } catch { /* non-fatal */ }

  client.close()
}

async function logRow({ success, lineCount, distinctSkus, totalQty, fileSize, message }) {
  // Same fields we used when logs were showing correctly.
  try {
    const payload = {
      success: success ?? null,
      line_count: lineCount ?? null,
      distinct_skus: distinctSkus ?? null,
      total_qty: totalQty ?? null,
      file_size: fileSize ?? null,
      remote_name: CSV_NAME,
      remote_dir: '/',
      ftps: false,
      message: message || 'OK',
    }
    const { error } = await supabase.from('ic_source_uploads').insert([payload])
    if (error) console.warn('⚠️ Log insert failed:', error.message)
  } catch (e) {
    console.warn('⚠️ Log insert threw:', e.message)
  }
}

// ---------- MAIN ----------
;(async () => {
  await fs.mkdir(CSV_DIR, { recursive: true })

  // 1) Fetch the same columns as before (minus vendor)
  const rows = await fetchAll()

  // 2) Build CSV with part_num,quantity only
  const { csv, lineCount, distinctSkus, totalQty } = buildCsv(rows)
  await fs.writeFile(CSV_PATH, csv)
  const st = await fs.stat(CSV_PATH)

  console.log(
    `Export: ${lineCount} lines, distinct SKUs ${distinctSkus}, total qty ${totalQty}`
  )

  let success = true
  let message = 'OK'

  // 3) Upload (skip if DRY_RUN)
  if (!DRY_RUN) {
    if (!FTP_USER || !FTP_PASS) {
      success = false
      message = 'FTP credentials missing'
      console.error(message)
    } else {
      try {
        await uploadFtp(CSV_PATH, CSV_NAME)
      } catch (e) {
        success = false
        message = `FTP error: ${e.message}`
        console.error(message)
      }
    }
  } else {
    console.log('Dry run: FTP upload skipped.')
  }

  // 4) Log to ic_source_uploads as before
  await logRow({ success, lineCount, distinctSkus, totalQty, fileSize: st.size, message })

  console.log('Done.')
})().catch(async err => {
  console.error(err?.stack || err?.message || String(err))
  await logRow({ success: false, message: err?.message || 'Unhandled error' })
  process.exit(1)
})
















