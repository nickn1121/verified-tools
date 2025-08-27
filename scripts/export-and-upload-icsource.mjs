// scripts/export-and-upload-icsource.mjs
// Export ONLY part_num + quantity (80% rounded), upload to IC Source via FTP,
// and log a minimal row to ic_source_uploads (only columns guaranteed to exist).

import { createClient } from '@supabase/supabase-js'
import { stringify } from 'csv-stringify/sync'
import * as fs from 'fs/promises'
import path from 'path'
import ftp from 'basic-ftp'

// ---------------- Env ----------------
const SUPABASE_URL = process.env.SUPABASE_URL
const SUPABASE_KEY = process.env.SUPABASE_KEY     // service role in Actions
const ICSOURCE_FTP_USER = process.env.ICSOURCE_FTP_USER
const ICSOURCE_FTP_PASS = process.env.ICSOURCE_FTP_PASS
const DRY_RUN = String(process.env.DRY_RUN || '').toLowerCase() === 'true'

if (!SUPABASE_URL || !SUPABASE_KEY) {
  throw new Error('SUPABASE_URL / SUPABASE_KEY missing')
}

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY)

const TABLE = 'excess_parts' // source table
const PAGE = 1000
const CSV_DIR = 'out'
const CSV_NAME = 'verified_inventory.csv'
const CSV_PATH = path.join(CSV_DIR, CSV_NAME)

// ---------------- Helpers ----------------
function eightyPercentRounded(n) {
  const v = Math.round((Number(n) || 0) * 0.8)
  return v < 0 ? 0 : v
}

async function detectColumnNames() {
  const { data, error } = await supabase.from(TABLE).select('*').limit(1)
  if (error) throw new Error(`Supabase fetch error: ${error.message}`)
  if (!data || data.length === 0) throw new Error(`Supabase table "${TABLE}" is empty`)

  const row = data[0]
  const keys = Object.keys(row)

  // map lower->actual
  const byLower = {}
  for (const k of keys) byLower[k.toLowerCase()] = k

  const partKey =
    byLower['part_num'] ||
    byLower['mpn'] ||
    byLower['pn'] ||
    byLower['sku'] ||
    byLower['part'] ||
    byLower['item'] ||
    byLower['partnumber'] ||
    null

  const qtyKey =
    byLower['quantity'] ||
    byLower['qty'] ||
    byLower['onhand'] ||
    byLower['stock'] ||
    byLower['total_qty'] ||
    byLower['totalqty'] ||
    null

  if (!partKey || !qtyKey) {
    throw new Error(
      `Could not find part/qty columns. Available: [${keys.join(', ')}]`
    )
  }

  return { partKey, qtyKey }
}

async function fetchAll(partKey, qtyKey) {
  let lastId = 0
  const rows = []

  while (true) {
    const { data, error } = await supabase
      .from(TABLE)
      .select(`id, ${partKey}, ${qtyKey}`)
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

function buildCsv(rows, partKey, qtyKey) {
  const out = []
  const uniq = new Set()
  let totalQty = 0

  for (const r of rows) {
    const pn = (r[partKey] || '').toString().trim()
    const qty = eightyPercentRounded(r[qtyKey])
    uniq.add(pn)
    totalQty += qty
    // EXPORT ONLY TWO COLUMNS (no vendor):
    out.push([pn, qty])
  }

  const csv = stringify(out, {
    header: true,
    columns: ['part_num', 'quantity'], // keep the header identical minus vendor
    record_delimiter: 'unix',
  })

  return { csv, lineCount: out.length, distinctSkus: uniq.size, totalQty }
}

async function uploadFtp(localPath, remoteName) {
  const client = new ftp.Client(0)
  client.ftp.verbose = false
  await client.access({
    host: 'ftp.icsource.com',
    user: ICSOURCE_FTP_USER,
    password: ICSOURCE_FTP_PASS,
    secure: false,
  })
  console.log('Connected. Server PWD:', await client.pwd())
  console.log(`Uploading: ${localPath} -> ${remoteName}`)
  await client.uploadFrom(localPath, remoteName)

  try {
    const list = await client.list()
    const f = list.find(x => x.name === remoteName)
    if (f?.size != null) {
      console.log(`✅ Upload confirmed on server: ${remoteName} (${f.size} bytes)`)
    } else {
      console.log('Upload done (size not reported by server).')
    }
  } catch {
    // non-fatal
  }
  client.close()
}

async function logMinimal({ success, message, totalQty, fileSize }) {
  // Insert only columns that are definitely safe/exist in your latest schema.
  // (status/aggregated/line_count/etc omitted on purpose.)
  try {
    const payload = {
      total_qty: totalQty ?? null,
      file_size: fileSize ?? null,
      remote_name: CSV_NAME,
      remote_dir: '/',
      ftps: false,
      success: success ?? null,
      message: message ?? null,
    }
    const { error } = await supabase.from('ic_source_uploads').insert([payload])
    if (error) console.warn('⚠️ Log insert failed:', error.message)
  } catch (e) {
    console.warn('⚠️ Log insert threw:', e.message)
  }
}

// ---------------- Main ----------------
;(async () => {
  await fs.mkdir(CSV_DIR, { recursive: true })

  // 1) discover column names that actually exist
  const { partKey, qtyKey } = await detectColumnNames()

  // 2) fetch everything with those names
  const data = await fetchAll(partKey, qtyKey)

  // 3) build CSV (part_num, quantity)
  const res = buildCsv(data, partKey, qtyKey)
  await fs.writeFile(CSV_PATH, res.csv)
  const st = await fs.stat(CSV_PATH)

  console.log(
    `Export (NO aggregation): ${res.lineCount} lines, distinct SKUs ${res.distinctSkus}, total qty ${res.totalQty}`
  )

  let success = true
  let message = 'OK'

  // 4) upload (unless DRY_RUN)
  if (!DRY_RUN) {
    if (!ICSOURCE_FTP_USER || !ICSOURCE_FTP_PASS) {
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

  // 5) log minimal row (works even if schema is missing optional columns)
  await logMinimal({
    success,
    message,
    totalQty: res.totalQty,
    fileSize: st.size,
  })

  console.log('Done.')
})().catch(async (err) => {
  console.error(err?.stack || err?.message || String(err))
  await logMinimal({ success: false, message: err?.message || 'Unhandled error' })
  process.exit(1)
})















