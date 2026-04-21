/**
 * sync-dialer-calls.js
 *
 * Fetches all human calls from JustCall Sales Dialer campaign #3085672,
 * matches them to Meta leads from Sheet1, and writes one aggregated row
 * per lead to a "Human Calls" sheet.
 *
 * Sheet columns:
 *   A  Lead Name
 *   B  Company
 *   C  Website
 *   D  Email
 *   E  Phone
 *   F  Form
 *   G  Meta Campaign
 *   H  Ad Name
 *   I  Total Calls
 *   J  Connected
 *   K  No Answer
 *   L  Machine / Voicemail
 *   M  Last Called
 *   N  Last Disposition
 *   O  Last Agent
 *   P  Last Duration
 *   Q  Last Notes
 *   R  Recording (last)
 */

require('dotenv').config();
const axios = require('axios');
const { google } = require('googleapis');

const SHEET_NAME   = 'Human Calls';
const SHEET_ID     = process.env.GOOGLE_SHEETS_ID;
const CAMPAIGN_ID  = 3085672;
const DELAY_MS     = 350;

const HEADERS = [
  'Lead Name',
  'Lead ID',
  'Company',
  'Website',
  'Email',
  'Phone',
  'Form',
  'Meta Campaign',
  'Ad Name',
  'Total Calls',
  'Connected',
  'No Answer',
  'Machine / Voicemail',
  'Last Called',
  'Last Disposition',
  'Last Agent',
  'Last Duration (s)',
  'Last Notes',
  'Recording (last)',
];

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

function normalizePhone(p = '') { return p.replace(/\D/g, ''); }

// ── Google Sheets ──────────────────────────────────────────────────────────

async function getSheets() {
  const credentials = process.env.GOOGLE_SERVICE_ACCOUNT_JSON
    ? JSON.parse(process.env.GOOGLE_SERVICE_ACCOUNT_JSON)
    : undefined;
  const auth = new google.auth.GoogleAuth({
    credentials,
    keyFile: credentials ? undefined : process.env.GOOGLE_SERVICE_ACCOUNT_KEY,
    scopes: ['https://www.googleapis.com/auth/spreadsheets'],
  });
  return google.sheets({ version: 'v4', auth });
}

// Build phone → lead map from Sheet1
async function buildLeadMap(sheets) {
  const { data } = await sheets.spreadsheets.values.get({
    spreadsheetId: SHEET_ID,
    range: 'leadform!A:N',
  });

  const [, ...rows] = data.values || [];
  const map = {};

  for (const r of rows) {
    const lead = {
      id:       r[1]  || '',
      name:     r[7]  || '',
      email:    r[8]  || '',
      phone:    r[9]  || r[10] || r[11] || '',
      company:  r[12] || '',
      website:  r[13] || '',
      form:     r[2]  || '',
      campaign: r[3]  || '',
      ad_name:  r[5]  || '',
    };
    const phones = [r[9], r[10], r[11]].map(normalizePhone).filter(Boolean);
    for (const ph of phones) {
      if (!map[ph]) map[ph] = lead;
    }
  }

  return map;
}

// ── JustCall API ───────────────────────────────────────────────────────────

function jcHeaders() {
  const auth = Buffer.from(
    `${process.env.JUSTCALL_API_KEY}:${process.env.JUSTCALL_API_SECRET}`
  ).toString('base64');
  return { Authorization: `Basic ${auth}`, Accept: 'application/json' };
}

async function fetchPage(params) {
  while (true) {
    let res;
    try {
      res = await axios.get('https://api.justcall.io/v2.1/sales_dialer/calls', {
        headers: jcHeaders(),
        params,
        validateStatus: s => (s >= 200 && s < 300) || s === 429,
      });
    } catch (e) {
      console.log(`\nNetwork error, retrying...`);
      await sleep(5000);
      continue;
    }
    if (res.status === 429) { await sleep(15000); continue; }
    return res.data;
  }
}

async function fetchAllDialerCalls() {
  const seen = new Set();
  const all  = [];

  // Pass 1: campaign filter — covers bulk of calls (API caps around Feb 27)
  console.log('  Pass 1: campaign filter...');
  let page = 1;
  while (true) {
    const data = await fetchPage({ per_page: 100, page, campaign_id: CAMPAIGN_ID });
    const calls = data.data || [];
    if (!calls.length) break;
    for (const c of calls) { if (!seen.has(c.call_id)) { seen.add(c.call_id); all.push(c); } }
    process.stdout.write(`\r    ${all.length} calls...`);
    if (!data.next_page_link) break;
    page++;
    await sleep(DELAY_MS);
  }
  console.log(`\n  Pass 1 done: ${all.length} calls`);

  // Pass 2: unfiltered recent pages — picks up calls the campaign filter misses
  // Stop once oldest call on a page is before our earliest campaign call
  console.log('  Pass 2: recent pages (unfiltered, client-side filter by campaign)...');
  let recentAdded = 0;
  page = 1;
  while (page <= 100) { // safety cap
    const data = await fetchPage({ per_page: 100, page });
    const calls = data.data || [];
    if (!calls.length) break;
    for (const c of calls) {
      if (c.campaign?.id !== CAMPAIGN_ID) continue;
      if (!seen.has(c.call_id)) { seen.add(c.call_id); all.push(c); recentAdded++; }
    }
    const oldest = calls[calls.length - 1]?.call_date || '';
    process.stdout.write(`\r    Page ${page}, oldest: ${oldest}, added: ${recentAdded}...`);
    // Stop once we're past March (campaign filter already covers pre-March)
    if (oldest < '2026-03-01') break;
    if (!data.next_page_link) break;
    page++;
    await sleep(DELAY_MS);
  }
  console.log(`\n  Pass 2 done: ${recentAdded} additional calls`);

  return all;
}

// ── Aggregate calls per lead ───────────────────────────────────────────────

function aggregateRow(lead, calls) {
  const sorted = [...calls].sort((a, b) =>
    `${a.call_date} ${a.call_time}`.localeCompare(`${b.call_date} ${b.call_time}`)
  );
  const last = sorted[sorted.length - 1];

  const connected = calls.filter(c => c.call_info?.type === 'Connected').length;
  const noAnswer  = calls.filter(c =>
    c.call_info?.type === 'Not Connected' || c.call_info?.disposition === 'No Answer'
  ).length;
  const machine   = calls.filter(c =>
    (c.call_info?.call_answered_by || '').toLowerCase().includes('machine') ||
    (c.call_info?.call_answered_by || '').toLowerCase().includes('voicemail')
  ).length;

  return [
    lead.name,
    lead.id,
    lead.company,
    lead.website,
    lead.email,
    lead.phone,
    lead.form,
    lead.campaign,
    lead.ad_name,
    calls.length,
    connected,
    noAnswer,
    machine,
    `${last.call_date} ${last.call_time} UTC`,
    last.call_info?.disposition || '',
    last.agent_name || '',
    last.call_info?.duration || 0,
    last.call_info?.notes || '',
    last.call_info?.recording || '',
  ];
}

// ── Sheet setup ────────────────────────────────────────────────────────────

async function ensureSheet(sheets) {
  const { data } = await sheets.spreadsheets.get({ spreadsheetId: SHEET_ID });
  const existing = data.sheets.find(s => s.properties.title === SHEET_NAME);
  if (existing) return existing.properties.sheetId;

  await sheets.spreadsheets.batchUpdate({
    spreadsheetId: SHEET_ID,
    requestBody: { requests: [{ addSheet: { properties: { title: SHEET_NAME } } }] },
  });
  console.log(`Created sheet: ${SHEET_NAME}`);
  const { data: fresh } = await sheets.spreadsheets.get({ spreadsheetId: SHEET_ID });
  return fresh.sheets.find(s => s.properties.title === SHEET_NAME).properties.sheetId;
}

async function writeSheet(sheets, sheetId, rows) {
  await sheets.spreadsheets.values.clear({ spreadsheetId: SHEET_ID, range: SHEET_NAME });

  await sheets.spreadsheets.values.update({
    spreadsheetId: SHEET_ID,
    range: `${SHEET_NAME}!A1`,
    valueInputOption: 'RAW',
    requestBody: { values: [HEADERS, ...rows] },
  });

  await sheets.spreadsheets.batchUpdate({
    spreadsheetId: SHEET_ID,
    requestBody: {
      requests: [
        {
          repeatCell: {
            range: { sheetId, startRowIndex: 0, endRowIndex: 1 },
            cell: { userEnteredFormat: { textFormat: { bold: true } } },
            fields: 'userEnteredFormat.textFormat.bold',
          },
        },
        {
          updateSheetProperties: {
            properties: { sheetId, gridProperties: { frozenRowCount: 1 } },
            fields: 'gridProperties.frozenRowCount',
          },
        },
      ],
    },
  });
}

// ── Main ───────────────────────────────────────────────────────────────────

async function run() {
  console.log('Connecting to Google Sheets...');
  const sheets = await getSheets();

  console.log('Loading Meta leads from Sheet1...');
  const leadMap = await buildLeadMap(sheets);
  console.log(`  ${Object.keys(leadMap).length} phone numbers indexed`);

  console.log(`\nFetching JustCall Sales Dialer calls (campaign ${CAMPAIGN_ID})...`);
  const calls = await fetchAllDialerCalls();
  console.log(`  ${calls.length} calls fetched`);

  // Group by normalized phone, only keep Meta leads
  const byPhone = {};
  let unmatched = 0;

  for (const call of calls) {
    const ph = normalizePhone(call.contact_number || '');
    if (!leadMap[ph]) { unmatched++; continue; }
    if (!byPhone[ph]) byPhone[ph] = [];
    byPhone[ph].push(call);
  }

  const matchedLeads = Object.keys(byPhone).length;
  console.log(`  ${matchedLeads} Meta leads dialed`);
  console.log(`  ${unmatched} calls to non-Meta numbers (excluded)`);

  // One row per lead, sorted by last called desc
  const rows = Object.entries(byPhone)
    .map(([ph, leadCalls]) => aggregateRow(leadMap[ph], leadCalls))
    .sort((a, b) => b[13].localeCompare(a[13]));

  console.log(`\nWriting "${SHEET_NAME}" sheet...`);
  const sheetId = await ensureSheet(sheets);
  await writeSheet(sheets, sheetId, rows);

  const totalConnected = rows.reduce((s, r) => s + r[10], 0);
  const totalCalls     = rows.reduce((s, r) => s + r[9], 0);

  console.log(`\n✓ Done — ${rows.length} leads, ${totalCalls} calls`);
  console.log(`  Connected: ${totalConnected} | No Answer: ${rows.reduce((s,r)=>s+r[11],0)} | Machine: ${rows.reduce((s,r)=>s+r[12],0)}`);
}

run().catch(err => {
  console.error('Fatal:', err.response?.data || err.message);
  process.exit(1);
});
