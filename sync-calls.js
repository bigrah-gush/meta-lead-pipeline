require('dotenv').config();
const axios = require('axios');
const { google } = require('googleapis');

const CALL_SHEET_NAME = 'Call Analysis';
const ADS_START = new Date('2026-01-21T00:00:00Z');

// ── JustCall auth ───────────────────────────────────────────────────────────
const JC_AUTH = 'Basic ' + Buffer.from(
  `${process.env.JUSTCALL_API_KEY}:${process.env.JUSTCALL_API_SECRET}`
).toString('base64');

// ── Google Sheets ───────────────────────────────────────────────────────────
async function getSheets() {
  const auth = new google.auth.GoogleAuth({
    keyFile: process.env.GOOGLE_SERVICE_ACCOUNT_KEY,
    scopes: ['https://www.googleapis.com/auth/spreadsheets'],
  });
  return google.sheets({ version: 'v4', auth });
}

// ── Normalize phone: strip +, spaces, dashes → plain digits ────────────────
function normalizePhone(p = '') {
  return p.replace(/\D/g, '');
}

// ── Fetch all Meta leads from Sheet1 (phone + metadata) ────────────────────
async function getMetaLeads(sheets) {
  const { data } = await sheets.spreadsheets.values.get({
    spreadsheetId: process.env.GOOGLE_SHEETS_ID,
    range: 'Sheet1!A:J',
  });
  const [headers, ...rows] = data.values || [];
  // Sheet columns: Timestamp, Lead ID, Full Name, Phone, Email, Company Name, Platform, Campaign, Ad Set, Ad Name
  return rows.map(r => ({
    timestamp:    r[0] || '',
    id:           r[1] || '',
    full_name:    r[2] || '',
    phone:        normalizePhone(r[3]),
    email:        r[4] || '',
    company_name: r[5] || '',
    platform:     r[6] || '',
    campaign:     r[7] || '',
  })).filter(l => l.phone);
}

// ── Fetch JustCall calls since ADS_START, stop when older ──────────────────
async function fetchCallsSinceAdsLive() {
  const calls = [];
  let page = 1;
  let done = false;

  while (!done) {
    const { data } = await axios.get('https://api.justcall.io/v2/calls', {
      headers: { Authorization: JC_AUTH },
      params: { per_page: 100, page },
    });

    const batch = data.data || [];
    if (batch.length === 0) break;

    for (const call of batch) {
      const callDate = new Date(`${call.call_date}T${call.call_time}Z`);
      if (callDate < ADS_START) {
        done = true; // older than ad launch, stop
        break;
      }
      calls.push(call);
    }

    console.log(`  Page ${page}: ${calls.length} calls fetched so far...`);
    if (data.next_page_link && !done) {
      page++;
    } else {
      break;
    }
  }

  return calls;
}

// ── Group calls by normalized phone number ──────────────────────────────────
function groupCallsByPhone(calls) {
  const map = {};
  for (const call of calls) {
    const phone = normalizePhone(call.contact_number);
    if (!map[phone]) map[phone] = [];
    map[phone].push(call);
  }
  return map;
}

// ── Summarize a list of calls for one prospect ──────────────────────────────
function summarizeCalls(calls) {
  // Sort newest first
  const sorted = [...calls].sort((a, b) =>
    new Date(`${b.call_date}T${b.call_time}Z`) - new Date(`${a.call_date}T${a.call_time}Z`)
  );

  const total      = calls.length;
  const connected  = calls.filter(c => c.call_info?.type === 'answered').length;
  const missed     = calls.filter(c => c.call_info?.type === 'unanswered').length;
  const voicemail  = calls.filter(c => c.call_info?.type === 'voicemail').length;

  const last       = sorted[0];
  const lastDate   = `${last.call_date} ${last.call_time}`;
  const lastDisp   = last.call_info?.disposition || '';
  const lastAgent  = last.agent_name || '';

  const agents = [...new Set(calls.map(c => c.agent_name).filter(Boolean))].join(', ');

  // Compact call history: date | outcome | agent
  const history = sorted.slice(0, 10).map(c =>
    `${c.call_date} ${c.call_time} | ${c.call_info?.disposition || c.call_info?.type} | ${c.agent_name}`
  ).join('\n');

  return { total, connected, missed, voicemail, lastDate, lastDisp, lastAgent, agents, history };
}

// ── Ensure "Call Analysis" sheet exists, create if not ─────────────────────
async function ensureCallSheet(sheets) {
  const meta = await sheets.spreadsheets.get({ spreadsheetId: process.env.GOOGLE_SHEETS_ID });
  const exists = meta.data.sheets.some(s => s.properties.title === CALL_SHEET_NAME);

  if (!exists) {
    await sheets.spreadsheets.batchUpdate({
      spreadsheetId: process.env.GOOGLE_SHEETS_ID,
      requestBody: {
        requests: [{ addSheet: { properties: { title: CALL_SHEET_NAME } } }],
      },
    });
    console.log(`Created sheet: ${CALL_SHEET_NAME}`);
  }
}

// ── Write headers + rows to Call Analysis sheet ─────────────────────────────
async function writeCallSheet(sheets, rows) {
  const HEADERS = [
    'Lead Name', 'Phone', 'Email', 'Company', 'Platform', 'Campaign',
    'Total Calls', 'Connected', 'Not Answered', 'Voicemail',
    'Last Called', 'Last Disposition', 'Last Agent', 'All Agents',
    'Call History (last 10)',
  ];

  // Clear sheet first
  await sheets.spreadsheets.values.clear({
    spreadsheetId: process.env.GOOGLE_SHEETS_ID,
    range: `${CALL_SHEET_NAME}!A:O`,
  });

  await sheets.spreadsheets.values.update({
    spreadsheetId: process.env.GOOGLE_SHEETS_ID,
    range: `${CALL_SHEET_NAME}!A1`,
    valueInputOption: 'RAW',
    requestBody: { values: [HEADERS, ...rows] },
  });
}

// ── Main ───────────────────────────────────────────────────────────────────
async function run() {
  console.log('Connecting to Google Sheets...');
  const sheets = await getSheets();

  console.log('Loading Meta leads from Sheet1...');
  const leads = await getMetaLeads(sheets);
  console.log(`  ${leads.length} leads with phone numbers`);

  // Build phone → lead map
  const leadByPhone = {};
  for (const lead of leads) {
    leadByPhone[lead.phone] = lead;
  }

  console.log('\nFetching JustCall calls since ads went live (2026-01-21)...');
  const calls = await fetchCallsSinceAdsLive();
  console.log(`  ${calls.length} total calls fetched\n`);

  // Group all calls by phone
  const callsByPhone = groupCallsByPhone(calls);

  // Match calls to Meta leads only
  const matchedPhones = Object.keys(callsByPhone).filter(p => leadByPhone[p]);
  const unmatchedCount = Object.keys(callsByPhone).length - matchedPhones.length;

  console.log(`  ${matchedPhones.length} Meta leads have call activity`);
  console.log(`  ${unmatchedCount} calls to non-Meta numbers (ignored)`);

  // Build rows: one row per Meta lead that was called
  const rows = matchedPhones.map(phone => {
    const lead = leadByPhone[phone];
    const s    = summarizeCalls(callsByPhone[phone]);
    return [
      lead.full_name,
      '+' + phone,
      lead.email,
      lead.company_name,
      lead.platform,
      lead.campaign,
      s.total,
      s.connected,
      s.missed,
      s.voicemail,
      s.lastDate,
      s.lastDisp,
      s.lastAgent,
      s.agents,
      s.history,
    ];
  });

  // Sort by total calls desc
  rows.sort((a, b) => b[6] - a[6]);

  console.log(`\nWriting ${rows.length} rows to "${CALL_SHEET_NAME}" sheet...`);
  await ensureCallSheet(sheets);
  await writeCallSheet(sheets, rows);

  console.log(`\n✓ Done. Call Analysis sheet populated.`);
  console.log(`  ${rows.length} Meta leads with calls`);
  console.log(`  ${leads.length - rows.length} Meta leads never called`);
}

run().catch(err => {
  console.error('Fatal:', err.message);
  process.exit(1);
});
