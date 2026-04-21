/**
 * push-ai-calls.js
 * Fetches all Retell outbound calls made to Meta leads and writes/updates
 * the "AI Calls analysis" tab in the Google Sheet.
 * Usage: node push-ai-calls.js
 */

require('dotenv').config();
const { google } = require('googleapis');
const fs = require('fs');
const path = require('path');

const RETELL_KEY = process.env.RETELL_API_KEY;
const SHEET_ID   = process.env.GOOGLE_SHEETS_ID;
const TAB_NAME   = 'AI Calls analysis';

function normalizePhone(p = '') { return p.replace(/\D/g, ''); }

// ── Fetch all outbound Retell calls ──────────────────────────────────────────

async function fetchRetellCalls() {
  const all = [];
  let paginationKey = null;
  do {
    const body = { limit: 1000, sort_order: 'ascending', filter_criteria: { direction: ['outbound'] } };
    if (paginationKey) body.pagination_key = paginationKey;
    const r = await fetch('https://api.retellai.com/v2/list-calls', {
      method: 'POST',
      headers: { Authorization: `Bearer ${RETELL_KEY}`, 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    const data = await r.json();
    if (!Array.isArray(data)) break;
    all.push(...data);
    paginationKey = data.length === 1000 ? data[data.length - 1].call_id : null;
  } while (paginationKey);
  return all;
}

// ── Build phone → lead map from Sheet1 ───────────────────────────────────────

async function buildLeadMap(sheets) {
  const res = await sheets.spreadsheets.values.get({ spreadsheetId: SHEET_ID, range: 'leadform!A1:T5000' });
  const rows = res.data.values || [];
  const h = rows[0] || [];

  const nameCol    = h.indexOf('Full Name') !== -1 ? h.indexOf('Full Name') : h.indexOf('Name');
  const companyCol = h.indexOf('Company') !== -1 ? h.indexOf('Company') : h.indexOf('company_name');
  const campCol    = h.indexOf('Campaign Name');
  const adCol      = h.indexOf('Ad Name');
  const emailCol   = h.indexOf('Email') !== -1 ? h.indexOf('Email') : h.indexOf('email');
  const phoneCol1  = h.indexOf('Phone');
  const phoneCol2  = h.indexOf('Phone Number');
  const phoneCol3  = h.indexOf('User Provided Phone');

  const leadMap = {};
  for (const row of rows.slice(1)) {
    const phones = [row[phoneCol1], row[phoneCol2], row[phoneCol3]]
      .map(p => normalizePhone(p || '')).filter(Boolean);
    if (!phones.length) continue;
    const lead = {
      name:     (nameCol >= 0    ? row[nameCol]    : '') || '',
      company:  (companyCol >= 0 ? row[companyCol] : '') || '',
      campaign: (campCol >= 0    ? row[campCol]    : '') || '',
      ad:       (adCol >= 0      ? row[adCol]      : '') || '',
      email:    (emailCol >= 0   ? row[emailCol]   : '') || '',
    };
    for (const ph of phones) {
      if (!leadMap[ph]) leadMap[ph] = lead;
    }
  }
  return leadMap;
}

// ── Ensure "AI Calls analysis" sheet tab exists ───────────────────────────────

async function ensureTab(sheets) {
  const meta = await sheets.spreadsheets.get({ spreadsheetId: SHEET_ID });
  const existing = meta.data.sheets.map(s => s.properties.title);
  if (!existing.includes(TAB_NAME)) {
    await sheets.spreadsheets.batchUpdate({
      spreadsheetId: SHEET_ID,
      requestBody: {
        requests: [{ addSheet: { properties: { title: TAB_NAME } } }],
      },
    });
    console.log(`Created tab: ${TAB_NAME}`);
  }
}

// ── Main ─────────────────────────────────────────────────────────────────────

(async () => {
  // Auth
  const creds = process.env.GOOGLE_SERVICE_ACCOUNT_JSON
    ? JSON.parse(process.env.GOOGLE_SERVICE_ACCOUNT_JSON)
    : JSON.parse(fs.readFileSync(path.join(__dirname, 'google-credentials.json'), 'utf8'));

  const auth = new google.auth.GoogleAuth({
    credentials: creds,
    scopes: ['https://www.googleapis.com/auth/spreadsheets'],
  });
  const sheets = google.sheets({ version: 'v4', auth });

  console.log('Fetching Retell calls + lead map in parallel...');
  const [retellCalls, leadMap] = await Promise.all([
    fetchRetellCalls(),
    buildLeadMap(sheets),
  ]);
  console.log(`  ${retellCalls.length} total Retell calls`);

  // Filter to only calls matched to Meta leads
  const matched = retellCalls.filter(call => {
    const ph = normalizePhone(call.to_number || '');
    return !!leadMap[ph];
  });
  console.log(`  ${matched.length} calls matched to Meta leads`);

  // Build sheet rows
  const header = [
    'Call Date (UTC)', 'Lead Name', 'Company', 'Email',
    'Campaign', 'Ad', 'Phone',
    'Attempt #', 'Call ID',
    'Status', 'Outcome', 'In Voicemail', 'Successful',
    'Duration (s)', 'Sentiment',
    'Summary',
    'Transcript',
    'Recording URL',
  ];

  const rows = [header];

  // Group calls by phone, sort each group by timestamp
  const callsByPhone = {};
  for (const call of matched) {
    const ph = normalizePhone(call.to_number || '');
    if (!callsByPhone[ph]) callsByPhone[ph] = [];
    callsByPhone[ph].push(call);
  }

  for (const ph of Object.keys(callsByPhone)) {
    const lead = leadMap[ph];
    const sorted = callsByPhone[ph].sort((a, b) => a.start_timestamp - b.start_timestamp);

    sorted.forEach((call, i) => {
      const ana  = call.call_analysis || {};
      const date = call.start_timestamp
        ? new Date(call.start_timestamp).toISOString().replace('T', ' ').slice(0, 16) + ' UTC'
        : '';
      rows.push([
        date,
        lead.name,
        lead.company,
        lead.email,
        lead.campaign,
        lead.ad,
        call.to_number || ph,
        i + 1,                              // attempt #
        call.call_id,
        call.call_status || '',
        call.disconnection_reason || '',
        ana.in_voicemail ? 'Yes' : 'No',
        ana.call_successful ? 'Yes' : 'No',
        Math.round((call.duration_ms || 0) / 1000),
        ana.user_sentiment || '',
        ana.call_summary || '',
        call.transcript || '',
        call.recording_url || '',
      ]);
    });
  }

  // Ensure tab + clear + write
  await ensureTab(sheets);

  await sheets.spreadsheets.values.clear({
    spreadsheetId: SHEET_ID,
    range: `${TAB_NAME}!A1:Z50000`,
  });

  await sheets.spreadsheets.values.update({
    spreadsheetId: SHEET_ID,
    range: `${TAB_NAME}!A1`,
    valueInputOption: 'RAW',
    requestBody: { values: rows },
  });

  console.log(`✓ Written ${rows.length - 1} call rows to "${TAB_NAME}"`);
})();
