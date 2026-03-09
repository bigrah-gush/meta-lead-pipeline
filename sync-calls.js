/**
 * sync-calls.js
 *
 * Fetches all outbound AI SDR calls from Retell, matches them to Meta leads,
 * and writes one row per Meta lead with their full details + aggregated call data.
 *
 * Sheet columns (one row per Meta lead that was called):
 *   A  Lead Name
 *   B  Company
 *   C  Website
 *   D  Email
 *   E  Phone
 *   F  Form
 *   G  Campaign
 *   H  Ad Name
 *   I  Total Calls
 *   J  Connected
 *   K  No Answer
 *   L  Voicemail
 *   M  Successful
 *   N  Last Called
 *   O  Last Outcome
 *   P  Sentiment (last)
 *   Q  Latest Summary
 */

require('dotenv').config();
const axios  = require('axios');
const { google } = require('googleapis');

const SHEET_NAME  = 'AI Calls';
const SHEET_ID    = process.env.GOOGLE_SHEETS_ID;
const RETELL_KEY  = process.env.RETELL_API_KEY;

const HEADERS = [
  'Lead Name',
  'Company',
  'Website',
  'Email',
  'Phone',
  'Form',
  'Campaign',
  'Ad Name',
  'Total Calls',
  'Connected',
  'No Answer',
  'Voicemail',
  'Successful',
  'Last Called',
  'Last Outcome',
  'Sentiment (last)',
  'Latest Summary',
];

// ── Helpers ────────────────────────────────────────────────────────────────

function normalizePhone(p = '') {
  return p.replace(/\D/g, '');
}

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

// ── Load leads from Sheet1, build phone → full lead object map ────────────
// Sheet1 columns: A=Timestamp B=Lead ID C=Form Name D=Campaign E=Adset
//   F=Ad Name G=Platform H=Full Name I=Email J=Phone K=Phone Number
//   L=User Provided Phone M=Company Name N=Website

async function buildLeadMap(sheets) {
  const { data } = await sheets.spreadsheets.values.get({
    spreadsheetId: SHEET_ID,
    range: 'Sheet1!A:N',
  });

  const [, ...rows] = data.values || [];
  const map = {};   // normalizedPhone → lead object
  const leads = []; // ordered list for output

  for (const r of rows) {
    const lead = {
      id:       r[1]  || '',
      form:     r[2]  || '',
      campaign: r[3]  || '',
      ad_name:  r[5]  || '',
      name:     r[7]  || '',
      email:    r[8]  || '',
      phone:    r[9]  || r[10] || r[11] || '', // first available phone
      company:  r[12] || '',
      website:  r[13] || '',
    };

    // Index all phone variants
    const phones = [r[9], r[10], r[11]].map(normalizePhone).filter(Boolean);
    for (const ph of phones) {
      if (!map[ph]) {
        map[ph] = lead;
        leads.push({ phone: ph, lead });
      }
    }
  }

  return { map, leads };
}

// ── Fetch all outbound Retell calls (paginated) ────────────────────────────

async function fetchAllCalls() {
  const all = [];
  let paginationKey = null;

  do {
    const body = {
      limit: 1000,
      sort_order: 'ascending',
      filter_criteria: { direction: ['outbound'] },
    };
    if (paginationKey) body.pagination_key = paginationKey;

    const { data } = await axios.post(
      'https://api.retellai.com/v2/list-calls',
      body,
      { headers: { Authorization: `Bearer ${RETELL_KEY}` } }
    );

    all.push(...data);
    paginationKey = data.length === 1000 ? data[data.length - 1].call_id : null;
    process.stdout.write(`\r  Fetched ${all.length} calls...`);
  } while (paginationKey);

  console.log();
  return all;
}

// ── Aggregate all calls for one lead into a single row ────────────────────

function aggregateLeadCalls(lead, calls) {
  // Sort oldest → newest
  const sorted = [...calls].sort((a, b) => a.start_timestamp - b.start_timestamp);
  const last   = sorted[sorted.length - 1];
  const lastAna = last.call_analysis || {};

  const lastDate = last.start_timestamp
    ? new Date(last.start_timestamp).toISOString().replace('T', ' ').slice(0, 16) + ' UTC'
    : '';

  const connected  = calls.filter(c => c.call_status === 'ended').length;
  const noAnswer   = calls.filter(c => c.disconnection_reason === 'dial_no_answer').length;
  const voicemail  = calls.filter(c => c.call_analysis?.in_voicemail).length;
  const successful = calls.filter(c => c.call_analysis?.call_successful).length;

  // Last meaningful outcome label
  const lastOutcome = last.disconnection_reason || last.call_status || '';

  return [
    lead.name,
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
    voicemail,
    successful,
    lastDate,
    lastOutcome,
    lastAna.user_sentiment  || '',
    lastAna.call_summary    || '',
  ];
}

// ── Ensure "AI Calls" sheet exists ────────────────────────────────────────

async function ensureSheet(sheets) {
  const { data } = await sheets.spreadsheets.get({ spreadsheetId: SHEET_ID });
  const exists = data.sheets.some(s => s.properties.title === SHEET_NAME);

  if (!exists) {
    await sheets.spreadsheets.batchUpdate({
      spreadsheetId: SHEET_ID,
      requestBody: {
        requests: [{ addSheet: { properties: { title: SHEET_NAME } } }],
      },
    });
    console.log(`Created sheet: ${SHEET_NAME}`);
    // Re-fetch so the new sheet appears
    const { data: fresh } = await sheets.spreadsheets.get({ spreadsheetId: SHEET_ID });
    return fresh.sheets.find(s => s.properties.title === SHEET_NAME).properties.sheetId;
  }

  return data.sheets.find(s => s.properties.title === SHEET_NAME).properties.sheetId;
}

// ── Write all rows + formatting ────────────────────────────────────────────

async function writeSheet(sheets, sheetId, rows) {
  // Clear
  await sheets.spreadsheets.values.clear({
    spreadsheetId: SHEET_ID,
    range: `${SHEET_NAME}`,
  });

  // Write data
  await sheets.spreadsheets.values.update({
    spreadsheetId: SHEET_ID,
    range: `${SHEET_NAME}!A1`,
    valueInputOption: 'RAW',
    requestBody: { values: [HEADERS, ...rows] },
  });

  // Bold header + freeze
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
  if (!RETELL_KEY) {
    console.error('RETELL_API_KEY not set in .env');
    process.exit(1);
  }

  console.log('Connecting to Google Sheets...');
  const sheets = await getSheets();

  console.log('Loading leads from Sheet1...');
  const { map: leadMap } = await buildLeadMap(sheets);
  console.log(`  ${Object.keys(leadMap).length} phone numbers indexed`);

  console.log('\nFetching Retell calls...');
  const calls = await fetchAllCalls();
  console.log(`  ${calls.length} outbound calls total`);

  // Group calls by normalized phone, only keep those matching Meta leads
  const callsByPhone = {};
  let unmatched = 0;
  for (const call of calls) {
    const ph = normalizePhone(call.to_number || '');
    if (!leadMap[ph]) { unmatched++; continue; }
    if (!callsByPhone[ph]) callsByPhone[ph] = [];
    callsByPhone[ph].push(call);
  }

  const matchedLeads = Object.keys(callsByPhone).length;
  console.log(`  ${matchedLeads} Meta leads were called`);
  console.log(`  ${unmatched} calls to non-Meta numbers (excluded)`);

  // One row per lead, sorted by most recently called
  const rows = Object.entries(callsByPhone)
    .map(([ph, leadCalls]) => aggregateLeadCalls(leadMap[ph], leadCalls))
    .sort((a, b) => b[14].localeCompare(a[14])); // sort by Last Called desc

  console.log(`\nWriting to "${SHEET_NAME}" sheet...`);
  const sheetId = await ensureSheet(sheets);
  await writeSheet(sheets, sheetId, rows);

  const totalCalls    = calls.length - unmatched;
  const successful    = calls.filter(c => leadMap[normalizePhone(c.to_number||'')] && c.call_analysis?.call_successful).length;
  const voicemail     = calls.filter(c => leadMap[normalizePhone(c.to_number||'')] && c.call_analysis?.in_voicemail).length;

  console.log(`\n✓ Done. "${SHEET_NAME}" — ${rows.length} leads, ${totalCalls} total calls.`);
  console.log(`  Successful: ${successful} | Voicemail: ${voicemail}`);
}

run().catch(err => {
  console.error('Fatal:', err.message);
  process.exit(1);
});
