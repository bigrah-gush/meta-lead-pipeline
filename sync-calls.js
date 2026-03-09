/**
 * sync-calls.js
 *
 * Fetches all outbound AI SDR calls from Retell, matches them to Meta leads
 * by phone number, and writes/refreshes the "AI Calls" sheet in the workbook.
 *
 * Sheet columns (one row per call, oldest first):
 *   A  Call Date/Time
 *   B  Call ID
 *   C  Lead Name
 *   D  Lead ID          (links to Sheet1 col B)
 *   E  Phone Called
 *   F  Duration (sec)
 *   G  Call Status
 *   H  Disconnection Reason
 *   I  Call Successful
 *   J  In Voicemail
 *   K  Sentiment
 *   L  Call Summary
 *   M  Agent
 *   N  Attempt #
 */

require('dotenv').config();
const axios  = require('axios');
const { google } = require('googleapis');

const SHEET_NAME  = 'AI Calls';
const SHEET_ID    = process.env.GOOGLE_SHEETS_ID;
const RETELL_KEY  = process.env.RETELL_API_KEY;

const HEADERS = [
  'Call Date/Time',
  'Call ID',
  'Lead Name',
  'Lead ID',
  'Phone Called',
  'Duration (sec)',
  'Call Status',
  'Disconnection Reason',
  'Call Successful',
  'In Voicemail',
  'Sentiment',
  'Call Summary',
  'Agent',
  'Attempt #',
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

// ── Load leads from Sheet1, build phone → {id, name} map ──────────────────

async function buildLeadMap(sheets) {
  const { data } = await sheets.spreadsheets.values.get({
    spreadsheetId: SHEET_ID,
    range: 'Sheet1!A:L',  // up to User Provided Phone (col L)
  });

  const [, ...rows] = data.values || [];
  const map = {};

  for (const r of rows) {
    const id   = r[1] || '';
    const name = r[7] || '';  // Full Name (col H)
    // cols J, K, L = phone, phone_number, user_provided_phone_number
    const phones = [r[9], r[10], r[11]].map(normalizePhone).filter(Boolean);
    for (const ph of phones) {
      if (!map[ph]) map[ph] = { id, name };
    }
  }

  return map;
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

// ── Map a Retell call to a sheet row ───────────────────────────────────────

function callToRow(call, leadMap) {
  const ph    = normalizePhone(call.to_number || '');
  const lead  = leadMap[ph] || {};
  const ana   = call.call_analysis || {};
  const meta  = call.metadata      || {};

  const startMs = call.start_timestamp;
  const dateStr = startMs
    ? new Date(startMs).toISOString().replace('T', ' ').slice(0, 19) + ' UTC'
    : '';

  const durSec = call.duration_ms ? (call.duration_ms / 1000).toFixed(1) : '0';

  return [
    dateStr,
    call.call_id                        || '',
    lead.name                           || '',
    lead.id                             || '',
    call.to_number                      || '',
    durSec,
    call.call_status                    || '',
    call.disconnection_reason           || '',
    ana.call_successful ? 'Yes' : 'No',
    ana.in_voicemail    ? 'Yes' : 'No',
    ana.user_sentiment                  || '',
    ana.call_summary                    || '',
    call.agent_name                     || '',
    meta.attempt_number                 || '',
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
  const leadMap = await buildLeadMap(sheets);
  console.log(`  ${Object.keys(leadMap).length} phone numbers indexed`);

  console.log('\nFetching Retell calls...');
  const calls = await fetchAllCalls();
  console.log(`  ${calls.length} outbound calls total`);

  const rows = calls.map(c => callToRow(c, leadMap));

  const matched = rows.filter(r => r[3]).length;
  console.log(`  ${matched} calls matched to Meta leads`);
  console.log(`  ${calls.length - matched} calls to untracked numbers`);

  console.log(`\nWriting to "${SHEET_NAME}" sheet...`);
  const sheetId = await ensureSheet(sheets);
  await writeSheet(sheets, sheetId, rows);

  // Stats
  const ended       = calls.filter(c => c.call_status === 'ended').length;
  const noAnswer    = calls.filter(c => c.disconnection_reason === 'dial_no_answer').length;
  const voicemail   = calls.filter(c => c.call_analysis?.in_voicemail).length;
  const successful  = calls.filter(c => c.call_analysis?.call_successful).length;

  console.log(`\n✓ Done. "${SHEET_NAME}" populated with ${rows.length} calls.`);
  console.log(`\nBreakdown:`);
  console.log(`  Ended (connected):  ${ended}`);
  console.log(`  No answer:          ${noAnswer}`);
  console.log(`  Voicemail:          ${voicemail}`);
  console.log(`  Successful:         ${successful}`);
}

run().catch(err => {
  console.error('Fatal:', err.message);
  process.exit(1);
});
