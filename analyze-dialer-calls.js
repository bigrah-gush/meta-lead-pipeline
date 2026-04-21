require('dotenv').config();
const { google } = require('googleapis');

const DIALER_SHEET     = 'Sales Dialer Dump';
const CALL_SHEET       = 'Call Analysis';
const CHUNK_SIZE       = 10000; // rows per read batch

// ── Google Sheets ────────────────────────────────────────────────────────────
async function getSheets() {
  const auth = new google.auth.GoogleAuth({
    keyFile: process.env.GOOGLE_SERVICE_ACCOUNT_KEY,
    scopes: ['https://www.googleapis.com/auth/spreadsheets'],
  });
  return google.sheets({ version: 'v4', auth });
}

// ── Normalize phone → plain digits ──────────────────────────────────────────
function normalizePhone(p = '') {
  return p.replace(/\D/g, '');
}

// ── Load Meta leads from Sheet1 ──────────────────────────────────────────────
async function getMetaLeads(sheets) {
  const { data } = await sheets.spreadsheets.values.get({
    spreadsheetId: process.env.GOOGLE_SHEETS_ID,
    range: 'leadform!A:J',
  });
  const [, ...rows] = data.values || [];
  return rows.map(r => ({
    full_name:    r[2] || '',
    phone:        normalizePhone(r[3]),
    email:        r[4] || '',
    company_name: r[5] || '',
    platform:     r[6] || '',
    campaign:     r[7] || '',
  })).filter(l => l.phone);
}

// ── Read Sales Dialer Dump in chunks, index by phone ────────────────────────
// Headers (0-based index):
//   0  Call ID          9  Agent Name      14  Type
//   2  Contact Number  11  Call Date       15  Disposition
//                      12  Call Time       17  Duration (sec)
async function readDialerDump(sheets) {
  // Get actual row count from sheet metadata
  const meta = await sheets.spreadsheets.get({ spreadsheetId: process.env.GOOGLE_SHEETS_ID });
  const dialerSheetMeta = meta.data.sheets.find(s => s.properties.title === DIALER_SHEET);
  const totalRows = dialerSheetMeta?.properties?.gridProperties?.rowCount ?? 270000;

  console.log(`  Reading Sales Dialer Dump (~${totalRows} rows in chunks of ${CHUNK_SIZE})...`);

  const callsByPhone = {};
  let totalRead = 0;

  for (let startRow = 2; startRow <= totalRows; startRow += CHUNK_SIZE) {
    const endRow = startRow + CHUNK_SIZE - 1;
    const { data } = await sheets.spreadsheets.values.get({
      spreadsheetId: process.env.GOOGLE_SHEETS_ID,
      range: `${DIALER_SHEET}!A${startRow}:U${endRow}`,
    });

    const rows = data.values || [];
    if (rows.length === 0) break;

    for (const row of rows) {
      const phone = normalizePhone(row[2]); // Contact Number
      if (!phone) continue;

      if (!callsByPhone[phone]) callsByPhone[phone] = [];
      callsByPhone[phone].push({
        agentName:   row[9]  || '',
        callDate:    row[11] || '',
        callTime:    row[12] || '',
        type:        (row[14] || '').toLowerCase(),
        disposition: row[15] || '',
        duration:    row[17] || '',
      });
    }

    totalRead += rows.length;
    process.stdout.write(
      `\r    ${totalRead.toLocaleString()} rows read, ${Object.keys(callsByPhone).length} unique phones`
    );

    if (rows.length < CHUNK_SIZE) break; // last chunk
  }

  console.log(''); // newline after progress
  return callsByPhone;
}

// ── Summarize calls for one lead ─────────────────────────────────────────────
function summarize(calls) {
  // Sort newest first
  const sorted = [...calls].sort((a, b) => {
    const da = new Date(`${a.callDate}T${a.callTime || '00:00:00'}Z`);
    const db = new Date(`${b.callDate}T${b.callTime || '00:00:00'}Z`);
    return db - da;
  });

  const total     = calls.length;
  const connected = calls.filter(c => c.type === 'answered').length;
  const missed    = calls.filter(c => c.type === 'unanswered' || c.type === 'missed').length;
  const voicemail = calls.filter(c => c.type === 'voicemail').length;
  const other     = total - connected - missed - voicemail;

  const last      = sorted[0];
  const lastDate  = `${last.callDate} ${last.callTime}`.trim();
  const lastDisp  = last.disposition || last.type || '';
  const lastAgent = last.agentName || '';

  const agents = [...new Set(calls.map(c => c.agentName).filter(Boolean))].join(', ');

  const history = sorted.slice(0, 10).map(c =>
    `${c.callDate} ${c.callTime} | ${c.disposition || c.type} | ${c.agentName}`
  ).join('\n');

  return { total, connected, missed, voicemail, other, lastDate, lastDisp, lastAgent, agents, history };
}

// ── Write Call Analysis sheet ────────────────────────────────────────────────
async function writeCallSheet(sheets, rows) {
  const HEADERS = [
    'Lead Name', 'Phone', 'Email', 'Company', 'Platform', 'Campaign',
    'Total Calls', 'Connected', 'Not Answered', 'Voicemail', 'Other',
    'Last Called', 'Last Disposition', 'Last Agent', 'All Agents',
    'Call History (last 10)',
  ];

  await sheets.spreadsheets.values.clear({
    spreadsheetId: process.env.GOOGLE_SHEETS_ID,
    range: `${CALL_SHEET}!A:P`,
  });

  await sheets.spreadsheets.values.update({
    spreadsheetId: process.env.GOOGLE_SHEETS_ID,
    range: `${CALL_SHEET}!A1`,
    valueInputOption: 'RAW',
    requestBody: { values: [HEADERS, ...rows] },
  });
}

// ── Main ─────────────────────────────────────────────────────────────────────
async function run() {
  console.log('Connecting to Google Sheets...');
  const sheets = await getSheets();

  console.log('\nLoading Meta leads from Sheet1...');
  const leads = await getMetaLeads(sheets);
  console.log(`  ${leads.length} leads with phone numbers`);

  // Build lookup: normalizedPhone → lead
  const leadByPhone = {};
  for (const lead of leads) {
    leadByPhone[lead.phone] = lead;
  }

  console.log('\nReading Sales Dialer Dump...');
  const callsByPhone = await readDialerDump(sheets);
  console.log(`  ${Object.keys(callsByPhone).length} unique phones in dialer dump`);

  // Match to Meta leads
  const matchedPhones = Object.keys(callsByPhone).filter(p => leadByPhone[p]);
  const unmatchedCount = Object.keys(callsByPhone).length - matchedPhones.length;
  console.log(`\n  ${matchedPhones.length} Meta leads have call activity`);
  console.log(`  ${unmatchedCount} phones in dialer that aren't Meta leads`);
  console.log(`  ${leads.length - matchedPhones.length} Meta leads never called`);

  // Build rows
  const rows = matchedPhones.map(phone => {
    const lead = leadByPhone[phone];
    const s    = summarize(callsByPhone[phone]);
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
      s.other,
      s.lastDate,
      s.lastDisp,
      s.lastAgent,
      s.agents,
      s.history,
    ];
  });

  // Sort by total calls desc
  rows.sort((a, b) => b[6] - a[6]);

  console.log(`\nWriting ${rows.length} rows to "${CALL_SHEET}"...`);
  await writeCallSheet(sheets, rows);

  console.log(`\n✓ Done.`);
  console.log(`  Leads with calls:    ${rows.length}`);
  console.log(`  Leads never called:  ${leads.length - rows.length}`);
  console.log(`  Top 5 by call count:`);
  rows.slice(0, 5).forEach((r, i) =>
    console.log(`    ${i + 1}. ${r[0]} — ${r[6]} calls (${r[7]} connected)`)
  );
}

run().catch(err => {
  console.error('\nFatal:', err.message);
  process.exit(1);
});
