require('dotenv').config();
const { google } = require('googleapis');

const NEVER_DIALED_SHEET = 'Never Dialed';

async function getSheets() {
  const auth = new google.auth.GoogleAuth({
    keyFile: process.env.GOOGLE_SERVICE_ACCOUNT_KEY,
    scopes: ['https://www.googleapis.com/auth/spreadsheets'],
  });
  return google.sheets({ version: 'v4', auth });
}

function normalizePhone(p = '') {
  return p.replace(/\D/g, '');
}

async function run() {
  console.log('Connecting to Google Sheets...');
  const sheets = await getSheets();

  // Load all Meta leads from Sheet1
  console.log('Loading Meta leads from Sheet1...');
  const { data: sheet1Data } = await sheets.spreadsheets.values.get({
    spreadsheetId: process.env.GOOGLE_SHEETS_ID,
    range: 'leadform!A:J',
  });
  const [, ...leadRows] = sheet1Data.values || [];
  const leads = leadRows.map(r => ({
    timestamp:    r[0] || '',
    lead_id:      r[1] || '',
    full_name:    r[2] || '',
    phone:        normalizePhone(r[3]),
    raw_phone:    r[3] || '',
    email:        r[4] || '',
    company_name: r[5] || '',
    platform:     r[6] || '',
    campaign:     r[7] || '',
    ad_set:       r[8] || '',
    ad_name:      r[9] || '',
  })).filter(l => l.phone);
  console.log(`  ${leads.length} leads with phone numbers`);

  // Load called phones from Call Analysis (column B = phone)
  console.log('Loading called phones from Call Analysis...');
  const { data: callData } = await sheets.spreadsheets.values.get({
    spreadsheetId: process.env.GOOGLE_SHEETS_ID,
    range: 'Call Analysis!B2:B',
  });
  const calledPhones = new Set(
    (callData.values || []).flat().map(normalizePhone).filter(Boolean)
  );
  console.log(`  ${calledPhones.size} leads have been called`);

  // Find never-dialed leads
  const neverDialed = leads.filter(l => !calledPhones.has(l.phone));
  console.log(`  ${neverDialed.length} leads never called`);

  // Write to Never Dialed sheet
  const HEADERS = [
    'Lead Name', 'Phone', 'Email', 'Company', 'Platform', 'Campaign',
    'Ad Set', 'Ad Name', 'Lead ID', 'Timestamp',
  ];

  const rows = neverDialed.map(l => [
    l.full_name,
    l.raw_phone,
    l.email,
    l.company_name,
    l.platform,
    l.campaign,
    l.ad_set,
    l.ad_name,
    l.lead_id,
    l.timestamp,
  ]);

  console.log(`\nWriting ${rows.length} rows to "${NEVER_DIALED_SHEET}"...`);
  await sheets.spreadsheets.values.clear({
    spreadsheetId: process.env.GOOGLE_SHEETS_ID,
    range: `${NEVER_DIALED_SHEET}!A:J`,
  });
  await sheets.spreadsheets.values.update({
    spreadsheetId: process.env.GOOGLE_SHEETS_ID,
    range: `${NEVER_DIALED_SHEET}!A1`,
    valueInputOption: 'RAW',
    requestBody: { values: [HEADERS, ...rows] },
  });

  console.log('\n✓ Done.');
  console.log(`  ${rows.length} never-dialed leads written to "${NEVER_DIALED_SHEET}"`);
  console.log('\nSample (first 5):');
  rows.slice(0, 5).forEach((r, i) =>
    console.log(`  ${i + 1}. ${r[0]} | ${r[1]} | ${r[2]} | ${r[3]}`)
  );
}

run().catch(err => {
  console.error('\nFatal:', err.message);
  process.exit(1);
});
