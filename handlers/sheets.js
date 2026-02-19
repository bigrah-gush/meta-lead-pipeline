const { google } = require('googleapis');

// Column order — matches HEADERS exactly
const HEADERS = [
  'Timestamp',
  'Lead ID',
  'Full Name',
  'Phone',
  'Email',
  'Company Name',
  'Platform',
  'Campaign',
  'Ad Set',
  'Ad Name',
];

function mapLeadToRow(lead) {
  return [
    lead.created_time  || '',
    `'${lead.id || ''}`,
    lead.full_name     || '',
    lead.phone         || '',
    lead.email         || '',
    lead.company_name  || '',
    lead.platform      || '',
    lead.campaign_name || '',
    lead.adset_name    || '',
    lead.ad_name       || '',
  ];
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

// Check if row 1 is empty and write headers if so
async function ensureHeaders(sheets) {
  const { data } = await sheets.spreadsheets.values.get({
    spreadsheetId: process.env.GOOGLE_SHEETS_ID,
    range: 'Sheet1!A1:J1',
  });

  const existingHeaders = data.values?.[0] || [];
  if (existingHeaders.length === 0) {
    await sheets.spreadsheets.values.update({
      spreadsheetId: process.env.GOOGLE_SHEETS_ID,
      range: 'Sheet1!A1:J1',
      valueInputOption: 'RAW',
      requestBody: { values: [HEADERS] },
    });
    console.log('Headers written to sheet');
  }
}

async function addToSheets(lead) {
  const sheets = await getSheets();
  await ensureHeaders(sheets);

  const row = mapLeadToRow(lead);

  await sheets.spreadsheets.values.append({
    spreadsheetId: process.env.GOOGLE_SHEETS_ID,
    range: 'Sheet1!A:J',
    valueInputOption: 'USER_ENTERED',
    insertDataOption: 'INSERT_ROWS',
    requestBody: { values: [row] },
  });

  console.log(`Sheet row added for lead ${lead.id}`);
}

module.exports = { addToSheets };
