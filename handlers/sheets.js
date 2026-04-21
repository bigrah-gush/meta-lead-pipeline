const { google } = require('googleapis');

const FORM_NAMES = {
  '1449187046631320': 'Lead + Savvycal',
  '906026261834302':  'Manufacturing',
  '1618694649166504': 'B2B | Leads',
  '1592360018634471': 'B2B - HI',
};

// Column order — matches HEADERS exactly (A through R)
const HEADERS = [
  'Timestamp',
  'Lead ID',
  'Form Name',
  'Campaign Name',
  'Adset Name',
  'Ad Name',
  'Platform',
  'Full Name',
  'Email',
  'Phone',               // field: phone (Lead+Savvycal, Manufacturing)
  'Phone Number',        // field: phone_number (B2B | Leads, B2B - HI)
  'User Provided Phone', // field: user_provided_phone_number (all)
  'Company Name',        // field: company_name (Lead+Savvycal, Manufacturing)
  'Website',             // field: website (B2B | Leads, B2B - HI)
  'Company Size',        // field: what_best_describes_your_company_size? (B2B | Leads)
  'Who They Sell To',    // field: who_do_you_primarily_sell_to? (B2B - HI)
  'Status',              // blank — for manual tracking
  'SMS Sent',            // populated after welcome SMS attempt
];

function mapLeadToRow(lead) {
  return [
    lead.created_time                                    || '',
    lead.id                                              || '',
    FORM_NAMES[lead.form_id] || lead.form_id             || '',
    lead.campaign_name                                   || '',
    lead.adset_name                                      || '',
    lead.ad_name                                         || '',
    lead.platform                                        || '',
    lead.full_name                                       || '',
    lead.email                                           || '',
    lead.phone                                           || '',
    lead.phone_number                                    || '',
    lead.user_provided_phone_number                      || '',
    lead.company_name                                    || '',
    lead.website                                         || '',
    lead['what_best_describes_your_company_size?']       || '',
    lead['who_do_you_primarily_sell_to?']                || '',
    '', // Status
    '', // SMS Sent
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
    range: 'leadform!A1:R1',
  });

  const existingHeaders = data.values?.[0] || [];
  if (existingHeaders.length === 0) {
    await sheets.spreadsheets.values.update({
      spreadsheetId: process.env.GOOGLE_SHEETS_ID,
      range: 'leadform!A1:R1',
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

  const { data } = await sheets.spreadsheets.values.append({
    spreadsheetId: process.env.GOOGLE_SHEETS_ID,
    range: 'leadform!A:R',
    valueInputOption: 'RAW',
    insertDataOption: 'INSERT_ROWS',
    requestBody: { values: [row] },
  });

  // Parse row number from updatedRange e.g. "Sheet1!A5:R5" → 5
  const updatedRange = data.updates?.updatedRange || '';
  const rowMatch = updatedRange.match(/:R?(\d+)$/);
  const rowNumber = rowMatch ? parseInt(rowMatch[1], 10) : null;

  console.log(`Sheet row added for lead ${lead.id} (row ${rowNumber})`);
  return { rowNumber };
}

async function updateSmsSentStatus(rowNumber, value) {
  if (!rowNumber) return;
  const sheets = await getSheets();
  await sheets.spreadsheets.values.update({
    spreadsheetId: process.env.GOOGLE_SHEETS_ID,
    range: `leadform!R${rowNumber}`,
    valueInputOption: 'RAW',
    requestBody: { values: [[value]] },
  });
  console.log(`SMS Sent column updated for row ${rowNumber}: ${value}`);
}

module.exports = { addToSheets, updateSmsSentStatus };
