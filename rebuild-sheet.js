require('dotenv').config();
const axios = require('axios');
const { google } = require('googleapis');

const FORM_IDS = process.env.META_FORM_IDS.split(',');
const SHEET_ID = process.env.GOOGLE_SHEETS_ID;

const FORM_NAMES = {
  '1449187046631320': 'Lead + Savvycal',
  '906026261834302':  'Manufacturing',
  '1618694649166504': 'B2B | Leads',
  '1592360018634471': 'B2B - HI',
};

// All columns in final sheet order
const HEADER = [
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
  'User Provided Phone', // field: user_provided_phone_number (all forms)
  'Company Name',        // field: company_name (Lead+Savvycal, Manufacturing)
  'Website',             // field: website (B2B | Leads, B2B - HI)
  'Company Size',        // field: what_best_describes_your_company_size? (B2B | Leads)
  'Who They Sell To',    // field: who_do_you_primarily_sell_to? (B2B - HI)
  'Status',              // blank — for manual tracking
];

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

async function fetchLeadsFromForm(formId) {
  const leads = [];
  let url = `https://graph.facebook.com/v19.0/${formId}/leads`;
  let params = {
    access_token: process.env.META_PAGE_ACCESS_TOKEN,
    fields: 'id,created_time,form_id,ad_name,adset_name,campaign_name,platform,field_data',
    limit: 100,
  };

  while (url) {
    const { data } = await axios.get(url, { params });
    leads.push(...(data.data || []));
    process.stdout.write(`\r  [${FORM_NAMES[formId]}] ${leads.length} leads fetched...`);
    url = data.paging?.next || null;
    params = {};
  }
  console.log();
  return leads;
}

function normalizeLead(raw) {
  const fields = {};
  for (const { name, values } of (raw.field_data || [])) {
    fields[name] = (values && values[0]) || '';
  }

  return [
    raw.created_time                                    || '',
    raw.id                                              || '',
    FORM_NAMES[raw.form_id] || raw.form_id              || '',
    raw.campaign_name                                   || '',
    raw.adset_name                                      || '',
    raw.ad_name                                         || '',
    raw.platform                                        || '',
    fields['full_name']                                 || '',
    fields['email']                                     || '',
    fields['phone']                                     || '',
    fields['phone_number']                              || '',
    fields['user_provided_phone_number']                || '',
    fields['company_name']                              || '',
    fields['website']                                   || '',
    fields['what_best_describes_your_company_size?']    || '',
    fields['who_do_you_primarily_sell_to?']             || '',
    '', // Status — blank
  ];
}

async function run() {
  console.log('Connecting to Google Sheets...');
  const sheets = await getSheets();

  // 1. Fetch all leads
  console.log(`\nFetching all leads from ${FORM_IDS.length} forms...`);
  const allRaw = [];
  for (const formId of FORM_IDS) {
    const leads = await fetchLeadsFromForm(formId);
    allRaw.push(...leads);
  }
  console.log(`\nTotal leads fetched: ${allRaw.length}`);

  // 2. Sort oldest first (newest at the bottom)
  allRaw.sort((a, b) => new Date(a.created_time) - new Date(b.created_time));

  // 3. Build rows
  const rows = allRaw.map(normalizeLead);

  // 4. Clear the sheet completely
  console.log('\nClearing sheet...');
  await sheets.spreadsheets.values.clear({
    spreadsheetId: SHEET_ID,
    range: 'Sheet1',
  });

  // 5. Write header + all data
  console.log('Writing header + all leads...');
  await sheets.spreadsheets.values.update({
    spreadsheetId: SHEET_ID,
    range: 'Sheet1!A1',
    valueInputOption: 'RAW',
    requestBody: { values: [HEADER, ...rows] },
  });

  // 6. Bold the header row
  const { data: spreadsheet } = await sheets.spreadsheets.get({ spreadsheetId: SHEET_ID });
  const sheetId = spreadsheet.sheets[0].properties.sheetId;

  await sheets.spreadsheets.batchUpdate({
    spreadsheetId: SHEET_ID,
    requestBody: {
      requests: [
        // Bold header
        {
          repeatCell: {
            range: { sheetId, startRowIndex: 0, endRowIndex: 1 },
            cell: { userEnteredFormat: { textFormat: { bold: true } } },
            fields: 'userEnteredFormat.textFormat.bold',
          },
        },
        // Freeze header row
        {
          updateSheetProperties: {
            properties: { sheetId, gridProperties: { frozenRowCount: 1 } },
            fields: 'gridProperties.frozenRowCount',
          },
        },
      ],
    },
  });

  console.log(`\n✓ Sheet rebuilt with ${rows.length} leads across ${FORM_IDS.length} forms.`);
  console.log('\nBreakdown by form:');
  for (const [formId, name] of Object.entries(FORM_NAMES)) {
    const count = allRaw.filter(l => l.form_id === formId).length;
    console.log(`  ${name}: ${count} leads`);
  }
}

run().catch(err => {
  console.error('Fatal error:', err.message);
  process.exit(1);
});
