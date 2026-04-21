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

// ── Google Sheets auth ──────────────────────────────────────────────────────
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

// ── Get Lead IDs already in the sheet (column B) ───────────────────────────
async function getExistingLeadIds(sheets) {
  const { data } = await sheets.spreadsheets.values.get({
    spreadsheetId: SHEET_ID,
    range: 'leadform!B:B',
  });
  const rows = data.values || [];
  // Skip header row, collect all lead IDs
  return new Set(rows.slice(1).map(r => r[0]).filter(Boolean));
}

// ── Normalize a raw Meta lead into a flat object ───────────────────────────
function normalizeLead(raw) {
  const lead = {
    id:            raw.id,
    created_time:  raw.created_time,
    form_id:       raw.form_id || '',
    platform:      raw.platform || '',
    campaign_name: raw.campaign_name || '',
    adset_name:    raw.adset_name || '',
    ad_name:       raw.ad_name || '',
  };
  for (const { name, values } of (raw.field_data || [])) {
    lead[name] = (values && values[0]) || '';
  }
  return lead;
}

// ── Map lead to sheet row ──────────────────────────────────────────────────
function toRow(lead) {
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
  ];
}

// ── Fetch all leads from a single form (handles pagination) ────────────────
async function fetchLeadsFromForm(formId) {
  const leads = [];
  let url = `https://graph.facebook.com/v19.0/${formId}/leads`;
  let params = {
    access_token: process.env.META_PAGE_ACCESS_TOKEN,
    fields: 'id,created_time,form_id,ad_id,ad_name,adset_id,adset_name,campaign_id,campaign_name,platform,field_data',
    limit: 100,
  };

  while (url) {
    const { data } = await axios.get(url, { params });
    leads.push(...(data.data || []));
    console.log(`  [${formId}] Fetched ${leads.length} leads so far...`);
    url = data.paging?.next || null;
    params = {};
  }

  return leads;
}

// ── Fetch all leads across all forms ───────────────────────────────────────
async function fetchAllMetaLeads() {
  const all = [];
  for (const formId of FORM_IDS) {
    console.log(`\nFetching from form ${formId}...`);
    const leads = await fetchLeadsFromForm(formId);
    all.push(...leads);
  }
  return all;
}

// ── Main ───────────────────────────────────────────────────────────────────
async function run() {
  console.log('Connecting to Google Sheets...');
  const sheets = await getSheets();

  console.log('Checking existing leads in sheet...');
  const existingIds = await getExistingLeadIds(sheets);
  console.log(`  ${existingIds.size} leads already in sheet`);

  console.log(`\nFetching all leads from ${FORM_IDS.length} forms...`);
  const rawLeads = await fetchAllMetaLeads();
  console.log(`  ${rawLeads.length} total leads on Meta\n`);

  const newLeads = rawLeads
    .map(normalizeLead)
    .filter(l => !existingIds.has(l.id));

  console.log(`${newLeads.length} new leads to add (${rawLeads.length - newLeads.length} already in sheet)\n`);

  if (newLeads.length === 0) {
    console.log('Sheet is already up to date.');
    return;
  }

  // Batch insert all new rows at once
  await sheets.spreadsheets.values.append({
    spreadsheetId: SHEET_ID,
    range: 'leadform!A:Q',
    valueInputOption: 'RAW',
    insertDataOption: 'INSERT_ROWS',
    requestBody: { values: newLeads.map(toRow) },
  });

  console.log(`✓ Added ${newLeads.length} leads to sheet:`);
  newLeads.forEach(l => console.log(`  - ${l.full_name || 'Unknown'} (${l.id})`));
}

run().catch(err => {
  console.error('Fatal error:', err.message);
  process.exit(1);
});
