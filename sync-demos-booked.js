#!/usr/bin/env node
require('dotenv').config();

const { google } = require('googleapis');
const { Client } = require('pg');

const SHEET_ID = process.env.GOOGLE_SHEETS_ID;
const SOURCE_RANGE = 'leadform!A:Q';
const DEST_SHEET_NAME = 'Demos Booked';

const DEMOS_HEADERS = [
  'Timestamp',
  'Lead ID',
  'Form Name',
  'Campaign Name',
  'Adset Name',
  'Ad Name',
  'Platform',
  'Full Name',
  'Email',
  'Phone',
  'Phone Number',
  'User Provided Phone',
  'Company Name',
  'Website',
  'Company Size',
  'Who They Sell To',
  'Status',
  // Demo booking fields
  'Demo Scheduled Date',
  'AE Name',
  'Show Status',
  'Source',
  'Match Type',
];

async function getSheetsClient() {
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

function normalizePhone(raw) {
  return String(raw || '').replace(/\D/g, '');
}

function normalizeEmail(raw) {
  return String(raw || '').trim().toLowerCase();
}

async function fetchLeadsFromSheet(sheets) {
  const { data } = await sheets.spreadsheets.values.get({
    spreadsheetId: SHEET_ID,
    range: SOURCE_RANGE,
  });
  const rows = data.values || [];
  if (rows.length < 2) return [];

  // Row 0 is headers, data starts at row 1
  return rows.slice(1).map((row, i) => ({
    rowIndex: i + 2,
    raw: row,
    timestamp: row[0] || '',
    leadId: row[1] || '',
    formName: row[2] || '',
    campaignName: row[3] || '',
    adsetName: row[4] || '',
    adName: row[5] || '',
    platform: row[6] || '',
    fullName: row[7] || '',
    email: normalizeEmail(row[8]),
    phone: normalizePhone(row[9]),
    phoneNumber: normalizePhone(row[10]),
    userProvidedPhone: normalizePhone(row[11]),
    companyName: row[12] || '',
    website: row[13] || '',
    companySize: row[14] || '',
    whoTheyTo: row[15] || '',
    status: row[16] || '',
  }));
}

async function fetchDemoBookings(pgClient) {
  const { rows } = await pgClient.query(`
    SELECT
      prospect_email,
      prospect_phone_number,
      prospect_first_name,
      prospect_company,
      source,
      demo_scheduled_date,
      ae_name,
      show_status
    FROM gist.gtm_inbound_demo_bookings
    WHERE prospect_email IS NOT NULL OR prospect_phone_number IS NOT NULL
  `);
  return rows;
}

async function ensureDestSheet(sheets) {
  const { data } = await sheets.spreadsheets.get({ spreadsheetId: SHEET_ID });
  const exists = data.sheets.some(
    (s) => s.properties.title === DEST_SHEET_NAME
  );

  if (!exists) {
    await sheets.spreadsheets.batchUpdate({
      spreadsheetId: SHEET_ID,
      requestBody: {
        requests: [{ addSheet: { properties: { title: DEST_SHEET_NAME } } }],
      },
    });
    console.log(`Created sheet: ${DEST_SHEET_NAME}`);
  }
}

async function writeToDestSheet(sheets, rows) {
  // Clear existing content
  await sheets.spreadsheets.values.clear({
    spreadsheetId: SHEET_ID,
    range: `${DEST_SHEET_NAME}!A:Z`,
  });

  const values = [DEMOS_HEADERS, ...rows];
  await sheets.spreadsheets.values.update({
    spreadsheetId: SHEET_ID,
    range: `${DEST_SHEET_NAME}!A1`,
    valueInputOption: 'RAW',
    requestBody: { values },
  });
}

async function main() {
  const sheets = await getSheetsClient();
  const pg = new Client({
    host: 'gw-rds-analytics.celzx4qnlkfp.us-east-1.rds.amazonaws.com',
    user: 'airbyte_user',
    password: 'airbyte_user_password',
    database: 'gw_prod',
    ssl: { rejectUnauthorized: false },
  });

  await pg.connect();

  try {
    console.log('Fetching leads from Sheet1...');
    const leads = await fetchLeadsFromSheet(sheets);
    console.log(`  ${leads.length} leads loaded`);

    console.log('Fetching demo bookings from Postgres...');
    const bookings = await fetchDemoBookings(pg);
    console.log(`  ${bookings.length} bookings loaded`);

    // Build lookup maps for fast matching
    const bookingByEmail = new Map();
    const bookingByPhone = new Map();

    for (const b of bookings) {
      const email = normalizeEmail(b.prospect_email);
      const phone = normalizePhone(b.prospect_phone_number);
      if (email) bookingByEmail.set(email, b);
      if (phone) bookingByPhone.set(phone, b);
    }

    // Match leads to bookings
    const matched = [];
    for (const lead of leads) {
      let booking = null;
      let matchType = '';

      if (lead.email && bookingByEmail.has(lead.email)) {
        booking = bookingByEmail.get(lead.email);
        matchType = 'email';
      } else {
        const phones = [lead.phone, lead.phoneNumber, lead.userProvidedPhone].filter(Boolean);
        for (const p of phones) {
          // Match last 10 digits to handle country code differences
          const last10 = p.slice(-10);
          for (const [key, val] of bookingByPhone) {
            if (key.slice(-10) === last10) {
              booking = val;
              matchType = 'phone';
              break;
            }
          }
          if (booking) break;
        }
      }

      if (booking) {
        matched.push([
          lead.timestamp,
          lead.leadId,
          lead.formName,
          lead.campaignName,
          lead.adsetName,
          lead.adName,
          lead.platform,
          lead.fullName,
          lead.email,
          lead.raw[9] || '',
          lead.raw[10] || '',
          lead.raw[11] || '',
          lead.companyName,
          lead.website,
          lead.companySize,
          lead.whoTheyTo,
          lead.status,
          booking.demo_scheduled_date ? String(booking.demo_scheduled_date).split('T')[0] : '',
          booking.ae_name || '',
          booking.show_status || '',
          booking.source || '',
          matchType,
        ]);
      }
    }

    console.log(`  ${matched.length} matches found`);

    console.log(`Ensuring "${DEST_SHEET_NAME}" sheet exists...`);
    await ensureDestSheet(sheets);

    console.log('Writing to sheet...');
    await writeToDestSheet(sheets, matched);

    console.log(`Done. ${matched.length} rows written to "${DEST_SHEET_NAME}".`);
  } finally {
    await pg.end();
  }
}

main().catch((err) => {
  console.error('Fatal:', err.message);
  process.exit(1);
});
