require('dotenv').config();
const axios = require('axios');
const { google } = require('googleapis');

// Both Meta Ads campaigns
const META_CAMPAIGNS = ['3085672', '3085673'];

function getJCHeaders() {
  const auth = Buffer.from(
    `${process.env.JUSTCALL_API_KEY}:${process.env.JUSTCALL_API_SECRET}`
  ).toString('base64');
  return { Authorization: `Basic ${auth}`, Accept: 'application/json' };
}

function normalizePhone(p = '') {
  const digits = p.replace(/\D/g, '');
  // Canonicalize US numbers to 11 digits (prepend 1 if 10 digits)
  if (digits.length === 10) return '1' + digits;
  return digits;
}

async function getSheets() {
  const auth = new google.auth.GoogleAuth({
    keyFile: process.env.GOOGLE_SERVICE_ACCOUNT_KEY,
    scopes: ['https://www.googleapis.com/auth/spreadsheets'],
  });
  return google.sheets({ version: 'v4', auth });
}

// Fetch all dialed phones from sales dialer campaigns
async function fetchDialedPhonesFromCampaigns() {
  const phoneSet = new Set();

  for (const campaignId of META_CAMPAIGNS) {
    let page = 1;
    let total = null;
    let fetched = 0;

    while (true) {
      const { data } = await axios.get('https://api.justcall.io/v2.1/sales_dialer/calls', {
        headers: getJCHeaders(),
        params: { per_page: 100, page, campaign_id: campaignId },
      });

      if (total === null) total = data.total_count;
      const batch = data.data || [];
      if (batch.length === 0) break;

      for (const call of batch) {
        const phone = normalizePhone(call.contact_number || '');
        if (phone) phoneSet.add(phone);
      }

      fetched += batch.length;
      process.stdout.write(`\r  Campaign ${campaignId}: ${fetched}/${total} calls...`);

      if (!data.next_page_link || batch.length < 100) break;
      page++;
    }
    console.log(); // newline after progress
  }

  return phoneSet;
}

async function run() {
  console.log('Fetching calls from Meta Ads sales dialer campaigns...');
  const dialedPhones = await fetchDialedPhonesFromCampaigns();
  console.log(`  ${dialedPhones.size} unique phones dialed via Meta campaigns`);

  // Also load Raw Call Dump as secondary source
  console.log('Loading Raw Call Dump as secondary source...');
  const sheets2 = await getSheets();
  const rd = await sheets2.spreadsheets.values.get({
    spreadsheetId: process.env.GOOGLE_SHEETS_ID,
    range: 'Raw Call Dump!C:C',
  });
  let rawCount = 0;
  for (const row of (rd.data.values || []).slice(1)) {
    const p = normalizePhone(row[0] || '');
    if (p && !dialedPhones.has(p)) { dialedPhones.add(p); rawCount++; }
  }
  console.log(`  ${rawCount} additional phones from Raw Call Dump`);
  console.log(`  ${dialedPhones.size} total unique phones across all sources\n`);

  console.log('Loading Sheet1...');
  const sheets = await getSheets();
  const { data } = await sheets.spreadsheets.values.get({
    spreadsheetId: process.env.GOOGLE_SHEETS_ID,
    range: 'leadform!A1:L',
  });

  const [headers, ...rows] = data.values || [];

  // Upsert "In Dialer" column
  let col = headers.indexOf('In Dialer');
  if (col === -1) {
    headers.push('In Dialer');
    col = headers.length - 1;
  }

  let yesCount = 0;
  const updatedRows = rows.map(row => {
    const phone = normalizePhone(row[3] || '');
    const inDialer = phone && dialedPhones.has(phone) ? 'Yes' : 'No';
    if (inDialer === 'Yes') yesCount++;
    const updated = [...row];
    while (updated.length <= col) updated.push('');
    updated[col] = inDialer;
    return updated;
  });

  console.log('Writing "In Dialer" column to Sheet1...');
  await sheets.spreadsheets.values.update({
    spreadsheetId: process.env.GOOGLE_SHEETS_ID,
    range: 'leadform!A1',
    valueInputOption: 'RAW',
    requestBody: { values: [headers, ...updatedRows] },
  });

  console.log(`\n✓ Done.`);
  console.log(`  Yes (dialed via Meta campaign): ${yesCount}`);
  console.log(`  No  (never dialed):             ${rows.length - yesCount}`);
}

run().catch(err => {
  console.error('Fatal:', err.response?.data || err.message);
  process.exit(1);
});
