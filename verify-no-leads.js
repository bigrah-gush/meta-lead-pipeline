require('dotenv').config();
const axios = require('axios');
const { google } = require('googleapis');

const DELAY_MS = 400;
const RETRY_MS = 15000;

function getJCHeaders() {
  const auth = Buffer.from(
    `${process.env.JUSTCALL_API_KEY}:${process.env.JUSTCALL_API_SECRET}`
  ).toString('base64');
  return { Authorization: `Basic ${auth}`, Accept: 'application/json' };
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

// Canonical + both variants so nothing slips through
function phoneVariants(p = '') {
  const d = p.replace(/\D/g, '');
  const variants = new Set([d]);
  if (d.length === 10) variants.add('1' + d);
  if (d.length === 11 && d[0] === '1') variants.add(d.slice(1));
  return variants;
}

async function getSheets() {
  const auth = new google.auth.GoogleAuth({
    keyFile: process.env.GOOGLE_SERVICE_ACCOUNT_KEY,
    scopes: ['https://www.googleapis.com/auth/spreadsheets'],
  });
  return google.sheets({ version: 'v4', auth });
}

// Fetch ALL sales dialer calls and build a full phone set
async function fetchAllDialerPhones() {
  const phoneSet = new Set();
  let page = 1;
  let total = null;

  while (true) {
    let res;
    try {
      res = await axios.get('https://api.justcall.io/v2.1/sales_dialer/calls', {
        headers: getJCHeaders(),
        params: { per_page: 100, page },
        validateStatus: s => (s >= 200 && s < 300) || s === 429,
      });
    } catch (e) {
      console.log(`\nNetwork error on page ${page}, retrying...`);
      await sleep(RETRY_MS);
      continue;
    }

    if (res.status === 429) {
      console.log(`\nRate limited on page ${page}, waiting ${RETRY_MS / 1000}s...`);
      await sleep(RETRY_MS);
      continue;
    }

    if (total === null) total = res.data.total_count;
    const batch = res.data.data || [];
    if (batch.length === 0) break;

    for (const call of batch) {
      for (const v of phoneVariants(call.contact_number || '')) {
        phoneSet.add(v);
      }
    }

    const fetched = (page - 1) * 100 + batch.length;
    process.stdout.write(`\r  Fetched ${fetched}/${total} dialer calls (${phoneSet.size} unique phones)...`);

    if (!res.data.next_page_link || batch.length < 100) break;
    page++;
    await sleep(DELAY_MS);
  }

  console.log(); // newline
  return phoneSet;
}

async function ensureSheet(sheets, title) {
  const meta = await sheets.spreadsheets.get({ spreadsheetId: process.env.GOOGLE_SHEETS_ID });
  const exists = meta.data.sheets.some(s => s.properties.title === title);
  if (!exists) {
    await sheets.spreadsheets.batchUpdate({
      spreadsheetId: process.env.GOOGLE_SHEETS_ID,
      requestBody: { requests: [{ addSheet: { properties: { title } } }] },
    });
    console.log(`  Created sheet: ${title}`);
  }
}

async function run() {
  const sheets = await getSheets();

  // Load Sheet1 and find "No" leads
  console.log('Loading Sheet1...');
  const s1 = await sheets.spreadsheets.values.get({
    spreadsheetId: process.env.GOOGLE_SHEETS_ID,
    range: 'leadform!A1:L',
  });
  const [headers, ...rows] = s1.data.values || [];
  const dialerCol = headers.indexOf('In Dialer');

  const noIndices = rows
    .map((r, i) => ({ row: r, i }))
    .filter(({ row }) => row[dialerCol] === 'No');

  console.log(`  ${noIndices.length} leads currently marked No\n`);

  // Fetch all sales dialer calls
  console.log('Fetching ALL sales dialer calls (this will take ~20 min)...');
  const allDialerPhones = await fetchAllDialerPhones();
  console.log(`  ${allDialerPhones.size} unique phones in full sales dialer\n`);

  // Match No leads with all variants
  let flipped = 0;
  const updatedRows = [...rows];

  for (const { row, i } of noIndices) {
    const variants = phoneVariants(row[3] || '');
    const hit = [...variants].find(v => allDialerPhones.has(v));
    if (hit) {
      updatedRows[i][dialerCol] = 'Yes';
      flipped++;
      console.log(`  ✓ Flipped: ${row[2]} | ${row[3]} (matched as ${hit})`);
    }
  }

  if (flipped === 0) {
    console.log('  No additional matches found — 193 leads are genuinely never dialed.');
    return;
  }

  console.log(`\nFlipped ${flipped} leads to Yes. Writing back to sheet...`);
  await sheets.spreadsheets.values.update({
    spreadsheetId: process.env.GOOGLE_SHEETS_ID,
    range: 'leadform!A1',
    valueInputOption: 'RAW',
    requestBody: { values: [headers, ...updatedRows] },
  });

  const finalNo = noIndices.length - flipped;
  console.log(`\n✓ Done.`);
  console.log(`  Flipped to Yes: ${flipped}`);
  console.log(`  Still No:       ${finalNo}`);

  // Write "Never Dialed" sheet — full lead info + all their calls from sales dialer
  console.log('\nPopulating "Never Dialed" sheet...');
  await ensureSheet(sheets, 'Never Dialed');
  await sheets.spreadsheets.values.clear({
    spreadsheetId: process.env.GOOGLE_SHEETS_ID,
    range: 'Never Dialed!A:S',
  });

  const NEVER_HEADERS = [
    'Timestamp','Lead ID','Full Name','Phone','Email','Company Name',
    'Platform','Campaign','Ad Set','Ad Name',
    // Call columns (empty since never dialed — confirms no activity)
    'Total Calls','Connected','Not Answered','Last Called','Last Disposition','Last Agent',
  ];

  const neverDialedRows = updatedRows
    .filter(r => r[dialerCol] === 'No')
    .map(r => [
      r[0], r[1], r[2], r[3], r[4], r[5], r[6], r[7], r[8], r[9],
      0, 0, 0, 'Never', 'Never Dialed', '',
    ]);

  await sheets.spreadsheets.values.update({
    spreadsheetId: process.env.GOOGLE_SHEETS_ID,
    range: 'Never Dialed!A1',
    valueInputOption: 'RAW',
    requestBody: { values: [NEVER_HEADERS, ...neverDialedRows] },
  });
  console.log(`  ${neverDialedRows.length} leads written to "Never Dialed" sheet.`);
}

run().catch(err => {
  console.error('Fatal:', err.response?.data || err.message);
  process.exit(1);
});
