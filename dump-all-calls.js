require('dotenv').config();
const axios = require('axios');
const { google } = require('googleapis');

const JUSTCALL_URL = 'https://api.justcall.io/v2/calls';
const SHEET_NAME = 'Raw Call Dump';
const BATCH_SIZE = 500;
const SUCCESS_DELAY_MS = 500;
const RETRY_DELAY_MS = 15000;

const HEADERS = [
  'Call ID',
  'Call SID',
  'Contact Number',
  'Contact Name',
  'Contact Email',
  'JustCall Number',
  'Line Name',
  'Agent Name',
  'Agent Email',
  'Call Date',
  'Call Time',
  'Direction',
  'Type',
  'Disposition',
  'Notes',
  'Duration (sec)',
  'Conversation Time (sec)',
  'Recording URL',
  'Cost',
];

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function getJustCallAuthHeader() {
  return (
    'Basic ' +
    Buffer.from(
      `${process.env.JUSTCALL_API_KEY}:${process.env.JUSTCALL_API_SECRET}`
    ).toString('base64')
  );
}

function validateEnv() {
  const required = [
    'JUSTCALL_API_KEY',
    'JUSTCALL_API_SECRET',
    'GOOGLE_SERVICE_ACCOUNT_KEY',
    'GOOGLE_SHEETS_ID',
  ];

  const missing = required.filter((key) => !process.env[key]);
  if (missing.length > 0) {
    throw new Error(`Missing required env var(s): ${missing.join(', ')}`);
  }
}

async function fetchAllCalls() {
  const allCalls = [];
  const authHeader = getJustCallAuthHeader();
  let page = 1;
  let totalCountFromApi = null;

  while (true) {
    let response;

    try {
      response = await axios.get(JUSTCALL_URL, {
        headers: {
          Authorization: authHeader,
        },
        params: {
          per_page: 100,
          page,
        },
        validateStatus: (status) => (status >= 200 && status < 300) || status === 429,
      });
    } catch (error) {
      if (error.response && error.response.status === 429) {
        console.log(`Page ${page}: received 429, waiting 15 seconds then retrying same page...`);
        await sleep(RETRY_DELAY_MS);
        continue;
      }
      throw error;
    }

    if (response.status === 429) {
      console.log(`Page ${page}: received 429, waiting 15 seconds then retrying same page...`);
      await sleep(RETRY_DELAY_MS);
      continue;
    }

    const payload = response.data || {};
    const batch = Array.isArray(payload.data) ? payload.data : [];

    if (totalCountFromApi === null) {
      totalCountFromApi = payload.total_count ?? null;
    }

    allCalls.push(...batch);

    console.log(
      `Page ${page}: fetched ${allCalls.length} so far (total_count: ${payload.total_count ?? 'unknown'})`
    );

    const hasNextPage = Boolean(payload.next_page_link);

    if (batch.length === 0 || !hasNextPage) {
      break;
    }

    page += 1;
    await sleep(SUCCESS_DELAY_MS);
  }

  return {
    calls: allCalls,
    totalCountFromApi,
  };
}

async function getSheetsClient() {
  const auth = new google.auth.GoogleAuth({
    keyFile: process.env.GOOGLE_SERVICE_ACCOUNT_KEY,
    scopes: ['https://www.googleapis.com/auth/spreadsheets'],
  });

  return google.sheets({
    version: 'v4',
    auth,
  });
}

async function ensureSheetExists(sheets) {
  const spreadsheetId = process.env.GOOGLE_SHEETS_ID;

  const meta = await sheets.spreadsheets.get({ spreadsheetId });
  const exists = (meta.data.sheets || []).some(
    (sheet) => sheet.properties && sheet.properties.title === SHEET_NAME
  );

  if (exists) {
    return;
  }

  await sheets.spreadsheets.batchUpdate({
    spreadsheetId,
    requestBody: {
      requests: [
        {
          addSheet: {
            properties: {
              title: SHEET_NAME,
            },
          },
        },
      ],
    },
  });

  console.log(`Created sheet tab: ${SHEET_NAME}`);
}

async function clearSheet(sheets) {
  await sheets.spreadsheets.values.clear({
    spreadsheetId: process.env.GOOGLE_SHEETS_ID,
    range: `${SHEET_NAME}!A:S`,
  });
}

async function writeHeaders(sheets) {
  await sheets.spreadsheets.values.update({
    spreadsheetId: process.env.GOOGLE_SHEETS_ID,
    range: `${SHEET_NAME}!A1`,
    valueInputOption: 'RAW',
    requestBody: {
      values: [HEADERS],
    },
  });
}

function mapCallsToRows(calls) {
  return calls.map((call) => [
    call.id,
    call.call_sid,
    call.contact_number,
    call.contact_name,
    call.contact_email,
    call.justcall_number,
    call.justcall_line_name,
    call.agent_name,
    call.agent_email,
    call.call_date,
    call.call_time,
    call.call_info?.direction,
    call.call_info?.type,
    call.call_info?.disposition,
    call.call_info?.notes,
    call.call_duration?.total_duration,
    call.call_duration?.conversation_time,
    call.call_info?.recording,
    call.cost_incurred,
  ]);
}

async function appendRowsInBatches(sheets, rows) {
  if (rows.length === 0) {
    return 0;
  }

  let batchNumber = 0;

  for (let i = 0; i < rows.length; i += BATCH_SIZE) {
    batchNumber += 1;

    const batch = rows.slice(i, i + BATCH_SIZE);
    const startRow = i + 1;
    const endRow = i + batch.length;

    await sheets.spreadsheets.values.append({
      spreadsheetId: process.env.GOOGLE_SHEETS_ID,
      range: `${SHEET_NAME}!A2`,
      valueInputOption: 'RAW',
      insertDataOption: 'INSERT_ROWS',
      requestBody: {
        values: batch,
      },
    });

    console.log(`Wrote batch ${batchNumber}: rows ${startRow}-${endRow}`);
  }

  return rows.length;
}

async function main() {
  validateEnv();

  console.log('Fetching all calls from JustCall...');
  const { calls, totalCountFromApi } = await fetchAllCalls();
  console.log(`Finished fetching calls. Total fetched: ${calls.length}`);

  console.log('Preparing Google Sheets client...');
  const sheets = await getSheetsClient();

  console.log(`Ensuring sheet tab exists: ${SHEET_NAME}`);
  await ensureSheetExists(sheets);

  console.log('Clearing sheet range Raw Call Dump!A:S...');
  await clearSheet(sheets);

  console.log('Writing header row...');
  await writeHeaders(sheets);

  console.log('Mapping calls to rows...');
  const rows = mapCallsToRows(calls);

  console.log(`Writing ${rows.length} rows in batches of ${BATCH_SIZE}...`);
  const totalRowsWritten = await appendRowsInBatches(sheets, rows);

  const matches =
    typeof totalCountFromApi === 'number' && totalRowsWritten === totalCountFromApi;

  console.log(`Total rows written: ${totalRowsWritten}`);
  console.log(`Total count from API (first page total_count): ${totalCountFromApi}`);
  console.log(`Match status: ${matches ? 'MATCH' : 'MISMATCH'}`);
}

main().catch((error) => {
  if (error.response) {
    console.error('Fatal error:', error.response.status, error.response.data);
  } else {
    console.error('Fatal error:', error.message);
  }
  process.exit(1);
});
