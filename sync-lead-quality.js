/**
 * sync-lead-quality.js
 *
 * Reads the Status column (Q) from the leads sheet and sends CAPI events
 * back to Meta so the algorithm learns which leads are valuable.
 *
 * Status values recognised:
 *   "qualified" | "good"        → QualifiedLead event
 *   "converted"                 → Purchase event
 *   "bad" | "disqualified"      → skipped (no signal = negative signal)
 *
 * Adds two tracking columns to the sheet:
 *   R — CAPI Event sent (e.g. "QualifiedLead")
 *   S — CAPI Sent At   (ISO timestamp)
 *
 * Run manually or on a cron:
 *   node sync-lead-quality.js
 *
 * Required env vars:
 *   META_PIXEL_ID        — find in Meta Events Manager
 *   META_PAGE_ACCESS_TOKEN — existing token (needs ads_management scope)
 *   GOOGLE_SHEETS_ID
 *   GOOGLE_SERVICE_ACCOUNT_JSON | GOOGLE_SERVICE_ACCOUNT_KEY
 */

require('dotenv').config();
const crypto = require('crypto');
const axios  = require('axios');
const { google } = require('googleapis');

const PIXEL_ID   = process.env.META_PIXEL_ID;
const TOKEN      = process.env.META_PAGE_ACCESS_TOKEN;
const SHEET_ID   = process.env.GOOGLE_SHEETS_ID;
const CAPI_URL   = `https://graph.facebook.com/v19.0/${PIXEL_ID}/events`;

// Column indices (0-based) matching the rebuilt sheet
const COL = {
  TIMESTAMP:   0,  // A
  LEAD_ID:     1,  // B
  FORM_NAME:   2,  // C
  CAMPAIGN:    3,  // D
  ADSET:       4,  // E
  AD_NAME:     5,  // F
  PLATFORM:    6,  // G
  FULL_NAME:   7,  // H
  EMAIL:       8,  // I
  PHONE:       9,  // J
  PHONE_NUM:   10, // K
  USER_PHONE:  11, // L
  COMPANY:     12, // M
  WEBSITE:     13, // N
  CO_SIZE:     14, // O
  SELLS_TO:    15, // P
  STATUS:      16, // Q  ← user fills this in
  CAPI_EVENT:  17, // R  ← we write this
  CAPI_SENT:   18, // S  ← we write this
};

// Status → CAPI event name mapping
const STATUS_MAP = {
  qualified:    'QualifiedLead',
  good:         'QualifiedLead',
  converted:    'Purchase',
};

function sha256(value) {
  return crypto.createHash('sha256').update(value.trim().toLowerCase()).digest('hex');
}

function hashPhone(raw) {
  // Strip everything except digits and leading +
  const clean = raw.replace(/[^\d+]/g, '').replace(/^\+/, '');
  return sha256(clean);
}

function buildUserData(row) {
  const ud = {};
  const email = row[COL.EMAIL];
  const phone  = row[COL.PHONE] || row[COL.PHONE_NUM] || row[COL.USER_PHONE];

  if (email) ud.em = [sha256(email)];
  if (phone)  ud.ph = [hashPhone(phone)];

  return ud;
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

async function readSheet(sheets) {
  const { data } = await sheets.spreadsheets.values.get({
    spreadsheetId: SHEET_ID,
    range: 'Sheet1!A:S',
  });
  return data.values || [];
}

async function sendCapiEvent(eventName, row, leadId) {
  const eventTime = row[COL.TIMESTAMP]
    ? Math.floor(new Date(row[COL.TIMESTAMP]).getTime() / 1000)
    : Math.floor(Date.now() / 1000);

  const payload = {
    data: [{
      event_name:    eventName,
      event_time:    eventTime,
      event_id:      `${leadId}_${eventName.toLowerCase()}`,
      action_source: 'system_generated',
      user_data:     buildUserData(row),
      custom_data: {
        lead_id: leadId,
        form:    row[COL.FORM_NAME]   || '',
        ad:      row[COL.AD_NAME]     || '',
      },
    }],
  };

  await axios.post(CAPI_URL, payload, {
    params: { access_token: TOKEN },
  });
}

async function markRowSent(sheets, sheetRow, eventName) {
  // sheetRow is already the actual 1-based sheet row number (caller handles offset)
  const sentAt = new Date().toISOString();

  await sheets.spreadsheets.values.update({
    spreadsheetId: SHEET_ID,
    range:         `Sheet1!R${sheetRow}:S${sheetRow}`,
    valueInputOption: 'RAW',
    requestBody:   { values: [[eventName, sentAt]] },
  });
}

async function run() {
  if (!PIXEL_ID) {
    console.error('META_PIXEL_ID is not set in .env — add it and re-run.');
    process.exit(1);
  }

  console.log('Reading sheet...');
  const sheets = await getSheets();
  const rows   = await readSheet(sheets);

  if (rows.length < 2) {
    console.log('No data rows found.');
    return;
  }

  const header   = rows[0];
  const dataRows = rows.slice(1); // skip header row

  let sent = 0, skipped = 0, alreadyDone = 0, bad = 0;

  for (let i = 0; i < dataRows.length; i++) {
    const row    = dataRows[i];
    const leadId = row[COL.LEAD_ID];
    if (!leadId) continue;

    const status    = (row[COL.STATUS]     || '').trim().toLowerCase();
    const capiEvent = (row[COL.CAPI_EVENT] || '').trim();

    // Skip if already sent
    if (capiEvent) {
      alreadyDone++;
      continue;
    }

    // Skip if no status set yet
    if (!status) {
      skipped++;
      continue;
    }

    const eventName = STATUS_MAP[status];

    // Known negative status — log and skip
    if (!eventName) {
      if (['bad', 'disqualified'].includes(status)) {
        bad++;
      } else {
        console.warn(`  [${leadId}] Unknown status "${status}" — skipping`);
      }
      continue;
    }

    try {
      await sendCapiEvent(eventName, row, leadId);
      // i is 0-based in dataRows; sheet row = i + 2 (row 1 = header, row 2 = first data row)
      await markRowSent(sheets, i + 2, eventName);
      console.log(`  ✓ ${leadId} (${row[COL.FULL_NAME] || 'Unknown'}) → ${eventName}`);
      sent++;
    } catch (err) {
      const detail = err.response?.data?.error?.message || err.message;
      console.error(`  ✗ ${leadId} failed: ${detail}`);
    }
  }

  console.log(`
Done.
  Sent:        ${sent}
  Already sent: ${alreadyDone}
  No status:   ${skipped}
  Bad/skip:    ${bad}
`);
}

run().catch(err => {
  console.error('Fatal:', err.message);
  process.exit(1);
});
