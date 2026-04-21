#!/usr/bin/env node
require('dotenv').config();

const fs = require('fs');
const path = require('path');
const { google } = require('googleapis');

const { hasBookedCallByEmail } = require('./handlers/gtm-booking');
const { bookConsultationCall, buildCandidateSlots, formatIst } = require('./handlers/calendar-booking');
const { sendBookingSms } = require('./handlers/justcall-sms');

const SHEET_RANGE = process.env.B2B_SHEET_RANGE || 'leadform!A:Q';
const STATE_FILE = path.resolve(
  __dirname,
  process.env.B2B_BOOKING_STATE_FILE || '.b2b-auto-book-state.json'
);
const TARGET_SEGMENT = 'businesses_(b2b)';
const DEFAULT_POLL_INTERVAL_SEC = 60;
const IST_OFFSET_MINUTES = 330;

const COL = {
  FULL_NAME: 7, // H
  EMAIL: 8, // I
  PHONE: 9, // J
  PHONE_NUMBER: 10, // K
  USER_PROVIDED_PHONE: 11, // L
  WHO_SELL_TO: 15, // P
};

function log(message) {
  console.log(`[auto-book-b2b] ${message}`);
}

function parseArgs(argv) {
  const options = {
    once: false,
    dryRun: false,
    selfTest: false,
    resetState: false,
    skipCalendarCheck: ['1', 'true', 'yes'].includes(
      String(process.env.B2B_DRY_RUN_SKIP_CALENDAR_CHECK || '').toLowerCase()
    ),
    intervalSec: Number(process.env.B2B_BOOKING_POLL_INTERVAL_SEC || DEFAULT_POLL_INTERVAL_SEC),
  };

  for (const arg of argv) {
    if (arg === '--once') options.once = true;
    else if (arg === '--dry-run') options.dryRun = true;
    else if (arg === '--self-test') options.selfTest = true;
    else if (arg === '--reset-state') options.resetState = true;
    else if (arg === '--skip-calendar-check') options.skipCalendarCheck = true;
    else if (arg.startsWith('--interval=')) {
      const value = Number(arg.split('=')[1]);
      if (Number.isFinite(value) && value > 0) options.intervalSec = value;
    }
  }

  return options;
}

function getSheetsClient() {
  const credentials = process.env.GOOGLE_SERVICE_ACCOUNT_JSON
    ? JSON.parse(process.env.GOOGLE_SERVICE_ACCOUNT_JSON)
    : undefined;

  const auth = new google.auth.GoogleAuth({
    credentials,
    keyFile: credentials ? undefined : process.env.GOOGLE_SERVICE_ACCOUNT_KEY,
    scopes: ['https://www.googleapis.com/auth/spreadsheets.readonly'],
  });

  return google.sheets({ version: 'v4', auth });
}

async function fetchSheetRows() {
  if (!process.env.GOOGLE_SHEETS_ID) {
    throw new Error('GOOGLE_SHEETS_ID is missing');
  }

  const sheets = getSheetsClient();
  const { data } = await sheets.spreadsheets.values.get({
    spreadsheetId: process.env.GOOGLE_SHEETS_ID,
    range: SHEET_RANGE,
  });

  return data.values || [];
}

function readState() {
  try {
    const raw = fs.readFileSync(STATE_FILE, 'utf8');
    const parsed = JSON.parse(raw);
    return {
      lastProcessedRow: Number(parsed.lastProcessedRow) || 1,
      updatedAt: parsed.updatedAt || null,
    };
  } catch (_err) {
    return { lastProcessedRow: 1, updatedAt: null };
  }
}

function writeState(state) {
  const payload = {
    lastProcessedRow: Number(state.lastProcessedRow) || 1,
    updatedAt: new Date().toISOString(),
  };
  fs.writeFileSync(STATE_FILE, JSON.stringify(payload, null, 2));
}

function resetState() {
  writeState({ lastProcessedRow: 1 });
}

function normalizeValue(value) {
  return String(value || '').trim().toLowerCase();
}

function isTargetSegment(segmentValue) {
  return normalizeValue(segmentValue) === TARGET_SEGMENT;
}

function isEmptyRow(row) {
  return !row || row.every((cell) => !String(cell || '').trim());
}

function pickPhone(row) {
  const values = [row[COL.PHONE], row[COL.PHONE_NUMBER], row[COL.USER_PROVIDED_PHONE]];
  return values.find((value) => String(value || '').trim()) || '';
}

function rowToLead(row, rowNumber) {
  return {
    rowNumber,
    full_name: String(row[COL.FULL_NAME] || '').trim(),
    email: String(row[COL.EMAIL] || '').trim(),
    phone: String(row[COL.PHONE] || '').trim(),
    phone_number: String(row[COL.PHONE_NUMBER] || '').trim(),
    user_provided_phone_number: String(row[COL.USER_PROVIDED_PHONE] || '').trim(),
    contact_number: String(pickPhone(row) || '').trim(),
    segment: String(row[COL.WHO_SELL_TO] || '').trim(),
  };
}

function formatSlotText(slot) {
  const endTime = new Intl.DateTimeFormat('en-IN', {
    timeZone: 'Asia/Kolkata',
    hour: '2-digit',
    minute: '2-digit',
    hour12: true,
  }).format(slot.end);
  return `${formatIst(slot.start)} to ${endTime} IST`;
}

function advanceState(state, rowNumber, dryRun) {
  state.lastProcessedRow = rowNumber;
  if (!dryRun) {
    writeState(state);
  }
}

async function processLeadRow(lead, { dryRun }) {
  const bookedAlready = await hasBookedCallByEmail(lead.email);
  if (bookedAlready) {
    return { status: 'already_booked' };
  }

  const booking = await bookConsultationCall(lead, { dryRun });
  const slotText = formatSlotText(booking.slot);
  const smsResult = await sendBookingSms(lead, { dryRun, slotText });

  return {
    status: 'booked',
    slotText,
    booking,
    smsResult,
  };
}

function buildDryRunSlotWithoutCalendar() {
  const slots = buildCandidateSlots({
    now: new Date(),
    durationMinutes: Number(process.env.B2B_SLOT_DURATION_MINUTES || 30),
    lookaheadDays: Number(process.env.B2B_SLOT_LOOKAHEAD_DAYS || 30),
    slotIntervalMinutes: Number(
      process.env.B2B_SLOT_INTERVAL_MINUTES || process.env.B2B_SLOT_DURATION_MINUTES || 30
    ),
  });
  return slots[0] || null;
}

async function runOnce({ dryRun = false, skipCalendarCheck = false } = {}) {
  const rows = await fetchSheetRows();
  const state = readState();
  const firstDataRow = 2;
  const totalRows = rows.length;

  const summary = {
    scanned: 0,
    skipped: 0,
    alreadyBooked: 0,
    booked: 0,
    failed: 0,
    lastProcessedRow: state.lastProcessedRow,
  };

  const startRow = Math.max(firstDataRow, state.lastProcessedRow + 1);
  if (startRow > totalRows) {
    log(`No new rows. lastProcessedRow=${state.lastProcessedRow}, totalRows=${totalRows}`);
    return summary;
  }

  for (let rowNumber = startRow; rowNumber <= totalRows; rowNumber += 1) {
    const row = rows[rowNumber - 1] || [];
    summary.scanned += 1;

    if (isEmptyRow(row)) {
      summary.skipped += 1;
      advanceState(state, rowNumber, dryRun);
      continue;
    }

    const lead = rowToLead(row, rowNumber);
    if (!isTargetSegment(lead.segment)) {
      summary.skipped += 1;
      advanceState(state, rowNumber, dryRun);
      continue;
    }

    if (!lead.email) {
      log(`Row ${rowNumber} skipped (target segment) because email is missing`);
      summary.skipped += 1;
      advanceState(state, rowNumber, dryRun);
      continue;
    }

    log(`Row ${rowNumber} processing ${lead.full_name || 'Unknown'} <${lead.email}>`);

    try {
      const result =
        dryRun && skipCalendarCheck
          ? await (async () => {
              const slot = buildDryRunSlotWithoutCalendar();
              if (!slot) {
                throw new Error('Unable to generate a candidate slot for dry-run');
              }
              const slotText = formatSlotText(slot);
              const smsResult = await sendBookingSms(lead, { dryRun: true, slotText });
              return {
                status: 'booked',
                slotText,
                booking: { dryRun: true, slot, calendarSkipped: true },
                smsResult,
              };
            })()
          : await processLeadRow(lead, { dryRun });
      if (result.status === 'already_booked') {
        summary.alreadyBooked += 1;
        log(`Row ${rowNumber} skipped: call already booked (found in Slack GTM channel)`);
      } else {
        summary.booked += 1;
        log(`Row ${rowNumber} booked for ${result.slotText}${dryRun ? ' [dry-run]' : ''}`);
      }

      advanceState(state, rowNumber, dryRun);
    } catch (err) {
      summary.failed += 1;
      log(`Row ${rowNumber} failed: ${err.message}`);
      break;
    }
  }

  summary.lastProcessedRow = dryRun ? state.lastProcessedRow : readState().lastProcessedRow;
  return summary;
}

function assert(condition, message) {
  if (!condition) throw new Error(message);
}

function runSelfTest() {
  assert(isTargetSegment('businesses_(b2b)'), 'segment equality failed');
  assert(isTargetSegment('  businesses_(b2b)  '), 'segment trimming failed');
  assert(!isTargetSegment('enterprises_(b2b)'), 'segment mismatch check failed');

  const row = [];
  row[COL.FULL_NAME] = 'Test Lead';
  row[COL.EMAIL] = 'test@example.com';
  row[COL.PHONE_NUMBER] = '919876543210';
  row[COL.WHO_SELL_TO] = 'businesses_(b2b)';

  const lead = rowToLead(row, 12);
  assert(lead.contact_number === '919876543210', 'phone pick failed');
  assert(lead.email === 'test@example.com', 'email mapping failed');

  const slots = buildCandidateSlots({
    now: new Date('2026-03-09T10:00:00.000Z'),
    durationMinutes: 30,
    lookaheadDays: 7,
  });
  assert(slots.length > 0, 'slot generation failed');

  const first = slots[0];
  const istShifted = new Date(first.start.getTime() + IST_OFFSET_MINUTES * 60 * 1000);
  const weekday = istShifted.getUTCDay();
  assert([1, 2, 3, 4, 5].includes(weekday), 'slot should be weekday in IST');
  assert(
    istShifted.getUTCHours() >= 19 && istShifted.getUTCHours() < 21,
    'slot should be between 7PM and 9PM IST'
  );

  log('Self-test passed');
}

async function runLoop(intervalSec, options) {
  let inFlight = false;

  const tick = async () => {
    if (inFlight) {
      log('Previous run still in progress, skipping this tick');
      return;
    }

    inFlight = true;
    try {
      const summary = await runOnce({
        dryRun: options.dryRun,
        skipCalendarCheck: options.skipCalendarCheck,
      });
      log(
        `Summary: scanned=${summary.scanned}, skipped=${summary.skipped}, alreadyBooked=${summary.alreadyBooked}, booked=${summary.booked}, failed=${summary.failed}, lastProcessedRow=${summary.lastProcessedRow}`
      );
    } catch (err) {
      log(`Tick failed: ${err.message}`);
    } finally {
      inFlight = false;
    }
  };

  await tick();
  setInterval(tick, intervalSec * 1000);
}

async function main() {
  const options = parseArgs(process.argv.slice(2));

  if (options.resetState) {
    resetState();
    log(`State reset at ${STATE_FILE}`);
    return;
  }

  if (options.selfTest) {
    runSelfTest();
    return;
  }

  if (options.once) {
    const summary = await runOnce({ dryRun: options.dryRun, skipCalendarCheck: options.skipCalendarCheck });
    log(JSON.stringify(summary));
    return;
  }

  log(
    `Starting watcher with interval=${options.intervalSec}s${options.dryRun ? ' [dry-run]' : ''}${
      options.skipCalendarCheck ? ' [skip-calendar-check]' : ''
    }. State file: ${STATE_FILE}`
  );
  await runLoop(options.intervalSec, options);
}

main().catch((err) => {
  console.error(`[auto-book-b2b] fatal: ${err.stack || err.message}`);
  process.exit(1);
});
