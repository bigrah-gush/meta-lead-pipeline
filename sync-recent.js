/**
 * sync-recent.js
 *
 * Failsafe backfill: fetches leads from all forms for the last N hours,
 * finds any missing from Google Sheets, and runs them through the full
 * pipeline (Sheets + JustCall + n8n webhook). No Slack — this is silent
 * gap-filling, not real-time notification.
 *
 * Usage:
 *   node sync-recent.js           # default: last 4 hours
 *   node sync-recent.js --hours 24  # last 24 hours (recovery)
 */

require('dotenv').config();
const axios = require('axios');
const { google } = require('googleapis');
const { addToJustCall } = require('./handlers/justcall');
const { sendToWebhook } = require('./handlers/webhook');
const { addToSheets, updateSmsSentStatus } = require('./handlers/sheets');
const { sendWelcomeSms } = require('./handlers/justcall-sms');

const FORM_IDS = process.env.META_FORM_IDS.split(',');

const FORM_NAMES = {
  '1449187046631320': 'Lead + Savvycal',
  '906026261834302':  'Manufacturing',
  '1618694649166504': 'B2B | Leads',
  '1592360018634471': 'B2B - HI',
};

const hoursArg = process.argv.find(a => a.startsWith('--hours=') || a === '--hours');
let LOOKBACK_HOURS = 4;
if (hoursArg) {
  const idx = process.argv.indexOf('--hours');
  LOOKBACK_HOURS = idx !== -1
    ? parseInt(process.argv[idx + 1], 10)
    : parseInt(hoursArg.split('=')[1], 10);
}

// Add 30-min overlap to avoid edge-case gaps
const LOOKBACK_MS = (LOOKBACK_HOURS * 60 + 30) * 60 * 1000;

// ── Google Sheets ─────────────────────────────────────────────────────────────

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

async function getExistingLeadIds(sheets) {
  const { data } = await sheets.spreadsheets.values.get({
    spreadsheetId: process.env.GOOGLE_SHEETS_ID,
    range: 'leadform!B:B',
  });
  return new Set((data.values || []).slice(1).map(r => r[0]).filter(Boolean));
}

// ── Meta API ──────────────────────────────────────────────────────────────────

function normalizeLead(raw) {
  const lead = {
    id:            raw.id,
    created_time:  raw.created_time,
    form_id:       raw.form_id       || '',
    platform:      raw.platform      || '',
    campaign_name: raw.campaign_name || '',
    adset_name:    raw.adset_name    || '',
    ad_name:       raw.ad_name       || '',
  };
  for (const { name, values } of (raw.field_data || [])) {
    lead[name] = (values && values[0]) || '';
  }
  return lead;
}

async function fetchRecentLeadsFromForm(formId, sinceTimestamp) {
  const leads = [];
  let url = `https://graph.facebook.com/v19.0/${formId}/leads`;
  let params = {
    access_token: process.env.META_PAGE_ACCESS_TOKEN,
    fields: 'id,created_time,form_id,ad_id,ad_name,adset_id,adset_name,campaign_id,campaign_name,platform,field_data',
    limit: 100,
    filtering: JSON.stringify([
      { field: 'time_created', operator: 'GREATER_THAN', value: sinceTimestamp },
    ]),
  };

  while (url) {
    const { data } = await axios.get(url, { params });
    leads.push(...(data.data || []));
    url = (data.paging && data.paging.next) ? data.paging.next : null;
    params = {};
  }

  return leads;
}

// ── Main ──────────────────────────────────────────────────────────────────────

async function run() {
  const since = new Date(Date.now() - LOOKBACK_MS);
  const sinceTimestamp = Math.floor(since.getTime() / 1000);
  console.log(`[sync-recent] Checking last ${LOOKBACK_HOURS}h (since ${since.toISOString()})`);

  const sheets = await getSheets();
  const existingIds = await getExistingLeadIds(sheets);
  console.log(`[sync-recent] ${existingIds.size} leads already in sheet`);

  // Fetch from all forms
  let allRecent = [];
  for (const formId of FORM_IDS) {
    const raw = await fetchRecentLeadsFromForm(formId, sinceTimestamp);
    allRecent.push(...raw);
  }
  console.log(`[sync-recent] ${allRecent.length} leads found on Meta in window`);

  const newLeads = allRecent.map(normalizeLead).filter(l => !existingIds.has(l.id));
  console.log(`[sync-recent] ${newLeads.length} missing — processing now`);

  if (newLeads.length === 0) {
    console.log('[sync-recent] All caught up.');
    return;
  }

  // Process each lead: Sheets → SMS → JustCall + Webhook
  let added = 0;
  for (const lead of newLeads) {
    const label = `${lead.full_name || 'Unknown'} (${lead.id})`;

    // Add to sheet, get row number back
    let rowNumber = null;
    try {
      const result = await addToSheets(lead);
      rowNumber = result?.rowNumber || null;
      added++;
      console.log(`[sync-recent] Sheet row added: ${label} (row ${rowNumber})`);
    } catch (err) {
      console.error(`[sync-recent] Sheets failed for ${label}:`, err.message);
    }

    // Welcome SMS + update SMS Sent column
    try {
      await sendWelcomeSms(lead);
      console.log(`[sync-recent] Welcome SMS sent: ${label}`);
      await updateSmsSentStatus(rowNumber, 'Yes').catch(() => {});
    } catch (smsErr) {
      console.error(`[sync-recent] Welcome SMS failed for ${label}:`, smsErr.message);
      await updateSmsSentStatus(rowNumber, smsErr.message).catch(() => {});
    }

    try {
      await addToJustCall(lead);
      console.log(`[sync-recent] JustCall OK: ${label}`);
    } catch (err) {
      console.error(`[sync-recent] JustCall failed for ${label}:`, err.message);
    }

    try {
      await sendToWebhook(lead);
    } catch (err) {
      console.error(`[sync-recent] Webhook failed for ${label}:`, err.message);
    }
  }

  console.log(`[sync-recent] Done. ${added} leads recovered.`);
}

run().catch(err => {
  console.error('[sync-recent] Fatal:', err.message);
  process.exit(1);
});
