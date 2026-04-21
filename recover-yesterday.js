require('dotenv').config();
const axios = require('axios');
const https = require('https');
const { google } = require('googleapis');

const FORM_IDS = process.env.META_FORM_IDS.split(',');
const SHEET_ID = process.env.GOOGLE_SHEETS_ID;
const CAMPAIGN_ALL = '3085672';

// Yesterday = 2026-03-06 UTC
const YESTERDAY_START = new Date('2026-03-06T00:00:00Z');
const YESTERDAY_END   = new Date('2026-03-07T00:00:00Z');

// ── Google Sheets ────────────────────────────────────────────────────────────
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
    spreadsheetId: SHEET_ID,
    range: 'leadform!B:B',
  });
  return new Set((data.values || []).slice(1).map(r => r[0]).filter(Boolean));
}

function toRow(lead) {
  const phone = lead.phone || lead.phone_number || lead.user_provided_phone_number || '';
  return [
    lead.created_time  || '',
    lead.id            || '',
    lead.full_name     || '',
    phone,
    lead.secondary_phone_number || '',
    lead.email         || '',
    lead.website       || '',
    lead['what_best_describes_your_company_size?'] || '',
    lead.platform      || '',
    lead.campaign_name || '',
    lead.adset_name    || '',
    lead.ad_name       || '',
    '',
    lead.form_id       || '',
  ];
}

// ── Slack ────────────────────────────────────────────────────────────────────
const SLACK_CHANNEL = 'C0A1RMXTUJ3';

function postSlackMessage(payload) {
  return new Promise((resolve, reject) => {
    const body = JSON.stringify(payload);
    const req = https.request(
      {
        hostname: 'slack.com',
        path: '/api/chat.postMessage',
        method: 'POST',
        headers: {
          Authorization: `Bearer ${process.env.SLACK_BOT_TOKEN}`,
          'Content-Type': 'application/json',
        },
      },
      (res) => {
        let data = '';
        res.on('data', (d) => (data += d));
        res.on('end', () => {
          const parsed = JSON.parse(data);
          if (!parsed.ok) return reject(new Error(`Slack error: ${parsed.error}`));
          resolve(parsed);
        });
      }
    );
    req.on('error', reject);
    req.write(body);
    req.end();
  });
}

async function notifySlack(lead) {
  const time = new Date(lead.created_time).toLocaleString('en-GB', {
    day: '2-digit', month: 'short', year: 'numeric',
    hour: '2-digit', minute: '2-digit', timeZone: 'America/New_York',
  });
  const sheetsUrl = `https://docs.google.com/spreadsheets/d/${process.env.GOOGLE_SHEETS_ID}`;

  await postSlackMessage({
    channel: SLACK_CHANNEL,
    text: `New Lead Captured: ${lead.full_name || 'Unknown'}`,
    unfurl_links: false,
    unfurl_media: false,
    blocks: [
      { type: 'header', text: { type: 'plain_text', text: '🚀 New Lead Captured' } },
      {
        type: 'section',
        text: { type: 'mrkdwn', text: `A new prospect has entered the pipeline.\nLead has been added to the *Meta Ads → Sales Dialer* campaign.` },
      },
      { type: 'divider' },
      {
        type: 'section',
        fields: [
          { type: 'mrkdwn', text: `*Name*\n${lead.full_name || 'N/A'}` },
          { type: 'mrkdwn', text: `*Email*\n${lead.email || 'N/A'}` },
          { type: 'mrkdwn', text: `*Phone*\n${lead.phone || lead.phone_number || 'N/A'}` },
          { type: 'mrkdwn', text: `*Website*\n${lead.website || 'N/A'}` },
          { type: 'mrkdwn', text: `*Company Size*\n${lead['what_best_describes_your_company_size?'] || 'N/A'}` },
        ],
      },
      {
        type: 'section',
        text: { type: 'mrkdwn', text: `*Campaign / Ad*\n_${lead.ad_name || lead.campaign_name || 'N/A'}_` },
      },
      { type: 'divider' },
      {
        type: 'actions',
        elements: [
          { type: 'button', text: { type: 'plain_text', text: '📊 View All Leads' }, url: sheetsUrl, style: 'primary' },
        ],
      },
      {
        type: 'context',
        elements: [{ type: 'mrkdwn', text: `⚡ Recovered lead from yesterday  •  :calendar: ${time}` }],
      },
    ],
  });
}

// ── JustCall ─────────────────────────────────────────────────────────────────
function getJustCallHeaders() {
  const auth = Buffer.from(
    `${process.env.JUSTCALL_API_KEY}:${process.env.JUSTCALL_API_SECRET}`
  ).toString('base64');
  return {
    Authorization: `Basic ${auth}`,
    'Content-Type': 'application/json',
    Accept: 'application/json',
  };
}

async function addToDialer(lead) {
  const rawPhone = (lead.phone || lead.phone_number || lead.user_provided_phone_number || '').trim();
  if (!rawPhone) {
    console.log(`  ⚠ Skipping JustCall for ${lead.full_name} — no phone`);
    return;
  }
  const phoneNumber = rawPhone.startsWith('+') ? rawPhone : `+${rawPhone}`;
  try {
    await axios.post(
      'https://api.justcall.io/v2.1/sales_dialer/campaigns/contact',
      { campaign_id: CAMPAIGN_ALL, name: lead.full_name || '', email: lead.email || '', phone_number: phoneNumber },
      { headers: getJustCallHeaders() }
    );
    console.log(`  ✓ JustCall: ${lead.full_name} (${phoneNumber})`);
  } catch (err) {
    const msg = String(err?.response?.data?.message || '').toLowerCase();
    if (err?.response?.status === 400 && msg.includes('already exists in campaign')) {
      console.log(`  ~ JustCall: already in campaign — ${phoneNumber}`);
    } else {
      console.error(`  ✗ JustCall failed for ${phoneNumber}:`, err?.response?.data || err.message);
    }
  }
}

// ── Meta fetch ───────────────────────────────────────────────────────────────
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

async function fetchYesterdayLeadsFromForm(formId) {
  const leads = [];
  let url = `https://graph.facebook.com/v19.0/${formId}/leads`;
  let params = {
    access_token: process.env.META_PAGE_ACCESS_TOKEN,
    fields: 'id,created_time,form_id,ad_id,ad_name,adset_id,adset_name,campaign_id,campaign_name,platform,field_data',
    limit: 100,
    // filter_time_range only works for insights; we filter client-side
    filtering: JSON.stringify([{ field: 'time_created', operator: 'GREATER_THAN', value: Math.floor(YESTERDAY_START.getTime() / 1000) }]),
  };

  let page = 0;
  while (url) {
    const { data } = await axios.get(url, { params });
    const batch = data.data || [];

    // Filter to yesterday only; once we hit older leads we can stop
    let doneEarly = false;
    for (const lead of batch) {
      const t = new Date(lead.created_time);
      if (t >= YESTERDAY_START && t < YESTERDAY_END) {
        leads.push(lead);
      } else if (t < YESTERDAY_START) {
        doneEarly = true;
      }
    }

    page++;
    console.log(`  [${formId}] page ${page}: ${batch.length} fetched, ${leads.length} in range so far`);
    url = (doneEarly || !data.paging?.next) ? null : data.paging.next;
    params = {};
  }

  return leads;
}

// ── Main ─────────────────────────────────────────────────────────────────────
async function run() {
  console.log(`Recovering leads from ${YESTERDAY_START.toISOString()} → ${YESTERDAY_END.toISOString()}\n`);

  const sheets = await getSheets();
  const existingIds = await getExistingLeadIds(sheets);
  console.log(`${existingIds.size} leads already in sheet\n`);

  let allYesterday = [];
  for (const formId of FORM_IDS) {
    console.log(`Fetching form ${formId}...`);
    const leads = await fetchYesterdayLeadsFromForm(formId);
    console.log(`  → ${leads.length} leads yesterday\n`);
    allYesterday.push(...leads);
  }

  console.log(`Total yesterday leads from Meta: ${allYesterday.length}`);

  const newLeads = allYesterday.map(normalizeLead).filter(l => !existingIds.has(l.id));
  const alreadyInSheet = allYesterday.length - newLeads.length;
  console.log(`${newLeads.length} new (${alreadyInSheet} already in sheet)\n`);

  const allNormalized = allYesterday.map(normalizeLead);

  if (newLeads.length > 0) {
    // Add to sheet
    await sheets.spreadsheets.values.append({
      spreadsheetId: SHEET_ID,
      range: 'leadform!A:N',
      valueInputOption: 'RAW',
      insertDataOption: 'INSERT_ROWS',
      requestBody: { values: newLeads.map(toRow) },
    });
    console.log(`✓ Added ${newLeads.length} rows to sheet\n`);

    // Add to JustCall
    console.log('Adding to JustCall sales dialer campaign...');
    for (const lead of newLeads) {
      await addToDialer(lead);
    }
  } else {
    console.log('Sheet and JustCall already up to date.\n');
  }

  // Slack — notify for ALL yesterday leads
  console.log(`\nSending Slack notifications for ${allNormalized.length} leads...`);
  for (const lead of allNormalized) {
    try {
      await notifySlack(lead);
      console.log(`  ✓ Slack: ${lead.full_name}`);
    } catch (err) {
      console.error(`  ✗ Slack failed for ${lead.full_name}:`, err.message);
    }
  }

  console.log(`\nDone. ${allNormalized.length} Slack notifications sent, ${newLeads.length} new added to sheet/JustCall.`);
}

run().catch(err => {
  console.error('Fatal:', err.message);
  process.exit(1);
});
