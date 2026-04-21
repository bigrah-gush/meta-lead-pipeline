/**
 * Meta Report Builder — build-funnel-report.js
 * Fetches live data from Meta API + Google Sheets, builds full HTML report.
 * Usage: node build-funnel-report.js
 */

require('dotenv').config();
const { google } = require('googleapis');
const fs = require('fs');
const path = require('path');

const TOKEN = process.env.META_PAGE_ACCESS_TOKEN;
const AD_ACCOUNT = 'act_586966237066942';
const SHEET_ID = process.env.GOOGLE_SHEETS_ID;
const ADS_START_DATE = process.env.ADS_START_DATE || '2026-01-21';
const TODAY = new Date().toISOString().slice(0, 10);
const RETELL_KEY = process.env.RETELL_API_KEY;
const AI_CALL_CAMPAIGNS = ['23March | Cold Call Audience', '2 March | SayPrimer'];

// ── helpers ───────────────────────────────────────────────────────────────────

async function metaFetch(url) {
  const r = await fetch(url);
  const d = await r.json();
  if (d.error) throw new Error(`Meta API: ${d.error.message}`);
  return d;
}

function getAction(actions, type) {
  return parseInt((actions || []).find(a => a.action_type === type)?.value || 0);
}

function getCPA(list, type) {
  return parseFloat((list || []).find(a => a.action_type === type)?.value || 0);
}

function fmt(n, decimals = 2) {
  return n ? `$${Number(n).toLocaleString('en-US', { minimumFractionDigits: decimals, maximumFractionDigits: decimals })}` : '—';
}

// ── RETELL: fetch all outbound AI calls ──────────────────────────────────────

async function fetchRetellCalls() {
  if (!RETELL_KEY) { console.log('  RETELL_API_KEY not set, skipping AI calls'); return []; }
  const all = [];
  let paginationKey = null;
  do {
    const body = { limit: 1000, sort_order: 'ascending', filter_criteria: { direction: ['outbound'] } };
    if (paginationKey) body.pagination_key = paginationKey;
    const r = await fetch('https://api.retellai.com/v2/list-calls', {
      method: 'POST',
      headers: { Authorization: `Bearer ${RETELL_KEY}`, 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    const data = await r.json();
    if (!Array.isArray(data)) break;
    all.push(...data);
    paginationKey = data.length === 1000 ? data[data.length - 1].call_id : null;
  } while (paginationKey);
  return all;
}

function normalizePhone(p = '') { return p.replace(/\D/g, ''); }

function buildAiCallsData(retellCalls, qualData) {
  const phoneMap = qualData._leadPhoneMap || {};

  // Count total unique leads in target campaigns (by phone)
  const targetPhones = new Set();
  for (const [ph, lead] of Object.entries(phoneMap)) {
    if (AI_CALL_CAMPAIGNS.includes(lead.campaign)) targetPhones.add(ph);
  }

  // Match Retell calls to leads in target campaigns
  const callsByPhone = {};
  const matchedPhones = new Set();

  for (const call of retellCalls) {
    const ph = normalizePhone(call.to_number || '');
    if (!ph) continue;
    const lead = phoneMap[ph];
    if (!lead) continue;
    if (!AI_CALL_CAMPAIGNS.includes(lead.campaign)) continue;
    matchedPhones.add(ph);
    if (!callsByPhone[ph]) callsByPhone[ph] = [];
    callsByPhone[ph].push(call);
  }

  // Build flat call rows, sorted by timestamp (newest first)
  const allCalls = [];
  for (const [ph, calls] of Object.entries(callsByPhone)) {
    const lead = phoneMap[ph];
    const sorted = calls.sort((a, b) => a.start_timestamp - b.start_timestamp);
    sorted.forEach((call, i) => {
      const ana = call.call_analysis || {};
      const isVoicemail = !!ana.in_voicemail;
      const isConnected = call.call_status === 'ended' && !isVoicemail;
      const isSuccessful = !!ana.call_successful;
      allCalls.push({
        date: call.start_timestamp ? new Date(call.start_timestamp).toISOString() : '',
        dateShort: call.start_timestamp ? new Date(call.start_timestamp).toISOString().slice(0, 10) : '',
        name: lead.name || '—',
        company: lead.company || '—',
        website: lead.website || '',
        email: lead.email || '',
        campaign: lead.campaign,
        ad: lead.ad || '—',
        phone: call.to_number || ph,
        attempt: i + 1,
        callId: call.call_id,
        status: call.call_status || '',
        outcome: call.disconnection_reason || '',
        voicemail: isVoicemail,
        connected: isConnected,
        successful: isSuccessful,
        duration: Math.round((call.duration_ms || 0) / 1000),
        sentiment: ana.user_sentiment || '',
        summary: (ana.call_summary || '').replace(/"/g, '&quot;').replace(/\n/g, ' '),
        transcript: (call.transcript || '').replace(/"/g, '&quot;').replace(/\n/g, '\\n'),
        recordingUrl: call.recording_url || '',
      });
    });
  }

  // Sort newest first
  allCalls.sort((a, b) => (b.date || '').localeCompare(a.date || ''));

  // Find leads NOT called — use phone map directly
  const notCalledLeads = [];
  const seenNotCalled = new Set();
  for (const [ph, lead] of Object.entries(phoneMap)) {
    if (!AI_CALL_CAMPAIGNS.includes(lead.campaign)) continue;
    if (matchedPhones.has(ph)) continue;
    const key = (lead.name + '|' + lead.company).toLowerCase();
    if (seenNotCalled.has(key)) continue;
    seenNotCalled.add(key);
    notCalledLeads.push({
      name: lead.name || '—', company: lead.company || '—', website: lead.website || '',
      campaign: lead.campaign, ad: lead.ad || '—', leadDate: lead.leadDate || '—',
    });
  }
  const leadsNotCalled = notCalledLeads;

  return { calls: allCalls, leadsNotCalled, totalLeads: targetPhones.size, leadsCalled: matchedPhones.size };
}

// ── REMINDER AGENT: fetch per-person call data from Google Sheet ─────────────
// Sheet "Reminder Agent" has headers at row 1, data from row 2.
// One row per call; we group by phone to produce one entry per person.

async function fetchReminderCalls(sheets) {
  let values = [];
  try {
    const { data } = await sheets.spreadsheets.values.get({
      spreadsheetId: SHEET_ID,
      range: 'Reminder Agent!A2:R',
    });
    values = data.values || [];
  } catch (e) {
    console.log('  Reminder Agent sheet not found, skipping');
    return [];
  }

  // Index: A=0 Name, B=1 Company, C=2 Phone, D=3 ApptDate, E=4 ApptTime,
  //        F=5 CallsMade, G=6 CallDate, H=7 Duration, I=8 Status,
  //        J=9 Successful, K=10 Sentiment, L=11 Summary, M=12 Transcript,
  //        N=13 RecordingURL, O=14 ShowStatus, P=15 DemoDate, Q=16 AE, R=17 Confirmed

  const byPhone = {};
  for (const r of values) {
    const phone = (r[2] || '').trim();
    if (!phone) continue;
    if (!byPhone[phone]) byPhone[phone] = [];
    byPhone[phone].push(r);
  }

  const people = [];
  for (const [phone, calls] of Object.entries(byPhone)) {
    const first  = calls[0];
    const connected = calls.filter(c => (c[8] || '').startsWith('Connected'));

    // A person "confirmed" only if a CONNECTED call (not voicemail/reschedule) has Confirmed = Yes
    const hasYes = connected.some(c => c[17] === 'Yes');
    const hasNo  = connected.some(c => c[17] === 'No');
    const confirmedInCall = hasYes ? 'Yes' : hasNo ? 'No' : '';

    const lastConn = connected[connected.length - 1] || null;

    people.push({
      name:            (first[0] || '').replace(/"/g, '&quot;'),
      company:         (first[1] || '').replace(/"/g, '&quot;'),
      phone,
      apptDate:        first[3] || '',
      apptTime:        first[4] || '',
      callsMade:       calls.length,
      isConnected:     connected.length > 0,
      lastCallDate:    calls[calls.length - 1]?.[6] || '',
      lastDuration:    lastConn?.[7] || '',
      sentiment:       lastConn?.[10] || '',
      lastSummary:     (lastConn?.[11] || '').replace(/"/g, '&quot;'),
      lastTranscript:  (lastConn?.[12] || '').slice(0, 2000).replace(/"/g, '&quot;').replace(/\n/g, '\\n'),
      lastRecording:   lastConn?.[13] || '',
      confirmedInCall,
      showStatus:      first[14] || '',
      demoDate:        first[15] || '',
      ae:              first[16] || '',
      // Strip summaries/transcripts from allCalls to keep payload lean
      allCalls:        calls.map(c => ({
        date:      c[6]  || '',
        duration:  c[7]  || '',
        status:    c[8]  || '',
        sentiment: c[10] || '',
        recording: c[13] || '',
        confirmed: c[17] || '',
      })),
    });
  }

  // Attach raw call count so the page can show 327 (calls) vs 311 (people)
  people._totalCallRows = values.length;
  people._connectedCallRows = values.filter(r => (r[8] || '').startsWith('Connected')).length;
  return people;
}

// ── META: all campaigns ───────────────────────────────────────────────────────

async function getAllCampaigns() {
  const rows = [];
  let url = `https://graph.facebook.com/v21.0/${AD_ACCOUNT}/campaigns` +
    `?fields=id,name,status,effective_status&limit=500&access_token=${TOKEN}`;
  while (url) {
    const d = await metaFetch(url);
    rows.push(...(d.data || []));
    url = d.paging?.next || null;
  }
  return rows;
}

// ── META: account-level daily spend ──────────────────────────────────────────

async function getAccountDailySpend() {
  const rows = [];
  let url = `https://graph.facebook.com/v21.0/${AD_ACCOUNT}/insights` +
    `?fields=spend&time_range={"since":"${ADS_START_DATE}","until":"${TODAY}"}` +
    `&time_increment=1&level=account&limit=500&access_token=${TOKEN}`;
  while (url) {
    const d = await metaFetch(url);
    rows.push(...(d.data || []));
    url = d.paging?.next || null;
  }
  return rows.map(r => ({ date: r.date_start, spend: parseFloat(r.spend || 0) }));
}

// ── META: campaign insights ───────────────────────────────────────────────────

async function getCampaignInsights(campId) {
  const fields = 'spend,impressions,reach,cpm,cpc,ctr,actions,cost_per_action_type,frequency';
  const d = await metaFetch(
    `https://graph.facebook.com/v21.0/${campId}/insights` +
    `?fields=${fields}` +
    `&time_range={"since":"${ADS_START_DATE}","until":"${TODAY}"}` +
    `&level=campaign&access_token=${TOKEN}`
  );
  return d.data?.[0] || null;
}

// ── META: ad-level insights ───────────────────────────────────────────────────

async function getAdInsights(campId) {
  const fields = 'ad_name,adset_name,spend,impressions,actions,cost_per_action_type';
  const d = await metaFetch(
    `https://graph.facebook.com/v21.0/${campId}/insights` +
    `?fields=${fields}` +
    `&time_range={"since":"${ADS_START_DATE}","until":"${TODAY}"}` +
    `&level=ad&access_token=${TOKEN}`
  );
  return d.data || [];
}

// ── META: daily spend per campaign ────────────────────────────────────────────

async function getCampaignDailyInsights(campId) {
  const d = await metaFetch(
    `https://graph.facebook.com/v21.0/${campId}/insights` +
    `?fields=spend,actions` +
    `&time_range={"since":"${ADS_START_DATE}","until":"${TODAY}"}` +
    `&time_increment=1` +
    `&limit=500` +
    `&level=campaign&access_token=${TOKEN}`
  );
  return (d.data || []).map(row => ({
    date:  row.date_start,
    spend: parseFloat(row.spend || 0),
    leads: getAction(row.actions, 'lead'),
  }));
}

// ── SHEETS: fetch a tab ───────────────────────────────────────────────────────

async function getSheetData(sheets, range) {
  const r = await sheets.spreadsheets.values.get({ spreadsheetId: SHEET_ID, range });
  return r.data.values || [];
}

// ── SHEETS: qualification data ────────────────────────────────────────────────

async function getQualData(sheets, campaignNames) {
  const rows = await getSheetData(sheets, 'leadform!A1:T5000');
  const h = rows[0];
  const data = rows.slice(1);
  const campCol       = h.indexOf('Campaign Name');
  const adCol         = h.indexOf('Ad Name');
  const statusCol     = h.indexOf('Status');
  const demoCol       = h.indexOf('Demo Booked');
  const showUpCol     = h.indexOf('Show up');
  const subscribedCol = h.indexOf('Subscribed at');
  const nameCol       = h.indexOf('Full Name') !== -1 ? h.indexOf('Full Name') : h.indexOf('Name');
  const companyCol    = h.indexOf('Company') !== -1 ? h.indexOf('Company') : h.indexOf('company_name');
  const websiteCol    = h.indexOf('Website') !== -1 ? h.indexOf('Website') : h.indexOf('website');
  const adsetCol      = h.indexOf('Adset Name') !== -1 ? h.indexOf('Adset Name') : h.indexOf('Ad Set');
  const sourceCol     = h.indexOf('Platform') !== -1 ? h.indexOf('Platform') : h.indexOf('Source');
  const leadDateCol   = h.indexOf('Created') !== -1 ? h.indexOf('Created') : h.indexOf('Lead Date') !== -1 ? h.indexOf('Lead Date') : h.indexOf('Timestamp');
  const demoDateCol   = h.indexOf('Demo Date') !== -1 ? h.indexOf('Demo Date') : h.indexOf('Call Date');
  const aeCol         = h.indexOf('AE') !== -1 ? h.indexOf('AE') : h.indexOf('Account Executive');

  const result = {};
  const allLeads = [];
  const bookedLeads = [];

  for (const row of data) {
    const camp = row[campCol];
    if (!campaignNames.includes(camp)) continue;
    const ad     = row[adCol] || 'Unknown';
    const status = (row[statusCol] || '').toLowerCase().trim();
    const demo   = (row[demoCol]   || '').toLowerCase().trim();
    const show   = (row[showUpCol] || '').toLowerCase().trim();
    const subscribed = (row[subscribedCol] || '').trim();
    const amount = subscribed ? parseFloat(subscribed.replace(/[^0-9.]/g, '')) || 0 : 0;
    const adset  = adsetCol >= 0 ? (row[adsetCol] || 'Unknown') : 'Unknown';

    if (!result[camp]) result[camp] = { total: 0, qualified: 0, bad: 0, bookedCalls: 0, shows: 0, closes: 0, totalAmount: 0, byAd: {}, byAdset: {} };
    if (!result[camp].byAd[ad]) result[camp].byAd[ad] = { total: 0, qualified: 0, bad: 0 };
    if (!result[camp].byAdset[adset]) result[camp].byAdset[adset] = { total: 0, qualified: 0, bad: 0, bookedCalls: 0, shows: 0, closes: 0, totalAmount: 0, byAd: {} };
    if (!result[camp].byAdset[adset].byAd[ad]) result[camp].byAdset[adset].byAd[ad] = { total: 0, qualified: 0, bad: 0, bookedCalls: 0, shows: 0, closes: 0, totalAmount: 0 };

    result[camp].total++;
    result[camp].byAd[ad].total++;
    result[camp].byAdset[adset].total++;
    result[camp].byAdset[adset].byAd[ad].total++;

    if (status === 'qualified') {
      result[camp].qualified++;
      result[camp].byAd[ad].qualified++;
      result[camp].byAdset[adset].qualified++;
      result[camp].byAdset[adset].byAd[ad].qualified++;
    } else if (status === 'bad') {
      result[camp].bad++;
      result[camp].byAd[ad].bad++;
      result[camp].byAdset[adset].bad++;
      result[camp].byAdset[adset].byAd[ad].bad++;
    }

    const isDemo = ['yes', 'true', '1', 'y'].includes(demo);
    const isShow = ['yes', 'true', '1', 'y'].includes(show);

    if (isDemo) {
      result[camp].bookedCalls++;
      result[camp].byAdset[adset].bookedCalls++;
      result[camp].byAdset[adset].byAd[ad].bookedCalls++;
    }
    if (isShow) {
      result[camp].shows++;
      result[camp].byAdset[adset].shows++;
      result[camp].byAdset[adset].byAd[ad].shows++;
    }
    if (subscribed) {
      result[camp].closes++;
      result[camp].totalAmount += amount;
      result[camp].byAdset[adset].closes++;
      result[camp].byAdset[adset].totalAmount += amount;
      result[camp].byAdset[adset].byAd[ad].closes++;
      result[camp].byAdset[adset].byAd[ad].totalAmount += amount;
    }

    allLeads.push({
      campaign: camp, ad, adset,
      isQual:  status === 'qualified',
      isBad:   status === 'bad',
      isDemo,
      isShow,
      amount,
      leadDate:    leadDateCol >= 0 ? (row[leadDateCol] || '') : '',
      demoDate:    demoDateCol >= 0 ? (row[demoDateCol] || '') : '',
      name:        nameCol >= 0    ? (row[nameCol]    || '—') : '—',
      company:     companyCol >= 0 ? (row[companyCol] || '—') : '—',
      website:     websiteCol >= 0 ? (row[websiteCol] || '') : '',
      status:      (row[statusCol] || '').trim(),
      subscribedAt: subscribed || '',
      ae:          aeCol >= 0 ? (row[aeCol] || '—') : '—',
    });

    if (isDemo) {
      const showUp = isShow;
      const closed = !!subscribed;
      let filterStatus = 'no_show';
      if (closed)  filterStatus = 'closed';
      else if (showUp) filterStatus = 'show_up';
      bookedLeads.push({
        name:        row[nameCol] || '—',
        company:     row[companyCol] || '—',
        website:     row[websiteCol] || '—',
        campaign:    camp,
        adset:       adsetCol >= 0 ? (row[adsetCol] || '—') : '—',
        ad,
        source:      sourceCol >= 0 ? (row[sourceCol] || '—') : '—',
        leadDate:    leadDateCol >= 0 ? (row[leadDateCol] || '—') : '—',
        demoDate:    demoDateCol >= 0 ? (row[demoDateCol] || '—') : '—',
        ae:          aeCol >= 0 ? (row[aeCol] || '—') : '—',
        showUp, closed,
        subscribedAt: subscribed || '',
        amount,
        filterStatus,
      });
    }
  }

  result._bookedLeads = bookedLeads;
  result._allLeads    = allLeads;

  // Build phone → lead map (used by AI Calls tab)
  const phoneCol1 = h.indexOf('Phone');
  const phoneCol2 = h.indexOf('Phone Number');
  const phoneCol3 = h.indexOf('User Provided Phone');
  const emailCol  = h.indexOf('Email') !== -1 ? h.indexOf('Email') : h.indexOf('email');
  const leadPhoneMap = {};
  for (const row of data) {
    const camp = row[campCol];
    if (!campaignNames.includes(camp)) continue;
    const phones = [row[phoneCol1], row[phoneCol2], row[phoneCol3]]
      .map(p => (p || '').replace(/\D/g, '')).filter(Boolean);
    const lead = {
      name:     nameCol >= 0 ? (row[nameCol] || '') : '',
      company:  companyCol >= 0 ? (row[companyCol] || '') : '',
      campaign: camp || '',
      ad:       row[adCol] || '',
      email:    emailCol >= 0 ? (row[emailCol] || '') : '',
      website:  websiteCol >= 0 ? (row[websiteCol] || '') : '',
      leadDate: leadDateCol >= 0 ? (row[leadDateCol] || '') : '',
    };
    for (const ph of phones) {
      if (!leadPhoneMap[ph]) leadPhoneMap[ph] = lead;
    }
  }
  result._leadPhoneMap = leadPhoneMap;
  return result;
}

// ── BUILD FUNNEL DATA ─────────────────────────────────────────────────────────

function buildFunnelData(camps, qualData) {
  const result = {};
  for (const [id, camp] of Object.entries(camps)) {
    const qual    = qualData[camp.name] || qualData[camp.name.toLowerCase()] || { total: 0, qualified: 0, bad: 0 };
    const qualKey = qualData[camp.name] ? camp.name : camp.name.toLowerCase();
    const ins     = camp.insights;
    const spend   = parseFloat(ins?.spend || 0);
    const leads   = ins ? getAction(ins.actions, 'lead') : (qual.total || 0);
    const qualified  = qual.qualified;
    const bookedCalls = camp.bookedCalls || 0;
    const shows   = camp.shows || 0;
    const closes  = (qualData[qualKey] || {}).closes || 0;
    const totalAmount = (qualData[qualKey] || {}).totalAmount || 0;

    result[id] = {
      name:        camp.name,
      spend:       fmt(spend, 0),
      spendRaw:    spend,
      impressions: (ins?.impressions || 0).toLocaleString(),
      impressionsRaw: parseInt(ins?.impressions || 0),
      reach:       (ins?.reach || 0).toLocaleString(),
      reachRaw:    parseInt(ins?.reach || 0),
      cpm:         fmt(parseFloat(ins?.cpm || 0)),
      cpmRaw:      parseFloat(ins?.cpm || 0),
      ctr:         parseFloat(ins?.ctr || 0).toFixed(2) + '%',
      ctrRaw:      parseFloat(ins?.ctr || 0),
      frequency:   parseFloat(ins?.frequency || 0).toFixed(2),
      frequencyRaw: parseFloat(ins?.frequency || 0),
      leads,
      cpl:         fmt(getCPA(ins?.cost_per_action_type, 'lead')),
      cplRaw:      getCPA(ins?.cost_per_action_type, 'lead'),
      qualified,
      bad:         qual.bad,
      qualRate:    leads > 0 ? Math.round((qualified / leads) * 100) : 0,
      bookedCalls,
      shows,
      closes,
      totalAmount,
      arr:         totalAmount * 12,
      costQual:    qualified > 0  ? fmt(spend / qualified, 0) : '—',
      costBook:    bookedCalls > 0 ? fmt(spend / bookedCalls, 0) : '—',
      costShow:    shows > 0      ? fmt(spend / shows, 0) : '—',
      costClose:   closes > 0     ? fmt(spend / closes, 0) : '—',
      pctQual:     leads > 0 ? Math.round((qualified / leads) * 100) : 0,
      pctBooked:   qualified > 0 ? Math.round((bookedCalls / qualified) * 100) : 0,
      pctShow:     bookedCalls > 0 ? Math.round((shows / bookedCalls) * 100) : 0,
      cplMeta:     fmt(getCPA(ins?.cost_per_action_type, 'lead')),
      ads:         camp.ads || [],
      qualByAd:    qual.byAd || {},
      qualByAdset: qual.byAdset || {},
      isActive:    camp.isActive !== false,
    };
  }
  return result;
}

// ── BUILD COMBINED TOTALS ─────────────────────────────────────────────────────

function buildCombined(funnels) {
  const vals = Object.values(funnels);
  const spend  = vals.reduce((s, c) => s + c.spendRaw, 0);
  const leads  = vals.reduce((s, c) => s + c.leads, 0);
  const qual   = vals.reduce((s, c) => s + c.qualified, 0);
  const bad    = vals.reduce((s, c) => s + c.bad, 0);
  const booked = vals.reduce((s, c) => s + c.bookedCalls, 0);
  const shows  = vals.reduce((s, c) => s + c.shows, 0);
  const closes = vals.reduce((s, c) => s + c.closes, 0);
  const totalAmount = vals.reduce((s, c) => s + c.totalAmount, 0);
  // Weighted averages for CPM/CTR/freq
  const totalImp = vals.reduce((s, c) => s + c.impressionsRaw, 0);
  const totalReach = vals.reduce((s, c) => s + c.reachRaw, 0);
  const wCPM  = spend > 0 ? vals.reduce((s, c) => s + c.cpmRaw * c.spendRaw, 0) / spend : 0;
  const wCTR  = spend > 0 ? vals.reduce((s, c) => s + c.ctrRaw * c.spendRaw, 0) / spend : 0;
  const wFreq = totalImp > 0 ? vals.reduce((s, c) => s + c.frequencyRaw * c.impressionsRaw, 0) / totalImp : 0;
  return {
    spend:       fmt(spend, 0),
    spendRaw:    spend,
    leads,
    cpl:         fmt(leads > 0 ? spend / leads : 0),
    cplRaw:      leads > 0 ? spend / leads : 0,
    qualified:   qual,
    bad,
    qualRate:    leads > 0 ? Math.round((qual / leads) * 100) : 0,
    bookedCalls: booked,
    shows,
    closes,
    totalAmount,
    costQual:    qual > 0   ? fmt(spend / qual, 0)   : '—',
    costBook:    booked > 0 ? fmt(spend / booked, 0) : '—',
    costShow:    shows > 0  ? fmt(spend / shows, 0)  : '—',
    costClose:   closes > 0 ? fmt(spend / closes, 0) : '—',
    pctQual:     leads > 0  ? Math.round((qual / leads) * 100) : 0,
    pctBooked:   qual > 0   ? Math.round((booked / qual) * 100) : 0,
    pctShow:     booked > 0 ? Math.round((shows / booked) * 100) : 0,
    closePct:    shows > 0  ? Math.round((closes / shows) * 100) : 0,
    junkRate:    leads > 0  ? (100 - Math.round((qual / leads) * 100)) : 0,
    arrFmt:      fmt(totalAmount * 12, 0),
    mrrFmt:      fmt(totalAmount, 0),
    cpmFmt:      wCPM > 0  ? `$${wCPM.toFixed(2)}` : '—',
    ctrFmt:      wCTR > 0  ? `${wCTR.toFixed(2)}%` : '—',
    freqFmt:     wFreq > 0 ? wFreq.toFixed(2) : '—',
    impressions: totalImp.toLocaleString(),
    reach:       totalReach.toLocaleString(),
    cplMeta:     fmt(leads > 0 ? spend / leads : 0),
  };
}

function buildCombinedActive(funnels) {
  return buildCombined(
    Object.fromEntries(Object.entries(funnels).filter(([, v]) => v.isActive))
  );
}

// ── HTML BUILDER ──────────────────────────────────────────────────────────────

function buildHtml(funnels, combined, combinedActive, reportDate, bookedLeads, allLeads = [], dailySpend = {}, accountDailySpend = [], aiCallsData = {}, reminderPeople = []) {
  // Escape </script> so embedded JSON never closes the script block prematurely
  function safeJson(v) { return JSON.stringify(v).replace(/<\/script/gi, '<\\/script'); }

  const campKeys = Object.keys(funnels);

  // ── Pre-read / Summary computations ──────────────────────────────────────────

  // Weekly AI call pickup rates (group by ISO week Monday)
  const weeklyPickup = (() => {
    const weeks = {};
    for (const c of (aiCallsData.calls || [])) {
      if (!c.date) continue;
      const d   = new Date(c.date);
      const dow = d.getUTCDay(); // 0=Sun
      const mon = new Date(d);
      mon.setUTCDate(d.getUTCDate() + (dow === 0 ? -6 : 1 - dow));
      const wk = mon.toISOString().slice(0, 10);
      if (!weeks[wk]) weeks[wk] = { total: 0, connected: 0 };
      weeks[wk].total++;
      if (c.connected) weeks[wk].connected++;
    }
    return Object.entries(weeks)
      .sort(([a], [b]) => a.localeCompare(b))
      .map(([wk, s]) => ({ week: wk, total: s.total, connected: s.connected, pct: s.total > 0 ? Math.round(s.connected / s.total * 100) : 0 }))
      .filter(w => w.total >= 5);
  })();

  const lastWeek        = weeklyPickup[weeklyPickup.length - 1] || { pct: 0, total: 0, week: '' };
  const peakWeek        = weeklyPickup.reduce((b, w) => w.pct > b.pct ? w : b, { pct: 0, week: '' });
  const currentPickupPct = lastWeek.pct;

  // AI call totals (summary tab — prefixed sm_ to avoid collision with AI Calls tab vars)
  const smAiCallsAll  = aiCallsData.calls || [];
  const smAiCallsSent = aiCallsData.leadsCalled || 0;
  const smAiConnected = smAiCallsAll.filter(c => c.connected).length;
  const smAiPickupPct = smAiCallsAll.length > 0 ? Math.round(smAiConnected / smAiCallsAll.length * 100) : 0;

  // Reminder agent summary stats (sm_ prefix)
  const smRcTotal       = reminderPeople.length;
  const smRcConnected   = reminderPeople.filter(p => p.isConnected).length;
  const smRcConnPct     = smRcTotal > 0 ? Math.round(smRcConnected / smRcTotal * 100) : 0;
  const smRcShowed      = reminderPeople.filter(p => p.showStatus === 'Showed').length;
  const smRcNoShow      = reminderPeople.filter(p => p.showStatus === 'No-Show').length;
  const smRcShowRate    = (smRcShowed + smRcNoShow) > 0 ? Math.round(smRcShowed / (smRcShowed + smRcNoShow) * 100) : 0;
  const smRcWithOutcome = reminderPeople.filter(p => p.isConnected && (p.showStatus === 'Showed' || p.showStatus === 'No-Show'));
  const smRcYesGrp      = smRcWithOutcome.filter(p => p.confirmedInCall === 'Yes');
  const smRcNoGrp       = smRcWithOutcome.filter(p => p.confirmedInCall === 'No');
  const smRcYesSR       = smRcYesGrp.length > 0 ? Math.round(smRcYesGrp.filter(p => p.showStatus === 'Showed').length / smRcYesGrp.length * 100) : 0;
  const smRcNoSR        = smRcNoGrp.length  > 0 ? Math.round(smRcNoGrp.filter(p => p.showStatus === 'Showed').length / smRcNoGrp.length  * 100) : 0;
  const smRcUplift      = smRcYesSR - smRcNoSR;

  // Weekly bar chart HTML (baked at build time)
  const wkMaxPct   = Math.max(...weeklyPickup.map(w => w.pct), 1);
  const wkChartBars = weeklyPickup.map((w, i) => {
    const isLast  = i === weeklyPickup.length - 1;
    const color   = w.pct >= 25 ? '#16a34a' : w.pct >= 15 ? '#d97706' : '#dc2626';
    const hPct    = Math.round((w.pct / wkMaxPct) * 72);
    const label   = w.week.slice(5); // MM-DD
    return `<div style="display:flex;flex-direction:column;align-items:center;flex:1;min-width:0;gap:2px" title="Week of ${w.week}: ${w.connected}/${w.total} connected">
      <div style="font-size:10px;font-weight:700;color:${color}">${w.pct}%</div>
      <div style="width:100%;height:72px;display:flex;flex-direction:column;justify-content:flex-end">
        <div style="background:${color};height:${hPct}px;width:100%;border-radius:3px 3px 0 0;${isLast ? 'opacity:.65' : ''}"></div>
      </div>
      <div style="font-size:10px;color:#6b7280;white-space:nowrap">${label}</div>
      <div style="font-size:10px;color:#9ca3af">${w.total}</div>
    </div>`;
  }).join('');

  // Funnel bars (baked at build time)
  const funnelSteps = [
    { label: 'Meta Leads', n: combined.leads,       color: '#2563eb', sub: null },
    { label: 'Qualified',  n: combined.qualified,   color: '#7c3aed', prevN: combined.leads },
    { label: 'Booked',     n: combined.bookedCalls, color: '#0891b2', prevN: combined.qualified },
    { label: 'Showed',     n: combined.shows,       color: '#059669', prevN: combined.bookedCalls },
    { label: 'Closed',     n: combined.closes,      color: '#16a34a', prevN: combined.shows },
  ];
  const funnelMax = funnelSteps[0].n || 1;
  const summaryFunnelHtml = funnelSteps.map(s => {
    const barPct = Math.round((s.n / funnelMax) * 100);
    const convPct = s.prevN > 0 ? Math.round(s.n / s.prevN * 100) + '%' : null;
    return `<div style="margin-bottom:10px">
      <div style="display:flex;justify-content:space-between;align-items:baseline;margin-bottom:3px">
        <span style="font-size:12px;font-weight:500;color:#374151">${s.label}</span>
        <span>
          <span style="font-size:14px;font-weight:700;color:#111827">${s.n.toLocaleString()}</span>
          ${convPct ? `<span style="font-size:11px;color:#9ca3af;margin-left:5px">${convPct}</span>` : ''}
        </span>
      </div>
      <div style="height:7px;background:#f3f4f6;border-radius:4px;overflow:hidden">
        <div style="height:100%;width:${barPct}%;background:${s.color};border-radius:4px"></div>
      </div>
    </div>`;
  }).join('');

  // ── Per-campaign ad rows: join Meta spend data + Google Sheet qual data ──────
  function buildCampAdData(d) {
    const rows = [];
    for (const [adset, asData] of Object.entries(d.qualByAdset || {})) {
      for (const [adName, adData] of Object.entries(asData.byAd || {})) {
        const metaAd = d.ads.find(a => a.name === adName && a.adset === adset);
        rows.push({
          ad: adName, adset,
          spendRaw:    metaAd?.spendRaw || 0,
          metaLeads:   metaAd?.leads || 0,
          total:       adData.total || 0,
          qualified:   adData.qualified || 0,
          bad:         adData.bad || 0,
          bookedCalls: adData.bookedCalls || 0,
          shows:       adData.shows || 0,
          closes:      adData.closes || 0,
          totalAmount: adData.totalAmount || 0,
        });
      }
    }
    // Include any Meta ads with spend that have no sheet data
    for (const metaAd of d.ads) {
      if (!rows.some(r => r.ad === metaAd.name && r.adset === metaAd.adset)) {
        rows.push({ ad: metaAd.name, adset: metaAd.adset, spendRaw: metaAd.spendRaw, metaLeads: metaAd.leads, total: 0, qualified: 0, bad: 0, bookedCalls: 0, shows: 0, closes: 0, totalAmount: 0 });
      }
    }
    return rows.sort((a, b) => b.spendRaw - a.spendRaw);
  }

  function fmtNum(n) { return n ? `$${Number(n).toLocaleString('en-US', { maximumFractionDigits: 0 })}` : '—'; }
  function fmtN(n)   { return n > 0 ? String(n) : '—'; }

  function adTableRows(d) {
    const rows = buildCampAdData(d);
    return rows.map(r => {
      const qualRate = r.total > 0 ? Math.round(r.qualified / r.total * 100) : 0;
      const qCls = qualRate >= 50 ? 'bg' : qualRate >= 25 ? 'by' : 'br';
      const costQual = r.qualified > 0 && r.spendRaw > 0 ? fmtNum(r.spendRaw / r.qualified) : '—';
      const costDemo = r.bookedCalls > 0 && r.spendRaw > 0 ? fmtNum(r.spendRaw / r.bookedCalls) : '—';
      const cpl = r.metaLeads > 0 && r.spendRaw > 0 ? `$${(r.spendRaw / r.metaLeads).toFixed(2)}` : '—';
      const rev = r.totalAmount > 0 ? `$${(r.totalAmount * 12).toLocaleString('en-US', {maximumFractionDigits:0})}/yr` : '—';
      const isCut = r.total >= 3 && r.qualified === 0 && r.bad >= 2;
      return `<tr>
        <td><div style="font-size:13px;font-weight:500;max-width:200px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${r.ad}">${r.ad}${isCut ? ' <span class="bd bg" style="font-size:10px">Cut</span>' : ''}</div><div style="font-size:11px;color:#9ca3af;margin-top:2px;max-width:200px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${r.adset}">${r.adset}</div></td>
        <td>${r.spendRaw > 0 ? fmtNum(r.spendRaw) : '—'}</td>
        <td>${fmtN(r.metaLeads)}</td>
        <td>${cpl}</td>
        <td>${fmtN(r.total)}</td>
        <td>${fmtN(r.qualified)}</td>
        <td>${r.total > 0 ? `<span class="bd ${qCls}">${qualRate}%</span>` : '—'}</td>
        <td>${costQual}</td>
        <td>${fmtN(r.bookedCalls)}</td>
        <td>${costDemo}</td>
        <td>${fmtN(r.shows)}</td>
        <td>${fmtN(r.closes)}</td>
        <td>${rev}</td>
      </tr>`;
    }).join('');
  }

  function adTableTotal(d) {
    const rows = buildCampAdData(d);
    const t = rows.reduce((acc, r) => ({
      spendRaw: acc.spendRaw + r.spendRaw, metaLeads: acc.metaLeads + r.metaLeads,
      total: acc.total + r.total, qualified: acc.qualified + r.qualified,
      bookedCalls: acc.bookedCalls + r.bookedCalls, shows: acc.shows + r.shows,
      closes: acc.closes + r.closes, totalAmount: acc.totalAmount + r.totalAmount,
    }), { spendRaw:0, metaLeads:0, total:0, qualified:0, bookedCalls:0, shows:0, closes:0, totalAmount:0 });
    const qr = t.total > 0 ? Math.round(t.qualified / t.total * 100) : 0;
    const qCls = qr >= 50 ? 'bg' : qr >= 25 ? 'by' : 'br';
    return `<tfoot><tr>
      <td style="font-weight:600">Total</td>
      <td style="font-weight:600">${t.spendRaw > 0 ? fmtNum(t.spendRaw) : '—'}</td>
      <td style="font-weight:600">${fmtN(t.metaLeads)}</td>
      <td>${t.metaLeads > 0 && t.spendRaw > 0 ? `$${(t.spendRaw/t.metaLeads).toFixed(2)}` : '—'}</td>
      <td style="font-weight:600">${fmtN(t.total)}</td>
      <td style="font-weight:600">${fmtN(t.qualified)}</td>
      <td><span class="bd ${qCls}">${qr}%</span></td>
      <td>${t.qualified > 0 && t.spendRaw > 0 ? fmtNum(t.spendRaw/t.qualified) : '—'}</td>
      <td style="font-weight:600">${fmtN(t.bookedCalls)}</td>
      <td>${t.bookedCalls > 0 && t.spendRaw > 0 ? fmtNum(t.spendRaw/t.bookedCalls) : '—'}</td>
      <td style="font-weight:600">${fmtN(t.shows)}</td>
      <td style="font-weight:600">${fmtN(t.closes)}</td>
      <td>${t.totalAmount > 0 ? `$${(t.totalAmount*12).toLocaleString('en-US',{maximumFractionDigits:0})}/yr` : '—'}</td>
    </tr></tfoot>`;
  }

  // ── Campaign tabs for Performance ────────────────────────────────────────────
  const perfTabBtns = campKeys.map((k, i) =>
    `<button class="ctab${i === 0 ? ' active' : ''}" onclick="switchPerf('${k}',this)">${funnels[k].name}${!funnels[k].isActive ? ' <span style="font-size:10px;opacity:.5">(paused)</span>' : ''}</button>`
  ).join('');

  const perfPanels = campKeys.map((k, i) => {
    const d = funnels[k];
    const freqWarn = d.frequencyRaw > 2.2 ? ' ⚠️' : '';
    const cpmWarn  = d.cpmRaw > 130 ? ' ⚠️' : '';
    return `<div class="cpanel${i === 0 ? ' active' : ''}" id="perf-${k}">
      <div class="perf-stats">
        <div class="pstat"><span class="pstat-label">Spend</span><span class="pstat-val">${d.spend}</span></div>
        <div class="pstat"><span class="pstat-label">CPM</span><span class="pstat-val">${d.cpm}${cpmWarn}</span></div>
        <div class="pstat"><span class="pstat-label">CTR</span><span class="pstat-val">${d.ctr}</span></div>
        <div class="pstat"><span class="pstat-label">Frequency</span><span class="pstat-val">${d.frequency}${freqWarn}</span></div>
        <div class="pstat"><span class="pstat-label">Impressions</span><span class="pstat-val">${d.impressions}</span></div>
        <div class="pstat"><span class="pstat-label">Meta Leads</span><span class="pstat-val">${d.leads}</span></div>
        <div class="pstat"><span class="pstat-label">CPL</span><span class="pstat-val">${d.cpl}</span></div>
        <div class="pstat"><span class="pstat-label">Qual Rate</span><span class="pstat-val">${d.qualRate}%</span></div>
      </div>
      <div class="table-container">
        <table>
          <thead><tr>
            <th>Ad · Ad Set</th>
            <th class="sortable" onclick="sortPerf('${k}',0,this)">Spend</th>
            <th class="sortable" onclick="sortPerf('${k}',1,this)">Meta Leads</th>
            <th>CPL</th>
            <th class="sortable" onclick="sortPerf('${k}',2,this)">Sheet Leads</th>
            <th class="sortable" onclick="sortPerf('${k}',3,this)">Qualified</th>
            <th class="sortable" onclick="sortPerf('${k}',4,this)">Qual%</th>
            <th>Cost/Qual</th>
            <th class="sortable" onclick="sortPerf('${k}',5,this)">Booked</th>
            <th>Cost/Demo</th>
            <th class="sortable" onclick="sortPerf('${k}',6,this)">Shows</th>
            <th class="sortable" onclick="sortPerf('${k}',7,this)">Closes</th>
            <th>Revenue</th>
          </tr></thead>
          <tbody id="pbody-${k}">${adTableRows(d)}</tbody>
          ${adTableTotal(d)}
        </table>
      </div>
    </div>`;
  }).join('');

  // ── All leads table rows ──────────────────────────────────────────────────────
  const leadsRows = allLeads.map(l => {
    const date    = l.leadDate ? l.leadDate.slice(0, 10) : '—';
    const sCls    = l.isQual ? 'bg' : l.isBad ? 'br' : 'bn';
    const sLabel  = l.isQual ? 'Qualified' : l.isBad ? 'Bad' : 'Unreviewed';
    const demo    = l.isDemo ? '<span style="color:#16a34a;font-weight:600">✓</span>' : '<span style="color:#d1d5db">—</span>';
    const show    = l.isShow ? '<span style="color:#16a34a;font-weight:600">✓</span>' : l.isDemo ? '<span style="color:#d97706;font-size:11px">No</span>' : '<span style="color:#d1d5db">—</span>';
    const rev     = l.amount > 0 ? `<span class="bd bg">$${l.amount}/mo</span>` : '<span style="color:#d1d5db">—</span>';
    const websiteLink = l.website ? ` <a href="${l.website.startsWith('http') ? l.website : 'https://' + l.website}" target="_blank" style="color:#2563eb;font-size:10px;text-decoration:none">${l.website.replace(/^https?:\/\//,'').split('/')[0]}</a>` : '';
    return `<tr
      data-search="${(l.name + ' ' + l.company).toLowerCase()}"
      data-campaign="${l.campaign}"
      data-adset="${l.adset}"
      data-ad="${l.ad}"
      data-status="${l.isQual ? 'qualified' : l.isBad ? 'bad' : 'unreviewed'}"
      data-demo="${l.isDemo ? 'yes' : 'no'}"
      data-show="${l.isShow ? 'yes' : l.isDemo ? 'no' : 'na'}"
      data-closed="${l.amount > 0 ? 'yes' : 'no'}"
      data-leaddate="${l.leadDate ? l.leadDate.slice(0,10) : ''}">
      <td style="font-size:11px;color:#9ca3af;white-space:nowrap;width:90px">${date}</td>
      <td style="font-weight:500;white-space:nowrap">${l.name}</td>
      <td style="font-size:12px;max-width:160px">${l.company}${websiteLink}</td>
      <td style="font-size:11px;color:#6b7280;max-width:140px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${l.campaign}">${l.campaign}</td>
      <td style="font-size:11px;color:#9ca3af;max-width:120px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${l.adset}">${l.adset}</td>
      <td style="font-size:11px;max-width:160px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${l.ad}">${l.ad}</td>
      <td><span class="bd ${sCls}">${sLabel}</span></td>
      <td style="text-align:center">${demo}</td>
      <td style="text-align:center">${show}</td>
      <td>${rev}</td>
    </tr>`;
  }).join('');

  // ── Booked calls table rows ───────────────────────────────────────────────────
  const bookedRows = bookedLeads.map(l => {
    const sCls   = l.filterStatus === 'closed' ? 'bg' : l.filterStatus === 'show_up' ? 'bb' : 'by';
    const sLabel = l.filterStatus === 'closed' ? 'Closed' : l.filterStatus === 'show_up' ? 'Showed' : 'No Show';
    const coHtml = l.website && l.website !== '—'
      ? `${l.company} <a href="${l.website.startsWith('http') ? l.website : 'https://'+l.website}" target="_blank" style="color:#2563eb;font-size:10px">${l.website.replace(/^https?:\/\//,'').split('/')[0]}</a>`
      : l.company;
    return `<tr data-status="${l.filterStatus}" data-search="${(l.name+' '+l.company).toLowerCase()}" data-campaign="${l.campaign}" data-adset="${l.adset}" data-ad="${l.ad}">
      <td style="font-weight:500">${l.name}</td>
      <td style="font-size:12px">${coHtml}</td>
      <td style="font-size:11px;color:#6b7280">${l.campaign}</td>
      <td style="font-size:11px;color:#9ca3af;max-width:130px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${l.adset}">${l.adset}</td>
      <td style="font-size:12px;white-space:nowrap">${l.leadDate}</td>
      <td style="font-size:12px;white-space:nowrap">${l.demoDate}</td>
      <td style="font-size:12px">${l.ae}</td>
      <td><span class="bd ${sCls}">${sLabel}</span>${l.filterStatus==='closed'&&l.subscribedAt?`<div style="font-size:10px;color:#6b7280;margin-top:2px">$${l.subscribedAt}/mo</div>`:''}</td>
    </tr>`;
  }).join('');

  // ── Funnel bars ───────────────────────────────────────────────────────────────
  const funnelStages = [
    { label: 'Meta Leads',      count: combined.leads,        rate: combined.cpl + ' CPL',                pct: 100,                                      color: '#2563eb' },
    { label: 'Qualified',       count: combined.qualified,    rate: combined.qualRate + '% of leads',     pct: Math.max(combined.qualRate, 2),           color: '#7c3aed' },
    { label: 'Booked Calls',    count: combined.bookedCalls,  rate: combined.pctBooked + '% of qualified',pct: Math.max(combined.pctBooked, 1),          color: '#d97706' },
    { label: 'Confirmed Shows', count: combined.shows,        rate: combined.pctShow + '% of booked',    pct: Math.max(combined.pctShow, 1),            color: '#16a34a' },
    { label: 'Closes',          count: combined.closes,       rate: combined.closePct + '% of shows',    pct: combined.shows > 0 ? Math.max(Math.round(combined.closes/combined.shows*100),1) : 1, color: '#059669' },
  ];
  const funnelHtml = funnelStages.map(s => `
    <div class="frow">
      <div class="flabel">${s.label}</div>
      <div class="ftrack"><div class="ffill" style="width:${s.pct}%;background:${s.color}"><span class="fcount">${s.count}</span></div></div>
      <div class="frate">${s.rate}</div>
    </div>`).join('');

  // ── Campaign overview table rows ──────────────────────────────────────────────
  const overviewRows = campKeys.map(k => {
    const d = funnels[k];
    const qCls = d.qualRate >= 50 ? 'bg' : d.qualRate >= 25 ? 'by' : 'br';
    const paused = !d.isActive ? ' <span class="bd bn" style="font-size:10px">Paused</span>' : '';
    return `<tr>
      <td style="font-weight:500">${d.name}${paused}</td>
      <td>${d.spend}</td>
      <td>${d.leads}</td>
      <td>${d.cplMeta}</td>
      <td>${d.cpm}</td>
      <td>${d.ctr}</td>
      <td>${d.frequency}</td>
      <td>${d.qualified}</td>
      <td><span class="bd ${qCls}">${d.qualRate}%</span></td>
      <td>${d.costQual}</td>
      <td>${d.bookedCalls}</td>
      <td>${d.costBook}</td>
      <td>${d.shows}</td>
      <td>${d.closes}</td>
      <td>${d.totalAmount > 0 ? `$${(d.totalAmount*12).toLocaleString('en-US',{maximumFractionDigits:0})}/yr` : '—'}</td>
    </tr>`;
  }).join('');

  // ── AI Calls tab data ────────────────────────────────────────────────────────
  const aiCalls = aiCallsData.calls || [];
  const aiNotCalled = aiCallsData.leadsNotCalled || [];
  const aiTotalLeads = aiCallsData.totalLeads || 0;
  const aiLeadsCalled = aiCallsData.leadsCalled || 0;
  const aiPctCalled = aiTotalLeads > 0 ? Math.round(aiLeadsCalled / aiTotalLeads * 100) : 0;
  const aiConnected = aiCalls.filter(c => c.connected).length;
  const aiSuccessful = aiCalls.filter(c => c.successful).length;
  const aiVoicemail = aiCalls.filter(c => c.voicemail).length;
  const connectedCalls = aiCalls.filter(c => c.connected);
  const aiAvgDuration = connectedCalls.length > 0 ? Math.round(connectedCalls.reduce((s, c) => s + c.duration, 0) / connectedCalls.length) : 0;
  const aiCallsPerLead = aiLeadsCalled > 0 ? (aiCalls.length / aiLeadsCalled).toFixed(1) : '0';

  function fmtCallDate(iso) {
    if (!iso) return '—';
    const d = new Date(iso);
    const months = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
    const h = d.getUTCHours(); const m = d.getUTCMinutes();
    const ampm = h >= 12 ? 'PM' : 'AM';
    const h12 = h % 12 || 12;
    return `${months[d.getUTCMonth()]} ${d.getUTCDate()}, ${h12}:${String(m).padStart(2,'0')} ${ampm}`;
  }

  function outcomeLabel(c) {
    if (c.successful) return { text: 'Successful', cls: 'bb' };
    if (c.connected) return { text: 'Connected', cls: 'bg' };
    if (c.voicemail) return { text: 'Voicemail', cls: 'by' };
    return { text: 'No Answer', cls: 'bn' };
  }

  function sentimentCls(s) {
    if (!s) return 'bn';
    const sl = s.toLowerCase();
    if (sl === 'positive') return 'bg';
    if (sl === 'negative') return 'br';
    return 'bn';
  }

  function durationCls(d, connected) {
    if (!connected) return 'color:#d1d5db';
    if (d >= 60) return 'color:#16a34a;font-weight:600';
    if (d >= 15) return 'color:#d97706';
    return 'color:#9ca3af';
  }

  function stripProto(url) { return (url || '').replace(/^https?:\/\//, '').split('/')[0]; }

  // Build unique filter option values for AI calls columns
  const aiUniqueCampaigns = [...new Set(aiCalls.map(c => c.campaign))].filter(Boolean).sort();
  const aiUniqueAds = [...new Set(aiCalls.map(c => c.ad))].filter(Boolean).sort();
  const aiUniqueOutcomes = ['Successful', 'Connected', 'Voicemail', 'No Answer'];
  const aiUniqueSentiments = [...new Set(aiCalls.map(c => c.sentiment))].filter(Boolean).sort();

  const aiCallRows = aiCalls.map((c, idx) => {
    const oc = outcomeLabel(c);
    const summaryShort = c.summary.length > 80 ? c.summary.slice(0, 80) + '…' : c.summary;
    const href = c.website ? (c.website.startsWith('http') ? c.website : 'https://' + c.website) : '';
    const websiteLink = c.website ? ' <a href="' + href + '" target="_blank" style="color:#2563eb;font-size:10px;text-decoration:none">' + stripProto(c.website) + '</a>' : '';
    return `<tr class="ai-row${c.successful ? ' ai-success' : c.voicemail ? ' ai-vm' : ''}"
      data-date="${c.dateShort}" data-campaign="${c.campaign}" data-ad="${c.ad}" data-outcome="${oc.text}" data-sentiment="${c.sentiment || ''}" data-search="${(c.name + ' ' + c.company).toLowerCase()}" data-idx="${idx}"
      onclick="toggleAiExpand(this)">
      <td style="font-size:11px;white-space:nowrap;color:#6b7280">${fmtCallDate(c.date)}</td>
      <td style="font-weight:500;white-space:nowrap">${c.name}</td>
      <td style="font-size:12px;max-width:140px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${c.company}${websiteLink}</td>
      <td style="font-size:11px;color:#6b7280;max-width:130px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${c.campaign}">${c.campaign}</td>
      <td style="font-size:11px;max-width:140px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${c.ad}">${c.ad}</td>
      <td style="text-align:center;font-size:12px">${c.attempt}</td>
      <td style="${durationCls(c.duration, c.connected)};font-size:12px;text-align:center">${c.duration}s</td>
      <td><span class="bd ${oc.cls}">${oc.text}</span></td>
      <td>${c.sentiment ? `<span class="bd ${sentimentCls(c.sentiment)}">${c.sentiment}</span>` : '<span style="color:#d1d5db">—</span>'}</td>
      <td style="font-size:11px;color:#6b7280;max-width:200px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${c.summary}">${summaryShort || '<span style="color:#d1d5db">—</span>'}</td>
      <td style="text-align:center">${c.recordingUrl ? `<button onclick="event.stopPropagation();playAiRecording(this,'${c.recordingUrl}')" style="background:none;border:none;cursor:pointer;font-size:16px;padding:2px 6px" title="Play recording">▶</button>` : '<span style="color:#d1d5db">—</span>'}</td>
    </tr>`;
  }).join('');

  const aiNotCalledRows = aiNotCalled.map(l => {
    const ncHref = l.website ? (l.website.startsWith('http') ? l.website : 'https://' + l.website) : '';
    const websiteLink = l.website ? ' <a href="' + ncHref + '" target="_blank" style="color:#2563eb;font-size:10px;text-decoration:none">' + stripProto(l.website) + '</a>' : '';
    return `<tr>
      <td style="font-weight:500">${l.name}</td>
      <td style="font-size:12px">${l.company}${websiteLink}</td>
      <td style="font-size:11px;color:#6b7280">${l.campaign}</td>
      <td style="font-size:11px;max-width:150px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${l.ad}">${l.ad}</td>
      <td style="font-size:11px;color:#9ca3af">${l.leadDate ? l.leadDate.slice(0, 10) : '—'}</td>
    </tr>`;
  }).join('');

  // ── Unique values for lead filters ───────────────────────────────────────────
  const uniqueCamps = [...new Set(allLeads.map(l => l.campaign))].filter(Boolean).sort();
  const campLeadOpts = uniqueCamps.map(c => `<option value="${c}">${c}</option>`).join('');

  // ── The _filterData structure (unchanged from original — powers KPI updates) ─
  const filterDataJs = JSON.stringify((() => {
    const data = {};
    for (const [campId, f] of Object.entries(funnels)) {
      const adsets = {};
      for (const ad of f.ads) {
        const as = ad.adset || 'Unknown';
        if (!adsets[as]) adsets[as] = { spendRaw:0, leads:0, qualified:0, bad:0, bookedCalls:0, shows:0, closes:0, totalAmount:0, ads:{} };
        adsets[as].spendRaw += ad.spendRaw;
        adsets[as].leads    += ad.leads;
        adsets[as].ads[ad.name] = { spendRaw: ad.spendRaw, leads: ad.leads, qualified:0, bad:0, bookedCalls:0, shows:0, closes:0, totalAmount:0 };
      }
      for (const [as, asData] of Object.entries(f.qualByAdset || {})) {
        if (!adsets[as]) adsets[as] = { spendRaw:0, leads:0, qualified:0, bad:0, bookedCalls:0, shows:0, closes:0, totalAmount:0, ads:{} };
        Object.assign(adsets[as], { qualified: asData.qualified, bad: asData.bad, bookedCalls: asData.bookedCalls, shows: asData.shows, closes: asData.closes, totalAmount: asData.totalAmount });
        for (const [adName, adData] of Object.entries(asData.byAd || {})) {
          if (!adsets[as].ads[adName]) adsets[as].ads[adName] = { spendRaw:0, leads:0 };
          Object.assign(adsets[as].ads[adName], { qualified: adData.qualified||0, bad: adData.bad||0, bookedCalls: adData.bookedCalls||0, shows: adData.shows||0, closes: adData.closes||0, totalAmount: adData.totalAmount||0 });
        }
      }
      data[campId] = { name: f.name, isActive: f.isActive, spendRaw: f.spendRaw, leads: f.leads, qualified: f.qualified, bad: f.bad, bookedCalls: f.bookedCalls, shows: f.shows, closes: f.closes, totalAmount: f.totalAmount, adsets };
    }
    return data;
  })());

  return `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>Gushwork · Meta Ads · ${reportDate}</title>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{background:#fff;color:#111827;font-family:-apple-system,BlinkMacSystemFont,'Inter','Segoe UI',sans-serif;font-size:14px;line-height:1.5}
a{color:inherit;text-decoration:none}

/* NAV */
nav{position:sticky;top:0;z-index:100;background:#fff;border-bottom:1px solid #e5e7eb;display:flex;align-items:center;padding:0 28px;height:46px;gap:0}
.nlogo{font-size:13px;font-weight:600;color:#111827;margin-right:28px;flex-shrink:0}
.ntab{padding:0 13px;height:46px;border:none;background:none;cursor:pointer;color:#6b7280;font-size:13px;font-weight:500;border-bottom:2px solid transparent;margin-bottom:-1px;transition:color .15s,border-color .15s;white-space:nowrap;flex-shrink:0}
.ntab:hover{color:#111827}.ntab.active{color:#111827;border-bottom-color:#111827}
.nmeta{margin-left:auto;font-size:11px;color:#9ca3af;flex-shrink:0}

/* PAGES */
.page{display:none;max-width:1600px;margin:0 auto;padding:24px 28px 80px}.page.active{display:block}

/* KPI GRID */
.krow{display:grid;grid-template-columns:repeat(6,1fr);gap:10px;margin-bottom:20px}
@media(max-width:1100px){.krow{grid-template-columns:repeat(3,1fr)}}
.kcard{background:#fff;border:1px solid #e5e7eb;border-radius:8px;padding:13px 15px;transition:border-color .15s}
.kcard:hover{border-color:#d1d5db}
.klabel{font-size:10px;color:#6b7280;font-weight:500;text-transform:uppercase;letter-spacing:.05em;margin-bottom:5px}
.kval{font-size:21px;font-weight:700;color:#111827;line-height:1;margin-bottom:3px}
.ksub{font-size:11px;color:#9ca3af}

/* FILTER BAR */
.fbar{display:flex;gap:7px;align-items:center;flex-wrap:wrap;padding:9px 13px;background:#f9fafb;border:1px solid #e5e7eb;border-radius:8px;margin-bottom:12px}
.fbar select,.fbar input{background:#fff;border:1px solid #e5e7eb;color:#374151;border-radius:6px;padding:4px 9px;font-size:12px;outline:none;height:28px;font-family:inherit}
.fbar select:focus,.fbar input:focus{border-color:#2563eb}
.fbar input[type=date]{color-scheme:light}
.fsep{color:#e5e7eb;font-size:16px}
.fclear{background:none;border:none;font-size:11px;color:#9ca3af;cursor:pointer;padding:0;white-space:nowrap}
.fclear:hover{color:#6b7280}

/* STATUS PILLS */
.prow{display:flex;gap:5px;align-items:center;margin-bottom:10px;flex-wrap:wrap}
.pbtn{padding:3px 10px;border-radius:99px;border:1px solid #e5e7eb;background:#fff;color:#6b7280;font-size:12px;cursor:pointer;font-weight:500;transition:all .1s}
.pbtn:hover{border-color:#9ca3af;color:#374151}
.pbtn.active{background:#111827;color:#fff;border-color:#111827}
.rcnt{font-size:11px;color:#9ca3af;margin-left:6px}

/* TABLE */
.tc{border:1px solid #e5e7eb;border-radius:8px;overflow:hidden;overflow-x:auto}
table{width:100%;border-collapse:collapse;font-size:13px}
thead tr{background:#f9fafb}
thead th{padding:8px 11px;text-align:left;font-size:10px;font-weight:500;text-transform:uppercase;letter-spacing:.05em;color:#6b7280;border-bottom:1px solid #e5e7eb;white-space:nowrap}
thead th.sortable{cursor:pointer}
thead th.sortable:hover{color:#111827}
thead th.sort-asc::after{content:' ↑';color:#2563eb}
thead th.sort-desc::after{content:' ↓';color:#2563eb}

/* COLUMN FILTERS */
.cf-th{display:flex;align-items:center;gap:4px;white-space:nowrap}
.cf-btn{background:none;border:none;cursor:pointer;padding:2px 3px;color:#d1d5db;font-size:9px;line-height:1;border-radius:3px;transition:color .1s}
.cf-btn:hover{color:#6b7280}
.cf-btn.active{color:#2563eb}
.cf-panel{position:fixed;z-index:9999;background:#fff;border:1px solid #e5e7eb;border-radius:8px;box-shadow:0 4px 20px rgba(0,0,0,.12);min-width:220px;max-width:300px;display:none}
.cf-panel.open{display:block}
.cf-top{padding:8px 10px 6px;border-bottom:1px solid #f3f4f6}
.cf-top input{width:100%;padding:4px 8px;border:1px solid #e5e7eb;border-radius:5px;font-size:12px;outline:none;font-family:inherit}
.cf-top input:focus{border-color:#2563eb}
.cf-actions{display:flex;gap:10px;padding:5px 10px 6px;border-bottom:1px solid #f3f4f6}
.cf-actions button{font-size:11px;color:#2563eb;background:none;border:none;cursor:pointer;padding:0;font-family:inherit}
.cf-actions button:hover{text-decoration:underline}
.cf-list{max-height:220px;overflow-y:auto;padding:4px 0}
.cf-item{display:flex;align-items:center;gap:8px;padding:5px 12px;cursor:pointer;user-select:none}
.cf-item:hover{background:#f9fafb}
.cf-item input[type=checkbox]{cursor:pointer;flex-shrink:0;accent-color:#2563eb}
.cf-item span{font-size:12px;color:#374151;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;flex:1}
tbody tr{border-bottom:1px solid #f3f4f6;transition:background .1s}
tbody tr:last-child{border-bottom:none}
tbody tr:hover{background:#f9fafb}
tbody td{padding:8px 11px;vertical-align:middle}
tfoot tr{background:#f9fafb;border-top:2px solid #e5e7eb}
tfoot td{padding:8px 11px;font-size:13px}

/* BADGES */
.bd{display:inline-block;padding:2px 7px;border-radius:4px;font-size:11px;font-weight:500}
.bg{background:#f0fdf4;color:#16a34a}
.br{background:#fef2f2;color:#dc2626}
.by{background:#fffbeb;color:#d97706}
.bb{background:#eff6ff;color:#2563eb}
.bp{background:#f5f3ff;color:#7c3aed}
.bn{background:#f3f4f6;color:#6b7280}

/* PERFORMANCE TAB */
.ctabs{display:flex;gap:0;border-bottom:1px solid #e5e7eb;margin-bottom:20px;overflow-x:auto}
.ctab{padding:8px 15px;border:none;background:none;cursor:pointer;font-size:13px;color:#6b7280;font-weight:500;border-bottom:2px solid transparent;margin-bottom:-1px;transition:all .15s;white-space:nowrap;flex-shrink:0;font-family:inherit}
.ctab:hover{color:#111827}.ctab.active{color:#111827;border-bottom-color:#111827}
.cpanel{display:none}.cpanel.active{display:block}
.perf-stats{display:flex;gap:20px;flex-wrap:wrap;margin-bottom:16px;padding:12px 16px;background:#f9fafb;border:1px solid #e5e7eb;border-radius:8px}
.pstat{display:flex;flex-direction:column;gap:2px}
.pstat-label{font-size:10px;color:#6b7280;text-transform:uppercase;letter-spacing:.05em;font-weight:500}
.pstat-val{font-size:15px;font-weight:700;color:#111827}

/* FUNNEL */
.frow{display:flex;align-items:center;gap:12px;padding:9px 0;border-bottom:1px solid #f3f4f6}
.frow:last-child{border-bottom:none}
.flabel{width:140px;font-size:12px;color:#6b7280;flex-shrink:0}
.ftrack{flex:1;background:#f3f4f6;border-radius:4px;height:30px;overflow:hidden}
.ffill{height:100%;border-radius:4px;transition:width 1.2s ease;display:flex;align-items:center;padding:0 10px;min-width:40px}
.fcount{font-size:13px;font-weight:700;color:#fff;white-space:nowrap}
.frate{width:160px;font-size:12px;color:#9ca3af;text-align:right;flex-shrink:0}

/* FUNNEL ECONOMICS */
.econ-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:10px;margin-top:20px}
.econ-card{background:#f9fafb;border:1px solid #e5e7eb;border-radius:8px;padding:12px 14px}
.econ-label{font-size:10px;color:#6b7280;font-weight:500;text-transform:uppercase;letter-spacing:.05em;margin-bottom:5px}
.econ-val{font-size:18px;font-weight:700;color:#111827}

/* PASSWORD PROTECT */
.pw-wrap{position:relative}
.pw-blur{filter:blur(5px);pointer-events:none;user-select:none}
.pw-overlay{position:absolute;inset:0;display:flex;align-items:center;justify-content:center;z-index:10}
.pw-box{background:#fff;border:1px solid #e5e7eb;border-radius:12px;padding:28px;text-align:center;max-width:300px;width:90%;box-shadow:0 4px 24px rgba(0,0,0,.08)}
.pw-box h3{font-size:14px;font-weight:600;margin-bottom:5px}
.pw-box p{font-size:12px;color:#9ca3af;margin-bottom:14px}
.pw-in{width:100%;padding:7px 11px;border:1px solid #e5e7eb;border-radius:6px;font-size:13px;outline:none;margin-bottom:9px;font-family:inherit}
.pw-in:focus{border-color:#2563eb}.pw-in.err{border-color:#dc2626}
.pw-btn{width:100%;padding:8px;border:none;border-radius:6px;background:#111827;color:#fff;font-size:13px;font-weight:600;cursor:pointer;font-family:inherit}
.pw-err{font-size:11px;color:#dc2626;margin-top:5px;display:none}
.pw-wrap:not(.locked) .pw-overlay{display:none}

/* PAGE HEADER */
.ph{margin-bottom:18px}
.pt{font-size:17px;font-weight:700;color:#111827;margin-bottom:3px}
.ps{font-size:12px;color:#9ca3af}

/* SECTION LABEL */
.slabel{font-size:12px;font-weight:600;color:#374151;text-transform:uppercase;letter-spacing:.05em;margin:24px 0 10px;padding-bottom:8px;border-bottom:1px solid #f3f4f6}
.slabel:first-child{margin-top:0}

/* AI CALLS */
.ai-row{cursor:pointer}
.ai-row:hover{background:#f0f9ff!important}
.ai-success{border-left:3px solid #16a34a}
.ai-vm td{opacity:.65}
.ai-detail td{cursor:default}

/* REMINDER CALLS */
.rc-efficacy{display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-bottom:20px}
@media(max-width:700px){.rc-efficacy{grid-template-columns:1fr}}
.rc-eff-card{border:1px solid #e5e7eb;border-radius:10px;padding:16px 18px;position:relative;overflow:hidden}
.rc-eff-card::before{content:'';position:absolute;top:0;left:0;right:0;height:3px}
.rc-eff-yes::before{background:#16a34a}
.rc-eff-no::before{background:#9ca3af}
.rc-eff-title{font-size:12px;font-weight:600;text-transform:uppercase;letter-spacing:.05em;margin-bottom:2px}
.rc-eff-yes .rc-eff-title{color:#16a34a}
.rc-eff-no .rc-eff-title{color:#6b7280}
.rc-eff-count{font-size:11px;color:#9ca3af;margin-bottom:12px}
.rc-bar-row{margin-bottom:8px}
.rc-bar-label{display:flex;justify-content:space-between;font-size:12px;margin-bottom:3px}
.rc-bar-track{background:#f3f4f6;border-radius:4px;height:10px;overflow:hidden}
.rc-bar-fill{height:100%;border-radius:4px;transition:width .6s ease}
.rc-verdict{margin-top:10px;font-size:11px;font-weight:500;color:#374151;padding:6px 10px;background:#f9fafb;border-radius:6px;text-align:center}
.rc-row{cursor:pointer}
.rc-row:hover{background:#f9fafb!important}
.rc-detail{background:#f9fafb}
.rc-detail td{cursor:default;padding:0!important}
.rc-calls-inner{padding:14px 18px 18px}
.rc-mini-call{display:flex;align-items:center;gap:10px;padding:6px 0;border-bottom:1px solid #f3f4f6;font-size:12px}
.rc-mini-call:last-child{border-bottom:none}
</style>
</head>
<body>

<nav>
  <div class="nlogo">Gushwork · Meta Ads</div>
  <button class="ntab active" onclick="showPage('reminder',this)">Reminder Calls</button>
  <button class="ntab" onclick="showPage('leads',this)">All Leads</button>
  <button class="ntab" onclick="showPage('performance',this)">Performance</button>
  <button class="ntab" onclick="showPage('funnel',this)">Funnel</button>
  <button class="ntab" onclick="showPage('overview',this)">Overview</button>
  <button class="ntab" onclick="showPage('aicalls',this)">AI Calls</button>
  <div class="nmeta">${ADS_START_DATE} – ${reportDate}</div>
</nav>

<!-- ═══════════════════ ALL LEADS ════════════════════════════════════════════ -->
<div class="page" id="page-leads">
  <div class="ph">
    <div class="pt">All Leads <span style="font-size:13px;font-weight:400;color:#9ca3af;margin-left:6px" id="al-count">${allLeads.length} total</span></div>
    <div class="ps">Every lead from Meta · filter by campaign, ad set, ad or status · KPIs update with filters</div>
  </div>

  <!-- KPI ROW 1 -->
  <div class="krow">
    <div class="kcard"><div class="klabel">Total Spend</div><div class="kval" id="lk-spend">${combined.spend}</div><div class="ksub" id="lk-spend-sub">${campKeys.length} campaigns</div></div>
    <div class="kcard"><div class="klabel">Meta Leads</div><div class="kval" id="lk-leads">${combined.leads}</div><div class="ksub" id="lk-cpl">${combined.cpl} CPL</div></div>
    <div class="kcard"><div class="klabel">Qualified</div><div class="kval" id="lk-qual">${combined.qualified}</div><div class="ksub" id="lk-qual-sub">${combined.qualRate}% qual rate</div></div>
    <div class="kcard"><div class="klabel">Cost / Qual</div><div class="kval" id="lk-cq">${combined.costQual}</div><div class="ksub">per qualified lead</div></div>
    <div class="kcard"><div class="klabel">CPM</div><div class="kval">${combined.cpmFmt}</div><div class="ksub">cost per 1k impressions</div></div>
    <div class="kcard"><div class="klabel">CTR · Frequency</div><div class="kval">${combined.ctrFmt}</div><div class="ksub">${combined.freqFmt}x frequency</div></div>
  </div>

  <!-- KPI ROW 2 -->
  <div class="krow">
    <div class="kcard"><div class="klabel">Booked Calls</div><div class="kval" id="lk-booked">${combined.bookedCalls}</div><div class="ksub" id="lk-booked-sub">${combined.pctBooked}% of qualified</div></div>
    <div class="kcard"><div class="klabel">Cost / Demo</div><div class="kval" id="lk-cd">${combined.costBook}</div><div class="ksub">per booked call</div></div>
    <div class="kcard"><div class="klabel">Confirmed Shows</div><div class="kval" id="lk-shows">${combined.shows}</div><div class="ksub" id="lk-shows-sub">${combined.pctShow}% show rate</div></div>
    <div class="kcard"><div class="klabel">Closes</div><div class="kval" id="lk-closes">${combined.closes}</div><div class="ksub" id="lk-closes-sub">${combined.closePct}% of shows</div></div>
    <div class="kcard"><div class="klabel">Revenue ARR</div><div class="kval" id="lk-arr">${combined.arrFmt}</div><div class="ksub" id="lk-arr-sub">${combined.mrrFmt}/mo MRR</div></div>
    <div class="kcard"><div class="klabel">Junk / Bad</div><div class="kval" id="lk-bad">${combined.bad}</div><div class="ksub" id="lk-bad-sub">${combined.junkRate}% junk rate</div></div>
  </div>

  <!-- FILTER BAR -->
  <div class="fbar">
    <input type="search" id="al-search" placeholder="Search name or company…" oninput="applyAl()" style="width:200px">
    <span class="fsep">|</span>
    <span style="font-size:11px;color:#9ca3af">Lead date:</span>
    <input type="date" id="al-from" onchange="applyAl()">
    <span style="font-size:11px;color:#9ca3af">to</span>
    <input type="date" id="al-to" onchange="applyAl()">
    <button class="fclear" onclick="clearAl()" id="al-clear" style="display:none">✕ Clear all</button>
  </div>

  <!-- STATUS PILLS -->
  <div class="prow">
    <span style="font-size:11px;color:#9ca3af">Show:</span>
    <button class="pbtn active" data-s="all"     onclick="setAlStatus('all',this)">All</button>
    <button class="pbtn"        data-s="qualified" onclick="setAlStatus('qualified',this)">Qualified</button>
    <button class="pbtn"        data-s="bad"       onclick="setAlStatus('bad',this)">Bad</button>
    <button class="pbtn"        data-s="demo"      onclick="setAlStatus('demo',this)">Demo Booked</button>
    <button class="pbtn"        data-s="closed"    onclick="setAlStatus('closed',this)">Closed</button>
    <span class="rcnt" id="al-rcnt">${allLeads.length} leads</span>
  </div>

  <!-- LEADS TABLE -->
  <div class="tc">
    <table id="leads-table">
      <thead>
        <tr>
          <th class="sortable" onclick="sortLeads(0,this)">Date</th>
          <th class="sortable" onclick="sortLeads(1,this)">Name</th>
          <th>Company</th>
          <th>
            <div class="cf-th">Campaign
              <button class="cf-btn" id="cfbtn-campaign" onclick="toggleCF('campaign',event)" title="Filter campaigns">▼</button>
            </div>
            <div class="cf-panel" id="cf-campaign">
              <div class="cf-top"><input type="search" placeholder="Search…" oninput="filterCFOpts('campaign',this.value)"></div>
              <div class="cf-actions">
                <button onclick="selectAllCF('campaign')">Select all</button>
                <button onclick="clearCF('campaign')">Clear</button>
              </div>
              <div class="cf-list" id="cflist-campaign">
                ${[...new Set(allLeads.map(l=>l.campaign))].filter(Boolean).sort().map(v=>`<label class="cf-item"><input type="checkbox" value="${v.replace(/"/g,'&quot;')}" onchange="toggleCFVal('campaign',this)"><span title="${v}">${v}</span></label>`).join('')}
              </div>
            </div>
          </th>
          <th>
            <div class="cf-th">Ad Set
              <button class="cf-btn" id="cfbtn-adset" onclick="toggleCF('adset',event)" title="Filter ad sets">▼</button>
            </div>
            <div class="cf-panel" id="cf-adset">
              <div class="cf-top"><input type="search" placeholder="Search…" oninput="filterCFOpts('adset',this.value)"></div>
              <div class="cf-actions">
                <button onclick="selectAllCF('adset')">Select all</button>
                <button onclick="clearCF('adset')">Clear</button>
              </div>
              <div class="cf-list" id="cflist-adset">
                ${[...new Set(allLeads.map(l=>l.adset))].filter(Boolean).sort().map(v=>`<label class="cf-item"><input type="checkbox" value="${v.replace(/"/g,'&quot;')}" onchange="toggleCFVal('adset',this)"><span title="${v}">${v}</span></label>`).join('')}
              </div>
            </div>
          </th>
          <th>
            <div class="cf-th">Ad
              <button class="cf-btn" id="cfbtn-ad" onclick="toggleCF('ad',event)" title="Filter ads">▼</button>
            </div>
            <div class="cf-panel" id="cf-ad">
              <div class="cf-top"><input type="search" placeholder="Search…" oninput="filterCFOpts('ad',this.value)"></div>
              <div class="cf-actions">
                <button onclick="selectAllCF('ad')">Select all</button>
                <button onclick="clearCF('ad')">Clear</button>
              </div>
              <div class="cf-list" id="cflist-ad">
                ${[...new Set(allLeads.map(l=>l.ad))].filter(Boolean).sort().map(v=>`<label class="cf-item"><input type="checkbox" value="${v.replace(/"/g,'&quot;')}" onchange="toggleCFVal('ad',this)"><span title="${v}">${v}</span></label>`).join('')}
              </div>
            </div>
          </th>
          <th>
            <div class="cf-th">Status
              <button class="cf-btn" id="cfbtn-status" onclick="toggleCF('status',event)" title="Filter status">▼</button>
            </div>
            <div class="cf-panel" id="cf-status">
              <div class="cf-actions" style="border-bottom:none;padding-top:8px">
                <button onclick="selectAllCF('status')">Select all</button>
                <button onclick="clearCF('status')">Clear</button>
              </div>
              <div class="cf-list" id="cflist-status">
                <label class="cf-item"><input type="checkbox" value="qualified" onchange="toggleCFVal('status',this)"><span>Qualified</span></label>
                <label class="cf-item"><input type="checkbox" value="bad" onchange="toggleCFVal('status',this)"><span>Bad</span></label>
                <label class="cf-item"><input type="checkbox" value="unreviewed" onchange="toggleCFVal('status',this)"><span>Unreviewed</span></label>
              </div>
            </div>
          </th>
          <th>
            <div class="cf-th">Demo
              <button class="cf-btn" id="cfbtn-demo" onclick="toggleCF('demo',event)" title="Filter demo">▼</button>
            </div>
            <div class="cf-panel" id="cf-demo">
              <div class="cf-actions" style="border-bottom:none;padding-top:8px">
                <button onclick="selectAllCF('demo')">Select all</button>
                <button onclick="clearCF('demo')">Clear</button>
              </div>
              <div class="cf-list" id="cflist-demo">
                <label class="cf-item"><input type="checkbox" value="yes" onchange="toggleCFVal('demo',this)"><span>Booked</span></label>
                <label class="cf-item"><input type="checkbox" value="no" onchange="toggleCFVal('demo',this)"><span>Not Booked</span></label>
              </div>
            </div>
          </th>
          <th>
            <div class="cf-th">Show
              <button class="cf-btn" id="cfbtn-show" onclick="toggleCF('show',event)" title="Filter show">▼</button>
            </div>
            <div class="cf-panel" id="cf-show">
              <div class="cf-actions" style="border-bottom:none;padding-top:8px">
                <button onclick="selectAllCF('show')">Select all</button>
                <button onclick="clearCF('show')">Clear</button>
              </div>
              <div class="cf-list" id="cflist-show">
                <label class="cf-item"><input type="checkbox" value="yes" onchange="toggleCFVal('show',this)"><span>Showed</span></label>
                <label class="cf-item"><input type="checkbox" value="no" onchange="toggleCFVal('show',this)"><span>No Show</span></label>
                <label class="cf-item"><input type="checkbox" value="na" onchange="toggleCFVal('show',this)"><span>Not Booked</span></label>
              </div>
            </div>
          </th>
          <th>
            <div class="cf-th">Revenue
              <button class="cf-btn" id="cfbtn-revenue" onclick="toggleCF('revenue',event)" title="Filter revenue">▼</button>
            </div>
            <div class="cf-panel" id="cf-revenue">
              <div class="cf-actions" style="border-bottom:none;padding-top:8px">
                <button onclick="selectAllCF('revenue')">Select all</button>
                <button onclick="clearCF('revenue')">Clear</button>
              </div>
              <div class="cf-list" id="cflist-revenue">
                <label class="cf-item"><input type="checkbox" value="yes" onchange="toggleCFVal('revenue',this)"><span>Has Revenue</span></label>
                <label class="cf-item"><input type="checkbox" value="no" onchange="toggleCFVal('revenue',this)"><span>No Revenue</span></label>
              </div>
            </div>
          </th>
        </tr>
      </thead>
      <tbody id="leads-tbody">${leadsRows}</tbody>
    </table>
  </div>
</div>

<!-- ═══════════════════ PERFORMANCE ═════════════════════════════════════════ -->
<div class="page" id="page-performance">
  <div class="ph">
    <div class="pt">Performance</div>
    <div class="ps">Ad-level breakdown · spend + qual data joined · click column headers to sort</div>
  </div>
  <div class="ctabs">${perfTabBtns}</div>
  ${perfPanels}
</div>

<!-- ═══════════════════ FUNNEL ═══════════════════════════════════════════════ -->
<div class="page" id="page-funnel">
  <div class="ph">
    <div class="pt">Funnel</div>
    <div class="ps">End-to-end from Meta lead to close · ${ADS_START_DATE} – ${reportDate}</div>
  </div>

  <div class="slabel">Funnel Stages — All Campaigns</div>
  <div style="background:#fff;border:1px solid #e5e7eb;border-radius:8px;padding:16px 20px;margin-bottom:20px">
    <div style="display:flex;align-items:center;gap:12px;padding:8px 0;margin-bottom:4px">
      <div class="flabel" style="font-weight:600">Total Spend</div>
      <div style="font-size:18px;font-weight:700;color:#111827;flex:1">${combined.spend}</div>
      <div class="frate">${combined.impressions} impressions</div>
    </div>
    ${funnelHtml}
  </div>

  <div class="econ-grid">
    <div class="econ-card"><div class="econ-label">Cost / Lead</div><div class="econ-val">${combined.cpl}</div></div>
    <div class="econ-card"><div class="econ-label">Cost / Qualified</div><div class="econ-val">${combined.costQual}</div></div>
    <div class="econ-card"><div class="econ-label">Cost / Demo</div><div class="econ-val">${combined.costBook}</div></div>
    <div class="econ-card"><div class="econ-label">Cost / Show</div><div class="econ-val">${combined.costShow}</div></div>
    <div class="econ-card"><div class="econ-label">Cost / Close</div><div class="econ-val">${combined.costClose}</div></div>
    <div class="econ-card"><div class="econ-label">Revenue ARR</div><div class="econ-val">${combined.arrFmt}</div></div>
  </div>

  <div class="slabel">Demos Booked <span style="font-size:11px;font-weight:400;color:#9ca3af">(password protected)</span></div>

  <div class="pw-wrap locked" id="demos-pw-wrap">
    <div class="pw-blur" id="demos-content">
      <div class="fbar" style="margin-bottom:12px">
        <input type="search" id="df-search" placeholder="Search name / company…" oninput="applyDemos()" style="width:180px">
        <select id="df-camp" onchange="onDfCamp()"><option value="all">All Campaigns</option>${[...new Set(bookedLeads.map(l=>l.campaign))].filter(Boolean).sort().map(c=>`<option value="${c}">${c}</option>`).join('')}</select>
        <select id="df-adset" onchange="onDfAdset()" disabled><option value="all">All Ad Sets</option></select>
        <select id="df-ad" onchange="applyDemos()" disabled><option value="all">All Ads</option></select>
        <span class="fsep">|</span>
        <span class="rcnt" id="df-cnt">${bookedLeads.length} of ${bookedLeads.length}</span>
      </div>
      <div class="prow">
        <span style="font-size:11px;color:#9ca3af">Status:</span>
        <button class="pbtn active" onclick="setDfStatus('all',this)">All (${bookedLeads.length})</button>
        <button class="pbtn" onclick="setDfStatus('show_up',this)">Showed (${bookedLeads.filter(l=>l.filterStatus==='show_up').length})</button>
        <button class="pbtn" onclick="setDfStatus('no_show',this)">No Show (${bookedLeads.filter(l=>l.filterStatus==='no_show').length})</button>
        <button class="pbtn" onclick="setDfStatus('closed',this)">Closed (${bookedLeads.filter(l=>l.filterStatus==='closed').length})</button>
      </div>
      <div class="tc">
        <table id="demos-table">
          <thead><tr><th>Name</th><th>Company</th><th>Campaign</th><th>Ad Set</th><th>Lead Date</th><th>Demo Date</th><th>AE</th><th>Status</th></tr></thead>
          <tbody id="demos-tbody">${bookedRows}</tbody>
        </table>
      </div>
    </div>
    <div class="pw-overlay">
      <div class="pw-box">
        <h3>Protected</h3>
        <p>Enter password to view booked calls</p>
        <input class="pw-in" id="demos-pw" type="password" placeholder="Password" onkeydown="if(event.key==='Enter')checkPw()">
        <button class="pw-btn" onclick="checkPw()">Unlock</button>
        <div class="pw-err" id="demos-pw-err">Incorrect password</div>
      </div>
    </div>
  </div>
</div>

<!-- ═══════════════════ OVERVIEW ═════════════════════════════════════════════ -->
<div class="page" id="page-overview">
  <div class="ph">
    <div class="pt">Campaign Overview</div>
    <div class="ps">Full metrics across all campaigns · ${ADS_START_DATE} – ${reportDate}</div>
  </div>
  <div class="slabel">Campaign Comparison</div>
  <div class="tc">
    <table>
      <thead><tr>
        <th>Campaign</th><th>Spend</th><th>Leads</th><th>CPL</th>
        <th>CPM</th><th>CTR</th><th>Freq</th>
        <th>Qualified</th><th>Qual%</th><th>Cost/Qual</th>
        <th>Booked</th><th>Cost/Demo</th><th>Shows</th><th>Closes</th><th>Revenue</th>
      </tr></thead>
      <tbody>${overviewRows}</tbody>
      <tfoot><tr>
        <td style="font-weight:600">Total</td>
        <td style="font-weight:600">${combined.spend}</td>
        <td style="font-weight:600">${combined.leads}</td>
        <td>${combined.cpl}</td>
        <td>${combined.cpmFmt}</td>
        <td>${combined.ctrFmt}</td>
        <td>${combined.freqFmt}</td>
        <td style="font-weight:600">${combined.qualified}</td>
        <td><span class="bd ${combined.qualRate >= 50 ? 'bg' : combined.qualRate >= 25 ? 'by' : 'br'}">${combined.qualRate}%</span></td>
        <td>${combined.costQual}</td>
        <td style="font-weight:600">${combined.bookedCalls}</td>
        <td>${combined.costBook}</td>
        <td style="font-weight:600">${combined.shows}</td>
        <td style="font-weight:600">${combined.closes}</td>
        <td style="font-weight:600">${combined.arrFmt}/yr</td>
      </tr></tfoot>
    </table>
  </div>
</div>

<!-- ═══════════════════ AI CALLS ═══════════════════════════════════════════ -->
<div class="page" id="page-aicalls">
  <div class="ph">
    <div class="pt">AI Calls <span style="font-size:13px;font-weight:400;color:#9ca3af;margin-left:6px" id="ai-count">${aiCalls.length} calls</span></div>
    <div class="ps">Retell AI calls to leads from ${AI_CALL_CAMPAIGNS.join(' & ')} · click any row to expand</div>
  </div>

  <!-- KPI ROW -->
  <div class="krow">
    <div class="kcard"><div class="klabel">Total Leads</div><div class="kval" id="ai-kpi-leads">${aiTotalLeads}</div><div class="ksub">in 2 campaigns</div></div>
    <div class="kcard"><div class="klabel">Leads Called</div><div class="kval" id="ai-kpi-called">${aiLeadsCalled}</div><div class="ksub" id="ai-kpi-called-sub">${aiPctCalled}% of leads</div></div>
    <div class="kcard"><div class="klabel">Total Calls</div><div class="kval" id="ai-kpi-total">${aiCalls.length}</div><div class="ksub" id="ai-kpi-total-sub">${aiCallsPerLead} calls/lead avg</div></div>
    <div class="kcard"><div class="klabel">Connected</div><div class="kval" id="ai-kpi-connected">${aiConnected}</div><div class="ksub" id="ai-kpi-connected-sub">${aiCalls.length > 0 ? Math.round(aiConnected / aiCalls.length * 100) : 0}% of calls</div></div>
    <div class="kcard"><div class="klabel">Successful</div><div class="kval" id="ai-kpi-successful">${aiSuccessful}</div><div class="ksub" id="ai-kpi-successful-sub">${aiConnected > 0 ? Math.round(aiSuccessful / aiConnected * 100) : 0}% of connected</div></div>
    <div class="kcard"><div class="klabel">Avg Duration</div><div class="kval" id="ai-kpi-duration">${aiAvgDuration}s</div><div class="ksub">connected calls</div></div>
  </div>

  <!-- DATE FILTER BAR -->
  <div class="fbar" style="margin-bottom:14px">
    <button class="pbtn" onclick="setAiDate('today',this)">Today</button>
    <button class="pbtn" onclick="setAiDate('yesterday',this)">Yesterday</button>
    <button class="pbtn" onclick="setAiDate('thisweek',this)">This Week</button>
    <button class="pbtn" onclick="setAiDate('lastweek',this)">Last Week</button>
    <button class="pbtn" onclick="setAiDate('thismonth',this)">This Month</button>
    <button class="pbtn" onclick="setAiDate('lastmonth',this)">Last Month</button>
    <button class="pbtn active" onclick="setAiDate('all',this)">All Time</button>
    <span class="fsep">|</span>
    <input type="date" id="ai-from" onchange="applyAiFilters()" title="From date">
    <span style="font-size:11px;color:#9ca3af">to</span>
    <input type="date" id="ai-to" onchange="applyAiFilters()" title="To date">
    <button class="fclear" onclick="clearAiFilters()" id="ai-clear" style="display:none">Clear filters</button>
  </div>

  <!-- CAMPAIGN TABS -->
  <div class="ctabs" style="margin-bottom:16px">
    <button class="ctab active" onclick="setAiCampaign('all',this)">All</button>
    ${AI_CALL_CAMPAIGNS.map(c => `<button class="ctab" onclick="setAiCampaign('${c}',this)">${c}</button>`).join('')}
  </div>

  <!-- CALLS TABLE -->
  <div class="tc">
    <table id="ai-table">
      <thead>
        <tr>
          <th class="sortable" onclick="sortAiCalls(0,this)">Date</th>
          <th class="sortable" onclick="sortAiCalls(1,this)">Lead</th>
          <th>Company</th>
          <th>
            <div class="cf-th">Campaign
              <button class="cf-btn" id="cfbtn-ai-campaign" onclick="toggleAiCF('campaign',event)" title="Filter campaigns">▼</button>
            </div>
            <div class="cf-panel" id="cf-ai-campaign">
              <div class="cf-top"><input type="search" placeholder="Search…" oninput="filterAiCFOpts('campaign',this.value)"></div>
              <div class="cf-actions"><button onclick="selectAllAiCF('campaign')">Select all</button><button onclick="clearAiCF('campaign')">Clear</button></div>
              <div class="cf-list" id="cflist-ai-campaign">${aiUniqueCampaigns.map(v => '<label class="cf-item"><input type="checkbox" value="' + v.replace(/"/g,'&quot;') + '" onchange="toggleAiCFVal(\'campaign\',this)"><span title="' + v + '">' + v + '</span></label>').join('')}</div>
            </div>
          </th>
          <th>
            <div class="cf-th">Ad
              <button class="cf-btn" id="cfbtn-ai-ad" onclick="toggleAiCF('ad',event)" title="Filter ads">▼</button>
            </div>
            <div class="cf-panel" id="cf-ai-ad">
              <div class="cf-top"><input type="search" placeholder="Search…" oninput="filterAiCFOpts('ad',this.value)"></div>
              <div class="cf-actions"><button onclick="selectAllAiCF('ad')">Select all</button><button onclick="clearAiCF('ad')">Clear</button></div>
              <div class="cf-list" id="cflist-ai-ad">${aiUniqueAds.map(v => '<label class="cf-item"><input type="checkbox" value="' + v.replace(/"/g,'&quot;') + '" onchange="toggleAiCFVal(\'ad\',this)"><span title="' + v + '">' + v + '</span></label>').join('')}</div>
            </div>
          </th>
          <th class="sortable" onclick="sortAiCalls(5,this)">Attempt</th>
          <th class="sortable" onclick="sortAiCalls(6,this)">Duration</th>
          <th>
            <div class="cf-th">Outcome
              <button class="cf-btn" id="cfbtn-ai-outcome" onclick="toggleAiCF('outcome',event)" title="Filter outcomes">▼</button>
            </div>
            <div class="cf-panel" id="cf-ai-outcome">
              <div class="cf-actions" style="border-bottom:none;padding-top:8px"><button onclick="selectAllAiCF('outcome')">Select all</button><button onclick="clearAiCF('outcome')">Clear</button></div>
              <div class="cf-list" id="cflist-ai-outcome">${aiUniqueOutcomes.map(v => '<label class="cf-item"><input type="checkbox" value="' + v + '" onchange="toggleAiCFVal(\'outcome\',this)"><span>' + v + '</span></label>').join('')}</div>
            </div>
          </th>
          <th>
            <div class="cf-th">Sentiment
              <button class="cf-btn" id="cfbtn-ai-sentiment" onclick="toggleAiCF('sentiment',event)" title="Filter sentiments">▼</button>
            </div>
            <div class="cf-panel" id="cf-ai-sentiment">
              <div class="cf-actions" style="border-bottom:none;padding-top:8px"><button onclick="selectAllAiCF('sentiment')">Select all</button><button onclick="clearAiCF('sentiment')">Clear</button></div>
              <div class="cf-list" id="cflist-ai-sentiment">${aiUniqueSentiments.map(v => '<label class="cf-item"><input type="checkbox" value="' + v + '" onchange="toggleAiCFVal(\'sentiment\',this)"><span>' + v + '</span></label>').join('')}</div>
            </div>
          </th>
          <th>Summary</th>
          <th>Listen</th>
        </tr>
      </thead>
      <tbody id="ai-tbody">${aiCallRows}</tbody>
    </table>
  </div>

  <!-- LEADS NOT CALLED -->
  <div style="margin-top:24px">
    <div class="slabel" style="cursor:pointer;user-select:none" onclick="document.getElementById('ai-notcalled-wrap').style.display=document.getElementById('ai-notcalled-wrap').style.display==='none'?'block':'none';this.querySelector('span').textContent=document.getElementById('ai-notcalled-wrap').style.display==='none'?'▶':'▼'">
      <span>▶</span> Leads Not Called <span style="font-weight:400;color:#9ca3af;text-transform:none;letter-spacing:0;font-size:12px">${aiNotCalled.length} leads</span>
    </div>
    <div id="ai-notcalled-wrap" style="display:none">
      <div class="tc">
        <table>
          <thead><tr>
            <th>Name</th><th>Company</th><th>Campaign</th><th>Ad</th><th>Lead Date</th>
          </tr></thead>
          <tbody>${aiNotCalledRows}</tbody>
        </table>
      </div>
    </div>
  </div>
</div>

${(() => {
  // ── Reminder Calls page ────────────────────────────────────────────────────
  if (!reminderPeople.length) return '<!-- no reminder data -->';

  // Use call-level totals to match the sheet (327 calls, 123 connected)
  const totalSent      = reminderPeople._totalCallRows || reminderPeople.length;
  const totalConnected = reminderPeople._connectedCallRows || reminderPeople.filter(p => p.isConnected).length;
  const connectedPct   = totalSent > 0 ? Math.round(totalConnected / totalSent * 100) : 0;

  const showed  = reminderPeople.filter(p => p.showStatus === 'Showed').length;
  const noShow  = reminderPeople.filter(p => p.showStatus === 'No-Show').length;
  const pending = reminderPeople.filter(p => p.showStatus === 'Pending').length;
  const showRate = (showed + noShow) > 0 ? Math.round(showed / (showed + noShow) * 100) : 0;

  // Efficacy: connected people with a known show outcome
  const connWithOutcome = reminderPeople.filter(p => p.isConnected && (p.showStatus === 'Showed' || p.showStatus === 'No-Show'));
  const yesGroup = connWithOutcome.filter(p => p.confirmedInCall === 'Yes');
  const noGroup  = connWithOutcome.filter(p => p.confirmedInCall === 'No');
  const yesShowed  = yesGroup.filter(p => p.showStatus === 'Showed').length;
  const yesNoShow  = yesGroup.filter(p => p.showStatus === 'No-Show').length;
  const noShowed   = noGroup.filter(p => p.showStatus === 'Showed').length;
  const noNoShow   = noGroup.filter(p => p.showStatus === 'No-Show').length;
  const yesSR = yesGroup.length > 0 ? Math.round(yesShowed / yesGroup.length * 100) : 0;
  const noSR  = noGroup.length  > 0 ? Math.round(noShowed  / noGroup.length  * 100) : 0;
  const uplift = yesSR - noSR;

  function rcPct(n, d) { return d > 0 ? Math.round(n / d * 100) : 0; }

  // AE list for filter
  const aeList = [...new Set(reminderPeople.map(p => p.ae).filter(Boolean))].sort();
  const aeOpts = aeList.map(a => `<option value="${a.replace(/"/g,'&quot;')}">${a}</option>`).join('');

  // Table rows (one per person)
  const rcRows = reminderPeople.map((p, idx) => {
    const sCls = p.showStatus === 'Showed' ? 'bg' : p.showStatus === 'No-Show' ? 'br' : p.showStatus === 'Pending' ? 'by' : 'bn';
    const sLabel = p.showStatus || '—';
    const cCls   = p.confirmedInCall === 'Yes' ? 'bg' : p.confirmedInCall === 'No' ? 'br' : 'bn';
    const cLabel = p.confirmedInCall || (p.isConnected ? 'N/A' : '—');
    const connBadge = p.isConnected
      ? '<span class="bd bg">Connected</span>'
      : '<span class="bd bn">Not Reached</span>';
    const summaryShort = p.lastSummary.length > 70 ? p.lastSummary.slice(0, 70) + '…' : p.lastSummary;
    const rowDate = p.lastCallDate ? p.lastCallDate.slice(0, 10) : '';
    return `<tr class="rc-row"
      data-show="${p.showStatus || 'none'}"
      data-confirmed="${p.confirmedInCall || 'unknown'}"
      data-ae="${(p.ae || '').replace(/"/g,'&quot;')}"
      data-search="${(p.name + ' ' + p.company).toLowerCase()}"
      data-date="${rowDate}"
      data-idx="${idx}"
      onclick="toggleRcExpand(this)">
      <td style="font-weight:500;white-space:nowrap">${p.name}</td>
      <td style="font-size:12px;max-width:160px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${p.company}">${p.company}</td>
      <td style="font-size:12px;white-space:nowrap;color:#6b7280">${p.apptDate}${p.apptTime ? '<br><span style="font-size:11px;color:#9ca3af">' + p.apptTime + '</span>' : ''}</td>
      <td style="font-size:12px;color:#6b7280">${p.ae || '—'}</td>
      <td style="text-align:center"><span style="display:inline-block;background:#f3f4f6;color:#374151;font-size:11px;font-weight:600;padding:2px 8px;border-radius:10px">${p.callsMade}</span></td>
      <td style="font-size:11px;color:#9ca3af;white-space:nowrap">${p.lastCallDate ? p.lastCallDate.slice(0, 16) : '—'}</td>
      <td>${connBadge}</td>
      <td>${p.confirmedInCall ? '<span class="bd ' + cCls + '">' + cLabel + '</span>' : '<span style="color:#d1d5db">—</span>'}</td>
      <td>${p.showStatus ? '<span class="bd ' + sCls + '">' + sLabel + '</span>' : '<span style="color:#d1d5db">—</span>'}</td>
      <td style="font-size:11px;color:#6b7280;max-width:200px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${p.lastSummary}">${summaryShort || '<span style="color:#d1d5db">—</span>'}</td>
      <td style="text-align:center">${p.lastRecording ? `<button onclick="event.stopPropagation();playAiRecording(this,'${p.lastRecording}')" style="background:none;border:none;cursor:pointer;font-size:16px;padding:2px 6px" title="Play recording">▶</button>` : '<span style="color:#d1d5db">—</span>'}</td>
    </tr>`;
  }).join('');

  return `
<!-- ═══════════════════ REMINDER CALLS ═══════════════════════════════════════ -->
<div class="page active" id="page-reminder">
  <div class="ph">
    <div class="pt">Reminder Calls</div>
    <div class="ps">AI agent (Sarah) calls booked leads before their demo — filter by date to see a period</div>
  </div>

  <!-- DATE FILTER FIRST -->
  <div class="fbar" style="margin-bottom:20px">
    <button class="pbtn" onclick="setRcDate('today',this)">Today</button>
    <button class="pbtn" onclick="setRcDate('yesterday',this)">Yesterday</button>
    <button class="pbtn" onclick="setRcDate('7days',this)">Last 7 Days</button>
    <button class="pbtn" onclick="setRcDate('30days',this)">Last 30 Days</button>
    <button class="pbtn" onclick="setRcDate('thismonth',this)">This Month</button>
    <button class="pbtn" onclick="setRcDate('lastmonth',this)">Last Month</button>
    <button class="pbtn active" onclick="setRcDate('all',this)">All Time</button>
    <span class="fsep">|</span>
    <input type="date" id="rc-from" onchange="setRcDate('custom',null)" style="color-scheme:light">
    <span style="font-size:12px;color:#9ca3af">–</span>
    <input type="date" id="rc-to" onchange="setRcDate('custom',null)" style="color-scheme:light">
  </div>

  ${(() => {
    const resolved      = showed + noShow;
    const connWithOut   = reminderPeople.filter(p => p.isConnected  && (p.showStatus === 'Showed' || p.showStatus === 'No-Show'));
    const noConnWithOut = reminderPeople.filter(p => !p.isConnected && (p.showStatus === 'Showed' || p.showStatus === 'No-Show'));
    const connSR        = connWithOut.length   > 0 ? Math.round(connWithOut.filter(p => p.showStatus === 'Showed').length   / connWithOut.length   * 100) : 0;
    const noConnSR      = noConnWithOut.length > 0 ? Math.round(noConnWithOut.filter(p => p.showStatus === 'Showed').length / noConnWithOut.length * 100) : 0;
    const connDelta     = connSR - noConnSR;
    const liftColor     = connDelta > 3 ? '#16a34a' : connDelta < -3 ? '#dc2626' : '#6b7280';
    const liftLabel     = connDelta > 3 ? '+' + connDelta + 'pp lift' : connDelta < -3 ? connDelta + 'pp' : '~0pp lift';
    const connVerdict   = Math.abs(connDelta) <= 3
      ? 'Connecting makes no difference — script or call timing may need work'
      : connDelta > 3 ? 'Connecting lifts show rate by +' + connDelta + 'pp — the call is working'
      : 'Not-reached leads show up more — investigate why';

    const yesOut = reminderPeople.filter(p => p.isConnected && p.confirmedInCall === 'Yes' && (p.showStatus === 'Showed' || p.showStatus === 'No-Show'));
    const noOut  = reminderPeople.filter(p => p.isConnected && p.confirmedInCall === 'No'  && (p.showStatus === 'Showed' || p.showStatus === 'No-Show'));
    const yesSR  = yesOut.length > 0 ? Math.round(yesOut.filter(p => p.showStatus === 'Showed').length / yesOut.length * 100) : 0;
    const noSR   = noOut.length  > 0 ? Math.round(noOut.filter(p => p.showStatus === 'Showed').length  / noOut.length  * 100) : 0;
    const cfmD   = yesSR - noSR;
    const cfmVerdict = Math.abs(cfmD) <= 3
      ? "Confirmation doesn't predict show-up — don't over-optimise for it"
      : cfmD > 3 ? 'Getting a "Yes" lifts show rate by +' + cfmD + 'pp — push for confirmation on every call'
      : "Those who don't confirm show up more — confirmation may be a weak signal";

    const connShowedN   = connWithOut.filter(p => p.showStatus === 'Showed').length;
    const noConnShowedN = noConnWithOut.filter(p => p.showStatus === 'Showed').length;

    return `
  <!-- KPI CARDS -->
  <div class="krow" style="grid-template-columns:repeat(5,1fr);margin-bottom:20px">
    <div class="kcard">
      <div class="klabel">Total Calls Made</div>
      <div class="kval" id="rc-kpi-sent">${totalSent}</div>
      <div class="ksub" id="rc-kpi-sent-sub">${reminderPeople.length} unique people</div>
    </div>
    <div class="kcard">
      <div class="klabel">Pickup Rate</div>
      <div class="kval" id="rc-kpi-conn">${connectedPct}%</div>
      <div class="ksub" id="rc-kpi-conn-sub">${totalConnected} of ${totalSent} answered</div>
    </div>
    <div class="kcard">
      <div class="klabel">Overall Show Rate</div>
      <div class="kval" style="color:${showRate >= 50 ? '#16a34a' : '#d97706'}" id="rc-kpi-showed">${showRate}%</div>
      <div class="ksub" id="rc-kpi-showed-sub">${showed} showed · ${noShow} no-show</div>
    </div>
    <div class="kcard">
      <div class="klabel">Agent Lift</div>
      <div class="kval" style="color:${liftColor}" id="rc-kpi-conn-show">${liftLabel}</div>
      <div class="ksub" id="rc-kpi-conn-show-sub">connected ${connSR}% vs not reached ${noConnSR}%</div>
    </div>
    <div class="kcard">
      <div class="klabel">Pending</div>
      <div class="kval" style="color:#d97706" id="rc-kpi-pending">${pending}</div>
      <div class="ksub">upcoming demos</div>
    </div>
  </div>

  <!-- FUNNEL 1: Overall — Booked → Showed / No-Show / Pending -->
  <div style="background:#fff;border:1px solid #e5e7eb;border-radius:10px;padding:18px 24px;margin-bottom:12px">
    <div style="font-size:11px;font-weight:600;text-transform:uppercase;letter-spacing:.06em;color:#9ca3af;margin-bottom:14px">Overall — what happened to every booked demo</div>
    <div style="display:flex;align-items:center;gap:0">
      <!-- Step: Booked -->
      <div style="text-align:center;min-width:110px">
        <div style="font-size:28px;font-weight:700;color:#111827" id="rc-f1-booked">${totalSent}</div>
        <div style="font-size:12px;color:#6b7280;margin-top:2px">Booked Demos</div>
      </div>
      <!-- Bar breakdown -->
      <div style="flex:1;margin:0 20px">
        <div style="display:flex;height:28px;border-radius:6px;overflow:hidden;margin-bottom:8px">
          <div id="rc-f1-show-bar" style="background:#16a34a;width:${resolved > 0 ? Math.round(showed/totalSent*100) : 0}%;transition:width .3s;display:flex;align-items:center;justify-content:center">
            ${showed/totalSent > 0.12 ? '<span style="font-size:11px;font-weight:600;color:#fff">' + Math.round(showed/totalSent*100) + '%</span>' : ''}
          </div>
          <div id="rc-f1-ns-bar" style="background:#fca5a5;width:${resolved > 0 ? Math.round(noShow/totalSent*100) : 0}%;transition:width .3s;display:flex;align-items:center;justify-content:center">
            ${noShow/totalSent > 0.12 ? '<span style="font-size:11px;font-weight:600;color:#dc2626">' + Math.round(noShow/totalSent*100) + '%</span>' : ''}
          </div>
          <div id="rc-f1-p-bar" style="background:#fde68a;width:${Math.round(pending/totalSent*100)}%;transition:width .3s"></div>
        </div>
        <div style="display:flex;gap:20px">
          <div style="display:flex;align-items:center;gap:5px"><div style="width:10px;height:10px;background:#16a34a;border-radius:2px"></div><span style="font-size:12px;color:#374151"><strong id="rc-f1-showed">${showed}</strong> Showed (<span id="rc-f1-show-pct">${showRate}%</span>)</span></div>
          <div style="display:flex;align-items:center;gap:5px"><div style="width:10px;height:10px;background:#fca5a5;border-radius:2px"></div><span style="font-size:12px;color:#374151"><strong id="rc-f1-noshow">${noShow}</strong> No-Show (<span id="rc-f1-ns-pct">${resolved > 0 ? 100 - showRate : 0}%</span>)</span></div>
          <div style="display:flex;align-items:center;gap:5px"><div style="width:10px;height:10px;background:#fde68a;border-radius:2px"></div><span style="font-size:12px;color:#374151"><strong id="rc-f1-pending">${pending}</strong> Pending</span></div>
        </div>
      </div>
    </div>
  </div>

  <!-- FUNNEL 2: Agent — Called → Picked Up → Showed (split by reached/not) -->
  <div style="background:#fff;border:1px solid #e5e7eb;border-radius:10px;padding:18px 24px;margin-bottom:20px">
    <div style="font-size:11px;font-weight:600;text-transform:uppercase;letter-spacing:.06em;color:#9ca3af;margin-bottom:14px">Agent impact — does picking up the call change the outcome?</div>
    <!-- Row A: Connected path -->
    <div style="display:flex;align-items:center;gap:0;margin-bottom:10px">
      <div style="width:110px;text-align:center">
        <div style="font-size:18px;font-weight:700;color:#2563eb" id="rc-f2-conn">${totalConnected}</div>
        <div style="font-size:11px;color:#6b7280">Picked Up</div>
        <div style="font-size:10px;color:#9ca3af" id="rc-f2-conn-pct">${connectedPct}% of called</div>
      </div>
      <div style="flex:1;margin:0 16px">
        <div style="display:flex;align-items:center;gap:10px">
          <div style="flex:1;height:18px;background:#f3f4f6;border-radius:4px;overflow:hidden">
            <div id="rc-f2-conn-bar" style="height:100%;background:#16a34a;width:${connSR}%;transition:width .3s;border-radius:4px"></div>
          </div>
          <span style="font-size:20px;font-weight:700;color:#16a34a;min-width:48px" id="rc-kpi-conn-show">${connSR}%</span>
          <span style="font-size:12px;color:#6b7280" id="rc-kpi-conn-show-sub">${connShowedN} of ${connWithOut.length} showed</span>
        </div>
      </div>
    </div>
    <!-- Row B: Not reached path -->
    <div style="display:flex;align-items:center;gap:0">
      <div style="width:110px;text-align:center">
        <div style="font-size:18px;font-weight:700;color:#9ca3af" id="rc-f2-notconn">${totalSent - totalConnected}</div>
        <div style="font-size:11px;color:#6b7280">Not Reached</div>
        <div style="font-size:10px;color:#9ca3af" id="rc-f2-notconn-pct">${100 - connectedPct}% of called</div>
      </div>
      <div style="flex:1;margin:0 16px">
        <div style="display:flex;align-items:center;gap:10px">
          <div style="flex:1;height:18px;background:#f3f4f6;border-radius:4px;overflow:hidden">
            <div id="rc-f2-noconn-bar" style="height:100%;background:#9ca3af;width:${noConnSR}%;transition:width .3s;border-radius:4px"></div>
          </div>
          <span style="font-size:20px;font-weight:700;color:#6b7280;min-width:48px" id="rc-kpi-noconn-show">${noConnSR}%</span>
          <span style="font-size:12px;color:#6b7280" id="rc-kpi-noconn-show-sub">${noConnShowedN} of ${noConnWithOut.length} showed</span>
        </div>
      </div>
    </div>
    <!-- Verdict -->
    <div id="rc-conn-verdict" style="margin-top:14px;padding:10px 14px;background:#f9fafb;border-radius:6px;font-size:13px;color:#374151;border-left:3px solid ${liftColor}">${connVerdict}</div>
  </div>

  <!-- INSIGHT: Does confirmation predict show-up? -->
  <div style="background:#fff;border:1px solid #e5e7eb;border-radius:10px;padding:18px 24px;margin-bottom:20px">
    <div style="font-size:11px;font-weight:600;text-transform:uppercase;letter-spacing:.06em;color:#9ca3af;margin-bottom:14px">Script signal — does saying "Yes" on the call predict show-up? (connected calls only)</div>
    <div style="display:grid;grid-template-columns:1fr 1fr;gap:12px">
      <div style="border:1px solid #e5e7eb;border-radius:8px;padding:14px 18px">
        <div style="font-size:11px;color:#9ca3af;margin-bottom:8px">Said "Yes, I'll be there" (${yesOut.length} people)</div>
        <div style="display:flex;align-items:center;gap:10px">
          <div style="flex:1;height:14px;background:#f3f4f6;border-radius:4px;overflow:hidden"><div id="rc-eff-yes-bar" style="height:100%;background:#16a34a;width:${yesSR}%;border-radius:4px"></div></div>
          <span style="font-size:22px;font-weight:700;color:#16a34a;min-width:52px" id="rc-eff-yes-showed">${yesSR}%</span>
        </div>
        <div style="font-size:11px;color:#6b7280;margin-top:4px" id="rc-eff-yes-count">${yesOut.filter(p=>p.showStatus==='Showed').length} of ${yesOut.length} showed</div>
      </div>
      <div style="border:1px solid #e5e7eb;border-radius:8px;padding:14px 18px;background:#fafafa">
        <div style="font-size:11px;color:#9ca3af;margin-bottom:8px">Did not confirm on call (${noOut.length} people)</div>
        <div style="display:flex;align-items:center;gap:10px">
          <div style="flex:1;height:14px;background:#f3f4f6;border-radius:4px;overflow:hidden"><div id="rc-eff-no-bar" style="height:100%;background:#9ca3af;width:${noSR}%;border-radius:4px"></div></div>
          <span style="font-size:22px;font-weight:700;color:#6b7280;min-width:52px" id="rc-eff-no-showed">${noSR}%</span>
        </div>
        <div style="font-size:11px;color:#6b7280;margin-top:4px" id="rc-eff-no-count">${noOut.filter(p=>p.showStatus==='Showed').length} of ${noOut.length} showed</div>
      </div>
    </div>
    <div id="rc-eff-verdict" style="margin-top:12px;padding:10px 14px;background:#f9fafb;border-radius:6px;font-size:13px;color:#374151;border-left:3px solid ${Math.abs(cfmD) <= 3 ? '#9ca3af' : cfmD > 3 ? '#16a34a' : '#dc2626'}">${cfmVerdict}</div>
  </div>`;
  })()}

  <!-- FILTER BAR -->
  <div class="fbar" style="margin-bottom:8px">
    <input type="search" id="rc-search" placeholder="Search name or company…" oninput="applyRc()" style="min-width:200px">
    <span class="fsep">|</span>
    <select id="rc-ae" onchange="applyRc()"><option value="">All AEs</option>${aeOpts}</select>
    <select id="rc-confirmed" onchange="applyRc()">
      <option value="">All — Confirmed?</option>
      <option value="Yes">Said Yes</option>
      <option value="No">Did Not Confirm</option>
    </select>
    <button class="fclear" id="rc-clear" onclick="clearRc()" style="display:none">✕ Clear filters</button>
    <span class="rcnt" id="rc-count" style="margin-left:4px"></span>
  </div>
  <div class="prow">
    <button class="pbtn active" data-rcs="all"     onclick="setRcStatus('all',this)">All <span style="font-size:10px;opacity:.6">${totalSent}</span></button>
    <button class="pbtn"        data-rcs="Showed"   onclick="setRcStatus('Showed',this)">Showed <span style="font-size:10px;opacity:.6">${showed}</span></button>
    <button class="pbtn"        data-rcs="No-Show"  onclick="setRcStatus('No-Show',this)">No-Show <span style="font-size:10px;opacity:.6">${noShow}</span></button>
    <button class="pbtn"        data-rcs="Pending"  onclick="setRcStatus('Pending',this)">Pending <span style="font-size:10px;opacity:.6">${pending}</span></button>
  </div>

  <!-- TABLE -->
  <div class="tc">
    <table>
      <thead><tr>
        <th>Name</th>
        <th>Company</th>
        <th>Appointment</th>
        <th>AE</th>
        <th style="text-align:center">Calls</th>
        <th>Last Called</th>
        <th>Connected</th>
        <th>Confirmed</th>
        <th>Show Status</th>
        <th>Summary</th>
        <th>Recording</th>
      </tr></thead>
      <tbody id="rc-tbody">${rcRows}</tbody>
    </table>
  </div>
</div>`;
})()}

<script>
// ── Navigation ────────────────────────────────────────────────────────────────
function showPage(id, btn) {
  document.querySelectorAll('.page').forEach(p => p.classList.remove('active'));
  document.querySelectorAll('.ntab').forEach(t => t.classList.remove('active'));
  document.getElementById('page-' + id).classList.add('active');
  if (btn) btn.classList.add('active');
}

// ── Data ──────────────────────────────────────────────────────────────────────
const _allCombined    = ${safeJson(combined)};
const _activeCombined = ${safeJson(combinedActive)};
const _filterData     = ${filterDataJs};
const _rawLeads       = ${safeJson(allLeads)};
const _dailySpend     = ${safeJson(dailySpend)};
const _acctDailySpend = ${safeJson(accountDailySpend)};

// Build hierarchy for cascading dropdowns (campaign → adset → ad)
const _alHierarchy = {};
const _dfHierarchy = {};
for (const l of _rawLeads) {
  if (!_alHierarchy[l.campaign]) _alHierarchy[l.campaign] = {};
  if (!_alHierarchy[l.campaign][l.adset]) _alHierarchy[l.campaign][l.adset] = new Set();
  _alHierarchy[l.campaign][l.adset].add(l.ad);
}
for (const c in _alHierarchy) for (const as in _alHierarchy[c]) _alHierarchy[c][as] = [..._alHierarchy[c][as]].sort();

const _bookedLeads = ${safeJson(bookedLeads)};
for (const l of _bookedLeads) {
  if (!_dfHierarchy[l.campaign]) _dfHierarchy[l.campaign] = {};
  if (!_dfHierarchy[l.campaign][l.adset]) _dfHierarchy[l.campaign][l.adset] = new Set();
  _dfHierarchy[l.campaign][l.adset].add(l.ad);
}
for (const c in _dfHierarchy) for (const as in _dfHierarchy[c]) _dfHierarchy[c][as] = [..._dfHierarchy[c][as]].sort();

// ── Column filters (multi-select) ────────────────────────────────────────────
const _cf = { campaign: new Set(), adset: new Set(), ad: new Set(), status: new Set(), demo: new Set(), show: new Set(), revenue: new Set() };

function toggleCF(col, evt) {
  evt.stopPropagation();
  const panel = document.getElementById('cf-' + col);
  const btn   = document.getElementById('cfbtn-' + col);
  const isOpen = panel.classList.contains('open');
  // Close all open panels first
  document.querySelectorAll('.cf-panel').forEach(p => p.classList.remove('open'));
  if (!isOpen) {
    // Position panel below the button
    const rect = btn.getBoundingClientRect();
    panel.style.top  = (rect.bottom + 4) + 'px';
    panel.style.left = Math.min(rect.left, window.innerWidth - 310) + 'px';
    panel.classList.add('open');
  }
}

// Close panels on outside click
document.addEventListener('click', function() {
  document.querySelectorAll('.cf-panel').forEach(p => p.classList.remove('open'));
});
// Prevent panel clicks from bubbling to document
document.querySelectorAll('.cf-panel').forEach(p => p.addEventListener('click', e => e.stopPropagation()));

function toggleCFVal(col, cb) {
  if (cb.checked) _cf[col].add(cb.value);
  else            _cf[col].delete(cb.value);
  document.getElementById('cfbtn-' + col).classList.toggle('active', _cf[col].size > 0);
  applyAl();
}

function selectAllCF(col) {
  document.querySelectorAll('#cflist-' + col + ' .cf-item').forEach(item => {
    if (item.style.display === 'none') return;
    const cb = item.querySelector('input');
    cb.checked = true;
    _cf[col].add(cb.value);
  });
  document.getElementById('cfbtn-' + col).classList.toggle('active', _cf[col].size > 0);
  applyAl();
}

function clearCF(col) {
  _cf[col].clear();
  document.querySelectorAll('#cflist-' + col + ' input').forEach(cb => cb.checked = false);
  document.getElementById('cfbtn-' + col).classList.remove('active');
  applyAl();
}

function filterCFOpts(col, q) {
  const lq = q.toLowerCase();
  document.querySelectorAll('#cflist-' + col + ' .cf-item').forEach(item => {
    const label = item.querySelector('span')?.textContent || '';
    item.style.display = label.toLowerCase().includes(lq) ? '' : 'none';
  });
}

// ── ALL LEADS filtering ───────────────────────────────────────────────────────
let _alStatus = 'all';

function setAlStatus(s, btn) {
  _alStatus = s;
  document.querySelectorAll('.pbtn[data-s]').forEach(b => b.classList.remove('active'));
  btn.classList.add('active');
  applyAl();
}

function applyAl() {
  const q     = (document.getElementById('al-search').value || '').toLowerCase();
  const dfrom = document.getElementById('al-from').value;
  const dto   = document.getElementById('al-to').value;

  const hasFilt = q || dfrom || dto || _alStatus !== 'all' ||
    _cf.campaign.size > 0 || _cf.adset.size > 0 || _cf.ad.size > 0;
  document.getElementById('al-clear').style.display = hasFilt ? '' : 'none';

  let visible = 0;
  const filtered = [];
  document.querySelectorAll('#leads-tbody tr').forEach((row, i) => {
    const d = row.dataset;
    let show = true;
    if (q && !d.search.includes(q)) show = false;
    if (_cf.campaign.size > 0 && !_cf.campaign.has(d.campaign)) show = false;
    if (_cf.adset.size   > 0 && !_cf.adset.has(d.adset))       show = false;
    if (_cf.ad.size      > 0 && !_cf.ad.has(d.ad))             show = false;
    if (_cf.status.size  > 0 && !_cf.status.has(d.status))     show = false;
    if (_cf.demo.size    > 0 && !_cf.demo.has(d.demo))         show = false;
    if (_cf.show.size    > 0 && !_cf.show.has(d.show))         show = false;
    if (_cf.revenue.size > 0 && !_cf.revenue.has(d.closed))    show = false;
    if (dfrom && d.leaddate && d.leaddate < dfrom) show = false;
    if (dto   && d.leaddate && d.leaddate > dto)   show = false;
    if (_alStatus === 'qualified' && d.status  !== 'qualified') show = false;
    if (_alStatus === 'bad'       && d.status  !== 'bad')       show = false;
    if (_alStatus === 'demo'      && d.demo    !== 'yes')       show = false;
    if (_alStatus === 'closed'    && d.closed  !== 'yes')       show = false;
    row.style.display = show ? '' : 'none';
    if (show) { visible++; filtered.push(_rawLeads[i]); }
  });

  document.getElementById('al-count').textContent = visible + ' of ' + _rawLeads.length + ' leads';
  document.getElementById('al-rcnt').textContent  = visible + ' leads';
  _updateLeadKPIs(filtered, dfrom, dto);
}

function _updateLeadKPIs(leads, dfrom, dto) {
  const n      = leads.length;
  const qual   = leads.filter(l => l.isQual).length;
  const bad    = leads.filter(l => l.isBad).length;
  const booked = leads.filter(l => l.isDemo).length;
  const shows  = leads.filter(l => l.isShow).length;
  const closes = leads.filter(l => l.amount > 0).length;
  const mrr    = leads.reduce((s, l) => s + l.amount, 0);
  const qualRate  = n > 0 ? Math.round(qual / n * 100) : 0;
  const pctBooked = qual > 0 ? Math.round(booked / qual * 100) : 0;
  const pctShow   = booked > 0 ? Math.round(shows / booked * 100) : 0;
  const closePct  = shows > 0 ? Math.round(closes / shows * 100) : 0;
  const junkRate  = n > 0 ? (100 - qualRate) : 0;

  // Spend: account-level daily data for date range (captures all campaigns),
  // per-campaign daily data when a campaign filter is also active.
  let spendRaw = 0;
  if (dfrom || dto) {
    if (_cf.campaign.size === 0 && _cf.adset.size === 0 && _cf.ad.size === 0) {
      // No campaign filter — use account-level daily spend (includes every campaign)
      for (const day of _acctDailySpend) {
        if (dfrom && day.date < dfrom) continue;
        if (dto   && day.date > dto)   continue;
        spendRaw += day.spend;
      }
    } else {
      // Campaign/adset/ad filter active — sum per-campaign daily data
      for (const [kid, campData] of Object.entries(_filterData)) {
        if (_cf.campaign.size > 0 && !_cf.campaign.has(campData.name)) continue;
        for (const day of (_dailySpend[kid] || [])) {
          if (dfrom && day.date < dfrom) continue;
          if (dto   && day.date > dto)   continue;
          spendRaw += day.spend;
        }
      }
    }
  } else {
    // No date filter: precise adset/ad level from filterData
    for (const campData of Object.values(_filterData)) {
      if (_cf.campaign.size > 0 && !_cf.campaign.has(campData.name)) continue;
      if (_cf.adset.size === 0 && _cf.ad.size === 0) {
        spendRaw += campData.spendRaw;
      } else {
        for (const [asName, asData] of Object.entries(campData.adsets || {})) {
          if (_cf.adset.size > 0 && !_cf.adset.has(asName)) continue;
          if (_cf.ad.size === 0) {
            spendRaw += asData.spendRaw;
          } else {
            for (const [adName, adData] of Object.entries(asData.ads || {})) {
              if (_cf.ad.has(adName)) spendRaw += adData.spendRaw || 0;
            }
          }
        }
      }
    }
  }

  const $f = (v, d=0) => v > 0 ? '$' + Number(v).toLocaleString('en-US', {maximumFractionDigits:d}) : '—';
  const cpl = n > 0 && spendRaw > 0 ? '$' + (spendRaw/n).toFixed(2) : '—';

  document.getElementById('lk-spend').textContent     = $f(spendRaw, 0);
  document.getElementById('lk-spend-sub').textContent = '';
  document.getElementById('lk-leads').textContent     = n;
  document.getElementById('lk-cpl').textContent       = cpl + ' CPL';
  document.getElementById('lk-qual').textContent      = qual;
  document.getElementById('lk-qual-sub').textContent  = qualRate + '% qual rate';
  document.getElementById('lk-cq').textContent        = qual > 0 && spendRaw > 0 ? $f(spendRaw/qual, 0) : '—';
  document.getElementById('lk-booked').textContent    = booked;
  document.getElementById('lk-booked-sub').textContent= pctBooked + '% of qualified';
  document.getElementById('lk-cd').textContent        = booked > 0 && spendRaw > 0 ? $f(spendRaw/booked, 0) : '—';
  document.getElementById('lk-shows').textContent     = shows;
  document.getElementById('lk-shows-sub').textContent = pctShow + '% show rate';
  document.getElementById('lk-closes').textContent    = closes;
  document.getElementById('lk-closes-sub').textContent= closePct + '% of shows';
  document.getElementById('lk-arr').textContent       = $f(mrr * 12, 0);
  document.getElementById('lk-arr-sub').textContent   = $f(mrr, 0) + '/mo MRR';
  document.getElementById('lk-bad').textContent       = bad;
  document.getElementById('lk-bad-sub').textContent   = junkRate + '% junk rate';
}

function clearAl() {
  document.getElementById('al-search').value = '';
  document.getElementById('al-from').value   = '';
  document.getElementById('al-to').value     = '';
  // Clear column filters
  ['campaign','adset','ad','status','demo','show','revenue'].forEach(col => {
    _cf[col].clear();
    document.querySelectorAll('#cflist-' + col + ' input').forEach(cb => cb.checked = false);
    document.getElementById('cfbtn-' + col).classList.remove('active');
  });
  _alStatus = 'all';
  document.querySelectorAll('.pbtn[data-s]').forEach((b, i) => {
    b.classList.toggle('active', i === 0);
  });
  applyAl();
}

// ── Performance tab ───────────────────────────────────────────────────────────
function switchPerf(campId, btn) {
  document.querySelectorAll('.cpanel').forEach(p => p.classList.remove('active'));
  document.querySelectorAll('.ctab').forEach(b => b.classList.remove('active'));
  document.getElementById('perf-' + campId).classList.add('active');
  btn.classList.add('active');
}

// Store original row data for sorting (baked in at build time)
const _perfData = {};
${campKeys.map(k => `_perfData[${safeJson(k)}] = ${safeJson(buildCampAdData(funnels[k]))};`).join('\n')}

const _perfSort = {}; // campKey -> { col, asc }

function sortPerf(campKey, colIdx, th) {
  const prev = _perfSort[campKey];
  const asc = prev?.col === colIdx ? !prev.asc : false; // default desc
  _perfSort[campKey] = { col: colIdx, asc };

  // Update header arrows
  const table = document.getElementById('perf-' + campKey).querySelector('table');
  table.querySelectorAll('thead th').forEach(h => h.classList.remove('sort-asc','sort-desc'));
  th.classList.add(asc ? 'sort-asc' : 'sort-desc');

  const cols = ['spendRaw','metaLeads','total','qualified','qualRate','bookedCalls','shows','closes'];
  const key  = cols[colIdx] || 'spendRaw';

  const rows = [..._perfData[campKey]].map(r => ({
    ...r,
    qualRate: r.total > 0 ? Math.round(r.qualified / r.total * 100) : 0,
  })).sort((a, b) => asc ? (a[key] - b[key]) : (b[key] - a[key]));

  // Re-render tbody
  const tbody = document.getElementById('pbody-' + campKey);
  const $f = (v, d=0) => v > 0 ? '$' + Number(v).toLocaleString('en-US',{maximumFractionDigits:d}) : '—';
  tbody.innerHTML = rows.map(r => {
    const qr   = r.qualRate;
    const qCls = qr >= 50 ? 'bg' : qr >= 25 ? 'by' : 'br';
    const costQ = r.qualified > 0 && r.spendRaw > 0 ? $f(r.spendRaw/r.qualified) : '—';
    const cpl   = r.metaLeads > 0 && r.spendRaw > 0 ? '$'+(r.spendRaw/r.metaLeads).toFixed(2) : '—';
    const costD = r.bookedCalls > 0 && r.spendRaw > 0 ? $f(r.spendRaw/r.bookedCalls) : '—';
    const rev   = r.totalAmount > 0 ? '$'+(r.totalAmount*12).toLocaleString('en-US',{maximumFractionDigits:0})+'/yr' : '—';
    const cut   = r.total >= 3 && r.qualified === 0 && r.bad >= 2;
    return '<tr>' +
      '<td><div style="font-size:13px;font-weight:500;max-width:200px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="'+r.ad+'">'+r.ad+(cut?' <span class="bd bg" style="font-size:10px">Cut</span>':'')+'</div><div style="font-size:11px;color:#9ca3af;margin-top:2px;max-width:200px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="'+r.adset+'">'+r.adset+'</div></td>' +
      '<td>'+(r.spendRaw>0?$f(r.spendRaw):'—')+'</td>' +
      '<td>'+(r.metaLeads||'—')+'</td>' +
      '<td>'+cpl+'</td>' +
      '<td>'+(r.total||'—')+'</td>' +
      '<td>'+(r.qualified||'—')+'</td>' +
      '<td>'+(r.total>0?'<span class="bd '+qCls+'">'+qr+'%</span>':'—')+'</td>' +
      '<td>'+costQ+'</td>' +
      '<td>'+(r.bookedCalls||'—')+'</td>' +
      '<td>'+costD+'</td>' +
      '<td>'+(r.shows||'—')+'</td>' +
      '<td>'+(r.closes||'—')+'</td>' +
      '<td>'+rev+'</td>' +
    '</tr>';
  }).join('');
}

// ── Lead table column sort ────────────────────────────────────────────────────
let _leadsSort = { col: 0, asc: false };

function sortLeads(colIdx, th) {
  const asc = _leadsSort.col === colIdx ? !_leadsSort.asc : false;
  _leadsSort = { col: colIdx, asc };
  document.querySelectorAll('#leads-table thead th').forEach(h => h.classList.remove('sort-asc','sort-desc'));
  th.classList.add(asc ? 'sort-asc' : 'sort-desc');

  const tbody = document.getElementById('leads-tbody');
  const rows  = [...tbody.querySelectorAll('tr')].filter(r => r.style.display !== 'none');
  const allRows = [...tbody.querySelectorAll('tr')];

  rows.sort((a, b) => {
    const av = a.cells[colIdx]?.textContent.trim() || '';
    const bv = b.cells[colIdx]?.textContent.trim() || '';
    const an = parseFloat(av.replace(/[^0-9.-]/g,''));
    const bn = parseFloat(bv.replace(/[^0-9.-]/g,''));
    if (!isNaN(an) && !isNaN(bn)) return asc ? an - bn : bn - an;
    return asc ? av.localeCompare(bv) : bv.localeCompare(av);
  });

  // Re-append sorted visible rows, hidden rows stay hidden
  rows.forEach(r => tbody.appendChild(r));
}

// ── Demos / Booked calls ──────────────────────────────────────────────────────
let _dfStatus = 'all';

function setDfStatus(s, btn) {
  _dfStatus = s;
  document.querySelectorAll('#page-funnel .pbtn').forEach(b => b.classList.remove('active'));
  btn.classList.add('active');
  applyDemos();
}

function onDfCamp() {
  const camp  = document.getElementById('df-camp').value;
  const asel  = document.getElementById('df-adset');
  const adsel = document.getElementById('df-ad');
  asel.innerHTML  = '<option value="all">All Ad Sets</option>';
  adsel.innerHTML = '<option value="all">All Ads</option>';
  if (camp !== 'all') {
    _populateSel(asel, Object.keys(_dfHierarchy[camp] || {}).sort(), 'All Ad Sets');
    asel.disabled = false;
  } else { asel.disabled = true; }
  adsel.disabled = true;
  applyDemos();
}

function onDfAdset() {
  const camp  = document.getElementById('df-camp').value;
  const adset = document.getElementById('df-adset').value;
  const adsel = document.getElementById('df-ad');
  adsel.innerHTML = '<option value="all">All Ads</option>';
  if (adset !== 'all') {
    _populateSel(adsel, (_dfHierarchy[camp]?.[adset] || []), 'All Ads');
    adsel.disabled = false;
  } else { adsel.disabled = true; }
  applyDemos();
}

function applyDemos() {
  const q    = (document.getElementById('df-search')?.value || '').toLowerCase();
  const camp = document.getElementById('df-camp')?.value  || 'all';
  const adset= document.getElementById('df-adset')?.value || 'all';
  const ad   = document.getElementById('df-ad')?.value    || 'all';
  let visible = 0;
  document.querySelectorAll('#demos-tbody tr').forEach(row => {
    const d = row.dataset;
    const show =
      (_dfStatus === 'all'  || d.status   === _dfStatus) &&
      (camp   === 'all'     || d.campaign === camp)      &&
      (adset  === 'all'     || d.adset    === adset)     &&
      (ad     === 'all'     || d.ad       === ad)        &&
      (!q                   || d.search.includes(q));
    row.style.display = show ? '' : 'none';
    if (show) visible++;
  });
  const total = document.querySelectorAll('#demos-tbody tr').length;
  const cnt   = document.getElementById('df-cnt');
  if (cnt) cnt.textContent = visible + ' of ' + total;
}

// ── Password protect ──────────────────────────────────────────────────────────
function checkPw() {
  const pw  = document.getElementById('demos-pw').value;
  const inp = document.getElementById('demos-pw');
  const err = document.getElementById('demos-pw-err');
  if (pw === 'gushwork2026') {
    document.getElementById('demos-pw-wrap').classList.remove('locked');
  } else {
    inp.classList.add('err');
    err.style.display = 'block';
    setTimeout(() => { inp.classList.remove('err'); err.style.display = 'none'; }, 2000);
  }
}

// ── AI Calls tab ─────────────────────────────────────────────────────────────
const _aiCalls = ${safeJson(aiCalls)};
let _aiDateFrom = '', _aiDateTo = '', _aiCampaign = 'all';
const _aiCF = { campaign: new Set(), ad: new Set(), outcome: new Set(), sentiment: new Set() };

// ── AI column filters ────────────────────────────────────────────────────────
function toggleAiCF(col, evt) {
  evt.stopPropagation();
  const panel = document.getElementById('cf-ai-' + col);
  const btn   = document.getElementById('cfbtn-ai-' + col);
  const isOpen = panel.classList.contains('open');
  document.querySelectorAll('.cf-panel').forEach(p => p.classList.remove('open'));
  if (!isOpen) {
    const rect = btn.getBoundingClientRect();
    panel.style.top  = (rect.bottom + 4) + 'px';
    panel.style.left = Math.min(rect.left, window.innerWidth - 310) + 'px';
    panel.classList.add('open');
  }
}

function toggleAiCFVal(col, cb) {
  if (cb.checked) _aiCF[col].add(cb.value);
  else            _aiCF[col].delete(cb.value);
  document.getElementById('cfbtn-ai-' + col).classList.toggle('active', _aiCF[col].size > 0);
  applyAiFilters();
}

function selectAllAiCF(col) {
  document.querySelectorAll('#cflist-ai-' + col + ' .cf-item').forEach(item => {
    if (item.style.display === 'none') return;
    const cb = item.querySelector('input');
    cb.checked = true;
    _aiCF[col].add(cb.value);
  });
  document.getElementById('cfbtn-ai-' + col).classList.toggle('active', _aiCF[col].size > 0);
  applyAiFilters();
}

function clearAiCF(col) {
  _aiCF[col].clear();
  document.querySelectorAll('#cflist-ai-' + col + ' input').forEach(cb => cb.checked = false);
  document.getElementById('cfbtn-ai-' + col).classList.remove('active');
  applyAiFilters();
}

function filterAiCFOpts(col, q) {
  const lq = q.toLowerCase();
  document.querySelectorAll('#cflist-ai-' + col + ' .cf-item').forEach(item => {
    const label = item.querySelector('span')?.textContent || '';
    item.style.display = label.toLowerCase().includes(lq) ? '' : 'none';
  });
}

// ── AI audio player ──────────────────────────────────────────────────────────
let _aiAudio = null;
let _aiPlayingBtn = null;

function playAiRecording(btn, url) {
  // If same button clicked again, toggle pause/play
  if (_aiAudio && _aiPlayingBtn === btn) {
    if (_aiAudio.paused) { _aiAudio.play(); btn.textContent = '⏸'; }
    else { _aiAudio.pause(); btn.textContent = '▶'; }
    return;
  }
  // Stop any existing playback
  if (_aiAudio) { _aiAudio.pause(); _aiAudio = null; if (_aiPlayingBtn) _aiPlayingBtn.textContent = '▶'; }
  // Start new
  _aiAudio = new Audio(url);
  _aiPlayingBtn = btn;
  btn.textContent = '⏸';
  _aiAudio.play();
  _aiAudio.addEventListener('ended', () => { btn.textContent = '▶'; _aiAudio = null; _aiPlayingBtn = null; });
  _aiAudio.addEventListener('error', () => { btn.textContent = '▶'; _aiAudio = null; _aiPlayingBtn = null; });
}

// ── AI date presets ──────────────────────────────────────────────────────────
function setAiDate(preset, btn) {
  document.querySelectorAll('#page-aicalls .fbar .pbtn').forEach(b => b.classList.remove('active'));
  btn.classList.add('active');
  const now = new Date();
  const today = now.toISOString().slice(0, 10);
  const y = new Date(now); y.setDate(y.getDate() - 1);
  const yesterday = y.toISOString().slice(0, 10);
  const dow = now.getDay();
  const mon = new Date(now); mon.setDate(mon.getDate() - ((dow + 6) % 7));
  const thisWeekStart = mon.toISOString().slice(0, 10);
  const lastMon = new Date(mon); lastMon.setDate(lastMon.getDate() - 7);
  const lastSun = new Date(mon); lastSun.setDate(lastSun.getDate() - 1);

  switch (preset) {
    case 'today':     _aiDateFrom = today; _aiDateTo = today; break;
    case 'yesterday': _aiDateFrom = yesterday; _aiDateTo = yesterday; break;
    case 'thisweek':  _aiDateFrom = thisWeekStart; _aiDateTo = today; break;
    case 'lastweek':  _aiDateFrom = lastMon.toISOString().slice(0,10); _aiDateTo = lastSun.toISOString().slice(0,10); break;
    case 'thismonth': _aiDateFrom = today.slice(0,7) + '-01'; _aiDateTo = today; break;
    case 'lastmonth': {
      const lm = new Date(now.getFullYear(), now.getMonth() - 1, 1);
      const lme = new Date(now.getFullYear(), now.getMonth(), 0);
      _aiDateFrom = lm.toISOString().slice(0,10); _aiDateTo = lme.toISOString().slice(0,10); break;
    }
    default: _aiDateFrom = ''; _aiDateTo = '';
  }
  document.getElementById('ai-from').value = _aiDateFrom;
  document.getElementById('ai-to').value = _aiDateTo;
  applyAiFilters();
}

function setAiCampaign(camp, btn) {
  document.querySelectorAll('#page-aicalls .ctab').forEach(b => b.classList.remove('active'));
  btn.classList.add('active');
  _aiCampaign = camp;
  applyAiFilters();
}

function applyAiFilters() {
  const from = document.getElementById('ai-from').value || _aiDateFrom;
  const to   = document.getElementById('ai-to').value   || _aiDateTo;
  const hasColFilt = _aiCF.campaign.size > 0 || _aiCF.ad.size > 0 || _aiCF.outcome.size > 0 || _aiCF.sentiment.size > 0;
  const hasFilt = from || to || _aiCampaign !== 'all' || hasColFilt;
  document.getElementById('ai-clear').style.display = hasFilt ? '' : 'none';

  let visible = 0;
  const filtered = [];
  document.querySelectorAll('#ai-tbody .ai-row').forEach(row => {
    const d = row.dataset;
    let show = true;
    if (from && d.date < from) show = false;
    if (to   && d.date > to)   show = false;
    if (_aiCampaign !== 'all' && d.campaign !== _aiCampaign) show = false;
    if (_aiCF.campaign.size  > 0 && !_aiCF.campaign.has(d.campaign))   show = false;
    if (_aiCF.ad.size        > 0 && !_aiCF.ad.has(d.ad))               show = false;
    if (_aiCF.outcome.size   > 0 && !_aiCF.outcome.has(d.outcome))     show = false;
    if (_aiCF.sentiment.size > 0 && !_aiCF.sentiment.has(d.sentiment)) show = false;
    row.style.display = show ? '' : 'none';
    // Also hide any expanded detail row
    const detail = row.nextElementSibling;
    if (detail && detail.classList.contains('ai-detail')) detail.style.display = 'none';
    if (show) { visible++; filtered.push(_aiCalls[parseInt(d.idx)]); }
  });

  document.getElementById('ai-count').textContent = visible + ' calls';
  _updateAiKPIs(filtered);
}

function _updateAiKPIs(calls) {
  const total = calls.length;
  const connected = calls.filter(c => c.connected).length;
  const successful = calls.filter(c => c.successful).length;
  const voicemail = calls.filter(c => c.voicemail).length;
  const conn = calls.filter(c => c.connected);
  const avgDur = conn.length > 0 ? Math.round(conn.reduce((s, c) => s + c.duration, 0) / conn.length) : 0;

  document.getElementById('ai-kpi-total').textContent = total;
  document.getElementById('ai-kpi-total-sub').textContent = '';
  document.getElementById('ai-kpi-connected').textContent = connected;
  document.getElementById('ai-kpi-connected-sub').textContent = total > 0 ? Math.round(connected / total * 100) + '% of calls' : '0% of calls';
  document.getElementById('ai-kpi-successful').textContent = successful;
  document.getElementById('ai-kpi-successful-sub').textContent = connected > 0 ? Math.round(successful / connected * 100) + '% of connected' : '0% of connected';
  document.getElementById('ai-kpi-duration').textContent = avgDur + 's';
}

function clearAiFilters() {
  _aiDateFrom = ''; _aiDateTo = ''; _aiCampaign = 'all';
  document.getElementById('ai-from').value = '';
  document.getElementById('ai-to').value = '';
  document.querySelectorAll('#page-aicalls .fbar .pbtn').forEach((b, i, arr) => b.classList.toggle('active', i === arr.length - 2));
  document.querySelectorAll('#page-aicalls .ctab').forEach((b, i) => b.classList.toggle('active', i === 0));
  // Clear all column filters
  ['campaign','ad','outcome','sentiment'].forEach(col => {
    _aiCF[col].clear();
    document.querySelectorAll('#cflist-ai-' + col + ' input').forEach(cb => cb.checked = false);
    document.getElementById('cfbtn-ai-' + col).classList.remove('active');
  });
  applyAiFilters();
}

function toggleAiExpand(row) {
  const next = row.nextElementSibling;
  if (next && next.classList.contains('ai-detail')) {
    next.remove();
    return;
  }
  const idx = parseInt(row.dataset.idx);
  const c = _aiCalls[idx];
  if (!c) return;
  const detail = document.createElement('tr');
  detail.className = 'ai-detail';
  const transcript = (c.transcript || '').replace(/\\\\n/g, '<br>').replace(/\\n/g, '<br>');
  detail.innerHTML = '<td colspan="11" style="padding:12px 16px;background:#f9fafb;border-bottom:2px solid #e5e7eb">' +
    '<div style="display:grid;grid-template-columns:1fr 1fr;gap:16px;max-width:1200px">' +
    '<div><div style="font-size:10px;font-weight:500;text-transform:uppercase;color:#6b7280;margin-bottom:4px">Summary</div><div style="font-size:13px;color:#374151;line-height:1.5">' + (c.summary || '—') + '</div></div>' +
    '<div><div style="font-size:10px;font-weight:500;text-transform:uppercase;color:#6b7280;margin-bottom:4px">Transcript</div><div style="font-size:12px;color:#6b7280;line-height:1.6;max-height:200px;overflow-y:auto">' + (transcript || '—') + '</div></div>' +
    '</div>' +
    (c.recordingUrl ? '<div style="margin-top:10px"><a href="' + c.recordingUrl + '" target="_blank" style="font-size:12px;color:#2563eb;text-decoration:none">▶ Listen to full recording</a></div>' : '') +
    '</td>';
  row.after(detail);
}

let _aiSort = { col: 0, asc: false };

// ── Reminder Calls data & logic ───────────────────────────────────────────────
const _rcPeople       = ${safeJson(reminderPeople)};
const _rcTotalCalls   = ${reminderPeople._totalCallRows || reminderPeople.length};
const _rcConnCalls    = ${reminderPeople._connectedCallRows || reminderPeople.filter(p => p.isConnected).length};
let _rcStatus   = 'all';
let _rcDateFrom = '';
let _rcDateTo   = '';

function setRcDate(preset, btn) {
  // Deactivate all date preset buttons, activate clicked one
  document.querySelectorAll('#page-reminder .fbar .pbtn').forEach(b => b.classList.remove('active'));
  if (btn) btn.classList.add('active');

  const now = new Date();
  const pad = n => String(n).padStart(2,'0');
  const fmt = d => d.getFullYear()+'-'+pad(d.getMonth()+1)+'-'+pad(d.getDate());
  const today = fmt(now);
  const yest  = fmt(new Date(now.getFullYear(), now.getMonth(), now.getDate() - 1));

  switch (preset) {
    case 'today':     _rcDateFrom = today;     _rcDateTo = today; break;
    case 'yesterday': _rcDateFrom = yest;      _rcDateTo = yest;  break;
    case '7days':     _rcDateFrom = fmt(new Date(now.getFullYear(), now.getMonth(), now.getDate() - 6)); _rcDateTo = today; break;
    case '30days':    _rcDateFrom = fmt(new Date(now.getFullYear(), now.getMonth(), now.getDate() - 29)); _rcDateTo = today; break;
    case 'thismonth': _rcDateFrom = today.slice(0,7)+'-01'; _rcDateTo = today; break;
    case 'lastmonth': {
      const lm  = new Date(now.getFullYear(), now.getMonth() - 1, 1);
      const lme = new Date(now.getFullYear(), now.getMonth(), 0);
      _rcDateFrom = fmt(lm); _rcDateTo = fmt(lme); break;
    }
    case 'custom':
      _rcDateFrom = document.getElementById('rc-from').value || '';
      _rcDateTo   = document.getElementById('rc-to').value   || '';
      break;
    default: _rcDateFrom = ''; _rcDateTo = '';
  }

  // Sync date inputs to state (unless they drove the change)
  if (preset !== 'custom') {
    const fi = document.getElementById('rc-from'); if (fi) fi.value = _rcDateFrom;
    const ti = document.getElementById('rc-to');   if (ti) ti.value = _rcDateTo;
  }
  applyRc();
}

function setRcStatus(s, btn) {
  _rcStatus = s;
  document.querySelectorAll('.pbtn[data-rcs]').forEach(b => b.classList.remove('active'));
  btn.classList.add('active');
  applyRc();
}

function applyRc() {
  const q    = (document.getElementById('rc-search')?.value    || '').toLowerCase();
  const ae   = document.getElementById('rc-ae')?.value         || '';
  const con  = document.getElementById('rc-confirmed')?.value  || '';
  const from = _rcDateFrom;
  const to   = _rcDateTo;

  const hasFilt = q || ae || con || _rcStatus !== 'all' || from || to;
  const clrBtn  = document.getElementById('rc-clear');
  if (clrBtn) clrBtn.style.display = hasFilt ? '' : 'none';

  // Collect visible indices to recompute KPIs
  const visibleIdxs = [];
  document.querySelectorAll('#rc-tbody .rc-row').forEach(row => {
    const d = row.dataset;
    let show = true;
    if (q    && !d.search.includes(q))    show = false;
    if (ae   && d.ae        !== ae)        show = false;
    if (con  && d.confirmed !== con)       show = false;
    if (_rcStatus !== 'all' && d.show !== _rcStatus) show = false;
    if (from && d.date && d.date < from)   show = false;
    if (to   && d.date && d.date > to)     show = false;
    row.style.display = show ? '' : 'none';
    if (!show) {
      const next = row.nextElementSibling;
      if (next && next.classList.contains('rc-detail')) next.remove();
    }
    if (show) visibleIdxs.push(parseInt(d.idx));
  });

  const cnt = document.getElementById('rc-count');
  if (cnt) cnt.textContent = visibleIdxs.length + ' people';

  // Recompute KPIs from visible people
  const vis = visibleIdxs.map(i => _rcPeople[i]).filter(Boolean);
  const isFiltered = vis.length < _rcPeople.length;
  // Use raw call counts when unfiltered (matches the sheet), person counts when filtered
  const sent      = isFiltered ? vis.reduce((a, p) => a + (p.callsMade || 1), 0) : _rcTotalCalls;
  const connected = isFiltered ? vis.filter(p => p.isConnected).length : _rcConnCalls;
  const connPct   = sent > 0 ? Math.round(connected / sent * 100) : 0;
  const showed    = vis.filter(p => p.showStatus === 'Showed').length;
  const noShow    = vis.filter(p => p.showStatus === 'No-Show').length;
  const pending   = vis.filter(p => p.showStatus === 'Pending').length;
  const showRate  = (showed + noShow) > 0 ? Math.round(showed / (showed + noShow) * 100) : 0;

  function s(id, v) { const el = document.getElementById(id); if (el) el.textContent = v; }
  function h(id, v) { const el = document.getElementById(id); if (el) el.innerHTML = v; }
  function w(id, pct) { const el = document.getElementById(id); if (el) el.style.width = pct + '%'; }
  function pct(n, d) { return d > 0 ? Math.round(n / d * 100) : 0; }

  s('rc-kpi-sent',    sent);
  s('rc-kpi-conn',    connected);
  s('rc-kpi-conn-sub', connPct + '% of calls reached');
  s('rc-kpi-showed',  showed);
  s('rc-kpi-showed-sub', showRate + '% show rate');
  s('rc-kpi-noshow',  noShow);
  s('rc-kpi-noshow-sub', (showed + noShow > 0 ? 100 - showRate : 0) + '% of resolved bookings');
  s('rc-kpi-pending', pending);

  // Panel 1: Connected vs Not Reached show rates
  const connOut    = vis.filter(p => p.isConnected  && (p.showStatus === 'Showed' || p.showStatus === 'No-Show'));
  const noConnOut  = vis.filter(p => !p.isConnected && (p.showStatus === 'Showed' || p.showStatus === 'No-Show'));
  const connShN    = connOut.filter(p => p.showStatus === 'Showed').length;
  const noConnShN  = noConnOut.filter(p => p.showStatus === 'Showed').length;
  const connSR2    = pct(connShN, connOut.length);
  const noConnSR2  = pct(noConnShN, noConnOut.length);
  const connDelta  = connSR2 - noConnSR2;
  s('rc-f2-conn',    connected);
  s('rc-f2-conn-pct', connPct + '% of called');
  s('rc-f2-notconn',  sent - connected);
  s('rc-f2-notconn-pct', (100 - connPct) + '% of called');
  s('rc-kpi-conn-show',       connSR2   + '%');
  s('rc-kpi-conn-show-sub',   connShN   + ' of ' + connOut.length   + ' showed');
  s('rc-kpi-noconn-show',     noConnSR2 + '%');
  s('rc-kpi-noconn-show-sub', noConnShN + ' of ' + noConnOut.length + ' showed');
  w('rc-f2-conn-bar',    connSR2);
  w('rc-f2-noconn-bar',  noConnSR2);
  s('rc-conn-verdict', Math.abs(connDelta) <= 3
    ? 'Reaching them made no difference — show rate is the same whether Sarah connects or not'
    : connDelta > 3
    ? 'Reaching leads lifts show rate by +' + connDelta + 'pp vs. those not reached'
    : 'Leads NOT reached showed up more — something else is driving show-up');

  // Panel 2: Confirmed vs Not Confirmed show rates
  const withOutcome = vis.filter(p => p.isConnected && (p.showStatus === 'Showed' || p.showStatus === 'No-Show'));
  const yesGrp  = withOutcome.filter(p => p.confirmedInCall === 'Yes');
  const noGrp   = withOutcome.filter(p => p.confirmedInCall === 'No');
  const yesSh   = yesGrp.filter(p => p.showStatus === 'Showed').length;
  const noSh    = noGrp.filter(p => p.showStatus === 'Showed').length;
  const yesSR   = pct(yesSh, yesGrp.length);
  const noSR    = pct(noSh,  noGrp.length);
  const cfmD    = yesSR - noSR;
  s('rc-eff-yes-showed', yesSR + '%');
  s('rc-eff-yes-count',  yesSh + ' of ' + yesGrp.length + ' showed');
  s('rc-eff-no-showed',  noSR  + '%');
  s('rc-eff-no-count',   noSh  + ' of ' + noGrp.length  + ' showed');
  s('rc-eff-verdict', Math.abs(cfmD) <= 3
    ? "Verbal confirmation doesn't reliably predict show-up"
    : cfmD > 3
    ? 'Getting a "Yes" lifts show rate by +' + cfmD + 'pp — push for confirmation on every call'
    : "Those who didn't confirm actually showed up more — confirmation may be a weak signal");
}

function clearRc() {
  const s = document.getElementById('rc-search');    if (s) s.value = '';
  const a = document.getElementById('rc-ae');        if (a) a.value = '';
  const c = document.getElementById('rc-confirmed'); if (c) c.value = '';
  const f = document.getElementById('rc-from');      if (f) f.value = '';
  const t = document.getElementById('rc-to');        if (t) t.value = '';
  _rcStatus = 'all'; _rcDateFrom = ''; _rcDateTo = '';
  document.querySelectorAll('.pbtn[data-rcs]').forEach((b,i) => b.classList.toggle('active', i === 0));
  // Reset date presets — activate "All Time"
  document.querySelectorAll('#page-reminder .fbar .pbtn').forEach((b,i,arr) => b.classList.toggle('active', i === arr.length - 1));
  applyRc();
}

function toggleRcExpand(row) {
  const next = row.nextElementSibling;
  if (next && next.classList.contains('rc-detail')) { next.remove(); return; }

  const idx = parseInt(row.dataset.idx);
  const p = _rcPeople[idx];
  if (!p) return;

  const detail = document.createElement('tr');
  detail.className = 'rc-detail';

  const transcript = (p.lastTranscript || '')
    .replace(/\\\\n/g,'<br>').replace(/\\n/g,'<br>');
  const callsHtml = p.allCalls.map(c => {
    const sBadge = c.status.startsWith('Connected')
      ? '<span class="bd bg" style="font-size:10px">Connected</span>'
      : '<span class="bd bn" style="font-size:10px">Not Reached</span>';
    const confBadge = c.confirmed === 'Yes'
      ? '<span class="bd bg" style="font-size:10px">Confirmed Yes</span>'
      : c.confirmed === 'No'
      ? '<span class="bd br" style="font-size:10px">Did Not Confirm</span>' : '';
    const sentBadge = c.sentiment
      ? '<span class="bd ' + (c.sentiment==='Positive'?'bg':c.sentiment==='Negative'?'br':'bn') + '" style="font-size:10px">' + c.sentiment + '</span>' : '';
    return '<div class="rc-mini-call">' +
      '<span style="color:#9ca3af;min-width:140px">' + (c.date||'—') + '</span>' +
      '<span style="min-width:60px">' + (c.duration||'—') + '</span>' +
      sBadge + ' ' + (confBadge||'') + ' ' + (sentBadge||'') +
      (c.recording ? ' <button data-rec="' + c.recording.replace(/"/g,'&quot;') + '" onclick="playAiRecording(this,this.dataset.rec)" style="background:none;border:none;cursor:pointer;color:#2563eb;font-size:12px;margin-left:8px;padding:0;font-family:inherit">▶ Play</button>' : '') +
    '</div>';
  }).join('');

  detail.innerHTML = '<td colspan="11" style="padding:0">' +
    '<div class="rc-calls-inner">' +
      '<div style="font-size:10px;font-weight:600;text-transform:uppercase;color:#6b7280;margin-bottom:8px">All Calls</div>' +
      callsHtml +
      (p.lastSummary || p.lastTranscript ? '<div style="display:grid;grid-template-columns:1fr 1fr;gap:16px;margin-top:14px">' +
        (p.lastSummary ? '<div><div style="font-size:10px;font-weight:600;text-transform:uppercase;color:#6b7280;margin-bottom:4px">Summary (last connected call)</div><div style="font-size:13px;color:#374151;line-height:1.5">' + (p.lastSummary||'—') + '</div></div>' : '') +
        (transcript ? '<div><div style="font-size:10px;font-weight:600;text-transform:uppercase;color:#6b7280;margin-bottom:4px">Transcript</div><div style="font-size:12px;color:#6b7280;line-height:1.6;max-height:220px;overflow-y:auto">' + transcript + '</div></div>' : '') +
      '</div>' : '') +
    '</div>' +
  '</td>';
  row.after(detail);
}

// Run count on load
document.addEventListener('DOMContentLoaded', () => { applyRc && applyRc(); });

function sortAiCalls(colIdx, th) {
  const asc = _aiSort.col === colIdx ? !_aiSort.asc : (colIdx === 0 ? true : false);
  _aiSort = { col: colIdx, asc };
  document.querySelectorAll('#ai-table thead th').forEach(h => h.classList.remove('sort-asc','sort-desc'));
  th.classList.add(asc ? 'sort-asc' : 'sort-desc');

  const tbody = document.getElementById('ai-tbody');
  const rows = [...tbody.querySelectorAll('.ai-row')];
  // Remove detail rows
  tbody.querySelectorAll('.ai-detail').forEach(d => d.remove());

  rows.sort((a, b) => {
    const av = a.cells[colIdx]?.textContent.trim() || '';
    const bv = b.cells[colIdx]?.textContent.trim() || '';
    const an = parseFloat(av.replace(/[^0-9.-]/g, ''));
    const bn = parseFloat(bv.replace(/[^0-9.-]/g, ''));
    if (!isNaN(an) && !isNaN(bn)) return asc ? an - bn : bn - an;
    return asc ? av.localeCompare(bv) : bv.localeCompare(av);
  });
  rows.forEach(r => tbody.appendChild(r));
}
</script>
</body>
</html>`;
}

// ── MAIN ──────────────────────────────────────────────────────────────────────

const PINNED_CAMPAIGNS       = ['5 March | Creative Testing', '2 March | SayPrimer', '23March | Cold Call Audience'];
const FORCE_ACTIVE_CAMPAIGNS = ['2 March | SayPrimer', '23March | Cold Call Audience'];
const EXCLUDED_CAMPAIGNS     = ['1 April | Meta Ads'];

async function main() {
  console.log('Fetching Meta campaigns...');
  const all = await getAllCampaigns();
  const excluded  = all.filter(c => EXCLUDED_CAMPAIGNS.some(name => c.name.includes(name)));
  const campaigns = all.filter(c =>
    (c.effective_status === 'ACTIVE' || PINNED_CAMPAIGNS.some(name => c.name.includes(name))) &&
    !EXCLUDED_CAMPAIGNS.some(name => c.name.includes(name))
  );
  console.log(`Found ${campaigns.length} campaigns (${campaigns.filter(c => c.effective_status === 'ACTIVE').length} active): ${campaigns.map(c => `${c.name} [${c.effective_status}]`).join(', ')}`);
  if (excluded.length) console.log(`Excluded: ${excluded.map(c => c.name).join(', ')}`);

  const creds = process.env.GOOGLE_SERVICE_ACCOUNT_JSON
    ? JSON.parse(process.env.GOOGLE_SERVICE_ACCOUNT_JSON)
    : JSON.parse(fs.readFileSync(path.join(__dirname, 'google-credentials.json'), 'utf8'));
  const auth   = new google.auth.GoogleAuth({ credentials: creds, scopes: ['https://www.googleapis.com/auth/spreadsheets.readonly'] });
  const sheets = google.sheets({ version: 'v4', auth });
  const campNames = [...campaigns.map(c => c.name), 'direct booking'];

  console.log('Fetching insights + sheet data + Retell calls in parallel...');
  const [qualData, accountDailySpendRaw, retellCalls, reminderPeople, ...allFetched] = await Promise.all([
    getQualData(sheets, campNames),
    getAccountDailySpend(),
    fetchRetellCalls(),
    fetchReminderCalls(sheets),
    ...excluded.map(c => getCampaignDailyInsights(c.id)),
    ...campaigns.flatMap(c => [getCampaignInsights(c.id), getAdInsights(c.id)]),
    ...campaigns.map(c => getCampaignDailyInsights(c.id)),
  ]);
  console.log(`  ${retellCalls.length} Retell calls fetched`);

  // Subtract excluded campaigns' spend from account-level daily totals
  const excludedDailyArr = allFetched.slice(0, excluded.length);
  const excludedByDate = {};
  for (const arr of excludedDailyArr) {
    for (const day of arr) {
      excludedByDate[day.date] = (excludedByDate[day.date] || 0) + day.spend;
    }
  }
  const accountDailySpend = accountDailySpendRaw.map(day => ({
    date:  day.date,
    spend: Math.max(0, day.spend - (excludedByDate[day.date] || 0)),
  }));

  const insightsAndAds   = allFetched.slice(excluded.length, excluded.length + campaigns.length * 2);
  const dailyInsightsArr = allFetched.slice(excluded.length + campaigns.length * 2);
  const dailySpend = {};
  campaigns.forEach((c, i) => { dailySpend[c.id] = dailyInsightsArr[i] || []; });

  const campMap = {};
  campaigns.forEach((c, i) => {
    const ins = insightsAndAds[i * 2];
    const ads = insightsAndAds[i * 2 + 1];
    campMap[c.id] = {
      name:     c.name,
      isActive: c.effective_status === 'ACTIVE' || FORCE_ACTIVE_CAMPAIGNS.some(n => c.name.includes(n)),
      insights: ins,
      bookedCalls: (qualData[c.name] || {}).bookedCalls || 0,
      shows:       (qualData[c.name] || {}).shows       || 0,
      ads: ads.map(ad => ({
        name:     ad.ad_name,
        adset:    ad.adset_name,
        spendRaw: parseFloat(ad.spend || 0),
        leads:    getAction(ad.actions, 'lead'),
        cpl:      fmt(getCPA(ad.cost_per_action_type, 'lead')),
      })),
    };
  });

  if (qualData['direct booking']) {
    campMap['direct-booking'] = {
      name: 'Direct Booking', isActive: true, insights: null,
      bookedCalls: qualData['direct booking'].bookedCalls || 0,
      shows:       qualData['direct booking'].shows       || 0,
      ads: [],
    };
  }

  const bookedLeads    = qualData._bookedLeads || [];
  const allLeads       = qualData._allLeads    || [];
  const aiCallsData    = buildAiCallsData(retellCalls, qualData);
  console.log(`  AI Calls: ${aiCallsData.calls.length} calls, ${aiCallsData.leadsCalled}/${aiCallsData.totalLeads} leads called, ${aiCallsData.leadsNotCalled.length} not called`);
  console.log(`  Reminder Calls: ${reminderPeople.length} people`);
  const funnels        = buildFunnelData(campMap, qualData);
  const combined       = buildCombined(funnels);
  const combinedActive = buildCombinedActive(funnels);
  const html           = buildHtml(funnels, combined, combinedActive, TODAY, bookedLeads, allLeads, dailySpend, accountDailySpend, aiCallsData, reminderPeople);

  const htmlBuf  = Buffer.from(html);
  const htmlSize = htmlBuf.length;

  console.log('Deploying to Vercel...');
  const VERCEL_TOKEN   = process.env.VERCEL_TOKEN;
  const VERCEL_PROJECT = 'prj_4MAHj0B4PO7zEVV8SoZf63YppHRY';
  const VERCEL_TEAM    = 'team_JFz7qvPtrisOLQGFx0I12yHf';

  if (!VERCEL_TOKEN) throw new Error('VERCEL_TOKEN env var not set');

  // Step 1: Upload file separately (avoids 10MB base64 request body limit)
  const crypto  = require('crypto');
  const fileSha = crypto.createHash('sha1').update(htmlBuf).digest('hex');

  const uploadRes = await fetch(
    `https://api.vercel.com/v2/now/files`,
    {
      method: 'POST',
      headers: {
        Authorization:    `Bearer ${VERCEL_TOKEN}`,
        'Content-Type':   'application/octet-stream',
        'x-now-digest':   fileSha,
        'Content-Length': String(htmlSize),
        'x-vercel-team-id': VERCEL_TEAM,
      },
      body: htmlBuf,
    }
  );
  const uploadData = await uploadRes.json();
  if (uploadData.error) throw new Error(`Vercel file upload failed: ${uploadData.error.message}`);
  console.log(`  File uploaded (sha: ${fileSha.slice(0,8)}…)`);

  // Step 2: Create deployment referencing the uploaded file by sha
  const deployRes = await fetch(
    `https://api.vercel.com/v13/deployments?teamId=${VERCEL_TEAM}`,
    {
      method: 'POST',
      headers: { Authorization: `Bearer ${VERCEL_TOKEN}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({
        name:    'meta-report',
        project: VERCEL_PROJECT,
        target:  'production',
        files: [{ file: 'index.html', sha: fileSha, size: htmlSize }],
        projectSettings: { framework: null },
      }),
    }
  );

  const deployData = await deployRes.json();
  if (deployData.error) throw new Error(`Vercel deploy failed: ${deployData.error.message}`);
  const liveUrl = 'https://meta-report-flame.vercel.app/';
  console.log(`✓ Report deployed: ${liveUrl}`);
  return liveUrl;
}

if (require.main === module) {
  main().catch(err => { console.error('Error:', err.message); process.exit(1); });
}

module.exports = { main };
