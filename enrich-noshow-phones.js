/**
 * enrich-noshow-phones.js
 *
 * For Meta no-shows that have no phone number in the DB:
 *  1. Submits them to FullEnrich bulk enrich API to find mobile phones.
 *  2. Polls until the batch is FINISHED.
 *  3. Adds any enriched contacts (with a phone found) to JustCall campaign #3190746.
 *
 * Usage:
 *   node enrich-noshow-phones.js            # dry-run
 *   node enrich-noshow-phones.js --commit   # enrich + add to campaign
 *
 * Requires: FULLENRICH_API_KEY in .env
 */

require('dotenv').config();
const { Client } = require('pg');
const { addToCampaign } = require('./handlers/justcall');

const NOSHOW_CAMPAIGN  = '3190746';
const FULLENRICH_URL   = 'https://app.fullenrich.com/api/v1/contact/enrich/bulk';
const BATCH_SIZE       = 50;
const POLL_INTERVAL_MS = 5000;
const POLL_TIMEOUT_MS  = 5 * 60 * 1000;
const isCommit         = process.argv.includes('--commit');

const PERSONAL_DOMAINS = new Set([
  'gmail.com','yahoo.com','hotmail.com','outlook.com','icloud.com',
  'aol.com','msn.com','live.com','me.com','mac.com','protonmail.com',
  'mail.com','ymail.com','comcast.net','verizon.net','att.net',
  'sbcglobal.net','bellsouth.net','cox.net','earthlink.net',
]);

const DB_CONFIG = {
  host:     'gw-rds-analytics.celzx4qnlkfp.us-east-1.rds.amazonaws.com',
  user:     'airbyte_user',
  password: 'airbyte_user_password',
  database: 'gw_prod',
  ssl:      { rejectUnauthorized: false },
};

const QUERY = `
WITH base AS (
    SELECT
        *,
        COALESCE(event_url, LOWER(TRIM(prospect_email))) AS booking_key
    FROM gist.gtm_inbound_demo_bookings
),
latest_rows AS (
    SELECT DISTINCT ON (LOWER(TRIM(b.prospect_email)))
        b.prospect_first_name,
        b.prospect_email,
        b.prospect_phone_number,
        b.prospect_company,
        b.prospect_website,
        b.show_status,
        b.source
    FROM base b
    WHERE b.is_latest = true
      AND b.prospect_first_name NOT IN ('Test','test','gushwork','Gushwork','df df','df')
      AND b.prospect_email NOT ILIKE '%gushwork%'
      AND LOWER(b.prospect_email) NOT ILIKE '%swapnil%'
      AND LOWER(b.prospect_email) NOT ILIKE '%getclientell%'
    ORDER BY LOWER(TRIM(b.prospect_email)), b.demo_scheduled_date DESC
),
finals AS (
    SELECT
        prospect_first_name,
        prospect_email,
        prospect_phone_number,
        prospect_company,
        prospect_website,
        show_status,
        CASE
            WHEN LOWER(source) LIKE '%facebook%'
              OR LOWER(source) LIKE '%instagram%'
              OR LOWER(source) = 'fb'
              OR LOWER(source) = 'meta'
              OR LOWER(source) LIKE '%lizzi%'
              OR LOWER(source) LIKE '%meta%'
              OR LOWER(source) LIKE '%fb%'
              OR LOWER(source) LIKE '%book%'
              OR LOWER(source) LIKE '%face%'
              OR LOWER(source) LIKE '%ig%'
              OR LOWER(source) LIKE '%fg%'
              OR LOWER(source) LIKE '%insta%'
              OR LOWER(source) LIKE '%instra%'
            THEN 'Meta'
            ELSE 'Other'
        END AS source_bucket
    FROM latest_rows
)
SELECT prospect_first_name, prospect_email, prospect_company, prospect_website
FROM finals
WHERE show_status = 'N'
  AND source_bucket = 'Meta'
  AND (prospect_phone_number IS NULL OR TRIM(prospect_phone_number) = '')
`;

function extractDomain(email, website) {
  // Try business email domain first
  if (email) {
    const parts = email.toLowerCase().split('@');
    const emailDomain = parts[1] || '';
    if (emailDomain && !PERSONAL_DOMAINS.has(emailDomain)) {
      return emailDomain;
    }
  }
  // Fall back to prospect_website
  if (website) {
    try {
      const url = website.startsWith('http') ? website : `https://${website}`;
      const host = new URL(url).hostname.replace(/^www\./, '');
      if (host && !PERSONAL_DOMAINS.has(host)) return host;
    } catch (_) {}
  }
  return null;
}

function normalizePhone(raw) {
  if (!raw) return null;
  return raw.replace(/\s/g, '');
}

function splitName(fullName) {
  const parts = (fullName || '').trim().split(/\s+/);
  return {
    firstname: parts[0] || '',
    lastname:  parts.slice(1).join(' ') || '',
  };
}

const delay = (ms) => new Promise(r => setTimeout(r, ms));

async function submitBatch(leads, batchName) {
  const key = process.env.FULLENRICH_API_KEY;
  if (!key) throw new Error('FULLENRICH_API_KEY not set in .env');

  const datas = leads.map(l => ({
    ...splitName(l.prospect_first_name),
    domain: l._domain,
    enrich_fields: ['contact.phones'],
    custom: { email: l.prospect_email },
  }));

  const res = await fetch(FULLENRICH_URL, {
    method: 'POST',
    headers: { Authorization: `Bearer ${key}`, 'Content-Type': 'application/json' },
    body: JSON.stringify({ name: batchName, datas }),
  });
  const data = await res.json();
  if (!res.ok) throw new Error(`FullEnrich submit failed: ${JSON.stringify(data)}`);
  return data.enrichment_id || data.id;
}

async function pollUntilDone(enrichmentId) {
  const key = process.env.FULLENRICH_API_KEY;
  const deadline = Date.now() + POLL_TIMEOUT_MS;

  while (Date.now() < deadline) {
    await delay(POLL_INTERVAL_MS);
    const res = await fetch(`${FULLENRICH_URL}/${enrichmentId}`, {
      headers: { Authorization: `Bearer ${key}` },
    });
    const data = await res.json();
    if (data.status === 'FINISHED') return data.datas || [];
    if (data.status === 'FAILED') throw new Error(`FullEnrich batch ${enrichmentId} failed`);
    process.stdout.write('.');
  }
  throw new Error(`FullEnrich batch ${enrichmentId} timed out after ${POLL_TIMEOUT_MS / 1000}s`);
}

async function run() {
  console.log(`[enrich-noshow] Starting${isCommit ? '' : ' (dry-run — pass --commit to apply)'}...`);

  if (!process.env.FULLENRICH_API_KEY) {
    console.error('[enrich-noshow] FULLENRICH_API_KEY not set. Add it to .env');
    process.exit(1);
  }

  // 1. Fetch no-phone Meta no-shows from DB
  const pg = new Client(DB_CONFIG);
  await pg.connect();
  const { rows } = await pg.query(QUERY);
  await pg.end();
  console.log(`[enrich-noshow] ${rows.length} Meta no-shows with missing phone found in DB`);

  // 2. Filter to those we can resolve a domain for
  const enrichable = rows
    .map(r => ({ ...r, _domain: extractDomain(r.prospect_email, r.prospect_website) }))
    .filter(r => r._domain);

  const skipped = rows.length - enrichable.length;
  console.log(`[enrich-noshow] ${enrichable.length} have a resolvable domain (${skipped} skipped — personal/missing email)`);

  if (!isCommit) {
    enrichable.slice(0, 10).forEach(r =>
      console.log(`  → ${r.prospect_first_name || r.prospect_email} (${r._domain})`)
    );
    if (enrichable.length > 10) console.log(`  … and ${enrichable.length - 10} more`);
    console.log('[enrich-noshow] Dry-run done. Re-run with --commit to enrich and add to campaign.');
    return;
  }

  // 3. Submit in batches
  let totalAdded = 0;
  let totalEnriched = 0;
  const batches = [];
  for (let i = 0; i < enrichable.length; i += BATCH_SIZE) {
    batches.push(enrichable.slice(i, i + BATCH_SIZE));
  }
  console.log(`[enrich-noshow] Submitting ${batches.length} batch(es) of up to ${BATCH_SIZE}...`);

  for (let b = 0; b < batches.length; b++) {
    const batch = batches[b];
    const batchName = `noshow-meta-${Date.now()}-batch${b + 1}`;
    console.log(`[enrich-noshow] Batch ${b + 1}/${batches.length} — submitting ${batch.length} contacts...`);

    const enrichmentId = await submitBatch(batch, batchName);
    console.log(`[enrich-noshow] Batch ${b + 1} submitted — enrichment_id: ${enrichmentId}`);

    process.stdout.write('[enrich-noshow] Polling');
    const results = await pollUntilDone(enrichmentId);
    console.log(` done.`);

    // Build email→phone map from results
    const phoneMap = new Map();
    for (const item of results) {
      const email = item.custom?.email;
      const phone = normalizePhone(item.contact?.most_probable_phone || item.contact?.phones?.[0]?.number);
      if (email && phone) phoneMap.set(email.toLowerCase(), phone);
    }
    console.log(`[enrich-noshow] Batch ${b + 1}: ${phoneMap.size}/${batch.length} contacts got a phone`);
    totalEnriched += phoneMap.size;

    // 4. Add enriched contacts to JustCall campaign
    for (const lead of batch) {
      const phone = phoneMap.get((lead.prospect_email || '').toLowerCase());
      if (!phone) continue;

      const jcLead = {
        full_name:    lead.prospect_first_name || '',
        email:        lead.prospect_email      || '',
        phone_number: phone,
      };
      const label = jcLead.full_name || jcLead.email;

      for (let attempt = 0; attempt < 2; attempt++) {
        try {
          await addToCampaign(jcLead, NOSHOW_CAMPAIGN);
          console.log(`[enrich-noshow] Added: ${label} (${phone})`);
          totalAdded++;
          break;
        } catch (err) {
          if (err?.response?.status === 429 && attempt === 0) {
            console.warn(`[enrich-noshow] Rate limited — backing off 3s for ${label}`);
            await delay(3000);
          } else {
            console.error(`[enrich-noshow] JustCall failed for ${label}: ${err.message}`);
            break;
          }
        }
      }
      await delay(300);
    }
  }

  console.log(`\n[enrich-noshow] Done.`);
  console.log(`  Enriched with phone : ${totalEnriched}/${enrichable.length}`);
  console.log(`  Added to campaign   : ${totalAdded}`);
}

run().catch(err => {
  console.error('[enrich-noshow] Fatal:', err.message);
  process.exit(1);
});
