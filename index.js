require('dotenv').config();
const express = require('express');
const crypto = require('crypto');

const { fetchLeadData } = require('./lib/meta');
const { notifySlack } = require('./handlers/slack');
const { addToSheets } = require('./handlers/sheets');
const { addToJustCall } = require('./handlers/justcall');

const app = express();
app.use(express.json());

// ── Meta webhook verification (GET) ────────────────────────────────────────
app.get('/webhook', (req, res) => {
  const mode = req.query['hub.mode'];
  const token = req.query['hub.verify_token'];
  const challenge = req.query['hub.challenge'];

  if (mode === 'subscribe' && token === process.env.META_VERIFY_TOKEN) {
    console.log('Webhook verified by Meta');
    return res.send(challenge);
  }
  res.sendStatus(403);
});

// ── Verify request signature from Meta ─────────────────────────────────────
function verifyMetaSignature(req) {
  const sig = req.headers['x-hub-signature-256'];
  if (!sig) return false;
  const expected =
    'sha256=' +
    crypto
      .createHmac('sha256', process.env.META_APP_SECRET)
      .update(JSON.stringify(req.body))
      .digest('hex');
  return crypto.timingSafeEqual(Buffer.from(sig), Buffer.from(expected));
}

// ── Main webhook receiver (POST) ────────────────────────────────────────────
app.post('/webhook', async (req, res) => {
  if (!verifyMetaSignature(req)) {
    console.warn('Invalid signature — rejected');
    return res.sendStatus(403);
  }

  res.sendStatus(200); // Respond to Meta immediately

  const changes = req.body?.entry?.[0]?.changes;
  if (!changes) return;

  for (const change of changes) {
    if (change.field !== 'leadgen') continue;

    const { leadgen_id } = change.value;
    console.log(`New lead received: ${leadgen_id}`);

    try {
      const lead = await fetchLeadData(leadgen_id);
      console.log('Lead data:', lead);

      const results = await Promise.allSettled([
        notifySlack(lead),
        addToSheets(lead),
        addToJustCall(lead),
      ]);

      results.forEach((r, i) => {
        const name = ['Slack', 'Sheets', 'JustCall'][i];
        if (r.status === 'rejected') {
          console.error(`${name} failed:`, r.reason?.message);
        }
      });
    } catch (err) {
      console.error(`Error processing lead ${leadgen_id}:`, err.message);
    }
  }
});

// ── Health check ────────────────────────────────────────────────────────────
app.get('/health', (req, res) => res.json({ status: 'ok' }));

// ── Test endpoint (simulate a lead without Meta) ────────────────────────────
app.post('/test-lead', async (req, res) => {
  const lead = req.body?.lead || {
    id: 'test_123',
    created_time: new Date().toISOString(),
    full_name: 'Test User',
    email: 'test@example.com',
    phone_number: '+911234567890',
  };

  console.log('Running test lead:', lead);
  const results = await Promise.allSettled([
    notifySlack(lead),
    addToSheets(lead),
    addToJustCall(lead),
  ]);

  const report = results.map((r, i) => ({
    service: ['Slack', 'Sheets', 'JustCall'][i],
    status: r.status,
    error: r.reason?.message || null,
  }));

  res.json(report);
});

app.listen(process.env.PORT, () => {
  console.log(`Server running on http://localhost:${process.env.PORT}`);
});
