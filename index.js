require('dotenv').config();
const express = require('express');
const crypto = require('crypto');
const { spawn } = require('child_process');

const { fetchLeadData } = require('./lib/meta');
const { notifySlack } = require('./handlers/slack');
const { addToSheets, updateSmsSentStatus } = require('./handlers/sheets');
const { addToJustCall } = require('./handlers/justcall');
const { sendToWebhook } = require('./handlers/webhook');
const { sendWelcomeSms } = require('./handlers/justcall-sms');
const { notifyFailure } = require('./lib/failure-alert');

const app = express();
app.use(
  express.json({
    verify: (req, _res, buf) => {
      // Preserve raw bytes for Meta webhook signature verification.
      req.rawBody = buf;
    },
  })
);

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
  if (!sig || !req.rawBody || !process.env.META_APP_SECRET) return false;
  const expected =
    'sha256=' +
    crypto
      .createHmac('sha256', process.env.META_APP_SECRET)
      .update(req.rawBody)
      .digest('hex');

  const sigBuf = Buffer.from(sig, 'utf8');
  const expectedBuf = Buffer.from(expected, 'utf8');
  if (sigBuf.length !== expectedBuf.length) return false;
  return crypto.timingSafeEqual(sigBuf, expectedBuf);
}

let syncAllLeadsRunning = false;
let syncRecentRunning = false;
let syncLeadQualityRunning = false;
let syncCallsRunning = false;
let syncDialerCallsRunning = false;
let autoBookB2BRunning = false;
let buildFunnelReportRunning = false;
const recentLeadgenIds = new Map();
const LEADGEN_DEDUPE_WINDOW_MS = 6 * 60 * 60 * 1000;

function sendFailureAlert(payload) {
  notifyFailure(payload).catch((err) => {
    console.error(`Failure alert send failed: ${err.message}`);
  });
}

function isDuplicateLeadgenEvent(leadgenId) {
  const now = Date.now();

  // opportunistic cleanup to bound memory usage
  for (const [id, seenAt] of recentLeadgenIds.entries()) {
    if (now - seenAt > LEADGEN_DEDUPE_WINDOW_MS) {
      recentLeadgenIds.delete(id);
    }
  }

  const seenAt = recentLeadgenIds.get(leadgenId);
  if (seenAt && now - seenAt <= LEADGEN_DEDUPE_WINDOW_MS) {
    return true;
  }

  recentLeadgenIds.set(leadgenId, now);
  return false;
}

function triggerSyncAllLeads(trigger = 'unknown') {
  if (syncAllLeadsRunning) return false;

  syncAllLeadsRunning = true;
  console.log(`[sync-all-leads] started by ${trigger}`);
  let stderrBuffer = '';

  const child = spawn(process.execPath, ['sync-all-leads.js'], {
    cwd: __dirname,
    env: process.env,
    stdio: ['ignore', 'pipe', 'pipe'],
  });

  child.stdout.on('data', (chunk) => {
    process.stdout.write(`[sync-all-leads] ${chunk}`);
  });

  child.stderr.on('data', (chunk) => {
    stderrBuffer = (stderrBuffer + chunk.toString('utf8')).slice(-2000);
    process.stderr.write(`[sync-all-leads] ${chunk}`);
  });

  child.on('close', (code) => {
    if (code === 0) {
      console.log('[sync-all-leads] completed successfully');
    } else {
      console.error(`[sync-all-leads] exited with code ${code}`);
      sendFailureAlert({
        context: 'sync-all-leads execution failed',
        component: 'sync-all-leads',
        error: new Error(`sync-all-leads exited with code ${code}`),
        details: `trigger=${trigger}${stderrBuffer ? `\nstderr_tail=${stderrBuffer}` : ''}`,
      });
    }
    syncAllLeadsRunning = false;
  });

  child.on('error', (err) => {
    console.error(`[sync-all-leads] failed to start: ${err.message}`);
    sendFailureAlert({
      context: 'sync-all-leads failed to start',
      component: 'sync-all-leads',
      error: err,
      details: `trigger=${trigger}`,
    });
    syncAllLeadsRunning = false;
  });

  return true;
}

function triggerSyncRecent(trigger = 'unknown', hours = 4) {
  if (syncRecentRunning) return false;

  syncRecentRunning = true;
  console.log(`[sync-recent] started by ${trigger}`);
  let stderrBuffer = '';

  const child = spawn(process.execPath, ['sync-recent.js', `--hours=${hours}`], {
    cwd: __dirname,
    env: process.env,
    stdio: ['ignore', 'pipe', 'pipe'],
  });

  child.stdout.on('data', (chunk) => {
    process.stdout.write(`[sync-recent] ${chunk}`);
  });

  child.stderr.on('data', (chunk) => {
    stderrBuffer = (stderrBuffer + chunk.toString('utf8')).slice(-2000);
    process.stderr.write(`[sync-recent] ${chunk}`);
  });

  child.on('close', (code) => {
    if (code === 0) {
      console.log('[sync-recent] completed successfully');
    } else {
      console.error(`[sync-recent] exited with code ${code}`);
      sendFailureAlert({
        context: 'sync-recent execution failed',
        component: 'sync-recent',
        error: new Error(`sync-recent exited with code ${code}`),
        details: `trigger=${trigger}${stderrBuffer ? `\nstderr_tail=${stderrBuffer}` : ''}`,
      });
    }
    syncRecentRunning = false;
  });

  child.on('error', (err) => {
    console.error(`[sync-recent] failed to start: ${err.message}`);
    sendFailureAlert({
      context: 'sync-recent failed to start',
      component: 'sync-recent',
      error: err,
      details: `trigger=${trigger}`,
    });
    syncRecentRunning = false;
  });

  return true;
}

function triggerSyncCalls(trigger = 'unknown') {
  if (syncCallsRunning) return false;

  syncCallsRunning = true;
  console.log(`[sync-calls] started by ${trigger}`);
  let stderrBuffer = '';

  const child = spawn(process.execPath, ['sync-calls.js'], {
    cwd: __dirname,
    env: process.env,
    stdio: ['ignore', 'pipe', 'pipe'],
  });

  child.stdout.on('data', (chunk) => {
    process.stdout.write(`[sync-calls] ${chunk}`);
  });

  child.stderr.on('data', (chunk) => {
    stderrBuffer = (stderrBuffer + chunk.toString('utf8')).slice(-2000);
    process.stderr.write(`[sync-calls] ${chunk}`);
  });

  child.on('close', (code) => {
    if (code === 0) {
      console.log('[sync-calls] completed successfully');
    } else {
      console.error(`[sync-calls] exited with code ${code}`);
      sendFailureAlert({
        context: 'sync-calls execution failed',
        component: 'sync-calls',
        error: new Error(`sync-calls exited with code ${code}`),
        details: `trigger=${trigger}${stderrBuffer ? `\nstderr_tail=${stderrBuffer}` : ''}`,
      });
    }
    syncCallsRunning = false;
  });

  child.on('error', (err) => {
    console.error(`[sync-calls] failed to start: ${err.message}`);
    sendFailureAlert({
      context: 'sync-calls failed to start',
      component: 'sync-calls',
      error: err,
      details: `trigger=${trigger}`,
    });
    syncCallsRunning = false;
  });

  return true;
}

function triggerSyncDialerCalls(trigger = 'unknown') {
  if (syncDialerCallsRunning) return false;

  syncDialerCallsRunning = true;
  console.log(`[sync-dialer-calls] started by ${trigger}`);
  let stderrBuffer = '';

  const child = spawn(process.execPath, ['sync-dialer-calls.js'], {
    cwd: __dirname,
    env: process.env,
    stdio: ['ignore', 'pipe', 'pipe'],
  });

  child.stdout.on('data', (chunk) => {
    process.stdout.write(`[sync-dialer-calls] ${chunk}`);
  });

  child.stderr.on('data', (chunk) => {
    stderrBuffer = (stderrBuffer + chunk.toString('utf8')).slice(-2000);
    process.stderr.write(`[sync-dialer-calls] ${chunk}`);
  });

  child.on('close', (code) => {
    if (code === 0) {
      console.log('[sync-dialer-calls] completed successfully');
    } else {
      console.error(`[sync-dialer-calls] exited with code ${code}`);
      sendFailureAlert({
        context: 'sync-dialer-calls execution failed',
        component: 'sync-dialer-calls',
        error: new Error(`sync-dialer-calls exited with code ${code}`),
        details: `trigger=${trigger}${stderrBuffer ? `\nstderr_tail=${stderrBuffer}` : ''}`,
      });
    }
    syncDialerCallsRunning = false;
  });

  child.on('error', (err) => {
    console.error(`[sync-dialer-calls] failed to start: ${err.message}`);
    sendFailureAlert({
      context: 'sync-dialer-calls failed to start',
      component: 'sync-dialer-calls',
      error: err,
      details: `trigger=${trigger}`,
    });
    syncDialerCallsRunning = false;
  });

  return true;
}

function triggerSyncLeadQuality(trigger = 'unknown') {
  if (syncLeadQualityRunning) return false;

  syncLeadQualityRunning = true;
  console.log(`[sync-lead-quality] started by ${trigger}`);
  let stderrBuffer = '';

  const child = spawn(process.execPath, ['sync-lead-quality.js'], {
    cwd: __dirname,
    env: process.env,
    stdio: ['ignore', 'pipe', 'pipe'],
  });

  child.stdout.on('data', (chunk) => {
    process.stdout.write(`[sync-lead-quality] ${chunk}`);
  });

  child.stderr.on('data', (chunk) => {
    stderrBuffer = (stderrBuffer + chunk.toString('utf8')).slice(-2000);
    process.stderr.write(`[sync-lead-quality] ${chunk}`);
  });

  child.on('close', (code) => {
    if (code === 0) {
      console.log('[sync-lead-quality] completed successfully');
    } else {
      console.error(`[sync-lead-quality] exited with code ${code}`);
      sendFailureAlert({
        context: 'sync-lead-quality execution failed',
        component: 'sync-lead-quality',
        error: new Error(`sync-lead-quality exited with code ${code}`),
        details: `trigger=${trigger}${stderrBuffer ? `\nstderr_tail=${stderrBuffer}` : ''}`,
      });
    }
    syncLeadQualityRunning = false;
  });

  child.on('error', (err) => {
    console.error(`[sync-lead-quality] failed to start: ${err.message}`);
    sendFailureAlert({
      context: 'sync-lead-quality failed to start',
      component: 'sync-lead-quality',
      error: err,
      details: `trigger=${trigger}`,
    });
    syncLeadQualityRunning = false;
  });

  return true;
}

function triggerAutoBookB2B(trigger = 'unknown', dryRun = false) {
  if (autoBookB2BRunning) return false;

  autoBookB2BRunning = true;
  console.log(`[auto-book-b2b] started by ${trigger}${dryRun ? ' [dry-run]' : ''}`);
  let stderrBuffer = '';

  const args = ['auto-book-b2b-leads.js', '--once'];
  if (dryRun) args.push('--dry-run');

  const child = spawn(process.execPath, args, {
    cwd: __dirname,
    env: process.env,
    stdio: ['ignore', 'pipe', 'pipe'],
  });

  child.stdout.on('data', (chunk) => {
    process.stdout.write(`[auto-book-b2b] ${chunk}`);
  });

  child.stderr.on('data', (chunk) => {
    stderrBuffer = (stderrBuffer + chunk.toString('utf8')).slice(-2000);
    process.stderr.write(`[auto-book-b2b] ${chunk}`);
  });

  child.on('close', (code) => {
    if (code === 0) {
      console.log('[auto-book-b2b] completed successfully');
    } else {
      console.error(`[auto-book-b2b] exited with code ${code}`);
      sendFailureAlert({
        context: 'auto-book-b2b execution failed',
        component: 'auto-book-b2b',
        error: new Error(`auto-book-b2b exited with code ${code}`),
        details: `trigger=${trigger}${dryRun ? ' dryRun=true' : ''}${stderrBuffer ? `\nstderr_tail=${stderrBuffer}` : ''}`,
      });
    }
    autoBookB2BRunning = false;
  });

  child.on('error', (err) => {
    console.error(`[auto-book-b2b] failed to start: ${err.message}`);
    sendFailureAlert({
      context: 'auto-book-b2b failed to start',
      component: 'auto-book-b2b',
      error: err,
      details: `trigger=${trigger}${dryRun ? ' dryRun=true' : ''}`,
    });
    autoBookB2BRunning = false;
  });

  return true;
}

function triggerBuildFunnelReport(trigger = 'unknown') {
  if (buildFunnelReportRunning) return false;

  buildFunnelReportRunning = true;
  console.log(`[build-funnel-report] started by ${trigger}`);
  let stderrBuffer = '';

  const child = spawn(process.execPath, ['build-funnel-report.js'], {
    cwd: __dirname,
    env: process.env,
    stdio: ['ignore', 'pipe', 'pipe'],
  });

  child.stdout.on('data', (chunk) => {
    process.stdout.write(`[build-funnel-report] ${chunk}`);
  });

  child.stderr.on('data', (chunk) => {
    stderrBuffer = (stderrBuffer + chunk.toString('utf8')).slice(-2000);
    process.stderr.write(`[build-funnel-report] ${chunk}`);
  });

  child.on('close', (code) => {
    if (code === 0) {
      console.log('[build-funnel-report] completed successfully');
    } else {
      console.error(`[build-funnel-report] exited with code ${code}`);
      sendFailureAlert({
        context: 'build-funnel-report execution failed',
        component: 'build-funnel-report',
        error: new Error(`build-funnel-report exited with code ${code}`),
        details: `trigger=${trigger}${stderrBuffer ? `\nstderr_tail=${stderrBuffer}` : ''}`,
      });
    }
    buildFunnelReportRunning = false;
  });

  child.on('error', (err) => {
    console.error(`[build-funnel-report] failed to start: ${err.message}`);
    sendFailureAlert({
      context: 'build-funnel-report failed to start',
      component: 'build-funnel-report',
      error: err,
      details: `trigger=${trigger}`,
    });
    buildFunnelReportRunning = false;
  });

  return true;
}

// Run sync-recent every 5 minutes as a failsafe for missed webhooks
setInterval(() => triggerSyncRecent('5m-cron'), 5 * 60 * 1000);

// Poll Sheet1 for new "businesses_(b2b)" rows and auto-book from 7PM-9PM IST slots.
const autoBookIntervalMs = parseInt(process.env.B2B_AUTO_BOOK_INTERVAL_MS || '60000', 10);
if (Number.isFinite(autoBookIntervalMs) && autoBookIntervalMs > 0) {
  setInterval(() => triggerAutoBookB2B('poller'), autoBookIntervalMs);
}

// ── Main webhook receiver (POST) ────────────────────────────────────────────
app.post('/webhook', async (req, res) => {
  if (!verifyMetaSignature(req)) {
    console.warn('Invalid signature — rejected');
    sendFailureAlert({
      context: 'Meta webhook rejected due invalid signature',
      component: 'webhook-signature',
      error: new Error('x-hub-signature-256 verification failed'),
      dedupeKey: 'invalid-signature',
      dedupeWindowMs: 10 * 60 * 1000,
    });
    return res.sendStatus(403);
  }

  res.sendStatus(200); // Respond to Meta immediately

  const changes = req.body?.entry?.[0]?.changes;
  if (!changes) return;

  for (const change of changes) {
    if (change.field !== 'leadgen') continue;

    const { leadgen_id } = change.value;
    if (!leadgen_id) continue;
    if (isDuplicateLeadgenEvent(leadgen_id)) {
      console.log(`Duplicate leadgen event skipped: ${leadgen_id}`);
      continue;
    }
    console.log(`New lead received: ${leadgen_id}`);

    try {
      const lead = await fetchLeadData(leadgen_id);
      console.log('Lead data:', lead);

      const results = await Promise.allSettled([
        notifySlack(lead),
        addToSheets(lead),
        addToJustCall(lead),
        sendToWebhook(lead),
      ]);

      results.forEach((r, i) => {
        const name = ['Slack', 'Sheets', 'JustCall', 'Webhook'][i];
        if (r.status === 'rejected') {
          console.error(`${name} failed:`, r.reason?.message);
          sendFailureAlert({
            context: `${name} execution failed while processing lead`,
            component: name.toLowerCase(),
            leadgenId: leadgen_id,
            leadId: lead.id,
            error: r.reason,
          });
        }
      });

      // Send welcome SMS once the sheet row is confirmed written, then update the SMS Sent column
      const sheetsResult = results[1];
      if (sheetsResult.status === 'fulfilled') {
        const { rowNumber } = sheetsResult.value || {};
        try {
          await sendWelcomeSms(lead);
          console.log(`Welcome SMS sent to ${lead.full_name}`);
          await updateSmsSentStatus(rowNumber, 'Yes');
        } catch (smsErr) {
          console.error('Welcome SMS failed:', smsErr.message);
          await updateSmsSentStatus(rowNumber, smsErr.message).catch(() => {});
          sendFailureAlert({
            context: 'Welcome SMS failed while processing lead',
            component: 'welcome-sms',
            leadgenId: leadgen_id,
            leadId: lead.id,
            error: smsErr,
          });
        }
      }
    } catch (err) {
      console.error(`Error processing lead ${leadgen_id}:`, err.message);
      sendFailureAlert({
        context: 'Lead webhook processing failed',
        component: 'webhook-processing',
        leadgenId: leadgen_id,
        error: err,
      });
    }
  }
});

// ── Health check ────────────────────────────────────────────────────────────
app.get('/health', (req, res) => res.json({ status: 'ok' }));

// ── Cron-triggered backfill endpoint ───────────────────────────────────────
app.post('/jobs/sync-all-leads', (req, res) => {
  const configuredSecret = process.env.CRON_SYNC_SECRET;
  const providedSecret = req.headers['x-cron-secret'];

  if (!configuredSecret) {
    sendFailureAlert({
      context: 'Cron sync endpoint misconfigured',
      component: 'cron-sync-endpoint',
      error: new Error('CRON_SYNC_SECRET not configured'),
      dedupeKey: 'cron-secret-missing',
      dedupeWindowMs: 60 * 60 * 1000,
    });
    return res.status(503).json({ status: 'error', message: 'CRON_SYNC_SECRET not configured' });
  }
  if (!providedSecret || providedSecret !== configuredSecret) {
    sendFailureAlert({
      context: 'Cron sync endpoint rejected unauthorized request',
      component: 'cron-sync-endpoint',
      error: new Error('Invalid x-cron-secret'),
      dedupeKey: 'cron-secret-invalid',
      dedupeWindowMs: 10 * 60 * 1000,
    });
    return res.sendStatus(403);
  }

  if (!triggerSyncAllLeads('cron')) {
    return res.status(409).json({ status: 'already_running' });
  }

  return res.status(202).json({ status: 'started' });
});

// ── Manual sync-recent trigger ──────────────────────────────────────────────
app.post('/jobs/sync-recent', (req, res) => {
  const configuredSecret = process.env.CRON_SYNC_SECRET;
  const providedSecret = req.headers['x-cron-secret'];

  if (!configuredSecret || !providedSecret || providedSecret !== configuredSecret) {
    return res.sendStatus(403);
  }

  const hours = parseInt(req.query.hours, 10) || 4;

  if (!triggerSyncRecent('manual', hours)) {
    return res.status(409).json({ status: 'already_running' });
  }

  return res.status(202).json({ status: 'started', hours });
});

// ── Cron-triggered Retell calls sync ────────────────────────────────────────
app.post('/jobs/sync-calls', (req, res) => {
  const configuredSecret = process.env.CRON_SYNC_SECRET;
  const providedSecret = req.headers['x-cron-secret'];

  if (!configuredSecret || !providedSecret || providedSecret !== configuredSecret) {
    return res.sendStatus(403);
  }

  if (!triggerSyncCalls('cron')) {
    return res.status(409).json({ status: 'already_running' });
  }

  return res.status(202).json({ status: 'started' });
});

// ── Cron-triggered JustCall dialer calls sync ────────────────────────────────
app.post('/jobs/sync-dialer-calls', (req, res) => {
  const configuredSecret = process.env.CRON_SYNC_SECRET;
  const providedSecret = req.headers['x-cron-secret'];

  if (!configuredSecret || !providedSecret || providedSecret !== configuredSecret) {
    return res.sendStatus(403);
  }

  if (!triggerSyncDialerCalls('cron')) {
    return res.status(409).json({ status: 'already_running' });
  }

  return res.status(202).json({ status: 'started' });
});

// ── Cron-triggered lead quality CAPI sync ───────────────────────────────────
app.post('/jobs/sync-lead-quality', (req, res) => {
  const configuredSecret = process.env.CRON_SYNC_SECRET;
  const providedSecret = req.headers['x-cron-secret'];

  if (!configuredSecret || !providedSecret || providedSecret !== configuredSecret) {
    return res.sendStatus(403);
  }

  if (!triggerSyncLeadQuality('cron')) {
    return res.status(409).json({ status: 'already_running' });
  }

  return res.status(202).json({ status: 'started' });
});

// ── Manual / cron-triggered B2B auto-booking ───────────────────────────────
app.post('/jobs/auto-book-b2b', (req, res) => {
  const configuredSecret = process.env.CRON_SYNC_SECRET;
  const providedSecret = req.headers['x-cron-secret'];

  if (!configuredSecret || !providedSecret || providedSecret !== configuredSecret) {
    return res.sendStatus(403);
  }

  const dryRun = ['1', 'true', 'yes'].includes(String(req.query.dryRun || '').toLowerCase());
  if (!triggerAutoBookB2B('manual', dryRun)) {
    return res.status(409).json({ status: 'already_running' });
  }

  return res.status(202).json({ status: 'started', dryRun });
});

// ── Cron-triggered funnel report build + Vercel deploy ──────────────────────
app.post('/jobs/build-funnel-report', (req, res) => {
  const configuredSecret = process.env.CRON_SYNC_SECRET;
  const providedSecret = req.headers['x-cron-secret'];

  if (!configuredSecret || !providedSecret || providedSecret !== configuredSecret) {
    return res.sendStatus(403);
  }

  if (!triggerBuildFunnelReport('cron')) {
    return res.status(409).json({ status: 'already_running' });
  }

  return res.status(202).json({ status: 'started' });
});

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
    sendToWebhook(lead),
  ]);

  const report = results.map((r, i) => ({
    service: ['Slack', 'Sheets', 'JustCall', 'Webhook'][i],
    status: r.status,
    error: r.reason?.message || null,
  }));

  results.forEach((r, i) => {
    if (r.status !== 'rejected') return;
    const service = ['Slack', 'Sheets', 'JustCall', 'Webhook'][i];
    sendFailureAlert({
      context: `${service} execution failed on /test-lead`,
      component: 'test-lead',
      leadId: lead.id,
      error: r.reason,
    });
  });

  const sheetsResult = results[1];
  if (sheetsResult.status === 'fulfilled') {
    const { rowNumber } = sheetsResult.value || {};
    try {
      const smsResult = await sendWelcomeSms(lead);
      await updateSmsSentStatus(rowNumber, 'Yes');
      report.push({ service: 'WelcomeSMS', status: 'fulfilled', error: null });
      console.log('Welcome SMS sent (test):', smsResult.payload.body);
    } catch (smsErr) {
      await updateSmsSentStatus(rowNumber, smsErr.message).catch(() => {});
      report.push({ service: 'WelcomeSMS', status: 'rejected', error: smsErr.message });
    }
  }

  res.json(report);
});

app.listen(process.env.PORT, () => {
  console.log(`Server running on http://localhost:${process.env.PORT}`);
});
