const https = require('https');

const CHANNEL = 'C0A1RMXTUJ3';

function postMessage(payload) {
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
  const platform = (lead.platform || 'META').toUpperCase();

  const time = new Date(lead.created_time).toLocaleString('en-GB', {
    day: '2-digit', month: 'short', year: 'numeric',
    hour: '2-digit', minute: '2-digit', timeZone: 'America/New_York',
  });

  const sheetsUrl = `https://docs.google.com/spreadsheets/d/${process.env.GOOGLE_SHEETS_ID}`;

  await postMessage({
    channel: CHANNEL,
    text: `New Lead Captured: ${lead.full_name || 'Unknown'}`,
    blocks: [
      {
        type: 'header',
        text: { type: 'plain_text', text: '🚀 New Lead Captured' },
      },
      {
        type: 'section',
        text: {
          type: 'mrkdwn',
          text: `A new prospect has entered the pipeline.\nLead has been added to the *Meta Ads → Sales Dialer* campaign.`,
        },
      },
      { type: 'divider' },
      {
        type: 'section',
        fields: [
          { type: 'mrkdwn', text: `*Name*\n${lead.full_name || 'N/A'}` },
          { type: 'mrkdwn', text: `*Company*\n${lead.company_name || 'N/A'}` },
          { type: 'mrkdwn', text: `*Email*\n${lead.email || 'N/A'}` },
          { type: 'mrkdwn', text: `*Phone*\n${lead.phone || lead.phone_number || 'N/A'}` },
        ],
      },
      {
        type: 'section',
        text: {
          type: 'mrkdwn',
          text: `*Campaign / Ad*\n_${lead.ad_name || lead.campaign_name || 'N/A'}_`,
        },
      },
      {
        type: 'section',
        text: { type: 'mrkdwn', text: `*Lead Status*\nQualified` },
      },
      { type: 'divider' },
      {
        type: 'actions',
        elements: [
          {
            type: 'button',
            text: { type: 'plain_text', text: '📊 View All Leads' },
            url: sheetsUrl,
            style: 'primary',
          },
        ],
      },
      {
        type: 'context',
        elements: [
          { type: 'mrkdwn', text: `⚡ Faster follow-ups convert better  •  :calendar: ${time}` },
        ],
      },
    ],
  });

  console.log('Slack notified');
}

module.exports = { notifySlack };
