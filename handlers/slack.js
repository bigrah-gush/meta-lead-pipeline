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

  const time = new Date(lead.created_time).toLocaleString('en-US', {
    month: 'short', day: 'numeric', year: 'numeric',
    hour: '2-digit', minute: '2-digit', timeZone: 'America/New_York',
  });

  await postMessage({
    channel: CHANNEL,
    text: `New ${platform} Lead: ${lead.full_name || 'Unknown'}`,
    blocks: [
      {
        type: 'header',
        text: { type: 'plain_text', text: `🔔 New ${platform} Lead` },
      },
      { type: 'divider' },
      {
        type: 'section',
        fields: [
          { type: 'mrkdwn', text: `👤 *Name*\n${lead.full_name || 'N/A'}` },
          { type: 'mrkdwn', text: `🏢 *Company*\n${lead.company_name || 'N/A'}` },
          { type: 'mrkdwn', text: `📞 *Phone*\n${lead.phone || lead.phone_number || 'N/A'}` },
          { type: 'mrkdwn', text: `📧 *Email*\n${lead.email || 'N/A'}` },
        ],
      },
      {
        type: 'section',
        fields: [
          { type: 'mrkdwn', text: `📣 *Campaign*\n${lead.campaign_name || 'N/A'}` },
          { type: 'mrkdwn', text: `🎯 *Ad Set*\n${lead.adset_name || 'N/A'}` },
        ],
      },
      { type: 'divider' },
      {
        type: 'context',
        elements: [
          { type: 'mrkdwn', text: `🕐 ${time} ET  •  Lead ID: ${lead.id}` },
        ],
      },
    ],
  });

  console.log('Slack notified');
}

module.exports = { notifySlack };
