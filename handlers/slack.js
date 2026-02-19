const https = require('https');

const CHANNEL = 'C077MFL1H4L';

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

  await postMessage({
    channel: CHANNEL,
    text: `New ${platform} Lead: ${lead.full_name || 'Unknown'}`,
    blocks: [
      {
        type: 'header',
        text: { type: 'plain_text', text: `🔔 New ${platform} Lead` },
      },
      {
        type: 'section',
        fields: [
          { type: 'mrkdwn', text: `*Name:*\n${lead.full_name || 'N/A'}` },
          { type: 'mrkdwn', text: `*Company:*\n${lead.company_name || 'N/A'}` },
          { type: 'mrkdwn', text: `*Phone:*\n${lead.phone || lead.phone_number || 'N/A'}` },
          { type: 'mrkdwn', text: `*Email:*\n${lead.email || 'N/A'}` },
        ],
      },
      {
        type: 'section',
        fields: [
          { type: 'mrkdwn', text: `*Campaign:*\n${lead.campaign_name || 'N/A'}` },
          { type: 'mrkdwn', text: `*Ad Set:*\n${lead.adset_name || 'N/A'}` },
        ],
      },
      {
        type: 'context',
        elements: [
          {
            type: 'mrkdwn',
            text: `Lead ID: ${lead.id} | ${lead.created_time}`,
          },
        ],
      },
    ],
  });

  console.log('Slack notified');
}

module.exports = { notifySlack };
