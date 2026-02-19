const axios = require('axios');

const CAMPAIGN_ALL   = '3085672';
const CAMPAIGN_RETIRE = '3085673';

function getHeaders() {
  const auth = Buffer.from(
    `${process.env.JUSTCALL_API_KEY}:${process.env.JUSTCALL_API_SECRET}`
  ).toString('base64');
  return {
    Authorization: `Basic ${auth}`,
    'Content-Type': 'application/json',
    Accept: 'application/json',
  };
}

async function addToCampaign(lead, campaignId) {
  const phone = (lead.phone || lead.phone_number || '').startsWith('+')
    ? (lead.phone || lead.phone_number)
    : `+${lead.phone || lead.phone_number}`;

  await axios.post(
    'https://api.justcall.io/v2.1/sales_dialer/campaigns/contact',
    {
      campaign_id: campaignId,
      name: lead.full_name || '',
      email: lead.email || '',
      phone,
    },
    { headers: getHeaders() }
  );

  console.log(`Added to JustCall campaign ${campaignId}`);
}

async function addToJustCall(lead) {
  const tasks = [addToCampaign(lead, CAMPAIGN_ALL)];

  if (/retire/i.test(lead.company_name || '')) {
    tasks.push(addToCampaign(lead, CAMPAIGN_RETIRE));
  }

  await Promise.all(tasks);
}

module.exports = { addToJustCall };
