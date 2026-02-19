const axios = require('axios');

async function addToJustCall(lead) {
  const auth = Buffer.from(
    `${process.env.JUSTCALL_API_KEY}:${process.env.JUSTCALL_API_SECRET}`
  ).toString('base64');

  // Ensure phone is in E.164 format (e.g. +911234567890)
  const phone = lead.phone_number?.startsWith('+')
    ? lead.phone_number
    : `+${lead.phone_number}`;

  await axios.post(
    'https://api.justcall.io/v1/autodialer/campaign/contacts/add',
    {
      campaign_id: process.env.JUSTCALL_LIST_ID,
      name: lead.full_name || '',
      email: lead.email || '',
      phone,
    },
    {
      headers: {
        Authorization: `Basic ${auth}`,
        'Content-Type': 'application/json',
      },
    }
  );
  console.log('Added to JustCall');
}

module.exports = { addToJustCall };
