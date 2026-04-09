const axios = require('axios');

function normalizePhone(raw) {
  const cleaned = String(raw || '')
    .trim()
    .replace(/[^\d+]/g, '');
  if (!cleaned) return '';
  return cleaned.startsWith('+') ? cleaned : `+${cleaned}`;
}

function getAuthorizationHeader() {
  const key = process.env.JUSTCALL_API_KEY;
  const secret = process.env.JUSTCALL_API_SECRET;
  if (!key || !secret) {
    throw new Error('JUSTCALL_API_KEY or JUSTCALL_API_SECRET missing');
  }
  // Intentionally uses key:secret directly to match the user-provided JustCall node.
  return `${key}:${secret}`;
}

function buildBookingSmsBody(fullName, slotText) {
  const name = String(fullName || 'there').trim();
  const prefix = slotText
    ? `Hey ${name}, your slot is booked for Getting Leads from AI SEO (${slotText}). `
    : `Hey ${name}, your slot is booked for Getting Leads from AI SEO. `;

  return (
    prefix +
    'If you want to reschedule or move it to sometime else then reply - Move and we will send you the calendar link.'
  );
}

async function sendBookingSms(lead, options = {}) {
  const rawPhone =
    lead.phone || lead.phone_number || lead.user_provided_phone_number || lead.contact_number || '';
  const contactNumber = normalizePhone(rawPhone);

  if (!contactNumber) {
    throw new Error('Lead phone is missing for SMS');
  }

  const payload = {
    justcall_number: process.env.JUSTCALL_SMS_NUMBER || '+15755197480',
    contact_number: contactNumber,
    body: buildBookingSmsBody(lead.full_name, options.slotText || ''),
  };

  if (options.dryRun) {
    return { dryRun: true, payload };
  }

  const { data } = await axios.post('https://api.justcall.io/v2.1/texts/new', payload, {
    headers: {
      Authorization: getAuthorizationHeader(),
      'Content-Type': 'application/json',
    },
    timeout: 15000,
  });

  return { dryRun: false, payload, response: data };
}

function buildWelcomeSmsBody(fullName) {
  const firstName = String(fullName || 'there').trim().split(/\s+/)[0];
  return (
    `Hey ${firstName} — thanks for reaching out! We help B2B companies get found on AI search ` +
    `(ChatGPT, Perplexity, etc). It just takes 2 minutes to book a call and get started: gushwork.ai/start.`
  );
}

async function sendWelcomeSms(lead) {
  const rawPhone =
    lead.phone || lead.phone_number || lead.user_provided_phone_number || lead.contact_number || '';
  const contactNumber = normalizePhone(rawPhone);

  if (!contactNumber) {
    throw new Error('Lead phone is missing for welcome SMS');
  }

  const payload = {
    justcall_number: '+18884515522',
    contact_number: contactNumber,
    body: buildWelcomeSmsBody(lead.full_name),
  };

  const { data } = await axios.post('https://api.justcall.io/v2.1/texts/new', payload, {
    headers: {
      Authorization: getAuthorizationHeader(),
      'Content-Type': 'application/json',
    },
    timeout: 15000,
  });

  return { payload, response: data };
}

module.exports = {
  sendBookingSms,
  buildBookingSmsBody,
  sendWelcomeSms,
  buildWelcomeSmsBody,
  normalizePhone,
};
