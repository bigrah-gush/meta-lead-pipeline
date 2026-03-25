const fetch = global.fetch || require('node-fetch');

async function sendToWebhook(lead) {
  const webhookUrl = process.env.WEBHOOK_URL;

  if (!webhookUrl) {
    console.warn('[Webhook] WEBHOOK_URL not set — skipping webhook delivery');
    return;
  }

  const payload = {
    timestamp:     lead.created_time || new Date().toISOString(),
    lead_id:       lead.id            || '',
    full_name:     lead.full_name     || '',
    phone:         lead.phone         || lead.phone_number || '',
    email:         lead.email         || '',
    company_url:   lead.website        || '',
    platform:      lead.platform      || '',
    campaign_name: lead.campaign_name || '',
    adset_name:    lead.adset_name    || '',
    ad_name:       lead.ad_name       || '',
  };

  try {
    const response = await fetch(webhookUrl, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });

    if (!response.ok) {
      throw new Error(`HTTP ${response.status} ${response.statusText}`);
    }

    console.log(`[Webhook] Delivered lead ${lead.id} — status ${response.status}`);
  } catch (err) {
    console.error(`[Webhook] Failed for lead ${lead.id}:`, err.message);
  }
}

module.exports = { sendToWebhook };
