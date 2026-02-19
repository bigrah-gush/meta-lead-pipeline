const axios = require('axios');

async function fetchLeadData(leadgenId) {
  const { data } = await axios.get(
    `https://graph.facebook.com/v19.0/${leadgenId}`,
    {
      params: {
        access_token: process.env.META_PAGE_ACCESS_TOKEN,
        // Request all fields including campaign/adset/ad info
        fields: 'id,created_time,ad_id,ad_name,adset_id,adset_name,campaign_id,campaign_name,platform,field_data',
      },
    }
  );

  // Start with all top-level fields
  const lead = {
    id:            data.id,
    created_time:  data.created_time,
    platform:      data.platform || '',
    campaign_id:   data.campaign_id || '',
    campaign_name: data.campaign_name || '',
    adset_id:      data.adset_id || '',
    adset_name:    data.adset_name || '',
    ad_id:         data.ad_id || '',
    ad_name:       data.ad_name || '',
  };

  // Spread form field_data on top (full_name, phone, email, etc.)
  for (const { name, values } of (data.field_data || [])) {
    lead[name] = values[0] || '';
  }

  return lead;
}

module.exports = { fetchLeadData };
