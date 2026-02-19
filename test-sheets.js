require('dotenv').config();
const { addToSheets } = require('./handlers/sheets');

// Real leads from Meta payload (normalized — field_data already spread into object)
const leads = [
  {
    id:            '1150246933741625',
    created_time:  '2026-02-19T15:25:20+0000',
    platform:      'ig',
    campaign_name: '21 Jan | Static Experiment | Mfg',
    adset_name:    'Been Burned Before / Zero Trust',
    ad_name:       'No Retainers or Fancy Reports like your previous Agency. Just AI Systems That Pull Leads from Online Search.',
    full_name:     'Hank Tap',
    phone:         '+14075950809',
    email:         'hanknkarilyn@gmail.com',
    company_name:  'No no',
  },
  {
    id:            '25738827602467530',
    created_time:  '2026-02-19T15:22:53+0000',
    platform:      'fb',
    campaign_name: '21 Jan | Static Experiment | Mfg',
    adset_name:    'Been Burned Before / Zero Trust',
    ad_name:       'No Retainers or Fancy Reports like your previous Agency. Just AI Systems That Pull Leads from Online Search.',
    full_name:     'Michael shafer',
    phone:         '+17249546867',
    email:         'mvshafer50@gmail.com',
    company_name:  'shafer hvac inc.',
  },
  {
    id:            '918278380680712',
    created_time:  '2026-02-19T15:15:28+0000',
    platform:      'fb',
    campaign_name: '21 Jan | Static Experiment | Mfg',
    adset_name:    'Been Burned Before / Zero Trust',
    ad_name:       'No Retainers or Fancy Reports like your previous Agency. Just AI Systems That Pull Leads from Online Search.',
    full_name:     'Dale Loucks',
    phone:         '+12673340568',
    email:         'buggsy21@msn.com',
    company_name:  'Philadelphia Electric Company',
  },
  {
    id:            '1717817225855941',
    created_time:  '2026-02-19T14:06:51+0000',
    platform:      'fb',
    campaign_name: '21 Jan | Static Experiment | Mfg',
    adset_name:    'Been Burned Before / Zero Trust',
    ad_name:       'No Retainers or Fancy Reports like your previous Agency. Just AI Systems That Pull Leads from Online Search.',
    full_name:     'Cecil Glass',
    phone:         '+12707923198',
    email:         'cecilglass@glasgow-ky.com',
    company_name:  'Coldwell Banker Real Estate Corporation',
  },
];

async function run() {
  for (const lead of leads) {
    try {
      await addToSheets(lead);
      console.log(`✓ ${lead.full_name} (${lead.id})`);
    } catch (err) {
      console.error(`✗ ${lead.full_name}: ${err.message}`);
    }
  }
}

run();
