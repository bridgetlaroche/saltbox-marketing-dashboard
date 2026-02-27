/**
 * Saltbox Marketing Dashboard — Data Pull Script
 *
 * Pulls data from:
 *   - NetSuite (SuiteQL) → revenue + marketing spend by location
 *   - HubSpot → lead counts + new members (Closed Won deals) by location
 *   - Google Sheet or config → active member counts by location
 *
 * Environment variables required:
 *   NETSUITE_ACCOUNT_ID, NETSUITE_CONSUMER_KEY, NETSUITE_CONSUMER_SECRET,
 *   NETSUITE_TOKEN_ID, NETSUITE_TOKEN_SECRET
 *   HUBSPOT_TOKEN
 */

const fs = require('fs');
const path = require('path');
const OAuth = require('oauth-1.0a');
const CryptoJS = require('crypto-js');

// ---------------------------------------------------------------------------
// CONFIG
// ---------------------------------------------------------------------------

const LOCATIONS = [
  'ATL-UWS', 'ATL-WSP', 'DAL-CAR', 'DAL-FB', 'DC-ALX',
  'DEN-PH', 'LA-DUA', 'LA-TOR', 'MIA-DR', 'PHX-ST', 'SEA-SODO'
];

// Monthly marketing salary costs — update when salaries change
// Source: Salaries tab in the Excel dashboard
const SALARY_CONFIG = {
  total: 72082, // Total marketing payroll per month
};

// HubSpot saltbox_location property → dashboard location code
const HUBSPOT_LOCATION_MAP = {
  'Atlanta Upper Westside': 'ATL-UWS',
  'Atlanta Westside Park': 'ATL-WSP',
  'Dallas Carrollton': 'DAL-CAR',
  'Dallas Farmers Branch': 'DAL-FB',
  'DC Alexandria': 'DC-ALX',
  'Denver': 'DEN-PH',
  'LA Duarte': 'LA-DUA',
  'LA Torrance': 'LA-TOR',
  'Miami': 'MIA-DR',
  'Phoenix': 'PHX-ST',
  'Seattle': 'SEA-SODO',
};

// HubSpot deal stage for "Closed Won" (new member signed)
const HUBSPOT_CLOSED_WON_STAGE = '268641940';

// Known member counts (from Google Sheet "All-Location Workspace Revenue Tracker")
// These are used as fallback when Google Sheet / OfficeRnD API is not available.
// Update monthly or replace with API call when OfficeRnD key is available.
const KNOWN_MEMBER_COUNTS = {
  '2025-12': {
    'ATL-UWS': 64, 'DAL-FB': 96, 'SEA-SODO': 86, 'DEN-PH': 94,
    'LA-TOR': 72, 'DC-ALX': 73, 'LA-DUA': 55, 'DAL-CAR': 92,
    'ATL-WSP': 142, 'MIA-DR': 66, 'PHX-ST': 79,
  },
  '2026-01': {
    'ATL-UWS': 63, 'DAL-FB': 95, 'SEA-SODO': 84, 'DEN-PH': 93,
    'LA-TOR': 73, 'DC-ALX': 73, 'LA-DUA': 58, 'DAL-CAR': 96,
    'ATL-WSP': 141, 'MIA-DR': 66, 'PHX-ST': 86,
  },
  '2026-03': {
    'ATL-UWS': 64, 'DAL-FB': 106, 'SEA-SODO': 78, 'DEN-PH': 87,
    'LA-TOR': 67, 'DC-ALX': 81, 'LA-DUA': 72, 'DAL-CAR': 108,
    'ATL-WSP': 142, 'MIA-DR': 79, 'PHX-ST': 79,
  },
};

// Average member duration by location (months) — from the Excel Summary KPIs
// Update periodically; these are relatively stable
const AVG_MEMBER_DURATION = {
  'ATL-UWS': 16, 'ATL-WSP': 12.1, 'DAL-CAR': 12.5, 'DAL-FB': 16.4,
  'DC-ALX': 12.6, 'DEN-PH': 17.6, 'LA-DUA': 11.9, 'LA-TOR': 14.2,
  'MIA-DR': 12.9, 'PHX-ST': 11.9, 'SEA-SODO': 16.4,
};

const MONTHS_BACK = 12;

// ---------------------------------------------------------------------------
// UTILITIES
// ---------------------------------------------------------------------------

function getMonthRange(monthsBack) {
  const months = [];
  const now = new Date();
  for (let i = monthsBack; i >= 1; i--) {
    const d = new Date(now.getFullYear(), now.getMonth() - i, 1);
    const yyyy = d.getFullYear();
    const mm = String(d.getMonth() + 1).padStart(2, '0');
    const lastDay = new Date(yyyy, d.getMonth() + 1, 0).getDate();
    months.push({
      key: `${yyyy}-${mm}`,
      start: `${yyyy}-${mm}-01`,
      end: `${yyyy}-${mm}-${String(lastDay).padStart(2, '0')}`,
    });
  }
  return months;
}

function env(name, required = true) {
  const val = process.env[name];
  if (!val && required) {
    console.error(`Missing required environment variable: ${name}`);
    process.exit(1);
  }
  return val || '';
}

function r2(v) {
  return v != null && isFinite(v) ? Math.round(v * 100) / 100 : null;
}

// ---------------------------------------------------------------------------
// NETSUITE — OAuth 1.0 + SuiteQL
// ---------------------------------------------------------------------------

function createNetSuiteClient() {
  const accountId = env('NETSUITE_ACCOUNT_ID');
  const consumerKey = env('NETSUITE_CONSUMER_KEY');
  const consumerSecret = env('NETSUITE_CONSUMER_SECRET');
  const tokenId = env('NETSUITE_TOKEN_ID');
  const tokenSecret = env('NETSUITE_TOKEN_SECRET');

  const baseUrl = `https://${accountId.replace(/_/g, '-').toLowerCase()}.suitetalk.api.netsuite.com`;

  const oauth = OAuth({
    consumer: { key: consumerKey, secret: consumerSecret },
    signature_method: 'HMAC-SHA256',
    hash_function(baseString, key) {
      return CryptoJS.HmacSHA256(baseString, key).toString(CryptoJS.enc.Base64);
    },
    realm: accountId,
  });

  const token = { key: tokenId, secret: tokenSecret };

  async function runSuiteQL(query, limit = 200) {
    let allItems = [];
    let offset = 0;
    let hasMore = true;

    while (hasMore) {
      const url = `${baseUrl}/services/rest/query/v1/suiteql?limit=${limit}&offset=${offset}`;
      const requestData = { url, method: 'POST' };
      const authHeader = oauth.toHeader(oauth.authorize(requestData, token));
      authHeader.Authorization += `, realm="${accountId}"`;

      const resp = await fetch(url, {
        method: 'POST',
        headers: {
          ...authHeader,
          'Content-Type': 'application/json',
          'Prefer': 'transient',
        },
        body: JSON.stringify({ q: query }),
      });

      if (!resp.ok) {
        const text = await resp.text();
        throw new Error(`NetSuite API ${resp.status}: ${text}`);
      }

      const data = await resp.json();
      const items = data.items || [];
      allItems = allItems.concat(items);
      hasMore = data.hasMore === true;
      offset += items.length;
    }

    return allItems;
  }

  return { runSuiteQL };
}

async function pullNetSuiteRevenue(ns, startDate, endDate) {
  const locList = LOCATIONS.map(l => `'${l}'`).join(', ');
  const query = `
    SELECT l.name AS location_name, SUM(paa.amount) AS total_amount
    FROM PostingAccountActivity paa
    JOIN Account a ON a.id = paa.account
    LEFT JOIN Location l ON l.id = paa.location
    WHERE paa.activityDate >= TO_DATE('${startDate}', 'YYYY-MM-DD')
      AND paa.activityDate <= TO_DATE('${endDate}', 'YYYY-MM-DD')
      AND a.acctnumber LIKE '4%'
      AND l.name IN (${locList})
    GROUP BY l.name
  `;
  const rows = await ns.runSuiteQL(query);
  const result = {};
  for (const row of rows) {
    // Revenue is negative in NetSuite — flip sign
    result[row.location_name] = Math.abs(row.total_amount || 0);
  }
  return result;
}

async function pullNetSuiteMarketingSpend(ns, startDate, endDate) {
  const locList = [...LOCATIONS, 'Corp'].map(l => `'${l}'`).join(', ');
  const query = `
    SELECT l.name AS location_name, SUM(paa.amount) AS total_amount
    FROM PostingAccountActivity paa
    JOIN Account a ON a.id = paa.account
    LEFT JOIN Location l ON l.id = paa.location
    WHERE paa.activityDate >= TO_DATE('${startDate}', 'YYYY-MM-DD')
      AND paa.activityDate <= TO_DATE('${endDate}', 'YYYY-MM-DD')
      AND a.acctnumber LIKE '65%'
      AND l.name IN (${locList})
    GROUP BY l.name
  `;
  const rows = await ns.runSuiteQL(query);
  const result = {};
  for (const row of rows) {
    result[row.location_name] = row.total_amount || 0;
  }
  return result;
}

// ---------------------------------------------------------------------------
// HUBSPOT — Leads + New Members (Closed Won deals)
// ---------------------------------------------------------------------------

async function hubspotSearch(endpoint, filters, properties, token) {
  const results = [];
  let after = undefined;

  while (true) {
    const body = {
      filterGroups: [{ filters }],
      properties,
      limit: 100,
    };
    if (after) body.after = after;

    const resp = await fetch(`https://api.hubapi.com/crm/v3/objects/${endpoint}/search`, {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${token}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(body),
    });

    if (!resp.ok) {
      const text = await resp.text();
      console.warn(`  HubSpot ${endpoint} search error ${resp.status}: ${text}`);
      break;
    }

    const data = await resp.json();
    results.push(...(data.results || []));

    if (data.paging?.next?.after) {
      after = data.paging.next.after;
    } else {
      break;
    }
  }

  return results;
}

function countByLocation(results, locationProp) {
  const counts = {};
  for (const item of results) {
    const loc = item.properties?.[locationProp] || '';
    const mapped = HUBSPOT_LOCATION_MAP[loc];
    if (mapped) {
      counts[mapped] = (counts[mapped] || 0) + 1;
    }
  }
  return counts;
}

async function pullHubSpotLeads(startDate, endDate) {
  const token = env('HUBSPOT_TOKEN');
  const startMs = new Date(startDate).getTime();
  const endMs = new Date(endDate + 'T23:59:59Z').getTime();

  console.log('  Pulling leads (contacts entering Lead stage)...');
  const contacts = await hubspotSearch('contacts', [
    { propertyName: 'hs_lifecyclestage_lead_date', operator: 'GTE', value: startMs },
    { propertyName: 'hs_lifecyclestage_lead_date', operator: 'LTE', value: endMs },
  ], ['saltbox_location'], token);

  return countByLocation(contacts, 'saltbox_location');
}

async function pullHubSpotNewMembers(startDate, endDate) {
  const token = env('HUBSPOT_TOKEN');
  const startMs = new Date(startDate).getTime();
  const endMs = new Date(endDate + 'T23:59:59Z').getTime();

  console.log('  Pulling new members (Closed Won deals)...');
  const deals = await hubspotSearch('deals', [
    { propertyName: 'dealstage', operator: 'EQ', value: HUBSPOT_CLOSED_WON_STAGE },
    { propertyName: 'closedate', operator: 'GTE', value: startMs },
    { propertyName: 'closedate', operator: 'LTE', value: endMs },
  ], ['saltbox_location', 'closedate'], token);

  return countByLocation(deals, 'saltbox_location');
}

// ---------------------------------------------------------------------------
// MEMBER COUNTS — from config (replace with OfficeRnD API when ready)
// ---------------------------------------------------------------------------

function getMemberCounts(monthKey) {
  // Use known counts if available; otherwise interpolate from nearest month
  if (KNOWN_MEMBER_COUNTS[monthKey]) {
    return KNOWN_MEMBER_COUNTS[monthKey];
  }

  // Find the closest known month
  const knownMonths = Object.keys(KNOWN_MEMBER_COUNTS).sort();
  let closest = knownMonths[knownMonths.length - 1]; // default to latest
  for (const km of knownMonths) {
    if (km <= monthKey) closest = km;
  }

  console.log(`  No member counts for ${monthKey}, using ${closest} as proxy`);
  return KNOWN_MEMBER_COUNTS[closest] || {};
}

// ---------------------------------------------------------------------------
// KPI CALCULATIONS
// ---------------------------------------------------------------------------

function calculateKPIs(revenue, mktgSpend, leads, newMembers, memberCounts) {
  const corpSpend = mktgSpend['Corp'] || 0;
  const totalLocSpend = LOCATIONS.reduce((sum, l) => sum + (mktgSpend[l] || 0), 0);

  // Distribute corp spend proportionally to each location's direct spend
  const corpAllocation = {};
  const salaryAllocation = {};
  for (const loc of LOCATIONS) {
    const locShare = totalLocSpend > 0
      ? (mktgSpend[loc] || 0) / totalLocSpend
      : 1 / LOCATIONS.length;
    corpAllocation[loc] = corpSpend * locShare;
    salaryAllocation[loc] = SALARY_CONFIG.total * locShare;
  }

  const data = {};
  let totRev = 0, totSpend = 0, totCorpAlloc = 0, totSalary = 0;
  let totMembers = 0, totNewMembers = 0, totLeads = 0;
  let durationWeightedSum = 0;

  for (const loc of LOCATIONS) {
    const rev = revenue[loc] || 0;
    const spend = mktgSpend[loc] || 0;
    const corpAlloc = corpAllocation[loc] || 0;
    const salary = salaryAllocation[loc] || 0;
    const leadCount = leads[loc] || 0;
    const newMem = newMembers[loc] || 0;
    const members = memberCounts[loc] || 0;
    const duration = AVG_MEMBER_DURATION[loc] || 0;

    const avgRevPerMember = members > 0 ? rev / members : null;
    const cac = newMem > 0 ? spend / newMem : null;
    const corpCac = newMem > 0 ? (spend + corpAlloc) / newMem : null;
    const allInCac = newMem > 0 ? (spend + corpAlloc + salary) / newMem : null;
    const ltv = avgRevPerMember && duration ? avgRevPerMember * duration : null;
    const mktgSpendToRev = rev > 0 ? spend / rev : null;
    const corpMktgToRev = rev > 0 ? (spend + corpAlloc) / rev : null;
    const allInSpendToRev = rev > 0 ? (spend + corpAlloc + salary) / rev : null;
    const costPerLead = leadCount > 0 ? spend / leadCount : null;

    data[loc] = {
      cac: r2(cac), corpCac: r2(corpCac), allInCac: r2(allInCac),
      avgRevPerMember: r2(avgRevPerMember), avgMemberDuration: r2(duration || null),
      ltv: r2(ltv), mktgSpendToRev: r2(mktgSpendToRev),
      corpMktgToRev: r2(corpMktgToRev), allInSpendToRev: r2(allInSpendToRev),
      costPerLead: r2(costPerLead),
    };

    totRev += rev;
    totSpend += spend;
    totCorpAlloc += corpAlloc;
    totSalary += salary;
    totMembers += members;
    totNewMembers += newMem;
    totLeads += leadCount;
    if (duration > 0 && members > 0) durationWeightedSum += duration * members;
  }

  const totDuration = totMembers > 0 ? durationWeightedSum / totMembers : null;
  const totAvgRev = totMembers > 0 ? totRev / totMembers : null;

  const totals = {
    cac: r2(totNewMembers > 0 ? totSpend / totNewMembers : null),
    corpCac: r2(totNewMembers > 0 ? (totSpend + totCorpAlloc) / totNewMembers : null),
    allInCac: r2(totNewMembers > 0 ? (totSpend + totCorpAlloc + totSalary) / totNewMembers : null),
    avgRevPerMember: r2(totAvgRev),
    avgMemberDuration: r2(totDuration),
    ltv: r2(totAvgRev && totDuration ? totAvgRev * totDuration : null),
    mktgSpendToRev: r2(totRev > 0 ? totSpend / totRev : null),
    corpMktgToRev: r2(totRev > 0 ? (totSpend + totCorpAlloc) / totRev : null),
    allInSpendToRev: r2(totRev > 0 ? (totSpend + totCorpAlloc + totSalary) / totRev : null),
    costPerLead: r2(totLeads > 0 ? totSpend / totLeads : null),
  };

  return { data, totals };
}

// ---------------------------------------------------------------------------
// MAIN
// ---------------------------------------------------------------------------

async function main() {
  console.log('Saltbox Marketing Dashboard — Data Pull');
  console.log('========================================\n');

  const months = getMonthRange(MONTHS_BACK);
  console.log(`Pulling ${months.length} months: ${months[0].key} → ${months[months.length - 1].key}\n`);

  const dataPath = path.join(__dirname, '..', 'data', 'dashboard-data.json');
  let existing = { months: [], locations: LOCATIONS, data: {}, totals: {} };
  if (fs.existsSync(dataPath)) {
    try {
      existing = JSON.parse(fs.readFileSync(dataPath, 'utf-8'));
    } catch (e) {
      console.warn('Could not read existing data, starting fresh');
    }
  }

  const ns = createNetSuiteClient();
  const result = {
    lastUpdated: new Date().toISOString(),
    months: months.map(m => m.key),
    locations: LOCATIONS,
    data: {},
    totals: {},
  };

  for (const month of months) {
    console.log(`--- ${month.key} ---`);

    // Cache older months (only re-pull current + prior month)
    const isRecent = month === months[months.length - 1] || month === months[months.length - 2];
    if (existing.data?.[month.key] && !isRecent) {
      console.log('  Using cached data\n');
      result.data[month.key] = existing.data[month.key];
      result.totals[month.key] = existing.totals[month.key];
      continue;
    }

    try {
      console.log('  Pulling NetSuite revenue...');
      const revenue = await pullNetSuiteRevenue(ns, month.start, month.end);

      console.log('  Pulling NetSuite marketing spend...');
      const spend = await pullNetSuiteMarketingSpend(ns, month.start, month.end);

      const leads = await pullHubSpotLeads(month.start, month.end);
      const newMembers = await pullHubSpotNewMembers(month.start, month.end);

      console.log('  Getting member counts...');
      const memberCounts = getMemberCounts(month.key);

      const { data, totals } = calculateKPIs(revenue, spend, leads, newMembers, memberCounts);
      result.data[month.key] = data;
      result.totals[month.key] = totals;

      // Log summary
      const totalRev = LOCATIONS.reduce((s, l) => s + (revenue[l] || 0), 0);
      const totalSpend = LOCATIONS.reduce((s, l) => s + (spend[l] || 0), 0) + (spend['Corp'] || 0);
      const totalLeads = Object.values(leads).reduce((a, b) => a + b, 0);
      const totalNew = Object.values(newMembers).reduce((a, b) => a + b, 0);
      console.log(`  Revenue: $${Math.round(totalRev).toLocaleString()} | Spend: $${Math.round(totalSpend).toLocaleString()} | Leads: ${totalLeads} | New Members: ${totalNew}\n`);
    } catch (err) {
      console.error(`  ERROR: ${err.message}`);
      if (existing.data?.[month.key]) {
        console.log('  Falling back to cached data\n');
        result.data[month.key] = existing.data[month.key];
        result.totals[month.key] = existing.totals[month.key];
      } else {
        console.log('  No cached data available, skipping\n');
      }
    }
  }

  fs.writeFileSync(dataPath, JSON.stringify(result, null, 2));
  console.log(`Written to ${dataPath}`);
  console.log('Done!');
}

main().catch(err => {
  console.error('Fatal error:', err);
  process.exit(1);
});
