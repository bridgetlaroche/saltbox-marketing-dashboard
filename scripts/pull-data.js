/**
 * Saltbox Marketing Dashboard — Data Pull Script
 *
 * Pulls data from NetSuite (revenue + spend), HubSpot (leads),
 * and OfficeRnD (members), calculates KPIs, writes dashboard-data.json.
 *
 * Environment variables required:
 *   NETSUITE_ACCOUNT_ID, NETSUITE_CONSUMER_KEY, NETSUITE_CONSUMER_SECRET,
 *   NETSUITE_TOKEN_ID, NETSUITE_TOKEN_SECRET
 *   HUBSPOT_TOKEN
 *   OFFICERND_API_KEY, OFFICERND_ORG_SLUG  (optional — falls back to config)
 */

const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const OAuth = require('oauth-1.0a');
const CryptoJS = require('crypto-js');

// ---------------------------------------------------------------------------
// CONFIG
// ---------------------------------------------------------------------------

const LOCATIONS = [
  'ATL-UWS', 'ATL-WSP', 'DAL-CAR', 'DAL-FB', 'DC-ALX',
  'DEN-PH', 'LA-DUA', 'LA-TOR', 'MIA-DR', 'PHX-ST', 'SEA-SODO'
];

// Monthly salary costs per location — update when salaries change
const SALARY_CONFIG = {
  'ATL-UWS': 0,
  'ATL-WSP': 0,
  'DAL-CAR': 0,
  'DAL-FB': 0,
  'DC-ALX': 0,
  'DEN-PH': 0,
  'LA-DUA': 0,
  'LA-TOR': 0,
  'MIA-DR': 0,
  'PHX-ST': 0,
  'SEA-SODO': 0,
  'Corp': 72082, // Total marketing payroll — update from Salaries tab
};

// HubSpot location property → dashboard location code
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

// How many months of history to pull
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

// ---------------------------------------------------------------------------
// NETSUITE — OAuth 1.0 + SuiteQL
// ---------------------------------------------------------------------------

function createNetSuiteClient() {
  const accountId = env('NETSUITE_ACCOUNT_ID');
  const consumerKey = env('NETSUITE_CONSUMER_KEY');
  const consumerSecret = env('NETSUITE_CONSUMER_SECRET');
  const tokenId = env('NETSUITE_TOKEN_ID');
  const tokenSecret = env('NETSUITE_TOKEN_SECRET');

  const baseUrl = `https://${accountId.replace('_', '-')}.suitetalk.api.netsuite.com`;

  const oauth = OAuth({
    consumer: { key: consumerKey, secret: consumerSecret },
    signature_method: 'HMAC-SHA256',
    hash_function(baseString, key) {
      return CryptoJS.HmacSHA256(baseString, key).toString(CryptoJS.enc.Base64);
    },
    realm: accountId,
  });

  const token = { key: tokenId, secret: tokenSecret };

  async function runSuiteQL(query, limit = 1000) {
    const url = `${baseUrl}/services/rest/query/v1/suiteql?limit=${limit}`;
    const requestData = { url, method: 'POST' };
    const authHeader = oauth.toHeader(oauth.authorize(requestData, token));
    authHeader.Authorization += `, realm="${accountId}"`;

    let allItems = [];
    let offset = 0;
    let hasMore = true;

    while (hasMore) {
      const paginatedUrl = `${url}&offset=${offset}`;
      const paginatedRequest = { url: paginatedUrl, method: 'POST' };
      const paginatedAuth = oauth.toHeader(oauth.authorize(paginatedRequest, token));
      paginatedAuth.Authorization += `, realm="${accountId}"`;

      const resp = await fetch(paginatedUrl, {
        method: 'POST',
        headers: {
          ...paginatedAuth,
          'Content-Type': 'application/json',
          'Prefer': 'transient',
        },
        body: JSON.stringify({ q: query }),
      });

      if (!resp.ok) {
        const text = await resp.text();
        throw new Error(`NetSuite API error ${resp.status}: ${text}`);
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
  const query = `
    SELECT
      l.name AS location_name,
      SUM(paa.amount) AS total_amount
    FROM PostingAccountActivity paa
    LEFT JOIN Account a ON paa.account = a.id
    LEFT JOIN Location l ON paa.location = l.id
    WHERE paa.activitydate >= TO_DATE('${startDate}', 'YYYY-MM-DD')
      AND paa.activitydate <= TO_DATE('${endDate}', 'YYYY-MM-DD')
      AND a.acctnumber LIKE '40%'
      AND l.name IN (${LOCATIONS.map(l => `'${l}'`).join(', ')})
    GROUP BY l.name
  `;
  const rows = await ns.runSuiteQL(query);
  const result = {};
  for (const row of rows) {
    // Revenue is negative in NetSuite, flip sign
    result[row.location_name] = Math.abs(row.total_amount || 0);
  }
  return result;
}

async function pullNetSuiteMarketingSpend(ns, startDate, endDate) {
  const query = `
    SELECT
      l.name AS location_name,
      SUM(paa.amount) AS total_amount
    FROM PostingAccountActivity paa
    LEFT JOIN Account a ON paa.account = a.id
    LEFT JOIN Location l ON paa.location = l.id
    WHERE paa.activitydate >= TO_DATE('${startDate}', 'YYYY-MM-DD')
      AND paa.activitydate <= TO_DATE('${endDate}', 'YYYY-MM-DD')
      AND a.acctnumber LIKE '65%'
      AND l.name IN (${LOCATIONS.map(l => `'${l}'`).join(', ')}, 'Corp')
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
// HUBSPOT — Lead counts by location
// ---------------------------------------------------------------------------

async function pullHubSpotLeads(startDate, endDate) {
  const token = env('HUBSPOT_TOKEN');
  const result = {};

  // Search for contacts that entered Lead lifecycle stage in the date range
  // We paginate through results, counting by location property
  let after = undefined;
  const locationCounts = {};

  while (true) {
    const body = {
      filterGroups: [{
        filters: [
          {
            propertyName: 'hs_lifecyclestage_lead_date',
            operator: 'GTE',
            value: new Date(startDate).getTime(),
          },
          {
            propertyName: 'hs_lifecyclestage_lead_date',
            operator: 'LTE',
            value: new Date(endDate + 'T23:59:59Z').getTime(),
          },
        ],
      }],
      properties: ['saltbox_location'],
      limit: 100,
    };
    if (after) body.after = after;

    const resp = await fetch('https://api.hubapi.com/crm/v3/objects/contacts/search', {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${token}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(body),
    });

    if (!resp.ok) {
      const text = await resp.text();
      console.warn(`HubSpot API error ${resp.status}: ${text}`);
      break;
    }

    const data = await resp.json();
    for (const contact of (data.results || [])) {
      const loc = contact.properties?.saltbox_location || '';
      const mapped = HUBSPOT_LOCATION_MAP[loc];
      if (mapped) {
        locationCounts[mapped] = (locationCounts[mapped] || 0) + 1;
      }
    }

    if (data.paging?.next?.after) {
      after = data.paging.next.after;
    } else {
      break;
    }
  }

  return locationCounts;
}

// ---------------------------------------------------------------------------
// OFFICERND — Member counts and duration
// ---------------------------------------------------------------------------

async function pullOfficeRnDMembers(startDate, endDate) {
  const apiKey = env('OFFICERND_API_KEY', false);
  const orgSlug = env('OFFICERND_ORG_SLUG', false);

  if (!apiKey || !orgSlug) {
    console.warn('OfficeRnD credentials not set — using placeholder member data');
    // Return placeholder data; the dashboard will show what's available
    return { memberCounts: {}, newMembers: {}, avgDuration: {} };
  }

  const baseUrl = `https://app.officernd.com/api/v1/organizations/${orgSlug}`;
  const headers = { 'Authorization': `Bearer ${apiKey}`, 'Content-Type': 'application/json' };

  // Pull active members
  const resp = await fetch(`${baseUrl}/members?status=active&$limit=1000`, { headers });
  if (!resp.ok) {
    console.warn(`OfficeRnD API error ${resp.status}: ${await resp.text()}`);
    return { memberCounts: {}, newMembers: {}, avgDuration: {} };
  }

  const members = await resp.json();
  const memberCounts = {};
  const newMembers = {};
  const durations = {};

  const monthStart = new Date(startDate);
  const monthEnd = new Date(endDate);

  for (const member of members) {
    const office = member.office?.name || '';
    // Map OfficeRnD office name to dashboard location
    // This mapping may need adjustment based on actual OfficeRnD office names
    const loc = LOCATIONS.find(l => office.includes(l)) || null;
    if (!loc) continue;

    memberCounts[loc] = (memberCounts[loc] || 0) + 1;

    // Check if member started in this month
    const startDt = new Date(member.startDate || member.createdAt);
    if (startDt >= monthStart && startDt <= monthEnd) {
      newMembers[loc] = (newMembers[loc] || 0) + 1;
    }

    // Calculate duration in months
    const memberStart = new Date(member.startDate || member.createdAt);
    const end = member.endDate ? new Date(member.endDate) : new Date();
    const durationMonths = (end - memberStart) / (1000 * 60 * 60 * 24 * 30.44);
    if (!durations[loc]) durations[loc] = [];
    durations[loc].push(durationMonths);
  }

  const avgDuration = {};
  for (const [loc, durs] of Object.entries(durations)) {
    avgDuration[loc] = durs.reduce((a, b) => a + b, 0) / durs.length;
  }

  return { memberCounts, newMembers, avgDuration };
}

// ---------------------------------------------------------------------------
// KPI CALCULATIONS
// ---------------------------------------------------------------------------

function calculateKPIs(revenue, mktgSpend, leads, memberData) {
  const { memberCounts, newMembers, avgDuration } = memberData;
  const corpSpend = mktgSpend['Corp'] || 0;
  const totalLocSpend = LOCATIONS.reduce((sum, l) => sum + (mktgSpend[l] || 0), 0);

  // Distribute corp spend proportionally to location spend
  const corpAllocation = {};
  for (const loc of LOCATIONS) {
    const locShare = totalLocSpend > 0 ? (mktgSpend[loc] || 0) / totalLocSpend : 1 / LOCATIONS.length;
    corpAllocation[loc] = corpSpend * locShare;
  }

  const totalSalary = SALARY_CONFIG['Corp'] || 0;
  const salaryAllocation = {};
  for (const loc of LOCATIONS) {
    const locShare = totalLocSpend > 0 ? (mktgSpend[loc] || 0) / totalLocSpend : 1 / LOCATIONS.length;
    salaryAllocation[loc] = totalSalary * locShare;
  }

  const data = {};
  let totRev = 0, totSpend = 0, totCorpAlloc = 0, totSalary2 = 0;
  let totMembers = 0, totNewMembers = 0, totLeads = 0;
  let durationSum = 0, durationCount = 0;

  for (const loc of LOCATIONS) {
    const rev = revenue[loc] || 0;
    const spend = mktgSpend[loc] || 0;
    const corpAlloc = corpAllocation[loc] || 0;
    const salary = salaryAllocation[loc] || 0;
    const leadCount = leads[loc] || 0;
    const members = memberCounts[loc] || 0;
    const newMem = newMembers[loc] || 0;
    const duration = avgDuration[loc] || 0;

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
      cac: r2(cac),
      corpCac: r2(corpCac),
      allInCac: r2(allInCac),
      avgRevPerMember: r2(avgRevPerMember),
      avgMemberDuration: r2(duration || null),
      ltv: r2(ltv),
      mktgSpendToRev: r2(mktgSpendToRev),
      corpMktgToRev: r2(corpMktgToRev),
      allInSpendToRev: r2(allInSpendToRev),
      costPerLead: r2(costPerLead),
    };

    totRev += rev;
    totSpend += spend;
    totCorpAlloc += corpAlloc;
    totSalary2 += salary;
    totMembers += members;
    totNewMembers += newMem;
    totLeads += leadCount;
    if (duration > 0) { durationSum += duration * members; durationCount += members; }
  }

  const totDuration = durationCount > 0 ? durationSum / durationCount : null;
  const totAvgRev = totMembers > 0 ? totRev / totMembers : null;

  const totals = {
    cac: r2(totNewMembers > 0 ? totSpend / totNewMembers : null),
    corpCac: r2(totNewMembers > 0 ? (totSpend + totCorpAlloc) / totNewMembers : null),
    allInCac: r2(totNewMembers > 0 ? (totSpend + totCorpAlloc + totSalary2) / totNewMembers : null),
    avgRevPerMember: r2(totAvgRev),
    avgMemberDuration: r2(totDuration),
    ltv: r2(totAvgRev && totDuration ? totAvgRev * totDuration : null),
    mktgSpendToRev: r2(totRev > 0 ? totSpend / totRev : null),
    corpMktgToRev: r2(totRev > 0 ? (totSpend + totCorpAlloc) / totRev : null),
    allInSpendToRev: r2(totRev > 0 ? (totSpend + totCorpAlloc + totSalary2) / totRev : null),
    costPerLead: r2(totLeads > 0 ? totSpend / totLeads : null),
  };

  return { data, totals };
}

function r2(v) {
  return v != null ? Math.round(v * 100) / 100 : null;
}

// ---------------------------------------------------------------------------
// MAIN
// ---------------------------------------------------------------------------

async function main() {
  console.log('Saltbox Marketing Dashboard — Data Pull');
  console.log('========================================\n');

  const months = getMonthRange(MONTHS_BACK);
  console.log(`Pulling ${months.length} months: ${months[0].key} → ${months[months.length - 1].key}\n`);

  // Load existing data if available (to preserve months we've already pulled)
  const dataPath = path.join(__dirname, '..', 'data', 'dashboard-data.json');
  let existing = { months: [], locations: LOCATIONS, data: {}, totals: {} };
  if (fs.existsSync(dataPath)) {
    try {
      existing = JSON.parse(fs.readFileSync(dataPath, 'utf-8'));
    } catch (e) {
      console.warn('Could not read existing data file, starting fresh');
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

    // Check if we already have this month and it's not the most recent
    const isCurrentMonth = month === months[months.length - 1];
    const isPreviousMonth = month === months[months.length - 2];
    if (existing.data[month.key] && !isCurrentMonth && !isPreviousMonth) {
      console.log('  Using cached data');
      result.data[month.key] = existing.data[month.key];
      result.totals[month.key] = existing.totals[month.key];
      continue;
    }

    try {
      // Pull from all sources
      console.log('  Pulling NetSuite revenue...');
      const revenue = await pullNetSuiteRevenue(ns, month.start, month.end);

      console.log('  Pulling NetSuite marketing spend...');
      const spend = await pullNetSuiteMarketingSpend(ns, month.start, month.end);

      console.log('  Pulling HubSpot leads...');
      const leads = await pullHubSpotLeads(month.start, month.end);

      console.log('  Pulling OfficeRnD members...');
      const members = await pullOfficeRnDMembers(month.start, month.end);

      // Calculate KPIs
      const { data, totals } = calculateKPIs(revenue, spend, leads, members);
      result.data[month.key] = data;
      result.totals[month.key] = totals;

      console.log('  Done.\n');
    } catch (err) {
      console.error(`  Error pulling ${month.key}: ${err.message}`);
      // Fall back to existing data if available
      if (existing.data[month.key]) {
        console.log('  Falling back to cached data');
        result.data[month.key] = existing.data[month.key];
        result.totals[month.key] = existing.totals[month.key];
      }
    }
  }

  // Write output
  fs.writeFileSync(dataPath, JSON.stringify(result, null, 2));
  console.log(`\nWritten to ${dataPath}`);
  console.log('Done!');
}

main().catch(err => {
  console.error('Fatal error:', err);
  process.exit(1);
});
