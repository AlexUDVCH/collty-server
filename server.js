// === server.js (Final version with Caching & 429/503 exponential backoff) ===
const express = require('express');
const { google } = require('googleapis');
const cors = require('cors');

require('dotenv').config();
const { randomUUID } = require('crypto');
// --- Vectors / Qdrant + Jina embeddings setup ---
const QDRANT_URL = process.env.QDRANT_URL;
const QDRANT_API_KEY = process.env.QDRANT_API_KEY;
const JINA_API_KEY = process.env.JINA_API_KEY;

// jina-embeddings-v4 outputs 2048-dim vectors
const VECTOR_SIZE = 2048;
const COLLECTION = 'orders';

if (!QDRANT_URL || !QDRANT_API_KEY || !JINA_API_KEY) {
  console.warn('[vectors] Missing env vars: QDRANT_URL / QDRANT_API_KEY / JINA_API_KEY');
}

const app = express();
const port = process.env.PORT || 3000;

const path = '/etc/secrets/credentials.json';
const spreadsheetId = '1GIl15j9L1-KPyn2evruz3F0sscNo308mAC7huXm0WkY';
const sheetOrders = 'DataBaseCollty_Teams';
const sheetLeads = 'LeadsCollty_Responses';


const allowed = [
  /^https?:\/\/([a-z0-9-]+\.)?collty\.com$/i,
  'http://localhost:3000'
];
app.use(cors({
  origin(origin, cb) {
    if (!origin) return cb(null, true);
    const ok = allowed.some(rule => rule.test ? rule.test(origin) : rule === origin);
    return cb(ok ? null : new Error('Not allowed by CORS'), ok);
  },
  methods: ['GET','HEAD','PUT','PATCH','POST','DELETE','OPTIONS'],
  allowedHeaders: ['Content-Type','Authorization'],
  credentials: true,
  maxAge: 86400,
}));
// preflight
app.options('*', cors());

app.use(express.json());


app.get('/', (req, res) => {
  res.send('✅ Server is running');
});

// === SIMPLE IN-MEMORY CACHE for leads/orders ===
const CACHE_TTL = 15 * 1000; // 15 seconds
let cacheLeads = { data: null, ts: 0 };
let cacheOrders = { data: null, ts: 0 };
const now = () => Date.now();

// === Exponential Backoff Retry helper ===
async function fetchSheetWithRetry(sheets, range, retries = 5, delayMs = 2000) {
  for (let i = 0; i < retries; i++) {
    try {
      const response = await sheets.spreadsheets.values.get({
        spreadsheetId,
        range,
      });
      return response.data.values;
    } catch (error) {
      if ((error.code === 503 || error.code === 429) && i < retries - 1) {
        const wait = delayMs * Math.pow(2, i);
        console.warn(`Retrying fetch for ${range} (${i + 1}) after ${wait}ms...`);
        await new Promise(res => setTimeout(res, wait));
      } else {
        throw error;
      }
    }
  }
}

// === Batch write helper with backoff (429/503 aware) ===
function sleep(ms){ return new Promise(r=>setTimeout(r, ms)); }

async function batchWriteValues({ sheets, spreadsheetId, updates, valueInputOption='USER_ENTERED', maxBatch=400, maxRetries=6 }) {
  // updates: Array<{ range: 'Sheet!A1', values: [[...]] }>
  const chunks = [];
  for (let i = 0; i < updates.length; i += maxBatch) {
    chunks.push(updates.slice(i, i + maxBatch));
  }

  for (const chunk of chunks) {
    let attempt = 0;
    while (true) {
      try {
        await sheets.spreadsheets.values.batchUpdate({
          spreadsheetId,
          requestBody: {
            valueInputOption,
            data: chunk
          }
        });
        break;
      } catch (err) {
        const code = err?.code || err?.response?.status;
        const retryAfter = Number(err?.response?.headers?.['retry-after'] || 0) * 1000;
        if ((code === 429 || code === 503) && attempt < maxRetries) {
          const backoff = Math.min(30000, 600 * 2 ** attempt); // up to 30s
          await sleep(Math.max(backoff, retryAfter));
          attempt++;
          continue;
        }
        throw err;
      }
    }
  }
}

// ------------- Jina embeddings (text → vector[2048]) -------------
async function embedText(text) {
  if (!JINA_API_KEY) throw new Error('JINA_API_KEY is missing');
  const resp = await fetch('https://api.jina.ai/v1/embeddings', {
    method: 'POST',
    headers: {
      'Authorization': `Bearer ${JINA_API_KEY}`,
      'Content-Type': 'application/json'
    },
    body: JSON.stringify({
      input: [String(text || '')],
      model: 'jina-embeddings-v4',
      task: 'retrieval.query',
      dimensions: 2048
    })
  });
  if (!resp.ok) {
    const t = await resp.text().catch(()=> '');
    throw new Error(`Jina error ${resp.status}: ${t}`);
  }
  const json = await resp.json();
  return json.data[0].embedding;
}

// ------------- Qdrant REST helpers -------------
async function qdrantFetch(path, init = {}) {
  if (!QDRANT_URL || !QDRANT_API_KEY) throw new Error('QDRANT_URL/QDRANT_API_KEY missing');
  const url = `${QDRANT_URL}${path}`;
  const headers = Object.assign({
    'Authorization': `Bearer ${QDRANT_API_KEY}`,
    'Content-Type': 'application/json'
  }, init.headers || {});
  const resp = await fetch(url, { ...init, headers });
  return resp;
}

async function ensureCollection() {
  // try get
  try {
    const info = await qdrantFetch(`/collections/${COLLECTION}`);
    if (info.status === 200) return true;
  } catch (_) {}
  // create
  const create = await qdrantFetch(`/collections/${COLLECTION}`, {
    method: 'PUT',
    body: JSON.stringify({ vectors: { size: VECTOR_SIZE, distance: 'Cosine' } })
  });
  if (!create.ok) {
    const t = await create.text().catch(()=> '');
    throw new Error(`Qdrant create failed ${create.status}: ${t}`);
  }
  return true;
}

async function upsertPoints(points) {
  if (!Array.isArray(points) || !points.length) return;
  const r = await qdrantFetch(`/collections/${COLLECTION}/points?wait=true`, {
    method: 'PUT',
    body: JSON.stringify({ points })
  });
  if (!r.ok) {
    const t = await r.text().catch(()=> '');
    throw new Error(`Qdrant upsert failed ${r.status}: ${t}`);
  }
}

async function vectorSearch(vector, limit = 50, filter = null) {
  const body = { vector, limit, with_payload: true };
  if (filter) body.filter = filter;
  const r = await qdrantFetch(`/collections/${COLLECTION}/points/search`, {
    method: 'POST',
    body: JSON.stringify(body)
  });
  if (!r.ok) {
    const t = await r.text().catch(()=> '');
    throw new Error(`Qdrant search failed ${r.status}: ${t}`);
  }
  const j = await r.json();
  return j.result || [];
}

// === GET /orders (with cache) ===
app.get('/orders', async (req, res) => {
  try {
    if (cacheOrders.data && now() - cacheOrders.ts < CACHE_TTL) {
      return respondFilteredOrders(cacheOrders.data, req, res);
    }
    const auth = new google.auth.GoogleAuth({ keyFile: path, scopes: ['https://www.googleapis.com/auth/spreadsheets.readonly'] });
    const client = await auth.getClient();
    const sheets = google.sheets({ version: 'v4', auth: client });
    const rows = await fetchSheetWithRetry(sheets, `${sheetOrders}!A1:ZZ1000`);
    cacheOrders = { data: rows, ts: now() };
    return respondFilteredOrders(rows, req, res);
  } catch (err) {
    console.error('Error in /orders:', err);
    res.status(200).json([]);
  }
});

function respondFilteredOrders(rows, req, res) {
  if (!rows || rows.length === 0) return res.json([]);
  const headers = rows[0].map(h => h.trim());
  const data = rows.slice(1).map(row => headers.reduce((obj, key, i) => {
    obj[key] = row[i] || '';
    return obj;
  }, {}));
  const emailQuery = (req.query.email || '').toLowerCase().trim();
  const typeQuery = (req.query.type || '').toLowerCase().trim();
  const type2Query = (req.query.type2 || '').toLowerCase().trim();
  const confirmed = req.query.confirmed === 'true';
  const typeTerms = typeQuery.split(/[+,]/).map(t => t.trim()).filter(Boolean);
  const filtered = data.filter(row => {
    const email = (row.Email || '').toLowerCase();
    const type = (row.Type || '').toLowerCase();
    const type2 = (row.Type2 || '').toLowerCase();
    const textarea = (row.Textarea || '').toLowerCase();
    const matchEmail = emailQuery ? email.includes(emailQuery) : true;
    const matchType = typeTerms.length ? typeTerms.every(term => type.includes(term) || type2.includes(term)) : true;
    const matchType2 = type2Query ? type2.includes(type2Query) : true;
    const matchConfirmed = confirmed ? textarea.includes('confirmed') : true;
    return matchEmail && matchType && matchType2 && matchConfirmed;
  });
  res.json(filtered);
}

// === GET /leads (with cache) ===
app.get('/leads', async (req, res) => {
  try {
    if (cacheLeads.data && now() - cacheLeads.ts < CACHE_TTL) {
      return respondFilteredLeads(cacheLeads.data, req, res);
    }
    const auth = new google.auth.GoogleAuth({ keyFile: path, scopes: ['https://www.googleapis.com/auth/spreadsheets.readonly'] });
    const client = await auth.getClient();
    const sheets = google.sheets({ version: 'v4', auth: client });
    const rows = await fetchSheetWithRetry(sheets, `${sheetLeads}!A1:ZZ1000`);
    cacheLeads = { data: rows, ts: now() };
    return respondFilteredLeads(rows, req, res);
  } catch (err) {
    console.error('Error in /leads:', err);
    res.status(200).json([]);
  }
});

function respondFilteredLeads(rows, req, res) {
  if (!rows || rows.length === 0) return res.json([]);
  const headers = rows[0].map(h => h.trim());
  const data = rows.slice(1).map(row => headers.reduce((obj, key, i) => {
    obj[key] = row[i] || '';
    return obj;
  }, {}));
  const emailQuery = (req.query.email || '').toLowerCase().trim();
  const partnerQuery = (req.query.partner || '').toLowerCase().trim();
  const confirmed = req.query.confirmed === 'true';
  const filtered = data.filter(row => {
    const email = (row.Email || row.email || '').toLowerCase();
    const partner = (row.partner || '').toLowerCase();
    const textarea = (row.Textarea || '').toLowerCase();
    const matchEmail = emailQuery ? email.includes(emailQuery) : true;
    const matchPartner = partnerQuery ? partner.includes(partnerQuery) : true;
    const matchConfirmed = confirmed ? textarea.includes('confirmed') : true;
    return matchEmail && matchPartner && matchConfirmed;
  });
  res.json(filtered);
}

function rowsToOrders(rows) {
  if (!rows || rows.length === 0) return [];
  const headers = rows[0].map(h => h.trim());
  return rows.slice(1).map(row => headers.reduce((obj, key, i) => {
    obj[key] = row[i] || '';
    return obj;
  }, {}));
}

function buildSearchText(order) {
  const S = v => String(v || '').trim();

  // Core fields
  const name = S(order.TeamName);
  const type = S(order.Type);
  const type2 = S(order.Type2);
  const tags = S(order.Textarea); // free-form tags/keywords
  const industry = S(order.industrymarket_expertise);
  const overview = S(order.X1Q);
  const status1 = S(order.Status1);
  const status2 = S(order.Status2);
  const partners = S(order.Partner_confirmation);

  // Derive short acronyms from Type/Type2 (PR, SMM, CI/CD -> CICD -> CI/CD etc.)
  const acronymOf = (str) => S(str)
    .split(/[^a-z0-9]+/i)
    .filter(Boolean)
    .map(w => w[0])
    .join('')
    .toUpperCase();
  const typeAcr = acronymOf(type);
  const type2Acr = acronymOf(type2);

  // Specialists & CV snippets (sp1..sp10, spcv1..spcv10)
  const specialists = [];
  for (let i = 1; i <= 10; i++) {
    specialists.push(S(order[`sp${i}`]));
    specialists.push(S(order[`spcv${i}`]));
  }

  // Extra context
  const projectId = S(order.projectid);
  const brief = S(order.Brief);
  const docs = S(order.Documents);
  const nda = S(order.nda);

  // Weighting: repeat highly-informative fields to bias embedding similarity
  const strong = [type, type2].filter(Boolean).join(' | ');
  const strongBoost = [strong, strong, strong].filter(Boolean).join(' | '); // 3x boost for Type/Type2

  const soft = [tags, industry, overview].filter(Boolean).join(' | ');

  // Include acronyms only if they look meaningful (2-5 chars). Also normalize CI/CD forms.
  const acrsRaw = [typeAcr, type2Acr]
    .filter(a => a && a.length >= 2 && a.length <= 5);
  const acrs = acrsRaw
    .map(a => a.replace(/\//g, '')) // CI/CD -> CICD
    .join(' ');

  return [
    name,
    strongBoost,            // boosted Type/Type2
    acrs,                   // PR / SMM / CI / CICD
    soft,                   // tags + industry + overview
    status1, status2,
    partners,
    specialists.join(' | '),
    projectId,
    brief, docs, nda,
  ]
    .filter(Boolean)
    .join(' | ');
}

// === GET /keywords ===
app.get('/keywords', async (req, res) => {
  try {
    // Нет кэша, так как редко используется и не критично для лимитов
    const auth = new google.auth.GoogleAuth({ keyFile: path, scopes: ['https://www.googleapis.com/auth/spreadsheets.readonly'] });
    const client = await auth.getClient();
    const sheets = google.sheets({ version: 'v4', auth: client });
    const rows = await fetchSheetWithRetry(sheets, `${sheetOrders}!A1:ZZ1000`);
    if (!rows || rows.length === 0) return res.json({ type: [], type2: [] });
    const headers = rows[0];
    const typeIndex = headers.findIndex(h => h.trim().toLowerCase() === 'type');
    const type2Index = headers.findIndex(h => h.trim().toLowerCase() === 'type2');
    const type = new Set();
    const type2 = new Set();
    rows.slice(1).forEach(row => {
      if (typeIndex >= 0 && row[typeIndex]) {
        row[typeIndex].split(',').map(s => s.trim()).forEach(s => type.add(s));
      }
      if (type2Index >= 0 && row[type2Index]) {
        row[type2Index].split(',').map(s => s.trim()).forEach(s => type2.add(s));
      }
    });
    res.json({ type: Array.from(type), type2: Array.from(type2) });
  } catch (err) {
    console.error('Error in /keywords:', err);
    res.status(500).json({ error: 'Failed to load keywords' });
  }
});

// === POST /addOrder ===
app.post('/addOrder', async (req, res) => {
  try {
    const {
      name, email, partner, teamName, specialists = [],
      Status1 = '', Status2 = '', "Payment status": PaymentStatus = '', Textarea = '', startDate = '',
      Type = '', Type2 = '',
      X1Q = '', industrymarket_expertise = '', anticipated_project_start_date = '',
      Partner_confirmation = '', Brief = '', Chat = '', Documents = '', nda = '',
      Link = '', totalsumm = '', month = '',
      Confirmation = '', PConfirmation = '',
      spcv1 = '', spcv2 = '', spcv3 = '', spcv4 = '', spcv5 = '',
      spcv6 = '', spcv7 = '', spcv8 = '', spcv9 = '', spcv10 = ''
    } = req.body;
    const auth = new google.auth.GoogleAuth({ keyFile: path, scopes: ['https://www.googleapis.com/auth/spreadsheets'] });
    const client = await auth.getClient();
    const sheets = google.sheets({ version: 'v4', auth: client });
    const now = new Date().toLocaleString('en-GB', { timeZone: 'Asia/Tbilisi' });
    const flat = [];
    for (let i = 0; i < 10; i++) {
      const s = specialists[i] || {};
      flat.push(s.sp || '', s.hours || '', s.rate || s.quantity || '', s.cost || '');
    }
    const spcvs = [spcv1, spcv2, spcv3, spcv4, spcv5, spcv6, spcv7, spcv8, spcv9, spcv10];
    const row = [
      now, name, email, partner, teamName,
      Status1, Status2, PaymentStatus, Textarea, startDate,
      Partner_confirmation, totalsumm, month, X1Q, '',
      anticipated_project_start_date, industrymarket_expertise, Type, Type2,
      ...flat, Brief, Chat, Documents, nda, Link,
      Confirmation, PConfirmation,
      ...spcvs
    ];
    await sheets.spreadsheets.values.append({
      spreadsheetId,
      range: `${sheetLeads}!A1`,
      valueInputOption: 'USER_ENTERED',
      insertDataOption: 'INSERT_ROWS',
      requestBody: { values: [row] },
    });
    res.status(200).json({ success: true });
    // Сброс кэша leads (иначе /leads отдаст устаревшие данные)
    cacheLeads = { data: null, ts: 0 };
  } catch (err) {
    console.error('Error in /addOrder:', err);
    res.status(500).json({ error: 'Failed to append data' });
  }
});

// === PATCH /confirm ===
app.patch('/confirm', async (req, res) => {
  const { email, timestamp } = req.body;
  if (!email || !timestamp) return res.status(400).json({ error: 'Missing email or timestamp' });
  try {
    const auth = new google.auth.GoogleAuth({ keyFile: path, scopes: ['https://www.googleapis.com/auth/spreadsheets'] });
    const client = await auth.getClient();
    const sheets = google.sheets({ version: 'v4', auth: client });
    const rows = await fetchSheetWithRetry(sheets, `${sheetLeads}!A1:ZZ1000`);
    const headers = rows[0];
    const emailCol = headers.findIndex(h => h.trim().toLowerCase() === 'email');
    const timeCol = headers.findIndex(h => h.trim().toLowerCase() === 'timestamp');
    const confirmCol = headers.findIndex(h => h.trim().toLowerCase() === 'confirmation');
    const targetRowIndex = rows.findIndex((row, i) =>
      i > 0 &&
      (row[emailCol] || '').toLowerCase().trim() === email.toLowerCase().trim() &&
      (row[timeCol] || '').trim() === timestamp.trim()
    );
    if (targetRowIndex < 1) return res.status(404).json({ error: 'Matching row not found' });
    const range = `${sheetLeads}!${columnToLetter(confirmCol)}${targetRowIndex + 1}`;
    await sheets.spreadsheets.values.update({
      spreadsheetId,
      range,
      valueInputOption: 'USER_ENTERED',
      requestBody: { values: [['Confirmed']] },
    });
    res.status(200).json({ success: true });
    cacheLeads = { data: null, ts: 0 };
  } catch (err) {
    console.error('Error in /confirm:', err);
    res.status(500).json({ error: 'Failed to confirm' });
  }
});

// === PATCH /updatePConfirmation ===
app.patch('/updatePConfirmation', async (req, res) => {
  const { email, timestamp, newValue } = req.body;
  if (!email || !timestamp) return res.status(400).json({ error: 'Missing email or timestamp' });
  try {
    const auth = new google.auth.GoogleAuth({ keyFile: path, scopes: ['https://www.googleapis.com/auth/spreadsheets'] });
    const client = await auth.getClient();
    const sheets = google.sheets({ version: 'v4', auth: client });
    const rows = await fetchSheetWithRetry(sheets, `${sheetLeads}!A1:ZZ1000`);
    const headers = rows[0];
    const emailCol = headers.findIndex(h => h.trim().toLowerCase() === 'email');
    const timeCol = headers.findIndex(h => h.trim().toLowerCase() === 'timestamp');
    const pConfirmCol = headers.findIndex(h => h.trim().toLowerCase() === 'pconfirmation');
    const targetRowIndex = rows.findIndex((row, i) =>
      i > 0 &&
      (row[emailCol] || '').toLowerCase().trim() === email.toLowerCase().trim() &&
      (row[timeCol] || '').trim() === timestamp.trim()
    );
    if (targetRowIndex < 1) return res.status(404).json({ error: 'Matching row not found' });
    const range = `${sheetLeads}!${columnToLetter(pConfirmCol)}${targetRowIndex + 1}`;
    await sheets.spreadsheets.values.update({
      spreadsheetId,
      range,
      valueInputOption: 'USER_ENTERED',
      requestBody: { values: [[newValue]] },
    });
    res.status(200).json({ success: true });
    cacheLeads = { data: null, ts: 0 };
  } catch (err) {
    console.error('Error in /updatePConfirmation:', err);
    res.status(500).json({ error: 'Failed to update PConfirmation' });
  }
});

// === PATCH /updateStatus2 ===
app.patch('/updateStatus2', async (req, res) => {
  const { email, timestamp, newValue } = req.body;
  if (!email || !timestamp || !newValue) return res.status(400).json({ error: 'Missing required fields' });
  try {
    const auth = new google.auth.GoogleAuth({ keyFile: path, scopes: ['https://www.googleapis.com/auth/spreadsheets'] });
    const client = await auth.getClient();
    const sheets = google.sheets({ version: 'v4', auth: client });
    const rows = await fetchSheetWithRetry(sheets, `${sheetLeads}!A1:ZZ1000`);
    const headers = rows[0];
    const emailCol = headers.findIndex(h => h.trim().toLowerCase() === 'email');
    const timeCol = headers.findIndex(h => h.trim().toLowerCase() === 'timestamp');
    const status2Col = headers.findIndex(h => h.trim().toLowerCase() === 'status2');
    const targetRowIndex = rows.findIndex((row, i) =>
      i > 0 &&
      (row[emailCol] || '').toLowerCase().trim() === email.toLowerCase().trim() &&
      (row[timeCol] || '').trim() === timestamp.trim()
    );
    if (targetRowIndex < 1) return res.status(404).json({ error: 'Matching row not found' });
    const range = `${sheetLeads}!${columnToLetter(status2Col)}${targetRowIndex + 1}`;
    await sheets.spreadsheets.values.update({
      spreadsheetId,
      range,
      valueInputOption: 'USER_ENTERED',
      requestBody: { values: [[newValue]] },
    });
    res.status(200).json({ success: true });
    cacheLeads = { data: null, ts: 0 };
  } catch (err) {
    console.error('Error in /updateStatus2:', err);
    res.status(500).json({ error: 'Failed to update Status2' });
  }
});

// === DELETE /deleteOrder ===
app.delete('/deleteOrder', async (req, res) => {
  try {
    const { email, timestamp } = req.body;
    if (!email || !timestamp) {
      return res.status(400).json({ error: 'Missing email or timestamp' });
    }
    const auth = new google.auth.GoogleAuth({ keyFile: path, scopes: ['https://www.googleapis.com/auth/spreadsheets'] });
    const client = await auth.getClient();
    const sheets = google.sheets({ version: 'v4', auth: client });
    const rows = await fetchSheetWithRetry(sheets, `${sheetLeads}!A1:ZZ1000`);
    const headers = rows[0];
    const emailCol = headers.findIndex(h => h.trim().toLowerCase() === 'email');
    const timeCol = headers.findIndex(h => h.trim().toLowerCase() === 'timestamp');
    const targetRowIndex = rows.findIndex((row, i) =>
      i > 0 &&
      (row[emailCol] || '').toLowerCase().trim() === email.toLowerCase().trim() &&
      (row[timeCol] || '').trim() === timestamp.trim()
    );
    if (targetRowIndex < 1) return res.status(404).json({ error: 'Matching row not found' });
    await sheets.spreadsheets.batchUpdate({
      spreadsheetId,
      requestBody: {
        requests: [
          {
            deleteDimension: {
              range: {
                sheetId: 1182114241,
                dimension: 'ROWS',
                startIndex: targetRowIndex,
                endIndex: targetRowIndex + 1,
              }
            }
          }
        ]
      }
    });
    res.json({ success: true });
    cacheLeads = { data: null, ts: 0 };
  } catch (err) {
    console.error('Error in /deleteOrder:', err);
    res.status(500).json({ error: 'Failed to delete order' });
  }
});

// === PATCH /updateOrderHours ===
app.patch('/updateOrderHours', async (req, res) => {
  const { email, timestamp, ...fields } = req.body;
  if (!email || !timestamp) return res.status(400).json({ error: 'Missing email or timestamp' });
  try {
    const auth = new google.auth.GoogleAuth({ keyFile: path, scopes: ['https://www.googleapis.com/auth/spreadsheets'] });
    const client = await auth.getClient();
    const sheets = google.sheets({ version: 'v4', auth: client });
    const rows = await fetchSheetWithRetry(sheets, `${sheetLeads}!A1:ZZ1000`);
    const headers = rows[0];
    const emailCol = headers.findIndex(h => h.trim().toLowerCase() === 'email');
    const timeCol = headers.findIndex(h => h.trim().toLowerCase() === 'timestamp');
    const targetRowIndex = rows.findIndex((row, i) =>
      i > 0 &&
      (row[emailCol] || '').toLowerCase().trim() === email.toLowerCase().trim() &&
      (row[timeCol] || '').trim() === timestamp.trim()
    );
    if (targetRowIndex < 1) return res.status(404).json({ error: 'Matching row not found' });
    const updates = [];
    Object.entries(fields).forEach(([key, value]) => {
      const col = headers.findIndex(h => h.trim().toLowerCase() === key.toLowerCase());
      if (col >= 0) {
        updates.push({
          range: `${sheetLeads}!${columnToLetter(col)}${targetRowIndex + 1}`,
          value: value
        });
      }
    });
    await batchWriteValues({
      sheets, spreadsheetId,
      updates: updates.map(u => ({ range: u.range, values: [[u.value]] }))
    });
    res.status(200).json({ success: true });
    cacheLeads = { data: null, ts: 0 };
  } catch (err) {
    console.error('Error in /updateOrderHours:', err);
    res.status(500).json({ error: 'Failed to update hours' });
  }
});


// === UPDATE TEAM (универсальный хэндлер для PATCH/POST/PUT и со слэшем/без) ===
async function updateTeamHandler(req, res) {
  // --- Normalize payload: enforce timestamp as required, support flexible renaming ---
  const rawBody = req.body || {};
  const tsBody = rawBody.timestamp ?? rawBody.Timestamp ?? null;
  if (!tsBody) {
    return res.status(400).json({ error: 'timestamp is required' });
  }
  // optional new name to rename
  const newTeamName = rawBody.newTeamName ?? rawBody.TeamName ?? rawBody.teamName ?? rawBody.teamNameNew ?? rawBody.team_name_new ?? null;
  // exclude known keys from fields to update
  const {
    teamName: _omitTN,
    TeamName: _omitTN2,
    teamname: _omitTN3,
    currentTeamName: _omitTN4,
    TeamName_old: _omitTN5,
    oldTeamName: _omitTN6,
    newTeamName: _omitTN7,
    teamNameNew: _omitTN8,
    team_name_new: _omitTN9,
    timestamp: _omitTS,
    Timestamp: _omitTS2,
    ...fields
  } = rawBody;
  try {
    const auth = new google.auth.GoogleAuth({ keyFile: path, scopes: ['https://www.googleapis.com/auth/spreadsheets'] });
    const client = await auth.getClient();
    const sheets = google.sheets({ version: 'v4', auth: client });
    const rows = await fetchSheetWithRetry(sheets, `${sheetOrders}!A1:ZZ1000`);
    const headers = rows[0];
    // EXACT column names as in the sheet
    const exactTrim = (s) => (s ?? '').toString().trim();
    const teamNameCol = headers.findIndex(h => exactTrim(h) === 'TeamName');
    const timestampCol = headers.findIndex(h => exactTrim(h) === 'timestamp');
    if (teamNameCol < 0 || timestampCol < 0) {
      return res.status(400).json({ error: 'Required columns not found', required: ['TeamName','timestamp'], headers: headers.map(h=>exactTrim(h)) });
    }
    const eq = (a,b) => exactTrim(a) === exactTrim(b);
    const targetRowIndex = rows.findIndex((row, i) => i > 0 && eq(row[timestampCol], tsBody));
    if (targetRowIndex < 1) return res.status(404).json({ error: 'Team not found by timestamp', timestamp: tsBody });

    const updates = [];
    const headerIndexByKey = (k) => {
      const t = (s) => (s ?? '').toString().trim();
      // 1) exact
      let idx = headers.findIndex(h => t(h) === t(k));
      if (idx >= 0) return idx;
      // 2) case-insensitive
      idx = headers.findIndex(h => t(h).toLowerCase() === t(k).toLowerCase());
      if (idx >= 0) return idx;
      // 3) ignore spaces (to tolerate e.g. "Payment status" vs "Payment Status")
      idx = headers.findIndex(h => t(h).replace(/\s+/g,'').toLowerCase() === t(k).replace(/\s+/g,'').toLowerCase());
      return idx;
    };
    Object.entries(fields).forEach(([key, value]) => {
      const col = headerIndexByKey(key);
      if (col >= 0) {
        updates.push({
          range: `${sheetOrders}!${columnToLetter(col)}${targetRowIndex + 1}`,
          value
        });
      }
    });
    // if client requested rename, update TeamName column explicitly
    if (newTeamName && teamNameCol >= 0) {
      updates.push({
        range: `${sheetOrders}!${columnToLetter(teamNameCol)}${targetRowIndex + 1}`,
        value: newTeamName
      });
    }

    if (!updates.length) {
      return res.status(400).json({ error: 'No valid fields to update', providedKeys: Object.keys(fields), knownHeaders: headers.map(h => exactTrim(h)) });
    }

    await batchWriteValues({
      sheets, spreadsheetId,
      updates: updates.map(u => ({ range: u.range, values: [[u.value]] }))
    });

    res.status(200).json({ success: true, renamed: newTeamName ? { to: newTeamName } : null });
    cacheOrders = { data: null, ts: 0 };
  } catch (err) {
    console.error('Error in /updateTeam:', err);
    res.status(500).json({ error: 'Failed to update team' });
  }
}
['patch','post','put'].forEach(m => {
  app[m]('/updateTeam', updateTeamHandler);
  app[m]('/updateTeam/', updateTeamHandler);
});

// === POST /addTeam ===
app.post('/addTeam', async (req, res) => {
  try {
    const auth = new google.auth.GoogleAuth({ keyFile: path, scopes: ['https://www.googleapis.com/auth/spreadsheets'] });
    const client = await auth.getClient();
    const sheets = google.sheets({ version: 'v4', auth: client });
    const {
      timestamp = new Date().toLocaleString('en-GB', { timeZone: 'Asia/Tbilisi' }),
      Status1 = '', Status2 = '', PaymentStatus = '', anticipated_project_start_date = '',
      TeamName = '', Textarea = '', startDate = '', partner = '', Partner_confirmation = '',
      totalsumm = '', month = '', X1Q = '', XXX = '', industrymarket_expertise = '',
      Brief = '', Chat = '', Documents = '', nda = '', Link = '', Type = '', Type2 = '',
      spcv1 = '', spcv2 = '', spcv3 = '', spcv4 = '', spcv5 = '', spcv6 = '', spcv7 = '', spcv8 = '', spcv9 = '', spcv10 = ''
    } = req.body;
    const getVal = (key) => req.body[key] || '';
    const specialistFields = [];
    for (let i = 1; i <= 10; i++) {
      specialistFields.push(getVal(`sp${i}`), getVal(`hours${i}`), getVal(`quantity${i}`), getVal(`summ${i}`));
    }
    const row = [
      timestamp, Status1, Status2, PaymentStatus, anticipated_project_start_date, TeamName, Textarea, startDate,
      partner, Partner_confirmation, '', totalsumm, month, X1Q, XXX, industrymarket_expertise,
      ...specialistFields, Brief, Chat, Documents, nda, Link, Type, Type2,
      spcv1, spcv2, spcv3, spcv4, spcv5, spcv6, spcv7, spcv8, spcv9, spcv10
    ];
    await sheets.spreadsheets.values.append({
      spreadsheetId,
      range: 'DataBaseCollty_Teams!A1',
      valueInputOption: 'USER_ENTERED',
      insertDataOption: 'INSERT_ROWS',
      requestBody: { values: [row] },
    });
    res.status(200).json({ success: true });
    cacheOrders = { data: null, ts: 0 };
  } catch (err) {
    console.error('Error in /addTeam:', err);
    res.status(500).json({ error: 'Failed to append data' });
  }
});

// === Utility: column to letter ===
function columnToLetter(col) {
  let letter = '';
  while (col >= 0) {
    letter = String.fromCharCode((col % 26) + 65) + letter;
    col = Math.floor(col / 26) - 1;
  }
  return letter;
}
// === POST /tasks ===
app.post('/tasks', async (req, res) => {
  try {
    const {
      projectid = '',
      title = '',
      description = '',
      link = '',
      link2 = '',
      start = '',
      end = '',
      status = 'pending',
      priority = ''
    } = req.body;
    // Добавляем новые поля часов:
    const hrFields = [];
    for (let i = 1; i <= 10; i++) {
      hrFields.push(req.body[`hr${i}`] || '');
    }
    if (!projectid || !title || !start || !end) {
      return res.status(400).json({ error: 'Missing required fields' });
    }
    const auth = new google.auth.GoogleAuth({ keyFile: path, scopes: ['https://www.googleapis.com/auth/spreadsheets'] });
    const client = await auth.getClient();
    const sheets = google.sheets({ version: 'v4', auth: client });
    const sheetTasks = 'Database_Projectmanagement';

    // Новый task: timestamp | projectid | title | description | link | link2 | start | end | status | priority | hr1..hr10
    const timestamp = new Date().toISOString();
    const row = [
      timestamp, projectid, title, description, link, link2, start, end, status, priority,
      ...hrFields // hr1, hr2, ..., hr10
    ];

    await sheets.spreadsheets.values.append({
      spreadsheetId,
      range: `${sheetTasks}!A1`,
      valueInputOption: 'USER_ENTERED',
      insertDataOption: 'INSERT_ROWS',
      requestBody: { values: [row] },
    });

    res.status(200).json({ success: true, timestamp });
  } catch (err) {
    console.error('Error in POST /tasks:', err);
    res.status(500).json({ error: 'Failed to add task' });
  }
});

// === PATCH /tasks/:timestamp ===
app.patch('/tasks/:timestamp', async (req, res) => {
  const { timestamp } = req.params;
  const { projectid, ...fields } = req.body;
  if (!projectid || !timestamp) {
    return res.status(400).json({ error: 'Missing projectid or timestamp' });
  }
  try {
    const auth = new google.auth.GoogleAuth({ keyFile: path, scopes: ['https://www.googleapis.com/auth/spreadsheets'] });
    const client = await auth.getClient();
    const sheets = google.sheets({ version: 'v4', auth: client });
    const sheetTasks = 'Database_Projectmanagement';
    const rows = await fetchSheetWithRetry(sheets, `${sheetTasks}!A1:ZZ1000`);
    const headers = rows[0];
    const timestampCol = headers.findIndex(h => h.trim().toLowerCase() === 'timestamp');
    const projectidCol = headers.findIndex(h => h.trim().toLowerCase() === 'projectid');
    if (timestampCol < 0 || projectidCol < 0) return res.status(400).json({ error: 'No timestamp or projectid column' });
    const rowIndex = rows.findIndex((row, i) =>
      i > 0 &&
      (row[timestampCol] || '').trim() === timestamp.trim() &&
      (row[projectidCol] || '').trim() === projectid.trim()
    );
    if (rowIndex < 1) return res.status(404).json({ error: 'Task not found' });

    const updates = [];
    Object.entries(fields).forEach(([key, value]) => {
      const col = headers.findIndex(h => h.trim().toLowerCase() === key.toLowerCase());
      if (col >= 0) {
        updates.push({
          range: `${sheetTasks}!${columnToLetter(col)}${rowIndex + 1}`,
          value: value
        });
      }
    });
    await batchWriteValues({
      sheets, spreadsheetId,
      updates: updates.map(u => ({ range: u.range, values: [[u.value]] }))
    });
    res.status(200).json({ success: true });
  } catch (err) {
    console.error('Error in PATCH /tasks/:timestamp', err);
    res.status(500).json({ error: 'Failed to update task' });
  }
});

// === DELETE /tasks/:timestamp ===
app.delete('/tasks/:timestamp', async (req, res) => {
  const { timestamp } = req.params;
  const { projectid } = req.query;
  if (!projectid || !timestamp) {
    return res.status(400).json({ error: 'Missing projectid or timestamp' });
  }
  try {
    const auth = new google.auth.GoogleAuth({ keyFile: path, scopes: ['https://www.googleapis.com/auth/spreadsheets'] });
    const client = await auth.getClient();
    const sheets = google.sheets({ version: 'v4', auth: client });
    const sheetTasks = 'Database_Projectmanagement';
    const rows = await fetchSheetWithRetry(sheets, `${sheetTasks}!A1:ZZ1000`);
    const headers = rows[0];
    const timestampCol = headers.findIndex(h => h.trim().toLowerCase() === 'timestamp');
    const projectidCol = headers.findIndex(h => h.trim().toLowerCase() === 'projectid');
    if (timestampCol < 0 || projectidCol < 0) return res.status(400).json({ error: 'No timestamp or projectid column' });
    const rowIndex = rows.findIndex((row, i) =>
      i > 0 &&
      (row[timestampCol] || '').trim() === timestamp.trim() &&
      (row[projectidCol] || '').trim() === projectid.trim()
    );
    if (rowIndex < 1) return res.status(404).json({ error: 'Task not found' });

    // Получи sheetId для листа задач (можно найти в URL Google Sheets)
    const sheetId = 759220666; // <-- это твой sheetId для Database_Projectmanagement
    await sheets.spreadsheets.batchUpdate({
      spreadsheetId,
      requestBody: {
        requests: [
          {
            deleteDimension: {
              range: {
                sheetId: sheetId,
                dimension: 'ROWS',
                startIndex: rowIndex,
                endIndex: rowIndex + 1,
              }
            }
          }
        ]
      }
    });
    res.json({ success: true });
  } catch (err) {
    console.error('Error in DELETE /tasks/:timestamp', err);
    res.status(500).json({ error: 'Failed to delete task' });
  }
});

// === GET /tasks ===
app.get('/tasks', async (req, res) => {
  try {
    const { projectid = '', start = '', end = '' } = req.query;
    const auth = new google.auth.GoogleAuth({ keyFile: path, scopes: ['https://www.googleapis.com/auth/spreadsheets.readonly'] });
    const client = await auth.getClient();
    const sheets = google.sheets({ version: 'v4', auth: client });
    const sheetTasks = 'Database_Projectmanagement';
    const rows = await fetchSheetWithRetry(sheets, `${sheetTasks}!A1:ZZ1000`);
    if (!rows || rows.length === 0) return res.json([]);
    const headers = rows[0].map(h => h.trim());
    const data = rows.slice(1).map(row => headers.reduce((obj, key, i) => {
      obj[key] = row[i] || '';
      return obj;
    }, {}));
    // Фильтрация по projectid и датам
    const filtered = data.filter(row => {
      const pid = (row.projectid || row.projectId || '').toString();
      const s = new Date(row.start).getTime();
      const e = new Date(row.end).getTime();
      const startQ = start ? new Date(start).getTime() : null;
      const endQ = end ? new Date(end).getTime() : null;
      let dateOk = true;
      if (startQ !== null && s < startQ) dateOk = false;
      if (endQ !== null && e > endQ) dateOk = false;
      return (!projectid || pid === projectid) && dateOk;
    });
    res.json(filtered);
  } catch (err) {
    console.error('Error in GET /tasks:', err);
    res.status(500).json([]);
  }
});
// --- GET /leads/:id ---
app.get('/leads/:id', async (req, res) => {
  const { id } = req.params;
  try {
    const auth = new google.auth.GoogleAuth({ keyFile: path, scopes: ['https://www.googleapis.com/auth/spreadsheets.readonly'] });
    const client = await auth.getClient();
    const sheets = google.sheets({ version: 'v4', auth: client });

    const rows = await fetchSheetWithRetry(sheets, `${sheetLeads}!A1:ZZ1000`);
    const headers = rows[0].map(h => h.trim());

    const idCol = headers.findIndex(h => h.trim().toLowerCase() === 'projectid');
    if (idCol < 0) return res.status(400).json({ error: 'No projectid column' });

    const row = rows.find((row, i) => i > 0 && (row[idCol] || '').trim() === id.trim());
    if (!row) return res.status(404).json({ error: 'Row not found' });

    const result = headers.reduce((obj, key, i) => {
      obj[key] = row[i] || '';
      return obj;
    }, {});
    // --- Добавляем ManagerChat ---
    ['ClientChat', 'PartnerChat', 'ManagerChat'].forEach(field => {
      if (result[field]) {
        try {
          result[field] = JSON.parse(result[field]);
        } catch {
          result[field] = [];
        }
      } else {
        result[field] = [];
      }
    });
    res.json(result);
  } catch (err) {
    console.error('Error in GET /leads/:id', err);
    res.status(500).json({ error: 'Failed to load lead by id' });
  }
});

// --- PATCH /leads/:id ---
app.patch('/leads/:id', async (req, res) => {
  const { id } = req.params;
  try {
    const auth = new google.auth.GoogleAuth({ keyFile: path, scopes: ['https://www.googleapis.com/auth/spreadsheets'] });
    const client = await auth.getClient();
    const sheets = google.sheets({ version: 'v4', auth: client });
    const rows = await fetchSheetWithRetry(sheets, `${sheetLeads}!A1:ZZ1000`);
    const headers = rows[0].map(h => h.trim());

    const idCol = headers.findIndex(h => h.trim().toLowerCase() === 'projectid');
    if (idCol < 0) return res.status(400).json({ error: 'No projectid column' });
    const rowIndex = rows.findIndex((row, i) => i > 0 && (row[idCol] || '').trim() === id.trim());
    if (rowIndex < 1) return res.status(404).json({ error: 'Row not found' });

    const updates = [];
    Object.entries(req.body).forEach(([key, value]) => {
      const col = headers.findIndex(h => h.trim() === key);
      if (col >= 0) {
        if ((key === 'ClientChat' || key === 'PartnerChat' || key === 'ManagerChat') && typeof value !== 'string') {
          try {
            value = JSON.stringify(value);
          } catch {
            value = '';
          }
        }
        updates.push({
          range: `${sheetLeads}!${columnToLetter(col)}${rowIndex + 1}`,
          value: value
        });
      }
    });
    if (!updates.length) return res.status(400).json({ error: 'No valid fields to update' });

    await batchWriteValues({
      sheets, spreadsheetId,
      updates: updates.map(u => ({ range: u.range, values: [[u.value]] }))
    });
    res.status(200).json({ success: true });
  } catch (err) {
    console.error('Error in PATCH /leads/:id', err);
    res.status(500).json({ error: 'Failed to update lead by id' });
  }
});

// Utility для безопасного парсинга JSON:
function safeJsonParse(str) {
  try { return JSON.parse(str); } catch (e) { return []; }
}

// === POST /indexVectors ===
// One-shot (or periodic) indexing: pulls all orders from Sheets and stores embeddings in Qdrant
app.post('/indexVectors', async (req, res) => {
  try {
    if (!QDRANT_URL || !QDRANT_API_KEY || !JINA_API_KEY) {
      return res.status(500).json({ error: 'Vector env vars are not set' });
    }
    // load orders from Sheets
    const auth = new google.auth.GoogleAuth({ keyFile: path, scopes: ['https://www.googleapis.com/auth/spreadsheets.readonly'] });
    const client = await auth.getClient();
    const sheets = google.sheets({ version: 'v4', auth: client });
    const rows = await fetchSheetWithRetry(sheets, `${sheetOrders}!A1:ZZ1000`);
    const orders = rowsToOrders(rows);

    // ensure collection exists
    await ensureCollection();

    const BATCH = 64;
    let upserted = 0;
    for (let i = 0; i < orders.length; i += BATCH) {
      const slice = orders.slice(i, i + BATCH);
      const texts = slice.map(buildSearchText);

      // batch embed
      const resp = await fetch('https://api.jina.ai/v1/embeddings', {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${JINA_API_KEY}`,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({
          input: texts,
          model: 'jina-embeddings-v4',
          task: 'retrieval.passage',
          dimensions: 2048
        })
      });
      if (!resp.ok) {
        const t = await resp.text().catch(()=> '');
        throw new Error(`Jina batch error ${resp.status}: ${t}`);
      }
      const emb = await resp.json();
      const vectors = emb.data.map(d => d.embedding);

      const points = slice.map((o, idx) => ({
        id: randomUUID(),
        vector: vectors[idx],
        payload: o
      }));

      await upsertPoints(points);
      upserted += points.length;
    }

    res.json({ ok: true, upserted });
  } catch (e) {
    console.error('indexVectors error:', e);
    res.status(500).json({ error: String(e.message || e) });
  }
});

// === POST /search ===
// Body: { q: string, limit?: number }
app.post('/search', async (req, res) => {
  try {
    const q = String(req.body.q || '').trim();
    const limit = Math.min(Number(req.body.limit || 50), 100);
    if (!q) return res.status(400).json({ error: 'empty query' });

    await ensureCollection();
    const vec = await embedText(q);
    const hits = await vectorSearch(vec, limit);

    // Map results and keep semantic score
    let items = (hits || [])
      .map(h => ({ ...(h.payload || {}), __score: (typeof h.score === 'number' ? h.score : 0) }))
      .filter(obj => Object.keys(obj).length > 0);

    // --- Intent-aware re-rank: prefer items whose Type/Type2/Tags contain query keywords ---
    const qn = String(q).toLowerCase();
    // Normalize common separators and variants
    const qTokens = qn
      .replace(/\//g, ' ') // CI/CD -> CI CD
      .replace(/[-_]+/g, ' ')
      .split(/[^a-z0-9]+/)
      .filter(Boolean);
    const qSet = new Set(qTokens);

    // Precompute preferred keywords for known intents (kept tiny & safe)
    const prefers = [];
    if (qSet.has('ci') || qn.includes('ci/cd') || qn.includes('ci cd') || qn.includes('cicd') || qn.includes('pipeline')) {
      prefers.push('ci', 'cicd', 'ci/cd', 'pipeline', 'monorepo', 'github actions', 'gitlab', 'jenkins', 'circleci');
    }
    if (qSet.has('pr') || qn.includes('public relations')) {
      prefers.push('pr', 'public relations', 'media relations');
    }
    if (qSet.has('marketing') || qn.includes('strategy')) {
      prefers.push('marketing', 'digital strategy', 'content', 'seo', 'brand strategy', 'go-to-market');
    }

    if (prefers.length) {
      const prefsLc = prefers.map(s => s.toLowerCase());
      items = items.map(it => {
        const t1 = String(it.Type || '').toLowerCase();
        const t2 = String(it.Type2 || '').toLowerCase();
        const tg = String(it.Textarea || '').toLowerCase();
        const hasPref = prefsLc.some(p => t1.includes(p) || t2.includes(p) || tg.includes(p));
        // gentle boost (not override semantics entirely)
        const boost = hasPref ? 0.15 : 0;
        return { ...it, __score: (it.__score || 0) + boost };
      });
    }

    items.sort((a,b) => (b.__score || 0) - (a.__score || 0));
    res.json(items);
  } catch (e) {
    console.error('search error:', e);
    res.status(500).json({ error: 'search failed' });
  }
});

app.use((req, res) => {
  console.warn('[404]', req.method, req.originalUrl);
  res.status(404).send('Not Found');
});

app.listen(port, () => {
  // console.log(`🚀 Server running on port ${port}`);
});