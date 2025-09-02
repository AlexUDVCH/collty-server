// === server.js (Final version with Caching & 429/503 exponential backoff) ===
const express = require('express');
const { google } = require('googleapis');
const cors = require('cors');
const { Agent } = require('undici'); // keep-alive agent for fetch

require('dotenv').config();
const { randomUUID, createHash } = require('crypto');
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
// --- Helper: detect if vectors are enabled (all env vars set) ---
function vectorsEnabled() {
  return Boolean(QDRANT_URL && QDRANT_API_KEY && JINA_API_KEY);
}

const app = express();
// Keep-Alive agent for all outgoing HTTP(S) requests
const KEEP_ALIVE_AGENT = new Agent({ connections: 16, pipelining: 0 });
// Disable etag/304 and force no-store to avoid empty 304 bodies confusing the client
app.set('etag', false);
app.use((req, res, next) => {
  res.set('Cache-Control', 'no-store');
  res.set('Pragma', 'no-cache');
  res.set('Expires', '0');
  next();
});
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
    }),
    dispatcher: KEEP_ALIVE_AGENT
  });
  if (!resp.ok) {
    const t = await resp.text().catch(()=> '');
    throw new Error(`Jina error ${resp.status}: ${t}`);
  }
  const json = await resp.json();
  return json.data[0].embedding;
}

// Hedged-request version: launches a second parallel call to Jina if the first hasn't answered
// within hedgeDelayMs. Whichever returns first wins; the other is aborted.
async function embedTextHedged(text, hedgeDelayMs = 900) {
  if (!JINA_API_KEY) throw new Error('JINA_API_KEY is missing');
  const url = 'https://api.jina.ai/v1/embeddings';
  const body = JSON.stringify({
    input: [String(text || '')],
    model: 'jina-embeddings-v4',
    task: 'retrieval.query',
    dimensions: 2048
  });

  const makeOnce = () => {
    const ctrl = new AbortController();
    const p = fetch(url, {
      method: 'POST',
      headers: { 'Authorization': `Bearer ${JINA_API_KEY}`, 'Content-Type': 'application/json' },
      body,
      signal: ctrl.signal,
      dispatcher: KEEP_ALIVE_AGENT
    }).then(async (r) => {
      if (!r.ok) {
        const t = await r.text().catch(()=> '');
        throw new Error(`Jina ${r.status}: ${t}`);
      }
      const j = await r.json();
      return j.data[0].embedding;
    });
    return { p, abort: () => ctrl.abort() };
  };

  const A = makeOnce();
  let B = null;
  let startedB = false;
  const startBTimer = setTimeout(() => { B = makeOnce(); startedB = true; }, hedgeDelayMs);

  try {
    const winner = await Promise.race([
      A.p,
      (async () => {
        // wait until B is potentially started
        await new Promise(r => setTimeout(r, hedgeDelayMs + 1));
        return startedB ? B.p : new Promise(()=>{});
      })()
    ]);
    // cancel losers
    if (startedB && B) B.abort();
    A.abort();
    return winner;
  } catch (e) {
    clearTimeout(startBTimer);
    // if A failed quickly and B hasn't started yet, try a fresh single attempt
    if (!startedB) {
      const F = makeOnce();
      try { const v = await F.p; F.abort(); return v; } catch (e2) { F.abort(); throw e2; }
    }
    throw e;
  } finally {
    clearTimeout(startBTimer);
  }
}

// ------------- Qdrant REST helpers -------------
async function qdrantFetch(path, init = {}) {
  if (!QDRANT_URL || !QDRANT_API_KEY) throw new Error('QDRANT_URL/QDRANT_API_KEY missing');
  const url = `${QDRANT_URL}${path}`;
  const headers = Object.assign({
    'Authorization': `Bearer ${QDRANT_API_KEY}`,
    'Content-Type': 'application/json'
  }, init.headers || {});
  const resp = await fetch(url, { ...init, headers, dispatcher: KEEP_ALIVE_AGENT });
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

// --- Exact CSV tag matching helpers (for /orders strict tag search) ---
const _normExact = s => String(s || '').toLowerCase().replace(/\s+/g, ' ').trim();
const _splitCSVExact = s => String(s || '').split(',').map(t => t.trim()).filter(Boolean);
const _acronymExact = s => String(s || '')
  .split(/[^a-z0-9]+/i).filter(Boolean).map(w => w[0]).join('').toUpperCase();

function csvHasTagExact(csv, wanted) {
  const w = _normExact(wanted);
  return _splitCSVExact(csv).some(tag => _normExact(tag) === w);
}
function csvHasAcronym(csv, wanted) {
  const W = String(wanted || '').toUpperCase();
  if (!W) return false;
  return _splitCSVExact(csv).some(tag => _acronymExact(tag) === W);
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

  // Take raw query strings (do NOT pre-lowercase or split by arbitrary chars).
  const typeRaw  = String(req.query.type  || '').trim();
  const type2Raw = String(req.query.type2 || '').trim();

  const confirmed = req.query.confirmed === 'true';

  // Support multiple tags in `type` query via CSV: ?type=SEO,PR
  const qTypes = _splitCSVExact(typeRaw);   // ["SEO","PR"] etc.

  const filtered = data.filter(row => {
    const email  = String(row.Email || '').toLowerCase();
    const type   = String(row.Type  || '');
    const type2  = String(row.Type2 || '');
    const text   = String(row.Textarea || '').toLowerCase();

    const matchEmail = emailQuery ? email.includes(emailQuery) : true;

    // For `type`: strict CSV tag equality on Type OR (optionally) acronym equality
    const matchType = qTypes.length
      ? qTypes.some(qt =>
          csvHasTagExact(type, qt)  ||
          csvHasAcronym(type, qt)   ||
          csvHasTagExact(type2, qt) ||   // keep backward-compatible behavior (Type or Type2)
          csvHasAcronym(type2, qt)
        )
      : true;

    // For `type2`: strict CSV tag equality only against Type2 (plus acronym)
    const matchType2 = type2Raw
      ? (csvHasTagExact(type2, type2Raw) || csvHasAcronym(type2, type2Raw))
      : true;

    const matchConfirmed = confirmed ? text.includes('confirmed') : true;

    return matchEmail && matchType && matchType2 && matchConfirmed;
  });
  const deduped = dedupeByTeamName(filtered);
  res.json(deduped);
}

// === GET /ordersPaged (same filters as /orders; dedup + page-cursor pagination) ===
app.get('/ordersPaged', async (req, res) => {
  try {
    // Load rows with cache (same as /orders)
    const getRows = async () => {
      if (cacheOrders.data && now() - cacheOrders.ts < CACHE_TTL) return cacheOrders.data;
      const auth = new google.auth.GoogleAuth({ keyFile: path, scopes: ['https://www.googleapis.com/auth/spreadsheets.readonly'] });
      const client = await auth.getClient();
      const sheets = google.sheets({ version: 'v4', auth: client });
      const rows = await fetchSheetWithRetry(sheets, `${sheetOrders}!A1:ZZ1000`);
      cacheOrders = { data: rows, ts: now() };
      return rows;
    };

    // Page-based cursor (like /searchPaged)
    const rawLimit = Number(req.query.limit || req.query.page_size || 50);
    const PAGE_SIZE = Math.min(Math.max(rawLimit || 50, 1), 50); // hard-cap 50

    let cursorObj = null;
    if (typeof req.query.cursor === 'string' && req.query.cursor) {
      try { cursorObj = JSON.parse(req.query.cursor); } catch(_) { cursorObj = null; }
    }
    const page = Math.max(1, Number(cursorObj?.page || 1));

    const rows = await getRows();
    if (!rows || rows.length === 0) return res.json({ items: [], next_cursor: null, total_estimate: 0 });

    // --- Same filtering logic as respondFilteredOrders ---
    const headers = rows[0].map(h => h.trim());
    const data = rows.slice(1).map(row => headers.reduce((obj, key, i) => { obj[key] = row[i] || ''; return obj; }, {}));

    const emailQuery = (req.query.email || '').toLowerCase().trim();
    const typeRaw  = String(req.query.type  || '').trim();
    const type2Raw = String(req.query.type2 || '').trim();
    const confirmed = req.query.confirmed === 'true';
    const qTypes = _splitCSVExact(typeRaw);

    const filtered = data.filter(row => {
      const email  = String(row.Email || '').toLowerCase();
      const type   = String(row.Type  || '');
      const type2  = String(row.Type2 || '');
      const text   = String(row.Textarea || '').toLowerCase();
      const matchEmail = emailQuery ? email.includes(emailQuery) : true;
      const matchType = qTypes.length
        ? qTypes.some(qt =>
            csvHasTagExact(type, qt)  || csvHasAcronym(type, qt) ||
            csvHasTagExact(type2, qt) || csvHasAcronym(type2, qt)
          )
        : true;
      const matchType2 = type2Raw ? (csvHasTagExact(type2, type2Raw) || csvHasAcronym(type2, type2Raw)) : true;
      const matchConfirmed = confirmed ? text.includes('confirmed') : true;
      return matchEmail && matchType && matchType2 && matchConfirmed;
    });

    // Server-side de-dup by TeamName
    let items = dedupeByTeamName(filtered);

    // Deterministic ordering to keep page sequence stable across requests
    items.sort((a, b) => (stableIdForOrder(a) > stableIdForOrder(b) ? 1 : -1));

    const total = items.length;
    const start = (page - 1) * PAGE_SIZE;
    const end = Math.min(page * PAGE_SIZE, total);
    const slice = start < end ? items.slice(start, end) : [];
    const hasMore = end < total;
    const next_cursor = hasMore ? JSON.stringify({ page: page + 1 }) : null;

    return res.json({ items: slice, next_cursor, total_estimate: total });
  } catch (err) {
    console.error('Error in /ordersPaged:', err);
    res.status(200).json({ items: [], next_cursor: null, total_estimate: 0 });
  }
});

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

function uniq(arr){ return Array.from(new Set(arr)); }

function dedupeByTeamName(items){
  const seen = new Set();
  const out = [];
  for (const it of items){
    const key = String(it.TeamName || '').trim().toLowerCase();
    if (!key) { out.push(it); continue; } // keep items without TeamName untouched
    if (seen.has(key)) continue;          // skip duplicates
    seen.add(key);
    out.push(it);
  }
  return out;
}

// Deduplicate by normalized TeamName (case-insensitive, trimmed); keep highest __score; fallback to stable key if TeamName missing
function dedupeByTeamNameScore(items){
  const out = new Map();
  for (const it of items){
    const keyTN = String(it.TeamName || '').trim().toLowerCase();
    const key = keyTN || stableKeyFromOrder(it);
    const prev = out.get(key);
    if (!prev || (it.__score || 0) > (prev.__score || 0)) out.set(key, it);
  }
  return Array.from(out.values());
}

// --- Stable identity helpers to avoid duplicate vector points and to deduplicate search hits ---
function stableKeyFromOrder(o) {
  const S = v => String(v || '').trim().toLowerCase();
  // Use a combination that is stable in your sheet
  return [S(o.timestamp), S(o.TeamName), S(o.Type), S(o.Type2), S(o.partner)].join('|');
}
function stableIdForOrder(o) {
  // 32-hex id derived from the stable key
  const key = stableKeyFromOrder(o);
  return createHash('sha256').update(key).digest('hex').slice(0, 32);
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
    // Be lenient on first-load: don't fail the UI because keywords are not critical
    res.status(200).json({ type: [], type2: [] });
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

// --- Exact-phrase helpers (to prioritize exact matches in TeamName / Type / Type2 / Textarea) ---
function escapeRegExp(s){ return String(s || '').replace(/[.*+?^${}()|[\]\\]/g, '\\$&'); }

// Extract meaningful multi-word phrases directly from the raw query (without over-normalizing).
// Example: "i need business strategy" -> ["business strategy"]
function extractExactPhrases(rawQ){
  const raw = String(rawQ || '').toLowerCase();
  // Replace separators with a single space, keep words
  const tokens = raw.split(/[^a-z0-9]+/).filter(Boolean);
  // Remove trivial words to avoid phrases like "i need"
  const stop = new Set(['i','me','my','we','our','need','want','a','an','the','team','for','to','please','looking','search','find','build','hire']);
  const filtered = tokens.filter(t => !stop.has(t));
  const phrases = new Set();

  // Collect contiguous bigrams and trigrams as candidate phrases
  for (let i = 0; i < filtered.length; i++){
    const one = filtered[i];
    const two = filtered[i+1];
    const three = filtered[i+2];

    if (two) phrases.add(`${one} ${two}`);
    if (two && three) phrases.add(`${one} ${two} ${three}`);
  }

  // Also include any quoted substrings from the raw query as-is
  const quoted = raw.match(/"([^"]+)"/g) || [];
  quoted.forEach(q => {
    const inner = q.slice(1, -1).trim();
    if (inner.split(/\s+/).length >= 2) phrases.add(inner.toLowerCase());
  });

  // Return only phrases of at least 2 words
  return Array.from(phrases).filter(p => p.split(/\s+/).length >= 2);
}

// Compute exact-phrase boost based on presence in Type/Type2 (as CSV tags) and in TeamName / Textarea.
function phraseBoostForItem(item, phrases){
  if (!phrases || !phrases.length) return 0;
  const team = String(item.TeamName || '').toLowerCase();
  const tagsCsv = String(item.Textarea || '');
  const typeCsv = String(item.Type || '');
  const type2Csv = String(item.Type2 || '');
  let boost = 0;

  for (const p of phrases){
    const pLc = String(p).toLowerCase();
    const re = new RegExp(`\\b${escapeRegExp(pLc)}\\b`, 'i');

    // Strongest: exact CSV tag match in Type / Type2
    if (csvHasTagExact(typeCsv, pLc) || csvHasTagExact(type2Csv, pLc)) boost += 0.35;

    // Medium: exact phrase in TeamName
    if (re.test(team)) boost += 0.20;

    // Light: exact phrase in free-form tags/keywords (Textarea)
    if (re.test(String(tagsCsv).toLowerCase())) boost += 0.10;
  }

  // Cap total phrase-derived boost to keep ranking stable
  return Math.min(boost, 0.60);
}
// Utility для безопасного парсинга JSON:
function safeJsonParse(str) {
  try { return JSON.parse(str); } catch (e) { return []; }
}

const STOPWORDS = new Set(['i','me','my','we','our','need','want','a','an','the','team','for','to','please','looking','search','find','build','hire']);
function normalizeQuery(q) {
  const base = String(q || '').toLowerCase()
    .replace(/\bci\/cd\b/g, 'ci cd')   // unify CI/CD
    .replace(/\bcicd\b/g, 'ci cd')     // unify cicd
    .replace(/[-_]+/g, ' ');
  const toks = base.split(/[^a-z0-9]+/).filter(Boolean);
  const filtered = toks.filter(t => !STOPWORDS.has(t));
  return filtered.join(' ').trim() || base.trim() || String(q || '').trim();
}
function _tokens(s){ return String(s||'').toLowerCase().split(/[^a-z0-9]+/).filter(Boolean); }
function _csvParts(s){ return String(s||'').split(',').map(x=>x.trim()).filter(Boolean); }
function _acronym(s){
  return String(s||'').split(/[^a-z0-9]+/i).filter(Boolean).map(w=>w[0]).join('').toUpperCase();
}
function keywordFeatures(order, qCoreTokens) {
  const typeParts  = _csvParts(order.Type);
  const type2Parts = _csvParts(order.Type2);
  const allText = [
    order.TeamName, order.Type, order.Type2, order.Textarea,
    order.X1Q, order.industrymarket_expertise,
    order.spcv1,order.spcv2,order.spcv3,order.spcv4,order.spcv5,
    order.spcv6,order.spcv7,order.spcv8,order.spcv9,order.spcv10
  ].map(x=>String(x||'').toLowerCase()).join(' ');
  const qSet = new Set(qCoreTokens);
  const textTokens = new Set(_tokens(allText));
  // direct hits in Type / Type2
  const typeHit  = typeParts.some(t => qSet.has(String(t||'').toLowerCase()));
  const type2Hit = type2Parts.some(t => qSet.has(String(t||'').toLowerCase()));
  // text overlap count
  let overlap = 0;
  qSet.forEach(t => { if (textTokens.has(t)) overlap++; });
  // acronym match (PR/SMM/CI/CD)
  const acrQ = _acronym([...qSet].join(' '));
  const hasAcr = acrQ && (typeParts.concat(type2Parts).some(t => _acronym(t) === acrQ));
  return { typeHit, type2Hit, overlap, hasAcr };
}

// Anchor intent helpers (to strictly prefer canonical tags when user asks for them)
function hasTag(order, term){
  const termLc = String(term || '').toLowerCase();
  const set = new Set(_csvParts(order.Type).concat(_csvParts(order.Type2)).map(s => String(s||'').trim().toLowerCase()));
  return set.has(termLc);
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
        id: stableIdForOrder(o),
        vector: vectors[idx],
        payload: o
      }));

      // Remove any legacy points for these TeamNames (from prior runs with non-deterministic IDs), then upsert
      try { await deletePointsByTeamNames(slice.map(o => o.TeamName)); } catch (_) {}
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
// === POST /search ===
app.post('/search', async (req, res) => {
  try {
    const rawQ = String(req.body.q || '').trim();
    const q = normalizeQuery(rawQ);
    const exactPhrases = extractExactPhrases(rawQ);
    const limit = Math.min(Number(req.body.limit || 50), 100);
    // Be lenient on first-load / empty submissions: return empty list instead of 400
    if (!q) return res.status(200).json([]);
    if (!vectorsEnabled()) {
      console.warn('[search] vectors are disabled (missing env) — returning empty result');
      return res.status(200).json([]);
    }

    await ensureCollection();
    const vec = await embedTextHedged(q);
    const hits = await vectorSearch(vec, limit);

    // Map results and keep semantic score
    let items = (hits || [])
      .map(h => ({ ...(h.payload || {}), __score: (typeof h.score === 'number' ? h.score : 0) }))
      .filter(obj => Object.keys(obj).length > 0);

    // Deduplicate by TeamName (keep highest score); fallback to stable key if TeamName missing
    items = dedupeByTeamNameScore(items);

    // --- Hybrid re-rank (VSS cosine + lightweight keyword/field boosts) ---
    {
      const qn = String(q).toLowerCase();
      const qTokens = qn.split(/[^a-z0-9]+/).filter(Boolean);
      for (const it of items) {
        const feat = keywordFeatures(it, qTokens);
        let final = (typeof it.__score === 'number' ? it.__score : 0);

        // Strong preference for exact tag hits
        if (feat.typeHit)  final += 0.30;
        if (feat.type2Hit) final += 0.15;

        // Small bonus for textual overlap in long fields (up to 3 tokens)
        final += Math.min(feat.overlap, 3) * 0.06;

        // Acronym-friendly nudge (PR, SMM, CICD...)
        if (feat.hasAcr) final += 0.08;

        // Guardrail: penalize items with zero overlap and no tag hits
        if (!feat.typeHit && !feat.type2Hit && feat.overlap === 0) final -= 0.10;

        // Social-proof micro-boost
        if (String(it.Partner_confirmation||'').trim()) final += 0.03;

        // --- Anchor-intent: if the query clearly asks for SEO/PR/CI/CD, punish items without that tag in Type/Type2 ---
        const wantSEO  = qTokens.includes('seo');
        const wantPR   = qTokens.includes('pr') || qn.includes('public relations');
        const wantCICD = qn.includes('ci/cd') || qn.includes('ci cd') || qn.includes('cicd') ||
                         (qTokens.includes('ci') && qTokens.includes('cd'));

        if (wantSEO) {
          const seoHit = hasTag(it,'seo') || hasTag(it,'technical seo') || hasTag(it,'on-page seo') ||
                         hasTag(it,'link building') || hasTag(it,'content seo');
          if (seoHit) final += 0.12; else final -= 0.22;
        }
        if (wantPR) {
          const prHit = hasTag(it,'pr') || hasTag(it,'public relations') || hasTag(it,'media relations');
          if (prHit) final += 0.10; else final -= 0.18;
        }
        if (wantCICD) {
          const cicdHit = hasTag(it,'ci/cd') || hasTag(it,'ci cd') || hasTag(it,'cicd') || hasTag(it,'ci') || hasTag(it,'cd');
          if (cicdHit) final += 0.10; else final -= 0.18;
        }

        // Exact-phrase priority (e.g., "business strategy")
        final += phraseBoostForItem(it, exactPhrases);

        it.__score = final;
      }
    }

    // --- Intent-aware re-rank: prefer items whose Type/Type2/Tags contain query keywords ---
    const qn = String(q).toLowerCase();
    const qTokens = qn.split(/[^a-z0-9]+/).filter(Boolean);
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

// === GET /search ===
app.get('/search', async (req, res) => {
  try {
    const rawQ = String(req.query.q || '').trim();
    const q = normalizeQuery(rawQ);
    const exactPhrases = extractExactPhrases(rawQ);
    const limit = Math.min(Number(req.query.limit || 50), 100);
    if (!q) return res.status(200).json([]);
    if (!vectorsEnabled()) {
      console.warn('[search:GET] vectors are disabled (missing env) — returning empty result');
      return res.status(200).json([]);
    }

    await ensureCollection();
    const vec = await embedTextHedged(q);
    const hits = await vectorSearch(vec, limit);

    let items = (hits || [])
      .map(h => ({ ...(h.payload || {}), __score: (typeof h.score === 'number' ? h.score : 0) }))
      .filter(obj => Object.keys(obj).length > 0);

    // Deduplicate and re-rank — same as POST /search
    items = dedupeByTeamNameScore(items);

    {
      const qn = String(q).toLowerCase();
      const qTokens = qn.split(/[^a-z0-9]+/).filter(Boolean);
      for (const it of items) {
        const feat = keywordFeatures(it, qTokens);
        let final = (typeof it.__score === 'number' ? it.__score : 0);
        if (feat.typeHit)  final += 0.30;
        if (feat.type2Hit) final += 0.15;
        final += Math.min(feat.overlap, 3) * 0.06;
        if (feat.hasAcr) final += 0.08;
        if (!feat.typeHit && !feat.type2Hit && feat.overlap === 0) final -= 0.10;
        if (String(it.Partner_confirmation||'').trim()) final += 0.03;

        const wantSEO  = qTokens.includes('seo') || qn.includes('technical seo');
        const wantPR   = qTokens.includes('pr') || qn.includes('public relations');
        const wantCICD = qn.includes('ci/cd') || qn.includes('ci cd') || qn.includes('cicd') ||
                         (qTokens.includes('ci') && qTokens.includes('cd'));
        if (wantSEO) {
          const seoHit = hasTag(it,'seo') || hasTag(it,'technical seo') || hasTag(it,'on-page seo') ||
                         hasTag(it,'link building') || hasTag(it,'content seo');
          if (seoHit) final += 0.12; else final -= 0.22;
        }
        if (wantPR) {
          const prHit = hasTag(it,'pr') || hasTag(it,'public relations') || hasTag(it,'media relations');
          if (prHit) final += 0.10; else final -= 0.18;
        }
        if (wantCICD) {
          const cicdHit = hasTag(it,'ci/cd') || hasTag(it,'ci cd') || hasTag(it,'cicd') || hasTag(it,'ci') || hasTag(it,'cd');
          if (cicdHit) final += 0.10; else final -= 0.18;
        }
        // Exact-phrase priority
        final += phraseBoostForItem(it, exactPhrases);
        it.__score = final;
      }
    }

    {
      const qn = String(q).toLowerCase();
      const qTokens = qn.split(/[^a-z0-9]+/).filter(Boolean);
      const qSet = new Set(qTokens);
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
          const boost = hasPref ? 0.15 : 0;
          return { ...it, __score: (it.__score || 0) + boost };
        });
      }
    }

    items.sort((a,b) => (b.__score || 0) - (a.__score || 0));
    res.json(items);
  } catch (e) {
    console.error('search (GET) error:', e);
    res.status(500).json({ error: 'search failed' });
  }
});

// === POST /searchPaged ===
// Body: { q: string, limit?: number (<=50), cursor?: string(JSON) }
app.post('/searchPaged', async (req, res) => {
  try {
    const rawQ = String(req.body.q || '').trim();
    const q = normalizeQuery(rawQ);
    const exactPhrases = extractExactPhrases(rawQ);
    const pageSizeReq = Number(req.body.limit || req.body.page_size || 50);
    const PAGE_SIZE = Math.min(Math.max(pageSizeReq || 50, 1), 50); // hard-cap 50

    let cursorObj = null;
    if (typeof req.body.cursor === 'string' && req.body.cursor) {
      try { cursorObj = JSON.parse(req.body.cursor); } catch(_) { cursorObj = null; }
    }
    const page = Math.max(1, Number(cursorObj?.page || 1));

    // Be lenient on first-load / empty submissions
    if (!q) return res.status(200).json({ items: [], next_cursor: null, total_estimate: 0 });
    if (!vectorsEnabled()) {
      console.warn('[searchPaged] vectors are disabled (missing env) — returning empty page');
      return res.status(200).json({ items: [], next_cursor: null, total_estimate: 0 });
    }

    await ensureCollection();
    const vec = await embedTextHedged(q);

    // Candidate pool grows with page to keep global order stable after re-rank
    const candidatesK = Math.min(1000, page * PAGE_SIZE * 2);
    const hits = await vectorSearch(vec, candidatesK);

    // Map & dedupe by TeamName (keep highest score); fallback to stable key if TeamName missing
    let items = (hits || [])
      .map(h => ({ ...(h.payload || {}), __score: (typeof h.score === 'number' ? h.score : 0) }))
      .filter(obj => Object.keys(obj).length > 0);
    items = dedupeByTeamNameScore(items);

    // ---- Hybrid re-rank (same logic as /search) ----
    {
      const qn = String(q).toLowerCase();
      const qTokens = qn.split(/[^a-z0-9]+/).filter(Boolean);
      for (const it of items) {
        const feat = keywordFeatures(it, qTokens);
        let final = (typeof it.__score === 'number' ? it.__score : 0);
        if (feat.typeHit)  final += 0.30;
        if (feat.type2Hit) final += 0.15;
        final += Math.min(feat.overlap, 3) * 0.06;
        if (feat.hasAcr) final += 0.08;
        if (!feat.typeHit && !feat.type2Hit && feat.overlap === 0) final -= 0.10;
        if (String(it.Partner_confirmation||'').trim()) final += 0.03;

        const wantSEO  = qTokens.includes('seo') || qn.includes('technical seo');
        const wantPR   = qTokens.includes('pr') || qn.includes('public relations');
        const wantCICD = qn.includes('ci/cd') || qn.includes('ci cd') || qn.includes('cicd') ||
                         (qTokens.includes('ci') && qTokens.includes('cd'));
        if (wantSEO) {
          const seoHit = hasTag(it,'seo') || hasTag(it,'technical seo') || hasTag(it,'on-page seo') ||
                         hasTag(it,'link building') || hasTag(it,'content seo');
          if (seoHit) final += 0.12; else final -= 0.22;
        }
        if (wantPR) {
          const prHit = hasTag(it,'pr') || hasTag(it,'public relations') || hasTag(it,'media relations');
          if (prHit) final += 0.10; else final -= 0.18;
        }
        if (wantCICD) {
          const cicdHit = hasTag(it,'ci/cd') || hasTag(it,'ci cd') || hasTag(it,'cicd') || hasTag(it,'ci') || hasTag(it,'cd');
          if (cicdHit) final += 0.10; else final -= 0.18;
        }
        // Exact-phrase priority
        final += phraseBoostForItem(it, exactPhrases);
        it.__score = final;
      }
    }

    // Intent-aware gentle preference (same as /search)
    {
      const qn = String(q).toLowerCase();
      const qTokens = qn.split(/[^a-z0-9]+/).filter(Boolean);
      const qSet = new Set(qTokens);
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
          const boost = hasPref ? 0.15 : 0;
          return { ...it, __score: (it.__score || 0) + boost };
        });
      }
    }

    // Stable sort with tie-breaker
    items.sort((a,b) => (b.__score || 0) - (a.__score || 0) || (stableIdForOrder(a) > stableIdForOrder(b) ? 1 : -1));

    const total = items.length;
    const start = (page - 1) * PAGE_SIZE;
    const end = Math.min(page * PAGE_SIZE, total);
    const slice = start < end ? items.slice(start, end) : [];
    const hasMore = end < total;
    const next_cursor = hasMore ? JSON.stringify({ page: page + 1 }) : null;

    return res.json({ items: slice, next_cursor, total_estimate: total });
  } catch (e) {
    console.error('searchPaged error:', e);
    res.status(500).json({ error: 'searchPaged failed' });
  }
});

// --- Optional: warmup endpoint to mitigate cold start of Qdrant/Jina ---
app.get('/warmup', async (req, res) => {
  try {
    if (vectorsEnabled()) {
      await ensureCollection();
      try { await embedTextHedged('ping', 300); } catch (_) {}
    }
    res.json({ ok: true });
  } catch (_) {
    res.json({ ok: true }); // never fail warmup
  }
});

app.use((req, res) => {
  console.warn('[404]', req.method, req.originalUrl);
  res.status(404).send('Not Found');
});

// --- Safe warmup wrapper for vector embedding warmup ---
async function safeWarmup() {
  try {
    await embedTextHedged("ping", 300);
  } catch (e) {
    console.warn("[warmup] ignored:", e.message);
  }
}

app.listen(port, () => {
  // console.log(`🚀 Server running on port ${port}`);
  if (vectorsEnabled()) {
    setTimeout(safeWarmup, 1500);
    setInterval(safeWarmup, 5 * 60 * 1000);
  }
});