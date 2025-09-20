// === server.js (Final version with Caching & 429/503 exponential backoff) ===
const express = require('express');
const { google } = require('googleapis');
const cors = require('cors');

require('dotenv').config();
const { randomUUID, createHash } = require('crypto');
// --- Vectors / Qdrant + Jina embeddings setup ---
const QDRANT_URL = process.env.QDRANT_URL;
const QDRANT_API_KEY = process.env.QDRANT_API_KEY;
const JINA_API_KEY = process.env.JINA_API_KEY;

// Embedding/model config (override via ENV without code changes)
const JINA_MODEL = process.env.JINA_MODEL || 'jina-embeddings-v4';
const EMB_DIM = Number(process.env.EMB_DIM || 2048);
// Keep VECTOR_SIZE synced with EMB_DIM
const VECTOR_SIZE = EMB_DIM;
const COLLECTION = 'orders';
// Runtime-overridable values (can be changed via /embeddingConfig)
let CURRENT_MODEL = JINA_MODEL;
let CURRENT_DIM = EMB_DIM;

if (!QDRANT_URL || !QDRANT_API_KEY || !JINA_API_KEY) {
  console.warn('[vectors] Missing env vars: QDRANT_URL / QDRANT_API_KEY / JINA_API_KEY');
}
// --- Helper: detect if vectors are enabled (all env vars set) ---
function vectorsEnabled() {
  return Boolean(QDRANT_URL && QDRANT_API_KEY && JINA_API_KEY);
}

const app = express();
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

// === SITEMAP (no line breaks inside <loc>) ===
function buildSitemapXml(urls) {
  const header = '<?xml version="1.0" encoding="UTF-8"?>\n';
  const open = '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n';
  const body = urls
    .filter(Boolean)
    .map(u => `  <url><loc>${String(u).trim()}</loc></url>`) // single-line <loc>
    .join('\n');
  const close = '\n</urlset>';
  return header + open + body + close;
}

app.get('/sitemap.xml', async (req, res) => {
  try {
    // Static base URLs
    const staticUrls = [
      'https://collty.com',
      'https://collty.com/about',
      'https://collty.com/partnership',
      'https://collty.com/tpost/vflg5kmre1-how-remote-teams-are-driving-business-gr',
      'https://collty.com/tpost/3y88bbbl31-remote-teams-with-ai-not-only-cost-reduc',
      'https://collty.com/tpost/nol9jhrsz1-common-outsourced-business-processes',
      'https://collty.com/tpost/fl45g5r4a1-collty-starts-testing-phase-for-services',
    ];

    // Fetch dynamic team URLs from Google Sheets
    const auth = new google.auth.GoogleAuth({ keyFile: path, scopes: ['https://www.googleapis.com/auth/spreadsheets.readonly'] });
    const client = await auth.getClient();
    const sheets = google.sheets({ version: 'v4', auth: client });
    const rows = await fetchSheetWithRetry(sheets, `${sheetOrders}!A1:ZZ1000`);
    let teamUrls = [];
    if (rows && rows.length > 1) {
      const headers = rows[0].map(h => h.trim());
      const teamNameIdx = headers.findIndex(h => h.toLowerCase() === 'teamname');
      if (teamNameIdx >= 0) {
        // Use a Set to dedupe team slugs (case-insensitive)
        const seenSlugs = new Set();
        for (let i = 1; i < rows.length; i++) {
          const row = rows[i];
          const teamName = (row[teamNameIdx] || '').trim();
          if (!teamName) continue;
          // slug: lowercased, spaces and underscores to hyphens, remove non-url-safe chars
          let slug = teamName.toLowerCase()
            .replace(/[_\s]+/g, '-')            // spaces/underscores to hyphens
            .replace(/[^a-z0-9\-]/g, '')        // remove non-url
            .replace(/\-+/g, '-')               // collapse multiple hyphens
            .replace(/^-+|-+$/g, '');           // trim hyphens
          if (!slug || seenSlugs.has(slug)) continue;
          seenSlugs.add(slug);
          teamUrls.push(`https://collty.com/team/${slug}`);
        }
      }
    }
    const urls = staticUrls.concat(teamUrls);
    res.set('Content-Type', 'application/xml');
    res.set('Cache-Control', 'public, max-age=3600');
    res.send(buildSitemapXml(urls));
  } catch (e) {
    console.error('sitemap error:', e);
    res.status(500).send('');
  }
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

// --- Small in-memory cache for query embeddings (dedup transient Jina errors) ---
const _EMB_CACHE = new Map(); // key -> { vec, exp }
const EMB_TTL_MS = Number(process.env.EMB_TTL_MS || 2 * 60 * 1000); // default 2 minutes
function _embGet(key){
  const v = _EMB_CACHE.get(key);
  if (v && v.exp > Date.now()) return v.vec;
  if (v) _EMB_CACHE.delete(key);
}
function _embSet(key, vec){
  _EMB_CACHE.set(key, { vec, exp: Date.now() + EMB_TTL_MS });
  // soft cap ~1000 entries
  if (_EMB_CACHE.size > 1000) {
    const firstKey = _EMB_CACHE.keys().next().value;
    _EMB_CACHE.delete(firstKey);
  }
}

// ------------- Jina embeddings (text → vector[EMB_DIM]) -------------
async function embedText(text) {
  if (!JINA_API_KEY) throw new Error('JINA_API_KEY is missing');
  const payload = {
    input: [String(text || '')],
    model: CURRENT_MODEL,
    task: 'retrieval.query',
    dimensions: CURRENT_DIM
  };
  const MAX_RETRIES = Number(process.env.JINA_MAX_RETRIES || 2); // 2 retries after first try
  const BASE_DELAY = 250; // ms

  let lastErr = null;
  for (let attempt = 0; attempt <= MAX_RETRIES; attempt++) {
    try {
      const resp = await fetch('https://api.jina.ai/v1/embeddings', {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${JINA_API_KEY}`,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(payload)
      });
      const textBody = await resp.text();
      if (!resp.ok) {
        // Try to extract RID and status
        const ridMatch = textBody && textBody.match(/RID:\s*([a-f0-9]+)/i);
        const rid = ridMatch ? ridMatch[1] : null;
        const err = new Error(`Jina error ${resp.status}${rid ? ` [RID:${rid}]` : ''}: ${textBody}`);
        err.status = resp.status;
        throw err;
      }
      // ok -> parse once
      const json = JSON.parse(textBody);
      return json.data[0].embedding;
    } catch (e) {
      lastErr = e;
      const status = e && (e.status || 0);
      const retriable = status === 500 || status === 503 || status === 429;
      if (attempt < MAX_RETRIES && retriable) {
        const delay = BASE_DELAY * Math.pow(2, attempt) + Math.floor(Math.random() * 100);
        console.warn(`[embedText] retry ${attempt + 1}/${MAX_RETRIES} after ${delay}ms due to`, String(e.message || e));
        await new Promise(r => setTimeout(r, delay));
        continue;
      }
      throw e;
    }
  }
  throw lastErr || new Error('Unknown Jina error');
}

async function embedTextCached(q){
  const key = `${CURRENT_MODEL}|${CURRENT_DIM}|${String(q || '').trim()}`;
  const hit = _embGet(key);
  if (hit) return hit;
  const vec = await embedText(key);
  _embSet(key, vec);
  return vec;
}

// Robust batch embedding with retry and fallback splitting
async function embedBatch(texts) {
  if (!Array.isArray(texts) || texts.length === 0) return [];
  const MAX_RETRIES = Number(process.env.JINA_MAX_RETRIES || 2);
  const BASE_DELAY = 250; // ms
  const payload = (arr) => ({
    input: arr,
    model: CURRENT_MODEL,
    task: 'retrieval.passage',
    dimensions: CURRENT_DIM
  });

  for (let attempt = 0; attempt <= MAX_RETRIES; attempt++) {
    try {
      const resp = await fetch('https://api.jina.ai/v1/embeddings', {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${JINA_API_KEY}`,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(payload(texts))
      });
      const textBody = await resp.text();
      if (!resp.ok) {
        const ridMatch = textBody && textBody.match(/RID:\s*([a-f0-9]+)/i);
        const rid = ridMatch ? ridMatch[1] : null;
        const err = new Error(`Jina batch error ${resp.status}${rid ? ` [RID:${rid}]` : ''}: ${textBody}`);
        err.status = resp.status;
        throw err;
      }
      const emb = JSON.parse(textBody);
      return emb.data.map(d => d.embedding);
    } catch (e) {
      const status = e && (e.status || 0);
      const retriable = status === 500 || status === 503 || status === 429;
      if (attempt < MAX_RETRIES && retriable) {
        const delay = BASE_DELAY * Math.pow(2, attempt) + Math.floor(Math.random() * 100);
        console.warn(`[embedBatch] retry ${attempt + 1}/${MAX_RETRIES} after ${delay}ms due to`, String(e.message || e));
        await new Promise(r => setTimeout(r, delay));
        continue;
      }
      // Final failure or non-retriable: split if possible
      if (texts.length > 1) {
        const mid = Math.floor(texts.length / 2);
        const left = await embedBatch(texts.slice(0, mid));
        const right = await embedBatch(texts.slice(mid));
        return left.concat(right);
      }
      // Single item fallback to per-item embed (has its own retries)
      const vec = await embedText(texts[0]);
      return [vec];
    }
  }
  // Should not reach here
  return [];
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
    body: JSON.stringify({ vectors: { size: CURRENT_DIM, distance: 'Cosine' } })
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

// === GET /qdrantInfo — show Qdrant collection config (size, distance, status) ===
app.get('/qdrantInfo', async (req, res) => {
  try {
    if (!vectorsEnabled()) {
      return res.status(200).json({ enabled: false, reason: 'Missing QDRANT/JINA env vars' });
    }
    const r = await qdrantFetch(`/collections/${COLLECTION}`);
    if (!r.ok) {
      const t = await r.text().catch(() => '');
      return res.status(500).json({ error: `Qdrant info failed ${r.status}: ${t}` });
    }
    const j = await r.json();
    const cfg = j?.result?.config || {};
    const params = cfg?.params || {};
    const vectors = params?.vectors || params?.vector || null; // Qdrant may use either key
    const out = {
      enabled: true,
      collection: COLLECTION,
      status: j?.result?.status || null,
      vectors,
      size: vectors?.size ?? null,
      distance: vectors?.distance ?? null,
      model: CURRENT_MODEL,
      emb_dim: CURRENT_DIM,
    };
    return res.json(out);
  } catch (e) {
    console.error('Error in /qdrantInfo:', e);
    return res.status(500).json({ error: 'Failed to fetch Qdrant collection info' });
  }
});

// === POST /vectors/recreate — drop & recreate collection with current EMB_DIM ===
app.post('/vectors/recreate', async (req, res) => {
  try {
    if (!vectorsEnabled()) {
      return res.status(400).json({ error: 'Vectors not enabled (missing env vars)' });
    }
    // Drop old collection if exists
    try {
      const del = await qdrantFetch(`/collections/${COLLECTION}`, { method: 'DELETE' });
      if (!del.ok && del.status !== 404) {
        const t = await del.text().catch(()=>'');
        return res.status(500).json({ error: `Delete failed ${del.status}: ${t}` });
      }
    } catch (e) {
      // continue even if delete errors (non-existent etc.)
      console.warn('[vectors/recreate] delete error (ignored):', String(e.message||e));
    }
    // Create with current VECTOR_SIZE/EMB_DIM
    const create = await qdrantFetch(`/collections/${COLLECTION}`, {
      method: 'PUT',
      body: JSON.stringify({ vectors: { size: CURRENT_DIM, distance: 'Cosine' } })
    });
    if (!create.ok) {
      const t = await create.text().catch(()=> '');
      return res.status(500).json({ error: `Create failed ${create.status}: ${t}` });
    }
    const info = await qdrantFetch(`/collections/${COLLECTION}`);
    const j = info.ok ? await info.json() : null;
    try { _EMB_CACHE.clear(); } catch (_) {}
    return res.json({ ok: true, collection: COLLECTION, model: CURRENT_MODEL, emb_dim: CURRENT_DIM, qdrant: j });
  } catch (e) {
    console.error('Error in /vectors/recreate:', e);
    res.status(500).json({ error: 'Failed to recreate collection' });
  }
});

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
  const parts = [];

  // === Primary weighting (boost important fields in the embedding text) ===
  // We duplicate key signals in a compact way so the embedding pays more attention to them.
  const svc = S(order.Type);
  const ind = S(order.Type2);
  const primaryChunks = [];
  if (svc) primaryChunks.push(svc);
  if (ind) primaryChunks.push(ind);
  if (primaryChunks.length) {
    // Two repetitions is usually enough; avoids token bloat but strengthens the signal
    parts.push(`Primary Focus: ${primaryChunks.join(' | ')} || ${primaryChunks.join(' | ')}`);
  }

  // === Основные поля (explicit structure for the model) ===
  if (order.TeamName) parts.push(`Team: ${S(order.TeamName)}`);
  if (svc) parts.push(`Service/Offering Tags: ${svc}`);
  if (ind) parts.push(`Industry Expertise: ${ind}`);
  if (order.industrymarket_expertise) parts.push(`Market Expertise: ${S(order.industrymarket_expertise)}`);
  if (order.X1Q) parts.push(`Overview: ${S(order.X1Q)}`);
  if (order.Textarea) parts.push(`Keywords: ${S(order.Textarea)}`);
  if (order.Status1) parts.push(`Status1: ${S(order.Status1)}`);
  if (order.Status2) parts.push(`Status2: ${S(order.Status2)}`);
  if (order.Partner_confirmation) parts.push(`Partner confirmation: ${S(order.Partner_confirmation)}`);

  // === Специалисты и их опыт ===
  for (let i = 1; i <= 10; i++) {
    const sp = S(order[`sp${i}`]);
    const cv = S(order[`spcv${i}`]);
    if (sp || cv) {
      parts.push(`Specialist Role: ${sp} | Specialist Experience: ${cv}`);
    }
  }

  // === Дополнительный контекст ===
  if (order.projectid) parts.push(`ProjectID: ${S(order.projectid)}`);
  if (order.Brief) parts.push(`Brief: ${S(order.Brief)}`);
  if (order.Documents) parts.push(`Docs: ${S(order.Documents)}`);
  if (order.nda) parts.push(`NDA: ${S(order.nda)}`);

  return parts.filter(Boolean).join(' | ');
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
// --- Lightweight MMR diversification (token/Jaccard based; no extra embeddings) ---
function _itemTokens(it){
  const parts = [];
  const pushCSV = (s) => String(s||'').split(',').map(x=>x.trim().toLowerCase()).filter(Boolean).forEach(t=>parts.push(t));
  const pushWords = (s) => String(s||'').toLowerCase().split(/[^a-z0-9]+/).filter(Boolean).forEach(w=>parts.push(w));
  // High-signal fields first
  pushCSV(it.Type);
  pushCSV(it.Type2);
  pushWords(it.TeamName);
  // Limited tail from tags/keywords to avoid noise
  const text = String(it.Textarea||'');
  pushWords(text.slice(0, 300));
  // Add acronyms (PR, SMM, CICD)
  const acr = _acronym([it.Type, it.Type2].map(x=>String(x||'')).join(' '));
  if (acr) parts.push(acr.toLowerCase());
  return Array.from(new Set(parts));
}
function _jaccard(aSet, bSet){
  let inter = 0;
  aSet.forEach(x => { if (bSet.has(x)) inter++; });
  const union = aSet.size + bSet.size - inter;
  return union ? (inter / union) : 0;
}
// Select K items in diversified order based on MMR.
// items: [{... , __score:number}], K: how many to keep in order, lambda: trade-off [0..1]
function mmrDiversifyOrder(items, K = 10, lambda = 0.7){
  if (!Array.isArray(items) || items.length <= 1) return items;
  // Precompute token sets
  const tokSets = items.map(_itemTokens).map(arr => new Set(arr));
  const picked = [];
  const remaining = items.map((_,i)=>i);
  // Start from the best by score
  remaining.sort((ia, ib) => (items[ib].__score||0) - (items[ia].__score||0));
  picked.push(remaining.shift());
  while (picked.length < Math.min(K, items.length) && remaining.length){
    let bestIdx = 0, bestVal = -Infinity;
    for (let r = 0; r < remaining.length; r++){
      const i = remaining[r];
      const relevance = items[i].__score || 0;
      let maxSim = 0;
      for (const j of picked){
        const s = _jaccard(tokSets[i], tokSets[j]);
        if (s > maxSim) maxSim = s;
      }
      const mmr = lambda * relevance - (1 - lambda) * maxSim;
      if (mmr > bestVal){ bestVal = mmr; bestIdx = r; }
    }
    picked.push(remaining.splice(bestIdx,1)[0]);
  }
  // Build diversified order for first K, then append the rest by score
  const ordered = picked.map(i => items[i]);
  const pickedSet = new Set(picked);
  const rest = items.filter((_,idx)=>!pickedSet.has(idx)).sort((a,b)=> (b.__score||0)-(a.__score||0));
  return ordered.concat(rest);
}
// Decide whether to apply diversification: only when top-N are too similar.
function shouldDiversify(items, checkTopN = 8, simThreshold = 0.55){
  const N = Math.min(checkTopN, items.length);
  if (N < 3) return false;
  const sets = items.slice(0, N).map(_itemTokens).map(arr => new Set(arr));
  let pairs = 0, high = 0;
  for (let i=0;i<N;i++){
    for (let j=i+1;j<N;j++){
      pairs++;
      if (_jaccard(sets[i], sets[j]) >= simThreshold) high++;
    }
  }
  // Diversify only if more than half of pairs are very similar
  return pairs > 0 && (high / pairs) > 0.5;
}
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

    const BATCH = Number(process.env.EMB_BATCH || 32); // default smaller batch for stability
    let upserted = 0;
    for (let i = 0; i < orders.length; i += BATCH) {
      const slice = orders.slice(i, i + BATCH);
      const texts = slice.map(buildSearchText);

      // robust batch embeddings (retries + split fallback)
      const vectors = await embedBatch(texts);

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
    const vec = await embedTextCached(q);
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

    // Final ordering: diversify if many top results are near-identical by tags/keywords
    items.sort((a,b) => (b.__score || 0) - (a.__score || 0));
    if (shouldDiversify(items, 8, 0.55)) {
      items = mmrDiversifyOrder(items, Math.min(10, items.length), 0.7);
    }
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
    const vec = await embedTextCached(q);
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

    // Final ordering: diversify if many top results are near-identical by tags/keywords
    items.sort((a,b) => (b.__score || 0) - (a.__score || 0));
    if (shouldDiversify(items, 8, 0.55)) {
      items = mmrDiversifyOrder(items, Math.min(10, items.length), 0.7);
    }
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
    const vec = await embedTextCached(q);

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

    // Stable sort + optional diversification for the current page window
    items.sort((a,b) => (b.__score || 0) - (a.__score || 0) || (stableIdForOrder(a) > stableIdForOrder(b) ? 1 : -1));
    if (shouldDiversify(items, 8, 0.55)) {
      items = mmrDiversifyOrder(items, Math.min(PAGE_SIZE, items.length), 0.7);
    }

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

// === POST /embeddingConfig — set model/dim at runtime; optional recreate ===
app.post('/embeddingConfig', async (req, res) => {
  try {
    const { model, dim, recreate = false } = req.body || {};

    if (typeof model === 'string' && model.trim()) {
      CURRENT_MODEL = model.trim();
    }
    if (dim !== undefined) {
      const d = Number(dim);
      if (!Number.isFinite(d) || d < 16 || d > 4096) {
        return res.status(400).json({ error: 'Invalid dim; must be a number between 16 and 4096' });
      }
      CURRENT_DIM = d;
    }
    try { _EMB_CACHE.clear(); } catch (_) {}

    let recreated = false;
    let qdrant = null;
    if (recreate) {
      try {
        const del = await qdrantFetch(`/collections/${COLLECTION}`, { method: 'DELETE' });
        if (!del.ok && del.status !== 404) {
          const t = await del.text().catch(()=> '');
          return res.status(500).json({ error: `Delete failed ${del.status}: ${t}` });
        }
      } catch (_) {}
      const create = await qdrantFetch(`/collections/${COLLECTION}`, {
        method: 'PUT',
        body: JSON.stringify({ vectors: { size: CURRENT_DIM, distance: 'Cosine' } })
      });
      if (!create.ok) {
        const t = await create.text().catch(()=> '');
        return res.status(500).json({ error: `Create failed ${create.status}: ${t}` });
      }
      recreated = true;
      const info = await qdrantFetch(`/collections/${COLLECTION}`);
      qdrant = info.ok ? await info.json() : null;
    }

    return res.json({ ok: true, model: CURRENT_MODEL, emb_dim: CURRENT_DIM, recreated, qdrant });
  } catch (e) {
    console.error('Error in /embeddingConfig:', e);
    res.status(500).json({ error: 'Failed to update embedding config' });
  }
});

// --- Optional: warmup endpoint to mitigate cold start of Qdrant/Jina ---
app.get('/warmup', async (req, res) => {
  try {
    if (vectorsEnabled()) { await ensureCollection(); try { await embedText('ping'); } catch (_) {} }
    res.json({ ok: true });
  } catch (_) {
    res.json({ ok: true }); // never fail warmup
  }
});


// === SEO-friendly team helpers and routes: HTML page, JSON API, and sitemap ===

// Simple HTML escaper for safe insertion into meta tags / HTML
function escapeHtml(str = '') {
  return String(str)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function fmtMoney(v){
  const n = Number(String(v).replace(/[^\d.\-]/g, ''));
  if (!Number.isFinite(n)) return '';
  return `$${Math.round(n).toLocaleString('en-US')}`;
}
function num(v){
  const n = Number(String(v).replace(/[^\d.\-]/g, ''));
  return Number.isFinite(n) ? n : 0;
}
function renderTeamHTML(team, canonicalSlug){
  const title = `${team.TeamName || 'Team'} — Collty`;
  const desc  = String(
    team.seoDescription
    || team.X1Q
    || team.Textarea
    || [team.Type, team.Type2].filter(Boolean).join(' / ')
  ).slice(0, 300);
  const canonical = canonicalSlug || makeCanonicalSlugForTeam(team);

  const rows = [];
  for (let i = 1; i <= 10; i++){
    const role = team[`sp${i}`];
    const hours = team[`hours${i}`] ?? team[`Hours${i}`] ?? '';
    const rate  = team[`rate${i}`]  ?? team[`Rate${i}`]  ?? '';
    const qty   = team[`quantity${i}`] ?? team[`Quantity${i}`] ?? '';
    const cost  = team[`summ${i}`]  ?? team[`Summ${i}`]  ?? team[`cost${i}`] ?? '';
    if (role || hours || rate || qty || cost) {
      rows.push({
        role: String(role||'').trim(),
        hours: num(hours),
        rate:  num(rate),
        qty:   num(qty) || null,
        cost:  num(cost)
      });
    }
  }

  let total = 0;
  const rowsHTML = rows.map(r => {
    const calc = r.cost || (r.hours * r.rate * (r.qty || 1));
    total += calc || 0;
    return `
      <tr>
        <td>${escapeHtml(r.role || '')}</td>
        <td class="num">${r.hours || ''}</td>
        <td class="num">${r.rate ? fmtMoney(r.rate) : ''}</td>
        <td class="num">${r.qty ?? ''}</td>
        <td class="num">${fmtMoney(calc)}</td>
      </tr>`;
  }).join('');

  const tags = []
    .concat(String(team.Type||'').split(','))
    .concat(String(team.Type2||'').split(','))
    .map(s=>s.trim()).filter(Boolean);

  const ld = {
    "@context":"https://schema.org",
    "@type":"Service",
    "name": team.TeamName,
    "description": desc,
    "category": [team.Type, team.Type2].filter(Boolean).join(' / '),
    "brand": "Collty",
    "offers": {
      "@type":"AggregateOffer",
      "priceCurrency":"USD",
      "lowPrice": total ? Math.round(total) : undefined
    },
    "url": `https://collty.com/team/${canonical}`
  };
  const bc = {
    "@context": "https://schema.org",
    "@type": "BreadcrumbList",
    "itemListElement": [
      { "@type": "ListItem", "position": 1, "name": "Home", "item": "https://collty.com/" },
      { "@type": "ListItem", "position": 2, "name": "Teams", "item": "https://collty.com/teams" },
      { "@type": "ListItem", "position": 3, "name": String(team.TeamName || 'Team'), "item": `https://collty.com/team/${canonical}` }
    ]
  };

  return `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>${escapeHtml(title)}</title>
  <meta name="description" content="${escapeHtml(desc)}">
  <link rel="canonical" href="https://collty.com/team/${canonical}">
  <meta name="robots" content="index,follow">
  <meta property="og:title" content="${escapeHtml(title)}">
  <meta property="og:description" content="${escapeHtml(desc)}">
  <meta property="og:url" content="https://collty.com/team/${canonical}">
  <meta property="og:type" content="website">
  <script type="application/ld+json">${JSON.stringify(ld)}</script>
  <script type="application/ld+json">${JSON.stringify(bc)}</script>
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <style>
    :root{--bg:#0f1115;--card:#171a21;--muted:#9aa4b2;--text:#e6e8ec;--chip:#223;--chip-bd:#2a3240;--bd:#273042;--acc:#4ade80}
    body{margin:0;background:var(--bg);color:var(--text);font:16px/1.5 system-ui,-apple-system,Segoe UI,Roboto,Ubuntu}
    .wrap{max-width:980px;margin:24px auto;padding:0 16px}
    .card{background:var(--card);border:1px solid var(--bd);border-radius:14px;padding:20px;box-shadow:0 10px 30px rgba(0,0,0,.35)}
    h1{font-size:22px;margin:0 0 6px}
    .muted{color:var(--muted)}
    .badges{display:flex;flex-wrap:wrap;gap:8px;margin-top:10px}
    .badge{border:1px solid var(--chip-bd);background:var(--chip);padding:6px 10px;border-radius:999px;font-size:12px}
    table{width:100%;border-collapse:separate;border-spacing:0 10px;margin-top:18px}
    th,td{padding:12px 14px;background:#111726}
    th{color:var(--muted);text-align:left;font-weight:600;background:#0e1522}
    tr td:first-child, tr th:first-child { border-top-left-radius:12px;border-bottom-left-radius:12px}
    tr td:last-child, tr th:last-child { border-top-right-radius:12px;border-bottom-right-radius:12px}
    .num{text-align:right;white-space:nowrap}
    .total{display:flex;justify-content:flex-end;margin-top:8px;font-size:15px}
    .total b{margin-left:10px}
    .topline{display:flex;justify-content:space-between;gap:16px;align-items:center}
    .pill{background:#0d1a10;border:1px solid #173c21;color:#9be8b5;padding:4px 8px;border-radius:8px;font-size:12px}
    a{color:#9ecbff;text-decoration:none}
    .cta{display:flex;gap:10px;margin-top:14px;flex-wrap:wrap}
    .btn{padding:10px 14px;border-radius:10px;border:1px solid #2a3240;background:#1b2232;color:#e6e8ec;cursor:pointer}
    .btn.primary{background:#142b1b;border-color:#1e5a2c;color:#bdf4cf}
    form.inline{display:flex;gap:8px;flex-wrap:wrap;margin-top:8px}
    input,textarea{background:#0e1522;border:1px solid #2a3240;border-radius:10px;color:#e6e8ec;padding:10px;font-size:14px}
    input::placeholder,textarea::placeholder{color:#6e7683}
    .lead{margin:8px 0 8px;color:#cbd5e1;font-size:16px;line-height:1.5}
  </style>
</head>
<body>
  <div class="wrap">
    <div class="card">
      <div class="topline">
        <div>
          <div class="muted">Team</div>
          <h1>${escapeHtml(team.TeamName || 'Team')}</h1>
        </div>
        ${ total ? `<div class="pill">Est. total&nbsp;≈&nbsp;<b>${fmtMoney(total)}</b></div>` : '' }
      </div>

      <p class="lead">${escapeHtml(desc)}</p>

      <div style="margin-top:12px">
        <div class="muted">Project types</div>
        <div class="badges">
          ${String(team.Type||'').split(',').map(s=>s.trim()).filter(Boolean).map(s=>`<span class="badge">${escapeHtml(s)}</span>`).join('')}
        </div>
      </div>

      <div style="margin-top:12px">
        <div class="muted">Industry Expertise</div>
        <div class="badges">
          ${String(team.Type2||'').split(',').map(s=>s.trim()).filter(Boolean).map(s=>`<span class="badge">${escapeHtml(s)}</span>`).join('')}
        </div>
      </div>

      ${tags.length ? `<div style="margin-top:12px" class="badges">
        ${tags.map(t=>`<span class="badge">${escapeHtml(t)}</span>`).join('')}
      </div>` : ''}

      ${rows.length ? `
        <table>
          <thead>
            <tr>
              <th>Specialist</th>
              <th class="num">Hours</th>
              <th class="num">Rate ($/h)</th>
              <th class="num">Qty</th>
              <th class="num">Cost</th>
            </tr>
          </thead>
          <tbody>
            ${rowsHTML}
          </tbody>
        </table>
        ${ total ? `<div class="total muted">Total:&nbsp;&nbsp;<b>${fmtMoney(total)}</b></div>` : '' }
      ` : ''}

      <div class="cta">
        <a class="btn" href="https://collty.com">← Back</a>
        <button class="btn primary" id="cta-contact">Contact team</button>
      </div>

      <form class="inline" id="contact-form" method="post" action="/addOrder" style="display:none">
        <input type="hidden" name="teamName" value="${escapeHtml(team.TeamName || '')}">
        <input type="hidden" name="Type" value="${escapeHtml(team.Type || '')}">
        <input type="hidden" name="Type2" value="${escapeHtml(team.Type2 || '')}">
        <input name="name" placeholder="Your name" required>
        <input name="email" placeholder="Email" type="email" required>
        <textarea name="Textarea" placeholder="Describe your needs" rows="3" style="flex:1 1 100%"></textarea>
        <button class="btn primary" type="submit">Send</button>
      </form>
    </div>

    <div style="margin:16px 2px" class="muted">
      <a href="https://collty.com">← Back to Collty</a>
    </div>
  </div>

  <script>
    // Progressive enhancement: show form on CTA, and let frontend hydrate if /static/app.js exists
    document.addEventListener('click', function(e){
      const btn = e.target.closest('#cta-contact');
      if (btn){ 
        e.preventDefault(); 
        const f = document.getElementById('contact-form'); 
        if (f) f.style.display = 'flex'; 
      }
    });
    window.__TEAM_SLUG__=${JSON.stringify(canonical)};
  </script>
  <script src="/static/app.js" defer></script>
</body>
</html>`;
}

// --- Team slug helpers (mirrors frontend slugify logic) ---
function slugifyTeamName(input=''){
  return String(input||'')
    .normalize('NFKD')
    .replace(/[\u0300-\u036f]/g,'')
    .replace(/[^A-Za-z0-9]+/g,'-')
    .replace(/^-+|-+$/g,'')
    .toLowerCase();
}

// --- Slug helpers: base slug + deterministic unique slug map ---
function baseSlugForTeam(team) {
  const explicit = (team && team.slug) ? String(team.slug).trim() : '';
  const byName = slugifyTeamName(team?.TeamName || '');
  return explicit || byName || 'team';
}

// Build deterministic unique slugs for all teams (stable across runs)
function buildSlugMaps(teams = []) {
  // sort by stable id to make the pass order deterministic
  const sorted = [...teams].sort((a, b) => (stableIdForOrder(a) > stableIdForOrder(b) ? 1 : -1));
  const seen = new Set();
  const teamBySlug = new Map();
  const slugByStableId = new Map();

  for (const t of sorted) {
    const base = baseSlugForTeam(t);
    let slug = base;
    if (seen.has(slug)) {
      // append a short deterministic suffix derived from the stable id
      const suf = createHash('sha1').update(stableIdForOrder(t)).digest('hex').slice(0, 6);
      slug = `${base}-${suf}`;
      // ultra-rare edge: if even with suffix collides, extend suffix
      let extra = 8;
      while (seen.has(slug) && extra <= 12) {
        slug = `${base}-${createHash('sha1').update(stableIdForOrder(t)).digest('hex').slice(0, extra)}`;
        extra += 2;
      }
      if (seen.has(slug)) {
        // final fallback (should not happen): add a numeric counter
        let i = 2;
        while (seen.has(`${base}-${suf}-${i}`)) i++;
        slug = `${base}-${suf}-${i}`;
      }
    }
    seen.add(slug);
    teamBySlug.set(slug.toLowerCase(), t);
    slugByStableId.set(stableIdForOrder(t), slug);
  }
  return { teamBySlug, slugByStableId };
}

async function _loadTeamsObjects() {
  const auth = new google.auth.GoogleAuth({ keyFile: path, scopes: ['https://www.googleapis.com/auth/spreadsheets.readonly'] });
  const client = await auth.getClient();
  const sheets = google.sheets({ version: 'v4', auth: client });
  const rows = await fetchSheetWithRetry(sheets, `${sheetOrders}!A1:ZZ1000`);
  return rowsToOrders(rows);
}

function makeCanonicalSlugForTeam(team) {
  return team.slug ? String(team.slug) : slugifyTeamName(team.TeamName || '');
}

async function _findTeamBySlug(slug) {
  const teams = await _loadTeamsObjects();
  const lower = String(slug||'').toLowerCase();
  return teams.find(t => makeCanonicalSlugForTeam(t).toLowerCase() === lower);
}

// HTML page for bots and users; front-end reads window.__TEAM_SLUG__ to auto-open the card
app.get('/team/:slug', async (req, res) => {
  try {
    const teams = await _loadTeamsObjects();
    const { teamBySlug, slugByStableId } = buildSlugMaps(teams);

    const raw = String(req.params.slug || '').toLowerCase();
    let team = teamBySlug.get(raw);

    // Back-compat: if user entered base slug without suffix but unique exists,
    // try to match by base and pick the first with that base
    if (!team) {
      const base = raw;
      // find any slug that equals base or starts with base + '-'
      for (const [slug, t] of teamBySlug.entries()) {
        if (slug === base || slug.startsWith(base + '-')) { team = t; break; }
      }
    }

    if (!team) return res.status(404).send('Not found');

    const canonical = slugByStableId.get(stableIdForOrder(team)) || baseSlugForTeam(team);
    // Hard canonicalization: if requested slug is not the canonical one, redirect permanently
    if (String(req.params.slug || '').toLowerCase() !== String(canonical).toLowerCase()) {
      return res.redirect(301, `/team/${canonical}`);
    }
    res.type('html').status(200).send(renderTeamHTML(team, canonical));
  } catch (e) {
    console.error('SEO /team/:slug error:', e);
    res.status(500).send('Server error');
  }
});

// JSON API to fetch one team by slug (used when user lands directly on /team/:slug)
app.get('/api/team/:slug', async (req, res) => {
  try {
    const teams = await _loadTeamsObjects();
    const { teamBySlug, slugByStableId } = buildSlugMaps(teams);

    const raw = String(req.params.slug || '').toLowerCase();
    let team = teamBySlug.get(raw);
    if (!team) {
      const base = raw;
      for (const [slug, t] of teamBySlug.entries()) {
        if (slug === base || slug.startsWith(base + '-')) { team = t; break; }
      }
    }
    if (!team) return res.status(404).json({ error: 'Not found' });

    const canonical = slugByStableId.get(stableIdForOrder(team)) || baseSlugForTeam(team);
    res.json({ ...team, slug: canonical });
  } catch (e) {
    console.error('/api/team/:slug error:', e);
    res.status(500).json({ error: 'Server error' });
  }
});

// --- Static catalog of teams with real anchors for crawlers and users ---
app.get('/teams', async (req, res) => {
  try {
    const page = Math.max(1, Number(req.query.page || 1));
    const size = Math.min(200, Math.max(10, Number(req.query.size || 100)));
    const teams = await _loadTeamsObjects();
    const { slugByStableId } = buildSlugMaps(teams);

    // Build list of {name, slug}
    const list = teams.map(t => ({
      name: String(t.TeamName || 'Team').trim() || 'Team',
      slug: slugByStableId.get(stableIdForOrder(t)) || baseSlugForTeam(t)
    }));

    // Deterministic order by name then slug
    list.sort((a, b) => a.name.localeCompare(b.name) || a.slug.localeCompare(b.slug));

    const total = list.length;
    const start = (page - 1) * size;
    const end = Math.min(start + size, total);
    const slice = start < end ? list.slice(start, end) : [];

    const prev = page > 1 ? `/teams?page=${page - 1}&amp;size=${size}` : null;
    const next = end < total ? `/teams?page=${page + 1}&amp;size=${size}` : null;

    const html = `&lt;!doctype html&gt;
  &lt;html lang="en"&gt;
  &lt;head&gt;
    &lt;meta charset="utf-8"&gt;
    &lt;title&gt;Teams — Collty&lt;/title&gt;
    &lt;meta name="robots" content="index,follow"&gt;
    &lt;link rel="canonical" href="https://collty.com/teams"&gt;
    &lt;meta name="viewport" content="width=device-width,initial-scale=1"&gt;
    &lt;style&gt;
      body{margin:0;font:16px/1.5 system-ui,-apple-system,Segoe UI,Roboto,Ubuntu;background:#0f1115;color:#e6e8ec}
      .wrap{max-width:980px;margin:24px auto;padding:0 16px}
      a{color:#9ecbff;text-decoration:none}
      ul{list-style:none;padding:0;margin:16px 0}
      li{padding:10px 12px;border-bottom:1px solid #273042}
      .muted{color:#9aa4b2}
      nav{display:flex;gap:10px;margin:14px 0}
      .btn{padding:8px 12px;border:1px solid #2a3240;border-radius:8px;background:#1b2232;color:#e6e8ec}
      .btn[disabled]{opacity:.5}
    &lt;/style&gt;
  &lt;/head&gt;
  &lt;body&gt;
    &lt;div class="wrap"&gt;
      &lt;h1&gt;All Teams&lt;/h1&gt;
      &lt;div class="muted"&gt;Total: ${total}. Page ${page} of ${Math.max(1, Math.ceil(total / size))}.&lt;/div&gt;
      &lt;ul&gt;
        ${slice.map(it =&gt; `&lt;li&gt;&lt;a href="/team/${it.slug}"&gt;${escapeHtml(it.name)}&lt;/a&gt;&lt;/li&gt;`).join('')}
      &lt;/ul&gt;
      &lt;nav&gt;
        ${prev ? `&lt;a class="btn" href="${prev}"&gt;← Prev&lt;/a&gt;` : `&lt;button class="btn" disabled&gt;← Prev&lt;/button&gt;`}
        ${next ? `&lt;a class="btn" href="${next}"&gt;Next →&lt;/a&gt;` : `&lt;button class="btn" disabled&gt;Next →&lt;/button&gt;`}
      &lt;/nav&gt;
      &lt;div class="muted"&gt;&lt;a href="/"&gt;← Back to Collty&lt;/a&gt;&lt;/div&gt;
    &lt;/div&gt;
  &lt;/body&gt;
  &lt;/html&gt;`;

    res.set('Cache-Control', 'public, max-age=300, must-revalidate');
    res.type('html').status(200).send(html);
  } catch (e) {
    console.error('/teams error:', e);
    res.status(500).send('Server error');
  }
});

// --- Auto-discovery of Tilda blog URLs with simple in-memory cache ---
const _tildaAutoCache = { urls: [], ts: 0 };
const TILDA_CACHE_TTL_MS = 15 * 60 * 1000; // 15 minutes

async function _fetchText(url) {
  try {
    const r = await fetch(url, { method: 'GET' });
    if (!r.ok) return null;
    return await r.text();
  } catch (_) { return null; }
}

function _extractLocsFromXml(xml) {
  if (!xml) return [];
  const out = [];
  const re = /<loc>\s*([^<]+)\s*<\/loc>/gi;
  let m; while ((m = re.exec(xml))) { out.push(m[1].trim()); }
  return out;
}

function _extractTpostFromHtml(html) {
  if (!html) return [];
  const out = new Set();
  // Capture href="...", href='...' and href=unquoted
  const re = /href\s*=\s*(?:"([^"]+)"|'([^']+)'|([^\s>]+))/gi;
  let m;
  while ((m = re.exec(html))) {
    const href = (m[1] || m[2] || m[3] || '').trim();
    if (!href) continue;
    if (/\/tpost\//i.test(href)) {
      const abs = href.startsWith('http')
        ? href
        : `https://collty.com${href.startsWith('/') ? '' : '/'}${href}`;
      out.add(abs.replace(/\/$/, '')); // normalize trailing slash
    }
  }
  return Array.from(out);
}
function _extractLinksFromRss(xml) {
  if (!xml) return [];
  const out = new Set();
  // Standard RSS: <item><link>https://collty.com/tpost/...</link></item>
  const re = /<link>\s*([^<]+?)\s*<\/link>/gi;
  let m;
  while ((m = re.exec(xml))) {
    const url = (m[1] || '').trim();
    if (/\/tpost\//i.test(url)) out.add(url.replace(/\/$/, ''));
  }
  return Array.from(out);
}

async function _autoDiscoverTildaPosts() {
  const nowTs = Date.now();
  if (_tildaAutoCache.urls.length && (nowTs - _tildaAutoCache.ts) < TILDA_CACHE_TTL_MS) {
    return _tildaAutoCache.urls;
  }

  const candidates = [
    process.env.TILDA_SITEMAP_URL,
    'https://collty.com/tilda-sitemap.xml',
    'https://collty.com/sitemap_tilda.xml',
    'https://collty.com/sitemap-blog.xml',
    // As a last resort, try the root sitemap (guarded below to avoid loops)
    'https://collty.com/sitemap.xml'
  ].filter(Boolean);

  let urls = [];
  for (const u of candidates) {
    // avoid self-recursion: skip our own sitemap endpoint path (exact match)
    if (u === 'https://collty.com/sitemap.xml') continue;
    // eslint-disable-next-line no-await-in-loop
    const xml = await _fetchText(u);
    if (!xml) continue;
    const locs = _extractLocsFromXml(xml).filter(x => /\/tpost\//.test(x));
    if (locs.length) { urls = locs; break; }
  }

  // Fallback #2: try RSS feed(s) if sitemap with tpost links not found
  if (!urls.length) {
    const rssCandidates = [
      process.env.TILDA_RSS_URL,
      'https://collty.com/rss.xml',
      'https://collty.com/rss',
      'https://collty.com/rss-feed-114489169791.xml' // known feed id in this project
    ].filter(Boolean);

    for (const u of rssCandidates) {
      // eslint-disable-next-line no-await-in-loop
      const xml = await _fetchText(u);
      if (!xml) continue;
      const locs = _extractLinksFromRss(xml);
      if (locs.length) { urls = locs; break; }
    }
  }

  // Fallback: crawl a few public pages and scrape /tpost/ anchors
  if (!urls.length) {
    const seeds = [
      'https://collty.com/',
      'https://collty.com/tpost/',
      'https://collty.com/blog',
    ];
    const found = new Set();
    for (const s of seeds) {
      // eslint-disable-next-line no-await-in-loop
      const html = await _fetchText(s);
      const links = _extractTpostFromHtml(html);
      links.forEach(u => found.add(String(u).replace(/\/$/, '')));
      if (found.size >= 200) break; // guard
    }
    urls = Array.from(found);
  }

  _tildaAutoCache.urls = urls;
  _tildaAutoCache.ts = nowTs;
  return urls;
}

// Sitemap enumerating all site pages (static pages + all team pages)
app.get('/sitemap.xml', async (req, res) => {
  try {
    // 1) Static pages you want indexed (edit this list as needed)
    const STATIC_PAGES = [
      'https://collty.com/',
      'https://collty.com/about',
      'https://collty.com/partnership',
    ];

    // 2) Auto-discovered Tilda blog posts (/tpost/...)
    const blogAuto = await _autoDiscoverTildaPosts();

    // 3) Dynamic team pages from Google Sheets
    const teams = await _loadTeamsObjects();
    const { slugByStableId } = buildSlugMaps(teams);
    const teamUrls = teams.map(t =>
      `https://collty.com/team/${slugByStableId.get(stableIdForOrder(t)) || baseSlugForTeam(t)}`
    );

    // 4) Merge + de-duplicate while preserving order (static → blog(auto) → teams)
    const seen = new Set();
    const urls = [];
    function pushUnique(list) {
      for (let u of list) {
        if (!u) continue;
        // Remove any whitespace (including newlines) accidentally included in source lists
        let s = String(u).replace(/\s+/g, '');
        // Unescape common HTML entity just in case feeds provide &amp;
        s = s.replace(/&amp;/g, '&').trim();
        const norm = s.replace(/\/$/, '');
        if (!seen.has(norm)) { seen.add(norm); urls.push(norm); }
      }
    }
    pushUnique(STATIC_PAGES);
    pushUnique(blogAuto);
    pushUnique(teamUrls);

    const body =
`<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
${urls.map(u => `<url><loc>${u}</loc></url>`).join('\n')}
</urlset>`;

    res.set('Cache-Control', 'public, max-age=300, must-revalidate');
    res.set('Vary', 'Accept-Encoding');
    res.type('application/xml').send(body);
  } catch (e) {
    console.error('sitemap.xml error:', e);
    res.status(500).send('Failed to build sitemap');
  }
});

// --- robots.txt ---
app.get('/robots.txt', (req, res) => {
  res.type('text/plain').send(
`User-agent: *
Allow: /

Sitemap: https://collty.com/sitemap.xml`
  );
});

app.use((req, res) => {
  console.warn('[404]', req.method, req.originalUrl);
  res.status(404).send('Not Found');
});

app.listen(port, () => {
  // console.log(`🚀 Server running on port ${port}`);
});