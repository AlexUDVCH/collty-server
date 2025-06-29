// === server.js (Final version with Caching & 429/503 exponential backoff) ===
const express = require('express');
const { google } = require('googleapis');
const cors = require('cors');
require('dotenv').config();

const app = express();
const port = process.env.PORT || 3000;

const path = '/etc/secrets/credentials.json';
const spreadsheetId = '1GIl15j9L1-KPyn2evruz3F0sscNo308mAC7huXm0WkY';
const sheetOrders = 'DataBaseCollty_Teams';
const sheetLeads = 'LeadsCollty_Responses';

app.use(cors({
  origin: [
    'https://collty.com',
    'https://www.collty.com',
    'http://localhost:3000'
  ],
  credentials: true, // если нужны куки или авторизация
}));
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
      Status1 = '', Status2 = '', "Payment status": PaymentStatus = '', Textarea = '', Type = '', Type2 = '',
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
      Status1, Status2, PaymentStatus, Textarea, '',
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
    await Promise.all(updates.map(u =>
      sheets.spreadsheets.values.update({
        spreadsheetId,
        range: u.range,
        valueInputOption: 'USER_ENTERED',
        requestBody: { values: [[u.value]] },
      })
    ));
    res.status(200).json({ success: true });
    cacheLeads = { data: null, ts: 0 };
  } catch (err) {
    console.error('Error in /updateOrderHours:', err);
    res.status(500).json({ error: 'Failed to update hours' });
  }
});

// === PATCH /leads/:id (обновление по id для чата и других полей) ===
app.patch('/leads/:id', async (req, res) => {
  const { id } = req.params;
  try {
    const auth = new google.auth.GoogleAuth({ keyFile: path, scopes: ['https://www.googleapis.com/auth/spreadsheets'] });
    const client = await auth.getClient();
    const sheets = google.sheets({ version: 'v4', auth: client });
    const rows = await fetchSheetWithRetry(sheets, `${sheetLeads}!A1:ZZ1000`);
    const headers = rows[0].map(h => h.trim());
    // Находим первый уникальный столбец
    const idCol = headers.findIndex(h =>
  h.trim().toLowerCase() === 'projectid' ||
  h.trim().toLowerCase() === 'id' ||
  h.trim().toLowerCase() === 'unique_id' ||
  h.trim().toLowerCase() === 'timestamp'
);
    if (idCol < 0) return res.status(400).json({ error: 'No id/unique_id/timestamp column' });
    const rowIndex = rows.findIndex((row, i) => {
      if (i === 0) return false;
      const obj = headers.reduce((acc, key, j) => { acc[key] = row[j] || ''; return acc; }, {});
      return (obj.projectid || '').trim() === id.trim();
    });
    if (rowIndex < 1) return res.status(404).json({ error: 'Row not found' });

    const updates = [];
    Object.entries(req.body).forEach(([key, value]) => {
      const col = headers.findIndex(h => h.trim() === key);
      if (col >= 0) {
        updates.push({
          range: `${sheetLeads}!${columnToLetter(col)}${rowIndex + 1}`,
          value: value
        });
      }
    });
    if (!updates.length) return res.status(400).json({ error: 'No valid fields to update' });

    await Promise.all(updates.map(u =>
      sheets.spreadsheets.values.update({
        spreadsheetId,
        range: u.range,
        valueInputOption: 'USER_ENTERED',
        requestBody: { values: [[u.value]] },
      })
    ));
    res.status(200).json({ success: true });
    cacheLeads = { data: null, ts: 0 };
  } catch (err) {
    console.error('Error in PATCH /leads/:id', err);
    res.status(500).json({ error: 'Failed to update lead by id' });
  }
});

// === PATCH /updateTeam ===
app.patch('/updateTeam', async (req, res) => {
  const { teamName, ...fields } = req.body;
  if (!teamName) return res.status(400).json({ error: 'Missing teamName' });
  try {
    const auth = new google.auth.GoogleAuth({ keyFile: path, scopes: ['https://www.googleapis.com/auth/spreadsheets'] });
    const client = await auth.getClient();
    const sheets = google.sheets({ version: 'v4', auth: client });
    const rows = await fetchSheetWithRetry(sheets, `${sheetOrders}!A1:ZZ1000`);
    const headers = rows[0];
    const teamNameCol = headers.findIndex(h => h.trim().toLowerCase() === 'teamname');
    const targetRowIndex = rows.findIndex((row, i) =>
      i > 0 &&
      (row[teamNameCol] || '').trim() === teamName.trim()
    );
    if (targetRowIndex < 1) return res.status(404).json({ error: 'Team not found' });
    const updates = [];
    Object.entries(fields).forEach(([key, value]) => {
      const col = headers.findIndex(h => h.trim().toLowerCase() === key.toLowerCase());
      if (col >= 0) {
        updates.push({
          range: `${sheetOrders}!${columnToLetter(col)}${targetRowIndex + 1}`,
          value: value
        });
      }
    });
    await Promise.all(updates.map(u =>
      sheets.spreadsheets.values.update({
        spreadsheetId,
        range: u.range,
        valueInputOption: 'USER_ENTERED',
        requestBody: { values: [[u.value]] },
      })
    ));
    res.status(200).json({ success: true });
    cacheOrders = { data: null, ts: 0 };
  } catch (err) {
    console.error('Error in /updateTeam:', err);
    res.status(500).json({ error: 'Failed to update team' });
  }
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
      TeamName = '', Textarea = '', Created_time = '', partner = '', Partner_confirmation = '',
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
      timestamp, Status1, Status2, PaymentStatus, anticipated_project_start_date, TeamName, Textarea, Created_time,
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
    await Promise.all(updates.map(u =>
      sheets.spreadsheets.values.update({
        spreadsheetId,
        range: u.range,
        valueInputOption: 'USER_ENTERED',
        requestBody: { values: [[u.value]] },
      })
    ));
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
app.get('/leads/:id', async (req, res) => {
  const { id } = req.params;
  try {
    const auth = new google.auth.GoogleAuth({ keyFile: path, scopes: ['https://www.googleapis.com/auth/spreadsheets.readonly'] });
    const client = await auth.getClient();
    const sheets = google.sheets({ version: 'v4', auth: client });

    const rows = await fetchSheetWithRetry(sheets, `${sheetLeads}!A1:ZZ1000`);
    const headers = rows[0].map(h => h.trim());
    console.log('[GET /leads/:id] Headers:', headers);

    // !!! SEARCH ONLY BY projectid
    const idCol = headers.findIndex(h => h.trim().toLowerCase() === 'projectid');
    if (idCol < 0) {
      console.log('[GET /leads/:id] Нет нужной id-колонки');
      return res.status(400).json({ error: 'No projectid column' });
    }
    // Выведи все projectid в консоль
    const allIds = rows.slice(1).map(row => row[idCol]);
    console.log('[GET /leads/:id] All projectids:', allIds);
    console.log('[GET /leads/:id] Looking for:', id);
    // Находим строку по projectid (сравниваем .trim())
    const row = rows.find((row, i) => i > 0 && (row[idCol] || '').trim() === id.trim());
    if (!row) {
      console.log('[GET /leads/:id] Row not found for:', id);
      return res.status(404).json({ error: 'Row not found' });
    }
    const result = headers.reduce((obj, key, i) => {
      obj[key] = row[i] || '';
      return obj;
    }, {});
    ['ClientChat', 'PartnerChat'].forEach(field => {
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

app.patch('/leads/:id', async (req, res) => {
  const { id } = req.params;
  console.log('PATCH /leads/:id body:', req.body); // Лог тела для отладки
  try {
    const auth = new google.auth.GoogleAuth({ keyFile: path, scopes: ['https://www.googleapis.com/auth/spreadsheets'] });
    const client = await auth.getClient();
    const sheets = google.sheets({ version: 'v4', auth: client });
    const rows = await fetchSheetWithRetry(sheets, `${sheetLeads}!A1:ZZ1000`);
    const headers = rows[0].map(h => h.trim());
    // !!! SEARCH ONLY BY projectid
    const idCol = headers.findIndex(h => h.trim().toLowerCase() === 'projectid');
    if (idCol < 0) return res.status(400).json({ error: 'No projectid column' });
    const rowIndex = rows.findIndex((row, i) => i > 0 && (row[idCol] || '').trim() === id.trim());
    if (rowIndex < 1) return res.status(404).json({ error: 'Row not found' });

    const updates = [];
    Object.entries(req.body).forEach(([key, value]) => {
      const col = headers.findIndex(h => h.trim() === key);
      if (col >= 0) {
        if ((key === 'ClientChat' || key === 'PartnerChat') && typeof value !== 'string') {
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

    await Promise.all(updates.map(u =>
      sheets.spreadsheets.values.update({
        spreadsheetId,
        range: u.range,
        valueInputOption: 'USER_ENTERED',
        requestBody: { values: [[u.value]] },
      })
    ));
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

app.listen(port, () => {
  console.log(`🚀 Server running on port ${port}`);
});