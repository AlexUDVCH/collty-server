// === server.js (Final version with PATCH /confirm using timestamp and dual email/partner support) ===
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

app.use(cors());
app.use(express.json());

app.get('/', (req, res) => {
  res.send('✅ Server is running');
});

// === GET /orders ===
app.get('/orders', async (req, res) => {
  try {
    const auth = new google.auth.GoogleAuth({ keyFile: path, scopes: ['https://www.googleapis.com/auth/spreadsheets.readonly'] });
    const client = await auth.getClient();
    const sheets = google.sheets({ version: 'v4', auth: client });

    const response = await sheets.spreadsheets.values.get({
      spreadsheetId,
      range: `${sheetOrders}!A1:ZZ1000`,
    });

    const rows = response.data.values;
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
  } catch (err) {
    console.error('Error in /orders:', err);
    res.status(200).json([]);
  }
});

// Helper function with retry for /leads
async function fetchLeadsWithRetry(sheets, retries = 4, delayMs = 1500) {
  for (let i = 0; i < retries; i++) {
    try {
      const response = await sheets.spreadsheets.values.get({
        spreadsheetId,
        range: `${sheetLeads}!A1:ZZ1000`,
      });
      return response.data.values;
    } catch (error) {
      if (error.code === 503 && i < retries - 1) {
        console.warn(`Retrying leads fetch (${i + 1}) after ${delayMs}ms...`);
        await new Promise((res) => setTimeout(res, delayMs));
      } else {
        throw error;
      }
    }
  }
}

// === GET /leads ===
app.get('/leads', async (req, res) => {
  try {
    const auth = new google.auth.GoogleAuth({ keyFile: path, scopes: ['https://www.googleapis.com/auth/spreadsheets.readonly'] });
    const client = await auth.getClient();
    const sheets = google.sheets({ version: 'v4', auth: client });

    const rows = await fetchLeadsWithRetry(sheets);

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
  } catch (err) {
    console.error('Error in /leads:', err);
    res.status(200).json([]);
  }
});

// === GET /keywords ===
app.get('/keywords', async (req, res) => {
  try {
    const auth = new google.auth.GoogleAuth({ keyFile: path, scopes: ['https://www.googleapis.com/auth/spreadsheets.readonly'] });
    const client = await auth.getClient();
    const sheets = google.sheets({ version: 'v4', auth: client });

    const response = await sheets.spreadsheets.values.get({
      spreadsheetId,
      range: `${sheetOrders}!A1:ZZ1000`,
    });

    const rows = response.data.values;
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

    const response = await sheets.spreadsheets.values.get({
      spreadsheetId,
      range: `${sheetLeads}!A1:ZZ1000`,
    });

    const rows = response.data.values;
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

    const response = await sheets.spreadsheets.values.get({
      spreadsheetId,
      range: `${sheetLeads}!A1:ZZ1000`,
    });

    const rows = response.data.values;
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

    const response = await sheets.spreadsheets.values.get({
      spreadsheetId,
      range: `${sheetLeads}!A1:ZZ1000`,
    });

    const rows = response.data.values;
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

    // Чтение всех строк
    const response = await sheets.spreadsheets.values.get({
      spreadsheetId,
      range: `${sheetLeads}!A1:ZZ1000`,
    });
    const rows = response.data.values;
    const headers = rows[0];
    const emailCol = headers.findIndex(h => h.trim().toLowerCase() === 'email');
    const timeCol = headers.findIndex(h => h.trim().toLowerCase() === 'timestamp');

    const targetRowIndex = rows.findIndex((row, i) =>
      i > 0 &&
      (row[emailCol] || '').toLowerCase().trim() === email.toLowerCase().trim() &&
      (row[timeCol] || '').trim() === timestamp.trim()
    );

    if (targetRowIndex < 1) return res.status(404).json({ error: 'Matching row not found' });

    // Удалить строку с помощью batchUpdate
    await sheets.spreadsheets.batchUpdate({
      spreadsheetId,
      requestBody: {
        requests: [
          {
            deleteDimension: {
              range: {
                sheetId: 1182114241, // LeadsCollty_Responses sheetId
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
  } catch (err) {
    console.error('Error in /deleteOrder:', err);
    res.status(500).json({ error: 'Failed to delete order' });
  }
});

// === Utility ===
function columnToLetter(col) {
  let letter = '';
  while (col >= 0) {
    letter = String.fromCharCode((col % 26) + 65) + letter;
    col = Math.floor(col / 26) - 1;
  }
  return letter;
}
// === PATCH /updateOrderHours ===
app.patch('/updateOrderHours', async (req, res) => {
  const { email, timestamp, ...fields } = req.body;
  if (!email || !timestamp) return res.status(400).json({ error: 'Missing email or timestamp' });

  try {
    const auth = new google.auth.GoogleAuth({ keyFile: path, scopes: ['https://www.googleapis.com/auth/spreadsheets'] });
    const client = await auth.getClient();
    const sheets = google.sheets({ version: 'v4', auth: client });

    // Получаем все строки
    const response = await sheets.spreadsheets.values.get({
      spreadsheetId,
      range: `${sheetLeads}!A1:ZZ1000`,
    });

    const rows = response.data.values;
    const headers = rows[0];
    const emailCol = headers.findIndex(h => h.trim().toLowerCase() === 'email');
    const timeCol = headers.findIndex(h => h.trim().toLowerCase() === 'timestamp');

    // Найти строку по email+timestamp
    const targetRowIndex = rows.findIndex((row, i) =>
      i > 0 &&
      (row[emailCol] || '').toLowerCase().trim() === email.toLowerCase().trim() &&
      (row[timeCol] || '').trim() === timestamp.trim()
    );

    if (targetRowIndex < 1) return res.status(404).json({ error: 'Matching row not found' });

    // Для каждого поля hours1..10, quantity1..10 — найти колонку и обновить
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

    // Одновременное обновление всех ячеек
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
    console.error('Error in /updateOrderHours:', err);
    res.status(500).json({ error: 'Failed to update hours' });
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

    // Читаем все строки
    const response = await sheets.spreadsheets.values.get({
      spreadsheetId,
      range: `${sheetOrders}!A1:ZZ1000`,
    });

    const rows = response.data.values;
    const headers = rows[0];
    const teamNameCol = headers.findIndex(h => h.trim().toLowerCase() === 'teamname');

    // Находим строку нужной команды
    const targetRowIndex = rows.findIndex((row, i) =>
      i > 0 &&
      (row[teamNameCol] || '').trim() === teamName.trim()
    );
    if (targetRowIndex < 1) return res.status(404).json({ error: 'Team not found' });

    // Обновляем только указанные поля
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
  } catch (err) {
    console.error('Error in /updateTeam:', err);
    res.status(500).json({ error: 'Failed to update team' });
  }
});
// === POST /addTeam ===
app.post('/addTeam', async (req, res) => {
  try {
    const {
      timestamp = new Date().toLocaleString('en-GB', { timeZone: 'Asia/Tbilisi' }),
      Status1 = '', Status2 = '', PaymentStatus = '', anticipated_project_start_date = '',
      TeamName = '', Textarea = '', Created_time = '', partner = '', Partner_confirmation = '',
      totalsumm = '', month = '', X1Q = '', XXX = '', industrymarket_expertise = '',
      Brief = '', Chat = '', Documents = '', nda = '', Link = '', Type = '', Type2 = '',
      spcv1 = '', spcv2 = '', spcv3 = '', spcv4 = '', spcv5 = '', spcv6 = '', spcv7 = '', spcv8 = '', spcv9 = '', spcv10 = ''
    } = req.body;

    // Specialists & hours
    const getVal = (key) => req.body[key] || '';
    const specialistFields = [];
    for (let i = 1; i <= 10; i++) {
      specialistFields.push(getVal(`sp${i}`), getVal(`hours${i}`), getVal(`quantity${i}`), getVal(`summ${i}`));
    }

    // === Если у тебя реально в таблице после Partner_confirmation идет пустая колонка, добавь '' сюда ===
    // (см. скриншот: Partner_confirmation -- тут есть лишний столбец перед totalsumm)
    // Если нет — убери.
    const row = [
      timestamp,              // timestamp
      Status1,                // Status1
      Status2,                // Status2
      PaymentStatus,          // Payment status
      anticipated_project_start_date, // anticipated_project_start_date
      TeamName,               // TeamName
      Textarea,               // Textarea
      Created_time,           // Created time
      partner,                // partner
      Partner_confirmation,   // Partner_confirmation
      '',                     // <--- Пустая колонка после Partner_confirmation (если она есть)
      totalsumm,              // totalsumm
      month,                  // month
      X1Q,                    // X1Q
      XXX,                    // XXX
      industrymarket_expertise, // industrymarket_expertise
      ...specialistFields,    // sp1, hours1, quantity1, summ1, ..., sp10, hours10, quantity10, summ10
      Brief, Chat, Documents, nda, Link, Type, Type2,
      spcv1, spcv2, spcv3, spcv4, spcv5, spcv6, spcv7, spcv8, spcv9, spcv10
    ];

    // Пишем в таблицу начиная с B1 (строго по колонкам)
    await sheets.spreadsheets.values.append({
      spreadsheetId,
      range: 'DataBaseCollty_Teams!B1',
      valueInputOption: 'USER_ENTERED',
      insertDataOption: 'INSERT_ROWS',
      requestBody: { values: [row] },
    });

    res.status(200).json({ success: true });
  } catch (err) {
    console.error('Error in /addTeam:', err);
    res.status(500).json({ error: 'Failed to append data' });
  }
});

app.listen(port, () => {
  console.log(`🚀 Server running on port ${port}`);
});