// === FULL server.js for Collty (Google Sheets version with leads + orders + keywords + form + confirmation) ===
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

    const response = await sheets.spreadsheets.values.get({ spreadsheetId, range: `${sheetOrders}!A1:ZZ1000` });
    const rows = response.data.values;
    if (!rows || rows.length === 0) return res.json([]);

    const headers = rows[0].map(h => h.trim());
    const data = rows.slice(1).map(row => headers.reduce((obj, key, i) => { obj[key] = row[i] || ''; return obj; }, {}));

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

// === GET /leads ===
app.get('/leads', async (req, res) => {
  try {
    const auth = new google.auth.GoogleAuth({ keyFile: path, scopes: ['https://www.googleapis.com/auth/spreadsheets.readonly'] });
    const client = await auth.getClient();
    const sheets = google.sheets({ version: 'v4', auth: client });

    const response = await sheets.spreadsheets.values.get({ spreadsheetId, range: `${sheetLeads}!A1:ZZ1000` });
    const rows = response.data.values;
    if (!rows || rows.length === 0) return res.json([]);

    const headers = rows[0].map(h => h.trim());
    const data = rows.slice(1).map(row => headers.reduce((obj, key, i) => { obj[key] = row[i] || ''; return obj; }, {}));

    const emailQuery = (req.query.email || '').toLowerCase().trim();
    const confirmed = req.query.confirmed === 'true';

    const filtered = data.filter(row => {
      const email = (row.Email || row.email || '').toLowerCase();
      const textarea = (row.Textarea || '').toLowerCase();
      const matchEmail = emailQuery ? email.includes(emailQuery) : true;
      const matchConfirmed = confirmed ? textarea.includes('confirmed') : true;
      return matchEmail && matchConfirmed;
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

    const response = await sheets.spreadsheets.values.get({ spreadsheetId, range: `${sheetOrders}!A1:ZZ1000` });
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

// === PATCH /confirm ===
app.patch('/confirm', async (req, res) => {
  const { email, timestamp } = req.body;
  if (!email || !timestamp) return res.status(400).json({ error: 'Missing email or timestamp' });

  try {
    const auth = new google.auth.GoogleAuth({ keyFile: path, scopes: ['https://www.googleapis.com/auth/spreadsheets'] });
    const client = await auth.getClient();
    const sheets = google.sheets({ version: 'v4', auth: client });

    const response = await sheets.spreadsheets.values.get({ spreadsheetId, range: `${sheetLeads}!A1:ZZ1000` });
    const rows = response.data.values;
    if (!rows || rows.length === 0) return res.status(404).json({ error: 'No data found' });

    const headers = rows[0];
    const timestampCol = headers.findIndex(h => h.trim().toLowerCase() === 'timestamp');
    const confirmationCol = headers.findIndex(h => h.trim().toLowerCase() === 'confirmation');

    const rowIndex = rows.findIndex((row, i) => i > 0 && row[timestampCol]?.trim() === timestamp.trim());
    if (rowIndex < 1) return res.status(404).json({ error: 'Matching row not found' });

    const colLetter = String.fromCharCode(65 + confirmationCol);
    const targetRange = `${sheetLeads}!${colLetter}${rowIndex + 1}`;

    await sheets.spreadsheets.values.update({
      spreadsheetId,
      range: targetRange,
      valueInputOption: 'USER_ENTERED',
      requestBody: { values: [['Confirmed']] },
    });

    res.status(200).json({ success: true });
  } catch (err) {
    console.error('Error in /confirm:', err);
    res.status(500).json({ error: 'Failed to confirm' });
  }
});

app.listen(port, () => {
  console.log(`🚀 Server running on port ${port}`);
});