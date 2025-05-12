// server.js (обновлённая версия для Google Sheets API)
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
  res.send('✅ Server is working');
});

app.get('/orders', async (req, res) => {
  try {
    const auth = new google.auth.GoogleAuth({
      keyFile: path,
      scopes: ['https://www.googleapis.com/auth/spreadsheets.readonly'],
    });

    const client = await auth.getClient();
    const sheets = google.sheets({ version: 'v4', auth: client });

    const response = await sheets.spreadsheets.values.get({
      spreadsheetId,
      range: `${sheetOrders}!A1:ZZ1000`,
    });

    const rows = response.data.values;
    if (!rows || rows.length === 0) return res.json([]);

    const headers = rows[0].map(h => h.trim());
    const data = rows.slice(1).map(row =>
      headers.reduce((obj, key, i) => {
        obj[key] = row[i] || '';
        return obj;
      }, {})
    );

    const confirmed = req.query.confirmed === 'true';
    const typeQueryRaw = (req.query.type || '').toLowerCase().trim();
    const type2Query = (req.query.type2 || '').toLowerCase().trim();
    const emailQuery = (req.query.email || '').toLowerCase().trim();

    const typeTerms = typeQueryRaw
      .split(/[+,]/)
      .map(s => s.trim())
      .filter(Boolean);

    const filtered = data.filter(row => {
      const type1 = (row.Type || '').toLowerCase();
      const type2 = (row.Type2 || '').toLowerCase();
      const email = (row.partner || '').toLowerCase();
      const status = (row.Textarea || '').toLowerCase();

      const matchesType = typeTerms.length
        ? typeTerms.every(term => type1.includes(term) || type2.includes(term))
        : true;

      const matchesType2 = type2Query ? type2.includes(type2Query) : true;
      const matchesEmail = emailQuery ? email.includes(emailQuery) : true;
      const matchesConfirmed = confirmed ? status.includes('confirmed') : true;

      return matchesType && matchesType2 && matchesEmail && matchesConfirmed;
    });

    res.json(filtered);
  } catch (error) {
    console.error('❌ Error in /orders:', error);
    res.status(200).json([]);
  }
});

app.get('/leads', async (req, res) => {
  try {
    const auth = new google.auth.GoogleAuth({
      keyFile: path,
      scopes: ['https://www.googleapis.com/auth/spreadsheets.readonly'],
    });

    const client = await auth.getClient();
    const sheets = google.sheets({ version: 'v4', auth: client });

    const response = await sheets.spreadsheets.values.get({
      spreadsheetId,
      range: `${sheetLeads}!A2:Z1000`, // предполагаем, что первая строка — заголовки
    });

    const leads = response.data.values || [];

    // фильтрация по email
    const emailQuery = (req.query.email || '').toLowerCase();
    const filteredLeads = leads.filter(row => (row[3] || '').toLowerCase().includes(emailQuery));

    res.json(filteredLeads);
  } catch (error) {
    console.error('❌ Error in /leads:', error);
    res.status(500).json({ error: 'Failed to load leads' });
  }
});

app.post('/addOrder', async (req, res) => {
  try {
    const { name, email, partner, specialists } = req.body;

    const auth = new google.auth.GoogleAuth({
      keyFile: path,
      scopes: ['https://www.googleapis.com/auth/spreadsheets'],
    });

    const client = await auth.getClient();
    const sheets = google.sheets({ version: 'v4', auth: client });

    const now = new Date().toLocaleString('en-GB', { timeZone: 'Asia/Tbilisi' });
    const row = [now, name, email, partner, name];

    for (let i = 0; i < 10; i++) {
      const item = specialists[i] || {};
      row.push(item.sp || '', item.hours || '', item.quantity || '', item.cost || '');
    }

    await sheets.spreadsheets.values.append({
      spreadsheetId,
      range: `${sheetLeads}!A1`,
      valueInputOption: 'USER_ENTERED',
      insertDataOption: 'INSERT_ROWS',
      requestBody: {
        values: [row],
      },
    });

    res.status(200).json({ success: true });
  } catch (err) {
    console.error('❌ Error in /addOrder:', err);
    res.status(500).json({ error: 'Failed to append data' });
  }
});

app.listen(port, () => {
  console.log(`🚀 Server running on port ${port}`);
});
