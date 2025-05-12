const express = require('express');
const { google } = require('googleapis');
const cors = require('cors');
require('dotenv').config();

const app = express();
const port = process.env.PORT || 3000;

const path = '/etc/secrets/credentials.json';
const spreadsheetId = '1GIl15j9L1-KPyn2evruz3F0sscNo308mAC7huXm0WkY';

app.use(cors());
app.use(express.json());

app.get('/', (req, res) => {
  res.send('✅ Server is working');
});

// ========== ORDERS ==========
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
      range: 'DataBaseCollty_Teams!A1:ZZ1000',
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

    const emailQuery = (req.query.email || '').toLowerCase().trim();
    const filtered = emailQuery
      ? data.filter(row => (row.partner || '').toLowerCase().includes(emailQuery))
      : data;

    res.json(filtered);
  } catch (error) {
    console.error('❌ Error in /orders:', error);
    res.status(500).json({ error: 'Error reading orders data' });
  }
});

// ========== LEADS ==========
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
      range: 'LeadsCollty_Responses!A1:ZZ1000',
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

    const emailQuery = (req.query.email || '').toLowerCase().trim();
    const filtered = emailQuery
      ? data.filter(row => (row.email || '').toLowerCase().includes(emailQuery))
      : data;

    res.json(filtered);
  } catch (error) {
    console.error('❌ Error in /leads:', error);
    res.status(500).json({ error: 'Error reading leads data' });
  }
});

app.listen(port, () => {
  console.log(`🚀 Server running on port ${port}`);
});