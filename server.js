// server.js (обновлённая версия для Google Sheets API с поддержкой /keywords)
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
      range: `${sheetLeads}!A1:ZZ1000`,
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
    const filtered = data.filter(row => (row.Email || '').toLowerCase().includes(emailQuery));

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
      range: `${sheetLeads}!A2:Z1000`,
    });

    const leads = response.data.values || [];
    const emailQuery = (req.query.email || '').toLowerCase();
    const filteredLeads = leads.filter(row => (row[2] || '').toLowerCase().includes(emailQuery));

    res.json(filteredLeads);
  } catch (error) {
    console.error('❌ Error in /leads:', error);
    res.status(500).json({ error: 'Failed to load leads' });
  }
});

app.get('/keywords', async (req, res) => {
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

    res.json({
      type: Array.from(type),
      type2: Array.from(type2),
    });
  } catch (err) {
    console.error('❌ Error in /keywords:', err);
    res.status(500).json({ error: 'Failed to load keywords' });
  }
});

app.post('/addOrder', async (req, res) => {
  try {
    const { name, email, partner, teamName, specialists } = req.body;

    const auth = new google.auth.GoogleAuth({
      keyFile: path,
      scopes: ['https://www.googleapis.com/auth/spreadsheets'],
    });

    const client = await auth.getClient();
    const sheets = google.sheets({ version: 'v4', auth: client });

    const now = new Date().toLocaleString('en-GB', { timeZone: 'Asia/Tbilisi' });
    const row = [now, name, email, partner, teamName];

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