// ✅ UNIVERSAL SERVER — Google Sheets backend
const express = require('express');
const { google } = require('googleapis');
const cors = require('cors');
require('dotenv').config();

const app = express();
const port = process.env.PORT || 3000;
const path = '/etc/secrets/credentials.json'; // your credentials path
const spreadsheetId = '1GIl15j9L1-KPyn2evruz3F0sscNo308mAC7huXm0WkY';
const sheetName = 'DataBaseCollty_Teams';

app.use(cors());
app.use(express.json());

app.get('/', (req, res) => {
  res.send('✅ Server is running');
});

app.get('/orders', async (req, res) => {
  try {
    const auth = new google.auth.GoogleAuth({
      keyFile: path,
      scopes: ['https://www.googleapis.com/auth/spreadsheets.readonly']
    });
    const client = await auth.getClient();
    const sheets = google.sheets({ version: 'v4', auth: client });

    const response = await sheets.spreadsheets.values.get({
      spreadsheetId,
      range: `${sheetName}!A1:ZZ1000`
    });

    const rows = response.data.values;
    if (!rows || rows.length < 2) return res.status(404).json([]);

    const headers = rows[0].map(h => h.trim());
    const data = rows.slice(1).map(row =>
      headers.reduce((obj, key, i) => {
        obj[key] = row[i] || '';
        return obj;
      }, {})
    );

    const type = (req.query.type || '').toLowerCase().trim();
    const type2 = (req.query.type2 || '').toLowerCase().trim();
    const email = (req.query.email || '').toLowerCase().trim();
    const confirmed = req.query.confirmed === 'true';

    const filtered = data.filter(row => {
      if (email && row.partner && row.partner.toLowerCase().includes(email)) return true;
      if (type && ((row.Type || '').toLowerCase().includes(type) || (row.Type2 || '').toLowerCase().includes(type))) return true;
      if (type2 && (row.Type2 || '').toLowerCase().includes(type2)) return true;
      if (confirmed && row.Textarea && row.Textarea.toLowerCase().includes('confirmed')) return true;
      if (!type && !type2 && !email && !confirmed) return true;
      return false;
    });

    res.json(filtered);
  } catch (err) {
    console.error('❌ Error:', err);
    res.status(500).json({ error: 'Server error while fetching orders.' });
  }
});

app.get('/keywords', async (req, res) => {
  try {
    const auth = new google.auth.GoogleAuth({
      keyFile: path,
      scopes: ['https://www.googleapis.com/auth/spreadsheets.readonly']
    });
    const client = await auth.getClient();
    const sheets = google.sheets({ version: 'v4', auth: client });

    const response = await sheets.spreadsheets.values.get({
      spreadsheetId,
      range: `${sheetName}!A1:ZZ1000`
    });

    const rows = response.data.values;
    if (!rows || rows.length < 2) return res.json([]);

    const headers = rows[0].map(h => h.trim());
    const typeIdx = headers.indexOf('Type');
    const type2Idx = headers.indexOf('Type2');

    const suggestions = new Set();

    rows.slice(1).forEach(row => {
      if (typeIdx !== -1 && row[typeIdx]) suggestions.add(row[typeIdx].trim());
      if (type2Idx !== -1 && row[type2Idx]) suggestions.add(row[type2Idx].trim());
    });

    res.json(Array.from(suggestions));
  } catch (err) {
    console.error('❌ Error in /keywords:', err);
    res.status(500).json({ error: 'Server error while fetching keywords.' });
  }
});

app.listen(port, () => {
  console.log(`🚀 Server listening on port ${port}`);
});