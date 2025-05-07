const express = require('express');
const { google } = require('googleapis');
const cors = require('cors');
require('dotenv').config();

const app = express();
const port = process.env.PORT || 3000;

const path = '/etc/secrets/credentials.json'; // путь к ключу на Render
const spreadsheetId = '1GIl15j9L1-KPyn2evruz3F0sscNo308mAC7huXm0WkY';
const sheetName = 'DataBaseCollty_Teams';

app.use(cors());
app.use(express.json());

// Проверка, что сервер жив
app.get('/', (req, res) => {
  res.send('✅ Server is running and connected to Google Sheets!');
});

// Получение заказов с фильтрацией
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
      range: `${sheetName}!A1:Z1000`,
    });

    const rows = response.data.values;
    if (!rows || rows.length === 0) return res.status(404).send('❌ No data found');

    const headers = rows[0];
    const data = rows.slice(1).map((row) =>
      headers.reduce((obj, key, i) => {
        obj[key] = row[i] || '';
        return obj;
      }, {})
    );

    const typeQuery = (req.query.type || '').toLowerCase().trim();

    const filtered = data.filter((row) => {
      if (!typeQuery) return true;
      const type1 = (row.Type || '').toLowerCase();
      const type2 = (row.Type2 || '').toLowerCase();
      return type1.includes(typeQuery) || type2.includes(typeQuery);
    });

    // ⬇️ Ключевой момент — оборачиваем в { properties: ... }
    const wrapped = filtered.map(item => ({ properties: item }));
    res.json(wrapped);
  } catch (error) {
    console.error('❌ Error in /orders:', error);
    res.status(500).send('Error retrieving orders');
  }
});

app.listen(port, () => {
  console.log(`🚀 Server running on port ${port}`);
});