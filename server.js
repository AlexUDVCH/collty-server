const express = require('express');
const { google } = require('googleapis');
const app = express();
const port = process.env.PORT || 3000;

const SHEET_ID = '1GIl15j9L1-KPyn2evruz3F0sscNo308mAC7huXm0WkY'; // ← Твой ID таблицы
const SHEET_NAME = 'DataBaseCollty_Teams'; // ← Имя листа в таблице

app.use(express.json());

app.get('/', (req, res) => {
  res.send('✅ Server is running and connected to Google Sheets!');
});

app.get('/data', async (req, res) => {
  try {
    const auth = new google.auth.GoogleAuth({
      keyFile: '/etc/secrets/credentials.json', // ← путь к файлу-секрету
      scopes: ['https://www.googleapis.com/auth/spreadsheets.readonly'],
    });

    const sheets = google.sheets({ version: 'v4', auth: await auth.getClient() });

    const response = await sheets.spreadsheets.values.get({
      spreadsheetId: SHEET_ID,
      range: SHEET_NAME,
    });

    const [header, ...rows] = response.data.values;
    const data = rows.map(row =>
      header.reduce((acc, key, i) => {
        acc[key] = row[i] || '';
        return acc;
      }, {})
    );

    res.json(data);
  } catch (error) {
    console.error('❌ Error:', error);
    res.status(500).send('Error retrieving data from Google Sheets');
  }
});

app.listen(port, () => {
  console.log(`🚀 Server listening on port ${port}`);
});
