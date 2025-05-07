const express = require('express');
const { google } = require('googleapis');
const fs = require('fs');
const app = express();
const port = process.env.PORT || 3000;

app.use(express.json());

async function accessSheet() {
  const auth = new google.auth.GoogleAuth({
    keyFile: 'credentials.json',
    scopes: ['https://www.googleapis.com/auth/spreadsheets.readonly'],
  });

  const client = await auth.getClient();
  const sheets = google.sheets({ version: 'v4', auth: client });

  const spreadsheetId = '1GIl15j9L1-KPyn2evruz3F0sscNo308mAC7huXm0WkY'; // твой ID таблицы
  const range = 'DataBaseCollty_Teams!A1:Z1000'; // диапазон данных

  const response = await sheets.spreadsheets.values.get({
    spreadsheetId,
    range,
  });

  return response.data.values;
}

app.get('/', (req, res) => {
  res.send('✅ Server is running and connected to Google Sheets!');
});

app.get('/orders', async (req, res) => {
  try {
    const data = await accessSheet();

    const [headers, ...rows] = data;
    const json = rows.map(row => {
      const obj = {};
      headers.forEach((header, i) => {
        obj[header] = row[i] || '';
      });
      return obj;
    });

    res.json(json);
  } catch (error) {
    console.error('❌ Error fetching data:', error);
    res.status(500).send('Error retrieving data from Google Sheets');
  }
});

app.listen(port, () => {
  console.log(`🚀 Server listening on port ${port}`);
});
