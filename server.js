const express = require('express');
const { google } = require('googleapis');
const app = express();
const port = process.env.PORT || 3000;

const SPREADSHEET_ID = process.env.SPREADSHEET_ID;

app.get('/', async (req, res) => {
  res.send('✅ Google Sheets API connected!');
});

app.get('/orders', async (req, res) => {
  try {
    const auth = new google.auth.GoogleAuth({
      keyFile: 'credentials.json',
      scopes: ['https://www.googleapis.com/auth/spreadsheets.readonly'],
    });

    const client = await auth.getClient();
    const sheets = google.sheets({ version: 'v4', auth: client });

    const response = await sheets.spreadsheets.values.get({
      spreadsheetId: SPREADSHEET_ID,
      range: 'Sheet1!A1:Z1000',
    });

    res.json(response.data.values);
  } catch (err) {
    console.error('Error reading from Google Sheets:', err);
    res.status(500).send('Failed to fetch data');
  }
});

app.listen(port, () => {
  console.log(`🚀 Server listening on port ${port}`);
});
