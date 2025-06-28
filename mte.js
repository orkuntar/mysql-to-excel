const { google } = require('googleapis');
const { GoogleAuth } = require('google-auth-library');
const sql = require('mssql');
require('dotenv').config();

const SHEET_ID = process.env.SHEET_ID;
const SHEET_NAME = 'Sayfa1';
const SHEET_RANGE = `${SHEET_NAME}!A1`;

// MSSQL bağlantı konfigürasyonu
const sqlConfig = {
  user: process.env.DB_USER,
  password: process.env.DB_PASS,
  server: process.env.DB_SERVER,
  database: process.env.DB_NAME,
  options: {
    encrypt: true,
    trustServerCertificate: true,
  },
  connectionTimeout: 160000,
  requestTimeout: 160000,
};

// 🔄 Takip edilen gün
let currentDay = new Date().getDate();

// 📅 Bugünün 00:00 UTC timestamp'inden itibaren verileri çeken SQL
function buildQuery() {
  return `
    
  `;
}

// Retry mekanizması
async function retryOperation(operation, maxRetries = 5) {
  let retries = 0;
  while (retries < maxRetries) {
    try {
      return await operation();
    } catch (error) {
      console.warn(`⚠️ Hata: ${error.message}`);
      retries++;
      const delay = Math.pow(2, retries) * 1000;
      console.log(`🔁 Yeniden deneniyor ${delay / 1000}sn sonra... (${retries}/${maxRetries})`);
      await new Promise(res => setTimeout(res, delay));
    }
  }
  throw new Error('🚫 Maksimum tekrar hakkı aşıldı.');
}

// Mevcut chat_id'leri al
async function getExistingChatIds(sheets) {
  return retryOperation(async () => {
    const res = await sheets.spreadsheets.values.get({
      spreadsheetId: SHEET_ID,
      range: `${SHEET_NAME}!C2:C10000`,
    });
    return new Set(res.data.values ? res.data.values.flat() : []);
  });
}

// Başlık kontrolü
async function ensureSheetSetup(sheets) {
  const headers = ['created_at', 'email', 'chat_id', 'Puan', 'Yorum', 'Reason'];
  return retryOperation(async () => {
    const res = await sheets.spreadsheets.values.get({
      spreadsheetId: SHEET_ID,
      range: `${SHEET_NAME}!A1:F1`,
    });

    if (!res.data.values || res.data.values.length === 0) {
      await sheets.spreadsheets.values.update({
        spreadsheetId: SHEET_ID,
        range: SHEET_RANGE,
        valueInputOption: 'RAW',
        resource: { values: [headers] },
      });
      console.log('📋 Başlıklar oluşturuldu.');
    }
  });
}

// Yeni satırları yaz
async function insertRowsAndWrite(sheets, rowsData) {
  if (rowsData.length === 0) return;

  return retryOperation(async () => {
    // Yeni satırlar için alan aç (başa değil, en alta yazılabilir istenirse)
    await sheets.spreadsheets.batchUpdate({
      spreadsheetId: SHEET_ID,
      resource: {
        requests: [{
          insertDimension: {
            range: {
              sheetId: 0,
              dimension: 'ROWS',
              startIndex: 1,
              endIndex: 1 + rowsData.length,
            },
            inheritFromBefore: false,
          },
        }],
      },
    });

    // Satırları yaz
    await sheets.spreadsheets.values.update({
      spreadsheetId: SHEET_ID,
      range: `${SHEET_NAME}!A2:F${1 + rowsData.length}`,
      valueInputOption: 'USER_ENTERED',
      resource: { values: rowsData },
    });

    console.log(`✅ ${rowsData.length} satır Google Sheets'e eklendi.`);
    rowsData.forEach(row => {
      console.log(`📅 ${row[0]} | 🧑 ${row[1]} | 💬 ${row[5]} | 🆔 ${row[2]}`);
    });
  });
}

// Ana görev
async function runJob() {
  console.log(`[${new Date().toLocaleString('tr-TR')}] ⏳ Görev başladı...`);

  try {
    const auth = new GoogleAuth({
      keyFile: 'credentials.json',
      scopes: ['https://www.googleapis.com/auth/spreadsheets'],
    });
    const sheets = google.sheets({ version: 'v4', auth: await auth.getClient() });

    await ensureSheetSetup(sheets);

    // Yeni güne geçildiyse Sheet1 temizlenir
    const today = new Date().getDate();
    if (today !== currentDay) {
      console.log('📆 Yeni gün algılandı. Sheet1 temizleniyor...');
      await sheets.spreadsheets.values.clear({
        spreadsheetId: SHEET_ID,
        range: `${SHEET_NAME}!A2:F1000000`,
      });
      currentDay = today;
    }

    await sql.connect(sqlConfig);

    const result = await sql.query(buildQuery());
    const rows = result.recordset;
    const existingChatIds = await getExistingChatIds(sheets);

    const newRows = [];
    for (const row of rows) {
      if (!existingChatIds.has(row.chat_id)) {
        const rowData = [
          row.created_at.slice(0, 16),
          row.email || '',
          row.chat_id || '',
          row.Puan || '',
          row.Yorum || '',
          row.Reason || ''
        ];
        newRows.push(rowData);
        existingChatIds.add(row.chat_id);
      }
    }

    await insertRowsAndWrite(sheets, newRows);
    console.log(`📦 Toplam ${newRows.length} yeni veri işlendi.`);
  } catch (err) {
    console.error('❌ runJob hatası:', err.message);
  } finally {
    sql.close();
  }
}

// Döngü başlatıcı
async function startLoop() {
  await runJob();
  setInterval(runJob, 2 * 60 * 1000); // Her 2 dakikada bir çalışır
}

startLoop().catch(console.error);
