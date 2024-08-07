const express = require('express');
const bodyParser = require('body-parser');
const cors = require('cors');
const multer = require('multer');
const mysql = require('mysql');
const path = require('path');
const fs = require('fs');
const csv = require('csv-parser');

const app = express();
const port = 5000;

// Middleware setup
app.use(cors());
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: true }));

// MySQL database connection setup
const db = mysql.createConnection({
  host: 'localhost',
  user: 'root',
  password: 'Rohitha@2004',
  database: 'barclays',
});

// Connect to MySQL
db.connect((err) => {
  if (err) {
    console.error('Database connection error:', err);
    throw err;
  }
  console.log('MySQL connected...');
});

// Multer setup for file upload
const storage = multer.diskStorage({
  destination: function (req, file, cb) {
    const uploadPath = path.join(__dirname, 'uploads');
    if (!fs.existsSync(uploadPath)) {
      fs.mkdirSync(uploadPath);
    }
    cb(null, uploadPath);
  },
  filename: function (req, file, cb) {
    cb(null, file.originalname);
  },
});
const upload = multer({ storage: storage });

// Endpoint for handling file uploads
app.post('/api/uploadFile', upload.single('file'), (req, res) => {
  try {
    const file = req.file;
    const { table, actionType } = req.body;

    if (!file) {
      return res.status(400).json({ message: 'No file uploaded.' });
    }
    if (!table) {
      return res.status(400).json({ message: 'No table selected.' });
    }
    if (!actionType || (actionType !== 'append' && actionType !== 'truncate_insert')) {
      return res.status(400).json({ message: 'Invalid action type specified.' });
    }

    const filePath = path.join(__dirname, 'uploads', file.originalname);
    const results = [];
    let headers = [];

    // Read CSV file
    fs.createReadStream(filePath)
      .pipe(csv())
      .on('headers', (headerList) => {
        headers = headerList;
      })
      .on('data', (data) => {
        results.push(data);
      })
      .on('end', () => {
        db.beginTransaction((err) => {
          if (err) {
            return res.status(500).json({ message: 'Failed to begin database transaction.', error: err.message });
          }

          if (actionType === 'truncate_insert') {
            const truncateSql = `TRUNCATE TABLE ${table}`;
            db.query(truncateSql, (err, result) => {
              if (err) {
                return db.rollback(() => {
                  res.status(500).json({ message: 'Failed to truncate table.', error: err.message });
                });
              }
              insertData(); // After truncating, insert new data
            });
          } else {
            insertData(); // If actionType is 'append', simply insert data
          }
        });
      });

    // Insert data into the table
    function insertData() {
      // Construct SQL insertion query
      const insertValues = results.map(row => {
        const values = headers.map(header => {
          const value = row[header];
          return value !== undefined && value !== null && value !== '' ? `'${value.replace(/'/g, "''")}'` : 'NULL';
        });
        return `(${values.join(',')})`;
      }).join(',');

      const insertSql = `INSERT INTO ${table} VALUES ${insertValues}`;

      // Execute insertion query
      db.query(insertSql, (err, result) => {
        if (err) {
          return db.rollback(() => {
            res.status(500).json({ message: 'Failed to insert data into table.', error: err.message });
          });
        }

        db.commit((err) => {
          if (err) {
            return db.rollback(() => {
              res.status(500).json({ message: 'Failed to commit transaction.', error: err.message });
            });
          }

          res.status(200).json({ message: 'File uploaded and data inserted successfully.' });
        });
      });
    }

  } catch (err) {
    res.status(500).json({ message: 'Failed to upload file.', error: err.message });
  }
});

app.listen(port, () => {
  console.log(`Server running on http://localhost:${port}`);
});
