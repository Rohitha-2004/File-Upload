const express = require('express');
const bodyParser = require('body-parser');
const cors = require('cors');
const multer = require('multer');
const mysql = require('mysql');
const path = require('path');
const fs = require('fs');
const csv = require('csv-parser');

// Create Express app
const app = express();
const port = 5000;

// Middleware setup
app.use(cors());
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: true }));

// MySQL database connection setup
const db = mysql.createConnection({
  host: 'localhost',
  user: 'root', // Replace with your MySQL username
  password: 'Rohitha@2004', // Replace with your MySQL password
  database: 'barclays', // Replace with your MySQL database name
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
    cb(null, uploadPath); // Save uploaded files to the 'uploads' directory
  },
  filename: function (req, file, cb) {
    cb(null, file.originalname); // Keep the original file name
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

    // Parse CSV file and insert or truncate/insert data into database based on actionType
    const filePath = path.join(__dirname, 'uploads', file.originalname);
    const results = [];
    let headers = [];

    fs.createReadStream(filePath)
      .pipe(csv())
      .on('headers', (headerList) => {
        headers = headerList; // Store headers to use for dynamic SQL column creation
      })
      .on('data', (data) => results.push(data))
      .on('end', () => {
        // Begin transaction
        db.beginTransaction((err) => {
          if (err) {
            console.error('Error beginning transaction:', err);
            return res.status(500).json({ message: 'Failed to begin database transaction.', error: err.message });
          }

          // Truncate table if actionType is 'truncate_insert'
          if (actionType === 'truncate_insert') {
            const truncateSql = `TRUNCATE TABLE ${table}`;
            db.query(truncateSql, (err) => {
              if (err) {
                return db.rollback(() => {
                  console.error('Error truncating table:', err);
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

    function insertData() {
      // Construct dynamic SQL query based on CSV headers
      const columns = headers.join(', ');
      const placeholders = headers.map(() => '?').join(', ');
      const insertSql = `INSERT INTO ${table} (${columns}) VALUES ?`;

      // Map data to match the column structure
      const values = results.map(row => headers.map(header => row[header] || null));

      // Insert all rows in one query
      db.query(insertSql, [values], (err) => {
        if (err) {
          return db.rollback(() => {
            console.error('Error inserting data:', err);
            res.status(500).json({ message: 'Failed to insert data into table.', error: err.message });
          });
        }

        db.commit((err) => {
          if (err) {
            return db.rollback(() => {
              console.error('Error committing transaction:', err);
              res.status(500).json({ message: 'Failed to commit transaction.', error: err.message });
            });
          }

          console.log('Transaction committed successfully.');
          res.status(200).json({ message: 'File uploaded and data inserted successfully.' });
        });
      });
    }

  } catch (err) {
    console.error('Error uploading file:', err);
    res.status(500).json({ message: 'Failed to upload file.', error: err.message });
  }
});

// Start the server
app.listen(port, () => {
  console.log(`Server running on http://localhost:${port}`);
});


