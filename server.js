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

app.use(cors());
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: true }));

const db = mysql.createConnection({
  host: 'localhost',
  user: 'root',
  password: 'Rohitha@2004',
  database: 'barclays',
});

db.connect((err) => {
  if (err) {
    console.error('Database connection error:', err);
    throw err;
  }
  console.log('MySQL connected...');
});

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

const convertValue = (value, columnType) => {
  if (value === undefined || value === null || value === '') {
    return 'NULL';
  }

  let convertedValue = value;

  if (columnType.includes('int') || columnType.includes('bigint')) {
    convertedValue = parseInt(value, 10);
    return isNaN(convertedValue) ? 'NULL' : convertedValue;
  } else if (columnType.includes('float') || columnType.includes('double')) {
    convertedValue = parseFloat(value);
    return isNaN(convertedValue) ? 'NULL' : convertedValue;
  } else if (columnType.includes('date') || columnType.includes('datetime')) {
    const date = new Date(value);
    return isNaN(date.getTime()) ? 'NULL' : `'${date.toISOString().slice(0, 19).replace('T', ' ')}'`;
  }

  return `'${value.replace(/'/g, "''")}'`;
};

const getTableColumns = (tableName, callback) => {
  const query = `DESCRIBE ${tableName}`;
  db.query(query, (err, results) => {
    if (err) {
      console.error('Error fetching table columns:', err);
      return callback(err);
    }
    callback(null, results);
  });
};

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
              insertData();
            });
          } else {
            insertData();
          }
        });
      });

    function insertData() {
      getTableColumns(table, (err, columns) => {
        if (err) {
          return db.rollback(() => {
            res.status(500).json({ message: 'Failed to get table columns.', error: err.message });
          });
        }

        const validColumns = columns.map(col => col.Field);
        const validHeaders = headers.filter(header => validColumns.includes(header));

        const insertValues = results.map(row => {
          const values = validHeaders.map(header => {
            const columnType = columns.find(col => col.Field === header).Type;
            const value = convertValue(row[header], columnType);
            return value;
          });
          return `(${values.join(',')})`;
        }).join(',');

        const insertSql = `INSERT INTO ${table} (${validHeaders.join(',')}) VALUES ${insertValues}`;

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
      });
    }

  } catch (err) {
    res.status(500).json({ message: 'Failed to upload file.', error: err.message });
  }
});

app.listen(port, () => {
  console.log(`Server running on http://localhost:${port}`);
});


