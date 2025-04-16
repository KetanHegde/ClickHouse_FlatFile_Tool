const express = require("express");
const multer = require("multer");
const csv = require("csv-parser");
const fs = require("fs");
const path = require("path");

const router = express.Router();
const upload = multer({ dest: "uploads/" });

const { ClickHouse } = require("clickhouse");

function connectClickHouse({ host, port, username, token, database }) {
  return new ClickHouse({
    url: host,
    port: port,
    basicAuth: {
      username: username,
      password: token,
    },
    format: "json",
    config: {
      database: database,
    },
  });
}

router.post("/", upload.single("file"), async (req, res) => {
  try {
    if (!req.file) {
      return res.status(400).json({ message: "No file uploaded" });
    }

    const filePath = req.file.path;
    const delimiter = req.body.delimiter || ",";
    const hasHeader = req.body.hasHeader === "true";

    let selectedColumns = [];
    let clickhouseConfig = {};
    let targetTable = "";

    try {
      selectedColumns = JSON.parse(req.body.columns || "[]");
      clickhouseConfig = JSON.parse(req.body.clickhouseConfig || "{}");
      targetTable = req.body.targetTable;
    } catch (e) {
      console.error("Error parsing request parameters:", e);
      return res.status(400).json({ message: "Invalid request parameters" });
    }

    if (!targetTable) {
      return res.status(400).json({ message: "Target table name is required" });
    }

    if (selectedColumns.length === 0) {
      return res.status(400).json({ message: "At least one column must be selected" });
    }

    const { host, port, database, username, token } = clickhouseConfig;

    if (!host || !database) {
      return res.status(400).json({ message: "ClickHouse connection details are incomplete" });
    }

    const client = connectClickHouse({ host, port, username, token, database });

    // Debug: Log raw CSV content
    const rawContent = fs.readFileSync(filePath, 'utf8').split('\n').slice(0, 5).join('\n');
    console.log("Raw CSV content (first 5 lines):", rawContent);

    const records = [];
    const normalizedColumns = selectedColumns.map(col => col.trim().toLowerCase());

    // FIX: Read the CSV directly without relying on the csv-parser's header handling
    const rawLines = fs.readFileSync(filePath, 'utf8').trim().split('\n');
    let dataLines = rawLines;
    let headers = normalizedColumns;

    // If CSV has headers, use first line as headers
    if (hasHeader && rawLines.length > 0) {
      headers = rawLines[0].split(delimiter).map(h => h.trim().toLowerCase());
      dataLines = rawLines.slice(1);
    }

    console.log("Using headers:", headers);
    console.log("Target columns:", normalizedColumns);

    // Parse each line manually
    dataLines.forEach(line => {
      if (!line.trim()) return; // Skip empty lines
      
      const values = line.split(delimiter);
      const record = {};
      
      normalizedColumns.forEach((column, index) => {
        // Find the index of this column in the headers array
        const headerIndex = headers.indexOf(column);
        
        if (headerIndex !== -1 && headerIndex < values.length) {
          record[column] = values[headerIndex].trim();
        } else if (index < values.length) {
          // If column not found in headers but we have a value at this position
          record[column] = values[index].trim();
        } else {
          record[column] = '';
        }
      });
      
      records.push(record);
    });

    if (records.length === 0) {
      return res.status(400).json({ message: "No data found in file" });
    }

    console.log("Parsed records preview:", records.slice(0, 3));
    console.log("Total parsed records:", records.length);

    // Infer column types from data
    const columnTypes = {};
    Object.entries(records[0]).forEach(([column, value]) => {
      if (!isNaN(Number(value)) && value !== '') {
        columnTypes[column] = "Float64";
      } else if (value === "true" || value === "false") {
        columnTypes[column] = "UInt8";
      } else if (!isNaN(Date.parse(value)) && value !== '') {
        columnTypes[column] = "DateTime";
      } else {
        columnTypes[column] = "String";
      }
    });

    console.log("Inferred column types:", columnTypes);

    // Create table if not exists
    const createTableColumns = normalizedColumns
      .map((column) => `\"${column}\" ${columnTypes[column]}`)
      .join(", ");

    const createTableQuery = `
      CREATE TABLE IF NOT EXISTS \"${database}\".\"${targetTable}\" (
        ${createTableColumns}
      ) ENGINE = MergeTree() ORDER BY tuple()
    `;

    console.log("Create table query:", createTableQuery);

    try {
      await client.query(createTableQuery).toPromise();
    } catch (tableError) {
      return res.status(500).json({
        message: "Error creating table in ClickHouse",
        error: tableError.message,
      });
    }

    // Use direct SQL INSERT
    try {
      // Prepare values for SQL INSERT
      const values = records.map(record => {
        const rowValues = normalizedColumns.map(col => {
          const val = record[col];
          // Escape single quotes and handle different data types
          if (val === null || val === undefined || val === '') {
            return "''"; // Empty string for null/undefined/empty values
          } else if (columnTypes[col] === "Float64" || columnTypes[col] === "UInt8") {
            return val; // Numbers don't need quotes
          } else {
            return `'${val.toString().replace(/'/g, "''")}'`; // Escape quotes for strings
          }
        }).join(', ');
        return `(${rowValues})`;
      }).join(',\n');

      // Build and execute the INSERT query
      const insertQuery = `
        INSERT INTO \"${database}\".\"${targetTable}\" (${normalizedColumns.map(col => `\"${col}\"`).join(', ')})
        VALUES ${values}
      `;

      console.log("Executing insert query with first few rows:", insertQuery.split('\n').slice(0, 5).join('\n') + '...');
      
      await client.query(insertQuery).toPromise();
      
      console.log("Insert completed successfully");
    } catch (insertError) {
      return res.status(500).json({
        message: "Error inserting data to ClickHouse",
        error: insertError.message
      });
    }

    // Clean up
    try {
      fs.unlinkSync(filePath);
    } catch (fileError) {
      console.warn("Warning: Could not delete temp file", fileError.message);
    }

    return res.json({
      message: `Data imported successfully to table ${database}.${targetTable}`,
      rowCount: records.length,
    });
  } catch (error) {
    console.error("Error importing data to ClickHouse:", error);
    return res.status(500).json({
      message: "Error importing data to ClickHouse",
      error: error.message,
    });
  }
});

module.exports = router;