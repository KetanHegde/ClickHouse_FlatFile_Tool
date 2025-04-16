const express = require("express");
const multer = require("multer");
const fs = require("fs");
const path = require("path");

const router = express.Router();
const upload = multer({ dest: "uploads/" });

router.post("/", upload.single("file"), async (req, res) => {
  try {
    if (!req.file) {
      return res.status(400).json({ message: "No file uploaded" });
    }

    const filePath = req.file.path;
    const delimiter = req.body.delimiter || ",";
    const hasHeader = req.body.hasHeader === "true";

    // Read the file content
    const fileContent = fs.readFileSync(filePath, 'utf8');
    const lines = fileContent.trim().split('\n');
    
    if (lines.length === 0) {
      return res.status(400).json({ message: "File is empty" });
    }
    
    // Process the first line to get headers
    const firstLine = lines[0];
    const headers = firstLine.split(delimiter).map(header => header.trim());
    
    // If file has headers, use them; otherwise generate column names
    let columns = [];
    
    if (hasHeader) {
      columns = headers;
    } else {
      // Generate column names (column1, column2, etc.)
      columns = headers.map((_, index) => `column${index + 1}`);
    }
    
    // Parse sample data
    const maxRowsToScan = 100;
    const results = [];
    
    const startIndex = hasHeader ? 1 : 0;
    const endIndex = Math.min(lines.length, startIndex + maxRowsToScan);
    
    for (let i = startIndex; i < endIndex; i++) {
      const values = lines[i].split(delimiter).map(val => val.trim());
      const row = {};
      
      columns.forEach((column, index) => {
        if (index < values.length) {
          row[column] = values[index];
        } else {
          row[column] = '';
        }
      });
      
      results.push(row);
    }
    
    console.log("Detected columns:", columns);
    console.log("Sample row:", results[0]);

    return res.json({
      message: "File schema analyzed successfully",
      columns,
      sampleCount: results.length,
    });
  } catch (error) {
    console.error("Error analyzing file schema:", error);
    return res
      .status(500)
      .json({ message: "Error analyzing file schema", error: error.message });
  }
});

module.exports = router;