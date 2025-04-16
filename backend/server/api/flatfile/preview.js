const express = require("express");
const multer = require("multer");
const fs = require("fs");
const path = require("path");

const router = express.Router();
const upload = multer({ dest: "uploads/" });

/**
 * Endpoint to preview data from a flat file
 * POST /api/flatfile/preview
 */
router.post("/", upload.single("file"), async (req, res) => {
  try {
    if (!req.file) {
      return res.status(400).json({ message: "No file uploaded" });
    }

    const filePath = req.file.path;
    const delimiter = req.body.delimiter || ",";
    const hasHeader = req.body.hasHeader === "true";

    console.log("Processing file with delimiter:", delimiter);
    console.log("Has header:", hasHeader);

    // Parse selected columns
    let selectedColumns = [];
    try {
      selectedColumns = req.body.columns ? JSON.parse(req.body.columns) : [];
      console.log("Parsed selected columns:", selectedColumns);
    } catch (e) {
      console.error("Error parsing columns:", e);
      return res.status(400).json({ message: "Invalid columns format" });
    }

    // Read the file content
    const fileContent = fs.readFileSync(filePath, 'utf8');
    const lines = fileContent.trim().split('\n');
    
    if (lines.length === 0) {
      return res.status(400).json({ message: "File is empty" });
    }

    console.log(`File has ${lines.length} lines`);
    
    // Process the first line to get headers
    const firstLine = lines[0];
    const headerValues = firstLine.split(delimiter).map(header => header.trim());
    
    console.log("Header values from first line:", headerValues);
    
    // Determine headers based on hasHeader flag
    let headers;
    let dataStartIndex;
    
    if (hasHeader) {
      headers = headerValues;
      dataStartIndex = 1;
    } else {
      // Generate column names (column1, column2, etc.)
      headers = headerValues.map((_, index) => `column${index + 1}`);
      dataStartIndex = 0;
    }
    
    console.log("Final headers:", headers);
    
    // Read first 100 rows for preview
    const results = [];
    const maxRows = 100;
    const endIndex = Math.min(lines.length, dataStartIndex + maxRows);
    
    for (let i = dataStartIndex; i < endIndex; i++) {
      const line = lines[i];
      if (!line || line.trim() === '') continue;
      
      const values = line.split(delimiter).map(val => val.trim());
      let row = {};
      
      // If we have selected columns, only include those
      if (selectedColumns.length > 0) {
        selectedColumns.forEach(column => {
          // Find the index of this column in the headers array
          const headerIndex = headers.indexOf(column);
          
          if (headerIndex !== -1 && headerIndex < values.length) {
            row[column] = values[headerIndex];
          } else {
            row[column] = '';
          }
        });
      } else {
        // Include all columns
        headers.forEach((header, index) => {
          if (index < values.length) {
            row[header] = values[index];
          } else {
            row[header] = '';
          }
        });
      }
      
      results.push(row);
    }
    
    console.log(`Processed ${results.length} rows`);
    console.log("Sample row:", results.length > 0 ? results[0] : "No rows");
    
    // Clean up
    try {
      fs.unlinkSync(filePath);
    } catch (e) {
      console.warn("Could not delete temporary file:", e);
    }

    // Create a more detailed response
    const response = {
      message: "File preview generated successfully",
      preview: results,
      headers: headers,
      rowCount: results.length,
      totalRows: lines.length - (hasHeader ? 1 : 0),
      selectedColumns: selectedColumns.length > 0 ? selectedColumns : headers
    };

    return res.json(response);
  } catch (error) {
    console.error("Error generating file preview:", error);
    return res
      .status(500)
      .json({ 
        message: "Error generating file preview", 
        error: error.message,
        stack: process.env.NODE_ENV === 'development' ? error.stack : undefined
      });
  }
});

module.exports = router;