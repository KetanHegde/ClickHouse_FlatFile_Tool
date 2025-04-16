const express = require("express");
const cors = require("cors");
const path = require("path");
const fs = require("fs");

// Import route handlers
const flatfileSchemaRouter = require("./api/flatfile/schema");
const flatfilePreviewRouter = require("./api/flatfile/preview");
const flatfileImportRouter = require("./api/flatfile/import");
const clickhouseColumnsRouter = require("./api/clickhouse/columns");
const clickhouseExportRouter = require("./api/clickhouse/export");

require("dotenv").config();

// Create Express app
const app = express();
const PORT = process.env.PORT || 3001;

// Create uploads directory if it doesn't exist
const uploadsDir = path.join(__dirname, "../uploads");
if (!fs.existsSync(uploadsDir)) {
  fs.mkdirSync(uploadsDir, { recursive: true });
}

// Middleware
app.use(cors());
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// Static files for downloads
app.use("/downloads", express.static(path.join(__dirname, "../uploads")));

// API routes
app.use("/api/flatfile/schema", flatfileSchemaRouter);
app.use("/api/flatfile/preview", flatfilePreviewRouter);
app.use("/api/flatfile/import", flatfileImportRouter);
app.use("/api/clickhouse/columns", clickhouseColumnsRouter);
app.use("/api/clickhouse/export", clickhouseExportRouter);

const { ClickHouse } = require("clickhouse");

function connectClickHouse({ host, port, username, token, database }) {
  return new ClickHouse({
    url: host,
    port,
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

app.post("/api/clickhouse/connect", async (req, res) => {
  try {
    const { host, port, database, username, token } = req.body;

    if (!host || !database) {
      return res
        .status(400)
        .json({ message: "Host and database are required" });
    }

    const client = connectClickHouse({
      host,
      port,
      username,
      token,
      database,
    });

    const query = `
      SELECT name
      FROM system.tables
      WHERE database = '${database}'
    `;

    const result = await client.query(query).toPromise();
    const tables = result.map((row) => row.name);

    console.log(tables);

    res.json({ message: "Connected to ClickHouse successfully", tables });
  } catch (error) {
    console.error("Error connecting to ClickHouse:", error);
    return res.status(500).json({
      message: "Error connecting to ClickHouse",
      error: error.message,
    });
  }
});

// Start server
app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
