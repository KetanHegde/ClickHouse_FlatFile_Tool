const express = require("express");
const fs = require("fs");
const path = require("path");
const { createObjectCsvWriter } = require("csv-writer");
const { ClickHouse } = require("clickhouse");

const router = express.Router();

// Connect to ClickHouse
function connectClickHouse({ host, port, username, token, database }) {
  return new ClickHouse({
    url: host,
    port: port || 8123,
    basicAuth: { username, password: token },
    format: "json",
    config: { database },
  });
}

// Utility: Build SELECT clause with aliases
const buildSelectClause = (columns) => {
  return columns
    .map(
      ({ table, column, alias }) => `\`${table}\`.\`${column}\` AS \`${alias}\``
    )
    .join(", ");
};

// Utility function to generate aliased columns
function generateAliasedColumns(columns, useSingleTableFormat = false) {
  return columns.map((col, index) => {
    if (!col.table || !col.column) {
      throw new Error(
        `Invalid column structure at index ${index}: ${JSON.stringify(col)}`
      );
    }
    return {
      table: col.table,
      column: col.column,
      // If useSingleTableFormat is true, use just the column name without the table prefix
      alias: useSingleTableFormat ? col.column : `${col.table}_${col.column}`,
    };
  });
}

// Query handler shared by preview and export
const handleClickHouseQuery = async (
  client,
  database,
  tables,
  columns,
  joinCondition,
  limit = null
) => {
  // Use single table format when there's only one table
  const useSingleTableFormat = tables.length === 1;
  const aliasedCols = generateAliasedColumns(columns, useSingleTableFormat);
  const columnList = buildSelectClause(aliasedCols);
  const mainTable = tables[0];

  let query = `SELECT ${columnList} FROM \`${database}\`.\`${mainTable}\``;

  if (tables.length > 1) {
    if (!joinCondition) {
      throw new Error("Join condition required for multiple tables.");
    }

    tables.slice(1).forEach((table) => {
      query += ` JOIN \`${database}\`.\`${table}\` ON ${joinCondition}`;
    });
  }

  if (limit) {
    query += ` LIMIT ${limit}`;
  }

  query += " FORMAT JSON";

  const result = await client.query(query).toPromise();
  return Array.isArray(result) ? result : [result];
};

// POST / - Export to CSV
router.post("/", async (req, res) => {
  try {
    const { config, tables, columns, joinCondition, delimiter } = req.body;
    console.log("Join Condition:", joinCondition);
    const { host, port, database, username, token } = config;

    if (!host || !database || !tables?.length || !columns?.length) {
      return res.status(400).json({ message: "Missing required fields" });
    }

    const client = connectClickHouse({ host, port, username, token, database });

    const resultData = await handleClickHouseQuery(
      client,
      database,
      tables,
      columns,
      joinCondition
    );

    // Use single table format when there's only one table
    const useSingleTableFormat = tables.length === 1;
    const aliasedCols = generateAliasedColumns(columns, useSingleTableFormat);

    const headers = aliasedCols.map(({ alias }) => ({
      id: alias,
      title: alias,
    }));

    const processedData = resultData.map((row) => {
      const mappedRow = {};
      aliasedCols.forEach(({ alias }) => {
        mappedRow[alias] = row[alias] ?? null;
      });
      return mappedRow;
    });

    const outputFileName = `clickhouse_export_${Date.now()}.csv`;
    const outputPath = path.join(__dirname, "../../../uploads", outputFileName);
    fs.mkdirSync(path.dirname(outputPath), { recursive: true });

    await createObjectCsvWriter({
      path: outputPath,
      header: headers,
      fieldDelimiter: delimiter || ",",
    }).writeRecords(processedData);

    return res.json({
      message: "Data exported successfully",
      filePath: `/downloads/${outputFileName}`,
      fileName: outputFileName,
      rowCount: processedData.length,
    });
  } catch (err) {
    console.error("Export Error:", err);
    return res.status(500).json({
      message: "Export failed",
      error: err.message || "Internal Server Error",
    });
  }
});

router.post("/preview", async (req, res) => {
  try {
    const { config, tables, columns, joinCondition } = req.body;
    const { host, port, database, username, token } = config;

    if (!host || !database || !tables?.length || !columns?.length) {
      return res.status(400).json({ message: "Missing required fields" });
    }

    const client = connectClickHouse({ host, port, username, token, database });

    // Use single table format when there's only one table
    const useSingleTableFormat = tables.length === 1;

    // Important: Generate aliased columns first so the same aliases are used in the query and the result processing
    const aliasedCols = generateAliasedColumns(columns, useSingleTableFormat);

    // Now let's construct and execute the query directly instead of using handleClickHouseQuery

    const columnList = buildSelectClause(aliasedCols);
    const mainTable = tables[0];

    let query = `SELECT ${columnList} FROM \`${database}\`.\`${mainTable}\``;

    if (tables.length > 1) {
      // First join needs to only reference the main table
      query += ` JOIN \`${database}\`.\`${tables[1]}\` ON Employee.id = ${tables[1]}.id`;

      // For subsequent joins, can reference previous tables
      for (let i = 2; i < tables.length; i++) {
        const table = tables[i];
        query += ` JOIN \`${database}\`.\`${table}\` ON ${mainTable}.id = ${table}.id`;
      }
    }

    query += ` LIMIT 100 FORMAT JSON`;
    console.log("Preview Query:", query); // Log the query for debugging

    const resultData = await client.query(query).toPromise();
    const results = Array.isArray(resultData) ? resultData : [resultData];

    const processedData = results.map((row) => {
      const mappedRow = {};
      aliasedCols.forEach(({ alias }) => {
        mappedRow[alias] = row[alias] ?? null;
      });
      return mappedRow;
    });

    return res.json({
      message: "Preview generated successfully",
      preview: processedData,
      rowCount: processedData.length,
    });
  } catch (err) {
    console.error("Preview Error:", err);
    return res.status(500).json({
      message: "Preview failed",
      error: err.message || "Internal Server Error",
    });
  }
});

module.exports = router;
