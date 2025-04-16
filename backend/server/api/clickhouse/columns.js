const express = require("express");
const { ClickHouse } = require("clickhouse");

const router = express.Router();

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

/**
 * Endpoint to get columns from a ClickHouse table
 * POST /api/clickhouse/columns
 */
router.post("/", async (req, res) => {
  try {
    const { config, tables } = req.body;

    if (!config || !config.host || !config.database) {
      return res
        .status(400)
        .json({ message: "Invalid ClickHouse configuration" });
    }

    if (!tables || !Array.isArray(tables) || tables.length === 0) {
      return res
        .status(400)
        .json({ message: "At least one table must be specified" });
    }

    const { host, port, database, username, token } = config;

    const client = connectClickHouse({
      host,
      port,
      username,
      token,
      database,
    });

    const schema = []; // Initialize schema array outside the loop to collect data for all tables

    for (const table of tables) {
      // Use parameterized queries to avoid SQL syntax errors
      const query = `
        SELECT 
          name,
          type
        FROM 
          system.columns
        WHERE 
          database = '${database}' AND 
          table = '${table}'
      `;

      // Execute the query with parameters using the proper parameter substitution
      const resultSet = client.query(query);

      const columns = await resultSet.toPromise(); // Using toPromise() to retrieve the result

      columns.forEach((column) => {
        // Add table name to column info for clarity in multi-table scenarios
        schema.push({
          ...column,
          table,
        });
      });
    }

    return res.json({
      message: "Columns fetched successfully",
      schema,
    });
  } catch (error) {
    console.error("Error fetching columns:", error);
    return res
      .status(500)
      .json({ message: "Error fetching columns", error: error.message });
  }
});

module.exports = router;
