"use client";

import { useState, useRef } from "react";
import axios from "axios";

export default function Home() {
  // Source selection state
  const [source, setSource] = useState("clickhouse");
  const [status, setStatus] = useState("");
  const [progress, setProgress] = useState(0);
  const [previewData, setPreviewData] = useState(null);
  const fileInputRef = useRef(null);

  // ClickHouse connection form
  const [clickhouseForm, setClickhouseForm] = useState({
    host: "",
    port: "",
    database: "",
    username: "",
    token: "",
  });

  // Flat file configuration
  const [flatFileForm, setFlatFileForm] = useState({
    delimiter: ",",
    hasHeader: true,
    fileName: "",
  });

  // Data selection states
  const [tables, setTables] = useState([]);
  const [selectedTables, setSelectedTables] = useState([]);
  const [joinCondition, setJoinCondition] = useState("");
  const [columns, setColumns] = useState([]);
  const [selectedColumns, setSelectedColumns] = useState([]);
  const [targetTable, setTargetTable] = useState("");

  // Handle source change
  const handleSourceChange = (newSource) => {
    setSource(newSource);
    resetSelections();
  };

  // Reset data selection states
  // Reset data selection states
  const resetSelections = () => {
    setTables([]);
    setSelectedTables([]);
    setJoinCondition("");
    setColumns([]);
    setSelectedColumns([]);
    setPreviewData(null);
    setStatus("");
    setProgress(0);
    setTargetTable("");

    // Reset file input if applicable
    if (fileInputRef.current) {
      fileInputRef.current.value = "";
    }
  };

  // Connect to ClickHouse
  const handleConnectClickhouse = async () => {
    try {
      setStatus("Connecting to ClickHouse...");
      const response = await axios.post(
        `${process.env.NEXT_PUBLIC_API_BASE}/api/clickhouse/connect`,
        clickhouseForm
      );
      setTables(response.data.tables || []);
      setStatus("Connected! Tables loaded.");
    } catch (err) {
      setStatus(
        "Connection failed: " + (err.response?.data?.message || err.message)
      );
    }
  };

  // Load columns for selected table
  // Load columns for selected table
  const handleLoadColumns = async () => {
    // Reset column-related states to avoid stale data
    setColumns([]);
    setSelectedColumns([]);
    setPreviewData(null);

    if (source === "clickhouse") {
      try {
        setStatus(
          `Loading columns for ${
            selectedTables.length > 1 ? "tables" : "table"
          } "${selectedTables.join(", ")}"...`
        );

        const response = await axios.post(
          `${process.env.NEXT_PUBLIC_API_BASE}/api/clickhouse/columns`,
          {
            config: clickhouseForm,
            tables: selectedTables,
          }
        );

        // Store the full schema information instead of just column names
        const schema = response.data.schema || [];
        setColumns(schema);
        setStatus("Columns loaded.");
      } catch (err) {
        setStatus(
          "Failed to load columns: " +
            (err.response?.data?.message || err.message)
        );
      }
    } else if (source === "flatfile") {
      // Rest of the flatfile handling code...
      if (!fileInputRef.current?.files?.[0]) {
        setStatus("Please select a file first");
        return;
      }

      const formData = new FormData();
      formData.append("file", fileInputRef.current.files[0]);
      formData.append("delimiter", flatFileForm.delimiter);
      formData.append("hasHeader", flatFileForm.hasHeader);

      try {
        setStatus("Analyzing file schema...");
        const response = await axios.post(
          `${process.env.NEXT_PUBLIC_API_BASE}/api/flatfile/schema`,
          formData
        );

        // Format flat file columns to match the structure from ClickHouse
        const fileColumns = response.data.columns || [];
        setColumns(
          fileColumns.map((col) => ({
            name: col,
            type: "unknown",
            table: "file",
          }))
        );

        setFlatFileForm((prev) => ({
          ...prev,
          fileName: fileInputRef.current.files[0].name,
        }));
        setStatus("File schema loaded.");
      } catch (err) {
        setStatus(
          "Failed to analyze file: " +
            (err.response?.data?.message || err.message)
        );
      }
    }
  };

  // Generate data preview
  // Generate data preview
  const handlePreview = async () => {
    if (selectedColumns.length === 0) {
      setStatus("Please select at least one column");
      return;
    }

    try {
      setStatus("Generating preview...");
      setPreviewData(null); // Clear previous preview data

      if (source === "clickhouse") {
        // Format the columns correctly as objects with table and column properties
        const formattedColumns = selectedColumns.map((col) => ({
          table: col.table,
          column: col.name,
        }));

        const response = await axios.post(
          `${process.env.NEXT_PUBLIC_API_BASE}/api/clickhouse/export/preview`,
          {
            config: clickhouseForm,
            tables: selectedTables,
            columns: formattedColumns,
            joinCondition:
              selectedTables.length > 1 ? joinCondition : undefined,
          }
        );

        // Make sure we're using the correct property names when displaying data
        setPreviewData(response.data.preview || []);
      } else if (source === "flatfile") {
        if (!fileInputRef.current?.files?.[0]) {
          setStatus("Please select a file first");
          return;
        }

        const formData = new FormData();
        formData.append("file", fileInputRef.current.files[0]);
        formData.append("delimiter", flatFileForm.delimiter);
        formData.append("hasHeader", flatFileForm.hasHeader);
        formData.append(
          "columns",
          JSON.stringify(selectedColumns.map((col) => col.name))
        );

        const response = await axios.post(
          `${process.env.NEXT_PUBLIC_API_BASE}/api/flatfile/preview`,
          formData
        );
        setPreviewData(response.data.preview || []);
      }

      setStatus("Preview generated.");
    } catch (err) {
      setStatus(
        "Failed to generate preview: " +
          (err.response?.data?.message || err.message)
      );
    }
  };
  // Start data ingestion process
  const handleStartIngestion = async () => {
    if (selectedColumns.length === 0) {
      setStatus("Please select at least one column");
      return;
    }

    try {
      setStatus("Starting ingestion...");
      setProgress(10);

      if (source === "clickhouse") {
        // Format the columns correctly
        const formattedColumns = selectedColumns.map((col) => ({
          table: col.table,
          column: col.name,
        }));

        const response = await axios.post(
          `${process.env.NEXT_PUBLIC_API_BASE}/api/clickhouse/export`,
          {
            config: clickhouseForm,
            tables: selectedTables,
            columns: formattedColumns,
            joinCondition:
              selectedTables.length > 1 ? joinCondition : undefined,
            fileName: flatFileForm.fileName || "export.csv",
            delimiter: flatFileForm.delimiter,
          },
          {
            onUploadProgress: (progressEvent) => {
              const percentCompleted = Math.round(
                (progressEvent.loaded * 100) / progressEvent.total
              );
              setProgress(Math.min(95, percentCompleted));
            },
          }
        );

        setProgress(100);
        setStatus(
          `✅ Exported ${response.data.rowCount} records to file ${response.data.fileName}.`
        );
      } else {
        // Flat File to ClickHouse
        if (!targetTable) {
          setStatus("Please enter a target table name");
          return;
        }

        const formData = new FormData();
        formData.append("file", fileInputRef.current.files[0]);
        formData.append("delimiter", flatFileForm.delimiter);
        formData.append("hasHeader", flatFileForm.hasHeader);
        formData.append(
          "columns",
          JSON.stringify(selectedColumns.map((col) => col.name))
        );
        formData.append("clickhouseConfig", JSON.stringify(clickhouseForm));
        formData.append("targetTable", targetTable);

        const response = await axios.post(
          `${process.env.NEXT_PUBLIC_API_BASE}/api/flatfile/import`,
          formData,
          {
            onUploadProgress: (progressEvent) => {
              const percentCompleted = Math.round(
                (progressEvent.loaded * 100) / progressEvent.total
              );
              setProgress(Math.min(95, percentCompleted));
            },
          }
        );
        setProgress(100);
        setStatus(
          `✅ Imported ${response.data.rowCount} records to ClickHouse table ${targetTable}.`
        );
      }
    } catch (err) {
      setProgress(0);
      setStatus(
        "Ingestion failed: " + (err.response?.data?.message || err.message)
      );
    }
  };

  // Handle column selection toggle
  const handleColumnToggle = (column) => {
    const columnId = `${column.table}-${column.name}`;

    setSelectedColumns((prev) => {
      const isSelected = prev.some(
        (col) => `${col.table}-${col.name}` === columnId
      );
      if (isSelected) {
        return prev.filter((col) => `${col.table}-${col.name}` !== columnId);
      } else {
        return [...prev, column];
      }
    });
  };

  // Handle table selection toggle for multi-table join (bonus feature)
  const handleTableToggle = (table) => {
    setSelectedTables((prev) =>
      prev.includes(table) ? prev.filter((t) => t !== table) : [...prev, table]
    );
  };

  // Handle select all columns
  const handleSelectAllColumns = () => {
    if (selectedColumns.length === columns.length) {
      setSelectedColumns([]);
    } else {
      setSelectedColumns([...columns]);
    }
  };

  // Group columns by table
  const columnsByTable = columns.reduce((acc, col) => {
    if (!acc[col.table]) {
      acc[col.table] = [];
    }
    acc[col.table].push(col);
    return acc;
  }, {});

  // Check if a column is selected
  const isColumnSelected = (column) => {
    return selectedColumns.some(
      (col) => col.name === column.name && col.table === column.table
    );
  };

  return (
    <div className="container mx-auto p-6 font-sans text-black">
      <h1 className="text-3xl font-bold mb-6 text-white">
        ClickHouse ↔ Flat File Ingestion Tool
      </h1>

      {/* Source Selection */}
      <div className="mb-6 p-4 bg-gray-100 rounded-lg">
        <h2 className="text-xl font-semibold mb-2">1. Select Data Source</h2>
        <div className="flex gap-4">
          <button
            className={`px-4 py-2 rounded ${
              source === "clickhouse" ? "bg-blue-500 text-white" : "bg-gray-200"
            }`}
            onClick={() => handleSourceChange("clickhouse")}
          >
            ClickHouse → Flat File
          </button>
          <button
            className={`px-4 py-2 rounded ${
              source === "flatfile" ? "bg-blue-500 text-white" : "bg-gray-200"
            }`}
            onClick={() => handleSourceChange("flatfile")}
          >
            Flat File → ClickHouse
          </button>
        </div>
      </div>

      {/* Source Configuration */}
      <div className="mb-6 p-4 bg-gray-100 rounded-lg">
        <h2 className="text-xl font-semibold mb-4">
          2. Configure{" "}
          {source === "clickhouse" ? "ClickHouse Source" : "Flat File Source"}
        </h2>

        {source === "clickhouse" && (
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4 mb-4">
            <input
              className="p-2 border rounded"
              placeholder="Host (e.g. http://localhost)"
              value={clickhouseForm.host}
              onChange={(e) =>
                setClickhouseForm({ ...clickhouseForm, host: e.target.value })
              }
            />
            <input
              className="p-2 border rounded"
              placeholder="Port (8123 for HTTP, 9440 for HTTPS)"
              value={clickhouseForm.port}
              onChange={(e) =>
                setClickhouseForm({ ...clickhouseForm, port: e.target.value })
              }
            />
            <input
              className="p-2 border rounded"
              placeholder="Database"
              value={clickhouseForm.database}
              onChange={(e) =>
                setClickhouseForm({
                  ...clickhouseForm,
                  database: e.target.value,
                })
              }
            />
            <input
              className="p-2 border rounded"
              placeholder="Username"
              value={clickhouseForm.username}
              onChange={(e) =>
                setClickhouseForm({
                  ...clickhouseForm,
                  username: e.target.value,
                })
              }
            />
            <input
              className="p-2 border rounded"
              type="password"
              placeholder="JWT Token / Password"
              value={clickhouseForm.token}
              onChange={(e) =>
                setClickhouseForm({ ...clickhouseForm, token: e.target.value })
              }
            />
            <button
              className="p-2 bg-blue-500 text-white rounded hover:bg-blue-600"
              onClick={handleConnectClickhouse}
            >
              Connect to ClickHouse
            </button>
          </div>
        )}

        {source === "flatfile" && (
          <div className="mb-4">
            <div className="mb-4">
              <label className="block mb-2">Select Flat File:</label>
              <input
                type="file"
                ref={fileInputRef}
                className="p-2 border rounded w-full"
                accept=".csv,.tsv,.txt"
              />
            </div>

            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              <div>
                <label className="block mb-1">Delimiter:</label>
                <select
                  className="p-2 border rounded w-full"
                  value={flatFileForm.delimiter}
                  onChange={(e) =>
                    setFlatFileForm({
                      ...flatFileForm,
                      delimiter: e.target.value,
                    })
                  }
                >
                  <option value=",">Comma (,)</option>
                  <option value="\t">Tab</option>
                  <option value="|">Pipe (|)</option>
                  <option value=";">Semicolon (;)</option>
                </select>
              </div>

              <div>
                <label className="block mb-1">File has header row:</label>
                <select
                  className="p-2 border rounded w-full"
                  value={flatFileForm.hasHeader.toString()}
                  onChange={(e) =>
                    setFlatFileForm({
                      ...flatFileForm,
                      hasHeader: e.target.value === "true",
                    })
                  }
                >
                  <option value="true">Yes</option>
                  <option value="false">No</option>
                </select>
              </div>
            </div>

            <div className="mt-4">
              <button
                className="p-2 bg-blue-500 text-white rounded hover:bg-blue-600"
                onClick={handleLoadColumns}
              >
                Analyze File Schema
              </button>
            </div>

            {/* ClickHouse target configuration for Flat File source */}
            {columns.length > 0 && (
              <div className="mt-6 border-t pt-4">
                <h3 className="text-lg font-semibold mb-2">
                  Configure ClickHouse Target:
                </h3>
                <div className="grid grid-cols-1 md:grid-cols-2 gap-4 mb-4">
                  <input
                    className="p-2 border rounded"
                    placeholder="Host (e.g. http://localhost)"
                    value={clickhouseForm.host}
                    onChange={(e) =>
                      setClickhouseForm({
                        ...clickhouseForm,
                        host: e.target.value,
                      })
                    }
                  />
                  <input
                    className="p-2 border rounded"
                    placeholder="Port (8123 for HTTP, 9440 for HTTPS)"
                    value={clickhouseForm.port}
                    onChange={(e) =>
                      setClickhouseForm({
                        ...clickhouseForm,
                        port: e.target.value,
                      })
                    }
                  />
                  <input
                    className="p-2 border rounded"
                    placeholder="Database"
                    value={clickhouseForm.database}
                    onChange={(e) =>
                      setClickhouseForm({
                        ...clickhouseForm,
                        database: e.target.value,
                      })
                    }
                  />
                  <input
                    className="p-2 border rounded"
                    placeholder="Username"
                    value={clickhouseForm.username}
                    onChange={(e) =>
                      setClickhouseForm({
                        ...clickhouseForm,
                        username: e.target.value,
                      })
                    }
                  />
                  <input
                    className="p-2 border rounded"
                    type="password"
                    placeholder="JWT Token / Password"
                    value={clickhouseForm.token}
                    onChange={(e) =>
                      setClickhouseForm({
                        ...clickhouseForm,
                        token: e.target.value,
                      })
                    }
                  />
                  <input
                    className="p-2 border rounded"
                    placeholder="Target Table Name"
                    required
                    value={targetTable}
                    onChange={(e) => setTargetTable(e.target.value)}
                  />
                </div>
              </div>
            )}
          </div>
        )}
      </div>

      {/* Table Selection for ClickHouse source */}
      {source === "clickhouse" && tables.length > 0 && (
        <div className="mb-6 p-4 bg-gray-100 rounded-lg">
          <h2 className="text-xl font-semibold mb-4">3. Select Table(s)</h2>
          <div className="mb-4">
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-2">
              {tables.map((table, idx) => (
                <label
                  key={idx}
                  className="flex items-center p-2 border rounded hover:bg-gray-50"
                >
                  <input
                    type="checkbox"
                    checked={selectedTables.includes(table.name || table)}
                    onChange={async () => {
                      await handleTableToggle(table.name || table);
                    }}
                    className="mr-2"
                  />
                  {table.name || table}
                </label>
              ))}
            </div>
          </div>

          {/* Join condition for multiple tables (bonus feature) */}
          {selectedTables.length > 1 && (
            <div className="mb-4">
              <label className="block mb-2">JOIN Condition:</label>
              <textarea
                className="p-2 border rounded w-full"
                rows="2"
                placeholder="e.g., table1.id = table2.table1_id"
                value={joinCondition}
                onChange={(e) => setJoinCondition(e.target.value)}
              ></textarea>
            </div>
          )}

          <button
            className="p-2 bg-blue-500 text-white rounded hover:bg-blue-600"
            onClick={handleLoadColumns}
            disabled={selectedTables.length === 0}
          >
            Load Columns
          </button>
        </div>
      )}

      {/* Column Selection */}
      {columns.length > 0 && (
        <div className="mb-6 p-4 bg-gray-100 rounded-lg">
          <h2 className="text-xl font-semibold mb-2">
            {source === "clickhouse" ? "4" : "3"}. Select Columns
          </h2>

          <div className="mb-2">
            <button
              className="text-sm text-blue-500 hover:underline"
              onClick={handleSelectAllColumns}
            >
              {selectedColumns.length === columns.length
                ? "Deselect All"
                : "Select All"}
            </button>
          </div>

          <div className="max-h-60 overflow-y-auto mb-4 border rounded bg-white p-2">
            <div className="space-y-4">
              {Object.entries(columnsByTable).map(
                ([tableName, tableColumns]) => (
                  <div
                    key={tableName}
                    className="border-b pb-2 last:border-b-0"
                  >
                    <h3 className="font-medium text-gray-700 mb-2">
                      {tableName}
                    </h3>
                    <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-2">
                      {tableColumns.map((col) => (
                        <label
                          key={`${col.table}-${col.name}`}
                          className="flex items-center p-1 hover:bg-gray-50"
                        >
                          <input
                            type="checkbox"
                            checked={isColumnSelected(col)}
                            onChange={() => {
                              handleColumnToggle(col);
                            }}
                            className="mr-2"
                          />
                          <span>{col.name}</span>
                          {col.type && col.type !== "unknown" && (
                            <span className="text-xs text-gray-500 ml-1">
                              ({col.type})
                            </span>
                          )}
                        </label>
                      ))}
                    </div>
                  </div>
                )
              )}
            </div>
          </div>

          <div className="flex gap-4">
            <button
              className="p-2 bg-blue-500 text-white rounded hover:bg-blue-600"
              onClick={handlePreview}
              disabled={selectedColumns.length === 0}
            >
              Preview Data
            </button>

            <button
              className="p-2 bg-green-500 text-white rounded hover:bg-green-600"
              onClick={handleStartIngestion}
              // disabled={
              //   selectedColumns.length === 0 ||
              //   (source === "flatfile" && !targetTable)
              // }
            >
              Start {source === "clickhouse" ? "Export" : "Import"}
            </button>
          </div>
        </div>
      )}

      {/* Data Preview */}
      {previewData && (
        <div className="mb-6 p-4 bg-gray-100 rounded-lg">
          <h2 className="text-xl font-semibold mb-4">
            Data Preview (First 100 Records)
          </h2>
          <div className="overflow-x-auto">
            <table className="min-w-full bg-white border">
              <thead>
                <tr className="bg-gray-200">
                  {selectedColumns.map((col) => (
                    <th
                      key={`${col.table}-${col.name}-header`}
                      className="p-2 border text-left"
                    >
                      {/* Display just the column name for single table, otherwise table_column */}
                      {selectedTables.length === 1
                        ? col.name
                        : `${col.table}_${col.name}`}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {previewData.map((row, rowIdx) => (
                  <tr
                    key={rowIdx}
                    className={rowIdx % 2 === 0 ? "bg-gray-50" : ""}
                  >
                    {selectedColumns.map((col) => {
                      // Try both keys to handle both single and multiple table scenarios
                      const multiTableKey = `${col.table}_${col.name}`;
                      const singleTableKey = col.name;

                      // First check if the multi-table format key exists, then try single table format
                      const value = row.hasOwnProperty(multiTableKey)
                        ? row[multiTableKey]
                        : row[singleTableKey];

                      return (
                        <td
                          key={`row-${rowIdx}-${col.table}-${col.name}`}
                          className="p-2 border"
                        >
                          {value !== undefined ? String(value) : ""}
                        </td>
                      );
                    })}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {/* Status and Progress */}
      {status && (
        <div className="mb-6 p-4 bg-gray-100 rounded-lg">
          <h2 className="text-xl font-semibold mb-2">Status</h2>
          <p
            className={
              status.startsWith("✅")
                ? "text-green-600"
                : status.startsWith("Failed")
                ? "text-red-600"
                : "text-blue-600"
            }
          >
            {status}
          </p>

          {progress > 0 && (
            <div className="mt-4 w-full bg-gray-200 rounded-full h-4">
              <div
                className="bg-blue-500 h-4 rounded-full transition-all duration-300 ease-in-out"
                style={{ width: `${progress}%` }}
              ></div>
            </div>
          )}
        </div>
      )}
    </div>
  );
}
