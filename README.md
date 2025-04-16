# Bidirectional ClickHouse & Flat File Data Ingestion Tool

A web-based tool to support seamless two-way data ingestion between ClickHouse and CSV flat files. Built with a modern tech stack: **Next.js** frontend, **Express.js** backend, and **ClickHouse** for high-performance analytics.

---

## ✨ Features

- 📤 Upload CSV files into ClickHouse
- 🔍 Preview CSV contents before ingestion
- 📥 Export ClickHouse data as CSV (supports JOINs)
- 🧾 CSV download for exported results

---

## 📁 Project Structure

```
project-root/
├── frontend/ # Next.js frontend
├── backend/ # Express.js backend
└── README.md
```

---

## ⚙️ Prerequisites

- Node.js (v18+)
- Docker & Docker Compose (for ClickHouse)
- npm or yarn

---

## 🚀 Getting Started

### 1. Clone the Repository

```bash
git clone https://github.com/KetanHegde/ClickHouse_FlatFile_Tool.git
cd clickhouse-ingestion-tool
```

---

### 2. Start ClickHouse via Docker

```bash
docker run -d --name clickhouse-server \
 -p 8123:8123 -p 9000:9000 \
 clickhouse/clickhouse-server
```

> Or use Docker Compose if you prefer a multi-service setup.

---

### 3. Backend Setup

```bash
cd backend
npm install
```

#### ➕ Create \`.env\` file in \`/backend\`

```env
PORT=5000
```

#### ▶️ Run Backend

```bash
npm start
```

The backend will be available at \`http://localhost:5000\`.

---

### 4. Frontend Setup

```bash
cd ../frontend
npm install
```

#### ➕ Create \`.env.local\` in \`/frontend\`

```env
NEXT_PUBLIC_API_BASE=http://localhost:5000
```

#### ▶️ Run Frontend

```bash
npm run dev
```

The frontend will be available at \`http://localhost:3000\`.

---

## 🔌 API Endpoints (Backend)

| Method | Endpoint                         | Description                                     |
| ------ | -------------------------------- | ----------------------------------------------- |
| POST   | `/api/clickhouse/connect`        | Connect to ClickHouse and list available tables |
| POST   | `/api/clickhouse/columns`        | Get column details for selected tables          |
| POST   | `/api/clickhouse/export/preview` | Generate a preview of data before export        |
| POST   | `/api/clickhouse/export`         | Export data from ClickHouse to a flat file      |
| POST   | `/api/flatfile/schema`           | Analyze uploaded file and extract schema        |
| POST   | `/api/flatfile/preview`          | Preview selected columns from the uploaded file |
| POST   | `/api/flatfile/import`           | Import data from flat file to ClickHouse table  |

---

## 🧠 Notes

- CSV ingestion creates ClickHouse tables based on the file header.
- Joined query exports auto-rename conflicting columns.
- CSV export is streamed for efficiency with large datasets.
- Adjust file size/upload limits in \`multer\` middleware.

---

## 👨‍💻 Author

This project was built as part of a software engineering internship assignment.

---
