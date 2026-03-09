const sqlite3 = require("sqlite3").verbose();
const path = require("path");

const DB_PATH = process.env.DB_PATH || path.join(__dirname, "data.db");
const db = new sqlite3.Database(DB_PATH);
console.log("SQLite DB:", DB_PATH);

function ensureColumn(table, column, definition) {
  db.all(`PRAGMA table_info(${table})`, (err, rows) => {
    if (err) return;
    const exists = (rows || []).some((r) => r.name === column);
    if (!exists) {
      db.run(`ALTER TABLE ${table} ADD COLUMN ${definition}`);
    }
  });
}

db.serialize(() => {
  db.run(`
    CREATE TABLE IF NOT EXISTS locations (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      name TEXT NOT NULL UNIQUE
    )
  `);

  // Departments independent from locations
  db.run(`
    CREATE TABLE IF NOT EXISTS departments (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      name TEXT NOT NULL UNIQUE
    )
  `);

  db.run(`
    CREATE TABLE IF NOT EXISTS entrepreneurs (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      name TEXT NOT NULL UNIQUE,
      street TEXT,
      postal_code TEXT,
      city TEXT,
      created_at TEXT NOT NULL DEFAULT (datetime('now'))
    )
  `);

  ensureColumn("entrepreneurs", "street", "street TEXT");
  ensureColumn("entrepreneurs", "postal_code", "postal_code TEXT");
  ensureColumn("entrepreneurs", "city", "city TEXT");

  db.run(`
    CREATE TABLE IF NOT EXISTS entrepreneur_history (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      location_id INTEGER NOT NULL,
      department_id INTEGER NOT NULL,
      created_by INTEGER NOT NULL,
      entrepreneur TEXT NOT NULL,
      license_plate TEXT,
      qty_in INTEGER NOT NULL DEFAULT 0,
      qty_out INTEGER NOT NULL DEFAULT 0,
      created_at TEXT NOT NULL DEFAULT (datetime('now')),
      FOREIGN KEY(location_id) REFERENCES locations(id),
      FOREIGN KEY(department_id) REFERENCES departments(id),
      FOREIGN KEY(created_by) REFERENCES users(id)
    )
  `);

  ensureColumn("entrepreneur_history", "qty_in", "qty_in INTEGER");
  ensureColumn("entrepreneur_history", "qty_out", "qty_out INTEGER");

  db.run(`
    CREATE TABLE IF NOT EXISTS users (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      username TEXT NOT NULL UNIQUE,
      password_hash TEXT NOT NULL,
      role TEXT NOT NULL CHECK(role IN ('admin','user')),
      location_id INTEGER,
      is_active INTEGER NOT NULL DEFAULT 1,
      created_at TEXT NOT NULL DEFAULT (datetime('now')),
      FOREIGN KEY(location_id) REFERENCES locations(id)
    )
  `);

  db.run(`
    CREATE TABLE IF NOT EXISTS bookings (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      location_id INTEGER NOT NULL,
      department_id INTEGER NOT NULL,
      user_id INTEGER NOT NULL,
      type TEXT NOT NULL CHECK(type IN ('IN','OUT')),
      quantity INTEGER NOT NULL CHECK(quantity > 0),
      note TEXT,
      receipt_no TEXT,
      license_plate TEXT,
      created_at TEXT NOT NULL DEFAULT (datetime('now')),
      FOREIGN KEY(location_id) REFERENCES locations(id),
      FOREIGN KEY(department_id) REFERENCES departments(id),
      FOREIGN KEY(user_id) REFERENCES users(id)
    )
  `);

  db.run(`
    CREATE TABLE IF NOT EXISTS receipt_seq (
      id INTEGER PRIMARY KEY CHECK (id = 1),
      next_no INTEGER NOT NULL
    )
  `);

  db.run(`INSERT OR IGNORE INTO receipt_seq (id, next_no) VALUES (1, 1)`);
});

module.exports = db;
