const { pool } = require("./db_pg");

(async () => {
  try {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS booking_cases (
        id SERIAL PRIMARY KEY,
        location_id INTEGER NOT NULL REFERENCES locations(id),
        department_id INTEGER REFERENCES departments(id) ON DELETE SET NULL,
        created_by INTEGER REFERENCES users(id) ON DELETE SET NULL,
        status INTEGER NOT NULL DEFAULT 1,
        license_plate TEXT NOT NULL,
        entrepreneur TEXT,
        note TEXT,
        qty_in INTEGER NOT NULL DEFAULT 0,
        qty_out INTEGER NOT NULL DEFAULT 0,
        receipt_no TEXT,
        claimed_by INTEGER REFERENCES users(id) ON DELETE SET NULL,
        claimed_at TIMESTAMPTZ,
        submitted_by INTEGER REFERENCES users(id) ON DELETE SET NULL,
        submitted_at TIMESTAMPTZ,
        approved_by INTEGER REFERENCES users(id) ON DELETE SET NULL,
        approved_at TIMESTAMPTZ,
        employee_code TEXT,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
      );

      CREATE INDEX IF NOT EXISTS idx_booking_cases_loc_status
        ON booking_cases(location_id, status);
    `);

    console.log("✅ booking_cases migration done");
  } catch (e) {
    console.error("❌ migration failed:", e);
    process.exit(1);
  } finally {
    await pool.end();
  }
})();
