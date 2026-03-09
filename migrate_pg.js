const bcrypt = require("bcryptjs");
const { pool } = require("./db_pg");

async function migrate() {
  // Basis-Tabelle
  await pool.query(`
    CREATE TABLE IF NOT EXISTS locations (
      id SERIAL PRIMARY KEY,
      name TEXT NOT NULL UNIQUE
    );

    CREATE TABLE IF NOT EXISTS departments (
      id SERIAL PRIMARY KEY,
      name TEXT NOT NULL UNIQUE
    );

    CREATE TABLE IF NOT EXISTS entrepreneurs (
      id SERIAL PRIMARY KEY,
      name TEXT NOT NULL UNIQUE,
      street TEXT,
      postal_code TEXT,
      city TEXT,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );

    CREATE TABLE IF NOT EXISTS users (
      id SERIAL PRIMARY KEY,
      username TEXT NOT NULL UNIQUE,
      password_hash TEXT NOT NULL,
      role TEXT NOT NULL CHECK(role IN ('admin','disponent','lager')),
      location_id INTEGER REFERENCES locations(id),
      is_active BOOLEAN NOT NULL DEFAULT TRUE,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );

    CREATE TABLE IF NOT EXISTS bookings (
      id SERIAL PRIMARY KEY,
      location_id INTEGER NOT NULL REFERENCES locations(id),
      department_id INTEGER REFERENCES departments(id) ON DELETE SET NULL,
      user_id INTEGER REFERENCES users(id) ON DELETE SET NULL,
      type TEXT NOT NULL CHECK(type IN ('IN','OUT')),
      quantity INTEGER NOT NULL CHECK(quantity > 0),
      note TEXT,
      receipt_no TEXT,
      license_plate TEXT,
      product_type TEXT NOT NULL DEFAULT 'euro' CHECK (product_type IN ('euro','h1','gitterbox')),
      created_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );

    CREATE TABLE IF NOT EXISTS entrepreneur_history (
      id SERIAL PRIMARY KEY,
      location_id INTEGER NOT NULL REFERENCES locations(id),
      department_id INTEGER REFERENCES departments(id) ON DELETE SET NULL,
      created_by INTEGER REFERENCES users(id) ON DELETE SET NULL,
      entrepreneur TEXT NOT NULL,
      license_plate TEXT,
      qty_in INTEGER NOT NULL DEFAULT 0,
      qty_out INTEGER NOT NULL DEFAULT 0,
      product_type TEXT NOT NULL DEFAULT 'euro' CHECK (product_type IN ('euro','h1','gitterbox')),
      created_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );

    CREATE TABLE IF NOT EXISTS booking_cases (
      id SERIAL PRIMARY KEY,
      location_id INTEGER NOT NULL REFERENCES locations(id),
      department_id INTEGER REFERENCES departments(id) ON DELETE SET NULL,
      created_by INTEGER REFERENCES users(id) ON DELETE SET NULL,
      status INTEGER NOT NULL DEFAULT 1, -- 0 Storniert, 1 Aviso, 2 In Bearbeitung, 3 In Prüfung, 4 Gebucht
      license_plate TEXT NOT NULL,
      entrepreneur TEXT,
      note TEXT,
      qty_in INTEGER NOT NULL DEFAULT 0,
      qty_out INTEGER NOT NULL DEFAULT 0,
      non_exchangeable_qty INTEGER NOT NULL DEFAULT 0,
      product_type TEXT NOT NULL DEFAULT 'euro' CHECK (product_type IN ('euro','h1','gitterbox')),
      translogica_transferred BOOLEAN NOT NULL DEFAULT FALSE,
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

    CREATE TABLE IF NOT EXISTS receipt_seq (
      id INTEGER PRIMARY KEY CHECK (id = 1),
      next_no BIGINT NOT NULL
    );

    INSERT INTO receipt_seq (id, next_no)
    VALUES (1, 1)
    ON CONFLICT (id) DO NOTHING;
  `);

  // Zusätze aus deinem aktuellen Stand
  await pool.query(`ALTER TABLE bookings ADD COLUMN IF NOT EXISTS entrepreneur TEXT;`);
  await pool.query(`ALTER TABLE bookings ADD COLUMN IF NOT EXISTS booking_group_id UUID;`);
  await pool.query(`ALTER TABLE bookings ADD COLUMN IF NOT EXISTS line_no INTEGER;`);
  await pool.query(`ALTER TABLE entrepreneurs ADD COLUMN IF NOT EXISTS street TEXT;`);
  await pool.query(`ALTER TABLE entrepreneurs ADD COLUMN IF NOT EXISTS postal_code TEXT;`);
  await pool.query(`ALTER TABLE entrepreneurs ADD COLUMN IF NOT EXISTS city TEXT;`);
  await pool.query(`ALTER TABLE entrepreneur_history ADD COLUMN IF NOT EXISTS license_plate TEXT;`);
  await pool.query(`ALTER TABLE entrepreneur_history ADD COLUMN IF NOT EXISTS qty_in INTEGER;`);
  await pool.query(`ALTER TABLE entrepreneur_history ADD COLUMN IF NOT EXISTS qty_out INTEGER;`);
  await pool.query(`ALTER TABLE entrepreneur_history ADD COLUMN IF NOT EXISTS product_type TEXT NOT NULL DEFAULT 'euro';`);
  await pool.query(`ALTER TABLE entrepreneur_history DROP CONSTRAINT IF EXISTS entrepreneur_history_product_type_check;`);
  await pool.query(`ALTER TABLE entrepreneur_history ADD CONSTRAINT entrepreneur_history_product_type_check CHECK (product_type IN ('euro','h1','gitterbox'));`);
  await pool.query(`ALTER TABLE booking_cases ADD COLUMN IF NOT EXISTS employee_code TEXT;`);
  await pool.query(`ALTER TABLE booking_cases ADD COLUMN IF NOT EXISTS product_type TEXT NOT NULL DEFAULT 'euro';`);
  await pool.query(`ALTER TABLE booking_cases ADD COLUMN IF NOT EXISTS translogica_transferred BOOLEAN NOT NULL DEFAULT FALSE;`);
  await pool.query(`ALTER TABLE booking_cases ADD COLUMN IF NOT EXISTS non_exchangeable_qty INTEGER NOT NULL DEFAULT 0;`);
  await pool.query(`ALTER TABLE booking_cases DROP CONSTRAINT IF EXISTS booking_cases_product_type_check;`);
  await pool.query(`ALTER TABLE booking_cases ADD CONSTRAINT booking_cases_product_type_check CHECK (product_type IN ('euro','h1','gitterbox'));`);
  await pool.query(`ALTER TABLE bookings ADD COLUMN IF NOT EXISTS product_type TEXT NOT NULL DEFAULT 'euro';`);
  await pool.query(`ALTER TABLE bookings DROP CONSTRAINT IF EXISTS bookings_product_type_check;`);
  await pool.query(`ALTER TABLE bookings ADD CONSTRAINT bookings_product_type_check CHECK (product_type IN ('euro','h1','gitterbox'));`);

  // FK-Constraints lockern (Benutzer/Abteilungen löschen -> SET NULL)
  await pool.query(`ALTER TABLE bookings ALTER COLUMN department_id DROP NOT NULL;`);
  await pool.query(`ALTER TABLE bookings ALTER COLUMN user_id DROP NOT NULL;`);
  await pool.query(`ALTER TABLE booking_cases ALTER COLUMN department_id DROP NOT NULL;`);
  await pool.query(`ALTER TABLE booking_cases ALTER COLUMN created_by DROP NOT NULL;`);
  await pool.query(`ALTER TABLE entrepreneur_history ALTER COLUMN department_id DROP NOT NULL;`);
  await pool.query(`ALTER TABLE entrepreneur_history ALTER COLUMN created_by DROP NOT NULL;`);
  await pool.query(`ALTER TABLE users ALTER COLUMN location_id DROP NOT NULL;`);

  await pool.query(`
    ALTER TABLE bookings
    DROP CONSTRAINT IF EXISTS bookings_department_id_fkey,
    DROP CONSTRAINT IF EXISTS bookings_user_id_fkey;
  `);
  await pool.query(`
    ALTER TABLE bookings
    ADD CONSTRAINT bookings_department_id_fkey FOREIGN KEY (department_id) REFERENCES departments(id) ON DELETE SET NULL,
    ADD CONSTRAINT bookings_user_id_fkey FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE SET NULL;
  `);

  await pool.query(`
    ALTER TABLE booking_cases
    DROP CONSTRAINT IF EXISTS booking_cases_department_id_fkey,
    DROP CONSTRAINT IF EXISTS booking_cases_created_by_fkey,
    DROP CONSTRAINT IF EXISTS booking_cases_claimed_by_fkey,
    DROP CONSTRAINT IF EXISTS booking_cases_submitted_by_fkey,
    DROP CONSTRAINT IF EXISTS booking_cases_approved_by_fkey;
  `);
  await pool.query(`
    ALTER TABLE booking_cases
    ADD CONSTRAINT booking_cases_department_id_fkey FOREIGN KEY (department_id) REFERENCES departments(id) ON DELETE SET NULL,
    ADD CONSTRAINT booking_cases_created_by_fkey FOREIGN KEY (created_by) REFERENCES users(id) ON DELETE SET NULL,
    ADD CONSTRAINT booking_cases_claimed_by_fkey FOREIGN KEY (claimed_by) REFERENCES users(id) ON DELETE SET NULL,
    ADD CONSTRAINT booking_cases_submitted_by_fkey FOREIGN KEY (submitted_by) REFERENCES users(id) ON DELETE SET NULL,
    ADD CONSTRAINT booking_cases_approved_by_fkey FOREIGN KEY (approved_by) REFERENCES users(id) ON DELETE SET NULL;
  `);

  await pool.query(`
    ALTER TABLE entrepreneur_history
    DROP CONSTRAINT IF EXISTS entrepreneur_history_department_id_fkey,
    DROP CONSTRAINT IF EXISTS entrepreneur_history_created_by_fkey;
  `);
  await pool.query(`
    ALTER TABLE entrepreneur_history
    ADD CONSTRAINT entrepreneur_history_department_id_fkey FOREIGN KEY (department_id) REFERENCES departments(id) ON DELETE SET NULL,
    ADD CONSTRAINT entrepreneur_history_created_by_fkey FOREIGN KEY (created_by) REFERENCES users(id) ON DELETE SET NULL;
  `);

  // users.email + users.fixed_department_id hinzufügen
  await pool.query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS email TEXT;`);
  await pool.query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS fixed_department_id INTEGER;`);

  await pool.query(`
    ALTER TABLE users
    DROP CONSTRAINT IF EXISTS users_fixed_department_id_fkey;
  `);
  await pool.query(`
    DO $$
    BEGIN
      IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = 'users'
          AND column_name = 'fixed_department_id'
      ) THEN
        ALTER TABLE users
        ADD CONSTRAINT users_fixed_department_id_fkey FOREIGN KEY (fixed_department_id) REFERENCES departments(id) ON DELETE SET NULL;
      END IF;
    END
    $$;
  `);
  await pool.query(`
    ALTER TABLE users
    DROP CONSTRAINT IF EXISTS users_location_id_fkey;
  `);
  await pool.query(`
    ALTER TABLE users
    ADD CONSTRAINT users_location_id_fkey FOREIGN KEY (location_id) REFERENCES locations(id) ON DELETE SET NULL;
  `);

  await pool.query(`
    ALTER TABLE users
    DROP CONSTRAINT IF EXISTS users_role_check;
  `);
  await pool.query(`
    UPDATE users
    SET role='disponent'
    WHERE role='user' OR role IS NULL;
  `);
  await pool.query(`
    ALTER TABLE users
    ADD CONSTRAINT users_role_check CHECK (role IN ('admin','disponent','lager'));
  `);

  // Rollen-Tabelle (permissions als JSONB)
  await pool.query(`
    CREATE TABLE IF NOT EXISTS roles (
      id SERIAL PRIMARY KEY,
      name TEXT NOT NULL UNIQUE,
      permissions JSONB NOT NULL DEFAULT '{}'::jsonb,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );
  `);

  // users.role_id hinzufügen
  await pool.query(`
    ALTER TABLE users
    ADD COLUMN IF NOT EXISTS role_id INTEGER REFERENCES roles(id);
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS ip_preferences (
      ip_address TEXT PRIMARY KEY,
      theme TEXT NOT NULL DEFAULT 'light' CHECK (theme IN ('light','dark')),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS user_notifications (
      id SERIAL PRIMARY KEY,
      user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
      case_id INTEGER REFERENCES booking_cases(id) ON DELETE SET NULL,
      title TEXT NOT NULL,
      message TEXT NOT NULL,
      is_read BOOLEAN NOT NULL DEFAULT FALSE,
      read_at TIMESTAMPTZ,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );
  `);
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_user_notifications_user_created ON user_notifications(user_id, created_at DESC);`);

  // Default Rolle anlegen
  await pool.query(`
    INSERT INTO roles (name, permissions)
    VALUES (
      'Standard',
      '{
        "bookings": { "create": true, "view": true, "export": true, "receipt": true, "edit": false, "delete": false, "translogica": false },
        "stock": { "view": true, "overall": true },
        "cases": {
          "create": true,
          "internal_transfer": false,
          "claim": false,
          "edit": false,
          "submit": false,
          "approve": false,
          "cancel": false,
          "delete": false,
          "require_employee_code": false
        },
        "filters": { "all_locations": false },
        "masterdata": { "manage": false },
        "users": { "manage": false, "view_department": false },
        "roles": { "manage": false }
      }'::jsonb
    )
    ON CONFLICT (name) DO NOTHING;
  `);

  // booking_cases Index
  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_booking_cases_loc_status
      ON booking_cases(location_id, status);
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_entrepreneur_history_loc_created
      ON entrepreneur_history(location_id, created_at DESC);
  `);

  // Seed Admin falls keiner existiert
  const existing = await pool.query(`SELECT id FROM users WHERE role='admin' LIMIT 1`);
  if (existing.rowCount === 0) {
    const hash = await bcrypt.hash("admin1234", 10);
    await pool.query(
      `INSERT INTO users (username, password_hash, role, is_active)
       VALUES ($1,$2,$3,TRUE)`,
      ["admin", hash, "admin"]
    );
    console.log("Seed admin created: admin / admin1234 (CHANGE IT!)");
  }

  console.log("Migration OK");
}

migrate()
  .then(() => pool.end())
  .catch((e) => {
    console.error(e);
    process.exit(1);
  });
