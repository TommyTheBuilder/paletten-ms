const express = require("express");
const cors = require("cors");
const helmet = require("helmet");
const bcrypt = require("bcryptjs");
const jwt = require("jsonwebtoken");
const { Parser } = require("json2csv");
const ExcelJS = require("exceljs");
const path = require("path");
const { randomUUID } = require("crypto");

const { pool } = require("./db_pg");
const { authRequired, adminRequired, JWT_SECRET } = require("./middleware_auth");
const { requirePermission } = require("./middleware_permissions");

const CORS_ORIGIN = process.env.CORS_ORIGIN || "*";
const ALWAYS_ALLOWED_ORIGINS = [
  "https://571188521.swh.strato-hosting.eu",
  "http://571188521.swh.strato-hosting.eu"
];
const MAX_BODY_SIZE = process.env.MAX_BODY_SIZE || "100kb";
const LOGIN_WINDOW_MS = Number(process.env.LOGIN_WINDOW_MS || 15 * 60 * 1000);
const LOGIN_MAX_ATTEMPTS = Number(process.env.LOGIN_MAX_ATTEMPTS || 10);
const PRODUCT_TYPES = ["euro", "h1", "gitterbox"];

function getAllowedOrigins() {
  if (CORS_ORIGIN === "*") return "*";
  return Array.from(new Set([
    ...CORS_ORIGIN.split(",").map((x) => x.trim()).filter(Boolean),
    ...ALWAYS_ALLOWED_ORIGINS
  ]));
}

function corsOriginResolver(origin, callback) {
  const allowedOrigins = getAllowedOrigins();
  if (allowedOrigins === "*") return callback(null, true);
  if (!origin || allowedOrigins.includes(origin)) return callback(null, true);
  return callback(new Error("Not allowed by CORS"));
}

const app = express();
app.set("trust proxy", 1);
app.disable("x-powered-by");
app.use(helmet());
app.use(cors({ origin: corsOriginResolver }));
app.use(express.json({ limit: MAX_BODY_SIZE }));
app.use(express.static(path.join(__dirname, "public")));

const httpServer = require("http").createServer(app);
const allowedOrigins = getAllowedOrigins();
const io = require("socket.io")(httpServer, {
  cors: {
    origin: allowedOrigins === "*" ? true : allowedOrigins
  }
});

io.on("connection", (socket) => {
  socket.on("joinLocation", (locationId) => {
    if (locationId) socket.join(`loc:${locationId}`);
  });

  socket.on("joinUser", (userId) => {
    const parsedUserId = Number(userId);
    if (Number.isInteger(parsedUserId) && parsedUserId > 0) {
      socket.join(`user:${parsedUserId}`);
    }
  });
});

async function q(sql, params = []) {
  return pool.query(sql, params);
}

const LOGIN_ATTEMPTS = new Map();

function tooManyLoginAttempts(ip) {
  const now = Date.now();
  const current = LOGIN_ATTEMPTS.get(ip);
  if (!current || current.expiresAt <= now) {
    LOGIN_ATTEMPTS.set(ip, { count: 1, expiresAt: now + LOGIN_WINDOW_MS });
    return false;
  }
  current.count += 1;
  LOGIN_ATTEMPTS.set(ip, current);
  return current.count > LOGIN_MAX_ATTEMPTS;
}

function clearLoginAttempts(ip) {
  LOGIN_ATTEMPTS.delete(ip);
}

function normalizeProductType(value) {
  const normalized = String(value || "euro").trim().toLowerCase();
  if (!PRODUCT_TYPES.includes(normalized)) {
    return { ok: false, msg: "product_type invalid" };
  }
  return { ok: true, productType: normalized };
}

// ---------- Helpers ----------
async function nextReceiptNo(locationId) {
  const today = new Date();
  const y = today.getFullYear();
  const m = String(today.getMonth() + 1).padStart(2, "0");
  const d = String(today.getDate()).padStart(2, "0");
  const datePart = `${y}${m}${d}`;
  const loc = await q(`SELECT name FROM locations WHERE id=$1`, [locationId]);
  const locName = loc.rowCount ? String(loc.rows[0].name || "") : "";
  const letterMatch = locName.match(/[A-Za-zÄÖÜ]/);
  const numberMatch = locName.match(/\d+/);
  const locLetter = letterMatch ? letterMatch[0].toUpperCase() : "L";
  const locNumber = numberMatch ? numberMatch[0] : String(locationId);
  const locationIndicator = `${locLetter}${locNumber}`;

  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    const row = await client.query(`SELECT next_no FROM receipt_seq WHERE id=1 FOR UPDATE`);
    const no = Number(row.rows[0].next_no);
    await client.query(`UPDATE receipt_seq SET next_no = next_no + 1 WHERE id=1`);
    await client.query("COMMIT");
    return `ICS${locationIndicator}-${datePart}-${String(no).padStart(6, "0")}`;
  } catch (e) {
    await client.query("ROLLBACK");
    throw e;
  } finally {
    client.release();
  }
}

async function previewReceiptNo(locationId) {
  const today = new Date();
  const y = today.getFullYear();
  const m = String(today.getMonth() + 1).padStart(2, "0");
  const d = String(today.getDate()).padStart(2, "0");
  const datePart = `${y}${m}${d}`;
  const loc = await q(`SELECT name FROM locations WHERE id=$1`, [locationId]);
  const locName = loc.rowCount ? String(loc.rows[0].name || "") : "";
  const letterMatch = locName.match(/[A-Za-zÄÖÜ]/);
  const numberMatch = locName.match(/\d+/);
  const locLetter = letterMatch ? letterMatch[0].toUpperCase() : "L";
  const locNumber = numberMatch ? numberMatch[0] : String(locationId);
  const locationIndicator = `${locLetter}${locNumber}`;

  const row = await q(`SELECT next_no FROM receipt_seq WHERE id=1`);
  const no = Number(row.rows[0]?.next_no || 1);
  return `ICS${locationIndicator}-${datePart}-${String(no).padStart(6, "0")}`;
}

function normalizePlate(plateRaw) {
  const plate = String(plateRaw || "").trim().toUpperCase();
  if (!plate) return { ok: false, msg: "Kennzeichen ist Pflicht" };
  if (plate.includes("-")) return { ok: false, msg: "Kennzeichen bitte ohne '-' eingeben" };
  if (/\s/.test(plate)) return { ok: false, msg: "Kennzeichen bitte ohne Leerzeichen eingeben" };
  if (!/^[A-Z0-9ÄÖÜ]+$/.test(plate)) return { ok: false, msg: "Kennzeichen nur Buchstaben/Zahlen (ohne Sonderzeichen)" };
  if (plate.length < 3) return { ok: false, msg: "Kennzeichen zu kurz" };
  return { ok: true, plate };
}

function normalizeEmployeeCode(codeRaw) {
  const code = safeTrim(codeRaw);
  if (!code) return null;
  const normalized = code.toUpperCase();
  if (!/^[A-Z0-9]{2}$/.test(normalized)) {
    return { ok: false, msg: "Lagermitarbeiter muss genau 2 Zeichen haben (Buchstaben/Zahlen)" };
  }
  return { ok: true, code: normalized };
}

function safeTrim(v) {
  const s = (v === undefined || v === null) ? "" : String(v);
  const t = s.trim();
  return t ? t : null;
}

function normalizeEmail(emailRaw) {
  const email = safeTrim(emailRaw);
  if (!email) return null;
  const normalized = email.toLowerCase();
  if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(normalized)) {
    return { ok: false, msg: "E-Mail-Adresse ungültig" };
  }
  return { ok: true, email: normalized };
}

async function createLocationStatus1Notifications(caseRow) {
  try {
    const locationInfo = await q(
      `SELECT name FROM locations WHERE id=$1`,
      [caseRow.location_id]
    );
    const locationName = locationInfo.rowCount ? locationInfo.rows[0].name : `Standort ${caseRow.location_id}`;

    const recipients = await q(
      `SELECT id FROM users WHERE is_active=TRUE AND location_id=$1`,
      [caseRow.location_id]
    );

    for (const recipient of recipients.rows) {
      if (Number(recipient.id) === Number(caseRow.created_by)) continue;
      const inserted = await q(
        `INSERT INTO user_notifications (user_id, case_id, title, message)
         VALUES ($1, $2, $3, $4)
         RETURNING id, user_id, case_id, title, message, is_read, created_at`,
        [recipient.id, caseRow.id, "Aviso Standort (Status 1)", `Neues Aviso #${caseRow.id} am ${locationName}.`]
      );
      io.to(`user:${recipient.id}`).emit("notificationCreated", inserted.rows[0]);
    }
  } catch (err) {
    console.error("Standort-Notification fehlgeschlagen:", err);
  }
}

async function createDepartmentStatus3Notifications(caseRow) {
  try {
    if (!caseRow.department_id) return;

    const departmentInfo = await q(
      `SELECT name FROM departments WHERE id=$1`,
      [caseRow.department_id]
    );
    const departmentName = departmentInfo.rowCount ? departmentInfo.rows[0].name : `Abteilung ${caseRow.department_id}`;

    const recipients = await q(
      `SELECT id FROM users WHERE is_active=TRUE AND fixed_department_id=$1`,
      [caseRow.department_id]
    );

    for (const recipient of recipients.rows) {
      if (Number(recipient.id) === Number(caseRow.submitted_by || caseRow.created_by)) continue;
      const inserted = await q(
        `INSERT INTO user_notifications (user_id, case_id, title, message)
         VALUES ($1, $2, $3, $4)
         RETURNING id, user_id, case_id, title, message, is_read, created_at`,
        [recipient.id, caseRow.id, "Aviso Abteilung (Status 3)", `Aviso #${caseRow.id} ist in Prüfung (${departmentName}).`]
      );
      io.to(`user:${recipient.id}`).emit("notificationCreated", inserted.rows[0]);
    }
  } catch (err) {
    console.error("Abteilungs-Notification fehlgeschlagen:", err);
  }
}

async function pruneNotificationsForUser(userId) {
  const deletedByStatus = await q(
    `DELETE FROM user_notifications n
     USING booking_cases c
     WHERE n.case_id = c.id
       AND n.user_id = $1
       AND (
         (n.title='Aviso Standort (Status 1)' AND c.status >= 3)
         OR (n.title='Aviso Abteilung (Status 3)' AND c.status >= 4)
       )
     RETURNING n.id`,
    [userId]
  );

  const deletedOrphans = await q(
    `DELETE FROM user_notifications n
     WHERE n.user_id = $1
       AND NOT EXISTS (
         SELECT 1 FROM booking_cases c WHERE c.id = n.case_id
       )
     RETURNING n.id`,
    [userId]
  );

  const deletedIds = [
    ...deletedByStatus.rows.map((row) => row.id),
    ...deletedOrphans.rows.map((row) => row.id)
  ];
  if (deletedIds.length > 0) {
    io.to(`user:${userId}`).emit("notificationsDeleted", {
      notification_ids: deletedIds
    });
  }
}

function emitNotificationsDeleted(payloadByUser) {
  for (const [userId, notificationIds] of payloadByUser.entries()) {
    io.to(`user:${userId}`).emit("notificationsDeleted", {
      notification_ids: notificationIds
    });
  }
}

async function deleteNotificationsForCase(caseId) {
  const deleted = await q(
    `DELETE FROM user_notifications
     WHERE case_id=$1
     RETURNING id, user_id`,
    [caseId]
  );

  if (deleted.rowCount === 0) return;

  const payloadByUser = new Map();
  for (const row of deleted.rows) {
    if (!payloadByUser.has(row.user_id)) payloadByUser.set(row.user_id, []);
    payloadByUser.get(row.user_id).push(row.id);
  }
  emitNotificationsDeleted(payloadByUser);
}

async function deleteNotificationsForCaseByTitle(caseId, title) {
  const deleted = await q(
    `DELETE FROM user_notifications
     WHERE case_id=$1 AND title=$2
     RETURNING id, user_id`,
    [caseId, title]
  );

  if (deleted.rowCount === 0) return;

  const payloadByUser = new Map();
  for (const row of deleted.rows) {
    if (!payloadByUser.has(row.user_id)) payloadByUser.set(row.user_id, []);
    payloadByUser.get(row.user_id).push(row.id);
  }
  emitNotificationsDeleted(payloadByUser);
}

async function getMyPermissions(user) {
  const fullAccessPerms = {
    bookings: { create: true, view: true, export: true, receipt: true, edit: true, delete: true, translogica: true },
    stock: { view: true, overall: true },
    cases: {
      create: true,
      internal_transfer: true,
      claim: true,
      edit: true,
      submit: true,
      approve: true,
      cancel: true,
      delete: true,
      require_employee_code: false
    },
    filters: { all_locations: true },
    masterdata: { manage: true, entrepreneurs_manage: true },
    users: { manage: true, view_department: true },
    roles: { manage: true },
    admin: { full_access: true }
  };

  if (user.role === "admin") {
    return fullAccessPerms;
  }

  const defaults = {
    bookings: { create: true, view: true, export: true, receipt: true, edit: false, delete: false, translogica: false },
    stock: { view: true, overall: true },
    cases: {
      create: true,
      internal_transfer: false,
      claim: false,
      edit: false,
      submit: false,
      approve: false,
      cancel: false,
      delete: false,
      require_employee_code: false
    },
    filters: { all_locations: false },
    masterdata: { manage: false, entrepreneurs_manage: false },
    users: { manage: false, view_department: false },
    roles: { manage: false },
    admin: { full_access: false }
  };

  if (!user.role_id) {
    return defaults;
  }

  const r = await q(`SELECT permissions FROM roles WHERE id=$1`, [user.role_id]);
  const raw = (r.rowCount ? r.rows[0].permissions : {}) || {};

  function merge(b, o) {
    const out = { ...b };
    for (const k of Object.keys(o || {})) {
      if (o[k] && typeof o[k] === "object" && !Array.isArray(o[k])) out[k] = merge(b[k] || {}, o[k]);
      else out[k] = o[k];
    }
    return out;
  }

  const p = merge(defaults, raw);
  if (p?.admin?.full_access) return fullAccessPerms;
  return p;
}

// ---------- AUTH ----------
app.post("/api/login", async (req, res) => {
  const clientIp = String(req.ip || req.headers["x-forwarded-for"] || "unknown");
  if (tooManyLoginAttempts(clientIp)) {
    return res.status(429).json({ error: "Zu viele Login-Versuche. Bitte später erneut versuchen." });
  }

  const { username, password } = req.body || {};
  const normalizedUsername = String(username || "").trim();
  if (!normalizedUsername || !password) return res.status(400).json({ error: "username/password required" });

  const r = await q(
    `SELECT id, username, password_hash, role, location_id, role_id, is_active
     FROM users
     WHERE LOWER(username)=LOWER($1)
     LIMIT 1`,
    [normalizedUsername]
  );

  const user = r.rows[0];
  if (!user || user.is_active !== true) return res.status(401).json({ error: "Invalid credentials" });

  const ok = await bcrypt.compare(password, user.password_hash);
  if (!ok) return res.status(401).json({ error: "Invalid credentials" });

  clearLoginAttempts(clientIp);

  const token = jwt.sign(
    {
      id: user.id,
      username: user.username,
      role: user.role,
      location_id: user.location_id,
      role_id: user.role_id || null
    },
    JWT_SECRET,
    { expiresIn: "12h" }
  );

  res.json({
    token,
    user: {
      id: user.id,
      username: user.username,
      role: user.role,
      location_id: user.location_id,
      role_id: user.role_id || null
    }
  });
});

app.get("/api/me", authRequired, async (req, res) => {
  const r = await q(
    `SELECT u.id,
            u.username,
            u.role,
            u.location_id,
            u.role_id,
            u.is_active,
            ro.name AS business_role_name
     FROM users u
     LEFT JOIN roles ro ON ro.id = u.role_id
     WHERE u.id=$1`,
    [req.user.id]
  );
  const user = r.rows[0];
  if (!user || user.is_active !== true) return res.status(401).json({ error: "Not authenticated" });
  res.json(user);
});

app.post("/api/change-password", authRequired, async (req, res) => {
  const currentPassword = String(req.body?.current_password || "").trim();
  const newPassword = String(req.body?.new_password || "").trim();

  if (!currentPassword || !newPassword) {
    return res.status(400).json({ error: "current_password und new_password erforderlich" });
  }
  if (newPassword.length < 8) {
    return res.status(400).json({ error: "Neues Passwort muss mindestens 8 Zeichen lang sein" });
  }

  const userResult = await q(
    `SELECT id, password_hash FROM users WHERE id=$1 LIMIT 1`,
    [req.user.id]
  );
  const user = userResult.rows[0];
  if (!user) return res.status(404).json({ error: "Benutzer nicht gefunden" });

  const ok = await bcrypt.compare(currentPassword, user.password_hash);
  if (!ok) return res.status(400).json({ error: "Aktuelles Passwort ist nicht korrekt" });

  if (currentPassword === newPassword) {
    return res.status(400).json({ error: "Neues Passwort muss sich vom alten Passwort unterscheiden" });
  }

  const hash = await bcrypt.hash(newPassword, 10);
  await q(`UPDATE users SET password_hash=$1 WHERE id=$2`, [hash, req.user.id]);
  res.json({ ok: true });
});

app.get("/api/theme", async (req, res) => {
  const ipAddress = String(req.ip || req.headers["x-forwarded-for"] || "unknown");
  const pref = await q(
    `SELECT theme FROM ip_preferences WHERE ip_address=$1 LIMIT 1`,
    [ipAddress]
  );
  res.json({ theme: pref.rowCount ? pref.rows[0].theme : "light" });
});

app.put("/api/theme", async (req, res) => {
  const nextTheme = String(req.body?.theme || "").trim().toLowerCase();
  if (!["light", "dark"].includes(nextTheme)) {
    return res.status(400).json({ error: "invalid theme" });
  }
  const ipAddress = String(req.ip || req.headers["x-forwarded-for"] || "unknown");
  await q(
    `INSERT INTO ip_preferences (ip_address, theme)
     VALUES ($1, $2)
     ON CONFLICT (ip_address)
     DO UPDATE SET theme=EXCLUDED.theme, updated_at=now()`,
    [ipAddress, nextTheme]
  );
  res.json({ ok: true, theme: nextTheme });
});

app.get("/api/my-permissions", authRequired, async (req, res) => {
  const perms = await getMyPermissions(req.user);
  res.json(perms);
});

app.get("/api/notifications", authRequired, async (req, res) => {
  await pruneNotificationsForUser(req.user.id);

  const rows = (await q(
    `SELECT id, user_id, case_id, title, message, is_read, created_at
     FROM user_notifications
     WHERE user_id=$1
     ORDER BY created_at DESC
     LIMIT 50`,
    [req.user.id]
  )).rows;
  const unread = rows.filter((item) => !item.is_read).length;
  res.json({ items: rows, unread });
});

app.put("/api/notifications/:id/read", authRequired, async (req, res) => {
  const id = Number(req.params.id);
  if (!id) return res.status(400).json({ error: "invalid id" });

  await q(
    `UPDATE user_notifications
     SET is_read=TRUE, read_at=now()
     WHERE id=$1 AND user_id=$2`,
    [id, req.user.id]
  );
  res.json({ ok: true });
});

// ---------- LOCATIONS ----------
app.get("/api/locations", authRequired, async (req, res) => {
  res.json((await q(`SELECT id, name FROM locations ORDER BY name`)).rows);
});

app.post("/api/admin/locations", authRequired, adminRequired, async (req, res) => {
  const { name } = req.body || {};
  if (!name) return res.status(400).json({ error: "name required" });

  const nm = String(name).trim();
  const r = await q(`INSERT INTO locations (name) VALUES ($1) RETURNING id`, [nm]);
  res.json({ id: r.rows[0].id, name: nm });
});

app.delete("/api/admin/locations/:id", authRequired, adminRequired, async (req, res) => {
  const id = Number(req.params.id);
  if (!id) return res.status(400).json({ error: "invalid id" });

  const used = await q(`SELECT 1 FROM bookings WHERE location_id=$1 LIMIT 1`, [id]);
  if (used.rowCount > 0) return res.status(400).json({ error: "Standort hat bereits Buchungen und kann nicht gelöscht werden" });

  const usedCases = await q(`SELECT 1 FROM booking_cases WHERE location_id=$1 LIMIT 1`, [id]);
  if (usedCases.rowCount > 0) return res.status(400).json({ error: "Standort hat bereits Vorgänge und kann nicht gelöscht werden" });

  await q(`DELETE FROM locations WHERE id=$1`, [id]);
  res.json({ ok: true });
});

// ---------- DEPARTMENTS ----------
app.get("/api/departments", authRequired, async (req, res) => {
  res.json((await q(`SELECT id, name FROM departments ORDER BY name`)).rows);
});

app.post("/api/admin/departments", authRequired, adminRequired, async (req, res) => {
  const { name } = req.body || {};
  if (!name) return res.status(400).json({ error: "name required" });

  const nm = String(name).trim();
  const r = await q(`INSERT INTO departments (name) VALUES ($1) RETURNING id`, [nm]);
  res.json({ id: r.rows[0].id, name: nm });
});

app.delete("/api/admin/departments/:id", authRequired, adminRequired, async (req, res) => {
  const id = Number(req.params.id);
  if (!id) return res.status(400).json({ error: "invalid id" });

  await q(`DELETE FROM departments WHERE id=$1`, [id]);
  res.json({ ok: true });
});

// ---------- ENTREPRENEURS ----------
app.get("/api/entrepreneurs", authRequired, async (req, res) => {
  res.json((await q(`SELECT id, name, street, postal_code, city FROM entrepreneurs ORDER BY name`)).rows);
});

app.post("/api/entrepreneurs", authRequired, async (req, res) => {
  const name = safeTrim(req.body?.name);
  const street = safeTrim(req.body?.street);
  const postal_code = safeTrim(req.body?.postal_code);
  const city = safeTrim(req.body?.city);
  if (!name) return res.status(400).json({ error: "Name erforderlich" });

  const r = await q(
    `INSERT INTO entrepreneurs (name, street, postal_code, city)
     VALUES ($1, $2, $3, $4)
     ON CONFLICT (name) DO UPDATE
     SET street = COALESCE(EXCLUDED.street, entrepreneurs.street),
         postal_code = COALESCE(EXCLUDED.postal_code, entrepreneurs.postal_code),
         city = COALESCE(EXCLUDED.city, entrepreneurs.city)
     RETURNING id, name, street, postal_code, city`,
    [name, street, postal_code, city]
  );
  res.json(r.rows[0]);
});

app.get("/api/entrepreneurs/manage", authRequired, requirePermission("masterdata.entrepreneurs_manage"), async (req, res) => {
  res.json((await q(`SELECT id, name, street, postal_code, city FROM entrepreneurs ORDER BY name`)).rows);
});

app.post("/api/entrepreneurs/manage", authRequired, requirePermission("masterdata.entrepreneurs_manage"), async (req, res) => {
  const name = safeTrim(req.body?.name);
  const street = safeTrim(req.body?.street);
  const postal_code = safeTrim(req.body?.postal_code);
  const city = safeTrim(req.body?.city);
  if (!name) return res.status(400).json({ error: "Name erforderlich" });

  try {
    const r = await q(
      `INSERT INTO entrepreneurs (name, street, postal_code, city)
       VALUES ($1, $2, $3, $4)
       RETURNING id, name, street, postal_code, city`,
      [name, street, postal_code, city]
    );
    res.json(r.rows[0]);
  } catch (e) {
    if (e && e.code === "23505") return res.status(400).json({ error: "Unternehmer existiert bereits" });
    throw e;
  }
});

app.put("/api/entrepreneurs/manage/:id", authRequired, requirePermission("masterdata.entrepreneurs_manage"), async (req, res) => {
  const id = Number(req.params.id);
  if (!id) return res.status(400).json({ error: "invalid id" });

  const name = safeTrim(req.body?.name);
  const street = safeTrim(req.body?.street);
  const postal_code = safeTrim(req.body?.postal_code);
  const city = safeTrim(req.body?.city);
  if (!name) return res.status(400).json({ error: "Name erforderlich" });

  try {
    const r = await q(
      `UPDATE entrepreneurs
       SET name=$1, street=$2, postal_code=$3, city=$4
       WHERE id=$5
       RETURNING id, name, street, postal_code, city`,
      [name, street, postal_code, city, id]
    );
    if (!r.rowCount) return res.status(404).json({ error: "Unternehmer nicht gefunden" });
    res.json(r.rows[0]);
  } catch (e) {
    if (e && e.code === "23505") return res.status(400).json({ error: "Unternehmer existiert bereits" });
    throw e;
  }
});

app.delete("/api/entrepreneurs/manage/:id", authRequired, requirePermission("masterdata.entrepreneurs_manage"), async (req, res) => {
  const id = Number(req.params.id);
  if (!id) return res.status(400).json({ error: "invalid id" });
  await q(`DELETE FROM entrepreneurs WHERE id=$1`, [id]);
  res.json({ ok: true });
});

app.get("/api/admin/entrepreneurs", authRequired, adminRequired, async (req, res) => {
  res.json((await q(`SELECT id, name, street, postal_code, city FROM entrepreneurs ORDER BY name`)).rows);
});

app.post("/api/admin/entrepreneurs", authRequired, adminRequired, async (req, res) => {
  const name = safeTrim(req.body?.name);
  const street = safeTrim(req.body?.street);
  const postal_code = safeTrim(req.body?.postal_code);
  const city = safeTrim(req.body?.city);
  if (!name) return res.status(400).json({ error: "Name erforderlich" });

  try {
    const r = await q(
      `INSERT INTO entrepreneurs (name, street, postal_code, city)
       VALUES ($1, $2, $3, $4)
       RETURNING id, name, street, postal_code, city`,
      [name, street, postal_code, city]
    );
    res.json(r.rows[0]);
  } catch (e) {
    if (e && e.code === "23505") return res.status(400).json({ error: "Unternehmer existiert bereits" });
    throw e;
  }
});

app.put("/api/admin/entrepreneurs/:id", authRequired, adminRequired, async (req, res) => {
  const id = Number(req.params.id);
  if (!id) return res.status(400).json({ error: "invalid id" });

  const name = safeTrim(req.body?.name);
  const street = safeTrim(req.body?.street);
  const postal_code = safeTrim(req.body?.postal_code);
  const city = safeTrim(req.body?.city);

  if (!name) return res.status(400).json({ error: "Name erforderlich" });

  await q(
    `UPDATE entrepreneurs
     SET name=$1, street=$2, postal_code=$3, city=$4
     WHERE id=$5`,
    [name, street, postal_code, city, id]
  );
  res.json({ ok: true });
});

app.delete("/api/admin/entrepreneurs/:id", authRequired, adminRequired, async (req, res) => {
  const id = Number(req.params.id);
  if (!id) return res.status(400).json({ error: "invalid id" });
  await q(`DELETE FROM entrepreneurs WHERE id=$1`, [id]);
  res.json({ ok: true });
});

// ---------- ROLES (Admin) ----------
app.get("/api/admin/roles", authRequired, adminRequired, async (req, res) => {
  const rows = (await q(`SELECT id, name, permissions, created_at FROM roles ORDER BY name`)).rows;
  res.json(rows);
});

app.post("/api/admin/roles", authRequired, adminRequired, async (req, res) => {
  const { name, permissions } = req.body || {};
  if (!name || !String(name).trim()) return res.status(400).json({ error: "name required" });

  const roleName = String(name).trim();
  const perms = (permissions && typeof permissions === "object") ? permissions : {};

  try {
    const r = await q(
      `INSERT INTO roles (name, permissions) VALUES ($1, $2::jsonb)
       RETURNING id, name, permissions`,
      [roleName, JSON.stringify(perms)]
    );
    res.json(r.rows[0]);
  } catch (e) {
    if (e && e.code === "23505") return res.status(400).json({ error: "role name already exists" });
    throw e;
  }
});

app.put("/api/admin/roles/:id", authRequired, adminRequired, async (req, res) => {
  const id = Number(req.params.id);
  if (!id) return res.status(400).json({ error: "invalid id" });

  const { name, permissions } = req.body || {};
  const roleName = name ? String(name).trim() : null;
  const perms = (permissions && typeof permissions === "object") ? permissions : null;

  await q(
    `UPDATE roles
     SET name = COALESCE($1, name),
         permissions = COALESCE($2::jsonb, permissions)
     WHERE id=$3`,
    [roleName, perms ? JSON.stringify(perms) : null, id]
  );

  res.json({ ok: true });
});

app.delete("/api/admin/roles/:id", authRequired, adminRequired, async (req, res) => {
  const id = Number(req.params.id);
  if (!id) return res.status(400).json({ error: "invalid id" });

  const used = await q(`SELECT 1 FROM users WHERE role_id=$1 LIMIT 1`, [id]);
  if (used.rowCount > 0) return res.status(400).json({ error: "role is assigned to users" });

  await q(`DELETE FROM roles WHERE id=$1`, [id]);
  res.json({ ok: true });
});

// ---------- USERS (Admin) ----------
app.get("/api/admin/users", authRequired, async (req, res) => {
  if (req.user.role === "admin") {
    const rows = (await q(
      `SELECT id, username, role, location_id, role_id, is_active, created_at, email, fixed_department_id
       FROM users
       ORDER BY username`
    )).rows;
    return res.json(rows);
  }

  const perms = await getMyPermissions(req.user);
  if (!perms?.users?.view_department) return res.status(403).json({ error: "No Permissions" });

  const fixedDepartmentId = req.user.fixed_department_id;
  if (!fixedDepartmentId) return res.status(400).json({ error: "Kein fixe Abteilung gesetzt" });

  const rows = (await q(
    `SELECT id, username, role, location_id, role_id, is_active, created_at, email, fixed_department_id
     FROM users
     WHERE fixed_department_id=$1
     ORDER BY username`,
    [fixedDepartmentId]
  )).rows;
  return res.json(rows);
});

app.post("/api/admin/users", authRequired, adminRequired, async (req, res) => {
  const {
    username,
    password,
    location_id = null,
    role_id = null,
    email,
    fixed_department_id = null
  } = req.body || {};
  if (!username || !password) return res.status(400).json({ error: "username + password required" });

  const name = String(username).trim();
  if (name.length < 3) return res.status(400).json({ error: "username too short" });

  const hash = await bcrypt.hash(String(password), 10);
  const emailCheck = normalizeEmail(email);
  if (emailCheck && emailCheck.ok === false) return res.status(400).json({ error: emailCheck.msg });
  const roleId = (role_id === null || role_id === undefined || role_id === "") ? null : Number(role_id);
  if (!roleId) return res.status(400).json({ error: "business role required" });
  const roleExists = await q(`SELECT 1 FROM roles WHERE id=$1`, [roleId]);
  if (roleExists.rowCount === 0) return res.status(400).json({ error: "Business-Rolle nicht gefunden" });

  const fixedDepartmentId = (fixed_department_id === null || fixed_department_id === undefined || fixed_department_id === "")
    ? null
    : Number(fixed_department_id);
  if (fixedDepartmentId) {
    const depExists = await q(`SELECT 1 FROM departments WHERE id=$1`, [fixedDepartmentId]);
    if (depExists.rowCount === 0) return res.status(400).json({ error: "Abteilung nicht gefunden" });
  }

  try {
    const r = await q(
      `INSERT INTO users (username, password_hash, role, location_id, role_id, is_active, email, fixed_department_id)
       VALUES ($1,$2,$3,$4,$5,TRUE,$6,$7)
       RETURNING id, username, role, location_id, role_id, is_active, email, fixed_department_id`,
      [
        name,
        hash,
        "disponent",
        (location_id === null || location_id === undefined || location_id === "") ? null : Number(location_id),
        roleId,
        emailCheck?.email || null,
        fixedDepartmentId
      ]
    );
    res.json(r.rows[0]);
  } catch (e) {
    if (e && e.code === "23505") return res.status(400).json({ error: "username already exists" });
    throw e;
  }
});

app.put("/api/admin/users/:id", authRequired, adminRequired, async (req, res) => {
  const id = Number(req.params.id);
  if (!id) return res.status(400).json({ error: "invalid id" });

  const { location_id, is_active, role_id, email, fixed_department_id } = req.body || {};

  const updates = [];
  const values = [];
  let idx = 1;


  if (Object.prototype.hasOwnProperty.call(req.body || {}, "location_id")) {
    const locValue = (location_id === null || location_id === undefined || location_id === "") ? null : Number(location_id);
    updates.push(`location_id=$${idx++}`);
    values.push(locValue);
  }

  if (Object.prototype.hasOwnProperty.call(req.body || {}, "is_active")) {
    if (typeof is_active !== "boolean") return res.status(400).json({ error: "invalid is_active" });
    updates.push(`is_active=$${idx++}`);
    values.push(is_active);
  }

  if (Object.prototype.hasOwnProperty.call(req.body || {}, "role_id")) {
    const roleValue = (role_id === null || role_id === undefined || role_id === "") ? null : Number(role_id);
    if (!roleValue) return res.status(400).json({ error: "business role required" });
    const roleExists = await q(`SELECT 1 FROM roles WHERE id=$1`, [roleValue]);
    if (roleExists.rowCount === 0) return res.status(400).json({ error: "Business-Rolle nicht gefunden" });
    updates.push(`role_id=$${idx++}`);
    values.push(roleValue);
  }

  if (Object.prototype.hasOwnProperty.call(req.body || {}, "email")) {
    const emailCheck = normalizeEmail(email);
    if (emailCheck && emailCheck.ok === false) return res.status(400).json({ error: emailCheck.msg });
    updates.push(`email=$${idx++}`);
    values.push(emailCheck?.email || null);
  }

  if (Object.prototype.hasOwnProperty.call(req.body || {}, "fixed_department_id")) {
    const fixedDepartmentId = (fixed_department_id === null || fixed_department_id === undefined || fixed_department_id === "")
      ? null
      : Number(fixed_department_id);
    if (fixedDepartmentId) {
      const depExists = await q(`SELECT 1 FROM departments WHERE id=$1`, [fixedDepartmentId]);
      if (depExists.rowCount === 0) return res.status(400).json({ error: "Abteilung nicht gefunden" });
    }
    updates.push(`fixed_department_id=$${idx++}`);
    values.push(fixedDepartmentId);
  }

  if (updates.length === 0) return res.status(400).json({ error: "no changes" });

  values.push(id);
  await q(
    `UPDATE users SET ${updates.join(", ")} WHERE id=$${idx}`,
    values
  );

  res.json({ ok: true });
});

app.delete("/api/admin/users/:id", authRequired, adminRequired, async (req, res) => {
  const id = Number(req.params.id);
  if (!id) return res.status(400).json({ error: "invalid id" });
  if (id === req.user.id) return res.status(400).json({ error: "cannot delete yourself" });

  await q(`DELETE FROM users WHERE id=$1`, [id]);
  res.json({ ok: true });
});

app.post("/api/admin/users/:id/reset-password", authRequired, adminRequired, async (req, res) => {
  const id = Number(req.params.id);
  const { password } = req.body || {};
  if (!id) return res.status(400).json({ error: "invalid id" });
  if (!password) return res.status(400).json({ error: "password required" });

  const hash = await bcrypt.hash(String(password), 10);
  await q(`UPDATE users SET password_hash=$1 WHERE id=$2`, [hash, id]);
  res.json({ ok: true });
});

// ---------- WORKFLOW CASES (Status 1-4) ----------
app.get("/api/cases", authRequired, async (req, res) => {
  const location_id = Number(req.query.location_id || 0);
  const status = req.query.status ? Number(req.query.status) : null;
  const translogicaRaw = req.query.translogica_transferred;
  const translogicaFilter = translogicaRaw === "1" ? true : translogicaRaw === "0" ? false : null;
  const mine = String(req.query.mine || "") === "1";
  const search = (req.query.search || "").trim();

  if (!location_id) return res.status(400).json({ error: "location_id required" });

  const perms = await getMyPermissions(req.user);
  const canUseAllLocations = !!perms?.filters?.all_locations;
  const isAllLocations = location_id === -1;

  if (isAllLocations) {
    if (!canUseAllLocations) return res.status(403).json({ error: "Keine Berechtigung für Alle Standorte" });
  } else if (req.user.role !== "admin" && req.user.location_id && location_id !== Number(req.user.location_id) && !canUseAllLocations) {
    return res.status(403).json({ error: "Forbidden" });
  }

  const where = ["1=1"];
  const params = [];
  let idx = 1;
  if (!isAllLocations) {
    where.push(`c.location_id=$${idx}`);
    params.push(location_id);
    idx += 1;
  }

  if (status) { where.push(`c.status=$${idx}`); params.push(status); idx++; }
  if (translogicaFilter !== null) { where.push(`c.translogica_transferred=$${idx}`); params.push(translogicaFilter); idx++; }
  if (mine) { where.push(`c.created_by=$${idx}`); params.push(req.user.id); idx++; }

  if (search) {
    const like = `%${search}%`;
    const isNum = /^\d+$/.test(search);
    if (isNum) {
      where.push(`(c.id=$${idx} OR c.license_plate ILIKE $${idx + 1} OR COALESCE(c.entrepreneur,'') ILIKE $${idx + 1} OR COALESCE(c.note,'') ILIKE $${idx + 1})`);
      params.push(Number(search));
      params.push(like);
      idx += 2;
    } else {
      where.push(`(c.license_plate ILIKE $${idx} OR COALESCE(c.entrepreneur,'') ILIKE $${idx} OR COALESCE(c.note,'') ILIKE $${idx})`);
      params.push(like);
      idx += 1;
    }
  }

  const rows = (await q(
    `
    SELECT
      c.*,
      COALESCE(d.name, '(gelöschte Abteilung)') AS department,
      l.name AS location,
      COALESCE(u.username, '(gelöscht)') AS created_by_name,
      COALESCE(cu.username, '(gelöscht)') AS claimed_by_name,
      COALESCE(su.username, '(gelöscht)') AS submitted_by_name,
      COALESCE(au.username, '(gelöscht)') AS approved_by_name
    FROM booking_cases c
    LEFT JOIN departments d ON d.id=c.department_id
    JOIN locations l ON l.id=c.location_id
    LEFT JOIN users u ON u.id=c.created_by
    LEFT JOIN users cu ON cu.id=c.claimed_by
    LEFT JOIN users su ON su.id=c.submitted_by
    LEFT JOIN users au ON au.id=c.approved_by
    WHERE ${where.join(" AND ")}
    ORDER BY c.id DESC
    LIMIT 500
    `,
    params
  )).rows;

  res.json(rows);
});

app.get("/api/cases/:id", authRequired, async (req, res) => {
  const id = Number(req.params.id);
  if (!id) return res.status(400).json({ error: "invalid id" });

  const result = await q(
    `
    SELECT
      c.*,
      COALESCE(d.name, '(gelöschte Abteilung)') AS department,
      l.name AS location,
      COALESCE(u.username, '(gelöscht)') AS created_by_name,
      COALESCE(cu.username, '(gelöscht)') AS claimed_by_name,
      COALESCE(su.username, '(gelöscht)') AS submitted_by_name,
      COALESCE(au.username, '(gelöscht)') AS approved_by_name
    FROM booking_cases c
    LEFT JOIN departments d ON d.id=c.department_id
    JOIN locations l ON l.id=c.location_id
    LEFT JOIN users u ON u.id=c.created_by
    LEFT JOIN users cu ON cu.id=c.claimed_by
    LEFT JOIN users su ON su.id=c.submitted_by
    LEFT JOIN users au ON au.id=c.approved_by
    WHERE c.id=$1
    LIMIT 1
    `,
    [id]
  );

  if (result.rowCount === 0) return res.status(404).json({ error: "Not found" });
  const row = result.rows[0];

  if (req.user.role !== "admin" && req.user.location_id && Number(req.user.location_id) !== Number(row.location_id)) {
    return res.status(403).json({ error: "Forbidden" });
  }

  res.json(row);
});

app.post("/api/cases", authRequired, async (req, res) => {
  const perms = await getMyPermissions(req.user);
  if (!perms?.cases?.create) return res.status(403).json({ error: "Keine Berechtigung" });

  const { location_id, department_id, license_plate, entrepreneur, note, qty_in, qty_out, employee_code, product_type } = req.body || {};
  const locId = Number(location_id);
  const depId = Number(department_id);

  if (!locId || !depId) return res.status(400).json({ error: "location_id + department_id required" });

  const plateCheck = normalizePlate(license_plate);
  if (!plateCheck.ok) return res.status(400).json({ error: plateCheck.msg });

  const inQty = Number(qty_in ?? 0);
  const outQty = Number(qty_out ?? 0);
  if (!Number.isInteger(inQty) || inQty < 0) return res.status(400).json({ error: "qty_in invalid" });
  if (!Number.isInteger(outQty) || outQty < 0) return res.status(400).json({ error: "qty_out invalid" });
  if (inQty === 0 && outQty === 0) return res.status(400).json({ error: "qty_in oder qty_out muss > 0 sein" });

  const productTypeCheck = normalizeProductType(product_type);
  if (!productTypeCheck.ok) return res.status(400).json({ error: productTypeCheck.msg });

  const employeeCodeCheck = normalizeEmployeeCode(employee_code);
  if (employeeCodeCheck && employeeCodeCheck.ok === false) {
    return res.status(400).json({ error: employeeCodeCheck.msg });
  }

  if (req.user.role !== "admin" && req.user.location_id && locId !== Number(req.user.location_id)) {
    return res.status(403).json({ error: "Forbidden" });
  }

  const r = await q(
    `
    INSERT INTO booking_cases (location_id, department_id, created_by, status, license_plate, entrepreneur, note, qty_in, qty_out, employee_code, product_type)
    VALUES ($1,$2,$3,1,$4,$5,$6,$7,$8,$9,$10)
    RETURNING id
    `,
    [locId, depId, req.user.id, plateCheck.plate, safeTrim(entrepreneur), safeTrim(note), inQty, outQty, employeeCodeCheck?.code || null, productTypeCheck.productType]
  );

  if (safeTrim(entrepreneur)) {
    await q(
      `
      INSERT INTO entrepreneur_history (location_id, department_id, created_by, entrepreneur, license_plate, qty_in, qty_out, product_type)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
      `,
      [locId, depId, req.user.id, safeTrim(entrepreneur), plateCheck.plate, inQty, outQty, productTypeCheck.productType]
    );
  }

  const caseId = Number(r.rows[0].id);
  io.to(`loc:${locId}`).emit("casesUpdated", { location_id: locId });
  void createLocationStatus1Notifications({
    id: caseId,
    location_id: locId,
    created_by: req.user.id
  });
  res.json({ id: caseId });
});

app.post("/api/internal-transfers", authRequired, async (req, res) => {
  const perms = await getMyPermissions(req.user);
  if (!perms?.cases?.internal_transfer) return res.status(403).json({ error: "Keine Berechtigung" });

  const fromLocationIdRaw = req.body?.from_location_id;
  const fromLocationId = fromLocationIdRaw !== null && fromLocationIdRaw !== undefined && String(fromLocationIdRaw) !== ""
    ? Number(fromLocationIdRaw)
    : null;
  const toLocationId = Number(req.body?.to_location_id || 0);
  const qty = Number(req.body?.qty || 0);
  const note = safeTrim(req.body?.note);

  if (!toLocationId) return res.status(400).json({ error: "to_location_id required" });
  if (!Number.isInteger(qty) || qty <= 0) return res.status(400).json({ error: "qty invalid" });
  if (!note) return res.status(400).json({ error: "Notiz ist Pflicht" });
  if (fromLocationId && fromLocationId === toLocationId) return res.status(400).json({ error: "from_location_id und to_location_id dürfen nicht identisch sein" });

  const productTypeCheck = normalizeProductType(req.body?.product_type || "euro");
  if (!productTypeCheck.ok) return res.status(400).json({ error: productTypeCheck.msg });

  const userLocationLock = (req.user.role !== "admin" && req.user.location_id) ? Number(req.user.location_id) : null;
  if (userLocationLock) {
    if (toLocationId !== userLocationLock) return res.status(403).json({ error: "Forbidden" });
    if (fromLocationId && fromLocationId !== userLocationLock) return res.status(403).json({ error: "Forbidden" });
  }

  const groupId = randomUUID();
  let line = 1;

  if (fromLocationId) {
    await q(
      `
      INSERT INTO bookings (location_id, department_id, user_id, type, quantity, note, receipt_no, license_plate, entrepreneur, booking_group_id, line_no, product_type)
      VALUES ($1,NULL,$2,'OUT',$3,$4,NULL,NULL,NULL,$5,$6,$7)
      `,
      [fromLocationId, req.user.id, qty, note, groupId, line, productTypeCheck.productType]
    );
    line += 1;
  }

  await q(
    `
    INSERT INTO bookings (location_id, department_id, user_id, type, quantity, note, receipt_no, license_plate, entrepreneur, booking_group_id, line_no, product_type)
    VALUES ($1,NULL,$2,'IN',$3,$4,NULL,NULL,NULL,$5,$6,$7)
    `,
    [toLocationId, req.user.id, qty, note, groupId, line, productTypeCheck.productType]
  );

  if (fromLocationId) {
    io.to(`loc:${fromLocationId}`).emit("stockUpdated", { from_location_id: fromLocationId, to_location_id: toLocationId });
    io.to(`loc:${fromLocationId}`).emit("bookingsUpdated", { location_id: fromLocationId });
  }
  io.to(`loc:${toLocationId}`).emit("stockUpdated", { from_location_id: fromLocationId, to_location_id: toLocationId });
  io.to(`loc:${toLocationId}`).emit("bookingsUpdated", { location_id: toLocationId });

  res.json({ ok: true, mode: fromLocationId ? "transfer" : "in_only" });
});

app.put("/api/cases/:id", authRequired, async (req, res) => {
  const id = Number(req.params.id);
  if (!id) return res.status(400).json({ error: "invalid id" });

  const existing = await q(`SELECT * FROM booking_cases WHERE id=$1`, [id]);
  if (existing.rowCount === 0) return res.status(404).json({ error: "Not found" });
  const c = existing.rows[0];

  if (req.user.role !== "admin" && req.user.location_id && Number(req.user.location_id) !== Number(c.location_id)) {
    return res.status(403).json({ error: "Forbidden" });
  }

  const perms = await getMyPermissions(req.user);
  const { action, department_id, license_plate, entrepreneur, note, qty_in, qty_out, non_exchangeable_qty, employee_code, product_type, translogica_transferred } = req.body || {};

  const inQty = qty_in !== undefined ? Number(qty_in) : null;
  const outQty = qty_out !== undefined ? Number(qty_out) : null;
  const nonExchangeableQty = non_exchangeable_qty !== undefined ? Number(non_exchangeable_qty) : null;

  if (action === "edit") {
    if (!perms?.cases?.edit) return res.status(403).json({ error: "Keine Berechtigung" });
    if (![1, 2].includes(Number(c.status))) return res.status(400).json({ error: "Nur in Status 1/2 editierbar" });

    let plate = null;
    if (license_plate !== undefined) {
      const check = normalizePlate(license_plate);
      if (!check.ok) return res.status(400).json({ error: check.msg });
      plate = check.plate;
    }

    if (inQty !== null && (!Number.isInteger(inQty) || inQty < 0)) return res.status(400).json({ error: "qty_in invalid" });
    if (outQty !== null && (!Number.isInteger(outQty) || outQty < 0)) return res.status(400).json({ error: "qty_out invalid" });
    if (nonExchangeableQty !== null && (!Number.isInteger(nonExchangeableQty) || nonExchangeableQty < 0)) {
      return res.status(400).json({ error: "non_exchangeable_qty invalid" });
    }

    const nextInQty = inQty !== null ? inQty : Number(c.qty_in || 0);
    const nextOutQty = outQty !== null ? outQty : Number(c.qty_out || 0);
    const positiveSoll = Math.max(nextInQty - nextOutQty, 0);

    if (Number(c.status) !== 2 && nonExchangeableQty !== null) {
      return res.status(400).json({ error: "non_exchangeable_qty nur in Status 2 editierbar" });
    }
    if (Number(c.status) === 2 && nonExchangeableQty !== null && nonExchangeableQty > positiveSoll) {
      return res.status(400).json({ error: "non_exchangeable_qty darf positives Soll nicht übersteigen" });
    }

    const productTypeCheck = product_type !== undefined ? normalizeProductType(product_type) : null;
    if (productTypeCheck && !productTypeCheck.ok) return res.status(400).json({ error: productTypeCheck.msg });

    let employeeCode = undefined;
    if (Object.prototype.hasOwnProperty.call(req.body || {}, "employee_code")) {
      if (Number(c.status) !== 2) {
        return res.status(400).json({ error: "employee_code nur in Status 2 editierbar" });
      }
      const employeeCodeCheck = normalizeEmployeeCode(employee_code);
      if (employeeCodeCheck && !employeeCodeCheck.ok) return res.status(400).json({ error: employeeCodeCheck.msg });
      employeeCode = employeeCodeCheck?.code || null;
      if (perms?.cases?.require_employee_code && !employeeCode) {
        return res.status(400).json({ error: "Lagermitarbeiter (2-stellig) ist bei Status 2 Pflicht" });
      }
    }

    await q(
      `
      UPDATE booking_cases
      SET department_id = COALESCE($1, department_id),
          license_plate = COALESCE($2, license_plate),
          entrepreneur = COALESCE($3, entrepreneur),
          note = COALESCE($4, note),
          qty_in = COALESCE($5, qty_in),
          qty_out = COALESCE($6, qty_out),
          product_type = COALESCE($7, product_type),
          non_exchangeable_qty = CASE WHEN status = 2 THEN COALESCE($8, non_exchangeable_qty) ELSE non_exchangeable_qty END,
          employee_code = CASE
            WHEN status = 2 AND $9::boolean THEN $10
            ELSE employee_code
          END,
          updated_at = now()
      WHERE id=$11
      `,
      [
        department_id ? Number(department_id) : null,
        plate,
        safeTrim(entrepreneur),
        safeTrim(note),
        inQty,
        outQty,
        productTypeCheck?.productType || null,
        nonExchangeableQty,
        employeeCode !== undefined,
        employeeCode ?? null,
        id
      ]
    );

    io.to(`loc:${c.location_id}`).emit("casesUpdated", { location_id: c.location_id });
    return res.json({ ok: true });
  }

  if (action === "claim") {
    if (!perms?.cases?.claim) return res.status(403).json({ error: "Keine Berechtigung" });
    if (Number(c.status) !== 1) return res.status(400).json({ error: "Nur aus Status 1 möglich" });

    await q(
      `UPDATE booking_cases SET status=2, claimed_by=$1, claimed_at=now(), updated_at=now() WHERE id=$2`,
      [req.user.id, id]
    );

    io.to(`loc:${c.location_id}`).emit("casesUpdated", { location_id: c.location_id });
    return res.json({ ok: true });
  }

  if (action === "submit") {
    if (!perms?.cases?.submit) return res.status(403).json({ error: "Keine Berechtigung" });
    if (Number(c.status) !== 2) return res.status(400).json({ error: "Nur aus Status 2 möglich" });

    if (nonExchangeableQty !== null) {
      if (!Number.isInteger(nonExchangeableQty) || nonExchangeableQty < 0) {
        return res.status(400).json({ error: "non_exchangeable_qty invalid" });
      }
      const positiveSoll = Math.max(Number(c.qty_in || 0) - Number(c.qty_out || 0), 0);
      if (nonExchangeableQty > positiveSoll) {
        return res.status(400).json({ error: "non_exchangeable_qty darf positives Soll nicht übersteigen" });
      }
    }

    let employeeCode = c.employee_code || null;
    if (Object.prototype.hasOwnProperty.call(req.body || {}, "employee_code")) {
      const employeeCodeCheck = normalizeEmployeeCode(employee_code);
      if (employeeCodeCheck && !employeeCodeCheck.ok) return res.status(400).json({ error: employeeCodeCheck.msg });
      employeeCode = employeeCodeCheck?.code || null;
    }

    if (perms?.cases?.require_employee_code && !employeeCode) {
      return res.status(400).json({ error: "Lagermitarbeiter (2-stellig) ist bei Status 2 Pflicht" });
    }

    await q(
      `UPDATE booking_cases
       SET status=3,
           submitted_by=$1,
           submitted_at=now(),
           non_exchangeable_qty=COALESCE($2, non_exchangeable_qty),
           employee_code=$3,
           updated_at=now()
       WHERE id=$4`,
      [req.user.id, nonExchangeableQty, employeeCode, id]
    );

    await deleteNotificationsForCaseByTitle(id, "Aviso Standort (Status 1)");
    void createDepartmentStatus3Notifications({
      id,
      department_id: c.department_id,
      submitted_by: req.user.id
    });
    io.to(`loc:${c.location_id}`).emit("casesUpdated", { location_id: c.location_id });
    return res.json({ ok: true });
  }

  if (action === "approve") {
    if (!perms?.cases?.approve) return res.status(403).json({ error: "Keine Berechtigung" });
    if (Number(c.status) !== 3) return res.status(400).json({ error: "Nur aus Status 3 möglich" });

    const receipt_no = await nextReceiptNo(c.location_id);

    const client = await pool.connect();
    try {
      await client.query("BEGIN");

      await client.query(
        `UPDATE booking_cases
         SET status=4, approved_by=$1, approved_at=now(), receipt_no=$2, updated_at=now()
         WHERE id=$3`,
        [req.user.id, receipt_no, id]
      );

      const groupId = randomUUID();
      let line = 1;

      const nonExchangeableQty = Number(c.non_exchangeable_qty || 0);
      const bookedInQty = Math.max(Number(c.qty_in || 0) - nonExchangeableQty, 0);

      if (bookedInQty > 0) {
        await client.query(
          `
          INSERT INTO bookings (location_id, department_id, user_id, type, quantity, note, receipt_no, license_plate, entrepreneur, booking_group_id, line_no, product_type)
          VALUES ($1,$2,$3,'IN',$4,$5,$6,$7,$8,$9,$10,$11)
          `,
          [c.location_id, c.department_id, req.user.id, bookedInQty, c.note, receipt_no, c.license_plate, c.entrepreneur, groupId, line, c.product_type || "euro"]
        );
        line++;
      }

      if (Number(c.qty_out) > 0) {
        await client.query(
          `
          INSERT INTO bookings (location_id, department_id, user_id, type, quantity, note, receipt_no, license_plate, entrepreneur, booking_group_id, line_no, product_type)
          VALUES ($1,$2,$3,'OUT',$4,$5,$6,$7,$8,$9,$10,$11)
          `,
          [c.location_id, c.department_id, req.user.id, Number(c.qty_out), c.note, receipt_no, c.license_plate, c.entrepreneur, groupId, line, c.product_type || "euro"]
        );
      }

      await client.query("COMMIT");

      io.to(`loc:${c.location_id}`).emit("casesUpdated", { location_id: c.location_id });
      io.to(`loc:${c.location_id}`).emit("stockUpdated", { location_id: c.location_id });

      // ✅ NEU: Historie/Bookings live aktualisieren
      io.to(`loc:${c.location_id}`).emit("bookingsUpdated", {
        location_id: c.location_id,
        department_id: c.department_id,
        receipt_no
      });

      await deleteNotificationsForCaseByTitle(id, "Aviso Abteilung (Status 3)");

      return res.json({ ok: true, receipt_no });
    } catch (e) {
      await client.query("ROLLBACK");
      throw e;
    } finally {
      client.release();
    }
  }

  if (action === "set_translogica") {
    if (!perms?.bookings?.translogica) return res.status(403).json({ error: "Keine Berechtigung" });
    if (Number(c.status) !== 4) return res.status(400).json({ error: "Nur für gebuchte Vorgänge möglich" });
    if (typeof translogica_transferred !== "boolean") {
      return res.status(400).json({ error: "translogica_transferred must be boolean" });
    }

    await q(
      `UPDATE booking_cases SET translogica_transferred=$1, updated_at=now() WHERE id=$2`,
      [translogica_transferred, id]
    );

    io.to(`loc:${c.location_id}`).emit("casesUpdated", { location_id: c.location_id });
    return res.json({ ok: true });
  }

  if (action === "cancel") {
    if (!perms?.cases?.cancel) return res.status(403).json({ error: "Keine Berechtigung" });
    if (Number(c.status) === 4) return res.status(400).json({ error: "Gebuchte Vorgänge können nicht storniert werden" });
    if (Number(c.status) === 0) return res.status(400).json({ error: "Vorgang ist bereits storniert" });

    await q(
      `UPDATE booking_cases SET status=0, updated_at=now() WHERE id=$1`,
      [id]
    );

    await deleteNotificationsForCase(id);

    io.to(`loc:${c.location_id}`).emit("casesUpdated", { location_id: c.location_id });
    return res.json({ ok: true });
  }

  return res.status(400).json({ error: "unknown action" });
});

app.delete("/api/cases/:id", authRequired, async (req, res) => {
  const id = Number(req.params.id);
  if (!id) return res.status(400).json({ error: "invalid id" });

  const existing = await q(`SELECT * FROM booking_cases WHERE id=$1`, [id]);
  if (existing.rowCount === 0) return res.status(404).json({ error: "Not found" });
  const c = existing.rows[0];

  if (req.user.role !== "admin" && req.user.location_id && Number(req.user.location_id) !== Number(c.location_id)) {
    return res.status(403).json({ error: "Forbidden" });
  }

  const perms = await getMyPermissions(req.user);
  if (!perms?.cases?.delete) return res.status(403).json({ error: "Keine Berechtigung" });
  await q(`DELETE FROM booking_cases WHERE id=$1`, [id]);
  await deleteNotificationsForCase(id);

  io.to(`loc:${c.location_id}`).emit("casesUpdated", { location_id: c.location_id });
  res.json({ ok: true });
});

app.get("/api/cases/:id/receipt", authRequired, requirePermission("bookings.receipt"), async (req, res) => {
  const id = Number(req.params.id);
  if (!id) return res.status(400).json({ error: "invalid id" });

  const r = await q(
    `
    SELECT
      c.id, c.created_at, c.license_plate, c.entrepreneur, c.note,
      c.qty_in, c.qty_out, c.non_exchangeable_qty, c.employee_code, c.product_type, c.status, c.receipt_no,
      l.id AS location_id, l.name AS location,
      d.id AS department_id, COALESCE(d.name, '(gelöschte Abteilung)') AS department,
      COALESCE(u.username, '(gelöscht)') AS aviso_created_by,
      e.street AS entrepreneur_street,
      e.postal_code AS entrepreneur_postal_code,
      e.city AS entrepreneur_city
    FROM booking_cases c
    JOIN locations l ON l.id=c.location_id
    LEFT JOIN departments d ON d.id=c.department_id
    LEFT JOIN users u ON u.id=c.created_by
    LEFT JOIN entrepreneurs e ON e.name=c.entrepreneur
    WHERE c.id=$1
    LIMIT 1
    `,
    [id]
  );

  if (r.rowCount === 0) return res.status(404).json({ error: "Not found" });
  const row = r.rows[0];

  if (req.user.role !== "admin" && req.user.location_id && Number(req.user.location_id) !== Number(row.location_id)) {
    return res.status(403).json({ error: "Forbidden" });
  }

  const qty_in = Number(row.qty_in ?? 0);
  const qty_out = Number(row.qty_out ?? 0);
  const nonExchangeableQty = Number(row.non_exchangeable_qty ?? 0);
  const displayQtyIn = Math.max(qty_in - nonExchangeableQty, 0);
  const isBooked = Number(row.status) === 4 && !!row.receipt_no;
  const displayReceiptNo = isBooked ? row.receipt_no : await previewReceiptNo(row.location_id);
  const lines = [];
  if (displayQtyIn > 0) lines.push({ type: "IN", quantity: displayQtyIn });
  if (qty_out > 0) lines.push({ type: "OUT", quantity: qty_out });

  res.json({
    receipt_no: displayReceiptNo,
    provisional: !isBooked,
    created_at: row.created_at,
    location: row.location,
    department: row.department,
    username: row.aviso_created_by,
    aviso_created_by: row.aviso_created_by,
    employee_code: row.employee_code,
    license_plate: row.license_plate,
    entrepreneur: row.entrepreneur,
    entrepreneur_street: row.entrepreneur_street,
    entrepreneur_postal_code: row.entrepreneur_postal_code,
    entrepreneur_city: row.entrepreneur_city,
    note: row.note,
    qty_in: displayQtyIn,
    qty_out,
    non_exchangeable_qty: nonExchangeableQty,
    product_type: row.product_type || "euro",
    lines
  });
});

// ---------- STOCK ----------
app.get("/api/stock", authRequired, requirePermission("stock.view"), async (req, res) => {
  const mode = (req.query.mode || "location").toLowerCase();
  const productTypeCheck = normalizeProductType(req.query.product_type || "euro");
  if (!productTypeCheck.ok) return res.status(400).json({ error: productTypeCheck.msg });
  const productType = productTypeCheck.productType;
  const userLocationLock =
    (req.user.role !== "admin" && req.user.location_id) ? Number(req.user.location_id) : null;

  if (mode === "entrepreneur") {
    const rows = (await q(
      `
      SELECT
        COALESCE(b.entrepreneur, '') AS entrepreneur,
        COALESCE(SUM(CASE WHEN b.type='IN'  THEN b.quantity END),0)  AS ins,
        COALESCE(SUM(CASE WHEN b.type='OUT' THEN b.quantity END),0) AS outs,
        COALESCE(SUM(CASE WHEN b.type='IN'  THEN b.quantity END),0) -
        COALESCE(SUM(CASE WHEN b.type='OUT' THEN b.quantity END),0) AS saldo
      FROM bookings b
      JOIN entrepreneurs e ON e.name=b.entrepreneur
      WHERE b.entrepreneur IS NOT NULL AND b.entrepreneur <> '' AND COALESCE(b.product_type, 'euro')=$1
      GROUP BY COALESCE(b.entrepreneur, '')
      ORDER BY COALESCE(b.entrepreneur, '')
      `,
      [productType]
    )).rows;

    return res.json(rows);
  }

  if (mode === "overall") {
    // Extra-Schalter: Komplett Bestand nur wenn erlaubt
    const perms = await getMyPermissions(req.user);
    if (!perms?.stock?.overall) return res.status(403).json({ error: "Keine Berechtigung" });
    const sql = userLocationLock
      ? `
        SELECT d.id AS department_id, d.name AS department,
               COALESCE(SUM(CASE WHEN b.type='IN'  THEN b.quantity END),0)  AS ins,
               COALESCE(SUM(CASE WHEN b.type='OUT' THEN b.quantity END),0) AS outs,
               COALESCE(SUM(CASE WHEN b.type='IN'  THEN b.quantity END),0) -
               COALESCE(SUM(CASE WHEN b.type='OUT' THEN b.quantity END),0) AS saldo
        FROM departments d
        LEFT JOIN bookings b ON b.department_id=d.id AND b.location_id=$1 AND COALESCE(b.product_type, 'euro')=$2
        GROUP BY d.id
        ORDER BY d.name
      `
      : `
        SELECT d.id AS department_id, d.name AS department,
               COALESCE(SUM(CASE WHEN b.type='IN'  THEN b.quantity END),0)  AS ins,
               COALESCE(SUM(CASE WHEN b.type='OUT' THEN b.quantity END),0) AS outs,
               COALESCE(SUM(CASE WHEN b.type='IN'  THEN b.quantity END),0) -
               COALESCE(SUM(CASE WHEN b.type='OUT' THEN b.quantity END),0) AS saldo
        FROM departments d
        LEFT JOIN bookings b ON b.department_id=d.id AND COALESCE(b.product_type, 'euro')=$1
        GROUP BY d.id
        ORDER BY d.name
      `;

    return res.json((await q(sql, userLocationLock ? [userLocationLock, productType] : [productType])).rows);
  }

  if (mode === "location_total") {
    const sql = userLocationLock
      ? `
        SELECT l.id AS location_id, l.name AS location,
               COALESCE(SUM(CASE WHEN b.type='IN'  THEN b.quantity END),0)  AS ins,
               COALESCE(SUM(CASE WHEN b.type='OUT' THEN b.quantity END),0) AS outs,
               COALESCE(SUM(CASE WHEN b.type='IN'  THEN b.quantity END),0) -
               COALESCE(SUM(CASE WHEN b.type='OUT' THEN b.quantity END),0) AS saldo
        FROM locations l
        LEFT JOIN bookings b ON b.location_id=l.id AND COALESCE(b.product_type, 'euro')=$2
        WHERE l.id=$1
        GROUP BY l.id
        ORDER BY l.name
      `
      : `
        SELECT l.id AS location_id, l.name AS location,
               COALESCE(SUM(CASE WHEN b.type='IN'  THEN b.quantity END),0)  AS ins,
               COALESCE(SUM(CASE WHEN b.type='OUT' THEN b.quantity END),0) AS outs,
               COALESCE(SUM(CASE WHEN b.type='IN'  THEN b.quantity END),0) -
               COALESCE(SUM(CASE WHEN b.type='OUT' THEN b.quantity END),0) AS saldo
        FROM locations l
        LEFT JOIN bookings b ON b.location_id=l.id AND COALESCE(b.product_type, 'euro')=$1
        GROUP BY l.id
        ORDER BY l.name
      `;

    return res.json((await q(sql, userLocationLock ? [userLocationLock, productType] : [productType])).rows);
  }

  const location_id = Number(req.query.location_id || 0);
  if (!location_id) return res.status(400).json({ error: "location_id required for mode=location" });
  if (userLocationLock && location_id !== userLocationLock) return res.status(403).json({ error: "Forbidden" });

  const rows = (await q(
    `
    SELECT d.id AS department_id, d.name AS department,
           COALESCE(SUM(CASE WHEN b.type='IN'  THEN b.quantity END),0)  AS ins,
           COALESCE(SUM(CASE WHEN b.type='OUT' THEN b.quantity END),0) AS outs,
           COALESCE(SUM(CASE WHEN b.type='IN'  THEN b.quantity END),0) -
           COALESCE(SUM(CASE WHEN b.type='OUT' THEN b.quantity END),0) AS saldo
    FROM departments d
    LEFT JOIN bookings b ON b.department_id=d.id AND b.location_id=$1 AND COALESCE(b.product_type, 'euro')=$2
    GROUP BY d.id
    ORDER BY d.name
    `,
    [location_id, productType]
  )).rows;

  res.json(rows);
});

// ---------- BOOKINGS LIST (Historie aggregiert pro Beleg) ----------
app.get("/api/bookings", authRequired, requirePermission("bookings.view"), async (req, res) => {
  const location_id = Number(req.query.location_id || 0);
  const department_id = Number(req.query.department_id || 0);
  const date_from = (req.query.date_from || "").trim();
  const date_to = (req.query.date_to || "").trim();
  const entrepreneur = (req.query.entrepreneur || "").trim();
  const license_plate = (req.query.license_plate || "").trim();
  const receipt_no = (req.query.receipt_no || "").trim();
  const limitRaw = Number(req.query.limit || 20);
  const offsetRaw = Number(req.query.offset || 0);
  const limit = Number.isInteger(limitRaw) ? Math.min(Math.max(limitRaw, 1), 100) : 20;
  const offset = Number.isInteger(offsetRaw) ? Math.max(offsetRaw, 0) : 0;

  if (!location_id || !department_id) return res.status(400).json({ error: "location_id + department_id required" });

  const perms = await getMyPermissions(req.user);
  const canUseAllLocations = !!perms?.filters?.all_locations;
  const isAllLocations = location_id === -1;

  if (isAllLocations) {
    if (!canUseAllLocations) return res.status(403).json({ error: "Keine Berechtigung für Alle Standorte" });
  } else if (req.user.role !== "admin" && req.user.location_id && location_id !== Number(req.user.location_id) && !canUseAllLocations) {
    return res.status(403).json({ error: "Forbidden" });
  }

  const where = [`b.department_id=$1`];
  const params = [department_id];
  let idx = 2;

  if (!isAllLocations) {
    where.push(`b.location_id=$${idx}`);
    params.push(location_id);
    idx += 1;
  }

  if (date_from) { where.push(`b.created_at >= $${idx}::date`); params.push(date_from); idx++; }
  if (date_to) { where.push(`b.created_at < ($${idx}::date + interval '1 day')`); params.push(date_to); idx++; }
  if (entrepreneur) { where.push(`COALESCE(b.entrepreneur,'') ILIKE $${idx}`); params.push(`%${entrepreneur}%`); idx++; }
  if (license_plate) { where.push(`COALESCE(b.license_plate,'') ILIKE $${idx}`); params.push(`%${license_plate}%`); idx++; }
  if (receipt_no) { where.push(`b.receipt_no ILIKE $${idx}`); params.push(`%${receipt_no}%`); idx++; }

  params.push(limit + 1, offset);

  const rows = (await q(
    `
    SELECT
      MIN(b.id) AS id,
      MIN(b.created_at) AS created_at,
      b.receipt_no,
      MAX(b.license_plate) AS license_plate,
      MAX(b.entrepreneur) AS entrepreneur,
      MAX(b.note) AS note,
      MAX(COALESCE(u.username, '(gelöscht)')) AS "user",
      MAX(COALESCE(uc.username, '(gelöscht)')) AS aviso_created_by,
      MAX(COALESCE(ua.username, '(gelöscht)')) AS aviso_approved_by,
      MAX(bc.employee_code) AS employee_code,
      MAX(COALESCE(b.product_type, 'euro')) AS product_type,
      COALESCE(SUM(CASE WHEN b.type='IN'  THEN b.quantity END),0)  AS qty_in,
      COALESCE(SUM(CASE WHEN b.type='OUT' THEN b.quantity END),0) AS qty_out
    FROM bookings b
    LEFT JOIN users u ON u.id=b.user_id
    LEFT JOIN booking_cases bc ON bc.receipt_no=b.receipt_no
    LEFT JOIN users uc ON uc.id=bc.created_by
    LEFT JOIN users ua ON ua.id=bc.approved_by
    WHERE ${where.join(" AND ")}
    GROUP BY b.receipt_no
    ORDER BY MIN(b.id) DESC
    LIMIT $${idx}
    OFFSET $${idx + 1}
    `,
    params
  )).rows;

  const has_more = rows.length > limit;
  res.json({
    items: has_more ? rows.slice(0, limit) : rows,
    has_more,
    limit,
    offset
  });
});

// ---------- ENTREPRENEUR HISTORY ----------
app.get("/api/entrepreneur-history", authRequired, requirePermission("bookings.view"), async (req, res) => {
  const location_id = Number(req.query.location_id || 0);
  const department_id = Number(req.query.department_id || 0);
  const entrepreneur = (req.query.entrepreneur || "").trim();
  const license_plate = (req.query.license_plate || "").trim();

  if (!location_id) return res.status(400).json({ error: "location_id required" });

  const perms = await getMyPermissions(req.user);
  const canUseAllLocations = !!perms?.filters?.all_locations;
  const isAllLocations = location_id === -1;

  if (isAllLocations) {
    if (!canUseAllLocations) return res.status(403).json({ error: "Keine Berechtigung für Alle Standorte" });
  } else if (req.user.role !== "admin" && req.user.location_id && location_id !== Number(req.user.location_id) && !canUseAllLocations) {
    return res.status(403).json({ error: "Forbidden" });
  }

  const where = [`c.status <> 0`];
  const params = [];
  let idx = 1;
  if (!isAllLocations) {
    where.push(`c.location_id=$${idx}`);
    params.push(location_id);
    idx += 1;
  }

  if (department_id) { where.push(`c.department_id=$${idx}`); params.push(department_id); idx++; }
  if (entrepreneur) { where.push(`c.entrepreneur ILIKE $${idx}`); params.push(`%${entrepreneur}%`); idx++; }
  if (license_plate) { where.push(`c.license_plate ILIKE $${idx}`); params.push(`%${license_plate}%`); idx++; }

  const rows = (await q(
    `
    SELECT
      MAX(c.created_at) AS last_seen,
      c.entrepreneur,
      c.license_plate,
      COALESCE(c.product_type, 'euro') AS product_type,
      COALESCE(d.name, '(gelöschte Abteilung)') AS department,
      COALESCE(SUM(c.qty_in), 0) AS qty_in,
      COALESCE(SUM(c.qty_out), 0) AS qty_out,
      COALESCE(SUM(c.qty_in), 0) - COALESCE(SUM(c.qty_out), 0)
        - COALESCE(SUM(CASE WHEN (c.qty_in - c.qty_out) > 0 THEN c.non_exchangeable_qty ELSE 0 END), 0) AS soll
    FROM booking_cases c
    LEFT JOIN departments d ON d.id=c.department_id
    WHERE ${where.join(" AND ")}
      AND COALESCE(c.entrepreneur, '') <> ''
    GROUP BY c.entrepreneur, c.license_plate, COALESCE(c.product_type, 'euro'), COALESCE(d.name, '(gelöschte Abteilung)')
    ORDER BY MAX(c.created_at) DESC
    LIMIT 500
    `,
    params
  )).rows;

  res.json(rows);
});

app.get("/api/entrepreneur-history/plates", authRequired, requirePermission("bookings.view"), async (req, res) => {
  const location_id = Number(req.query.location_id || 0);
  const department_id = Number(req.query.department_id || 0);

  if (!location_id) return res.status(400).json({ error: "location_id required" });

  const perms = await getMyPermissions(req.user);
  const canUseAllLocations = !!perms?.filters?.all_locations;
  const isAllLocations = location_id === -1;

  if (isAllLocations) {
    if (!canUseAllLocations) return res.status(403).json({ error: "Keine Berechtigung für Alle Standorte" });
  } else if (req.user.role !== "admin" && req.user.location_id && location_id !== Number(req.user.location_id) && !canUseAllLocations) {
    return res.status(403).json({ error: "Forbidden" });
  }

  const where = ["1=1"];
  const params = [];
  let idx = 1;
  if (!isAllLocations) {
    where.push(`eh.location_id=$${idx}`);
    params.push(location_id);
    idx += 1;
  }
  if (department_id) { where.push(`eh.department_id=$${idx}`); params.push(department_id); idx++; }

  const rows = (await q(
    `
    SELECT DISTINCT eh.license_plate
    FROM entrepreneur_history eh
    WHERE ${where.join(" AND ")} AND eh.license_plate IS NOT NULL AND eh.license_plate <> ''
    ORDER BY eh.license_plate
    `,
    params
  )).rows;

  res.json(rows);
});

// ---------- BOOKINGS EDIT (Ledger) ----------
app.put("/api/bookings/:id", authRequired, requirePermission("bookings.edit"), async (req, res) => {
  const id = Number(req.params.id);
  if (!id) return res.status(400).json({ error: "invalid id" });

  const { quantity, note, entrepreneur, license_plate } = req.body || {};

  let qty = null;
  if (quantity !== undefined && quantity !== null && quantity !== "") {
    qty = Number(quantity);
    if (!Number.isInteger(qty) || qty <= 0) return res.status(400).json({ error: "quantity must be positive integer" });
  }

  let plate = null;
  if (license_plate !== undefined && license_plate !== null && String(license_plate).trim() !== "") {
    const check = normalizePlate(license_plate);
    if (!check.ok) return res.status(400).json({ error: check.msg });
    plate = check.plate;
  }

  const existing = await q(`SELECT id, location_id, department_id, receipt_no FROM bookings WHERE id=$1`, [id]);
  if (existing.rowCount === 0) return res.status(404).json({ error: "Not found" });

  const row = existing.rows[0];
  if (req.user.role !== "admin" && req.user.location_id && Number(req.user.location_id) !== Number(row.location_id)) {
    return res.status(403).json({ error: "Forbidden" });
  }

  await q(
    `
    UPDATE bookings
    SET quantity = COALESCE($1, quantity),
        note = COALESCE($2, note),
        entrepreneur = COALESCE($3, entrepreneur),
        license_plate = COALESCE($4, license_plate)
    WHERE id=$5
    `,
    [
      qty,
      (note !== undefined ? safeTrim(note) : null),
      (entrepreneur !== undefined ? safeTrim(entrepreneur) : null),
      plate,
      id
    ]
  );

  io.to(`loc:${row.location_id}`).emit("stockUpdated", { location_id: row.location_id });

  // ✅ NEU: Historie/Bookings live aktualisieren
  io.to(`loc:${row.location_id}`).emit("bookingsUpdated", {
    location_id: row.location_id,
    department_id: row.department_id,
    receipt_no: row.receipt_no
  });

  res.json({ ok: true });
});

// ---------- RECEIPT ----------
app.get("/api/receipt/:bookingId", authRequired, requirePermission("bookings.receipt"), async (req, res) => {
  const id = Number(req.params.bookingId);

  const base = await q(`SELECT receipt_no FROM bookings WHERE id=$1`, [id]);
  if (base.rowCount === 0) return res.status(404).json({ error: "Not found" });
  const receiptNo = base.rows[0].receipt_no;

  const r = await q(
    `
    SELECT
      b.id, b.receipt_no, b.license_plate, b.entrepreneur, b.type, b.quantity, b.note, b.created_at,
      COALESCE(b.product_type, 'euro') AS product_type,
      b.booking_group_id, b.line_no,
      COALESCE(u.username, '(gelöscht)') AS username,
      l.id AS location_id, l.name AS location,
      d.id AS department_id, COALESCE(d.name, '(gelöschte Abteilung)') AS department,
      e.street AS entrepreneur_street,
      e.postal_code AS entrepreneur_postal_code,
      e.city AS entrepreneur_city,
      COALESCE(uc.username, '(gelöscht)') AS aviso_created_by,
      bc.employee_code,
      bc.non_exchangeable_qty
    FROM bookings b
    LEFT JOIN users u ON u.id=b.user_id
    JOIN locations l ON l.id=b.location_id
    LEFT JOIN departments d ON d.id=b.department_id
    LEFT JOIN entrepreneurs e ON e.name=b.entrepreneur
    LEFT JOIN booking_cases bc ON bc.receipt_no=b.receipt_no
    LEFT JOIN users uc ON uc.id=bc.created_by
    WHERE b.receipt_no = $1
    ORDER BY COALESCE(b.line_no, 999999) ASC, b.id ASC
    `,
    [receiptNo]
  );

  const rows = r.rows;
  if (rows.length === 0) return res.status(404).json({ error: "Not found" });

  const locationId = Number(rows[0].location_id);
  if (req.user.role !== "admin" && req.user.location_id && locationId !== Number(req.user.location_id)) {
    return res.status(403).json({ error: "Forbidden" });
  }

  const first = rows[0];
  const lines = rows.map(x => ({ type: x.type, quantity: Number(x.quantity) }));

  const qty_in = lines.reduce((s, x) => s + (x.type === "IN" ? x.quantity : 0), 0);
  const qty_out = lines.reduce((s, x) => s + (x.type === "OUT" ? x.quantity : 0), 0);

  res.json({
    receipt_no: first.receipt_no,
    created_at: first.created_at,
    location: first.location,
    department: first.department,
    username: first.username,
    license_plate: first.license_plate,
    entrepreneur: first.entrepreneur,
    entrepreneur_street: first.entrepreneur_street,
    entrepreneur_postal_code: first.entrepreneur_postal_code,
    entrepreneur_city: first.entrepreneur_city,
    aviso_created_by: first.aviso_created_by,
    employee_code: first.employee_code,
    note: first.note,
    qty_in,
    qty_out,
    non_exchangeable_qty: Number(first.non_exchangeable_qty || 0),
    product_type: first.product_type || "euro",
    lines
  });
});

// ---------- EXPORTS ----------
app.get("/api/export/csv", authRequired, requirePermission("bookings.export"), async (req, res) => {
  const location_id = Number(req.query.location_id || 0);
  const department_id = Number(req.query.department_id || 0);
  if (!location_id || !department_id) return res.status(400).json({ error: "location_id + department_id required" });

  if (req.user.role !== "admin" && req.user.location_id && location_id !== Number(req.user.location_id)) {
    return res.status(403).json({ error: "Forbidden" });
  }

  const loc = await q(`SELECT name FROM locations WHERE id=$1`, [location_id]);
  const dep = await q(`SELECT name FROM departments WHERE id=$1`, [department_id]);
  if (loc.rowCount === 0 || dep.rowCount === 0) return res.status(404).json({ error: "location/department not found" });

  const rows = (await q(
    `
    SELECT b.created_at, b.receipt_no, b.license_plate, b.entrepreneur, COALESCE(u.username, '(gelöscht)') AS username, b.type, b.quantity, b.note
    FROM bookings b LEFT JOIN users u ON u.id=b.user_id
    WHERE b.location_id=$1 AND b.department_id=$2
    ORDER BY b.id ASC
    `,
    [location_id, department_id]
  )).rows;

  const parser = new Parser({ fields: ["created_at","receipt_no","license_plate","entrepreneur","username","type","quantity","note"] });
  const csv = parser.parse(rows);

  res.setHeader("Content-Type", "text/csv; charset=utf-8");
  res.setHeader("Content-Disposition", `attachment; filename="${loc.rows[0].name}-${dep.rows[0].name}-buchungen.csv"`);
  res.send(csv);
});

app.get("/api/export/xlsx", authRequired, requirePermission("bookings.export"), async (req, res) => {
  const location_id = Number(req.query.location_id || 0);
  const department_id = Number(req.query.department_id || 0);
  if (!location_id || !department_id) return res.status(400).json({ error: "location_id + department_id required" });

  if (req.user.role !== "admin" && req.user.location_id && location_id !== Number(req.user.location_id)) {
    return res.status(403).json({ error: "Forbidden" });
  }

  const loc = await q(`SELECT name FROM locations WHERE id=$1`, [location_id]);
  const dep = await q(`SELECT name FROM departments WHERE id=$1`, [department_id]);
  if (loc.rowCount === 0 || dep.rowCount === 0) return res.status(404).json({ error: "location/department not found" });

  const rows = (await q(
    `
    SELECT b.created_at, b.receipt_no, b.license_plate, b.entrepreneur, COALESCE(u.username, '(gelöscht)') AS username, b.type, b.quantity, b.note
    FROM bookings b LEFT JOIN users u ON u.id=b.user_id
    WHERE b.location_id=$1 AND b.department_id=$2
    ORDER BY b.id ASC
    `,
    [location_id, department_id]
  )).rows;

  const wb = new ExcelJS.Workbook();
  const ws = wb.addWorksheet("Buchungen");
  ws.columns = [
    { header: "Datum/Zeit", key: "created_at", width: 22 },
    { header: "Belegnr.", key: "receipt_no", width: 20 },
    { header: "Kennzeichen", key: "license_plate", width: 16 },
    { header: "Unternehmer", key: "entrepreneur", width: 22 },
    { header: "Benutzer", key: "username", width: 18 },
    { header: "Typ", key: "type", width: 8 },
    { header: "Menge", key: "quantity", width: 10 },
    { header: "Notiz", key: "note", width: 30 }
  ];
  ws.addRows(rows);

  res.setHeader("Content-Type", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet");
  res.setHeader("Content-Disposition", `attachment; filename="${loc.rows[0].name}-${dep.rows[0].name}-buchungen.xlsx"`);
  await wb.xlsx.write(res);
  res.end();
});


async function ensureRuntimeTables() {
  await q(`
    CREATE TABLE IF NOT EXISTS ip_preferences (
      ip_address TEXT PRIMARY KEY,
      theme TEXT NOT NULL DEFAULT 'light' CHECK (theme IN ('light','dark')),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );
  `);

  await q(`
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
  await q(`CREATE INDEX IF NOT EXISTS idx_user_notifications_user_created ON user_notifications(user_id, created_at DESC);`);

  await q(`ALTER TABLE booking_cases ADD COLUMN IF NOT EXISTS non_exchangeable_qty INTEGER NOT NULL DEFAULT 0;`);
}

const PORT = process.env.PORT || 3000;
ensureRuntimeTables()
  .then(() => {
    httpServer.listen(PORT, () => console.log(`API running on http://localhost:${PORT}`));
  })
  .catch((err) => {
    console.error("Startup failed:", err);
    process.exit(1);
  });
