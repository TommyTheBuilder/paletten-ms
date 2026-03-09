// middleware_permissions.js
const { pool } = require("./db_pg");

// Defaults (wie in server.js), damit ältere Rollen nicht plötzlich alles verlieren
const DEFAULTS = {
  bookings: { create: true, view: true, export: true, receipt: true, edit: false, delete: false, translogica: false },
  stock: { view: true, overall: true },
  cases: { create: true, internal_transfer: false, claim: false, edit: false, submit: false, approve: false, cancel: false, delete: false, require_employee_code: false },
  filters: { all_locations: false },
  masterdata: { manage: false, entrepreneurs_manage: false },
  users: { manage: false, view_department: false },
  roles: { manage: false },
  admin: { full_access: false }
};

function isObject(x) {
  return x && typeof x === "object" && !Array.isArray(x);
}

function deepMerge(base, override) {
  const out = { ...base };
  for (const k of Object.keys(override || {})) {
    if (isObject(override[k])) out[k] = deepMerge(base[k] || {}, override[k]);
    else out[k] = override[k];
  }
  return out;
}

function hasPerm(perms, permPath) {
  if (perms?.admin?.full_access) return true;
  const parts = String(permPath || "").split(".");
  let cur = perms;
  for (const p of parts) {
    if (!cur || typeof cur !== "object") return false;
    cur = cur[p];
  }
  return cur === true;
}

async function loadPermissionsForUser(user) {
  // Admin: immer alles
  if (user?.role === "admin") {
    return {
      bookings: { create: true, view: true, export: true, receipt: true, edit: true, delete: true, translogica: true },
      stock: { view: true, overall: true },
      cases: { create: true, internal_transfer: true, claim: true, edit: true, submit: true, approve: true, cancel: true, delete: true, require_employee_code: false },
      filters: { all_locations: true },
      masterdata: { manage: true, entrepreneurs_manage: true },
      users: { manage: true, view_department: true },
      roles: { manage: true },
      admin: { full_access: true }
    };
  }

  // Wenn keine Role-ID gesetzt ist -> Defaults
  if (!user?.role_id) {
    return { ...DEFAULTS };
  }

  // Rollenrechte aus DB holen
  const r = await pool.query(`SELECT permissions FROM roles WHERE id=$1`, [Number(user.role_id)]);
  const raw = (r.rowCount ? r.rows[0].permissions : {}) || {};
  return deepMerge(DEFAULTS, raw);
}

function requirePermission(permissionPath) {
  return async (req, res, next) => {
    try {
      // Admin immer durchlassen (wichtig!)
      if (req.user?.role === "admin") return next();

      // Falls bereits geladen (Caching pro Request/User)
      if (!req.user.permissions) {
        req.user.permissions = await loadPermissionsForUser(req.user);
      }

      if (!hasPerm(req.user.permissions, permissionPath)) {
        return res.status(403).json({ error: "No Permissions" });
      }

      return next();
    } catch (e) {
      console.error("requirePermission error:", e);
      return res.status(500).json({ error: "Permission check failed" });
    }
  };
}

module.exports = {
  requirePermission
};
