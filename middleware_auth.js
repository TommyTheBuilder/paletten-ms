const jwt = require("jsonwebtoken");
const { pool } = require("./db_pg");

const JWT_SECRET = process.env.JWT_SECRET || "CHANGE_ME_SUPER_SECRET";
if (JWT_SECRET === "CHANGE_ME_SUPER_SECRET" && process.env.ALLOW_INSECURE_JWT !== "true") {
  throw new Error("JWT_SECRET must be set (or explicitly set ALLOW_INSECURE_JWT=true for local dev only)");
}

function authRequired(req, res, next) {
  const header = req.headers.authorization || "";
  const token = header.startsWith("Bearer ") ? header.slice(7) : null;
  if (!token) return res.status(401).json({ error: "Not authenticated" });

  try {
    req.user = jwt.verify(token, JWT_SECRET);
    next();
  } catch {
    return res.status(401).json({ error: "Invalid token" });
  }
}

async function hasAdminFullAccess(user) {
  if (user?.role === "admin") return true;
  if (!user?.role_id) return false;
  const r = await pool.query(`SELECT permissions FROM roles WHERE id=$1`, [Number(user.role_id)]);
  const perms = (r.rowCount ? r.rows[0].permissions : {}) || {};
  return perms?.admin?.full_access === true;
}

async function adminRequired(req, res, next) {
  try {
    if (await hasAdminFullAccess(req.user)) return next();
    return res.status(403).json({ error: "Admin only" });
  } catch (e) {
    console.error("adminRequired error:", e);
    return res.status(500).json({ error: "Permission check failed" });
  }
}

module.exports = { authRequired, adminRequired, JWT_SECRET };
