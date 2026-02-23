// Sélection de la base de travail (DB métier)
db = db.getSiblingDB(process.env.MONGO_DB || "healthcare");

// === Utilisateur applicatif (readWrite) ===
db.createUser({
  user: process.env.APP_USER || "appuser",
  pwd: process.env.APP_PASSWORD || "appsecret",
  roles: [
    { role: "readWrite", db: process.env.MONGO_DB || "healthcare" }
  ]
});

// === Utilisateur lecture seule (read) ===
db.createUser({
  user: process.env.READONLY_USER || "readOnlyUser",
  pwd: process.env.READONLY_PASSWORD || "lectureseule",
  roles: [
    { role: "read", db: process.env.MONGO_DB || "healthcare" }
  ]
});

// === Utilisateur support (small-admin : support technique) ===
db.createUser({
  user: process.env.SUPPORT_USER || "supportUser",
  pwd: process.env.SUPPORT_PASSWORD || "supportpassword",
  roles: [
    { role: "read", db: process.env.MONGO_DB || "healthcare" },
    { role: "readWrite", db: process.env.MONGO_DB || "healthcare" },
    { role: "dbAdmin", db: process.env.MONGO_DB || "healthcare" } // dbAdmin : Gérer les collections, les index et les utilisateurs de la base de données.
  ]
});

// === Admin complet (SUPER-admin: Create,Read,Write,Delete) ===
db.createUser({
  user: process.env.ADMIN_USER || "adminUser",
  pwd: process.env.ADMIN_PASSWORD || "adminpassword",
  roles: [
    { role: "readWrite", db: process.env.MONGO_DB || "healthcare" },
    { role: "dbAdmin", db: process.env.MONGO_DB || "healthcare" }, // dbAdmin : Gérer les collections, les index et les utilisateurs de la base de données.
    { role: "clusterAdmin", db: "admin" } // clusterAdmin doit être créé dans "admin" : Gérer la configuration du cluster, les utilisateurs et les rôles (clusterAdmin).
  ]
});