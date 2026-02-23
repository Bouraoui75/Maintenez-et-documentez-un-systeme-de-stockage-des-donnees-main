
# MongoDB Cheat Sheet – Projet Healthcare (Docker)

## 1) Connexion au conteneur Mongo
```bash
docker exec -it mongodb mongosh -u admin -p adminpass --authenticationDatabase admin
```

## 2) Utilisateurs & rôles
### Lister tous les utilisateurs
```javascript
use healthcare
show users
use admin
show users
```

### Détails d’un utilisateur
```javascript
db.getUser("appuser")
db.getUser("supportUser")
db.getUser("adminUser")
```

## 3) Bases & collections
```javascript
show dbs
use healthcare
show collections
db.getCollectionInfos()
```

## 4) Documents (patients)
```javascript
db.patients.findOne()
db.patients.countDocuments({})
db.patients.find({}, { patient_id:1, first_name:1, last_name:1, _id:0 }).limit(5)
```

## 5) Index
```javascript
db.patients.getIndexes()
db.patients.stats()
```

## 6) Santé serveur & DB
```javascript
db.stats()
db.serverStatus()
db.version()
```

## 7) Commandes Docker utiles
```bash
docker compose ps
docker compose logs -f mongodb
docker compose logs -f loader
docker exec -it mongodb bash
```

## 8) Sauvegarde & restauration
### Sauvegarde
```bash
docker exec -it mongodb mongodump --db healthcare --out /backup
docker cp mongodb:/backup ./backup
```

### Restauration
```bash
docker exec -it mongodb mongorestore --drop /backup
```
