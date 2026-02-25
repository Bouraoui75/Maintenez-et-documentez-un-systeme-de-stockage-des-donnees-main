# Migration et Maintenance d'un Système de Stockage MongoDB

## Objectif du projet

Ce projet met en place un système de stockage de données basé sur
MongoDB avec :

-   Un conteneur MongoDB (Docker)
-   Un script de migration (`loader.py`)
-   Une initialisation automatique de la base
-   Des tests unitaires avec couverture de code

Les données proviennent d'un fichier CSV : `healthcare_dataset.csv`.

------------------------------------------------------------------------

## Architecture du projet

    .
    ├── loader/                
    ├── initdb/                
    ├── data/                  
    ├── test_unitaire/         
    ├── docker-compose.yml     
    ├── requirements.txt       
    ├── pytest.ini
    └── README.md

------------------------------------------------------------------------

# Schéma de la base de données

La base `healthcare` contient une collection principale :

### Collection : `patients`

Exemple de document :

``` json
{
  "_id": ObjectId("..."),
  "patient_id": "12345",
  "name": "John Doe",
  "age": 45,
  "gender": "Male",
  "admission_date": ISODate("2024-01-12T00:00:00Z")
}
```

### Indexation

-   Index par défaut sur `_id`
-   Index unique dynamique sur la clé primaire détectée (ex :
    `patient_id`)
-   Garantie d'unicité et d'idempotence

------------------------------------------------------------------------

# Authentification et rôles utilisateurs

L'authentification MongoDB est activée.

Les utilisateurs sont créés automatiquement via `init-mongo.js`.

  Utilisateur    Rôle
  -------------- ----------------------------------
  appuser        readWrite sur healthcare
  readOnlyUser   read sur healthcare
  supportUser    read, readWrite, dbAdmin
  adminUser      readWrite, dbAdmin, clusterAdmin
  admin          root (base admin)

Principe du moindre privilège respecté.

------------------------------------------------------------------------

# Sécurité des mots de passe

MongoDB ne stocke jamais les mots de passe en clair.

Les mots de passe sont automatiquement : - Hachés via SCRAM - Basés sur
SHA-256 - Non réversibles

------------------------------------------------------------------------

## Lancement rapide

### 1) Configurer l'environnement

``` bash
cp env.example .env
```

### 2) Démarrer le projet

``` bash
docker compose up -d --build
```

### 3) Vérifier les conteneurs

``` bash
docker compose ps
```

### 4) Vérifier les logs

``` bash
docker compose logs loader
docker compose logs mongo
```

------------------------------------------------------------------------

## Tests unitaires

``` bash
pip install pytest pytest-cov mongomock
pytest --cov=loader --cov-report=term-missing
```

Couverture actuelle : 92% sur loader/loader.py

------------------------------------------------------------------------

# CE2 - Qualité de l'implémentation

-   Configuration externalisée via `.env`
-   Séparation des responsabilités (Docker / Loader / Mongo)
-   Pipeline idempotente (bulk_upsert + index unique)
-   Tests unitaires (92% de couverture)
-   Authentification sécurisée et gestion des rôles
-   Logging structuré

------------------------------------------------------------------------

## Auteur

Projet réalisé dans le cadre du module de maintenance et documentation
d'un système de stockage de données.
