
# Migration et Maintenance d’un Système de Stockage MongoDB

## Objectif du projet

Ce projet met en place un système de stockage de données basé sur MongoDB avec :

- Un conteneur MongoDB (Docker)
- Un script de migration (`loader.py`)
- Une initialisation automatique de la base
- Des tests unitaires avec couverture de code

Les données proviennent d’un fichier CSV : `healthcare_dataset.csv`.

---

## Architecture du projet

```
.
├── loader/                # Script de migration des données
├── initdb/                # Script d'initialisation Mongo
├── data/                  # Dataset CSV
├── test_unitaire/         # Tests unitaires
├── docker-compose.yml     # Orchestration des conteneurs
├── requirements.txt       # Dépendances principales
├── pytest.ini
└── README.md
```

---

## Lancement rapide

### 1) Configurer l’environnement

```bash
cp env.example .env
```

Modifier si nécessaire les variables (utilisateur, mot de passe, base, etc.).

### 2) Démarrer le projet

```bash
docker compose up -d --build
```

### 3) Vérifier les conteneurs

```bash
docker compose ps
```

### 4) Vérifier les logs

```bash
docker compose logs loader
docker compose logs mongo
```

Le loader doit afficher l’insertion des documents (ex: 54 966 documents insérés).

---

## Tests unitaires

### Installation des dépendances de test

```bash
pip install pytest pytest-cov mongomock
```

### Lancer les tests

```bash
pytest
```

### Couverture de code

```bash
pytest --cov=loader --cov-report=term-missing
```

---

## Connexion à MongoDB

MongoDB est exposé sur :

```
localhost:27017
```

Connexion via MongoDB Compass :

```
mongodb://appuser:<mot_de_passe>@localhost:27017/healthcare?authSource=admin
```

---

## Fonctionnement de la migration

1. Docker démarre MongoDB  
2. Le script `init-mongo.js` initialise la base  
3. `loader.py` lit le CSV, transforme les données et insère les documents dans MongoDB  
4. Les tests utilisent `mongomock` pour simuler MongoDB  

---

## Bonnes pratiques

- Le fichier `.env` n’est pas versionné  
- Les dossiers `__pycache__`, `.venv`, `.pytest_cache` sont ignorés  
- Les tests ne dépendent pas d’un Mongo réel  

---

## Auteur

Projet réalisé dans le cadre du module de maintenance et documentation d’un système de stockage de données.
