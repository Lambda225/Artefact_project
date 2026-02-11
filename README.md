# 🛒 E-commerce Sales Data Pipeline

**Data Engineering Mini-Project – Artefact CI**

## 📌 Contexte

Ce projet s’inscrit dans le cadre d’un challenge technique Data Engineer.  
L’objectif est de concevoir et déployer un **pipeline de données complet**, depuis l’analyse exploratoire jusqu’à l’orchestration avec **Apache Airflow**, en s’appuyant sur des technologies standards du Data Engineering.

Le jeu de données représente les ventes d’un site e-commerce et est stocké dans un **bucket MinIO**. Les données sont ensuite **ingérées, normalisées et stockées dans PostgreSQL**, selon un modèle relationnel avancé.

---

## 🎯 Objectifs du projet

- Analyser un jeu de données métier réel
- Concevoir un modèle de données normalisé (**3FN et DKNF**)
- Implémenter le modèle dans **PostgreSQL**
- Déployer l’infrastructure avec **Docker & Docker Compose**
- Développer un script Python d’ingestion **idempotent**
- Orchestrer l’ingestion avec **Apache Airflow**
- Structurer un projet de manière professionnelle

---

## 🧱 Architecture globale

```text
CSV (Minio / S3)
      ↓
Tables DKNF (PostgreSQL)
      ↓
Vue analytique en étoile
      ↓
Exploitation BI / Analytics
```

---

## 🛠️ Technologies utilisées

- **Langage** : Python 3
- **Base de données** : PostgreSQL
- **Stockage objet** : MinIO (S3 compatible)
- **Orchestration** : Apache Airflow 3.x
- **Conteneurisation** : Docker & Docker Compose
- **SQL** : PostgreSQL compatible

---

## 📁 Structure du projet

```text
├── 📁config/
├── 📁dags/
│ └── 📄fashion_sales_dag.py
├── 📁ingestion/
│ └── 📄main.py
├── 📁logs/
├── 📁minio-data/
│ └── 📄fashion_store_sales _ Data Eng.csv
├── 📁plugins/
├── 📁sql/
│ ├── 📄01_create_tables.sql
│ └── 📄02_view_sales_star.sql
├── 📄analysis.ipynb
├── 📄docker-compose.yaml
└── 📄README.md
```

---

## 🔍 Analyse exploratoire

Une analyse exploratoire a été réalisée afin de :

- comprendre la structure des données
- identifier les entités métier
- détecter les redondances et anomalies
- préparer la phase de modélisation

📄 Livrable : `analysis.ipynb`

---

## 🧩 Modélisation des données

- Normalisation jusqu’à la **3ᵉ forme normale (3FN)**
- Poursuite de la normalisation jusqu’à la **DKNF**
- Définition claire des :
  - tables
  - clés primaires
  - clés étrangères
- Création d’une **vue en étoile** pour faciliter l’analyse

📄 Scripts SQL disponibles dans le dossier `sql/`

---

## 🐳 Lancer Docker

### Lancer PostgreSQL, MinIO, PGadmin & Airflow

```bash
docker compose up
```

- Les tables DKNF et la vue sont créées automatiquement au démarrage
- Le fichier source est uploadé dans le bucket MinIO `folder-source`
- Initialisation automatique des variables et connexions requises par le DAG Airflow
- Déclenchement automatique du DAG chargé d’ingérer les ventes du jour courant et peut être exécuter manuellement avec en paramètre:

```json
{ "run_date": "20250616" }
```

Accès UI :

- http://localhost:8080 -> accès à Airflow
- http://localhost:5050 -> accès à PGadmin
- http://localhost:9000 -> accès à Minio

Login :

- Airflow -> ( login : airflow, password : airflow )
- Minio -> ( login : minioadmin, password : minioadmin123 )
- PGadmin -> ( login : admin@local.com, password : adminpassword )
- postgres -> ( user : airflow, password: airflow, DB: artefect_db, host: postgres or localhost:5432 )

## 🐍Script Python d’ingestion

- Prend une date en paramètre : YYYYMMDD
- Lit le fichier source depuis MinIO
- Filtre les ventes correspondant à la date
- Alimente les tables PostgreSQL normalisées
- Idempotence garantie
- Gestion des erreurs (date, connexion, insertion)
- Logging intégré

**NB:** _Avant de lancer le script créer un fichier `.env` à la racine du projet contenant les lignes suivantes:_

```bash
AIRFLOW_UID=50000

# Minio
MINIO_ENDPOINT=localhost:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin123
MINIO_SECURE=false
MINIO_BUCKET=folder-source
MINIO_OBJECT_KEY=fashion_store_sales _ Data Eng.csv

# Postgres
PG_HOST=localhost
PG_PORT=5432
PG_DB=artefect_db
PG_USER=airflow
PG_PASSWORD=airflow
```

**Recommander:** créer un environnement virtuelle python avant d'installer les dependants pour le lancement du script :

```bash
pip install -r requirements.txt
```

### Exemple d’exécution

```bash
python ingestion/main.py 20250616
```

**NB:** Assurer vous d'avoir tous les containers docker qui tourne bien avant le lancement du script

## ✅ Points clés techniques

- Pipeline reproductible et idempotent
- Séparation claire des responsabilités
- SQL robuste avec contraintes d’intégrité
- Dockerisation complète
- Orchestration fiable et maintenable

## 👤 Auteur

Projet réalisé par :

**Kouamé Antonio Parfait**

Data Engineer
