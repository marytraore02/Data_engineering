# 📊 Data Engineering — Central Repository

> **Dépôt central** regroupant l'ensemble de mes projets, architectures et pipelines de **Data Engineering** (Streaming temps réel, Batch processing, Data Lakehouse, Orchestration ETL, etc.).
>
> 👤 **Auteur** : Mary Traore ([@marytraore02](https://github.com/marytraore02))  
> 🛠️ **Technologies** : Apache Flink, Apache Kafka, Apache Spark, PostgreSQL, Elasticsearch, Docker, Python, Java.

---

## 🏗️ Structure du Monorepo

Chaque dossier à la racine représente une **stack de projet** ou un **domaine fonctionnel** indépendant.

```
Data_engineering/
│
├── 🛒 E-commerce/                    ← Stack de Streaming Temps Réel E-commerce
│   ├── 🐍 flink-ecommerce/          ← Producteur Kafka de transactions (Python + Faker)
│   ├── ☕ Flink-ecomerce-java/      ← Engine Flink (Kafka → Flink → PostgreSQL + ES)
│   └── 📖 README.md                 ← Documentation complète de la stack E-commerce
│
├── 🚀 [Futur Projet 1]/              ← Prochainement (ex: Data Lakehouse / Iceberg)
├── 🔄 [Futur Projet 2]/              ← Prochainement (ex: Pipeline Batch Spark / Airflow)
│
├── .gitignore                       ← Exclusion globale pour tout le repo
├── LICENSE                          ← Licence Apache 2.0
└── README.md                        ← Documentation centrale du repo
```

---

## 📚 Stacks de Projets Disponibles

### 1. 🛒 [E-commerce — Streaming Temps Réel](./E-commerce/)

Pipeline complet de streaming d'événements e-commerce à haut débit.

- **Producteur** : Script Python simulant des transactions d'achat en temps réel et les publiant sur Apache Kafka.
- **Traitement & Analytics** : Application Apache Flink en Java consommant Kafka, réalisant des agrégations glissantes (ventes/jour, ventes/catégorie, ventes/mois) et persistant en temps réel dans **PostgreSQL** et **Elasticsearch**.

👉 **[Accéder au projet E-commerce](./E-commerce/)**

---

## 🧩 Technologies Clés de l'Écosystème

| Catégorie | Technologies Utilisées |
|-----------|------------------------|
| **Ingestion & Streaming** | Apache Kafka, Apache Flink, Spark Streaming |
| **Stockage & Bases de données** | PostgreSQL, Elasticsearch, MongoDB, MySQL |
| **Traitement Batch** | Apache Spark, PySpark |
| **Orchestration** | Apache Airflow _(à venir)_ |
| **Conteneurisation** | Docker, Docker Compose |
| **Langages** | Java (8/11/17), Python (3.8+), SQL |

---

## 🛠️ Convention d'Ajout d'un Nouveau Projet

Pour maintenir ce dépôt propre et structuré, chaque nouveau projet doit respecter la structure suivante :

```
nom-de-la-stack/
├── README.md              # Présentation, architecture et guide de démarrage
├── docker-compose.yml     # Infrastructure Docker locale du projet
├── .gitignore             # (Optionnel) Exclusions spécifiques au projet
└── [modules/composants]   # Code source (Java, Python, scripts SQL, DAGs Airflow...)
```

---

## 🚀 Démarrage Rapide

```bash
# Cloner le dépôt central
git clone https://github.com/marytraore02/Data_engineering.git
cd Data_engineering

# Naviguer dans le projet de votre choix, par exemple E-commerce :
cd E-commerce
```

---

## 📄 Licence

Ce projet est sous licence **Apache License 2.0** — voir le fichier [LICENSE](LICENSE) pour plus de détails.
