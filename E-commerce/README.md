# 🛒 E-Commerce Project Stack — Real-Time Data Streaming

> **Stack de projet E-commerce** dédiée au traitement en temps réel d'événements de vente, combinant un générateur de transactions Python et un pipeline de streaming Apache Flink en Java.

---

## 📐 Architecture de la Stack

```
                                ┌──────────────────────────┐
                                │   Apache Kafka Broker    │
                                │   Topic:                 │
                                │   financial_transactions │
                                └─────────────┬────────────┘
                                              │
              ┌───────────────────────────────┴───────────────────────────────┐
              │                                                               │
              ▼                                                               ▼
┌────────────────────────────┐                              ┌─────────────────────────────────┐
│ 🐍 flink-ecommerce         │                              │ ☕ Flink-ecomerce-java          │
│ (Producteur Python)        │                              │ (Consommateur & Stream Engine)  │
│                            │                              │                                 │
│ • Faker (simule acheteurs, │                              │ • Désérialisation JSON          │
│   prix, catégories, etc.)  │                              │ • Agrégation glissante (KeyBy)  │
│ • confluent-kafka          │                              │ • Sinks JDBC (PostgreSQL)       │
│ • 1 transaction / sec      │                              │ • Sink Elasticsearch 7          │
└────────────────────────────┘                              └────────────────┬────────────────┘
                                                                             │
                                                     ┌───────────────────────┴───────────────────────┐
                                                     │                                               │
                                                     ▼                                               ▼
                                         ┌───────────────────────┐                       ┌───────────────────────┐
                                         │ PostgreSQL (OLTP/BI)  │                       │ Elasticsearch (Search)│
                                         │                       │                       │                       │
                                         │ • transactions        │                       │ • Index: transactions │
                                         │ • sales_per_category  │                       │ • Kibana dashboards   │
                                         │ • sales_per_day       │                       └───────────────────────┘
                                         │ • sales_per_month     │
                                         └───────────────────────┘
```

---

## 📁 Modules de la Stack

| Module | Langage | Description | Documentation |
|--------|---------|-------------|---------------|
| **[`flink-ecommerce/`](./flink-ecommerce/)** | 🐍 Python | Script de génération et publication des transactions simulées vers Kafka. | [Voir le README](./flink-ecommerce/README.md) |
| **[`Flink-ecomerce-java/`](./Flink-ecomerce-java/)** | ☕ Java | Job Flink qui consomme Kafka, agrège les données et les écrit dans PostgreSQL et Elasticsearch. | [Voir le README](./Flink-ecomerce-java/README.md) |

---

## 🚀 Guide de Démarrage Rapide

### 1. Démarrer les services d'infrastructure

Assurez-vous d'avoir démarré :
- **Kafka** (port `29092`)
- **PostgreSQL** (port `5432`)
- **Elasticsearch** (port `9200`)

### 2. Démarrer le producteur (Python)

```bash
cd flink-ecommerce
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python main.py
```

### 3. Démarrer le job Flink (Java)

```bash
cd Flink-ecomerce-java
mvn clean package -DskipTests
mvn exec:java -Dexec.mainClass="FlinkEcommerceJava.DataStreamJob"
```

---

## 📊 Schéma des Données & Tables

Les tables suivantes sont créées et alimentées automatiquement dans PostgreSQL par Flink :

- `transactions` : Transactions brutes.
- `sales_per_category` : Total des ventes ventilé par catégorie de produit et par date.
- `sales_per_day` : Total des ventes quotidien.
- `sales_per_month` : Total des ventes mensuel.
