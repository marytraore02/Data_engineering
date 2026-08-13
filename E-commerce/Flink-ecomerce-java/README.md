# 🚀 Flink E-Commerce — Pipeline de Streaming Temps Réel (Java)

> **Pipeline de Data Engineering** qui ingère des transactions e-commerce depuis Apache Kafka, les transforme avec Apache Flink, puis les persiste simultanément dans **PostgreSQL** et **Elasticsearch** pour l'analytique et la recherche.

---

## 📐 Architecture Globale

```
┌─────────────────┐     ┌──────────────────────┐     ┌──────────────────┐
│   Kafka Topic   │────▶│  Apache Flink Job     │────▶│   PostgreSQL     │
│  (financial_    │     │  (DataStreamJob.java) │     │  - transactions  │
│  transactions)  │     │                       │     │  - sales/categ.  │
└─────────────────┘     │  Désérialisation JSON │     │  - sales/jour    │
                        │  Agrégation temps réel│     │  - sales/mois    │
                        │  Fenêtrage par clé    │     └──────────────────┘
                        │                       │
                        │                       │────▶┌──────────────────┐
                        └──────────────────────┘     │  Elasticsearch   │
                                                     │  Index:           │
                                                     │  "transactions"   │
                                                     └──────────────────┘
```

## 🧩 Stack Technologique

| Composant          | Technologie                  | Version   |
|--------------------|------------------------------|-----------|
| **Stream Engine**  | Apache Flink                 | `1.18.0`  |
| **Message Broker** | Apache Kafka                 | —         |
| **Base de données**| PostgreSQL                   | —         |
| **Moteur Search**  | Elasticsearch                | `7.x`     |
| **Build Tool**     | Apache Maven                 | `3.x+`    |
| **Langage**        | Java                         | `1.8+`    |
| **Annotations**    | Lombok                       | `1.18.38` |
| **Logging**        | Log4j 2                      | `2.17.1`  |

---

## 📁 Structure du Projet

```
Flink-ecomerce-java/
├── pom.xml                                  # Configuration Maven & dépendances
├── .gitignore                               # Fichiers et dossiers ignorés par Git
├── .mvn/                                    # Maven Wrapper
├── src/
│   └── main/
│       ├── java/FlinkEcommerceJava/
│       │   ├── DataStreamJob.java           # 🎯 Point d'entrée — Pipeline principal
│       │   ├── Deserializer/
│       │   │   └── JSONKeyValueDeserializationSchema.java  # Désérialisation Kafka → Transaction
│       │   ├── Dto/
│       │   │   ├── Transaction.java         # DTO transaction complète
│       │   │   ├── SalesPerCategory.java    # DTO agrégation par catégorie
│       │   │   ├── SalesPerDay.java         # DTO agrégation par jour
│       │   │   └── SalesPerMonth.java       # DTO agrégation par mois
│       │   └── utils/
│       │       └── JsonUtil.java            # Sérialisation JSON pour Elasticsearch
│       └── resources/
│           └── log4j2.properties            # Configuration du logging
└── target/                                  # Artefacts de build (généré)
```

---

## ⚙️ Prérequis

Avant de démarrer, assurez-vous d'avoir les éléments suivants installés :

- **Java JDK 8+** — `java -version`
- **Apache Maven 3.x+** — `mvn -version`
- **Apache Kafka** — Broker accessible sur `localhost:29092`
- **PostgreSQL** — Serveur accessible sur `localhost:5432`
  - Base : `postgres` / User : `postgres` / Password : `postgres`
- **Elasticsearch 7.x** — Nœud accessible sur `localhost:9200`

> [!TIP]
> Utilisez Docker Compose pour provisionner Kafka, PostgreSQL et Elasticsearch rapidement. Voir le projet compagnon `flink-ecommerce` (Python) pour le producteur de données simulées.

---

## 🚀 Démarrage Rapide

### 1. Cloner le projet

```bash
git clone <url-du-repo>
cd Flink-ecomerce-java
```

### 2. Compiler le projet

```bash
mvn clean package -DskipTests
```

Le Fat JAR sera généré dans `target/Flink-ecomerce-java-1.0-SNAPSHOT.jar`.

### 3. Lancer les services d'infrastructure

Assurez-vous que Kafka, PostgreSQL et Elasticsearch sont démarrés et accessibles.

### 4. Créer le topic Kafka

```bash
kafka-topics.sh --create \
  --topic financial_transactions \
  --bootstrap-server localhost:29092 \
  --partitions 3 \
  --replication-factor 1
```

### 5. Lancer le producteur de données (projet Python compagnon)

```bash
cd ../flink-ecommerce
pip install -r requirements.txt
python main.py
```

### 6. Soumettre le job Flink

```bash
# En mode local (développement)
mvn exec:java -Dexec.mainClass="FlinkEcommerceJava.DataStreamJob"

# Ou soumettre au cluster Flink
flink run target/Flink-ecomerce-java-1.0-SNAPSHOT.jar
```

---

## 🔄 Flux de Données Détaillé

### Ingestion
1. Les transactions JSON sont consommées depuis le topic Kafka `financial_transactions`
2. Le `JSONKeyValueDeserializationSchema` désérialise chaque message en objet `Transaction`

### Transformation & Agrégation
Le pipeline crée **4 flux parallèles** :

| Sink                      | Table PostgreSQL       | Logique                                                      |
|---------------------------|------------------------|--------------------------------------------------------------|
| **Transactions brutes**   | `transactions`         | Upsert de chaque transaction (clé : `transaction_id`)        |
| **Ventes par catégorie**  | `sales_per_category`   | `keyBy(category)` → `reduce(sum)` → Upsert par (date, cat.) |
| **Ventes par jour**       | `sales_per_day`        | `keyBy(date)` → `reduce(sum)` → Upsert par date             |
| **Ventes par mois**       | `sales_per_month`      | `keyBy(month)` → `reduce(sum)` → Upsert par (year, month)   |

### Indexation
5. Chaque transaction est également indexée dans **Elasticsearch** (index `transactions`) pour la recherche full-text et les dashboards Kibana.

---

## 🗃️ Schéma de la Base de Données (PostgreSQL)

```sql
-- Transactions brutes
CREATE TABLE transactions (
    transaction_id    VARCHAR(255) PRIMARY KEY,
    product_id        VARCHAR(255),
    product_name      VARCHAR(255),
    product_category  VARCHAR(255),
    product_price     DOUBLE PRECISION,
    product_quantity  INTEGER,
    product_brand     VARCHAR(255),
    total_amount      DOUBLE PRECISION,
    currency          VARCHAR(255),
    customer_id       VARCHAR(255),
    transaction_date  TIMESTAMP,
    payment_method    VARCHAR(255)
);

-- Agrégations
CREATE TABLE sales_per_category (
    transaction_date  DATE,
    category          VARCHAR(255),
    total_sales       DOUBLE PRECISION,
    PRIMARY KEY (transaction_date, category)
);

CREATE TABLE sales_per_day (
    transaction_date  DATE PRIMARY KEY,
    total_sales       DOUBLE PRECISION
);

CREATE TABLE sales_per_month (
    year        INTEGER,
    month       INTEGER,
    total_sales DOUBLE PRECISION,
    PRIMARY KEY (year, month)
);
```

> [!NOTE]
> Les tables sont créées automatiquement par le job Flink via des sinks JDBC dédiés (`CREATE TABLE IF NOT EXISTS`).

---

## 🔧 Configuration

Les paramètres de connexion sont actuellement définis en dur dans `DataStreamJob.java` :

| Paramètre            | Valeur par défaut           |
|-----------------------|-----------------------------|
| Kafka Bootstrap       | `localhost:29092`           |
| Kafka Topic           | `financial_transactions`    |
| Kafka Group ID        | `flink-group`               |
| PostgreSQL JDBC URL   | `jdbc:postgresql://localhost:5432/postgres` |
| PostgreSQL Username   | `postgres`                  |
| PostgreSQL Password   | `postgres`                  |
| Elasticsearch Host    | `localhost:9200`            |

> [!IMPORTANT]
> Pour un déploiement en production, externalisez ces paramètres via des variables d'environnement ou un fichier de configuration dédié.

---

## 📦 Dépendances Principales

| Dépendance                           | Rôle                                    |
|--------------------------------------|-----------------------------------------|
| `flink-streaming-java`              | API de streaming Flink                  |
| `flink-clients`                      | Client Flink pour soumission de jobs    |
| `flink-connector-kafka`             | Connecteur source Kafka                 |
| `flink-connector-jdbc`              | Connecteur sink JDBC (PostgreSQL)       |
| `flink-sql-connector-elasticsearch7`| Connecteur sink Elasticsearch 7         |
| `postgresql`                         | Driver JDBC PostgreSQL                  |
| `lombok`                             | Réduction du boilerplate Java (DTOs)    |
| `log4j2`                            | Framework de logging                    |

---

## 🧪 Tests

```bash
# Lancer les tests unitaires
mvn test

# Lancer les tests avec couverture
mvn test jacoco:report
```

---

## 🤝 Contribution

1. Fork le projet
2. Créez votre branche feature (`git checkout -b feature/ma-feature`)
3. Committez vos changements (`git commit -m 'feat: ajout de ma feature'`)
4. Pushez sur la branche (`git push origin feature/ma-feature`)
5. Ouvrez une Pull Request

---

## 📄 Licence

Ce projet est sous licence **Apache License 2.0** — voir le fichier [LICENSE](LICENSE) pour plus de détails.

---

## 👤 Auteur

Développé par **Mary** — Data Engineer

---

> *Ce projet fait partie de l'écosystème E-commerce Data Engineering. Voir aussi le projet compagnon [`flink-ecommerce`](../flink-ecommerce/) (producteur Python de données simulées).*
