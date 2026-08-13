# 🎲 Flink E-Commerce — Générateur de Transactions Simulées (Python)

> **Producteur de données Kafka** qui génère des transactions e-commerce réalistes en temps réel à l'aide de la bibliothèque Faker, alimentant le pipeline de streaming Apache Flink.

---

## 📐 Rôle dans l'Architecture

```
                                          ┌──────────────────────────────┐
                                          │   Flink-ecomerce-java        │
┌────────────────────┐     ┌─────────┐    │   (Consumer & Transformateur)│
│  🐍 main.py        │────▶│  Kafka  │───▶│   → PostgreSQL               │
│  (Ce projet)       │     │  Topic  │    │   → Elasticsearch            │
│  Générateur de     │     │         │    └──────────────────────────────┘
│  transactions      │     └─────────┘
│  simulées          │     financial_
└────────────────────┘     transactions
```

Ce projet est le **producteur de données** de l'écosystème. Il simule un flux continu de transactions e-commerce et les publie sur un topic Kafka, où elles sont ensuite consommées par le pipeline Flink Java pour le traitement temps réel.

---

## 🧩 Stack Technologique

| Composant          | Technologie          | Version    |
|--------------------|----------------------|------------|
| **Langage**        | Python               | `3.8+`     |
| **Kafka Client**   | confluent-kafka      | `2.3.0`    |
| **Données Fake**   | Faker                | `20.1.0`   |
| **Sérialisation**  | json (stdlib)        | —          |

---

## 📁 Structure du Projet

```
flink-ecommerce/
├── main.py              # 🎯 Point d'entrée — Générateur & producteur Kafka
├── requirements.txt     # Dépendances Python
├── .gitignore           # Fichiers et dossiers ignorés par Git
├── .venv/               # Environnement virtuel (ignoré par Git)
└── venv/                # Environnement virtuel alternatif (ignoré par Git)
```

---

## ⚙️ Prérequis

- **Python 3.8+** — `python3 --version`
- **pip** — `pip --version`
- **Apache Kafka** — Broker accessible sur `localhost:29092`

> [!TIP]
> Utilisez Docker pour démarrer Kafka rapidement :
> ```bash
> docker-compose up -d kafka zookeeper
> ```

---

## 🚀 Démarrage Rapide

### 1. Cloner le projet

```bash
git clone <url-du-repo>
cd flink-ecommerce
```

### 2. Créer un environnement virtuel

```bash
python3 -m venv .venv
source .venv/bin/activate    # Linux/macOS
# .venv\Scripts\activate     # Windows
```

### 3. Installer les dépendances

```bash
pip install -r requirements.txt
```

### 4. S'assurer que Kafka est démarré

Vérifiez que le broker Kafka est accessible sur `localhost:29092`.

### 5. Lancer le générateur

```bash
python main.py
```

Le script va produire des transactions pendant **2 minutes** (120 secondes), à raison d'**1 transaction par seconde**.

---

## 📊 Schéma des Données Générées

Chaque transaction produite est un objet JSON avec la structure suivante :

```json
{
  "transactionId": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "productId": "product3",
  "productName": "tablet",
  "productCategory": "electronic",
  "productPrice": 459.99,
  "productQuantity": 3,
  "productBrand": "apple",
  "totalAmount": 1379.97,
  "currency": "USD",
  "customerId": "john_doe_42",
  "transactionDate": "2026-08-13T02:15:30.123456",
  "paymentMethod": "credit_card"
}
```

### Détail des Champs

| Champ               | Type     | Valeurs possibles                                                |
|---------------------|----------|------------------------------------------------------------------|
| `transactionId`     | UUID     | Généré aléatoirement (unique)                                    |
| `productId`         | String   | `product1` à `product6`                                          |
| `productName`       | String   | `laptop`, `mobile`, `tablet`, `watch`, `headphone`, `speaker`    |
| `productCategory`   | String   | `electronic`, `fashion`, `grocery`, `home`, `beauty`, `sports`   |
| `productPrice`      | Float    | Entre `10.00` et `1000.00`                                       |
| `productQuantity`   | Integer  | Entre `1` et `10`                                                |
| `productBrand`      | String   | `apple`, `samsung`, `oneplus`, `mi`, `boat`, `sony`              |
| `totalAmount`       | Float    | `productPrice × productQuantity` (calculé)                       |
| `currency`          | String   | `USD`, `GBP`                                                     |
| `customerId`        | String   | Username généré via Faker                                        |
| `transactionDate`   | DateTime | Horodatage UTC au moment de la génération                        |
| `paymentMethod`     | String   | `credit_card`, `debit_card`, `online_transfer`                   |

---

## 🔧 Configuration

Les paramètres sont définis directement dans `main.py` :

| Paramètre                | Valeur par défaut       | Description                                  |
|--------------------------|-------------------------|----------------------------------------------|
| `bootstrap.servers`      | `localhost:29092`       | Adresse du broker Kafka                      |
| `topic`                  | `financial_transactions`| Nom du topic Kafka cible                     |
| **Durée d'exécution**    | `120 secondes`          | Durée totale de génération de données        |
| **Intervalle**           | `1 seconde`             | Pause entre chaque transaction               |

> [!IMPORTANT]
> Pour un usage en production, externalisez ces paramètres dans un fichier `.env` ou via des arguments CLI (`argparse`).

---

## 📦 Dépendances

| Package            | Version   | Rôle                                          |
|--------------------|-----------|-----------------------------------------------|
| `confluent-kafka`  | `2.3.0`   | Client Kafka performant (basé sur librdkafka) |
| `Faker`            | `20.1.0`  | Génération de données réalistes               |
| `python-dateutil`  | `2.8.2`   | Manipulation avancée des dates                |
| `simplejson`       | `3.19.2`  | Sérialisation JSON optimisée                  |
| `six`              | `1.16.0`  | Compatibilité Python 2/3                      |

---

## 🛠️ Personnalisation

### Modifier la durée de génération

Dans `main.py`, changez la condition de la boucle :

```python
# Actuellement : 120 secondes
while (datetime.now() - curr_time).seconds < 120:

# Pour 10 minutes :
while (datetime.now() - curr_time).seconds < 600:
```

### Ajouter de nouveaux produits

Modifiez les listes dans la fonction `generate_sales_transactions()` :

```python
"productName": random.choice(['laptop', 'mobile', 'tablet', ...]),  # Ajoutez ici
"productCategory": random.choice(['electronic', 'fashion', ...]),    # Et ici
```

### Modifier la fréquence de génération

```python
time.sleep(1)   # 1 transaction/seconde
time.sleep(0.1) # 10 transactions/seconde
```

---

## 🧪 Vérification

Pour vérifier que les messages arrivent bien dans Kafka :

```bash
# Consumer console Kafka
kafka-console-consumer.sh \
  --bootstrap-server localhost:29092 \
  --topic financial_transactions \
  --from-beginning
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

> *Ce projet fait partie de l'écosystème E-commerce Data Engineering. Voir aussi le projet compagnon [`Flink-ecomerce-java`](../Flink-ecomerce-java/) (pipeline de streaming Flink en Java).*
