# Big Data Frameworks -- Ngy François, Theissen Antoine

Projet pipeline de données complet pour le traitement de big data grace a des big data frameworks.

# E-Commerce Lakehouse — Traitement de données massives

Pipeline big data de bout en bout sur le dataset **eCommerce behavior** (~67M d'événements
pour le mois de novembre 2019, ~9 Go CSV), bâti autour d'un **datalake MinIO** suivant
l'**architecture médaillon** (Bronze / Silver / Gold), traité par **Spark**, catalogué dans
**Hive**, et orchestré par **Airflow**. Le tout 100 % conteneurisé avec Docker.

> Projet réalisé dans le cadre du Bloc 2 — *Concevoir, développer et déployer une solution
> de traitement des données massives*.

---

## 1. Architecture

```
                  ┌──────────── Airflow (orchestration @daily) ──────────┐
                  │                                                        │
CSV brut ─────────▼─► [Spark] ─► MinIO Bronze ─► Silver ─► Gold ─► Hive (catalogue)
(/data/2019-Nov.csv)   (CSV brut)  (Parquet      (Parquet     (3 tables
                                    nettoyé,      analytique)   Parquet)
                                    sessionizé)
```

**Modèle médaillon :**
- **Bronze** — données brutes copiées telles quelles (CSV), traçables et rejouables
- **Silver** — nettoyées, typées, dédupliquées, sessions recalculées (gap 30 min)
- **Gold** — 3 tables analytiques : funnel de conversion, top produits/marques/catégories, analyse temporelle

---

## 2. Stack technique

| Couche | Outil | Image Docker |
|---|---|---|
| Datalake S3 | MinIO | `minio/minio` |
| Traitement distribué | Apache Spark 3.5.1 | `apache/spark:3.5.1` |
| Catalogue de tables | Hive Metastore 3.1.3 | `apache/hive:3.1.3` |
| Backend metastore + Airflow | PostgreSQL 15 | `postgres:15` |
| Bus d'événements (streaming) | Apache Kafka 3.7 | `apache/kafka:3.7.1` |
| Orchestration | Apache Airflow 2.9.3 | `apache/airflow:2.9.3` |
| Conteneurisation | Docker Compose | — |

> **Note** : on utilise les images **officielles Apache** et non Bitnami. Depuis fin août 2025,
> Bitnami a déplacé ses images gratuites vers un dépôt `bitnamilegacy` non maintenu — les images
> `bitnami/spark` et `bitnami/kafka` ne sont plus disponibles publiquement.

---

## 3. Structure du projet

```
Big-Data-Framework/
├── docker-compose.yml              # toute la stack
├── README.md
├── .gitignore
├── pytest.ini
├── requirements-dev.txt            # dépendances pour les tests
│
├── data/                           # données sources (NON versionné)
│   └── 2019-Nov.csv                # à télécharger depuis Kaggle
│
├── hive-jars/                      # JARs S3A pour Hive (NON versionné, à copier)
│   ├── org.apache.hadoop_hadoop-aws-3.3.4.jar
│   ├── com.amazonaws_aws-java-sdk-bundle-1.12.262.jar
│   └── org.wildfly.openssl_wildfly-openssl-1.0.7.Final.jar
│
├── jobs/                           # scripts Spark (montés dans /jobs)
│   ├── bronze_ingestion.py
│   ├── silver_transform.py
│   ├── gold_analytics.py
│   ├── kafka_producer.py           # streaming (optionnel)
│   ├── streaming_consumer.py       # streaming (optionnel)
│   └── spark_session.py            # helper de session Spark
│
├── dags/                           # DAGs Airflow (montés dans /opt/airflow/dags)
│   └── lakehouse_pipeline.py
│
├── scripts/
│   └── init-airflow-db.sql         # crée la base airflow dans Postgres
│
├── tests/                          # tests pytest
│   ├── conftest.py                 # session Spark de test (INDISPENSABLE)
│   ├── test_transformations.py
│   └── test_data_quality.py
│
└── .github/workflows/
    └── ci.yml                      # CI : lance les tests à chaque push
```

---

## 4. Prérequis

- **Docker Desktop** (avec WSV2 sur Windows)
- **~6 Go de RAM** alloués à Docker minimum (le traitement Spark est gourmand)
- Le dataset **2019-Nov.csv** placé dans `data/`
  → [eCommerce behavior data (Kaggle)](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store)

---

## 5. Installation pas à pas (reproductible de zéro)

### Étape 1 — Démarrer la stack

```bash
docker compose up -d
docker compose ps -a
```

Au premier lancement, Docker télécharge toutes les images (~plusieurs minutes).

Vérifie que tout tourne :
- `createbuckets` doit être `Exited (0)` (il crée les buckets bronze/silver/gold puis s'arrête — normal)
- tous les autres doivent être `Up`

**Interfaces web :**
| Service | URL | Login |
|---|---|---|
| Console MinIO | http://localhost:9001 | minio / minio123 |
| Spark Master | http://localhost:8080 | — |
| Airflow | http://localhost:8088 | admin / admin |

### Étape 2 — Préparer les JARs S3A pour Hive

Hive a besoin des connecteurs S3A pour cataloguer des tables stockées sur MinIO.
On les récupère depuis le conteneur Spark (qui les télécharge au premier `spark-submit`).

D'abord, déclenche un téléchargement des JARs en lançant n'importe quel job avec `--packages`
(voir étape 3), puis copie-les :

```bash
mkdir hive-jars
docker compose cp spark:/tmp/.ivy2/jars/org.apache.hadoop_hadoop-aws-3.3.4.jar ./hive-jars/
docker compose cp spark:/tmp/.ivy2/jars/com.amazonaws_aws-java-sdk-bundle-1.12.262.jar ./hive-jars/
docker compose cp spark:/tmp/.ivy2/jars/org.wildfly.openssl_wildfly-openssl-1.0.7.Final.jar ./hive-jars/
```

Puis recrée Hive pour qu'il charge ces JARs :

```bash
docker compose up -d hive
docker compose ps hive     # doit être "Up"
```

### Étape 3 — Lancer le pipeline manuellement (Bronze → Silver → Gold)

Tous les jobs se lancent via `spark-submit` dans le conteneur Spark.

**Note sur les options récurrentes :**
- `--conf spark.jars.ivy=/tmp/.ivy2` : évite une erreur de cache Ivy de l'image officielle Spark
- `--driver-memory 6g` : sans ça, Spark tourne avec ~434 Mo et plante en OutOfMemory
- `--conf spark.sql.shuffle.partitions=64` : optimise le shuffle (sessions, agrégations)
- `--jars ...` (et non `--packages`) pour le Gold : nécessaire pour que Hive accède à S3A

**3.1 — Bronze (ingestion brute) :**
```bash
docker compose exec spark /opt/spark/bin/spark-submit \
  --packages org.apache.hadoop:hadoop-aws:3.3.4 \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  /jobs/bronze_ingestion.py /data/2019-Nov.csv
```

**3.2 — Silver (nettoyage + sessions) :**
```bash
docker compose exec spark /opt/spark/bin/spark-submit \
  --packages org.apache.hadoop:hadoop-aws:3.3.4 \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --driver-memory 6g \
  --conf spark.sql.shuffle.partitions=64 \
  /jobs/silver_transform.py
```

**3.3 — Gold (analytique + tables Hive) :**
```bash
docker compose exec spark /opt/spark/bin/spark-submit \
  --jars /tmp/.ivy2/jars/org.apache.hadoop_hadoop-aws-3.3.4.jar,/tmp/.ivy2/jars/com.amazonaws_aws-java-sdk-bundle-1.12.262.jar,/tmp/.ivy2/jars/org.wildfly.openssl_wildfly-openssl-1.0.7.Final.jar \
  --driver-memory 6g \
  --conf spark.sql.shuffle.partitions=64 \
  /jobs/gold_analytics.py
```

À la fin, vérifie sur MinIO (http://localhost:9001) : les buckets `bronze`, `silver`, `gold`
contiennent les données. Les 3 tables Gold sont aussi enregistrées dans Hive (base `gold`).

### Étape 4 — Orchestration avec Airflow

Airflow rejoue ce pipeline automatiquement (planifié `@daily`).

**4.1 — Créer la base Airflow dans Postgres** (si pas déjà fait au premier démarrage) :
```bash
docker compose exec postgres psql -U hive -d metastore -c "CREATE DATABASE airflow;"
docker compose up -d airflow
```

**4.2 — Dans l'UI Airflow** (http://localhost:8088, admin/admin) :
1. Active le DAG `lakehouse_pipeline` (toggle à gauche du nom)
2. Le « Next Run » affiche la prochaine exécution quotidienne
3. Pour lancer tout de suite : bouton ▶ (Trigger DAG)
4. Suis l'exécution dans la vue **Grid** : `bronze → silver → gold` passent au vert

Le DAG lance les jobs Spark via `docker exec` sur le conteneur Spark. Il est configuré avec
`max_active_runs=1` pour éviter que plusieurs exécutions saturent la RAM en parallèle.

---

## 6. Tests

8 tests pytest valident la logique de transformation et la qualité des données.

```bash
pip install -r requirements-dev.txt
pytest tests/ -v
```

Résultat attendu : `8 passed`.

- **test_transformations.py** : sessionization (gap 30 min), isolation par user, nettoyage des nuls, déduplication
- **test_data_quality.py** : clés non nulles, prix positifs, event_types valides, invariant funnel (purchases ≤ carts ≤ views)

La CI GitHub Actions (`.github/workflows/ci.yml`) lance ces tests à chaque push.

---

## 7. Streaming temps réel (optionnel)

Couche temps réel avec Kafka + Spark Structured Streaming.

**Terminal 1 — Consumer** (CA + ventes en fenêtres glissantes) :
```bash
docker compose exec spark pip install kafka-python
docker compose exec spark /opt/spark/bin/spark-submit \
  --jars /tmp/.ivy2/jars/org.apache.hadoop_hadoop-aws-3.3.4.jar,/tmp/.ivy2/jars/com.amazonaws_aws-java-sdk-bundle-1.12.262.jar,/tmp/.ivy2/jars/org.wildfly.openssl_wildfly-openssl-1.0.7.Final.jar \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  /jobs/streaming_consumer.py
```

**Terminal 2 — Producer** (rejoue un échantillon du CSV dans Kafka) :
```bash
docker compose exec spark python3 /jobs/kafka_producer.py /data/2019-Nov.csv --limit 50000 --rate 200
```

---

## 8. Commandes utiles

```bash
docker compose ps -a                  # état des conteneurs
docker compose logs <service> -f      # suivre les logs d'un service
docker compose restart <service>      # redémarrer un seul service
docker compose down                   # arrêter (garde les données)
docker compose down -v                # arrêter + SUPPRIMER les volumes (reset total)
```

---

## 9. Pièges rencontrés & solutions (mémo de dépannage)

| Symptôme | Cause | Solution |
|---|---|---|
| `bitnami/spark:3.5 not found` | Bitnami a retiré ses images gratuites (août 2025) | Utiliser les images officielles `apache/spark`, `apache/kafka` |
| Hive : `authentication type 10 is not supported` | Postgres 16 utilise scram-sha-256, le driver Hive ne le gère pas | Postgres **15** + `POSTGRES_HOST_AUTH_METHOD: md5` |
| Hive redémarre en boucle, `relation already exists` | Hive réinitialise un schéma déjà présent | `IS_RESUME: "true"` dans le service Hive |
| `spark-submit: executable not found` | Pas dans le PATH de l'image officielle | Chemin complet `/opt/spark/bin/spark-submit` |
| Ivy : `FileNotFoundException ... resolved-...xml` | Cache Ivy non inscriptible | `--conf spark.jars.ivy=/tmp/.ivy2` |
| `Path does not exist: /data/...` | Dossier data non monté dans Spark | Monter `./data:/data` dans spark + spark-worker |
| `OutOfMemoryError: Java heap space` | Spark tourne avec 434 Mo par défaut | `--driver-memory 6g` |
| `ClassNotFoundException: S3AFileSystem` (au Gold) | Hive n'a pas les JARs S3A | Copier les JARs dans `hive-jars/` (monté dans `/opt/hive/auxlib`) |
| Airflow : tâches `up_for_retry` / runs parallèles | Plusieurs runs saturent la RAM | `max_active_runs=1` dans le DAG |

---

## 10. Dataset

[eCommerce behavior data from multi category store](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store)
— événements `view` / `cart` / `purchase`, octobre–novembre 2019.

Schéma : `event_time, event_type, product_id, category_id, category_code, brand, price, user_id, user_session`.

Le mois utilisé ici (`2019-Nov.csv`) contient **~67 millions d'événements** (~9 Go), ce qui
justifie pleinement l'usage de Spark : un traitement mono-machine (Pandas) est impossible.