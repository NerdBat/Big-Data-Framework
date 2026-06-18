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
│   ├── bronze_ingestion.py         # ingestion CSV -> Bronze
│   ├── silver_transform.py         # nettoyage + sessions (instrumenté bench)
│   ├── gold_analytics.py           # 3 tables analytiques + Hive
│   ├── bench.py                    # module de mesure de performance
│   ├── bench_compare.py            # comparatif des benchmarks
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

- **Docker Desktop** (avec WSL2 sur Windows)
- **~6 Go de RAM** alloués à Docker minimum
- Le dataset **2019-Nov.csv** placé dans `data/`
  → [eCommerce behavior data (Kaggle)](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store)

---

## 5. Installation complète — commandes dans l'ordre

> ⚠️ **Suis cet ordre exact.** Le premier job Spark télécharge les JARs S3A, qui sont
> ensuite copiés pour Hive. Inverser les étapes provoque des erreurs.

### Étape 1 — Démarrer toute la stack

```bash
docker compose up -d
docker compose ps -a
```

Au premier lancement, Docker télécharge les images (~plusieurs minutes). Vérifie :
- `createbuckets` = `Exited (0)` (crée les buckets puis s'arrête — normal)
- tous les autres = `Up`

### Étape 2 — Initialiser le schéma Hive (UNE SEULE FOIS, sur base vierge)

Au tout premier démarrage (ou après un reset), la base est vide : il faut créer le schéma
du metastore. Le `-e IS_RESUME=false` force l'initialisation.

```bash
docker compose run --rm -e IS_RESUME=false hive /opt/hive/bin/schematool -dbType postgres -initSchema
```

Attends `schemaTool completed`, puis `Ctrl+C` pour sortir. Redémarre Hive proprement :

```bash
docker compose up -d hive
docker compose ps hive          # doit être "Up" durablement
```

### Étape 3 — Ingestion Bronze (télécharge aussi les JARs S3A)

```bash
docker compose exec spark /opt/spark/bin/spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.4 --conf spark.jars.ivy=/tmp/.ivy2 /jobs/bronze_ingestion.py /data/2019-Nov.csv
```

### Étape 4 — Copier les JARs S3A pour Hive

Maintenant que Spark les a téléchargés (étape 3), on les copie et on recrée Hive :

```bash
mkdir hive-jars
docker compose cp spark:/tmp/.ivy2/jars/org.apache.hadoop_hadoop-aws-3.3.4.jar ./hive-jars/
docker compose cp spark:/tmp/.ivy2/jars/com.amazonaws_aws-java-sdk-bundle-1.12.262.jar ./hive-jars/
docker compose cp spark:/tmp/.ivy2/jars/org.wildfly.openssl_wildfly-openssl-1.0.7.Final.jar ./hive-jars/
docker compose up -d hive
```

### Étape 5 — Transformation Silver

```bash
docker compose exec spark /opt/spark/bin/spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.4 --conf spark.jars.ivy=/tmp/.ivy2 --driver-memory 6g --conf spark.sql.shuffle.partitions=64 /jobs/silver_transform.py baseline
```

### Étape 6 — Analytique Gold (+ tables Hive)

```bash
docker compose exec spark /opt/spark/bin/spark-submit --jars /tmp/.ivy2/jars/org.apache.hadoop_hadoop-aws-3.3.4.jar,/tmp/.ivy2/jars/com.amazonaws_aws-java-sdk-bundle-1.12.262.jar,/tmp/.ivy2/jars/org.wildfly.openssl_wildfly-openssl-1.0.7.Final.jar --driver-memory 6g --conf spark.sql.shuffle.partitions=64 /jobs/gold_analytics.py
```

### Vérification

- **MinIO** http://localhost:9001 (minio / minio123) : buckets `bronze`, `silver`, `gold` remplis
- **Spark** http://localhost:8080 : 1 worker ALIVE
- Les 3 tables Gold sont dans Hive (base `gold`)

**Rappel des options Spark et pourquoi :**
| Option | Rôle |
|---|---|
| `--packages org.apache.hadoop:hadoop-aws:3.3.4` | télécharge le connecteur S3A (MinIO) |
| `--conf spark.jars.ivy=/tmp/.ivy2` | évite l'erreur de cache Ivy de l'image officielle |
| `--driver-memory 6g` | sans ça Spark tourne avec ~434 Mo et plante en OutOfMemory |
| `--conf spark.sql.shuffle.partitions=64` | optimise le shuffle (sessions, agrégations) |
| `--jars ...` (Gold) | au lieu de `--packages`, requis pour que Hive accède à S3A |

---

## 6. Mesure de performance (benchmark)

Le job Silver est instrumenté : il chronomètre chaque étape et enregistre les résultats
dans `s3a://gold/_benchmarks`.

**Mesurer une version (label au choix) :**
```bash
docker compose exec spark /opt/spark/bin/spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.4 --conf spark.jars.ivy=/tmp/.ivy2 --driver-memory 6g --conf spark.sql.shuffle.partitions=64 /jobs/silver_transform.py baseline
```

**Comparer plusieurs versions (baseline vs optimized) :**
```bash
# après une optimisation, relancer avec un autre label :
docker compose exec spark /opt/spark/bin/spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.4 --conf spark.jars.ivy=/tmp/.ivy2 --driver-memory 6g --conf spark.sql.shuffle.partitions=64 /jobs/silver_transform.py optimized

# puis afficher le comparatif avec le gain en % :
docker compose exec spark /opt/spark/bin/spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.4 --conf spark.jars.ivy=/tmp/.ivy2 /jobs/bench_compare.py
```

**Résultats baseline mesurés (67,5 M lignes) :**
| Étape | Durée (s) |
|---|---|
| lecture_bronze | 9,2 |
| nettoyage_nuls | 42,1 |
| ecriture_silver (déclenche sessions + dédup + typage) | 163,9 |
| **TOTAL** | **215,8 (~3 min 36)** |

> Les étapes intermédiaires affichent ~0s à cause de la *lazy evaluation* de Spark :
> le calcul réel n'est déclenché qu'à l'écriture (la seule action), qui concentre 76 % du temps.

---

## 7. Orchestration Airflow

### Étape 1 — Créer la base Airflow dans Postgres (une fois)

```bash
docker compose exec postgres psql -U hive -d metastore -c "CREATE DATABASE airflow;"
docker compose up -d airflow
```

### Étape 2 — Utiliser l'interface

Ouvre http://localhost:8088 (admin / admin) :
1. Active le DAG `lakehouse_pipeline` (toggle à gauche)
2. « Next Run » affiche la prochaine exécution (planifié `@daily`)
3. Pour lancer maintenant : bouton ▶ (Trigger DAG)
4. Vue **Grid** : `bronze → silver → gold` passent au vert

Le DAG lance les jobs Spark via `docker exec` et limite à un seul run simultané
(`max_active_runs=1`) pour éviter la saturation mémoire.

---

## 8. Tests

```bash
pip install -r requirements-dev.txt
pytest tests/ -v
```

Résultat attendu : `8 passed`. Tests sur les transformations (sessions, nettoyage,
déduplication) et la qualité des données (clés non nulles, prix positifs, invariant funnel).
Exécutés aussi en CI à chaque push (`.github/workflows/ci.yml`).

---

## 9. Streaming temps réel (optionnel)

**Terminal 1 — Consumer** (CA + ventes en fenêtres glissantes) :
```bash
docker compose exec spark pip install kafka-python
docker compose exec spark /opt/spark/bin/spark-submit --jars /tmp/.ivy2/jars/org.apache.hadoop_hadoop-aws-3.3.4.jar,/tmp/.ivy2/jars/com.amazonaws_aws-java-sdk-bundle-1.12.262.jar,/tmp/.ivy2/jars/org.wildfly.openssl_wildfly-openssl-1.0.7.Final.jar --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 --conf spark.jars.ivy=/tmp/.ivy2 /jobs/streaming_consumer.py
```

**Terminal 2 — Producer** (rejoue un échantillon dans Kafka) :
```bash
docker compose exec spark python3 /jobs/kafka_producer.py /data/2019-Nov.csv --limit 50000 --rate 200
```

---

## 10. Commandes utiles

```bash
docker compose ps -a                  # état des conteneurs
docker compose logs <service> -f      # suivre les logs d'un service
docker compose restart <service>      # redémarrer un seul service
docker compose stop                   # arrêter sans rien perdre
docker compose down                   # arrêter + supprimer conteneurs (GARDE les données)
docker compose down -v                # ⚠️ SUPPRIME les volumes (reset total)
```

> ⚠️ **Ne jamais faire `docker compose down -v` sauf pour repartir de zéro.** Le `-v` efface
> les volumes (données MinIO + base Hive), ce qui oblige à refaire tout le pipeline ET
> réinitialiser le schéma Hive (étape 2). Pour un arrêt normal : `docker compose stop`.

---

## 11. Pièges rencontrés & solutions (mémo de dépannage)

| Symptôme | Cause | Solution |
|---|---|---|
| `bitnami/spark:3.5 not found` | Bitnami a retiré ses images gratuites (août 2025) | Images officielles `apache/spark`, `apache/kafka` |
| Hive : `authentication type 10 is not supported` | Postgres 16 utilise scram-sha-256, incompatible driver Hive | Postgres **15** + `POSTGRES_HOST_AUTH_METHOD: md5` |
| Hive boucle, `relation already exists` | Hive réinitialise un schéma déjà présent | `IS_RESUME: "true"` dans le service Hive |
| Hive boucle, `Version information not found in metastore` | Base recréée vierge, schéma jamais initialisé | `docker compose run --rm -e IS_RESUME=false hive /opt/hive/bin/schematool -dbType postgres -initSchema` |
| `spark-submit: executable not found` | Pas dans le PATH de l'image officielle | Chemin complet `/opt/spark/bin/spark-submit` |
| Ivy : `FileNotFoundException ... resolved-...xml` | Cache Ivy non inscriptible | `--conf spark.jars.ivy=/tmp/.ivy2` |
| `Path does not exist: /data/...` | Dossier data non monté dans Spark | Monter `./data:/data` dans spark + spark-worker |
| `OutOfMemoryError: Java heap space` | Spark tourne avec 434 Mo par défaut | `--driver-memory 6g` |
| `ClassNotFoundException: S3AFileSystem` (au Gold) | Hive n'a pas les JARs S3A | Copier les JARs dans `hive-jars/` (étape 4) |
| `Could not find file /tmp/.ivy2/jars/...` à la copie | JARs pas encore téléchargés | Lancer d'abord un job Spark avec `--packages` (étape 3) |
| Airflow : tâches `up_for_retry` / runs parallèles | Plusieurs runs saturent la RAM | `max_active_runs=1` dans le DAG |

---

## 12. Dataset

[eCommerce behavior data from multi category store](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store)
— événements `view` / `cart` / `purchase`, octobre–novembre 2019.

Schéma : `event_time, event_type, product_id, category_id, category_code, brand, price, user_id, user_session`.

Le mois utilisé (`2019-Nov.csv`) contient **~67 millions d'événements** (~9 Go), ce qui justifie
pleinement l'usage de Spark : un traitement mono-machine (pandas) est impossible.