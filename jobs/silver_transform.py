"""
Phase 2 — Transformation Silver (avec mesure de performance + mode optimisé/non optimisé)
Lit le Bronze brut (CSV), nettoie, type, recalcule les sessions (gap 30 min),
ajoute les colonnes techniques, écrit en Parquet partitionné par jour
dans s3a://silver/events. Chronométré via bench.py.

Deux modes pour le comparatif de performance :
  - NON OPTIMISÉ (défaut) : pas de cache + shuffle partitions par défaut (200)
      -> chaque action (count) relit et recalcule tout depuis le CSV
  - OPTIMISÉ (--optimize) : cache du DataFrame + shuffle partitions = 64
      -> les données lues une fois sont réutilisées, shuffle bien dimensionné

Lancement NON OPTIMISÉ (label "baseline") :
  docker compose exec spark /opt/spark/bin/spark-submit \
    --packages org.apache.hadoop:hadoop-aws:3.3.4 \
    --conf spark.jars.ivy=/tmp/.ivy2 --driver-memory 6g \
    /jobs/silver_transform.py baseline

Lancement OPTIMISÉ (label "optimized") :
  docker compose exec spark /opt/spark/bin/spark-submit \
    --packages org.apache.hadoop:hadoop-aws:3.3.4 \
    --conf spark.jars.ivy=/tmp/.ivy2 --driver-memory 6g \
    /jobs/silver_transform.py optimized --optimize
"""
import sys
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, DoubleType,
)
from pyspark.storagelevel import StorageLevel
from bench import Benchmark

BRONZE_PATH = "s3a://bronze/events"
SILVER_PATH = "s3a://silver/events"
SESSION_GAP_MIN = 30
NULL_DROP_THRESHOLD = 0.05

SCHEMA = StructType([
    StructField("event_time",    StringType(), True),
    StructField("event_type",    StringType(), True),
    StructField("product_id",    StringType(), True),
    StructField("category_id",   StringType(), True),
    StructField("category_code", StringType(), True),
    StructField("brand",         StringType(), True),
    StructField("price",         StringType(), True),
    StructField("user_id",       StringType(), True),
    StructField("user_session",  StringType(), True),
])


def get_spark(optimize):
    builder = (
        SparkSession.builder.appName("silver_transform")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minio")
        .config("spark.hadoop.fs.s3a.secret.key", "minio123")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
    )
    # OPTIMISATION 1 : nombre de partitions de shuffle adapté au volume.
    # Sans ce réglage, Spark utilise 200 (défaut) -> trop de petites tâches.
    if optimize:
        builder = builder.config("spark.sql.shuffle.partitions", "64")
    else:
        builder = builder.config("spark.sql.shuffle.partitions", "200")
    return builder.getOrCreate()


def main(label, optimize):
    spark = get_spark(optimize)
    spark.sparkContext.setLogLevel("WARN")
    bench = Benchmark(spark, job_name="silver_transform", label=label)

    mode = "OPTIMISÉ (cache + 64 partitions)" if optimize else "NON OPTIMISÉ (sans cache + 200 partitions)"
    print(f"\n>> MODE : {mode}\n")

    # ---- Lecture du Bronze ----
    with bench.step("lecture_bronze"):
        df = (
            spark.read.option("header", "true").schema(SCHEMA).csv(BRONZE_PATH)
        )

    # ---- Typage ----
    with bench.step("typage"):
        df = (
            df
            .withColumn(
                "event_time",
                F.to_timestamp(F.regexp_replace("event_time", " UTC$", ""),
                               "yyyy-MM-dd HH:mm:ss"),
            )
            .withColumn("product_id",  F.col("product_id").cast(LongType()))
            .withColumn("category_id", F.col("category_id").cast(LongType()))
            .withColumn("user_id",     F.col("user_id").cast(LongType()))
            .withColumn("price",       F.col("price").cast(DoubleType()))
        )

    # ---- Nettoyage ----
    with bench.step("nettoyage_nuls"):
        key_cols = ["event_time", "event_type", "user_id", "user_session"]
        df = df.dropna(subset=key_cols)

        # OPTIMISATION 2 : cache. Le DataFrame est relu plusieurs fois ci-dessous
        # (3 count + l'écriture finale). Sans cache, Spark recalcule TOUTE la chaîne
        # (lecture CSV + typage + dropna) à chaque action -> très coûteux.
        if optimize:
            df = df.persist(StorageLevel.MEMORY_AND_DISK)

        # Plusieurs actions : c'est ici que l'absence de cache fait mal
        n_after_keys = df.count()
        n_price_null = df.filter(F.col("price").isNull()).count()
        price_null_rate = n_price_null / n_after_keys if n_after_keys else 0
        print(f"   Lignes après nettoyage clés : {n_after_keys:,}")
        print(f"   Taux de nuls sur price : {price_null_rate:.2%}")

        if price_null_rate == 0:
            print("   price : aucun nul.")
        elif price_null_rate < NULL_DROP_THRESHOLD:
            print(f"   price : < 5% -> suppression de {n_price_null:,} lignes.")
            df = df.filter(F.col("price").isNotNull())
        else:
            median = df.approxQuantile("price", [0.5], 0.01)[0]
            print(f"   price : >= 5% -> imputation médiane ({median}).")
            df = df.fillna({"price": median})

        df = df.fillna({"category_code": "unknown", "brand": "unknown"})

    # ---- Déduplication ----
    with bench.step("deduplication"):
        df = df.dropDuplicates()

    # ---- Sessionization ----
    with bench.step("sessionization"):
        w = Window.partitionBy("user_id").orderBy("event_time")
        gap_sec = SESSION_GAP_MIN * 60
        df = (
            df
            .withColumn("prev_time", F.lag("event_time").over(w))
            .withColumn(
                "gap_sec",
                F.when(F.col("prev_time").isNull(), None)
                 .otherwise(F.col("event_time").cast("long") - F.col("prev_time").cast("long")),
            )
            .withColumn(
                "is_new_session",
                F.when(F.col("gap_sec").isNull() | (F.col("gap_sec") > gap_sec), 1).otherwise(0),
            )
            .withColumn("session_index", F.sum("is_new_session").over(w))
            .withColumn("session_id", F.concat_ws("_", F.col("user_id"), F.col("session_index")))
            .drop("prev_time", "gap_sec", "is_new_session", "session_index")
        )

    # ---- Colonnes techniques ----
    with bench.step("colonnes_techniques"):
        df = (
            df
            .withColumn("event_date", F.to_date("event_time"))
            .withColumn("ingestion_ts", F.current_timestamp())
        )

    # ---- Écriture Silver ----
    with bench.step("ecriture_silver"):
        (
            df.write
            .mode("overwrite")
            .partitionBy("event_date")
            .parquet(SILVER_PATH)
        )

    bench.report()
    print(f">> Transformation Silver terminée ✓  [{mode}]")
    spark.stop()


if __name__ == "__main__":
    # 1er argument = label (baseline / optimized) ; --optimize active les optimisations
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    label = args[0] if args else "baseline"
    optimize = "--optimize" in sys.argv
    main(label, optimize)
