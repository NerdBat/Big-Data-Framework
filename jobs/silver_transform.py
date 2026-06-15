"""
Phase 2 — Transformation Silver
Lit le Bronze brut (CSV), nettoie, type, recalcule les sessions (gap 30 min),
ajoute les colonnes techniques, écrit en Parquet partitionné par jour
dans s3a://silver/events. Termine par un rapport qualité.

Lancement :
  docker compose exec spark /opt/spark/bin/spark-submit \
    --packages org.apache.hadoop:hadoop-aws:3.3.4 \
    --conf spark.jars.ivy=/tmp/.ivy2 \
    /jobs/silver_transform.py
"""
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, DoubleType,
)

BRONZE_PATH = "s3a://bronze/events"
SILVER_PATH = "s3a://silver/events"
SESSION_GAP_MIN = 30          # nouvelle session si inactivité > 30 min
NULL_DROP_THRESHOLD = 0.05    # < 5% de nuls sur price -> drop, sinon médiane

# Le Bronze est en CSV brut : on redonne un schéma à la lecture (tout en string
# pour price, qu'on castera, afin de tolérer d'éventuelles valeurs aberrantes).
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


def get_spark():
    return (
        SparkSession.builder.appName("silver_transform")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minio")
        .config("spark.hadoop.fs.s3a.secret.key", "minio123")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def main():
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    # ---- 1. Lecture du Bronze ----
    print(f">> Lecture Bronze : {BRONZE_PATH}")
    df = (
        spark.read
        .option("header", "true")
        .schema(SCHEMA)
        .csv(BRONZE_PATH)
    )
    n_raw = df.count()
    print(f"   Lignes Bronze : {n_raw:,}")

    # ---- 2. Typage ----
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

    # ---- 3. Drop des clés non récupérables ----
    key_cols = ["event_time", "event_type", "user_id", "user_session"]
    df = df.dropna(subset=key_cols)

    # ---- 4. Nettoyage des nuls selon le tableau validé ----
    # price : règle des 5% (drop sinon médiane)
    n_after_keys = df.count()
    n_price_null = df.filter(F.col("price").isNull()).count()
    price_null_rate = n_price_null / n_after_keys if n_after_keys else 0
    print(f"   Taux de nuls sur price : {price_null_rate:.2%}")

    if price_null_rate == 0:
        print("   price : aucun nul.")
    elif price_null_rate < NULL_DROP_THRESHOLD:
        print(f"   price : < 5% -> suppression de {n_price_null:,} lignes.")
        df = df.filter(F.col("price").isNotNull())
    else:
        median = df.approxQuantile("price", [0.5], 0.01)[0]
        print(f"   price : >= 5% -> imputation par la médiane ({median}).")
        df = df.fillna({"price": median})

    # category_code / brand : texte -> "unknown" (pas de médiane sur du texte)
    df = df.fillna({"category_code": "unknown", "brand": "unknown"})

    # ---- 5. Déduplication ----
    df = df.dropDuplicates()

    # ---- 6. Recalcul des sessions (gap 30 min) ----
    # Pour chaque user, on ordonne par temps ; si l'écart avec l'event précédent
    # dépasse 30 min, on démarre une nouvelle session. La somme cumulée de ces
    # ruptures forme un index de session, combiné à user_id -> session_id stable.
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
        .withColumn(
            "session_id",
            F.concat_ws("_", F.col("user_id"), F.col("session_index")),
        )
        .drop("prev_time", "gap_sec", "is_new_session", "session_index")
    )

    # ---- 7. Colonnes techniques ----
    df = (
        df
        .withColumn("event_date", F.to_date("event_time"))   # clé de partition
        .withColumn("ingestion_ts", F.current_timestamp())   # traçabilité
    )

    # ---- 8. Écriture Silver (Parquet partitionné) ----
    print(f">> Écriture Silver : {SILVER_PATH} (Parquet, partitionné par event_date)")
    (
        df.write
        .mode("overwrite")
        .partitionBy("event_date")
        .parquet(SILVER_PATH)
    )

    # ---- 9. Rapport qualité ----
    quality_report(df, n_raw)

    print(">> Transformation Silver terminée ✓")
    spark.stop()


def quality_report(df, n_raw):
    print("\n========== RAPPORT QUALITÉ (SILVER) ==========")
    total = df.count()
    print(f"Lignes Bronze -> Silver : {n_raw:,} -> {total:,} "
          f"({100*total/n_raw:.1f}% conservées)")

    print("\nRépartition event_type :")
    df.groupBy("event_type").count().orderBy(F.desc("count")).show(truncate=False)

    print("Taux de valeurs nulles par colonne (%) :")
    df.select([
        F.round(100 * F.sum(F.col(c).isNull().cast("int")) / total, 3).alias(c)
        for c in df.columns
    ]).show(truncate=False)

    print("Sessions recalculées :")
    n_sessions = df.select("session_id").distinct().count()
    n_users = df.select("user_id").distinct().count()
    print(f"   Sessions distinctes : {n_sessions:,}")
    print(f"   Utilisateurs distincts : {n_users:,}")
    print(f"   Sessions / utilisateur (moy.) : {n_sessions/n_users:.2f}")

    print("\nPlage de dates :")
    df.select(
        F.min("event_time").alias("min"),
        F.max("event_time").alias("max"),
    ).show(truncate=False)
    print("==============================================\n")


if __name__ == "__main__":
    main()
