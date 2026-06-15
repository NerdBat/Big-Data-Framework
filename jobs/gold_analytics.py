"""
Phase 3 — Couche Gold (analytique métier)
Lit le Silver et produit 3 tables analytiques, écrites en Parquet dans s3a://gold/.

Stratégie d'écriture : Spark écrit les Parquet sur MinIO (Spark sait parler S3A),
puis on enregistre les tables dans Hive via CREATE TABLE ... LOCATION. Les tables
Hive sont EXTERNES : Hive ne stocke que les métadonnées, les données restent sur MinIO.

  1. gold_funnel        : funnel view -> cart -> purchase + taux de conversion
  2. gold_top_products  : top produits / marques / catégories par CA et volume
  3. gold_temporal      : activité par heure et par jour de semaine

Lancement :
  docker compose exec spark /opt/spark/bin/spark-submit \
    --jars /tmp/.ivy2/jars/org.apache.hadoop_hadoop-aws-3.3.4.jar,/tmp/.ivy2/jars/com.amazonaws_aws-java-sdk-bundle-1.12.262.jar,/tmp/.ivy2/jars/org.wildfly.openssl_wildfly-openssl-1.0.7.Final.jar \
    --driver-memory 6g --conf spark.sql.shuffle.partitions=64 \
    /jobs/gold_analytics.py
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

SILVER_PATH = "s3a://silver/events"
GOLD_PATH = "s3a://gold"


def get_spark():
    return (
        SparkSession.builder.appName("gold_analytics")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minio")
        .config("spark.hadoop.fs.s3a.secret.key", "minio123")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )


def write_parquet(df, name):
    """Écrit une table en Parquet sur MinIO (Gold)."""
    path = f"{GOLD_PATH}/{name}"
    print(f">> Écriture {name} -> {path}")
    df.write.mode("overwrite").parquet(path)
    return path


def build_funnel(events):
    print("\n--- 1. Funnel de conversion ---")
    by_type = (
        events.groupBy("event_type")
        .agg(
            F.count("*").alias("events"),
            F.countDistinct("user_id").alias("users"),
            F.countDistinct("session_id").alias("sessions"),
        )
    )
    counts = {r["event_type"]: r["events"] for r in by_type.collect()}
    n_view = counts.get("view", 0)
    n_cart = counts.get("cart", 0)
    n_purchase = counts.get("purchase", 0)

    funnel = by_type.withColumn(
        "conversion_rate_pct",
        F.when(F.col("event_type") == "view", F.lit(100.0))
         .when(F.col("event_type") == "cart",
               F.round(F.lit(100.0 * n_cart / n_view), 2) if n_view else F.lit(None))
         .when(F.col("event_type") == "purchase",
               F.round(F.lit(100.0 * n_purchase / n_view), 2) if n_view else F.lit(None))
         .otherwise(F.lit(None)),
    )
    funnel.show(truncate=False)
    if n_view:
        print(f"   view->cart : {100*n_cart/n_view:.2f}%")
        print(f"   view->buy  : {100*n_purchase/n_view:.2f}%")
    if n_cart:
        print(f"   cart->buy  : {100*n_purchase/n_cart:.2f}%")
    return funnel


def build_top_products(events):
    print("\n--- 2. Top produits / marques / catégories ---")
    purchases = events.filter(F.col("event_type") == "purchase")
    views = events.filter(F.col("event_type") == "view")

    def agg_dim(col_name, dim_label):
        revenue = (
            purchases.groupBy(col_name)
            .agg(
                F.round(F.sum("price"), 2).alias("revenue"),
                F.count("*").alias("purchases"),
            )
        )
        view_cnt = views.groupBy(col_name).agg(F.count("*").alias("views"))
        return (
            revenue.join(view_cnt, col_name, "left")
            .withColumn("dimension", F.lit(dim_label))
            .withColumn("value", F.col(col_name).cast("string"))
            .select("dimension", "value", "revenue", "purchases", "views")
        )

    top = (
        agg_dim("product_id", "product")
        .unionByName(agg_dim("brand", "brand"))
        .unionByName(agg_dim("category_code", "category"))
    )
    print("   Top 5 marques par CA :")
    (top.filter(F.col("dimension") == "brand")
        .orderBy(F.desc("revenue")).show(5, truncate=False))
    return top


def build_temporal(events):
    print("\n--- 3. Analyse temporelle ---")
    enriched = (
        events
        .withColumn("hour", F.hour("event_time"))
        .withColumn("dow", F.date_format("event_time", "E"))
    )
    temporal = (
        enriched.groupBy("dow", "hour", "event_type")
        .agg(F.count("*").alias("events"))
        .orderBy("dow", "hour")
    )
    print("   Heures de pic (tous events) :")
    (enriched.groupBy("hour").agg(F.count("*").alias("events"))
        .orderBy(F.desc("events")).show(5))
    return temporal


def register_hive_tables(spark, paths):
    """Enregistre les Parquet de MinIO comme tables externes Hive."""
    print("\n>> Enregistrement des tables Hive (externes)...")
    spark.sql("CREATE DATABASE IF NOT EXISTS gold")
    for name, path in paths.items():
        spark.sql(f"DROP TABLE IF EXISTS gold.{name}")
        spark.sql(
            f"CREATE TABLE gold.{name} USING PARQUET LOCATION '{path}'"
        )
        print(f"   gold.{name} -> {path}")
    spark.sql("SHOW TABLES IN gold").show(truncate=False)


def main():
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    print(f">> Lecture Silver : {SILVER_PATH}")
    events = spark.read.parquet(SILVER_PATH)
    events.cache()
    print(f"   Lignes Silver : {events.count():,}")

    # 1. Spark écrit les Parquet sur MinIO (pas de Hive ici)
    paths = {
        "gold_funnel":       write_parquet(build_funnel(events),       "gold_funnel"),
        "gold_top_products": write_parquet(build_top_products(events), "gold_top_products"),
        "gold_temporal":     write_parquet(build_temporal(events),     "gold_temporal"),
    }

    # 2. On tente d'enregistrer dans Hive (facultatif : si le metastore
    #    ne sait pas lire S3A, on log l'erreur sans faire échouer le job).
    try:
        spark2 = (
            SparkSession.builder.appName("gold_hive_register")
            .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
            .config("spark.hadoop.fs.s3a.access.key", "minio")
            .config("spark.hadoop.fs.s3a.secret.key", "minio123")
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .config("spark.hadoop.hive.metastore.uris", "thrift://hive:9083")
            .enableHiveSupport()
            .getOrCreate()
        )
        register_hive_tables(spark2, paths)
    except Exception as e:
        print(f"\n[!] Enregistrement Hive ignoré (données bien écrites sur MinIO) : {e}")

    print("\n>> Couche Gold terminée ✓ (Parquet sur s3a://gold/)")
    spark.stop()


if __name__ == "__main__":
    main()
