import sys
from pyspark.sql import SparkSession

BRONZE_PATH = "s3a://bronze/events"


def get_spark():
    return (
        SparkSession.builder.appName("bronze_ingestion")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minio")
        .config("spark.hadoop.fs.s3a.secret.key", "minio123")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )


def main(input_path):
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    print(f">> Lecture brute du CSV : {input_path}")
    # Lecture sans schéma ni typage : tout reste en texte, tel quel.
    raw = (
        spark.read
        .option("header", "true")
        .csv(input_path)
    )

    print(f">> Écriture brute dans Bronze : {BRONZE_PATH} (format CSV, inchangé)")
    (
        raw.write
        .mode("overwrite")
        .option("header", "true")
        .csv(BRONZE_PATH)
    )

    print(f">> {raw.count():,} lignes copiées dans Bronze ✓")
    spark.stop()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: bronze_ingestion.py <chemin_csv>")
        sys.exit(1)
    main(sys.argv[1])
