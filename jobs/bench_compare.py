"""
Affiche un comparatif des benchmarks enregistrés sur MinIO.
Lit s3a://gold/_benchmarks et présente, pour chaque job, les durées par
étape et par label (baseline vs optimized...), avec le gain en %.

Lancement :
  docker compose exec spark /opt/spark/bin/spark-submit \
    --packages org.apache.hadoop:hadoop-aws:3.3.4 \
    --conf spark.jars.ivy=/tmp/.ivy2 \
    /jobs/bench_compare.py
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

BENCH_PATH = "s3a://gold/_benchmarks"


def get_spark():
    return (
        SparkSession.builder.appName("bench_compare")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minio")
        .config("spark.hadoop.fs.s3a.secret.key", "minio123")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )


def main():
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.option("header", "true").csv(BENCH_PATH)
    df = df.withColumn("duration_sec", F.col("duration_sec").cast("double"))

    # On garde, pour chaque (job, label, step), le dernier run mesuré
    latest = (
        df.groupBy("job", "label", "step")
        .agg(F.max("timestamp").alias("ts"))
    )
    runs = df.join(latest,
                   (df.job == latest.job) & (df.label == latest.label) &
                   (df.step == latest.step) & (df.timestamp == latest.ts)) \
             .select(df.job, df.label, df.step, df.duration_sec)

    print("\n========== COMPARATIF DES BENCHMARKS ==========\n")

    jobs = [r["job"] for r in runs.select("job").distinct().collect()]
    for job in sorted(jobs):
        print(f"### Job : {job}")
        pivot = (
            runs.filter(F.col("job") == job)
            .groupBy("step")
            .pivot("label")
            .agg(F.first("duration_sec"))
        )
        labels = [c for c in pivot.columns if c != "step"]
        # Si on a baseline + optimized : calcul du gain
        if "baseline" in labels and "optimized" in labels:
            pivot = pivot.withColumn(
                "gain_%",
                F.round(100 * (F.col("baseline") - F.col("optimized")) / F.col("baseline"), 1),
            )
        pivot.orderBy(
            F.when(F.col("step") == "TOTAL", 1).otherwise(0), "step"
        ).show(truncate=False)

    print("===============================================\n")
    spark.stop()


if __name__ == "__main__":
    main()
