"""Session Spark préconfigurée pour MinIO (S3A) + Hive.
Importé par tous les jobs : from spark_session import get_spark
"""
from pyspark.sql import SparkSession


def get_spark(app="job"):
    return (
        SparkSession.builder.appName(app)
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minio")
        .config("spark.hadoop.fs.s3a.secret.key", "minio123")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.hive.metastore.uris", "thrift://hive:9083")
        .enableHiveSupport()
        .getOrCreate()
    )
