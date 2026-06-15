"""
DAG Airflow — Pipeline Lakehouse e-commerce
Orchestre le pipeline médaillon : Bronze -> Silver -> Gold.

Chaque tâche lance un job Spark via `docker exec` sur le conteneur Spark.
Airflow ne fait qu'ordonnancer : tout le calcul se passe dans Spark.

Déclenchement : manuel (ou planifiable via schedule_interval).
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# Conteneur Spark cible et JARs S3A (chemins dans le conteneur)
SPARK = "big-data-framework-spark-1"
JARS = (
    "/tmp/.ivy2/jars/org.apache.hadoop_hadoop-aws-3.3.4.jar,"
    "/tmp/.ivy2/jars/com.amazonaws_aws-java-sdk-bundle-1.12.262.jar,"
    "/tmp/.ivy2/jars/org.wildfly.openssl_wildfly-openssl-1.0.7.Final.jar"
)
SUBMIT = "/opt/spark/bin/spark-submit"
COMMON_OPTS = f"--jars {JARS} --driver-memory 6g --conf spark.sql.shuffle.partitions=64"

default_args = {
    "owner": "data-eng",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


def spark_task(task_id, script, extra_args="", dag=None):
    """Construit une tâche BashOperator qui lance un job Spark via docker exec."""
    cmd = (
        f"docker exec {SPARK} {SUBMIT} {COMMON_OPTS} "
        f"/jobs/{script} {extra_args}"
    )
    return BashOperator(task_id=task_id, bash_command=cmd, dag=dag)


with DAG(
    dag_id="lakehouse_pipeline",
    description="Pipeline médaillon Bronze -> Silver -> Gold",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",    # exécution quotidienne (minuit UTC)
    catchup=False,
    max_active_runs=1,             # un seul run à la fois (évite la saturation RAM)
    tags=["lakehouse", "spark", "ecommerce"],
) as dag:

    bronze = spark_task(
        "bronze_ingestion",
        "bronze_ingestion.py",
        extra_args="/data/2019-Nov.csv",
        dag=dag,
    )

    silver = spark_task(
        "silver_transform",
        "silver_transform.py",
        dag=dag,
    )

    gold = spark_task(
        "gold_analytics",
        "gold_analytics.py",
        dag=dag,
    )

    bronze >> silver >> gold