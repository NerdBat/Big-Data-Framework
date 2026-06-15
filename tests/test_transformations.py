"""
Tests unitaires sur la logique de transformation Silver.
On teste les comportements clés : sessionization (gap 30 min), nettoyage
des nuls texte ('unknown'), déduplication, typage.
"""
from datetime import datetime
from pyspark.sql import Window
from pyspark.sql import functions as F


# --- Réimplémentation de la logique de sessions (identique au job Silver) ---
def add_sessions(df, gap_min=30):
    w = Window.partitionBy("user_id").orderBy("event_time")
    gap_sec = gap_min * 60
    return (
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


def test_session_split_on_gap(spark):
    """Deux events espacés de > 30 min => deux sessions différentes."""
    data = [
        (1, datetime(2019, 11, 1, 10, 0, 0)),
        (1, datetime(2019, 11, 1, 10, 20, 0)),   # +20 min -> même session
        (1, datetime(2019, 11, 1, 11, 30, 0)),   # +70 min -> nouvelle session
    ]
    df = spark.createDataFrame(data, ["user_id", "event_time"])
    result = add_sessions(df).orderBy("event_time").collect()

    assert result[0]["session_id"] == result[1]["session_id"]
    assert result[1]["session_id"] != result[2]["session_id"]


def test_sessions_isolated_per_user(spark):
    """Les sessions de deux users différents ne se mélangent pas."""
    data = [
        (1, datetime(2019, 11, 1, 10, 0, 0)),
        (2, datetime(2019, 11, 1, 10, 1, 0)),
    ]
    df = spark.createDataFrame(data, ["user_id", "event_time"])
    sids = {r["user_id"]: r["session_id"] for r in add_sessions(df).collect()}
    assert sids[1] != sids[2]


def test_fillna_unknown_on_text(spark):
    """Les colonnes texte nulles deviennent 'unknown'."""
    data = [("apple", "electronics"), (None, None)]
    df = spark.createDataFrame(data, ["brand", "category_code"])
    out = df.fillna({"brand": "unknown", "category_code": "unknown"}).collect()
    assert out[1]["brand"] == "unknown"
    assert out[1]["category_code"] == "unknown"


def test_deduplication(spark):
    """dropDuplicates supprime les lignes identiques."""
    data = [(1, "view"), (1, "view"), (2, "cart")]
    df = spark.createDataFrame(data, ["user_id", "event_type"])
    assert df.dropDuplicates().count() == 2
