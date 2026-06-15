"""
Tests de validation de la qualité des données (contrats de données).
Vérifie qu'après transformation, les invariants métier sont respectés.
"""
from datetime import datetime
from pyspark.sql import functions as F


def _silver_like(spark):
    """Petit jeu de données représentatif d'une sortie Silver."""
    data = [
        (datetime(2019, 11, 1, 10, 0), "view",     489.07, 1, "s1"),
        (datetime(2019, 11, 1, 10, 5), "cart",     489.07, 1, "s1"),
        (datetime(2019, 11, 1, 10, 9), "purchase", 489.07, 1, "s1"),
        (datetime(2019, 11, 1, 11, 0), "view",     120.00, 2, "s2"),
    ]
    return spark.createDataFrame(
        data, ["event_time", "event_type", "price", "user_id", "session_id"]
    )


def test_no_null_keys(spark):
    """Aucune clé critique ne doit être nulle."""
    df = _silver_like(spark)
    for col in ["event_time", "event_type", "user_id", "session_id"]:
        assert df.filter(F.col(col).isNull()).count() == 0


def test_price_positive(spark):
    """Tous les prix doivent être strictement positifs."""
    df = _silver_like(spark)
    assert df.filter(F.col("price") <= 0).count() == 0


def test_event_types_valid(spark):
    """event_type ne contient que les valeurs attendues."""
    df = _silver_like(spark)
    valid = {"view", "cart", "purchase"}
    found = {r["event_type"] for r in df.select("event_type").distinct().collect()}
    assert found.issubset(valid)


def test_funnel_monotonic(spark):
    """Dans le funnel, purchases <= carts <= views (invariant métier)."""
    df = _silver_like(spark)
    counts = {r["event_type"]: r["c"]
              for r in df.groupBy("event_type").agg(F.count("*").alias("c")).collect()}
    views = counts.get("view", 0)
    carts = counts.get("cart", 0)
    purchases = counts.get("purchase", 0)
    assert purchases <= carts <= views
