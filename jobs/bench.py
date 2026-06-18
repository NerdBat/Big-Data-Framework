"""
Utilitaire de mesure de performance.
Chronomètre des étapes nommées et enregistre les résultats dans un CSV
sur MinIO (s3a://gold/_benchmarks/results.csv) ET en console.

Usage dans un job :
    from bench import Benchmark
    bench = Benchmark(spark, job_name="silver_transform")
    with bench.step("lecture_bronze"):
        df = spark.read.csv(...)
    with bench.step("sessionization"):
        df = add_sessions(df)
    bench.report()        # affiche le récap + sauvegarde
"""
import time
from contextlib import contextmanager
from datetime import datetime


class Benchmark:
    def __init__(self, spark, job_name, label="baseline"):
        """
        spark    : session Spark (pour écrire le CSV des résultats)
        job_name : nom du job mesuré (bronze/silver/gold)
        label    : étiquette de la version ('baseline', 'optimized', ...)
                   -> permet de comparer plusieurs versions du même job
        """
        self.spark = spark
        self.job_name = job_name
        self.label = label
        self.steps = []          # liste de (step_name, duration_sec)
        self._t0 = time.time()

    @contextmanager
    def step(self, name):
        """Chronomètre un bloc de code nommé."""
        print(f"   [bench] début : {name}")
        t = time.time()
        yield
        dt = time.time() - t
        self.steps.append((name, dt))
        print(f"   [bench] fin   : {name} -> {dt:.1f}s")

    def report(self, save=True):
        """Affiche le récap et (optionnel) sauvegarde sur MinIO."""
        total = time.time() - self._t0
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        print("\n========== BENCHMARK ==========")
        print(f"Job    : {self.job_name}  (label: {self.label})")
        print(f"Date   : {ts}")
        print(f"{'Étape':<25} {'Durée (s)':>12}")
        print("-" * 38)
        for name, dt in self.steps:
            print(f"{name:<25} {dt:>12.1f}")
        print("-" * 38)
        print(f"{'TOTAL':<25} {total:>12.1f}")
        print("===============================\n")

        if save:
            self._save(ts, total)

    def _save(self, ts, total):
        """Ajoute les lignes au CSV de résultats sur MinIO."""
        rows = [
            (ts, self.job_name, self.label, name, round(dt, 2))
            for name, dt in self.steps
        ]
        rows.append((ts, self.job_name, self.label, "TOTAL", round(total, 2)))

        cols = ["timestamp", "job", "label", "step", "duration_sec"]
        new_df = self.spark.createDataFrame(rows, cols)

        path = "s3a://gold/_benchmarks"
        try:
            # append : on accumule l'historique des runs pour comparer
            new_df.coalesce(1).write.mode("append").option("header", "true").csv(path)
            print(f"   [bench] résultats ajoutés -> {path}")
        except Exception as e:
            print(f"   [bench] sauvegarde ignorée : {e}")
