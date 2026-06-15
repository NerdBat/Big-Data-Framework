-- Crée la base Airflow si elle n'existe pas (séparée du metastore Hive)
SELECT 'CREATE DATABASE airflow'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow')\gexec
