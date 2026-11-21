import csv
import json
import os
from urllib.parse import quote
from uuid import uuid4

import psycopg
from psycopg import sql
from psycopg.types.json import Jsonb
from pyspark.sql import functions as F

from src.config import RESULTS

AGGREGATE_SCHEMA = """
    run_id STRING, vehicle_type STRING, weekday SMALLINT, time_slot STRING,
    algorithm STRING, trip_count BIGINT, total_revenue DOUBLE,
    avg_distance DOUBLE, avg_speed DOUBLE
"""

# Si collegano aggregati e misure al rapporto di qualità tramite l'identificativo dell'esecuzione.
DDL = (
    """
    CREATE TABLE IF NOT EXISTS tlc_quality (
        run_id text PRIMARY KEY,
        report jsonb NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS tlc_aggregates (
        run_id text NOT NULL REFERENCES tlc_quality,
        vehicle_type text NOT NULL CHECK (vehicle_type IN ('yellow', 'green', 'hvfhv')),
        weekday smallint NOT NULL CHECK (weekday BETWEEN 1 AND 7),
        time_slot text NOT NULL CHECK (time_slot IN ('00-04', '04-08', '08-12', '12-16', '16-20', '20-24')),
        algorithm text NOT NULL CHECK (algorithm IN ('rdd', 'sql')),
        trip_count bigint NOT NULL CHECK (trip_count > 0),
        total_revenue double precision NOT NULL CHECK (total_revenue >= 0 AND total_revenue < 'Infinity'),
        avg_distance double precision NOT NULL CHECK (avg_distance > 0 AND avg_distance < 'Infinity'),
        avg_speed double precision NOT NULL CHECK (avg_speed > 0 AND avg_speed < 'Infinity'),
        PRIMARY KEY (run_id, vehicle_type, weekday, time_slot, algorithm)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS tlc_benchmark (
        run_id text NOT NULL REFERENCES tlc_quality,
        category text NOT NULL CHECK (category IN ('yellow', 'green', 'hvfhv')),
        algorithm text NOT NULL CHECK (algorithm IN ('rdd', 'sql')),
        run smallint NOT NULL CHECK (run BETWEEN 1 AND 5),
        seconds double precision NOT NULL CHECK (seconds >= 0 AND seconds < 'Infinity'),
        position smallint NOT NULL CHECK (position IN (1, 2)),
        config jsonb NOT NULL,
        PRIMARY KEY (run_id, category, algorithm, run),
        UNIQUE (run_id, category, run, position)
    )
    """,
)


def connect():
    return psycopg.connect(
        host="postgres", port=5432, dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"], password=os.environ["POSTGRES_PASSWORD"],
        connect_timeout=5,
    )


def read_artifacts(directory=RESULTS):
    quality = json.loads((directory / "quality.json").read_text(encoding="utf-8"))
    aggregates = json.loads((directory / "aggregates.json").read_text(encoding="utf-8"))
    with (directory / "benchmark.csv").open(newline="", encoding="utf-8") as stream:
        timings = list(csv.DictReader(stream))
    # Si esportano solo risultati coerenti con la stessa esecuzione e con un benchmark completo.
    run_id = quality["run_id"]
    if aggregates["run_id"] != run_id or any(row["run_id"] != run_id for row in timings):
        raise ValueError("Report di run diversi: rieseguire il notebook 02 sul report di pulizia corrente")
    if aggregates["comparison"]["verdict"] != "PASS" or len(timings) != 30:
        raise ValueError("Servono aggregazioni equivalenti e 30 misure del benchmark")
    return quality, aggregates, timings


def _verify_counts(rows, expected, source):
    actual = {(category, algorithm): (groups, trips) for category, algorithm, groups, trips in rows}
    if actual != expected:
        raise RuntimeError(f"Gruppi o conteggi PostgreSQL incoerenti in {source}: {actual}, attesi {expected}")


def export_run(aggregate_frame, quality, aggregates, timings):
    run_id = quality["run_id"]
    # Si ricavano i conteggi attesi dai report locali per verificare i dati scritti in PostgreSQL.
    expected = {
        (category, algorithm): (
            sum(row["vehicle_type"] == category for row in aggregates[algorithm]),
            item["counts"]["valid_count"],
        )
        for category, item in quality["categories"].items()
        if item["counts"]["valid_count"] > 0
        for algorithm in ("rdd", "sql")
    }
    timing_rows = [
        (run_id, row["category"], row["algorithm"], int(row["run"]), float(row["seconds"]),
         int(row["order"]), Jsonb({
             "spark_version": row["spark_version"],
             "spark_master": row["spark_master"],
             "driver_memory": row["driver_memory"],
             "shuffle_partitions": int(row["shuffle_partitions"]),
             "input_storage": row["input_storage"],
             "input_rows": int(row["input_rows"]),
         }))
        for row in timings
    ]

    with connect() as connection, connection.cursor() as cursor:
        for statement in DDL:
            cursor.execute(statement)
        cursor.execute("SELECT current_schema()")
        (schema,) = cursor.fetchone()
        stage = sql.Identifier(schema, f"tlc_stage_{uuid4().hex}")
        # Si crea una tabella di staging visibile alle connessioni JDBC aperte da Spark.
        cursor.execute(sql.SQL("CREATE TABLE {} (LIKE tlc_aggregates INCLUDING ALL)").format(stage))

    try:
        database = quote(os.environ["POSTGRES_DB"], safe="")
        url = f"jdbc:postgresql://postgres:5432/{database}"
        properties = {
            "driver": "org.postgresql.Driver",
            "user": os.environ["POSTGRES_USER"],
            "password": os.environ["POSTGRES_PASSWORD"],
            "numPartitions": "1",
        }
        table = stage.as_string()
        aggregate_frame.write.mode("append").jdbc(url, table, properties=properties)
        # Si rileggono i dati via JDBC per verificare identificativo, gruppi e corse prima della pubblicazione.
        spark = aggregate_frame.sparkSession
        staged = (
            spark.read.jdbc(url, table, properties=properties)
            .groupBy("run_id", "vehicle_type", "algorithm")
            .agg(F.count("*").alias("groups"), F.sum("trip_count").alias("trips"))
            .collect()
        )
        if any(row.run_id != run_id for row in staged):
            raise RuntimeError("Il DataFrame JDBC contiene aggregati di un altro run")
        _verify_counts(
            [(row.vehicle_type, row.algorithm, row.groups, row.trips) for row in staged],
            expected, "staging JDBC",
        )
        return _publish_run(run_id, quality, timing_rows, stage, expected)
    except BaseException as error:
        # Si tenta di rimuovere la tabella di staging preservando l'errore originale.
        try:
            with connect() as connection, connection.cursor() as cursor:
                cursor.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(stage))
        except psycopg.Error as cleanup_error:
            error.add_note(f"Pulizia della tabella di staging {stage.as_string()} non riuscita: {cleanup_error}")
        raise


def _publish_run(run_id, quality, timing_rows, stage, expected):
    with connect() as connection, connection.cursor() as cursor:
        # Si acquisisce un lock sulla riga del rapporto per serializzare le esportazioni dello stesso run.
        cursor.execute("""
            INSERT INTO tlc_quality (run_id, report) VALUES (%s, %s)
            ON CONFLICT (run_id) DO UPDATE SET report = EXCLUDED.report
        """, (run_id, Jsonb(quality)))
        # Si sostituiscono i risultati precedenti nella stessa transazione e si verifica quanto inserito.
        cursor.execute("DELETE FROM tlc_aggregates WHERE run_id = %s", (run_id,))
        cursor.execute("DELETE FROM tlc_benchmark WHERE run_id = %s", (run_id,))
        cursor.execute(sql.SQL("INSERT INTO tlc_aggregates SELECT * FROM {}").format(stage))
        cursor.executemany("""
            INSERT INTO tlc_benchmark (run_id, category, algorithm, run, seconds, position, config)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, timing_rows)

        cursor.execute("""
            SELECT vehicle_type, algorithm, COUNT(*) AS groups, SUM(trip_count)::bigint AS trips
            FROM tlc_aggregates WHERE run_id = %s
            GROUP BY vehicle_type, algorithm ORDER BY vehicle_type, algorithm
        """, (run_id,))
        stored = cursor.fetchall()
        _verify_counts(stored, expected, "tlc_aggregates")

        cursor.execute("SELECT COUNT(*) FROM tlc_benchmark WHERE run_id = %s", (run_id,))
        if cursor.fetchone()[0] != 30:
            raise RuntimeError("Numero di misure PostgreSQL diverso da 30")
        cursor.execute(sql.SQL("DROP TABLE {}").format(stage))
    return stored
