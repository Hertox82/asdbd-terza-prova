import csv
import json
import os

import psycopg
from psycopg.types.json import Jsonb

from src.config import RESULTS

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
    run_id = quality["run_id"]
    if aggregates["run_id"] != run_id or any(row["run_id"] != run_id for row in timings):
        raise ValueError("Report di run diversi: rieseguire il notebook 02 sul report di pulizia corrente")
    if aggregates["comparison"]["verdict"] != "PASS" or len(timings) != 30:
        raise ValueError("Servono aggregazioni equivalenti e 30 misure del benchmark")
    return quality, aggregates, timings


def export_run(quality, aggregates, timings):
    run_id = quality["run_id"]
    aggregate_rows = [
        (run_id, row["vehicle_type"], row["weekday"], row["time_slot"], algorithm,
         row["trip_count"], row["total_revenue"], row["avg_distance"], row["avg_speed"])
        for algorithm in ("rdd", "sql")
        for row in aggregates[algorithm]
    ]
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
        # La riga del run serializza le esportazioni concorrenti dello stesso identificativo.
        cursor.execute("""
            INSERT INTO tlc_quality (run_id, report) VALUES (%s, %s)
            ON CONFLICT (run_id) DO UPDATE SET report = EXCLUDED.report
        """, (run_id, Jsonb(quality)))
        cursor.execute("DELETE FROM tlc_aggregates WHERE run_id = %s", (run_id,))
        cursor.execute("DELETE FROM tlc_benchmark WHERE run_id = %s", (run_id,))
        cursor.executemany("""
            INSERT INTO tlc_aggregates (
                run_id, vehicle_type, weekday, time_slot, algorithm,
                trip_count, total_revenue, avg_distance, avg_speed
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, aggregate_rows)
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
        for category, algorithm, groups, trips in stored:
            expected = quality["categories"][category]["counts"]["valid_count"]
            if trips != expected or groups != sum(row["vehicle_type"] == category for row in aggregates[algorithm]):
                raise RuntimeError(f"Conteggi PostgreSQL incoerenti per {category}, {algorithm}")
        expected_pairs = {
            (category, algorithm)
            for category, item in quality["categories"].items()
            if item["counts"]["valid_count"] > 0
            for algorithm in ("rdd", "sql")
        }
        if {(category, algorithm) for category, algorithm, _, _ in stored} != expected_pairs:
            raise RuntimeError("Categorie o algoritmi mancanti nella query di verifica")

        cursor.execute("SELECT COUNT(*) FROM tlc_benchmark WHERE run_id = %s", (run_id,))
        if cursor.fetchone()[0] != 30:
            raise RuntimeError("Numero di misure PostgreSQL diverso da 30")
    return stored
