import math
from uuid import uuid4

from pyspark.sql import Row

KEY_COLUMNS = ("vehicle_type", "weekday", "time_slot")
INPUT_COLUMNS = KEY_COLUMNS + ("revenue_usd", "distance_miles", "speed_mph")
ABS_TOLERANCES = {"total_revenue": 1e-6, "avg_distance": 1e-9, "avg_speed": 1e-9}
TOLERANCES = {"relative": 1e-10, "absolute": ABS_TOLERANCES}
Aggregate = Row(*KEY_COLUMNS, "trip_count", "total_revenue", "avg_distance", "avg_speed")


def pair(row):
    # Si associa ogni corsa alla chiave del gruppo e si inizializza il conteggio a uno.
    return (
        (row.vehicle_type, row.weekday, row.time_slot),
        (row.revenue_usd, row.distance_miles, row.speed_mph, 1),
    )


def merge(left, right):
    return (
        left[0] + right[0],
        left[1] + right[1],
        left[2] + right[2],
        left[3] + right[3],
    )


def finish(item):
    # Si calcolano le medie dopo aver sommato valori e conteggi di ciascun gruppo.
    key, (revenue, distance, speed, count) = item
    return Aggregate(*key, count, revenue, distance / count, speed / count)


def aggregate_rdd(frame):
    rows = frame.rdd.map(pair).reduceByKey(merge).map(finish).collect()
    return sorted(rows, key=lambda row: tuple(row[name] for name in KEY_COLUMNS))


def aggregate_sql(frame):
    spark = frame.sparkSession
    # Si usa un nome univoco per evitare collisioni tra esecuzioni nella stessa sessione.
    view = f"tlc_{uuid4().hex}"
    frame.createOrReplaceTempView(view)
    try:
        return spark.sql(f"""
            SELECT vehicle_type, weekday, time_slot, COUNT(*) AS trip_count,
                   SUM(revenue_usd) AS total_revenue, AVG(distance_miles) AS avg_distance,
                   AVG(speed_mph) AS avg_speed
            FROM {view}
            GROUP BY vehicle_type, weekday, time_slot
            ORDER BY vehicle_type, weekday, time_slot
        """).collect()
    finally:
        spark.catalog.dropTempView(view)


def compare(rdd_rows, sql_rows):
    # Si confrontano le righe per chiave, indipendentemente dall'ordine dei risultati.
    def index(rows):
        return {tuple(row[name] for name in KEY_COLUMNS): row for row in rows}

    rdd = index(rdd_rows)
    sql = index(sql_rows)
    problems = []
    if len(rdd) != len(rdd_rows) or len(sql) != len(sql_rows):
        problems.append("Chiavi duplicate")
    if rdd.keys() != sql.keys():
        problems.append(f"Solo RDD: {sorted(rdd.keys() - sql.keys())}; solo SQL: {sorted(sql.keys() - rdd.keys())}")

    # Si confrontano esattamente i conteggi e si ammettono piccoli scarti in virgola mobile.
    maxima = dict.fromkeys(ABS_TOLERANCES, 0.0)
    for key in sorted(rdd.keys() & sql.keys()):
        if rdd[key].trip_count != sql[key].trip_count:
            problems.append(f"{key}: trip_count diverso")
        for name, tolerance in ABS_TOLERANCES.items():
            left, right = rdd[key][name], sql[key][name]
            if not (math.isfinite(left) and math.isfinite(right)):
                problems.append(f"{key}: {name} non finito")
                continue
            maxima[name] = max(maxima[name], abs(left - right))
            if not math.isclose(left, right, rel_tol=TOLERANCES["relative"], abs_tol=tolerance):
                problems.append(f"{key}: {name} RDD={left}, SQL={right}")

    return {
        "verdict": "FAIL" if problems else "PASS",
        "groups_rdd": len(rdd),
        "groups_sql": len(sql),
        "max_abs_difference": maxima,
        "problems": problems,
    }
