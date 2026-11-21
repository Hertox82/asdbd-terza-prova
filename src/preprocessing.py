from datetime import datetime, timezone
from functools import reduce
from operator import or_
from uuid import uuid4

from pyspark.sql import functions as F

from src.config import CLEANED, RESULTS, SLOTS, THRESHOLDS, TIMEZONE, save_json

SOURCE_COLUMNS = {
    "yellow": ("tpep_pickup_datetime", "tpep_dropoff_datetime", "trip_distance", "fare_amount"),
    "green": ("lpep_pickup_datetime", "lpep_dropoff_datetime", "trip_distance", "fare_amount"),
    "hvfhv": ("pickup_datetime", "dropoff_datetime", "trip_miles", "base_passenger_fare"),
}
BASELINE_FIELDS = (
    "original_count", "removed_unique", "valid_count", "pickup_outside_month",
    "null_timestamp", "duration_nonpositive", "duration_over_limit",
    "distance_invalid", "distance_over_limit", "speed_over_limit", "revenue_invalid",
)
BASELINE = {
    "yellow": (3_898_963, 405_122, 3_493_841, 7, 0, 56_064, 1_073, 123_774, 201, 947, 247_088),
    "green": (48_205, 2_008, 46_197, 13, 0, 40, 215, 1_516, 14, 205, 130),
    "hvfhv": (19_653_012, 3_028, 19_649_984, 0, 0, 1, 373, 2_155, 155, 4, 470),
}
NORMALIZED = (
    "vehicle_type", "pickup_datetime", "dropoff_datetime", "distance_miles",
    "revenue_usd", "duration_seconds", "speed_mph", "weekday", "time_slot",
)


def finite(column):
    return column.isNotNull() & ~F.isnan(column) & (F.abs(column) < F.lit(float("inf")))


def normalize(raw, category):
    pickup, dropoff, distance, revenue = SOURCE_COLUMNS[category]
    frame = raw.select(
        *(name for name in ("total_amount", "trip_time") if name in raw.columns),
        F.col(pickup).cast("timestamp_ntz").alias("pickup_datetime"),
        F.col(dropoff).cast("timestamp_ntz").alias("dropoff_datetime"),
        F.col(distance).cast("double").alias("distance_miles"),
        F.col(revenue).cast("double").alias("revenue_usd"),
    )
    return (
        frame.withColumn("vehicle_type", F.lit(category))
        .withColumn("duration_seconds", F.expr(
            "timestampdiff(MICROSECOND, pickup_datetime, dropoff_datetime) / 1000000D"
        ))
        .withColumn("speed_mph", F.when(
            F.col("duration_seconds") > 0, F.col("distance_miles") * 3600 / F.col("duration_seconds")
        ))
        .withColumn("weekday", ((F.dayofweek("pickup_datetime") + 5) % 7 + 1).cast("int"))
        .withColumn("time_slot", F.get(F.array(*map(F.lit, SLOTS)),
                                      F.floor(F.hour("pickup_datetime") / 4).cast("int")))
    )


def violations():
    pickup = F.col("pickup_datetime")
    dropoff = F.col("dropoff_datetime")
    duration = F.col("duration_seconds")
    distance = F.col("distance_miles")
    revenue = F.col("revenue_usd")
    speed = F.col("speed_mph")
    speed_computable = (duration > 0) & finite(distance) & (distance > 0)
    rules = {
        "pickup_outside_month": (pickup < F.lit(THRESHOLDS["pickup_start"]).cast("timestamp_ntz"))
        | (pickup >= F.lit(THRESHOLDS["pickup_end"]).cast("timestamp_ntz")),
        "null_timestamp": pickup.isNull() | dropoff.isNull(),
        "duration_nonpositive": duration <= 0,
        "duration_over_limit": duration > THRESHOLDS["max_duration_seconds"],
        "distance_invalid": ~finite(distance) | (distance <= 0),
        "distance_over_limit": finite(distance) & (distance > THRESHOLDS["max_distance_miles"]),
        "speed_nonpositive_or_non_finite": speed_computable & (~finite(speed) | (speed <= 0)),
        "speed_over_limit": speed_computable & finite(speed) & (speed > THRESHOLDS["max_speed_mph"]),
        "revenue_invalid": ~finite(revenue) | (revenue < THRESHOLDS["min_revenue_usd"]),
    }
    return {name: F.coalesce(condition, F.lit(False)) for name, condition in rules.items()}


def screen(raw, category):
    frame = normalize(raw, category)
    rules = violations()
    removed = reduce(or_, rules.values())
    revenue = F.col("revenue_usd")
    cents = revenue * 100
    diagnostics = {
        "valid_revenue_subcent": ~removed & (F.abs(cents - F.round(cents)) > 1e-6),
    }
    if category == "yellow" and "total_amount" in raw.columns:
        other_rules = (rule for name, rule in rules.items() if name != "revenue_invalid")
        physically_valid_negative = ~reduce(or_, other_rules) & finite(revenue) & (revenue < 0)
        diagnostics["negative_fare_physically_valid"] = physically_valid_negative
        diagnostics["negative_fare_physically_valid_positive_total"] = (
            physically_valid_negative & (F.col("total_amount") > 0)
        )
    if category == "hvfhv" and "trip_time" in raw.columns:
        diagnostics["trip_time_diff_over_60s"] = F.abs(F.col("trip_time") - F.col("duration_seconds")) > 60

    counts = frame.agg(
        F.count("*").alias("original_count"),
        F.count_if(removed).alias("removed_unique"),
        F.count_if(~removed).alias("valid_count"),
        *(F.count_if(rule).alias(name) for name, rule in rules.items()),
        *(F.count_if(check).alias(name) for name, check in diagnostics.items()),
    ).first().asDict()
    if counts["original_count"] != counts["valid_count"] + counts["removed_unique"]:
        raise RuntimeError(f"Conteggi incoerenti per {category}: {counts}")

    return frame.where(~removed).select(*NORMALIZED), counts


def _progress(message):
    timestamp = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")
    print(f"[{timestamp}] {message}", flush=True)


def preprocess(spark, paths):
    started = datetime.now(timezone.utc)
    report = {
        "run_id": f"{started:%Y%m%dT%H%M%SZ}-{uuid4().hex[:8]}",
        "started_at": started.isoformat(),
        "spark_version": spark.version,
        "session_timezone": TIMEZONE,
        "thresholds": THRESHOLDS,
        "categories": {},
    }
    report_path = RESULTS / "quality.json"
    _progress(
        f"Avvio preprocessing {report['run_id']}. Spark {spark.version}, "
        f"master {spark.sparkContext.master}, memoria driver "
        f"{spark.sparkContext.getConf().get('spark.driver.memory', 'predefinita')}, "
        f"partizioni shuffle {spark.conf.get('spark.sql.shuffle.partitions')}, "
        f"fuso sessione {spark.conf.get('spark.sql.session.timeZone')}."
    )
    _progress(
        f"Soglie v{THRESHOLDS['version']}. "
        f"Pickup in [{THRESHOLDS['pickup_start']}, {THRESHOLDS['pickup_end']}), "
        f"durata fino a {THRESHOLDS['max_duration_seconds']} s, "
        f"distanza fino a {THRESHOLDS['max_distance_miles']} mi, "
        f"velocità fino a {THRESHOLDS['max_speed_mph']} mph, "
        f"ricavo minimo {THRESHOLDS['min_revenue_usd']} USD."
    )
    report_path.unlink(missing_ok=True)
    for category, path in paths.items():
        label = "HVFHV" if category == "hvfhv" else category.title()
        output = f"{CLEANED}/{category}"
        phase = "lettura del Parquet originale"
        try:
            _progress(f"{label} — inizio {phase}: {path.name}")
            raw = spark.read.parquet(path.as_uri())

            phase = "calcolo delle statistiche e controlli di qualità"
            _progress(f"{label} — inizio {phase}")
            clean, counts = screen(raw, category)
            _progress(
                f"{label} — {counts['original_count']} record originali, "
                f"{counts['removed_unique']} esclusi e {counts['valid_count']} validi."
            )

            phase = "scrittura su HDFS"
            _progress(f"{label} — inizio {phase}: {output}")
            clean.write.mode("overwrite").parquet(output)
            _progress(f"{label} — {phase} completata.")

            phase = "verifica di leggibilità su HDFS"
            _progress(f"{label} — {phase}")
            if bool(spark.read.parquet(output).select(*NORMALIZED).take(1)) != (counts["valid_count"] > 0):
                raise RuntimeError(f"Output Parquet incoerente per {category}")
            _progress(f"{label} — {phase} completata.")

            phase = "raccolta dei metadati per il report"
            baseline = dict(zip(BASELINE_FIELDS, BASELINE[category]))
            report["categories"][category] = {
                "source": path.name,
                "size_bytes": path.stat().st_size,
                "schema": {field.name: field.dataType.simpleString() for field in raw.schema},
                "cleaned": output,
                "counts": counts,
                "removed_pct": 100 * counts["removed_unique"] / max(counts["original_count"], 1),
                "baseline": baseline,
                "delta_from_baseline": {name: counts[name] - value for name, value in baseline.items()},
            }
        except Exception as error:
            error.add_note(f"Categoria {label}, fase {phase}.")
            _progress(f"{label} — errore durante {phase}.")
            raise

    _progress(f"Salvataggio del report: {report_path}")
    save_json(report_path, report)
    categories = report["categories"]
    original, removed, valid = (
        sum(item["counts"][field] for item in categories.values())
        for field in ("original_count", "removed_unique", "valid_count")
    )
    _progress(
        f"Preprocessing completato per {len(categories)} categorie. "
        f"{original} record originali, {removed} esclusi e {valid} validi."
    )
    return report
