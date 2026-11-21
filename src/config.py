import json
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "data" / "raw"
RESULTS = ROOT / "results"
MONTH = "2025-07"
HDFS_URI = "hdfs://hdfs:9000"
CLEANED = f"{HDFS_URI}/data/nyc-tlc/{MONTH}/cleaned"
# Si imposta il fuso della sessione Spark su UTC. Per le corse si conservano gli orari civili.
TIMEZONE = "UTC"
SLOTS = ("00-04", "04-08", "08-12", "12-16", "16-20", "20-24")
# Si include l'inizio del mese e si esclude l'inizio del mese successivo.
THRESHOLDS = {
    "version": "1",
    "pickup_start": "2025-07-01 00:00:00",
    "pickup_end": "2025-08-01 00:00:00",
    "max_duration_seconds": 14_400,
    "max_distance_miles": 200,
    "max_speed_mph": 100,
    "min_revenue_usd": 0,
}
INPUTS = {
    "yellow": f"yellow_tripdata_{MONTH}.parquet",
    "green": f"green_tripdata_{MONTH}.parquet",
    "hvfhv": f"fhvhv_tripdata_{MONTH}.parquet",
}


def raw_paths(directory=RAW):
    # Si controlla la presenza di tutti i Parquet prima di avviare l'elaborazione.
    paths = {category: directory / name for category, name in INPUTS.items()}
    missing = [path.name for path in paths.values() if not path.is_file()]
    if missing:
        raise FileNotFoundError(f"Parquet mancanti in {directory}: {', '.join(missing)}")
    return paths


def spark_session(name):
    from pyspark.sql import SparkSession

    # Si possono configurare master, memoria e partizioni di shuffle tramite variabili d'ambiente.
    return (
        SparkSession.builder.appName(name)
        .master(os.getenv("SPARK_MASTER", "local[4]"))
        .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "3g"))
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "12"))
        .config("spark.sql.session.timeZone", TIMEZONE)
        .config("spark.hadoop.fs.defaultFS", HDFS_URI)
        .config("spark.hadoop.dfs.client.use.datanode.hostname", "true")
        .config("spark.jars", "/opt/postgresql.jar" if Path("/opt/postgresql.jar").is_file() else "")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


def save_json(path, content):
    # Si scrive su un file temporaneo per evitare di esporre un JSON parziale.
    temporary = path.with_suffix(".tmp")
    temporary.write_text(
        json.dumps(content, ensure_ascii=False, indent=2, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)
