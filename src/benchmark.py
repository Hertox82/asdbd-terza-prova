import csv
from statistics import mean, median, stdev
from time import perf_counter

from pyspark import StorageLevel

from src.aggregations import aggregate_rdd, aggregate_sql
from src.config import RESULTS, save_json

CATEGORIES = ("green", "yellow", "hvfhv")
METHODS = {"rdd": aggregate_rdd, "sql": aggregate_sql}
RUNS = 5


def benchmark(frames, run_id):
    timings = []
    summary = {}
    for index, category in enumerate(CATEGORIES):
        # Si materializza una categoria alla volta sul disco locale con count().
        frame = frames[category].persist(StorageLevel.DISK_ONLY)
        try:
            count = frame.count()
            spark = frame.sparkSession
            config = {
                "spark_version": spark.version,
                "spark_master": spark.sparkContext.master,
                "driver_memory": spark.sparkContext.getConf().get("spark.driver.memory"),
                "shuffle_partitions": spark.conf.get("spark.sql.shuffle.partitions"),
                "input_storage": "DISK_ONLY",
                "input_rows": count,
            }
            # Si alterna l'ordine iniziale tra categorie e si esegue un riscaldamento senza cronometro.
            algorithms = tuple(METHODS)
            if index % 2:
                algorithms = algorithms[::-1]
            for algorithm in algorithms:
                METHODS[algorithm](frame)

            # Si alterna l'ordine nelle prove per ridurre l'effetto della posizione sui tempi.
            samples = {algorithm: [] for algorithm in algorithms}
            for run in range(1, RUNS + 1):
                order = algorithms if run % 2 else algorithms[::-1]
                for position, algorithm in enumerate(order, start=1):
                    start = perf_counter()
                    METHODS[algorithm](frame)
                    seconds = perf_counter() - start
                    samples[algorithm].append(seconds)
                    timings.append({
                        "run_id": run_id,
                        "category": category,
                        "algorithm": algorithm,
                        "run": run,
                        "seconds": seconds,
                        "order": position,
                        **config,
                    })
        finally:
            frame.unpersist(blocking=True)

        summary[category] = {
            algorithm: {
                "mean": mean(values),
                "median": median(values),
                "stdev": stdev(values),
            }
            for algorithm, values in samples.items()
        }
        # Si divide la mediana RDD per quella SQL. Un valore maggiore di uno indica che SQL è più veloce.
        summary[category]["speedup_rdd_over_sql"] = (
            summary[category]["rdd"]["median"] / summary[category]["sql"]["median"]
        )

    return timings, summary


def save_benchmark(timings, summary):
    import matplotlib.pyplot as plt

    run_id = timings[0]["run_id"]
    save_json(RESULTS / "benchmark_summary.json", {"run_id": run_id, "categories": summary})

    fig, axis = plt.subplots(figsize=(7, 4), layout="constrained")
    positions = range(len(CATEGORIES))
    for algorithm, shift, label, color in (
        ("rdd", -0.19, "RDD", "#216869"),
        ("sql", 0.19, "Spark SQL", "#764a83"),
    ):
        bars = axis.bar(
            [position + shift for position in positions],
            [summary[category][algorithm]["median"] for category in CATEGORIES],
            width=0.38,
            label=label,
            color=color,
        )
        axis.bar_label(bars, fmt="%.2f s", padding=4)
    axis.set_xticks(list(positions), [category.upper() for category in CATEGORIES])
    axis.set_ylabel("Mediana (secondi)")
    axis.set_title("Tempo di aggregazione per categoria")
    axis.margins(y=0.12)
    axis.legend()
    fig.savefig(RESULTS / "benchmark.png", dpi=150)
    plt.close(fig)

    # Si sostituisce il CSV solo dopo aver terminato la scrittura.
    temporary = RESULTS / "benchmark.tmp"
    with temporary.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(timings[0]))
        writer.writeheader()
        writer.writerows(timings)
    temporary.replace(RESULTS / "benchmark.csv")
