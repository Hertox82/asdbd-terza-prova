import json
from pathlib import Path

from matplotlib.colors import LogNorm, Normalize
from matplotlib.figure import Figure
from matplotlib.ticker import FuncFormatter

from src.config import RESULTS, SLOTS

CATEGORIES = ("Yellow", "Green", "HVFHV")
WEEKDAYS = ("Lunedì", "Martedì", "Mercoledì", "Giovedì", "Venerdì", "Sabato", "Domenica")
METRICS = (
    ("total_revenue", "Ricavo totale", "USD · scala logaritmica", "heatmap_revenue.png", LogNorm),
    ("avg_distance", "Distanza media", "Miglia per corsa", "heatmap_distance.png", Normalize),
    ("avg_speed", "Velocità media", "mph", "heatmap_speed.png", Normalize),
)


def generate_heatmaps(directory=RESULTS):
    """Si generano i tre PNG dagli aggregati Spark SQL già verificati."""
    directory = Path(directory)
    aggregate_path = directory / "aggregates.json"
    report = json.loads(aggregate_path.read_text(encoding="utf-8"))
    if report.get("comparison", {}).get("verdict") != "PASS":
        raise ValueError(f"Equivalenza RDD/Spark SQL non verificata in {aggregate_path}")

    rows = {
        (row["vehicle_type"], row["weekday"], row["time_slot"]): row
        for row in report["sql"]
    }
    outputs = []
    for metric, title, unit, filename, normalization in METRICS:
        # Si dispone ciascun valore nella cella del giorno e della fascia oraria del prelievo.
        matrices = [
            [
                [rows[(category.lower(), weekday, slot)][metric] for slot in SLOTS]
                for weekday in range(1, len(WEEKDAYS) + 1)
            ]
            for category in CATEGORIES
        ]
        # Si condividono i limiti cromatici tra le tre categorie per confrontare i valori assoluti.
        values = [value for matrix in matrices for row in matrix for value in row]
        scale = normalization(vmin=min(values), vmax=max(values))

        figure = Figure(figsize=(7.1, 3.0), layout="constrained", facecolor="white")
        axes = figure.subplots(ncols=len(CATEGORIES), sharey=True)
        for axis, category, matrix in zip(axes, CATEGORIES, matrices):
            heatmap = axis.imshow(
                matrix, cmap="viridis", norm=scale, aspect="auto", origin="upper", interpolation="nearest"
            )
            axis.set_title(category, fontsize=10, fontweight="bold")
            axis.set_xticks(range(len(SLOTS)), SLOTS, fontsize=8, rotation=45, ha="right", rotation_mode="anchor")
            axis.tick_params(axis="both", length=0)
            for spine in axis.spines.values():
                spine.set_visible(False)

        axes[0].set_yticks(range(len(WEEKDAYS)), WEEKDAYS, fontsize=8)
        figure.suptitle(title, fontsize=13, fontweight="bold")
        figure.supxlabel("Fascia oraria del prelievo", fontsize=9)
        figure.supylabel("Giorno della settimana", fontsize=9)

        colorbar = figure.colorbar(heatmap, ax=axes, pad=0.025, fraction=0.04)
        colorbar.set_label(unit, fontsize=8)
        colorbar.ax.tick_params(labelsize=8)
        if normalization is LogNorm:
            colorbar.formatter = FuncFormatter(lambda value, _: f"{value:,.0f}".replace(",", "."))
        else:
            colorbar.formatter = FuncFormatter(lambda value, _: f"{value:g}".replace(".", ","))

        output = directory / filename
        figure.savefig(output, dpi=300)
        outputs.append(output)

    return tuple(outputs)


if __name__ == "__main__":
    for path in generate_heatmaps():
        print(path)
