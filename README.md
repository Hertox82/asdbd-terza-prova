# NYC TLC Trip Record Data — luglio 2025

## Avvio rapido

Servono Git, Python 3.11 o successivo, Docker con container Linux e Compose v2, una connessione a Internet e circa 7,6 GiB di RAM per Docker. Su disco occorrono circa 530 MiB per i dataset, oltre allo spazio per le immagini Docker, i dati elaborati e la cache di Spark.

1. Clonare il repository, scaricare i dataset e copiare il modello di configurazione.

   ```sh
   git clone https://github.com/Hertox82/asdbd-terza-prova.git
   cd asdbd-terza-prova
   python scripts/download_datasets.py
   cp .env.example .env
   ```

   Se Python è disponibile come `python3`, usare quel comando al posto di `python`. Lo script salva i dataset in `data/raw/` e verifica dimensioni e SHA-256. Per il download manuale, consultare il [README dei dataset](data/raw/README.md).

2. Aprire `.env` in un editor e impostare `JUPYTER_TOKEN` e `POSTGRES_PASSWORD` con valori a scelta. Su Linux, verificare che `notebooks/` e `results/` siano scrivibili dall’utente del container (UID 1000).

3. Avviare i servizi dalla radice del repository e controllarne lo stato.

   ```sh
   docker compose up -d --build
   docker compose ps
   ```

4. Aprire nel browser `http://127.0.0.1:8888` e accedere con il valore di `JUPYTER_TOKEN` impostato in `.env`. Se `JUPYTER_PORT` è diversa da `8888`, usare la porta configurata nell’indirizzo.

## Esecuzione dei notebook

In JupyterLab aprire la cartella `notebooks/`. Eseguire tutte le celle di ciascun notebook, completando il primo prima di avviare il secondo.

1. **`01_preprocessing.ipynb`** pulisce e normalizza i dataset Yellow, Green e HVFHV ed esporta i Parquet puliti su HDFS in configurazione pseudo-distribuita.
2. **`02_rdd_vs_sql.ipynb`** legge i Parquet da HDFS, calcola gli aggregati con RDD e Spark SQL, verifica l’equivalenza dei risultati, esegue il benchmark, genera i grafici ed esporta aggregati, misure e report di qualità in PostgreSQL.

Spark gira in modalità `local[4]` nel container Jupyter. I notebook aggiornano i file in `results/`.

## Risultati già disponibili

I risultati della consegna si possono consultare direttamente in [`results/`](results/).

- [Controlli di qualità e scarti](results/quality.json)
- [Aggregati RDD e Spark SQL](results/aggregates.json)
- [Misure del benchmark](results/benchmark.csv), [statistiche](results/benchmark_summary.json) e [grafico delle mediane](results/benchmark.png)
- Heatmap di [ricavo base totale](results/heatmap_revenue.png), [distanza media](results/heatmap_distance.png) e [velocità individuale media](results/heatmap_speed.png)

## Arresto dei servizi

Dalla radice del repository, arrestare Docker Compose mantenendo i volumi di HDFS e PostgreSQL.

```sh
docker compose down
```
