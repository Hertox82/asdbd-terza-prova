# NYC TLC Trip Record Data — luglio 2025

Il progetto confronta RDD/MapReduce e Spark SQL sulle corse Yellow, Green e HVFHV. Per categoria, giorno della settimana e fascia oraria, calcola ricavo base totale, distanza media, velocità individuale media e numero di corse.

## Requisiti e avvio

Servono Docker con Compose v2 o successivo e circa 7,6 GiB disponibili a Docker. Lasciare spazio per le immagini, i Parquet puliti persistenti e la cache Spark temporanea su disco. Spark parte con `local[4]`, heap driver di `3g` e 12 partizioni di shuffle. I tre valori si possono modificare in `.env`.

Copiare i tre Parquet originali in `data/raw/`. I nomi devono coincidere con quelli di luglio 2025. [NYC TLC](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)

| Categoria | File |
|---|---|
| Yellow | `yellow_tripdata_2025-07.parquet` |
| Green | `green_tripdata_2025-07.parquet` |
| HVFHV | `fhvhv_tripdata_2025-07.parquet` |

Copiare `.env.example` in `.env` e valorizzare `JUPYTER_TOKEN` e `POSTGRES_PASSWORD`. Dalla radice eseguire questi comandi.

```sh
docker compose up -d --build
docker compose ps
```

Aprire `http://127.0.0.1:8888` e accedere usando il token di `.env`. Se si cambia la porta, usare `JUPYTER_PORT` nell'indirizzo. Jupyter è l'unico servizio pubblicato sull'host. HDFS e PostgreSQL sono raggiungibili nella rete Compose, rispettivamente su `hdfs://hdfs:9000` e `postgres:5432`.

I Dockerfile fissano Python 3.11.11, Eclipse Temurin Java 11.0.26+4, PySpark 3.5.5, JupyterLab 4.3.6 e Hadoop 3.3.6. Spark usa il client Hadoop incluso in PySpark. PostgreSQL usa l'immagine ufficiale 16.8. Il driver JDBC PostgreSQL 42.7.5 è nel classpath Spark. L'export dei pochi aggregati usa Psycopg 3.2.6 per inserimenti e verifica nella stessa transazione.

## Esecuzione dei notebook

Aprire la cartella `notebooks/` in JupyterLab ed eseguire tutte le celle nell'ordine seguente. Eseguire un notebook alla volta per condividere le risorse Docker.

1. `01_preprocessing.ipynb` controlla i tre originali, mostra metadati e scarti e scrive tre directory Parquet sotto `hdfs://hdfs:9000/data/nyc-tlc/2025-07/cleaned/`. Il report `results/quality.json` conserva i conteggi per regola, la differenza rispetto alla pre-analisi, le soglie e il `run_id`.
2. `02_rdd_vs_sql.ipynb` legge gli output HDFS, verifica l'equivalenza RDD/SQL e salva `results/aggregates.json` se il confronto passa. Produce poi `benchmark.csv` con 30 tempi individuali, `benchmark_summary.json` con medie, mediane, deviazioni standard campionarie e rapporti tra mediane, e `benchmark.png` con il grafico.
3. `03_postgresql.ipynb` controlla la coerenza del `run_id` dei report ed esporta in `tlc_aggregates`, `tlc_benchmark` e `tlc_quality`. Il notebook confronta gruppi e corse letti dal database con i report prima del commit. La riesecuzione con lo stesso `run_id` sostituisce solo i risultati di quella run in una transazione.

Per ripetere il benchmark sullo stesso input HDFS, riavviare il kernel del notebook 02 ed eseguirlo per intero a parità di configurazione. Il notebook rigenera il CSV, il riepilogo e il grafico. Copiare gli output che si vogliono conservare prima di rilanciare. Se serve esportare la nuova misurazione, rieseguire anche il notebook 03. Il notebook 01 genera un nuovo `run_id` ogni volta e sovrascrive i tre Parquet puliti.

Gli originali sono montati in sola lettura e non entrano nel contesto di build. `.env`, dati originali e output generati in `results/` sono ignorati da Git. Gli appunti per la relazione sono in [report/notes.md](report/notes.md).

## Verifiche minime

Eseguire i due test di logica nel servizio Jupyter.

```sh
docker compose exec jupyter python -m unittest discover -s tests -v
```

Per lo smoke check, aprire una console Python nuova in JupyterLab ed eseguire questa cella. Spark scrive e rilegge un Parquet temporaneo in HDFS e trasferisce il risultato in una tabella PostgreSQL temporanea. La tabella scompare alla chiusura della connessione e il file di prova viene rimosso.

```python
from uuid import uuid4

from src.config import HDFS_URI, spark_session
from src.postgresql import connect

spark = spark_session("TLC - smoke")
uri = f"{HDFS_URI}/data/smoke-{uuid4().hex}"
path = spark._jvm.org.apache.hadoop.fs.Path(uri)
fs = path.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
try:
    spark.range(1).write.parquet(uri)
    (value,) = spark.read.parquet(uri).select("id").first()
    with connect() as conn, conn.cursor() as cur:
        cur.execute("CREATE TEMP TABLE smoke_tlc (id bigint)")
        cur.execute("INSERT INTO smoke_tlc VALUES (%s)", (value,))
        cur.execute("SELECT id FROM smoke_tlc")
        assert cur.fetchone() == (0,)
    print("PASS: Spark, HDFS e PostgreSQL")
finally:
    try:
        fs.delete(path, True)
    finally:
        spark.stop()
```

Il notebook 03 esegue inoltre la query di verifica sugli aggregati effettivi. Rieseguirlo con gli stessi report per controllare che gruppi e conteggi restino identici.

## Arresto, ripristino e problemi comuni

- `docker compose down` arresta i servizi e conserva i volumi HDFS e PostgreSQL.
- Per ricostruire l'intero ambiente da zero, `docker compose down -v` elimina i due volumi, inclusi i Parquet puliti e gli export PostgreSQL. Riavviare con `docker compose up -d --build` ed eseguire di nuovo 01 → 02 → 03. I file nelle directory host `data/raw/` e `results/` restano su disco.
- Se manca un input, il notebook 01 elenca tutti i file richiesti assenti. Verificare i nomi, incluso il prefisso `fhvhv_`.
- Se Jupyter non si apre, controllare `docker compose ps`, la porta locale e lo stato dei servizi. Per errori HDFS o PostgreSQL, usare `docker compose logs hdfs postgres`.
- Su Linux Jupyter lavora come utente con UID 1000. Assicurarsi che `notebooks/` e `results/` siano scrivibili da quell'utente.
- In caso di pressione sulla RAM, arrestare gli altri kernel e ridurre parallelismo o heap in `.env`, poi ricreare il servizio Jupyter con `docker compose up -d jupyter`. Verificare anche lo spazio disco prima del benchmark HVFHV.
- PostgreSQL usa le credenziali di `.env` per inizializzare un volume nuovo. Se la password cambia dopo l'inizializzazione, occorre aggiornare anche il ruolo PostgreSQL o ricreare consapevolmente il volume.
