# Appunti per la relazione — NYC TLC, luglio 2025

## Definizioni e soglie implementate

- L'analisi usa il pickup in `[2025-07-01 00:00:00, 2025-08-01 00:00:00)`. Il dropoff può cadere in agosto. I timestamp TLC sono letti come orari civili di New York. La sessione Spark usa UTC per preservare i campi del calendario durante il cast a `TIMESTAMP_NTZ`, senza spostare l'ora di pickup.
- Il giorno va da lunedì = 1 a domenica = 7. Le fasce di quattro ore, da `00-04` a `20-24`, includono l'estremo iniziale ed escludono quello finale.
- I controlli di coerenza richiedono entrambi i timestamp, durata strettamente positiva, distanza positiva e velocità positiva. Le soglie sperimentali sono durata fino a 14.400 s, distanza fino a 200 mi e velocità fino a 100 mph. La velocità individuale è distanza × 3.600 / durata.
- Si sommano le tariffe base (`fare_amount` per Yellow e Green, `base_passenger_fare` per HVFHV). Sono ammesse tariffe finite e non negative, compreso lo zero. La somma usa i valori double originali senza arrotondamento. `valid_revenue_subcent` misura eventuali frazioni del centesimo con tolleranza di 10⁻⁶ centesimi. Distanza e velocità sono medie aritmetiche per corsa.
- I conteggi per regola si sovrappongono. `removed_unique` usa l'OR delle violazioni. `quality.json` conserva i delta dalla baseline categoria per categoria.

## Preprocessing del run verificato

Identificativo `20261119T232529Z-74e5d06d`. Tutti i conteggi confrontabili coincidono con la pre-analisi.

| Categoria | Originali | Scartati | Validi | Percentuale esclusa |
|---|---:|---:|---:|---:|
| Yellow | 3.898.963 | 405.122 | 3.493.841 | 10,39% |
| Green | 48.205 | 2.008 | 46.197 | 4,17% |
| HVFHV | 19.653.012 | 3.028 | 19.649.984 | 0,02% |
| **Totale** | 23.600.180 | 410.158 | 23.190.022 | 1,74% |

Spark ha confermato le 247.088 tariffe base negative in Yellow riportate dalla pre-analisi. Di queste, 224.798 corse superano gli altri controlli e 153.517 hanno anche `total_amount` positivo. Possibili rettifiche o peculiarità contabili restano ipotesi. L'esclusione delle tariffe negative delimita il perimetro economico dell'analisi. La diagnostica `valid_revenue_subcent` ha restituito zero in tutte e tre le categorie. In HVFHV, 724 righe hanno una differenza superiore a 60 secondi tra `trip_time` e la durata dai timestamp.

## Equivalenza RDD e Spark SQL

Il confronto è passato su 126 gruppi, 42 per categoria, con 23.190.022 corse per metodo. Le differenze assolute massime osservate sono 0 USD per il ricavo, 0 mi per la distanza e 0 mph per la velocità. Il codice usa una tolleranza relativa di 10⁻¹⁰ e tolleranze assolute di 10⁻⁶ USD per il ricavo e 10⁻⁹ per le due medie, per tenere conto di possibili differenze nell'ordine delle somme floating-point.

## Benchmark

| Categoria | Metodo | Media (s) | Mediana (s) | Dev. std. campionaria (s) | Mediana RDD / SQL |
|---|---|---:|---:|---:|---:|
| Green | RDD | 0,452 | 0,448 | 0,018 | 1,96× |
| Green | Spark SQL | 0,241 | 0,228 | 0,047 |  |
| Yellow | RDD | 9,773 | 9,726 | 0,373 | 24,35× |
| Yellow | Spark SQL | 0,402 | 0,399 | 0,045 |  |
| HVFHV | RDD | 52,327 | 52,240 | 0,534 | 52,75× |
| HVFHV | Spark SQL | 0,998 | 0,990 | 0,083 |  |

In questa esecuzione Spark SQL ha avuto una mediana inferiore in tutte le categorie. Il rapporto RDD/SQL è stato di 1,96× per Green, 24,35× per Yellow e 52,75× per HVFHV. Le 30 misure sono in [benchmark.csv](../results/benchmark.csv) e il confronto visivo è in [benchmark.png](../results/benchmark.png).

Ogni metodo ha eseguito un warm-up seguito da cinque misure per categoria. Il tempo include la scansione della stessa cache su disco e termina dopo la raccolta della tabella aggregata. Il confronto include l'eventuale passaggio JVM/Python di RDD. Vale per questa macchina, con 12 CPU visibili a Docker e 7,58 GiB di RAM per Docker. Spark ha usato `local[4]`, heap driver `3g` e 12 partizioni di shuffle configurate. CPU, cache del sistema operativo e disco possono spostare i tempi in una riesecuzione.

## Verifiche e persistenza

L'ambiente verificato usa Python 3.11.11, Java 11.0.26+4, PySpark 3.5.5, JupyterLab 4.3.6, Hadoop 3.3.6 e PostgreSQL 16.8. In Compose sono passati i due test Spark sintetici e lo smoke check di lettura e scrittura HDFS e PostgreSQL.

Il notebook 03 è stato eseguito due volte con lo stesso identificativo. Dopo entrambe le esportazioni, PostgreSQL conteneva 252 aggregati, 30 tempi e un report qualità per quel run. La query ha confermato per ciascuna categoria entrambi i metodi, 42 gruppi per metodo e i conteggi di corse del preprocessing. Spark ha letto 252 aggregati anche tramite JDBC.
