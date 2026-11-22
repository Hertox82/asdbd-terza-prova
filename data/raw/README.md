# Dataset originali NYC TLC di luglio 2025

I tre file provengono da [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) e occupano complessivamente circa 530 MiB.

| Categoria | File e URL ufficiale |
|---|---|
| Yellow | [yellow_tripdata_2025-07.parquet](https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-07.parquet) |
| Green | [green_tripdata_2025-07.parquet](https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-07.parquet) |
| HVFHV | [fhvhv_tripdata_2025-07.parquet](https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2025-07.parquet) |

Dalla radice del repository eseguire con Python 3.11 o successivo.

```sh
python scripts/download_datasets.py
```

Lo script scarica i file mancanti in `data/raw/` e li rende disponibili solo dopo il controllo della dimensione e dello SHA-256. Verifica anche i file già presenti senza riscaricarli o sovrascriverli. Il [manifest](manifest.json) contiene le dimensioni e gli SHA-256 calcolati sugli originali usati per l'esperimento.

Per il download manuale, salvare i file dai link della tabella in `data/raw/` con i nomi indicati e lanciare lo script per verificarli. Usare questa procedura anche se il filesystem di destinazione non supporta gli hard link usati per salvare in sicurezza i download. Se un file esistente fallisce la verifica, spostarlo fuori da `data/raw/` e rilanciare lo script.
