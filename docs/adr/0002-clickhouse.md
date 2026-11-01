# ADR 0002 : ClickHouse plutôt que Postgres ou Timescale

## Contexte
Avec environ 2 millions de statuts par jour, les requêtes des marts et du dashboard agrègent de longues séries temporelles.

## Options
- **PostgreSQL** : très polyvalent, mais il faut gérer soi-même le partitionnement et l'agrégation devient coûteuse à ce volume.
- **TimescaleDB** : bonne option pour des séries temporelles, sans ingestion Kafka native.
- **ClickHouse** : stockage en colonnes, moteur Kafka natif, `ReplacingMergeTree` pour l'idempotence.

## Décision
ClickHouse. Le moteur Kafka et une vue matérialisée remplissent `raw_station_status`, sans service d'ingestion à maintenir.

## Conséquences
- `ReplacingMergeTree` ne dédoublonne qu'au moment des fusions, sans garantie de délai. La couche staging de dbt dédoublonne donc explicitement à la lecture, et un test le vérifie.
- La colonne `ingested_at` permet de mesurer la latence réelle.
