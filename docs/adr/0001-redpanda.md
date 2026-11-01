# ADR 0001 : Redpanda plutôt que Kafka

## Contexte
Il faut un journal de messages qui garde les données si ClickHouse tombe. Le débit reste modeste : environ 1 500 messages par minute, sur une seule machine.

## Options
- **Apache Kafka** : la référence, avec plus d'opérations à gérer pour un nœud unique.
- **Redpanda** : compatible avec l'API Kafka, un seul binaire, peu de mémoire.
- **Écriture directe dans ClickHouse** : plus simple, mais perte de données à la moindre panne.

## Décision
Redpanda mono-nœud, avec 48 heures de rétention sur le topic principal.

## Conséquences
- Le code producteur utilise `confluent-kafka` : migrer vers Kafka ne demande qu'un changement d'adresse.
- La tolérance se limite à une panne de ClickHouse. Un nœud unique ne protège pas d'une panne de Redpanda lui-même.
