# ADR 0003 : Dagster plutôt qu'Airflow

## Contexte
L'orchestrateur lance dbt, rafraîchit le référentiel des stations et porte les contrôles qualité. L'ingestion en continu reste hors de son périmètre.

## Options
- **Airflow** : très répandu, centré sur les tâches, avec plus de services à faire tourner.
- **Dagster** : centré sur les données produites, avec des contrôles (`asset_check`) rattachés à chaque asset, et un seul processus en développement.

## Décision
Dagster, avec un asset par table produite, un planning quotidien pour le référentiel et un planning toutes les 5 minutes pour les marts.

## Conséquences
- Fraîcheur, stations manquantes et cohérence avec la capacité apparaissent à côté de l'asset concerné.
- Le mode `dagster dev` convient à ce projet, mais il ne serait pas le bon choix en production réelle.
