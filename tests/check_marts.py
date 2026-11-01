"""After `dbt build` on the CI fixture: the marts must hold exactly what the fixture implies."""

import os

import clickhouse_connect

client = clickhouse_connect.get_client(
    host=os.environ.get("CLICKHOUSE_HOST", "localhost"),
    port=int(os.environ.get("CLICKHOUSE_PORT", "8123")),
)


def scalar(sql: str):
    return client.query(sql).result_rows[0][0]


rows = scalar("select count() from marts.stg_station_status where station_id = 2")
assert rows == 1, f"un rejeu a créé {rows} lignes pour une seule clé logique"

episodes = client.query(
    "select state, duration_minutes from marts.mart_station_empty_full "
    "where station_id = 1 order by started_at"
).result_rows
assert episodes == [("empty", 30), ("full", 30)], f"épisodes inattendus : {episodes}"

# At 09:30, the network counts station 1 (6 bikes) and station 2 with its last known status (4).
bikes = scalar(
    "select bikes_available from marts.mart_network_kpis where bucket_utc = '2035-01-01 09:30:00'"
)
assert bikes == 10, f"réseau à 09:30 : {bikes} vélos au lieu de 10"

print("marts conformes")
