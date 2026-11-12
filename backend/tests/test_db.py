import psycopg

from pipeline.cli import main

LAYER_SCHEMAS = {"meta", "bronze", "silver", "gold"}


def test_init_db_is_idempotent_and_creates_layer_schemas(conn: psycopg.Connection) -> None:
    assert main(["init-db"]) == 0
    assert main(["init-db"]) == 0

    rows = conn.execute(
        "SELECT schema_name FROM information_schema.schemata WHERE schema_name = ANY(%s)",
        (list(LAYER_SCHEMAS),),
    ).fetchall()
    assert {r["schema_name"] for r in rows} == LAYER_SCHEMAS
