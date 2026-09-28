"""Connection helpers for TransactAlloc.
"""

import psycopg

DB_NAME = "transactalloc_p3"
_BASE = "host=localhost port=5432 user=postgres password=crm"
DSN = f"{_BASE} dbname={DB_NAME}"
ADMIN_DSN = f"{_BASE} dbname=postgres"


def connect(autocommit=False, row_factory=None):
    if row_factory is None:
        return psycopg.connect(DSN, autocommit=autocommit)
    return psycopg.connect(DSN, autocommit=autocommit, row_factory=row_factory)


def create_database():
    with psycopg.connect(ADMIN_DSN, autocommit=True) as conn:
        try:
            conn.execute(f"CREATE DATABASE {DB_NAME}")
            print(f"[DB] created database {DB_NAME}")
        except psycopg.errors.DuplicateDatabase:
            print(f"[DB] database {DB_NAME} already exists")

import pathlib

def run_sql_file(name):
    sql = (pathlib.Path(__file__).parent / name).read_text(encoding="utf-8")
    with connect(autocommit=True) as conn:
        conn.execute(sql)
    print(f"[DB] applied {name}")
