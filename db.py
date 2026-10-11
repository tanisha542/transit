"""Connection helpers for TransactAlloc (phase 4+).

Points that never drift from the original lab's spirit: one connection per
database session, autocommit is deliberately opt-in, and SQL scripts are
executed in a single round trip.
"""

import pathlib

import psycopg

DB_NAME = "transactalloc_p3"
_BASE = "host=localhost port=5432 user=postgres password=crm"
DSN = f"{_BASE} dbname={DB_NAME}"
ADMIN_DSN = f"{_BASE} dbname=postgres"


def connect(autocommit=False, row_factory=None):
    # Default row factory is the plain tuple: every console module in this
    # repo unpacks rows positionally (mid, host, ... = row). api.py opts into
    # dict rows so it can serialise straight to JSON.
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


def run_sql_file(name):
    sql = (pathlib.Path(__file__).parent / name).read_text(encoding="utf-8")
    with connect(autocommit=True) as conn:
        conn.execute(sql)
    print(f"[DB] applied {name}")


def reset_all():
    with connect() as conn:
        conn.execute("SELECT ResetCluster(NULL::int)")
        conn.commit()


def reset_with_job_seq(restart_at: int | None = None):
    with connect() as conn:
        if restart_at is None:
            conn.execute("SELECT ResetCluster(NULL::int)")
        else:
            conn.execute("SELECT ResetCluster(%s)", (restart_at,))
        conn.commit()
