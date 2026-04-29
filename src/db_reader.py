import pandas as pd
from sqlalchemy import text
from src.config import POSTGRES_SCHEMA
from src.db import get_engine


def table_has_run_id(table_name: str) -> bool:
    engine = get_engine()
    query = text(
        """
        SELECT COUNT(*) AS cnt
        FROM information_schema.columns
        WHERE table_schema = :schema_name
          AND table_name = :table_name
          AND column_name = 'run_id'
        """
    )
    result = pd.read_sql(
        query,
        engine,
        params={"schema_name": POSTGRES_SCHEMA, "table_name": table_name},
    )
    return int(result.iloc[0]["cnt"]) > 0


def read_latest_run_id() -> int | None:
    engine = get_engine()
    query = f"SELECT run_id FROM {POSTGRES_SCHEMA}.analysis_runs ORDER BY run_id DESC LIMIT 1"
    result = pd.read_sql(query, engine)
    if result.empty:
        return None
    return int(result.iloc[0]["run_id"])


def read_table(table_name: str, limit: int | None = None, run_id: int | None = None) -> pd.DataFrame:
    engine = get_engine()

    query = f"SELECT * FROM {POSTGRES_SCHEMA}.{table_name}"
    conditions = []

    if run_id is not None and table_has_run_id(table_name):
        conditions.append(f"run_id = {int(run_id)}")

    if conditions:
        query += " WHERE " + " AND ".join(conditions)

    if limit is not None:
        query += f" LIMIT {int(limit)}"

    return pd.read_sql(query, engine)


def read_recent_runs(limit: int = 10) -> pd.DataFrame:
    engine = get_engine()
    query = (
        f"SELECT * FROM {POSTGRES_SCHEMA}.analysis_runs "
        f"ORDER BY run_id DESC LIMIT {int(limit)}"
    )
    return pd.read_sql(query, engine)


def read_table_names() -> pd.DataFrame:
    engine = get_engine()
    query = text(
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = :schema_name
        ORDER BY table_name
        """
    )
    return pd.read_sql(query, engine, params={"schema_name": POSTGRES_SCHEMA})


def read_table_count(table_name: str, run_id: int | None = None) -> int:
    engine = get_engine()

    query = f"SELECT COUNT(*) AS row_count FROM {POSTGRES_SCHEMA}.{table_name}"
    if run_id is not None and table_has_run_id(table_name):
        query += f" WHERE run_id = {int(run_id)}"

    result = pd.read_sql(query, engine)
    return int(result.iloc[0]["row_count"])


def read_table_preview(table_name: str, limit: int = 20, run_id: int | None = None) -> pd.DataFrame:
    return read_table(table_name=table_name, limit=limit, run_id=run_id)
