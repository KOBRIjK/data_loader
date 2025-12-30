from __future__ import annotations

from typing import Any, Iterable

import pandas as pd
from sqlalchemy import text


_NUMERIC_TYPES = {
    "NUMBER",
    "FLOAT",
    "BINARY_FLOAT",
    "BINARY_DOUBLE",
    "INTEGER",
    "DECIMAL",
}
_DATETIME_TYPES = {
    "DATE",
    "TIMESTAMP",
    "TIMESTAMP WITH TIME ZONE",
    "TIMESTAMP WITH LOCAL TIME ZONE",
}
_STRING_TYPES = {
    "CHAR",
    "NCHAR",
    "VARCHAR2",
    "NVARCHAR2",
    "CLOB",
    "NCLOB",
}


def _split_table_name(table: str) -> tuple[str | None, str]:
    if "." in table:
        schema, table_name = table.split(".", 1)
        return schema.strip('"'), table_name.strip('"')
    return None, table.strip('"')


def _fetch_table_columns(conn: Any, table: str) -> list[tuple[str, str]]:
    schema, table_name = _split_table_name(table)
    if schema:
        query = (
            "SELECT column_name, data_type "
            "FROM all_tab_columns "
            "WHERE owner = :owner AND table_name = :table_name "
            "ORDER BY column_id"
        )
        params = {"owner": schema.upper(), "table_name": table_name.upper()}
    else:
        query = (
            "SELECT column_name, data_type "
            "FROM user_tab_columns "
            "WHERE table_name = :table_name "
            "ORDER BY column_id"
        )
        params = {"table_name": table_name.upper()}
    result = conn.execute(text(query), params)
    return [(row[0], row[1]) for row in result.fetchall()]


def _prepare_dataframe(df: pd.DataFrame, columns: Iterable[tuple[str, str]]) -> pd.DataFrame:
    df_columns_map = {col.upper(): col for col in df.columns}
    table_columns = [column_name for column_name, _ in columns]
    table_column_set = {column_name.upper() for column_name in table_columns}

    extra_columns = [col for col in df.columns if col.upper() not in table_column_set]
    if extra_columns:
        raise ValueError(
            "DataFrame содержит колонки, которых нет в таблице: "
            + ", ".join(extra_columns)
        )

    ordered_columns = [
        column_name for column_name, _ in columns if column_name.upper() in df_columns_map
    ]

    if not ordered_columns:
        return df.iloc[0:0].copy()

    prepared = df[[df_columns_map[col.upper()] for col in ordered_columns]].copy()
    prepared.columns = ordered_columns

    oracle_types = {column_name: data_type for column_name, data_type in columns}
    for column_name, data_type in oracle_types.items():
        if column_name not in prepared.columns:
            continue
        normalized_type = data_type.upper()
        if normalized_type in _NUMERIC_TYPES:
            prepared[column_name] = pd.to_numeric(prepared[column_name], errors="coerce")
        elif normalized_type in _DATETIME_TYPES:
            prepared[column_name] = pd.to_datetime(prepared[column_name], errors="coerce")
        elif normalized_type in _STRING_TYPES:
            prepared[column_name] = prepared[column_name].astype("string")

    return prepared


def _open_connection(conn_or_engine: Any) -> tuple[Any, bool]:
    if hasattr(conn_or_engine, "connect") and not hasattr(conn_or_engine, "execute"):
        return conn_or_engine.connect(), True
    return conn_or_engine, False


def df_to_table(conn_or_engine: Any, df: pd.DataFrame, table: str) -> dict[str, int]:
    connection, should_close = _open_connection(conn_or_engine)
    try:
        columns = _fetch_table_columns(connection, table)
        if not columns:
            raise ValueError(f"Не найдена таблица {table} или отсутствуют колонки")

        prepared_df = _prepare_dataframe(df, columns)
        if prepared_df.empty:
            return {"rowcount": 0}

        schema, table_name = _split_table_name(table)
        with connection.begin():
            prepared_df.to_sql(
                table_name,
                con=connection,
                schema=schema,
                if_exists="append",
                index=False,
                method="multi",
            )
        return {"rowcount": len(prepared_df)}
    finally:
        if should_close:
            connection.close()
