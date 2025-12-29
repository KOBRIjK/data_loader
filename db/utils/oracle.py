from __future__ import annotations

from typing import Any, Iterable

import pandas as pd


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
    cursor = conn.cursor()
    try:
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
        cursor.execute(query, params)
        return [(row[0], row[1]) for row in cursor.fetchall()]
    finally:
        cursor.close()


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


def df_to_table(conn: Any, df: pd.DataFrame, table: str) -> dict[str, int]:
    columns = _fetch_table_columns(conn, table)
    if not columns:
        raise ValueError(f"Не найдена таблица {table} или отсутствуют колонки")

    prepared_df = _prepare_dataframe(df, columns)
    if prepared_df.empty:
        return {"rowcount": 0}

    column_names = list(prepared_df.columns)
    column_list = ", ".join(column_names)
    bind_list = ", ".join(f":{col}" for col in column_names)
    insert_sql = f"INSERT INTO {table} ({column_list}) VALUES ({bind_list})"
    data = prepared_df.to_dict(orient="records")

    cursor = conn.cursor()
    try:
        cursor.executemany(insert_sql, data)
        conn.commit()
        return {"rowcount": cursor.rowcount}
    finally:
        cursor.close()
