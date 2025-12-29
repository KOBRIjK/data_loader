from typing import Any, Optional

import pandas as pd
import vertica_python

from .base import DatabaseConnector
from .utils import coerce_to_pandas_dataframe


class VerticaConnector(DatabaseConnector):
    def __init__(self, conn: Optional[Any] = None) -> None:
        """Коннектор Vertica; можно передать готовое соединение (для тестов)."""
        self.conn = conn

    def connect(
        self,
        host: Optional[str] = None,
        port: int = 5433,
        user: Optional[str] = None,
        password: Optional[str] = None,
        database: Optional[str] = None,
    ) -> None:
        """Создает соединение через vertica_python.connect(**conn_info)."""
        conn_info = {
            "host": host,
            "port": port,
            "user": user,
            "password": password,
            "database": database,
        }
        self.conn = vertica_python.connect(**conn_info)

    def read(self, query: str, params: Optional[dict] = None, **kwargs) -> Any:
        """Выполняет SELECT и возвращает DataFrame."""
        return pd.read_sql(query, self.conn, params=params)

    def write(
        self,
        sql: Optional[str] = None,
        df: Optional[Any] = None,
        table: Optional[str] = None,
        params: Optional[dict] = None,
    ) -> Any:
        """Поддерживает запись DataFrame (df) — ожидается pandas.DataFrame или Spark DataFrame.
        Для загрузки DataFrame требуется указать table (имя таблицы в Vertica).
        Если df не передан, выполняется sql как раньше с params.
        """

        if df is not None:
            df = coerce_to_pandas_dataframe(df)

            if not table:
                raise ValueError("Для записи DataFrame в Vertica необходимо указать table")

            columns = list(df.columns)
            if not columns:
                return {"rowcount": 0}

            column_list = ", ".join(columns)
            placeholders = ", ".join(["%s"] * len(columns))
            insert_sql = f"INSERT INTO {table} ({column_list}) VALUES ({placeholders})"
            data = [tuple(row) for row in df.itertuples(index=False, name=None)]

            with self.conn.cursor() as cursor:
                cursor.executemany(insert_sql, data)
                return {"rowcount": cursor.rowcount}

        if not sql:
            raise ValueError("Vertica write требует sql или df")

        with self.conn.cursor() as cursor:
            cursor.execute(sql, params or {})
            return {"rowcount": cursor.rowcount}

    def close(self) -> None:
        """Закрывает соединение."""
        if self.conn:
            try:
                self.conn.close()
            finally:
                self.conn = None
