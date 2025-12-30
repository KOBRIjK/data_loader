from typing import Any, Optional

import pandas as pd
from sqlalchemy import create_engine, text

from .base import DatabaseConnector
from ..utils.dataframe import coerce_to_pandas_dataframe
from ..utils.vertica import df_to_table


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
        """Создает соединение через SQLAlchemy Engine."""
        if not user or not password:
            raise ValueError("Нужно указать user и password")
        if not host or not database:
            raise ValueError("Нужно указать host и database")
        self.conn = create_engine(
            f"vertica+vertica_python://{user}:{password}@{host}:{port}/{database}"
        )

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
            return df_to_table(self.conn, df, table)

        if not sql:
            raise ValueError("Vertica write требует sql или df")

        with self.conn.begin() as connection:
            result = connection.execute(text(sql), params or {})
            return {"rowcount": result.rowcount}

    def close(self) -> None:
        """Закрывает соединение."""
        if self.conn:
            try:
                self.conn.dispose()
            finally:
                self.conn = None
