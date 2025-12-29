from typing import Any, Optional

try:
    import cx_Oracle
except Exception:
    cx_Oracle = None  # type: ignore

import pandas as pd

from .base import DatabaseConnector


class OracleConnector(DatabaseConnector):
    def __init__(self, conn: Optional[cx_Oracle.Connection] = None) -> None:
        """Коннектор Oracle; можно передать готовое соединение (для тестов)."""
        self.conn = conn

    def connect(
        self,
        dsn: Optional[str] = None,
        host: Optional[str] = None,
        port: int = 1521,
        service: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
    ) -> None:
        """Подключается через cx_Oracle (dsn/user/password или host/port/service)."""


    def read(self, query: str, params: Optional[dict] = None, **kwargs) -> Any:
        """Выполняет SELECT и возвращает DataFrame."""
        return pd.read_sql(query, con=self.conn, params=params)

    def write(
        self,
        sql: str,
        df: Optional[Any] = None,
        table: Optional[str] = None,
        params: Optional[dict] = None,
    ) -> Any:
        """Если df передан — выполняем загрузку DataFrame (pandas или Spark -> pandas) в таблицу table.
        В противном случае выполняем DML из sql как раньше (с params).
        """
        if df is not None:
            # Поддерживаем Spark DataFrame: конвертация в pandas
            try:
                from pyspark.sql import DataFrame as SparkDataFrame

                if isinstance(df, SparkDataFrame):
                    df = df.toPandas()
            except Exception:
                pass

            if not isinstance(df, pd.DataFrame):
                raise ValueError("Oracle write ожидает pandas.DataFrame или Spark DataFrame в аргументе df")

            if not table:
                raise ValueError("Для записи DataFrame в Oracle необходимо указать table")

            # Загрузка pandas DataFrame в Oracle

        # Если df не передан: выполнить sql
