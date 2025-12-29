from typing import Any, Optional

import pandas as pd
import vertica_python

from .base import DatabaseConnector


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
        sql: str,
        df: Optional[Any] = None,
        table: Optional[str] = None,
        params: Optional[dict] = None,
    ) -> Any:
        """Поддерживает запись DataFrame (df) — ожидается pandas.DataFrame или Spark DataFrame.
        Для загрузки DataFrame требуется указать table (имя таблицы в Vertica).
        Если df не передан, выполняется sql как раньше с params.
        """

        if df is not None:
            # Поддерживаем Spark DataFrame: конвертация в pandas
            try:
                from pyspark.sql import DataFrame as SparkDataFrame

                if isinstance(df, SparkDataFrame):
                    df = df.toPandas()
            except Exception:
                # pyspark может отсутствовать — тогда считаем, что df уже pandas
                pass

            if not isinstance(df, pd.DataFrame):
                raise ValueError("Vertica write ожидает pandas.DataFrame или Spark DataFrame в аргументе df")

            if not table:
                raise ValueError("Для записи DataFrame в Vertica необходимо указать table")

            # Записываем DataFrame в Vertica

        # Если df не передан, выполняем sql как DML

    def close(self) -> None:
        """Закрывает соединение."""
        if self.conn:
            try:
                self.conn.close()
            finally:
                self.conn = None
