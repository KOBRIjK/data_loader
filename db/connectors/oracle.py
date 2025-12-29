from typing import Any, Optional

import importlib.util

import pandas as pd

from .base import DatabaseConnector
from ..utils.dataframe import coerce_to_pandas_dataframe
from ..utils.oracle import df_to_table


class OracleConnector(DatabaseConnector):
    def __init__(self, conn: Optional[Any] = None) -> None:
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
        if self.conn:
            return

        if importlib.util.find_spec("cx_Oracle") is None:
            raise ImportError("cx_Oracle не установлен")

        import cx_Oracle

        if dsn:
            dsn_value = dsn
        elif host and service:
            dsn_value = cx_Oracle.makedsn(host, port, service_name=service)
        else:
            raise ValueError("Нужно указать dsn или host/service")

        if not user or not password:
            raise ValueError("Нужно указать user и password")

        self.conn = cx_Oracle.connect(user=user, password=password, dsn=dsn_value)


    def read(self, query: str, params: Optional[dict] = None, **kwargs) -> Any:
        """Выполняет SELECT и возвращает DataFrame."""
        return pd.read_sql(query, con=self.conn, params=params)

    def write(
        self,
        sql: Optional[str] = None,
        df: Optional[Any] = None,
        table: Optional[str] = None,
        params: Optional[dict] = None,
    ) -> Any:
        """Если df передан — выполняем загрузку DataFrame (pandas или Spark -> pandas) в таблицу table.
        В противном случае выполняем DML из sql как раньше (с params).
        """
        if df is not None:
            df = coerce_to_pandas_dataframe(df)

            if not table:
                raise ValueError("Для записи DataFrame в Oracle необходимо указать table")

            return df_to_table(self.conn, df, table)

        if not sql:
            raise ValueError("Oracle write требует sql или df")

        cursor = self.conn.cursor()
        try:
            cursor.execute(sql, params or {})
            self.conn.commit()
            return {"rowcount": cursor.rowcount}
        finally:
            cursor.close()
