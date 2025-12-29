from typing import Any, Optional, TYPE_CHECKING

import pandas as pd

from .base import DatabaseConnector
from .utils import build_spark_session

if TYPE_CHECKING:
    from pyspark.sql import SparkSession
else:
    SparkSession = Any  # type: ignore


class HiveConnector(DatabaseConnector):
    def __init__(self, spark: Optional[SparkSession] = None) -> None:
        """Коннектор Hive поверх Spark; хранит SparkSession."""
        self.spark = spark

    def connect(
        self,
        app_name: Optional[str] = None,
        spark_conf: Optional[dict] = None,
    ) -> None:
        """Создает SparkSession для работы с Hive."""
        if self.spark:
            return  # Уже подключены

        self.spark = build_spark_session(
            app_name=app_name or "hive-connector",
            spark_conf=spark_conf,
            enable_hive=True,
        )

    def read(
        self,
        query: str,
        params: Optional[dict] = None,
        as_pandas: bool = True,
        **kwargs,
    ) -> Any:
        """Выполняет spark.sql(SELECT...) и возвращает pandas.DataFrame или Spark DataFrame."""
        query = query.format(**(params or {}))
        spark_df = self.spark.sql(query)
        return spark_df.toPandas() if as_pandas else spark_df

    def write(
        self,
        sql: Optional[str] = None,
        df: Optional[Any] = None,
        target: Optional[str] = None,
        mode: str = "overwrite",
    ) -> Any:
        """
        Если передан df и target — записывает DataFrame (используя Spark DataFrame).
        Иначе просто выполняет spark.sql(sql).

        Поддерживает передачу pandas.DataFrame: тогда происходит конвертация в Spark DataFrame.
        """
        if sql:
            self.spark.sql(sql)
            return {"status": "ok"}

        if df is None:
            raise ValueError("Hive(Spark): df обязателен для записи")

        if target is None:
            raise ValueError("Hive(Spark): target обязателен для записи")

        try:
            from pyspark.sql import DataFrame as SparkDataFrame
        except Exception:
            SparkDataFrame = None  # type: ignore

        if SparkDataFrame is not None and isinstance(df, SparkDataFrame):
            spark_df = df
        elif isinstance(df, pd.DataFrame):
            spark_df = self.spark.createDataFrame(df)  # Конвертация pandas в Spark DataFrame
        else:
            raise ValueError("Hive(Spark): df должен быть pandas.DataFrame или Spark DataFrame")

        spark_df.write.mode(mode).format("parquet").save(target)

        return {"status": "ok"}
