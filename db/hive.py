from typing import Any, Optional

from pyspark.sql import DataFrame, SparkSession
import pandas as pd

from .base import DatabaseConnector


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

        builder = SparkSession.builder.appName(app_name or "hive-connector").enableHiveSupport()
        for key, value in (spark_conf or {}).items():
            builder = builder.config(key, value)
        self.spark = builder.getOrCreate()

    def read(self, query: str, params: Optional[dict] = None, **kwargs) -> DataFrame:
        """Выполняет spark.sql(SELECT...) и возвращает DataFrame."""
        query = query.format(**params)
        return self.spark.sql(query)

    def write(
        self,
        sql: str,
        df: Optional[Any] = None,
        target: Optional[str] = None,
        mode: str = "overwrite",
    ) -> Any:
        """
        Если передан df и target — записывает DataFrame (используя Spark DataFrame).
        Иначе просто выполняет spark.sql(sql).

        Поддерживает передачу pandas.DataFrame: тогда происходит конвертация в Spark DataFrame.
        """
        if df is None:
            raise ValueError("Hive(Spark): df обязателен для записи")

        if target is None:
            raise ValueError("Hive(Spark): target обязателен для записи")

        if sql:
            self.spark.sql(sql)
            return {"status": "ok"}

        if isinstance(df, pd.DataFrame):
            df = self.spark.createDataFrame(df) # Конвертация pandas в Spark DataFrame

        df.write.mode(mode).format('parquet').save(target)

        return {"status": "ok"}



