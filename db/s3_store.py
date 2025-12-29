from typing import Any, Optional

import pandas as pd
from pyspark.sql import SparkSession

from .base import DatabaseConnector
from .utils import build_spark_session, configure_s3_access


class S3Connector(DatabaseConnector):
    def __init__(self, spark: Optional[SparkSession] = None) -> None:
        """Коннектор для чтения/записи parquet в S3 через SparkSession."""
        self.spark = spark

    def connect(
        self,
        app_name: Optional[str] = None,
        spark_conf: Optional[dict] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
    ) -> None:
        """Создает SparkSession и настраивает доступ к S3 (через s3a://)."""
        if self.spark:
            return  # Уже подключены

        self.spark = build_spark_session(
            app_name=app_name or "s3-connector",
            spark_conf=spark_conf,
        )
        configure_s3_access(
            self.spark,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token,
        )

    def read(
        self,
        query: Optional[str] = None,
        bucket: Optional[str] = None,
        key: Optional[str] = None,
        **kwargs,
    ) -> Any:
        """Читает parquet-файл из S3 в pandas.DataFrame."""
        if not bucket or not key:
            raise ValueError("Поля bucket и key обязательны для чтения из S3")

        s3_path = f"s3a://{bucket}/{key}"
        return self.spark.read.parquet(s3_path).toPandas()

    def write(
        self,
        sql: Optional[str] = None,
        df: Optional[Any] = None,
        target: Optional[str] = None,
        table: Optional[str] = None,
        mode: str = "overwrite",
    ) -> Any:
        """Пишет DataFrame в указанный путь S3 (s3a://...)."""

        if df is None:
            raise ValueError("S3 write ожидает DataFrame в аргументе df")

        if not isinstance(df, pd.DataFrame):
            raise ValueError("S3 write ожидает pandas.DataFrame")

        spark_df = self.spark.createDataFrame(df)
        spark_df.write.mode(mode).parquet(target)
        return {"target": target, "mode": mode}
