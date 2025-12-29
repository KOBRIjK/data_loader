from typing import Any, Optional, Dict

from pyspark.sql import SparkSession
import pandas as pd

from .base import DatabaseConnector


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

        builder = SparkSession.builder.appName(app_name or "s3-connector")
        for key, value in (spark_conf or {}).items():
            builder = builder.config(key, value)
        self.spark = builder.getOrCreate()

        if aws_access_key_id and aws_secret_access_key:
            jsc = getattr(self.spark.sparkContext, "_jsc", None)
            if jsc is not None:
                hadoop_conf = jsc.hadoopConfiguration()
                hadoop_conf.set("fs.s3a.access.key", aws_access_key_id)
                hadoop_conf.set("fs.s3a.secret.key", aws_secret_access_key)
                if aws_session_token:
                    hadoop_conf.set("fs.s3a.session.token", aws_session_token)

    def read(self, bucket: str, key: str, **kwargs) -> Any:
        """Читает parquet-файл из S3 в DataFrame."""
        if not bucket or not key:
            raise ValueError("Поля bucket и key обязательны для чтения из S3")

        s3_path = f"s3a://{bucket}/{key}"
        return self.spark.read.parquet(s3_path)

    def write(
        self,
        sql: str,
        df: Optional[Any] = None,
        target: Optional[str] = None,
        table: Optional[str] = None,
        mode: str = "overwrite",
    ) -> Any:
        """Пишет DataFrame в указанный путь S3 (s3a://...)."""

        if df is None:
            raise ValueError("S3 write ожидает DataFrame в аргументе df")

        # Если передан pandas DataFrame — конвертируем в Spark DataFrame
        if isinstance(df, pd.DataFrame):
            df = self.spark.createDataFrame(df)

        df.write.mode(mode).parquet(target)
        return {"target": target, "mode": mode}
