from __future__ import annotations

import importlib.util
from typing import Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


def build_spark_session(
    *,
    app_name: str,
    spark_conf: Optional[dict] = None,
    enable_hive: bool = False,
) -> "SparkSession":
    if importlib.util.find_spec("pyspark") is None:
        raise ImportError("pyspark не установлен")

    from pyspark.sql import SparkSession

    builder = SparkSession.builder.appName(app_name)
    if enable_hive:
        builder = builder.enableHiveSupport()
    for key, value in (spark_conf or {}).items():
        builder = builder.config(key, value)
    return builder.getOrCreate()


def configure_s3_access(
    spark: "SparkSession",
    *,
    aws_access_key_id: Optional[str] = None,
    aws_secret_access_key: Optional[str] = None,
    aws_session_token: Optional[str] = None,
) -> None:
    if not aws_access_key_id or not aws_secret_access_key:
        return

    jsc = getattr(spark.sparkContext, "_jsc", None)
    if jsc is None:
        return

    hadoop_conf = jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", aws_access_key_id)
    hadoop_conf.set("fs.s3a.secret.key", aws_secret_access_key)
    if aws_session_token:
        hadoop_conf.set("fs.s3a.session.token", aws_session_token)
