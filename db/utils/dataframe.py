from __future__ import annotations

import importlib.util
from typing import Any

import pandas as pd


def coerce_to_pandas_dataframe(df: Any) -> pd.DataFrame:
    if df is None:
        raise ValueError("Ожидался DataFrame, но получено None")

    if importlib.util.find_spec("pyspark") is not None:
        from pyspark.sql import DataFrame as SparkDataFrame

        if isinstance(df, SparkDataFrame):
            return df.toPandas()

    if not isinstance(df, pd.DataFrame):
        raise ValueError("Ожидался pandas.DataFrame или Spark DataFrame")

    return df
