import sys
import types

import pandas as pd

from db.connectors.hive import HiveConnector
from db.connectors.oracle import OracleConnector
from db.connectors.s3_store import S3Connector
from db.connectors.vertica import VerticaConnector


class FakeWriter:
    def __init__(self) -> None:
        self.mode_value = None
        self.format_value = None
        self.target = None

    def mode(self, mode: str) -> "FakeWriter":
        self.mode_value = mode
        return self

    def format(self, fmt: str) -> "FakeWriter":
        self.format_value = fmt
        return self

    def save(self, target: str) -> None:
        self.target = target


class FakeSparkDataFrame:
    def __init__(self, pandas_df: pd.DataFrame) -> None:
        self._pandas_df = pandas_df
        self.write = FakeWriter()

    def toPandas(self) -> pd.DataFrame:
        return self._pandas_df


class FakeSparkReader:
    def __init__(self, spark_df: FakeSparkDataFrame) -> None:
        self._spark_df = spark_df
        self.last_path = None

    def parquet(self, path: str) -> FakeSparkDataFrame:
        self.last_path = path
        return self._spark_df


class FakeSparkSession:
    def __init__(self, spark_df: FakeSparkDataFrame) -> None:
        self._spark_df = spark_df
        self.read = FakeSparkReader(spark_df)
        self.last_query = None
        self.created_from = []

    def sql(self, query: str) -> FakeSparkDataFrame:
        self.last_query = query
        return self._spark_df

    def createDataFrame(self, df: pd.DataFrame) -> FakeSparkDataFrame:
        self.created_from.append(df)
        return self._spark_df


class FakeResult:
    def __init__(self, rows: list[tuple[str, str]] | None = None, rowcount: int = 0) -> None:
        self._rows = rows or []
        self.rowcount = rowcount

    def fetchall(self) -> list[tuple[str, str]]:
        return list(self._rows)

    def scalar(self) -> str | None:
        if not self._rows:
            return None
        return self._rows[0][0]

class FakeTransaction:
    def __enter__(self) -> "FakeTransaction":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None


class FakeOracleConnection:
    def __init__(self) -> None:
        self.executed_sql = []
        self.executed_params = []
        self.metadata_rows = []
        self.to_sql_calls = []

    def begin(self) -> FakeTransaction:
        return FakeTransaction()

    def execute(self, sql: object, params: dict | list[dict]) -> FakeResult:
        sql_text = str(sql)
        self.executed_sql.append(sql_text)
        self.executed_params.append(params)
        if "current_schema" in sql_text:
            return FakeResult(rows=[("public",)])
        if "user_tab_columns" in sql_text or "all_tab_columns" in sql_text:
            return FakeResult(rows=self.metadata_rows)
        if "v_catalog.columns" in sql_text:
            return FakeResult(rows=self.metadata_rows)
        if isinstance(params, list):
            return FakeResult(rowcount=len(params))
        return FakeResult(rowcount=1)


class FakeVerticaCursor:
    def __init__(self) -> None:
        self.executemany_sql = None
        self.executemany_data = None
        self.rowcount = 0

    def executemany(self, sql: str, data: list[tuple]) -> None:
        self.executemany_sql = sql
        self.executemany_data = data
        self.rowcount = len(data)

    def __enter__(self) -> "FakeVerticaCursor":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None


class FakeVerticaConnection:
    def __init__(self, cursor: FakeVerticaCursor) -> None:
        self._cursor = cursor

    def cursor(self) -> FakeVerticaCursor:
        return self._cursor


def install_fake_pyspark():
    pyspark_module = types.ModuleType("pyspark")
    sql_module = types.ModuleType("pyspark.sql")

    class DataFrame:
        pass

    sql_module.DataFrame = DataFrame
    pyspark_module.sql = sql_module
    sys.modules["pyspark"] = pyspark_module
    sys.modules["pyspark.sql"] = sql_module
    return DataFrame


def test_hive_read_as_pandas_true():
    pandas_df = pd.DataFrame({"id": [1]})
    spark_df = FakeSparkDataFrame(pandas_df)
    spark_session = FakeSparkSession(spark_df)
    connector = HiveConnector(spark=spark_session)

    result = connector.read("select * from t", as_pandas=True)

    assert result.equals(pandas_df)
    assert spark_session.last_query == "select * from t"


def test_hive_read_as_pandas_false():
    pandas_df = pd.DataFrame({"id": [1]})
    spark_df = FakeSparkDataFrame(pandas_df)
    spark_session = FakeSparkSession(spark_df)
    connector = HiveConnector(spark=spark_session)

    result = connector.read("select * from t", as_pandas=False)

    assert result is spark_df


def test_hive_write_accepts_spark_df():
    DataFrame = install_fake_pyspark()
    pandas_df = pd.DataFrame({"id": [1]})

    class SparkDF(FakeSparkDataFrame, DataFrame):
        pass

    spark_df = SparkDF(pandas_df)
    spark_session = FakeSparkSession(spark_df)
    connector = HiveConnector(spark=spark_session)

    response = connector.write(df=spark_df, target="s3://bucket/path")

    assert response == {"status": "ok"}
    assert spark_df.write.target == "s3://bucket/path"


def test_hive_write_accepts_pandas_df():
    install_fake_pyspark()
    pandas_df = pd.DataFrame({"id": [1]})
    spark_df = FakeSparkDataFrame(pandas_df)
    spark_session = FakeSparkSession(spark_df)
    connector = HiveConnector(spark=spark_session)

    response = connector.write(df=pandas_df, target="s3://bucket/path")

    assert response == {"status": "ok"}
    assert spark_session.created_from == [pandas_df]
    assert spark_df.write.target == "s3://bucket/path"


def test_s3_read_returns_pandas_df():
    pandas_df = pd.DataFrame({"id": [1]})
    spark_df = FakeSparkDataFrame(pandas_df)
    spark_session = FakeSparkSession(spark_df)
    connector = S3Connector(spark=spark_session)

    result = connector.read(bucket="bucket", key="path/file.parquet")

    assert result.equals(pandas_df)
    assert spark_session.read.last_path == "s3a://bucket/path/file.parquet"


def test_s3_write_requires_pandas_df():
    pandas_df = pd.DataFrame({"id": [1]})
    spark_df = FakeSparkDataFrame(pandas_df)
    spark_session = FakeSparkSession(spark_df)
    connector = S3Connector(spark=spark_session)

    response = connector.write(df=pandas_df, target="s3a://bucket/path")

    assert response["target"] == "s3a://bucket/path"
    assert spark_session.created_from == [pandas_df]


def test_oracle_write_dataframe_inserts():
    conn = FakeOracleConnection()
    conn.metadata_rows = [("ID", "NUMBER"), ("NAME", "VARCHAR2")]
    connector = OracleConnector(conn=conn)
    df = pd.DataFrame({"name": ["a", "b"], "id": ["1", "2"]})
    original_to_sql = pd.DataFrame.to_sql

    def fake_to_sql(self, name, con, schema=None, if_exists=None, index=None, method=None):
        con.to_sql_calls.append(
            {
                "name": name,
                "schema": schema,
                "if_exists": if_exists,
                "index": index,
                "method": method,
                "columns": list(self.columns),
                "rows": self.to_dict(orient="records"),
            }
        )

    pd.DataFrame.to_sql = fake_to_sql
    try:
        result = connector.write(df=df, table="target_table")
    finally:
        pd.DataFrame.to_sql = original_to_sql

    assert result["rowcount"] == 2
    assert conn.to_sql_calls == [
        {
            "name": "target_table",
            "schema": None,
            "if_exists": "append",
            "index": False,
            "method": "multi",
            "columns": ["ID", "NAME"],
            "rows": [{"ID": 1, "NAME": "a"}, {"ID": 2, "NAME": "b"}],
        }
    ]


def test_vertica_write_dataframe_inserts():
    conn = FakeOracleConnection()
    conn.metadata_rows = [("id", "int"), ("name", "varchar")]
    connector = VerticaConnector(conn=conn)
    df = pd.DataFrame({"name": ["a"], "id": [1]})
    original_to_sql = pd.DataFrame.to_sql

    def fake_to_sql(self, name, con, schema=None, if_exists=None, index=None, method=None):
        con.to_sql_calls.append(
            {
                "name": name,
                "schema": schema,
                "if_exists": if_exists,
                "index": index,
                "method": method,
                "columns": list(self.columns),
                "rows": self.to_dict(orient="records"),
            }
        )

    pd.DataFrame.to_sql = fake_to_sql
    try:
        result = connector.write(df=df, table="target_table")
    finally:
        pd.DataFrame.to_sql = original_to_sql

    assert result["rowcount"] == 1
    assert conn.to_sql_calls == [
        {
            "name": "target_table",
            "schema": None,
            "if_exists": "append",
            "index": False,
            "method": "multi",
            "columns": ["id", "name"],
            "rows": [{"id": 1, "name": "a"}],
        }
    ]
