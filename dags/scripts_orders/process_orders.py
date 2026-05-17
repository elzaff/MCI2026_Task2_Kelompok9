import glob
import os
from pathlib import Path

import pandas as pd
from clickhouse_driver import Client
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


DATA_LAKE_DIR = os.getenv("ORDERS_DATA_LAKE_DIR", "/opt/airflow/data_lake/orders")
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse-server")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "admin")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "rahasia")
CLICKHOUSE_DATABASE = os.getenv("CLICKHOUSE_DATABASE", "analytics")


RAW_TABLE = "orders_order_items"
PRODUCT_TABLE = "orders_product_summary"
DEPARTMENT_TABLE = "orders_department_summary"
HOURLY_TABLE = "orders_hourly_summary"

INTEGER_COLUMNS = {
    RAW_TABLE: {
        "order_id",
        "user_id",
        "order_number",
        "order_dow",
        "order_hour_of_day",
        "product_id",
        "aisle_id",
        "department_id",
        "add_to_cart_order",
        "reordered",
    },
    PRODUCT_TABLE: {
        "product_id",
        "total_order_lines",
        "unique_orders",
        "reordered_count",
    },
    DEPARTMENT_TABLE: {
        "department_id",
        "total_order_lines",
        "unique_orders",
        "unique_products",
        "reordered_count",
    },
    HOURLY_TABLE: {
        "order_dow",
        "order_hour_of_day",
        "total_orders",
        "total_order_lines",
        "unique_users",
    },
}


def _clickhouse_client():
    return Client(
        host=CLICKHOUSE_HOST,
        user=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
    )


def _spark_file_uri(path):
    resolved_path = str(Path(path).resolve()).replace("\\", "/")
    if resolved_path.startswith("/"):
        return f"file://{resolved_path}"
    return f"file:///{resolved_path}"


def _prepare_pandas_for_clickhouse(df, table):
    if df.empty:
        return []

    integer_columns = INTEGER_COLUMNS.get(table, set())
    rows = []

    for record in df.to_dict("records"):
        row = []
        for column, value in record.items():
            if pd.isna(value):
                row.append(None)
            elif column in integer_columns:
                row.append(int(value))
            elif isinstance(value, pd.Timestamp):
                row.append(value.to_pydatetime())
            else:
                row.append(value)
        rows.append(tuple(row))

    return rows


def _create_tables(client):
    client.execute(f"CREATE DATABASE IF NOT EXISTS {CLICKHOUSE_DATABASE}")

    client.execute(f"""
        CREATE TABLE IF NOT EXISTS {CLICKHOUSE_DATABASE}.{RAW_TABLE} (
            order_id UInt64,
            user_id UInt64,
            order_number UInt32,
            order_dow UInt8,
            order_hour_of_day UInt8,
            days_since_prior_order Nullable(Float64),
            eval_set String,
            product_id Nullable(UInt64),
            product_name Nullable(String),
            aisle_id Nullable(UInt32),
            aisle Nullable(String),
            department_id Nullable(UInt32),
            department Nullable(String),
            add_to_cart_order Nullable(UInt32),
            reordered Nullable(UInt8),
            ingested_at DateTime
        ) ENGINE = MergeTree()
        ORDER BY order_id
    """)

    client.execute(f"""
        CREATE TABLE IF NOT EXISTS {CLICKHOUSE_DATABASE}.{PRODUCT_TABLE} (
            product_id UInt64,
            product_name String,
            aisle String,
            department String,
            total_order_lines UInt64,
            unique_orders UInt64,
            reordered_count UInt64,
            reorder_rate Float64,
            avg_add_to_cart_order Float64
        ) ENGINE = MergeTree()
        ORDER BY total_order_lines
    """)

    client.execute(f"""
        CREATE TABLE IF NOT EXISTS {CLICKHOUSE_DATABASE}.{DEPARTMENT_TABLE} (
            department_id UInt32,
            department String,
            total_order_lines UInt64,
            unique_orders UInt64,
            unique_products UInt64,
            reordered_count UInt64,
            reorder_rate Float64
        ) ENGINE = MergeTree()
        ORDER BY total_order_lines
    """)

    client.execute(f"""
        CREATE TABLE IF NOT EXISTS {CLICKHOUSE_DATABASE}.{HOURLY_TABLE} (
            order_dow UInt8,
            order_hour_of_day UInt8,
            total_orders UInt64,
            total_order_lines UInt64,
            unique_users UInt64,
            avg_items_per_order Float64
        ) ENGINE = MergeTree()
        ORDER BY (order_dow, order_hour_of_day)
    """)


def _truncate_tables(client):
    for table in [RAW_TABLE, PRODUCT_TABLE, DEPARTMENT_TABLE, HOURLY_TABLE]:
        client.execute(f"TRUNCATE TABLE {CLICKHOUSE_DATABASE}.{table}")


def _insert_dataframe(client, table, df):
    rows = _prepare_pandas_for_clickhouse(df, table)
    if rows:
        client.execute(f"INSERT INTO {CLICKHOUSE_DATABASE}.{table} VALUES", rows)


def _cleanup_processed_files(files):
    print("Membersihkan file Parquet orders yang sudah diproses...")
    for file_path in files:
        try:
            os.remove(file_path)
        except OSError as exc:
            print(f"Gagal menghapus {file_path}: {exc}")


def run_orders_pipeline():
    parquet_files = glob.glob(os.path.join(DATA_LAKE_DIR, "*.parquet"))
    if not parquet_files:
        print(f"Tidak ada file Parquet orders di {DATA_LAKE_DIR}. Tidak ada yang diproses.")
        return

    spark = SparkSession.builder \
        .appName("Orders_Ecommerce_Analytics") \
        .config("spark.driver.memory", "1g") \
        .getOrCreate()

    try:
        print("Membaca data orders dari Data Lake...")
        df_raw = spark.read.parquet(_spark_file_uri(DATA_LAKE_DIR))

        clean_df = df_raw.select(
            F.col("order_id").cast("long").alias("order_id"),
            F.col("user_id").cast("long").alias("user_id"),
            F.col("order_number").cast("int").alias("order_number"),
            F.col("order_dow").cast("int").alias("order_dow"),
            F.col("order_hour_of_day").cast("int").alias("order_hour_of_day"),
            F.col("days_since_prior_order").cast("double").alias("days_since_prior_order"),
            F.coalesce(F.col("eval_set"), F.lit("")).alias("eval_set"),
            F.col("product_id").cast("long").alias("product_id"),
            F.coalesce(F.col("product_name"), F.lit("")).alias("product_name"),
            F.col("aisle_id").cast("int").alias("aisle_id"),
            F.coalesce(F.col("aisle"), F.lit("")).alias("aisle"),
            F.col("department_id").cast("int").alias("department_id"),
            F.coalesce(F.col("department"), F.lit("")).alias("department"),
            F.col("add_to_cart_order").cast("int").alias("add_to_cart_order"),
            F.col("reordered").cast("int").alias("reordered"),
            F.col("ingested_at").cast("timestamp").alias("ingested_at"),
        )

        product_rows = clean_df.filter(F.col("product_id").isNotNull())

        print("Menghitung ringkasan produk, department, dan pola waktu order...")
        product_summary = product_rows.groupBy(
            "product_id",
            "product_name",
            "aisle",
            "department",
        ).agg(
            F.count("*").alias("total_order_lines"),
            F.countDistinct("order_id").alias("unique_orders"),
            F.sum(F.coalesce(F.col("reordered"), F.lit(0))).alias("reordered_count"),
            F.avg(F.coalesce(F.col("reordered"), F.lit(0))).alias("reorder_rate"),
            F.avg("add_to_cart_order").alias("avg_add_to_cart_order"),
        ).orderBy(F.desc("total_order_lines")).limit(100)

        department_rows = product_rows.select(
            F.coalesce(F.col("department_id"), F.lit(0)).alias("department_id"),
            F.when(F.col("department") == "", F.lit("unknown"))
            .otherwise(F.col("department"))
            .alias("department"),
            "order_id",
            "product_id",
            "reordered",
        )

        department_summary = department_rows.groupBy(
            "department_id",
            "department",
        ).agg(
            F.count("*").alias("total_order_lines"),
            F.countDistinct("order_id").alias("unique_orders"),
            F.countDistinct("product_id").alias("unique_products"),
            F.sum(F.coalesce(F.col("reordered"), F.lit(0))).alias("reordered_count"),
            F.avg(F.coalesce(F.col("reordered"), F.lit(0))).alias("reorder_rate"),
        ).orderBy(F.desc("total_order_lines"))

        hourly_summary = clean_df.groupBy(
            "order_dow",
            "order_hour_of_day",
        ).agg(
            F.countDistinct("order_id").alias("total_orders"),
            F.count("*").alias("total_order_lines"),
            F.countDistinct("user_id").alias("unique_users"),
        ).withColumn(
            "avg_items_per_order",
            F.col("total_order_lines") / F.col("total_orders"),
        ).orderBy("order_dow", "order_hour_of_day")

        raw_pdf = clean_df.toPandas()
        product_pdf = product_summary.toPandas()
        department_pdf = department_summary.toPandas()
        hourly_pdf = hourly_summary.toPandas()
    finally:
        spark.stop()

    print("Memuat data orders ke ClickHouse...")
    client = _clickhouse_client()
    _create_tables(client)
    _truncate_tables(client)
    _insert_dataframe(client, RAW_TABLE, raw_pdf)
    _insert_dataframe(client, PRODUCT_TABLE, product_pdf)
    _insert_dataframe(client, DEPARTMENT_TABLE, department_pdf)
    _insert_dataframe(client, HOURLY_TABLE, hourly_pdf)

    _cleanup_processed_files(parquet_files)
    print("Pipeline Orders selesai.")


if __name__ == "__main__":
    run_orders_pipeline()
