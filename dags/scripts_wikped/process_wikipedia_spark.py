from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from clickhouse_driver import Client
import os
import glob

def run_spark_analytics():
    spark = SparkSession.builder \
        .appName("Wikipedia_Streaming_Analytics") \
        .config("spark.driver.memory", "1g") \
        .getOrCreate()

    print("Membaca seluruh aliran data dari Data Lake...")
    # Spark dengan mudah membaca SEMUA file parquet di folder ini sekaligus
    df_raw = spark.read.parquet("file:///opt/airflow/data_lake/wikipedia/")

    print("Kalkulasi Top K (Heavy Hitters) & Deteksi Bot...")
    trending_df = df_raw.groupBy("title") \
        .agg(
            F.count("edit_id").alias("total_edits"),
            F.sum("size_diff").alias("total_bytes_changed"),
            F.sum(F.when(F.col("is_bot") == True, 1).otherwise(0)).alias("bot_edits")
        ) \
        .orderBy(F.desc("total_edits")) \
        .limit(30) # Ambil 30 Artikel Paling "Panas"

    final_results = trending_df.toPandas()
    spark.stop()

    print("Memuat ke ClickHouse Warehouse...")
    
    # --- PERBAIKAN MULAI DI SINI ---
    # Tambahkan parameter user dan password sesuai dengan pengaturan ClickHouse Anda
    # Jika Anda menggunakan default bawaan docker, biasanya user='default' dan password='' (kosong)
    # ATAU jika Anda mengatur password di docker-compose.yml, masukkan di sini.
    client = Client(
        host='clickhouse-server',
        user='admin',          # Ganti jika nama user Anda berbeda
        password='rahasia' # GANTI DENGAN PASSWORD CLICKHOUSE ANDA
    )
    # --- PERBAIKAN SELESAI ---

    client.execute('CREATE DATABASE IF NOT EXISTS analytics')
    client.execute('''
        CREATE TABLE IF NOT EXISTS analytics.wikipedia_trending (
            title String,
            total_edits Int32,
            total_bytes_changed Int32,
            bot_edits Int32
        ) ENGINE = MergeTree()
        ORDER BY total_edits
    ''')
    
    # Mode Overwrite (Truncate & Insert) agar dasbor Metabase selalu fresh
    client.execute('TRUNCATE TABLE analytics.wikipedia_trending')
    data_tuples = [tuple(x) for x in final_results.to_numpy()]
    if data_tuples:
        client.execute('INSERT INTO analytics.wikipedia_trending VALUES', data_tuples)
    
    # Menghapus file .parquet yang sudah diproses agar tidak menumpuk
    print("Membersihkan file Parquet lama dari Data Lake...")
    files = glob.glob('/opt/airflow/data_lake/wikipedia/*.parquet')
    for f in files:
        try:
            os.remove(f)
        except OSError as e:
            print(f"Error: {f} : {e.strerror}")
    
    print("✅ Pipeline Selesai!")

if __name__ == "__main__":
    run_spark_analytics()