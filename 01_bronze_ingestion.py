# Databricks notebook source
# Databricks notebook source

from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime

# COMMAND ----------

CATALOG = "nba"

BRONZE = "bronze"

VOLUME = "/Volumes/nba/bronze/"

# COMMAND ----------

TABLES = [
    "team",
    "player",
    "game",
    "game_summary",
    "line_score",
    "officials",
    "other_stats",
    "inactive_players"
]

# COMMAND ----------

def ingest_table(table_name):

    source = f"{VOLUME}/{table_name}/{table_name}.parquet"

    target = f"{CATALOG}.{BRONZE}.{table_name}_raw"

    print(f"Ingesting {table_name}")

    df = (
        spark.read
             .format("parquet")
             .load(source)
             .withColumn(
                 "ingestion_timestamp",
                 F.current_timestamp()
             )
             .withColumn(
                 "source_system",
                 F.lit("SQLite")
             )
    )

    (
        df.write
          .format("delta")
          .mode("overwrite")
          .saveAsTable(target)
    )

    print(
        f"{table_name}: {df.count():,} rows"
    )

# COMMAND ----------

for table in TABLES:
    ingest_table(table)

# COMMAND ----------

for table in TABLES:
    print(table)

    display(
        spark.table(
            f"{CATALOG}.{BRONZE}.{table}_raw"
        ).limit(5)
    )

# COMMAND ----------

display(spark.table("nba.bronze.teams_raw"))