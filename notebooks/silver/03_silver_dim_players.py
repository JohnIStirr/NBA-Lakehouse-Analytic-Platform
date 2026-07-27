# Databricks notebook source
player_df = spark.table("nba.bronze.player_raw")

player_df.printSchema()

display(player_df.limit(5))

# COMMAND ----------

from pyspark.sql import functions as F
player_df = spark.table("nba.bronze.player_raw")

print(f"Bronze rows: {player_df.count():,}")

display(player_df)

# COMMAND ----------

# Check duplicate player IDs
display(
    player_df.groupBy("id")
             .count()
             .filter(F.col("count") > 1)
)

# Check null values
display(
    player_df.select([
        F.count(F.when(F.col(c).isNull(), c)).alias(c)
        for c in player_df.columns
    ])
)

# COMMAND ----------

dim_player = (
    player_df
    .select(
        F.col("id").cast("int").alias("player_id"),
        F.trim(F.col("full_name")).alias("player_name"),
        F.trim(F.col("first_name")).alias("first_name"),
        F.trim(F.col("last_name")).alias("last_name"),
        F.col("is_active").cast("boolean").alias("is_active"),
        F.col("source_system"),
        F.current_timestamp().alias("silver_load_timestamp")
    )
    .dropDuplicates(["player_id"])
)

# COMMAND ----------

print(f"Silver rows: {dim_player.count():,}")

display(dim_player)

# COMMAND ----------

dim_player.write.format("delta").mode("overwrite").saveAsTable("nba.silver.dim_player")
display(spark.table("nba.silver.dim_player"))
