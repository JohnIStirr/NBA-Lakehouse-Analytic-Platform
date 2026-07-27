# Databricks notebook source
from pyspark.sql import functions as F

team_df = spark.table("nba.bronze.team_raw")

display(team_df)

# COMMAND ----------

team_df.printSchema()

# COMMAND ----------

team_df.count()

team_df.dropDuplicates().count()

# COMMAND ----------

from pyspark.sql.functions import col, count, when

display(
    team_df.select([
        count(when(col(c).isNull(), c)).alias(c)
        for c in team_df.columns
    ])
)

# COMMAND ----------

dim_team = (
    team_df
    .select(
        F.col("id").cast("int").alias("team_id"),
        F.trim(F.col("full_name")).alias("team_name"),
        F.upper(F.trim(F.col("abbreviation"))).alias("team_abbreviation"),
        F.trim(F.col("nickname")).alias("nickname"),
        F.trim(F.col("city")).alias("city"),
        F.trim(F.col("state")).alias("state"),
        F.col("year_founded").cast("int").alias("year_founded"),
        F.col("source_system"),
        F.current_timestamp().alias("silver_load_timestamp")
    )
    .dropDuplicates(["team_id"])
)

# COMMAND ----------

print(f"Silver rows: {dim_team.count():,}")

display(dim_team)

# COMMAND ----------

dim_team.write.format("delta").mode("overwrite").saveAsTable("nba.silver.dim_team")

# COMMAND ----------

display(spark.table("nba.silver.dim_team"))