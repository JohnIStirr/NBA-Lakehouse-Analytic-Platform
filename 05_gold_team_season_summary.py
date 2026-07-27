# Databricks notebook source
# MAGIC %md
# MAGIC One row per team per season

# COMMAND ----------

# MAGIC %md
# MAGIC # Read Silver Tables

# COMMAND ----------

from pyspark.sql import functions as F

fact_game = spark.table("nba.silver.fact_game")
dim_team = spark.table("nba.silver.dim_team")

# COMMAND ----------

# MAGIC %md
# MAGIC # Aggregate team statistics

# COMMAND ----------

team_summary = (
    fact_game
    .groupBy("season", "team_id")
    .agg(
        F.count("*").alias("games_played"),

        F.sum(
            F.when(F.col("is_win"), 1).otherwise(0)
        ).alias("wins"),

        F.sum(
            F.when(~F.col("is_win"), 1).otherwise(0)
        ).alias("losses"),

        F.avg("points").alias("avg_points"),
        F.avg("rebounds").alias("avg_rebounds"),
        F.avg("assists").alias("avg_assists"),

        F.avg("fg_pct").alias("avg_fg_pct"),
        F.avg("fg3_pct").alias("avg_fg3_pct"),
        F.avg("ft_pct").alias("avg_ft_pct"),

        F.avg("point_diff").alias("avg_point_diff")
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Calculate win percentage

# COMMAND ----------

team_summary = (
    team_summary
    .withColumn(
        "win_pct",
        F.round(
            F.col("wins") / F.col("games_played"),
            3
        )
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Join Team Names

# COMMAND ----------

team_summary = (
    team_summary
    .join(
        dim_team.select(
            "team_id",
            "team_name",
            "team_abbreviation",
            "city"
        ),
        "team_id",
        "left"
    )
)

# COMMAND ----------

team_summary.show()