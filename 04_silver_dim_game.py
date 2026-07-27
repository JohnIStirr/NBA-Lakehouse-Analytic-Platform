# Databricks notebook source
tables = [
    "game_raw",
    "game_summary_raw",
    "line_score_raw"
]

for t in tables:
    print("=" * 80)
    print(t)
    df = spark.table(f"nba.bronze.{t}")
    df.printSchema()
    display(df.limit(3))

# COMMAND ----------

from pyspark.sql import functions as F

game = spark.table("nba.bronze.game_raw")
summary = spark.table("nba.bronze.game_summary_raw")
line = spark.table("nba.bronze.line_score_raw")

# COMMAND ----------

home = (
    game.select(
        F.col("game_id"),
        F.col("season_id"),
        F.to_date("game_date").alias("game_date"),

        F.col("team_id_home").cast("int").alias("team_id"),
        F.col("team_id_away").cast("int").alias("opponent_team_id"),

        F.lit("HOME").alias("home_away"),

        F.col("wl_home").alias("result"),

        F.col("pts_home").cast("int").alias("points"),
        F.col("reb_home").cast("int").alias("rebounds"),
        F.col("ast_home").cast("int").alias("assists"),
        F.col("stl_home").cast("int").alias("steals"),
        F.col("blk_home").cast("int").alias("blocks"),
        F.col("tov_home").cast("int").alias("turnovers"),

        F.col("fg_pct_home").alias("fg_pct"),
        F.col("fg3_pct_home").alias("fg3_pct"),
        F.col("ft_pct_home").alias("ft_pct"),

        F.col("plus_minus_home").alias("plus_minus"),

        F.col("season_type")
    )
)

# COMMAND ----------

away = (
    game.select(
        F.col("game_id"),
        F.col("season_id"),
        F.to_date("game_date").alias("game_date"),

        F.col("team_id_away").cast("int").alias("team_id"),
        F.col("team_id_home").cast("int").alias("opponent_team_id"),

        F.lit("AWAY").alias("home_away"),

        F.col("wl_away").alias("result"),

        F.col("pts_away").cast("int").alias("points"),
        F.col("reb_away").cast("int").alias("rebounds"),
        F.col("ast_away").cast("int").alias("assists"),
        F.col("stl_away").cast("int").alias("steals"),
        F.col("blk_away").cast("int").alias("blocks"),
        F.col("tov_away").cast("int").alias("turnovers"),

        F.col("fg_pct_away").alias("fg_pct"),
        F.col("fg3_pct_away").alias("fg3_pct"),
        F.col("ft_pct_away").alias("ft_pct"),

        F.col("plus_minus_away").alias("plus_minus"),

        F.col("season_type")
    )
)

# COMMAND ----------

fact_game = home.unionByName(away)

# COMMAND ----------

summary_clean = (
    summary
    .groupBy("game_id")
    .agg(
        F.first("game_status_text").alias("game_status_text"),
        F.first("season").alias("season"),
        F.concat_ws(
            ", ",
            F.collect_set("natl_tv_broadcaster_abbreviation")
        ).alias("national_tv")
    )
)

# COMMAND ----------

summary_clean.columns

# COMMAND ----------

# Join game supplemental information to the game row

fact_game = (
    fact_game
        .join(
            summary_clean.select(
                "game_id",
                "game_status_text",
                "season",
                "national_tv"
            ),
            "game_id",
            "left"
        )
)

# COMMAND ----------

fact_game = (
    fact_game
    .withColumn(
        "is_win",
        F.when(F.col("result") == "W", True).otherwise(False)
    )
    .withColumn(
        "is_home",
        F.col("home_away") == "HOME"
    )
    .withColumn(
        "point_diff",
        F.col("plus_minus").cast("int")
    )
    .withColumn(
        "silver_load_timestamp",
        F.current_timestamp()
    )
    .withColumn("season",
    F.coalesce(
        F.col("season").cast("int"),
        F.substring("season_id", 2, 4).cast("int")
    )
    )
)

# COMMAND ----------

display(fact_game.filter(F.col("season").isNotNull()).limit(500))

# COMMAND ----------

fact_game.withColumn("season", F.col("season").cast("string")).write.format("delta").mode("overwrite").saveAsTable("nba.silver.fact_game")
display(spark.table("nba.silver.fact_game"))