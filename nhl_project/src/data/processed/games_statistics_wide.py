import pandas as pd
from datetime import datetime

from pyspark.sql import SparkSession
import pyspark.sql.functions as F

from functions import read_table_from_pg, write_df_to_pg


spark = (
    SparkSession.builder.config(
        "spark.jars",
        "/System/Volumes/Data/Users/shiryaevva/Library/Python/3.9/lib/python/site-packages/pyspark/jars/postgresql-42.3.1.jar",
    )
    .master("local[*]")
    .appName("parse_teams")
    .getOrCreate()
)


dm_games = read_table_from_pg(spark, "dwh_common.dm_games")
tl_teams_stat = read_table_from_pg(spark,"dwh_detailed.tl_teams_stat")
tl_teams_games = read_table_from_pg(spark, "dwh_detailed.tl_teams_games")
hub_teams = read_table_from_pg(spark, "dwh_detailed.hub_teams")
sat_teams_core = read_table_from_pg(spark, "dwh_detailed.sat_teams_core")


games = (
    dm_games.filter(F.col("is_active") == "True")
    .join(tl_teams_games, "game_id", "inner")
    .join(
        hub_teams.alias("df1"),
        F.col("df1.team_id") == tl_teams_games["home_team_id"],
        "inner",
    )
    .join(
        hub_teams.alias("df2"),
        F.col("df2.team_id") == tl_teams_games["visiting_team_id"],
        "inner",
    )
    .select(
        "game_source_id",
        dm_games["game_date"],
        "season",
        F.col("df1.team_business_id").alias("home_team_code"),
        F.col("df2.team_business_id").alias("visiting_team_code"),
        "game_type",
        "home_score",
        "visiting_score",
        (F.col("home_score") - F.col("visiting_score")).alias("score_delta"),
    )
)

teams_stat = tl_teams_stat.join(
    sat_teams_core.filter(F.col("is_active") == "True"),
    tl_teams_stat.team_id == sat_teams_core.team_id,
    "inner",
).select(
    tl_teams_stat["*"], sat_teams_core.conference_name, sat_teams_core.division_name
)


tl_teams_stat_home = teams_stat.select(
    [F.col(col_name).alias("h_" + col_name) for col_name in teams_stat.columns]
)

tl_teams_stat_vis = teams_stat.select(
    [F.col(col_name).alias("v_" + col_name) for col_name in teams_stat.columns]
)

joined_df = games.join(
    tl_teams_stat_home,
    (games.home_team_code == tl_teams_stat_home.h_team_business_id)
    & (games.game_date == F.date_sub(tl_teams_stat_home.h_date, -1)),
    "inner",
)

joined_df = joined_df.join(
    tl_teams_stat_vis,
    (joined_df.visiting_team_code == tl_teams_stat_vis.v_team_business_id)
    & (joined_df.game_date == F.date_sub(tl_teams_stat_vis.v_date, -1)),
    "inner",
)

result_df = joined_df.select(
    games["*"],
    *[
        tl_teams_stat_home[col_name]
        for col_name in tl_teams_stat_home.columns
        if col_name
        not in [
            "h_date",
            "h_season_id",
            "h__source_load_datetime",
            "h__source_is_deleted",
            "h__source",
            "h__batch_id",
            "h_team_id",
            "h_team_source_id",
            "h_team_business_id",
        ]
    ],
    *[
        tl_teams_stat_vis[col_name]
        for col_name in tl_teams_stat_vis.columns
        if col_name
        not in [
            "v_date",
            "v_season_id",
            "v__source_load_datetime",
            "v__source_is_deleted",
            "v__source",
            "v__batch_id",
            "v_team_id",
            "v_team_source_id",
            "v_team_business_id",
        ]
    ]
)

current_date = datetime.now().strftime("%Y-%m-%d")

df_games = result_df.toPandas()
df_games = df_games[
    (df_games.game_date < current_date) & (df_games.game_type == 2)
].sort_values(by="game_date")
df_games["home_team_winner"] = df_games["score_delta"].apply(
    lambda x: 0 if x < 0 else 1
)
df_games["game_date"] = pd.to_datetime(df_games["game_date"])
df_games["game_month"] = df_games["game_date"].dt.month

write_df_to_pg(df_games, "games_wide_datamart")