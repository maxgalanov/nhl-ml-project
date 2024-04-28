import pandas as pd
from datetime import timedelta, datetime

from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from pyspark.sql import SparkSession
from pyspark.sql.window import Window
import pyspark.sql.functions as F

from nhl_project.src.data.functions import read_table_from_pg, write_table_to_pg


DEFAULT_ARGS = {
    "owner": "Galanov, Shiryeava",
    "email": "maxglnv@gmail.com",
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(seconds=15),
}

dag = DAG(
    dag_id="create_datamarts",
    schedule_interval="30 13 * * *",
    start_date=days_ago(2),
    catchup=False,
    tags=["hse_nhl_ml_project"],
    default_args=DEFAULT_ARGS,
    description="Create datamart with games statistics",
)


def create_teams_games_datamarts():
    spark = (
        SparkSession.builder.config(
            "spark.jars",
            "~/airflow_venv/lib/python3.10/site-packages/pyspark/jars/postgresql-42.3.1.jar",
        )
        .master("local[*]")
        .appName("parse_teams")
        .getOrCreate()
    )

    dm_games = read_table_from_pg(spark, "dwh_common.dm_games")
    tl_teams_stat = read_table_from_pg(spark,"dwh_detailed.tl_teams_stat")
    tl_teams_games = read_table_from_pg(spark, "dwh_detailed.tl_teams_games")
    dm_teams = read_table_from_pg(spark, "dwh_common.dm_teams").filter(F.col("is_active") == "True")
    sat_teams_core = read_table_from_pg(spark, "dwh_detailed.sat_teams_core")

    games = (
        dm_games.filter(F.col("is_active") == "True")
        .join(tl_teams_games, "game_id", "inner")
        .join(
            dm_teams.alias("df1"),
            F.col("df1.team_id") == tl_teams_games["home_team_id"],
            "inner",
        )
        .join(
            dm_teams.alias("df2"),
            F.col("df2.team_id") == tl_teams_games["visiting_team_id"],
            "inner",
        )
        .select(
            "game_source_id",
            dm_games["game_date"],
            dm_games["eastern_start_time"],
            "season",
            F.col("df1.team_business_id").alias("home_team_code"),
            F.col("df1.team_name").alias("home_team_name"),
            F.col("df2.team_business_id").alias("visiting_team_code"),
            F.col("df2.team_name").alias("visiting_team_name"),
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
        tl_teams_stat["*"],
        sat_teams_core.team_name,
        sat_teams_core.conference_name,
        sat_teams_core.division_name
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
        "left",
    )

    joined_df = joined_df.join(
        tl_teams_stat_vis,
        (joined_df.visiting_team_code == tl_teams_stat_vis.v_team_business_id)
        & (joined_df.game_date == F.date_sub(tl_teams_stat_vis.v_date, -1)),
        "left",
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
    
    df_games = (
        result_df.filter((F.col("game_type") == 2) | (F.col("game_type") == 3))
        .orderBy("game_date")
        .withColumn("home_team_winner", F.when(F.col("score_delta") < 0, 0).otherwise(1))
        .withColumn("game_date", F.to_date(F.col("game_date")))
        .withColumn("game_month", F.month(F.col("game_date")))
    )

    write_table_to_pg(df_games, spark, "overwrite", "public.games_wide_datamart")
    write_table_to_pg(teams_stat, spark, "overwrite", "public.teams_stat_wide_datamart")


def create_teams_stat_agg():
    spark = (
        SparkSession.builder.config(
            "spark.jars",
            "~/airflow_venv/lib/python3.10/site-packages/pyspark/jars/postgresql-42.3.1.jar",
        )
        .master("local[*]")
        .appName("parse_teams")
        .getOrCreate()
    )

    teams_stat = read_table_from_pg(spark, "public.teams_stat")

    window_spec = Window.partitionBy("team_name", "season_id").orderBy(F.desc("date"))

    teams_stat_agg = (
        teams_stat.withColumn("rank", F.row_number().over(window_spec))
        .filter(F.col("rank") == 1)
        .drop("rank")
        .groupBy(
            "team_name", "team_business_id", "season_id", "conference_name", "division_name"
        )
        .agg(
            F.first("date").alias("date"),
            F.first("conference_home_sequence").alias("conference_home_sequence"),
            F.first("conference_l10_sequence").alias("conference_l10_sequence"),
            F.first("conference_road_sequence").alias("conference_road_sequence"),
            F.first("conference_sequence").alias("conference_sequence"),
            F.first("division_home_sequence").alias("division_home_sequence"),
            F.first("division_l10_sequence").alias("division_l10_sequence"),
            F.first("division_road_sequence").alias("division_road_sequence"),
            F.first("division_sequence").alias("division_sequence"),
            F.first("games_played").alias("games_played"),
            F.first("goal_differential").alias("goal_differential"),
            F.first("goal_differential_pctg").alias("goal_differential_pctg"),
            F.first("goal_against").alias("goal_against"),
            F.first("goal_for").alias("goal_for"),
            F.first("goals_for_pctg").alias("goals_for_pctg"),
            F.first("home_games_played").alias("home_games_played"),
            F.first("home_goal_differential").alias("home_goal_differential"),
            F.first("home_goals_against").alias("home_goals_against"),
            F.first("home_goals_for").alias("home_goals_for"),
            F.first("home_losses").alias("home_losses"),
            F.first("home_ot_losses").alias("home_ot_losses"),
            F.first("home_points").alias("home_points"),
            F.first("home_regulation_plus_ot_wins").alias("home_regulation_plus_ot_wins"),
            F.first("home_regulation_wins").alias("home_regulation_wins"),
            F.first("home_ties").alias("home_ties"),
            F.first("home_wins").alias("home_wins"),
            F.first("l10_games_played").alias("l10_games_played"),
            F.first("l10_goal_differential").alias("l10_goal_differential"),
            F.first("l10_goals_against").alias("l10_goals_against"),
            F.first("l10_goals_for").alias("l10_goals_for"),
            F.first("l10_losses").alias("l10_losses"),
            F.first("l10_ot_losses").alias("l10_ot_losses"),
            F.first("l10_points").alias("l10_points"),
            F.first("l10_regulation_plus_ot_wins").alias("l10_regulation_plus_ot_wins"),
            F.first("l10_regulation_wins").alias("l10_regulation_wins"),
            F.first("l10_ties").alias("l10_ties"),
            F.first("l10_wins").alias("l10_wins"),
            F.first("league_home_sequence").alias("league_home_sequence"),
            F.first("league_l10_sequence").alias("league_l10_sequence"),
            F.first("league_road_sequence").alias("league_road_sequence"),
            F.first("league_sequence").alias("league_sequence"),
            F.first("losses").alias("losses"),
            F.first("ot_losses").alias("ot_losses"),
            F.first("point_pctg").alias("point_pctg"),
            F.first("points").alias("points"),
            F.first("regulation_plus_ot_win_pctg").alias("regulation_plus_ot_win_pctg"),
            F.first("regulation_plus_ot_wins").alias("regulation_plus_ot_wins"),
            F.first("regulation_win_pctg").alias("regulation_win_pctg"),
            F.first("regulation_wins").alias("regulation_wins"),
            F.first("road_games_played").alias("road_games_played"),
            F.first("road_goal_differential").alias("road_goal_differential"),
            F.first("road_goals_against").alias("road_goals_against"),
            F.first("road_goals_for").alias("road_goals_for"),
            F.first("road_losses").alias("road_losses"),
            F.first("road_ot_losses").alias("road_ot_losses"),
            F.first("road_points").alias("road_points"),
            F.first("road_regulation_plus_ot_wins").alias("road_regulation_plus_ot_wins"),
            F.first("road_regulation_wins").alias("road_regulation_wins"),
            F.first("road_ties").alias("road_ties"),
            F.first("road_wins").alias("road_wins"),
            F.first("shootout_losses").alias("shootout_losses"),
            F.first("shootout_wins").alias("shootout_wins"),
            F.first("streak_code").alias("streak_code"),
            F.first("streak_count").alias("streak_count"),
            F.first("ties").alias("ties"),
            F.first("waivers_sequence").alias("waivers_sequence"),
            F.first("wildcard_sequence").alias("wildcard_sequence"),
            F.first("win_pctg").alias("win_pctg"),
            F.first("wins").alias("wins"),
        )
        .orderBy("team_name", "season_id")
    )

    write_table_to_pg(teams_stat_agg, spark, "overwrite", "public.teams_stat_agg")


def create_teams_games_all():
    spark = (
        SparkSession.builder.config(
            "spark.jars",
            "~/airflow_venv/lib/python3.10/site-packages/pyspark/jars/postgresql-42.3.1.jar",
        )
        .master("local[*]")
        .appName("parse_teams")
        .getOrCreate()
    )

    df_games = read_table_from_pg(spark, "public.games_wide_datamart")

    games_home_team = df_games.filter(F.col("season") >= F.lit("20122013")).select(
        "game_source_id",
        "game_date",
        "eastern_start_time",
        "season",
        F.col("home_team_name").alias("team_name"),
        F.col("home_team_code").alias("team_code"),
        F.col("visiting_team_name").alias("opposing_team_name"),
        F.col("visiting_team_code").alias("opposing_team_code"),
        "game_type",
        F.lit("H").alias("home_road_flag"),
        F.col("home_score").alias("score"),
        F.col("visiting_score").alias("opposing_score"),
        F.col("h_win_pctg").alias("win_pctg"),
    )

    games_opp_team = df_games.filter(F.col("season") >= F.lit("20122013")).select(
        "game_source_id",
        "game_date",
        "eastern_start_time",
        "season",
        F.col("visiting_team_name").alias("team_name"),
        F.col("visiting_team_code").alias("team_code"),
        F.col("home_team_name").alias("opposing_team_name"),
        F.col("home_team_code").alias("opposing_team_code"),
        "game_type",
        F.lit("R").alias("home_road_flag"),
        F.col("visiting_score").alias("score"),
        F.col("home_score").alias("opposing_score"),
        F.col("v_win_pctg").alias("win_pctg"),
    )

    teams_games_all = games_home_team.unionByName(games_opp_team).orderBy("game_source_id")
    write_table_to_pg(teams_games_all, spark, "overwrite", "public.teams_games_all")


def create_players_stat_datamart():
    spark = (
        SparkSession.builder.config(
            "spark.jars",
            "~/airflow_venv/lib/python3.10/site-packages/pyspark/jars/postgresql-42.3.1.jar",
        )
        .master("local[*]")
        .appName("parse_teams")
        .getOrCreate()
    )

    dm_players = read_table_from_pg(spark, "dwh_common.dm_players")
    tl_players_games_stat = read_table_from_pg(spark,"dwh_detailed.tl_players_games_stat")
    dm_teams = read_table_from_pg(spark, "dwh_common.dm_teams")
    dm_games = read_table_from_pg(spark, "dwh_common.dm_games")

    dm_players = (
        dm_players.filter(F.col("is_active") == "True")
        .withColumn(
            "player_full_name", F.concat_ws(" ", F.col("first_name"), F.col("last_name"))
        )
        .drop("effective_from", "effective_to", "is_active", "first_name", "last_name")
    )

    dm_teams = dm_teams.filter(F.col("is_active") == "True").select(
        F.col("team_id"), F.col("team_name"), F.col("team_logo")
    )

    dm_games = dm_games.filter(F.col("is_active") == "True").select(
        F.col("game_id"), F.col("season")
    )

    tl_players_games_stat = tl_players_games_stat.drop(
        "_source_load_datetime",
        "_source_is_deleted",
        "_source",
        "_batch_id",
        "player_business_id",
        "game_business_id",
        "common_name",
        "opponent_common_name",
    )

    def convert_to_seconds(time_str):
        try:
            minutes, seconds = map(int, time_str.split(":"))
            return minutes * 60 + seconds
        except ValueError:
            return 0


    convert_to_seconds_udf = spark.udf.register("convert_to_seconds", convert_to_seconds)

    df_games_info = (
        tl_players_games_stat.join(
            dm_players.filter(F.col("is_active") == "True"), "player_id", "left"
        )
        .drop("player_id", dm_players["player_source_id"])
        .join(dm_teams, "team_id", "left")
        .drop("team_id")
        .withColumnRenamed("team_name", "team_full_name")
        .join(
            dm_teams.drop("team_logo"),
            tl_players_games_stat.opponent_team_id == dm_teams.team_id,
            "left",
        )
        .drop("team_id", "opponent_team_id")
        .withColumnRenamed("team_name", "opponent_team_full_name")
        .join(dm_games, "game_id", "left")
        .drop("game_id")
        .withColumn("toi", convert_to_seconds_udf("toi"))
        .orderBy("game_date", "game_source_id", "player_source_id")
    )

    df_games_info = df_games_info.filter(F.col("game_date") >= F.lit("2018-10-03")).orderBy(
        "game_date"
    )

    write_table_to_pg(df_games_info, spark, "overwrite", "public.players_stat_datamart")


def create_skaters_agg():
    spark = (
        SparkSession.builder.config(
            "spark.jars",
            "~/airflow_venv/lib/python3.10/site-packages/pyspark/jars/postgresql-42.3.1.jar",
        )
        .master("local[*]")
        .appName("parse_teams")
        .getOrCreate()
    )

    df_games_info = read_table_from_pg(spark, "public.players_stat_datamart")

    df_skaters_agg = (
        df_games_info.filter(F.col("position_code") != "G")
        .groupBy(
            "player_source_id",
            "season",
            "headshot",
            "player_full_name",
            "team_business_id",
            "team_full_name",
            "team_logo",
            "home_road_flag",
            "birth_date",
            "birth_city",
            "birth_country",
            "birth_state_province",
            "position_code",
            "shoots_catches",
        )
        .agg(
            F.count("game_source_id").alias("game_cnt"),
            F.sum("goals").alias("goals"),
            F.sum("points").alias("points"),
            F.sum("plus_minus").alias("plus_minus"),
            F.sum("power_play_goals").alias("power_play_goals"),
            F.sum("power_play_points").alias("power_play_points"),
            F.sum("game_winning_goals").alias("game_winning_goals"),
            F.sum("ot_goals").alias("ot_goals"),
            F.sum("shots").alias("shots"),
            F.sum("shifts").alias("shifts"),
            F.sum("shorthanded_goals").alias("shorthanded_goals"),
            F.sum("toi").alias("toi"),
            F.sum("pim").alias("pim"),
        )
        .orderBy("player_source_id", "season")
    )
    
    write_table_to_pg(df_skaters_agg, spark, "overwrite", "public.skaters_agg")


def create_goalies_agg():
    spark = (
        SparkSession.builder.config(
            "spark.jars",
            "~/airflow_venv/lib/python3.10/site-packages/pyspark/jars/postgresql-42.3.1.jar",
        )
        .master("local[*]")
        .appName("parse_teams")
        .getOrCreate()
    )

    df_games_info = read_table_from_pg(spark, "public.players_stat_datamart")

    df_goalies_agg = (
        df_games_info.filter(F.col("position_code") == "G")
        .groupBy(
            "player_source_id",
            "season",
            "headshot",
            "player_full_name",
            "team_business_id",
            "team_full_name",
            "team_logo",
            "home_road_flag",
            "birth_date",
            "birth_city",
            "birth_country",
            "birth_state_province",
            "position_code",
            "shoots_catches",
        )
        .agg(
            F.count("game_source_id").alias("game_cnt"),
            F.sum("goals").alias("goals"),
            F.sum("games_started").alias("games_started"),
            F.sum("shots_against").alias("shots_against"),
            F.sum("goals_against").alias("goals_against"),
            F.sum("shutouts").alias("shutouts"),
            F.sum("toi").alias("toi"),
            F.sum("pim").alias("pim"),
        )
        .orderBy("player_source_id", "season")
    )
    
    write_table_to_pg(df_goalies_agg, spark, "overwrite", "public.goalies_agg")


task_create_teams_games_datamarts = PythonOperator(
    task_id="create_teams_games_datamarts",
    python_callable=create_teams_games_datamarts,
    dag=dag,
)

task_create_teams_stat_agg = PythonOperator(
    task_id="create_teams_stat_agg",
    python_callable=create_teams_stat_agg,
    dag=dag,
)

task_create_teams_games_all = PythonOperator(
    task_id="create_teams_games_all",
    python_callable=create_teams_games_all,
    dag=dag,
)

task_create_players_stat_datamart = PythonOperator(
    task_id="create_players_stat_datamart",
    python_callable=create_players_stat_datamart,
    dag=dag,
)

task_create_skaters_agg = PythonOperator(
    task_id="create_skaters_agg",
    python_callable=create_skaters_agg,
    dag=dag,
)

task_create_goalies_agg = PythonOperator(
    task_id="create_goalies_agg",
    python_callable=create_goalies_agg,
    dag=dag,
)

task_create_teams_games_datamarts >> [task_create_teams_stat_agg, task_create_teams_games_all]
task_create_players_stat_datamart >> [task_create_skaters_agg, task_create_goalies_agg]