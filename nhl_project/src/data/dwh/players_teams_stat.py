import pandas as pd
import requests
import os
from datetime import timedelta, datetime

from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, concat_ws, when, isnan, hash, sha1, coalesce, lag, lead, expr, last
from pyspark.sql.window import Window
import pyspark.sql.functions as F


DEFAULT_ARGS = {
    "owner": "Galanov, Shiryeava",
    "email": "maxglnv@gmail.com",
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(seconds=15),
}

dag = DAG(
    dag_id="nhl_players",
    schedule_interval="45 5 1 10 *",
    start_date=days_ago(2),
    catchup=False,
    tags=["hse_nhl_ml_project"],
    default_args=DEFAULT_ARGS,
    description="ETL process for getting list of NHL teams daily statistics",
)

SOURCE_PATH = "/nhl-ml-project/data/dwh/source/"
STAGING_PATH = "/nhl-ml-project/data/dwh/vault/staging/"
OPERATIONAL_PATH = "/nhl-ml-project/data/dwh/vault/operational/"
DETAILED_PATH = "/nhl-ml-project/data/dwh/vault/detailed/"
COMMON_PATH = "/nhl-ml-project/data/dwh/vault/common/"


def get_information(endpoint, base_url="https://api-web.nhle.com"):
    base_url = f"{base_url}"
    endpoint = f"{endpoint}"
    full_url = f"{base_url}{endpoint}"

    response = requests.get(full_url)

    if response.status_code == 200:
        player_data = response.json()
        return player_data
    else:
        print(f"Error: Unable to fetch data. Status code: {response.status_code}")


def get_teams_stat_to_source(**kwargs):
    current_date = kwargs["ds"]

    spark = SparkSession.builder.master("local[*]").appName("parse_teams").getOrCreate()

    current_dt = datetime.strptime(current_date, "%Y-%m-%d").date()
    dates_last_week = []

    for i in range(5):
        date = current_dt - timedelta(days=i)
        dates_last_week.append(date.strftime('%Y-%m-%d'))

    teams_stat = pd.DataFrame()

    for date in dates_last_week:
        try:
            data_team_stat = get_information(f"/v1/standings/{date}")
            df_teams_stat = pd.DataFrame(data_team_stat['standings'])

            teams_stat = pd.concat([teams_stat, df_teams_stat], ignore_index=True)
        except:
            continue

    teams_stat["teamName"] = teams_stat["teamName"].apply(
        lambda x: x.get("default", "") if type(x) == dict else ""
    )
    teams_stat["teamCommonName"] = teams_stat["teamCommonName"].apply(
        lambda x: x.get("default", "") if type(x) == dict else ""
    )
    teams_stat["teamAbbrev"] = teams_stat["teamAbbrev"].apply(
        lambda x: x.get("default", "") if type(x) == dict else ""
    )
    teams_stat["placeName"] = teams_stat["placeName"].apply(
        lambda x: x.get("default", "") if type(x) == dict else ""
    )

    dt = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    df_teams_stat = spark.createDataFrame(teams_stat)\
        .withColumn("_source_load_datetime", lit(dt))\
        .withColumn("_source", lit("API_NHL"))

    df_teams_stat.repartition(1).write.mode("overwrite").parquet(SOURCE_PATH + f"teams_stat/{current_date}")
    

def get_penultimate_file_name(directory):
    files = os.listdir(directory)
    files = [f for f in files if os.path.isdir(os.path.join(directory, f))]
    
    if len(files) < 2:
        return None
    
    files.sort(key=lambda x: os.path.getmtime(os.path.join(directory, x)), reverse=True)
    penultimate_file = files[1]
    
    return penultimate_file


def get_teama_stat_to_staging(**kwargs):
    current_date = kwargs["ds"]

    spark = SparkSession.builder.master("local[*]").appName("parse_teams").getOrCreate()

    df_new = spark.read.parquet(SOURCE_PATH + f"teams_stat/{current_date}")
    df_new = df_new.withColumn("_source_is_deleted", lit("False"))

    df_final = df_new.withColumn("_batch_id", lit(current_date))

    df_final.repartition(1).write.mode("overwrite").parquet(STAGING_PATH + f"teams_stat/{current_date}")


def get_teama_stat_to_operational(**kwargs):
    current_date = kwargs["ds"]

    spark = SparkSession.builder.master("local[*]").appName("parse_teams").getOrCreate()

    df = spark.read.parquet(STAGING_PATH + f"teams_stat/{current_date}")
    hub_teams = spark.read.parquet(DETAILED_PATH + f"hub_teams")

    df = df.select(
                    col("date").alias("date"),
                    col("seasonId").alias("season_id"),
                    col("teamName").alias("team_name"),
                    col("teamCommonName").alias("team_common_name"),
                    col("teamAbbrev").alias("team_business_id"),
                    col("conferenceAbbrev").alias("conference_abbrev"),
                    col("conferenceHomeSequence").cast("int").alias("conference_home_sequence"),
                    col("conferenceL10Sequence").cast("int").alias("conference_l10_sequence"),
                    col("conferenceName").alias("conference_name"),
                    col("conferenceRoadSequence").cast("int").alias("conference_road_sequence"),
                    col("conferenceSequence").cast("int").alias("conference_sequence"),
                    col("divisionAbbrev").alias("division_abbrev"),
                    col("divisionHomeSequence").cast("int").alias("division_home_sequence"),
                    col("divisionL10Sequence").cast("int").alias("division_l10_sequence"),
                    col("divisionName").alias("division_name"),
                    col("divisionRoadSequence").cast("int").alias("division_road_sequence"),
                    col("divisionSequence").cast("int").alias("division_sequence"),
                    col("gameTypeId").alias("game_type_id"),
                    col("gamesPlayed").cast("int").alias("games_played"),
                    col("goalDifferential").cast("int").alias("goal_differential"),
                    col("goalDifferentialPctg").cast("double").alias("goal_differential_pctg"),
                    col("goalAgainst").cast("int").alias("goal_against"),
                    col("goalFor").cast("int").alias("goal_for"),
                    col("goalsForPctg").cast("double").alias("goals_for_pctg"),
                    col("homeGamesPlayed").cast("int").alias("home_games_played"),
                    col("homeGoalDifferential").cast("int").alias("home_goal_differential"),
                    col("homeGoalsAgainst").cast("int").alias("home_goals_against"),
                    col("homeGoalsFor").cast("int").alias("home_goals_for"),
                    col("homeLosses").cast("int").alias("home_losses"),
                    col("homeOtLosses").cast("int").alias("home_ot_losses"),
                    col("homePoints").cast("int").alias("home_points"),
                    col("homeRegulationPlusOtWins").cast("int").alias("home_regulation_plus_ot_wins"),
                    col("homeRegulationWins").cast("int").alias("home_regulation_wins"),
                    col("homeTies").cast("int").alias("home_ties"),
                    col("homeWins").cast("int").alias("home_wins"),
                    col("l10GamesPlayed").cast("int").alias("l10_games_played"),
                    col("l10GoalDifferential").cast("int").alias("l10_goal_differential"),
                    col("l10GoalsAgainst").cast("int").alias("l10_goals_against"),
                    col("l10GoalsFor").cast("int").alias("l10_goals_for"),
                    col("l10Losses").cast("int").alias("l10_losses"),
                    col("l10OtLosses").cast("int").alias("l10_ot_losses"),
                    col("l10Points").cast("int").alias("l10_points"),
                    col("l10RegulationPlusOtWins").cast("int").alias("l10_regulation_plus_ot_wins"),
                    col("l10RegulationWins").cast("int").alias("l10_regulation_wins"),
                    col("l10Ties").cast("int").alias("l10_ties"),
                    col("l10Wins").cast("int").alias("l10_wins"),
                    col("leagueHomeSequence").cast("int").alias("league_home_sequence"),
                    col("leagueL10Sequence").cast("int").alias("league_l10_sequence"),
                    col("leagueRoadSequence").cast("int").alias("league_road_sequence"),
                    col("leagueSequence").cast("int").alias("league_sequence"),
                    col("losses").cast("int").alias("losses"),
                    col("otLosses").cast("int").alias("ot_losses"),
                    col("placeName").alias("place_name"),
                    col("pointPctg").cast("double").alias("point_pctg"),
                    col("points").cast("int").alias("points"),
                    col("regulationPlusOtWinPctg").cast("double").alias("regulation_plus_ot_win_pctg"),
                    col("regulationPlusOtWins").cast("int").alias("regulation_plus_ot_wins"),
                    col("regulationWinPctg").cast("double").alias("regulation_win_pctg"),
                    col("regulationWins").cast("int").alias("regulation_wins"),
                    col("roadGamesPlayed").cast("int").alias("road_games_played"),
                    col("roadGoalDifferential").cast("int").alias("road_goal_differential"),
                    col("roadGoalsAgainst").cast("int").alias("road_goals_against"),
                    col("roadGoalsFor").cast("int").alias("road_goals_for"),
                    col("roadLosses").cast("int").alias("road_losses"),
                    col("roadOtLosses").cast("int").alias("road_ot_losses"),
                    col("roadPoints").cast("int").alias("road_points"),
                    col("roadRegulationPlusOtWins").cast("int").alias("road_regulation_plus_ot_wins"),
                    col("roadRegulationWins").cast("int").alias("road_regulation_wins"),
                    col("roadTies").cast("int").alias("road_ties"),
                    col("roadWins").cast("int").alias("road_wins"),
                    col("shootoutLosses").cast("int").alias("shootout_losses"),
                    col("shootoutWins").cast("int").alias("shootout_wins"),
                    col("streakCode").alias("streak_code"),
                    col("streakCount").cast("int").alias("streak_count"),
                    col("teamLogo").alias("team_logo"),
                    col("ties").cast("int").alias("ties"),
                    col("waiversSequence").cast("int").alias("waivers_sequence"),
                    col("wildcardSequence").cast("int").alias("wildcard_sequence"),
                    col("winPctg").cast("double").alias("win_pctg"),
                    col("wins").cast("int").alias("wins"),

                    col("_source_load_datetime"),
                    col("_source_is_deleted"),
                    col("_source"),
                    col("_batch_id"))

    df = df.join(hub_teams, "team_business_id", "left") \
                .select(df["*"], hub_teams["team_id"], hub_teams["team_source_id"])
    
    df.repartition(1).write.mode("append").parquet(OPERATIONAL_PATH + f"teams_stat")


def sat_teams_core(**kwargs):
    current_date = kwargs["ds"]

    spark = SparkSession.builder.master("local[*]").appName("teams_to_dwh").getOrCreate()

    increment = spark.read.parquet(OPERATIONAL_PATH + f"teams_stat")\
        .filter(col("_batch_id") == lit(current_date))\
        .select(col("team_id"),
                col("team_name"),
                col("team_common_name"),
                col("conference_abbrev"),
                col("conference_name"),
                col("division_abbrev"),
                col("division_name"),
                col("place_name"),
                col("team_logo"),
                
                col("_source_is_deleted"),
                col("_source_load_datetime").alias("effective_from"),
                col("_source"))\
        .withColumn("_data_hash", sha1(concat_ws("_", col("team_name"),
                                                    col("team_common_name"),
                                                    col("conference_abbrev"),
                                                    col("conference_name"),
                                                    col("division_abbrev"),
                                                    col("division_name"),
                                                    col("place_name"),
                                                    col("team_logo"),
                                                    col("_source_is_deleted"))))
    
    try:    
        sat_teams_core = spark.read.parquet(DETAILED_PATH + f"sat_teams_core")
        state = sat_teams_core.filter(col("is_active") == "False")

        active = sat_teams_core.filter(col("is_active") == "True")\
            .select(col("team_id"),
                col("team_name"),
                col("team_common_name"),
                col("conference_abbrev"),
                col("conference_name"),
                col("division_abbrev"),
                col("division_name"),
                col("place_name"),
                col("team_logo"),
                col("_source_is_deleted"),
                col("effective_from"),
                col("_source"),
                col("_data_hash")) \
            .union(increment)
    except:
        active = increment


    scd_window = Window.partitionBy("team_id").orderBy("effective_from")
    is_change = when(lag("_data_hash").over(scd_window) != col("_data_hash"), "True").otherwise("False")

    row_changes = active.select(
        col("team_id"),
        col("team_name"),
        col("team_common_name"),
        col("conference_abbrev"),
        col("conference_name"),
        col("division_abbrev"),
        col("division_name"),
        col("place_name"),
        col("team_logo"),

        col("effective_from"),
        coalesce(is_change.cast("string"), lit("True")).alias("is_change"),
        col("_data_hash"),
        col("_source_is_deleted"),
        col("_source")
    )


    scd_window = Window.partitionBy("team_id").orderBy(col("effective_from").asc(), col("is_change").asc())

    next_effective_from = lead("effective_from").over(scd_window)
    version_count = F.sum(when(col("is_change") == "True", 1).otherwise(0)).over(scd_window.rangeBetween(Window.unboundedPreceding, 0))

    row_versions = row_changes.select(
                    col("team_id"),
                    col("team_name"),
                    col("team_common_name"),
                    col("conference_abbrev"),
                    col("conference_name"),
                    col("division_abbrev"),
                    col("division_name"),
                    col("place_name"),
                    col("team_logo"),

                    col("effective_from"),
                    next_effective_from.cast("string").alias("effective_to"),
                    version_count.alias("_version"),
                    "_data_hash",
                    "_source_is_deleted",
                    "_source"
                )\
        .withColumn("effective_to", coalesce(expr("effective_to - interval 1 second"), lit("2040-01-01 00:00:00")))


    scd2 = row_versions.groupBy(
            col("team_id"),
            col("team_name"),
            col("team_common_name"),
            col("conference_abbrev"),
            col("conference_name"),
            col("division_abbrev"),
            col("division_name"),
            col("place_name"),
            col("team_logo"),

            col("_source_is_deleted"),
            col("_data_hash"),
            col("_version")
        ).agg(
            F.min(col("effective_from")).alias("effective_from"),
            F.max(col("effective_to")).alias("effective_to"),
            F.min_by("_source", "effective_from").alias("_source")
        ).withColumn("is_active", when(col("effective_to") == "2040-01-01 00:00:00", "True").otherwise("False"))\
        .drop("_version")\
        .withColumn("_version", sha1(concat_ws("_", col("_data_hash"), col("effective_from"))))

    try:
        union = state.union(scd2).orderBy("team_id", "effective_from")
    except:
        union = scd2.orderBy("team_id", "effective_from")

    union.repartition(1).write.mode("overwrite").parquet(DETAILED_PATH + f"sat_teams_core")


def tl_teams_roaster(**kwargs):
    current_date = kwargs["ds"]

    spark = SparkSession.builder.master("local[*]").appName("teams_to_dwh").getOrCreate()

    df = spark.read.parquet(OPERATIONAL_PATH + f"teams_stat") \
        .drop("team_name",
            "team_common_name",
            "conference_abbrev",
            "conference_name",
            "division_abbrev",
            "division_name",
            "place_name",
            "team_logo")
    
    window_spec = Window.partitionBy("team_id", "date").orderBy(F.col("_source_load_datetime").desc())

    result_df = df.withColumn("rank", F.row_number().over(window_spec)) \
        .filter("rank = 1") \
        .drop("rank")

    result_df = result_df.dropDuplicates(["team_id", "date"])\
        .orderBy("date", "team_id")

    result_df.repartition(1).write.mode("overwrite").parquet(DETAILED_PATH + f"tl_teams_stat")


def pit_teams(**kwargs):
    current_date = kwargs["ds"]

    spark = SparkSession.builder.master("local[*]").appName("teams_to_dwh").getOrCreate()

    df_sat = spark.read.parquet(DETAILED_PATH + f"sat_teams_core")

    distinct_dates = df_sat.select(col("team_id"), col("effective_from")).distinct()

    window_spec = Window.partitionBy("team_id").orderBy("effective_from")
    effective_to = lead("effective_from").over(window_spec)

    date_grid = distinct_dates.withColumn("effective_to",
        when(effective_to.isNull(), "2040-01-01 00:00:00").otherwise(effective_to)
    )

    data_versions = date_grid.join(df_sat, (date_grid.effective_from == df_sat.effective_from) & (date_grid.team_id == df_sat.team_id), "left")\
        .select(
            date_grid.team_id,
            date_grid.effective_from,
            date_grid.effective_to,
            when(date_grid.effective_to == "2040-01-01 00:00:00", True).otherwise(False).alias("is_active"),
            col("_version").alias("sat_teams_name_version")
        )
    
    fill = Window.partitionBy("team_id").orderBy("effective_from").rowsBetween(Window.unboundedPreceding, Window.currentRow)

    result = data_versions.withColumn("sat_teams_name_version", last("sat_teams_name_version", ignorenulls=True).over(fill)) \
        .withColumn("is_active", when(col("is_active"), "True").otherwise("False")) \
        .select("team_id",
                "effective_from",
                "effective_to",
                "sat_teams_name_version",
                "is_active"
        ).orderBy("team_id", "effective_from")
    
    result.repartition(1).write.mode("overwrite").parquet(COMMON_PATH + f"pit_teams")


def dm_teams(**kwargs):
    current_date = kwargs["ds"]

    spark = SparkSession.builder.master("local[*]").appName("teams_to_dwh").getOrCreate()

    df_hub = spark.read.parquet(DETAILED_PATH + f"hub_teams")
    df_sat = spark.read.parquet(DETAILED_PATH + f"sat_teams_core")
    df_pit = spark.read.parquet(COMMON_PATH + f"pit_teams")

    df_dm = df_hub.join(df_pit, df_hub.team_id == df_pit.team_id, "inner")\
        .join(df_sat, (df_pit.sat_teams_name_version == df_sat._version) & (df_pit.team_id == df_sat.team_id), "left")\
        .select(df_hub.team_id,
                df_hub.team_business_id,

                df_sat.team_name,
                df_sat.team_common_name,
                df_sat.conference_abbrev,
                df_sat.conference_name,
                df_sat.division_abbrev,
                df_sat.division_name,
                df_sat.place_name,
                df_sat.team_logo,

                df_pit.effective_from,
                df_pit.effective_to,
                df_pit.is_active
                )\
        .orderBy("team_id", "effective_from")
    
    df_dm.repartition(1).write.mode("overwrite").parquet(COMMON_PATH + f"dm_teams")



