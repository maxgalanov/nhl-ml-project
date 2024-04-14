import pandas as pd
import requests
import os
from datetime import timedelta, datetime

from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

import pyspark
from pyspark.sql import SparkSession
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
    dag_id="nhl_players_games",
    schedule_interval="45 5 1 10 *",
    start_date=days_ago(2),
    catchup=False,
    tags=["hse_nhl_ml_project"],
    default_args=DEFAULT_ARGS,
    description="ETL process for getting list of NHL teams",
)

SOURCE_PATH = "/nhl_project/data/dwh/source/"
STAGING_PATH = "/nhl_project/data/dwh/vault/staging/"
OPERATIONAL_PATH = "/nhl_project/data/dwh/vault/operational/"
DETAILED_PATH = "/nhl_project/data/dwh/vault/detailed/"
COMMON_PATH = "/nhl_project/data/dwh/vault/common/"


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


def get_players_games_stat_to_source(**kwargs):
    current_date = kwargs["ds"]

    spark = (
        SparkSession.builder.master("local[*]")
        .appName("parse_players_games_stat")
        .getOrCreate()
    )

    df_players = spark.read.parquet(DETAILED_PATH + f"hub_players").select(
        "player_source_id"
    )
    players_lst = df_players.distinct().rdd.map(lambda x: x[0]).collect()

    df_games = pd.DataFrame()

    for player in players_lst:
        try:
            player_data = get_information(f"/v1/player/{player}/game-log/now")
            df_player = pd.DataFrame(player_data["gameLog"])
            df_player["playerId"] = player

            df_games = pd.concat([df_games, df_player], ignore_index=True)
        except:
            continue

    df_games["commonName"] = df_games["commonName"].apply(
        lambda x: x.get("default", "") if type(x) == dict else ""
    )
    df_games["opponentCommonName"] = df_games["opponentCommonName"].apply(
        lambda x: x.get("default", "") if type(x) == dict else ""
    )
    df_games["gameId"] = df_games.gameId.astype("int")

    dt = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    df_games_info = (
        spark.createDataFrame(df_games)
        .withColumn("_source_load_datetime", F.lit(dt))
        .withColumn("_source", F.lit("API_NHL"))
    )

    df_games_info.repartition(1).write.mode("overwrite").parquet(
        SOURCE_PATH + f"players_games_stat/{current_date}"
    )


def get_penultimate_file_name(directory):
    files = os.listdir(directory)
    files = [f for f in files if os.path.isdir(os.path.join(directory, f))]

    if len(files) < 2:
        return None

    files.sort(key=lambda x: os.path.getmtime(os.path.join(directory, x)), reverse=True)
    penultimate_file = files[1]

    return penultimate_file


def get_players_games_stat_to_staging(**kwargs):
    current_date = kwargs["ds"]

    spark = (
        SparkSession.builder.master("local[*]")
        .appName("parse_players_games_stat")
        .getOrCreate()
    )

    df_new = spark.read.parquet(SOURCE_PATH + f"players_games_stat/{current_date}")
    df_new = df_new.withColumn("_source_is_deleted", F.lit("False"))

    prev_file_name = get_penultimate_file_name(SOURCE_PATH + "players_games_stat")

    if prev_file_name:
        df_prev = spark.read.parquet(
            SOURCE_PATH + f"players_games_stat/{prev_file_name}"
        )
        df_prev = df_prev.withColumn("_source_is_deleted", F.lit("True"))

        df_deleted = df_prev.join(
            df_new, ["gameId", "playerId"], "leftanti"
        ).withColumn(
            "_source_load_datetime",
            F.lit(df_new.select("_source_load_datetime").first()[0]),
        )

        df_changed = df_new.select(
            F.col("gameId"),
            F.col("playerId"),
            F.col("teamAbbrev"),
            F.col("homeRoadFlag"),
            F.col("gameDate"),
            F.col("goals"),
            F.col("assists"),
            F.col("commonName"),
            F.col("opponentCommonName"),
            F.col("points"),
            F.col("plusMinus"),
            F.col("powerPlayGoals"),
            F.col("powerPlayPoints"),
            F.col("gameWinningGoals"),
            F.col("otGoals"),
            F.col("shots"),
            F.col("shifts"),
            F.col("shots"),
            F.col("shorthandedGoals"),
            F.col("shorthandedPoints"),
            F.col("opponentAbbrev"),
            F.col("pim"),
            F.col("toi"),
            F.col("gamesStarted"),
            F.col("decision"),
            F.col("shotsAgainst"),
            F.col("goalsAgainst"),
            F.col("savePctg"),
            F.col("shutouts"),
        ).subtract(
            df_prev.select(
                F.col("gameId"),
                F.col("playerId"),
                F.col("teamAbbrev"),
                F.col("homeRoadFlag"),
                F.col("gameDate"),
                F.col("goals"),
                F.col("assists"),
                F.col("commonName"),
                F.col("opponentCommonName"),
                F.col("points"),
                F.col("plusMinus"),
                F.col("powerPlayGoals"),
                F.col("powerPlayPoints"),
                F.col("gameWinningGoals"),
                F.col("otGoals"),
                F.col("shots"),
                F.col("shifts"),
                F.col("shots"),
                F.col("shorthandedGoals"),
                F.col("shorthandedPoints"),
                F.col("opponentAbbrev"),
                F.col("pim"),
                F.col("toi"),
                F.col("gamesStarted"),
                F.col("decision"),
                F.col("shotsAgainst"),
                F.col("goalsAgainst"),
                F.col("savePctg"),
                F.col("shutouts"),
            )
        )

        df_changed = df_new.join(df_changed, ["gameId", "playerId"], "inner").select(
            df_new["*"]
        )

        df_final = df_changed.union(df_deleted).withColumn(
            "_batch_id", F.lit(current_date)
        )
    else:
        df_final = df_new.withColumn("_batch_id", F.lit(current_date))

    df_final.repartition(1).write.mode("overwrite").parquet(
        STAGING_PATH + f"players_games_stat/{current_date}"
    )


def get_players_games_stat_to_operational(**kwargs):
    current_date = kwargs["ds"]

    spark = (
        SparkSession.builder.master("local[*]")
        .appName("parse_players_games_stat")
        .getOrCreate()
    )

    df = spark.read.parquet(STAGING_PATH + f"players_games_stat/{current_date}")
    hub_teams = spark.read.parquet(DETAILED_PATH + f"hub_teams")

    df = (
        df.select(
            F.col("gameId").cast("string").alias("game_source_id"),
            F.col("playerId").cast("string").alias("player_source_id"),
            F.col("teamAbbrev").cast("string").alias("team_business_id"),
            F.col("opponentAbbrev").cast("string").alias("opponent_team_business_id"),
            F.col("homeRoadFlag").cast("string").alias("home_road_flag"),
            F.col("gameDate").cast("string").alias("game_date"),
            F.col("goals").cast("string"),
            F.col("assists").cast("string"),
            F.col("commonName").cast("string").alias("common_name"),
            F.col("opponentCommonName").cast("string").alias("opponent_common_name"),
            F.col("points").cast("string"),
            F.col("plusMinus").cast("string").alias("plus_minus"),
            F.col("powerPlayGoals").cast("string").alias("power_play_goals"),
            F.col("powerPlayPoints").cast("string").alias("power_play_points"),
            F.col("gameWinningGoals").cast("string").alias("game_winning_goals"),
            F.col("otGoals").cast("string").alias("ot_goals"),
            F.col("shots").cast("string"),
            F.col("shifts").cast("string"),
            F.col("shorthandedGoals").cast("string").alias("shorthanded_goals"),
            F.col("shorthandedPoints").cast("string").alias("shorthanded_points"),
            F.col("pim").cast("string"),
            F.col("toi").cast("string"),
            F.col("gamesStarted").cast("string").alias("games_started"),
            F.col("decision").cast("string"),
            F.col("shotsAgainst").cast("string").alias("shots_against"),
            F.col("goalsAgainst").cast("string").alias("goals_against"),
            F.col("savePctg").cast("string").alias("save_pctg"),
            F.col("shutouts").cast("string"),
            F.col("_source_load_datetime").cast("string"),
            F.col("_source_is_deleted").cast("string"),
            F.col("_source").cast("string"),
            F.col("_batch_id").cast("string"),
        )
        .withColumn(
            "game_business_id",
            F.concat_ws("_", F.col("game_source_id"), F.col("_source")),
        )
        .withColumn(
            "player_business_id",
            F.concat_ws("_", F.col("player_source_id"), F.col("_source")),
        )
        .withColumn(
            "game_id",
            F.sha1(F.concat_ws("_", F.col("game_source_id"), F.col("_source"))),
        )
        .withColumn(
            "player_id",
            F.sha1(F.concat_ws("_", F.col("player_source_id"), F.col("_source"))),
        )
    )

    df = (
        df.alias("df1")
        .join(
            hub_teams.alias("df2"),
            df.team_business_id == hub_teams.team_business_id,
            "left",
        )
        .select(df["*"], hub_teams["team_id"].alias("team_id"))
    )

    df = (
        df.alias("df3")
        .join(
            hub_teams.alias("df4"),
            F.col("df3.opponent_team_business_id") == F.col("df4.team_business_id"),
            "left",
        )
        .select(df["*"], F.col("df4.team_id").alias("opponent_team_id"))
    )

    df.repartition(1).write.mode("append").parquet(
        OPERATIONAL_PATH + f"players_games_stat"
    )


def tl_players_games_stat(**kwargs):
    spark = (
        SparkSession.builder.master("local[*]")
        .appName("parse_players_games_stat")
        .getOrCreate()
    )

    df = spark.read.parquet(OPERATIONAL_PATH + f"players_games_stat")

    window_spec = Window.partitionBy(
        "game_id", "player_id", "team_id", "opponent_team_id"
    ).orderBy(F.col("_source_load_datetime").desc())

    result_df = (
        df.withColumn("rank", F.row_number().over(window_spec))
        .filter("rank = 1")
        .drop("rank")
    )

    result_df = result_df.dropDuplicates(
        ["game_id", "player_id", "team_id", "opponent_team_id"]
    ).orderBy("game_date", "game_id")

    result_df.repartition(1).write.mode("overwrite").parquet(
        DETAILED_PATH + f"tl_players_games_stat"
    )


task_get_players_games_stat_to_source = PythonOperator(
    task_id="get_players_games_stat_to_source",
    python_callable=get_players_games_stat_to_source,
    dag=dag,
)

task_get_players_games_stat_to_staging = PythonOperator(
    task_id="get_players_games_stat_to_staging",
    python_callable=get_players_games_stat_to_staging,
    dag=dag,
)

task_get_players_games_stat_to_operational = PythonOperator(
    task_id="get_players_games_stat_to_operational",
    python_callable=get_players_games_stat_to_operational,
    dag=dag,
)

task_tl_players_games_stat = PythonOperator(
    task_id="tl_players_games_stat",
    python_callable=tl_players_games_stat,
    dag=dag,
)

(
    task_get_players_games_stat_to_source
    >> task_get_players_games_stat_to_staging
    >> task_get_players_games_stat_to_operational
)
task_get_players_games_stat_to_operational >> task_tl_players_games_stat