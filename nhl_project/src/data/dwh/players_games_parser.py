import pandas as pd
from datetime import timedelta, datetime

from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from pyspark.sql import SparkSession
from pyspark.sql.window import Window
import pyspark.sql.functions as F

from nhl_project.src.data.functions import get_information, read_table_from_pg, write_table_to_pg


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


def get_players_games_stat_to_source(**kwargs):
    current_date = kwargs["ds"]

    spark = (
        SparkSession.builder.master("local[*]")
        .appName("parse_players_games_stat")
        .getOrCreate()
    )

    df_players = read_table_from_pg(spark, "dwh_detailed.hub_players").select("player_source_id")
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

    table_name = f"dwh_source.players_games_stat_{current_date.replace('-', '_')}"
    write_table_to_pg(df_games_info, spark, "overwrite", table_name)

    data = [("dwh_source.players_games_stat", dt)]
    df_meta = spark.createDataFrame(data, schema=["table_name", "updated_at"])
    df_meta.show()

    write_table_to_pg(df_meta, spark, "append", "dwh_source.metadata_table")


def get_penultimate_table_name(spark, table_name):

    df_meta = read_table_from_pg(spark, "dwh_source.metadata_table")
    
    sorted_df_meta = df_meta.filter(F.col("table_name") == table_name).orderBy(F.col("updated_at").desc())
    if sorted_df_meta.count() < 2:
        return None
    else:
        second_to_last_date = sorted_df_meta.select("updated_at").limit(2).orderBy(F.col("updated_at").asc()).collect()[0][0]
        return second_to_last_date


def get_players_games_stat_to_staging(**kwargs):
    current_date = kwargs["ds"]

    spark = (
        SparkSession.builder.master("local[*]")
        .appName("parse_players_games_stat")
        .getOrCreate()
    )

    df_new = read_table_from_pg(spark, f"dwh_source.players_games_stat_{current_date.replace('-', '_')}")
    df_new = df_new.withColumn("_source_is_deleted", F.lit("False"))

    prev_table_name = get_penultimate_table_name(spark, "dwh_source.players_games_stat")

    if prev_table_name:
        df_prev = read_table_from_pg(spark, f"dwh_source.players_games_stat_{prev_table_name[:10].replace('-', '_')}")
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

        df_final = df_changed.unionByName(df_deleted).withColumn(
            "_batch_id", F.lit(current_date)
        )
    else:
        df_final = df_new.withColumn("_batch_id", F.lit(current_date))

    write_table_to_pg(df_final, spark, "overwrite", "dwh_staging.players_games_stat")


def get_players_games_stat_to_operational(**kwargs):
    current_date = kwargs["ds"]

    spark = (
        SparkSession.builder.master("local[*]")
        .appName("parse_players_games_stat")
        .getOrCreate()
    )

    df = read_table_from_pg(spark, "dwh_staging.players_games_stat")
    hub_teams = read_table_from_pg(spark, "dwh_detailed.hub_teams")

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

    write_table_to_pg(df, spark, "append", "dwh_operational.players_games_stat")


def tl_players_games_stat(**kwargs):
    spark = (
        SparkSession.builder.master("local[*]")
        .appName("parse_players_games_stat")
        .getOrCreate()
    )

    df = read_table_from_pg(spark, "dwh_operational.players_games_stat")

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

    write_table_to_pg(result_df, spark, "overwrite", "dwh_detailed.tl_players_games_stat")


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