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
    dag_id="nhl_teams",
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


def get_games_to_source(**kwargs):
    current_date = kwargs["ds"]

    spark = (
        SparkSession.builder.master("local[*]")
        .appName("parse_players_games_stat")
        .getOrCreate()
    )

    dt = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    data_games = get_information("/stats/rest/en/game", base_url="https://api.nhle.com")

    df_games_pd = pd.DataFrame(data_games["data"])

    df_games = (
        spark.createDataFrame(df_games_pd)
        .withF.Column("_source_load_datetime", F.lit(dt))
        .withF.Column("_source", F.lit("API_NHL"))
    )

    df_games.repartition(1).write.mode("overwrite").parquet(
        SOURCE_PATH + f"games/{current_date}"
    )


def get_penultimate_file_name(directory):
    files = os.listdir(directory)
    files = [f for f in files if os.path.isdir(os.path.join(directory, f))]

    if len(files) < 2:
        return None

    files.sort(key=lambda x: os.path.getmtime(os.path.join(directory, x)), reverse=True)
    penultimate_file = files[1]

    return penultimate_file


def get_games_to_staging(**kwargs):
    current_date = kwargs["ds"]

    spark = (
        SparkSession.builder.master("local[*]")
        .appName("parse_players_games_stat")
        .getOrCreate()
    )

    df_new = spark.read.parquet(SOURCE_PATH + f"games/{current_date}")
    df_new = df_new.withF.Column("_source_is_deleted", F.lit("False"))

    prev_file_name = get_penultimate_file_name(SOURCE_PATH + "games")

    if prev_file_name:
        df_prev = spark.read.parquet(SOURCE_PATH + f"games/{prev_file_name}")
        df_prev = df_prev.withF.Column("_source_is_deleted", F.lit("True"))

        df_deleted = df_prev.join(df_new, "id", "leftanti").withF.Column(
            "_source_load_datetime",
            F.lit(df_new.select("_source_load_datetime").first()[0]),
        )

        df_changed = df_new.select(
            F.col("id"),
            F.col("easternStartTime"),
            F.col("gameDate"),
            F.col("gameNumber"),
            F.col("gameScheduleStateId"),
            F.col("gameStateId"),
            F.col("gameType"),
            F.col("homeScore"),
            F.col("homeTeamId"),
            F.col("period"),
            F.col("season"),
            F.col("visitingScore"),
            F.col("visitingTeamId"),
        ).subtract(
            df_prev.select(
                F.col("id"),
                F.col("easternStartTime"),
                F.col("gameDate"),
                F.col("gameNumber"),
                F.col("gameScheduleStateId"),
                F.col("gameStateId"),
                F.col("gameType"),
                F.col("homeScore"),
                F.col("homeTeamId"),
                F.col("period"),
                F.col("season"),
                F.col("visitingScore"),
                F.col("visitingTeamId"),
            )
        )
        df_changed = df_new.join(df_changed, "id", "inner").select(df_new["*"])

        df_final = df_changed.union(df_deleted).withF.Column(
            "_batch_id", F.lit(current_date)
        )
    else:
        df_final = df_new.withF.Column("_batch_id", F.lit(current_date))

    df_final.repartition(1).write.mode("overwrite").parquet(
        STAGING_PATH + f"games/{current_date}"
    )


def get_games_to_operational(**kwargs):
    current_date = kwargs["ds"]

    spark = (
        SparkSession.builder.master("local[*]")
        .appName("parse_players_games_stat")
        .getOrCreate()
    )

    df_games = spark.read.parquet(STAGING_PATH + f"games/{current_date}")

    df_games = (
        df_games.select(
            F.col("id").alias("game_source_id"),
            F.concat_ws("_", F.col("game_source_id"), F.col("_source")).alias(
                "game_business_id"
            ),
            F.col("gameDate").alias("game_date"),
            F.col("season"),
            F.col("homeTeamId").alias("home_team_source_id"),
            F.col("visitingTeamId").alias("visiting_team_source_id"),
            F.col("easternStartTime").alias("eastern_start_time"),
            F.col("gameNumber").alias("game_number"),
            F.col("gameScheduleStateId").alias("game_schedule_state_id"),
            F.col("gameStateId").alias("game_state_id"),
            F.col("gameType").alias("game_type"),
            F.col("homeScore").alias("home_score"),
            F.col("visitingScore").alias("visiting_score"),
            F.col("period"),
            F.col("_source_load_datetime"),
            F.col("_source_is_deleted"),
            F.col("_source"),
            F.col("_batch_id"),
        )
        .withF.Column("game_id", F.sha1(F.col("game_business_id")))
        .withF.Column(
            "home_team_id",
            F.sha1(F.concat_ws("_", F.col("home_team_source_id"), F.col("_source"))),
        )
        .withF.Column(
            "visiting_team_id",
            F.sha1(
                F.concat_ws("_", F.col("visiting_team_source_id"), F.col("_source"))
            ),
        )
    )

    df_games.repartition(1).write.mode("append").parquet(OPERATIONAL_PATH + f"games")


def hub_games(**kwargs):
    current_date = kwargs["ds"]

    spark = (
        SparkSession.builder.master("local[*]").appName("teams_to_dwh").getOrCreate()
    )

    df_new = spark.read.parquet(OPERATIONAL_PATH + f"games").filter(
        F.col("_batch_id") == F.lit(current_date)
    )

    windowSpec = (
        Window.partitionBy("game_id")
        .orderBy(F.col("_source_load_datetime").desc())
        .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    )

    df_new = (
        df_new.withF.Column(
            "_source_load_datetime", F.last("_source_load_datetime").over(windowSpec)
        )
        .withF.Column("_source", F.last("_source").over(windowSpec))
        .select(
            F.col("game_id"),
            F.col("game_business_id"),
            F.col("game_source_id"),
            F.col("_source_load_datetime"),
            F.col("_source"),
        )
    )

    try:
        df_old = spark.read.parquet(DETAILED_PATH + f"hub_games")

        df_new = df_new.join(df_old, "game_id", "leftanti")
        df_final = df_new.union(df_old).orderBy("_source_load_datetime", "game_id")
    except:
        df_final = df_new.orderBy("_source_load_datetime", "game_id")

    df_final.repartition(1).write.mode("overwrite").parquet(
        DETAILED_PATH + f"hub_games"
    )


def sat_games(**kwargs):
    current_date = kwargs["ds"]

    spark = (
        SparkSession.builder.master("local[*]").appName("teams_to_dwh").getOrCreate()
    )

    increment = (
        spark.read.parquet(OPERATIONAL_PATH + f"games")
        .filter(F.col("_batch_id") == F.lit(current_date))
        .select(
            F.col("game_id"),
            F.col("game_date"),
            F.col("season"),
            F.col("eastern_start_time"),
            F.col("game_number"),
            F.col("game_type"),
            F.col("home_score"),
            F.col("visiting_score"),
            F.col("period"),
            F.col("_source_is_deleted"),
            F.col("_source_load_datetime").alias("effective_from"),
            F.col("_source"),
        )
        .withF.Column(
            "_data_hash",
            F.sha1(
                F.concat_ws(
                    "_",
                    F.col("game_date"),
                    F.col("season"),
                    F.col("eastern_start_time"),
                    F.col("game_number"),
                    F.col("game_type"),
                    F.col("home_score"),
                    F.col("visiting_score"),
                    F.col("period"),
                    F.col("_source_is_deleted"),
                )
            ),
        )
    )

    try:
        sat_games = spark.read.parquet(DETAILED_PATH + f"sat_games")
        state = sat_games.filter(F.col("is_active") == "False")

        active = (
            sat_games.filter(F.col("is_active") == "True")
            .select(
                F.col("game_id"),
                F.col("game_date"),
                F.col("season"),
                F.col("eastern_start_time"),
                F.col("game_number"),
                F.col("game_type"),
                F.col("home_score"),
                F.col("visiting_score"),
                F.col("period"),
                F.col("_source_is_deleted"),
                F.col("effective_from"),
                F.col("_source"),
                F.col("_data_hash"),
            )
            .union(increment)
        )
    except:
        active = increment

    scd_window = Window.partitionBy("game_id").orderBy("effective_from")
    is_change = F.when(
        F.lag("_data_hash").over(scd_window) != F.col("_data_hash"), "True"
    ).otherwise("False")

    row_changes = active.select(
        F.col("game_id"),
        F.col("game_date"),
        F.col("season"),
        F.col("eastern_start_time"),
        F.col("game_number"),
        F.col("game_type"),
        F.col("home_score"),
        F.col("visiting_score"),
        F.col("period"),
        F.col("effective_from"),
        F.coalesce(is_change.cast("string"), F.lit("True")).alias("is_change"),
        F.col("_data_hash"),
        F.col("_source_is_deleted"),
        F.col("_source"),
    )

    scd_window = Window.partitionBy("game_id").orderBy(
        F.col("effective_from").asc(), F.col("is_change").asc()
    )

    next_effective_from = F.lead("effective_from").over(scd_window)
    version_count = F.sum(F.when(F.col("is_change") == "True", 1).otherwise(0)).over(
        scd_window.rangeBetween(Window.unboundedPreceding, 0)
    )

    row_versions = row_changes.select(
        F.col("game_id"),
        F.col("game_date"),
        F.col("season"),
        F.col("eastern_start_time"),
        F.col("game_number"),
        F.col("game_type"),
        F.col("home_score"),
        F.col("visiting_score"),
        F.col("period"),
        F.col("effective_from").alias("effective_from"),
        next_effective_from.cast("string").alias("effective_to"),
        version_count.alias("_version"),
        "_data_hash",
        "_source_is_deleted",
        "_source",
    ).withF.Column(
        "effective_to",
        F.coalesce(
            F.expr("effective_to - interval 1 second"), F.lit("2040-01-01 00:00:00")
        ),
    )

    scd2 = (
        row_versions.groupBy(
            F.col("game_id"),
            F.col("game_date"),
            F.col("season"),
            F.col("eastern_start_time"),
            F.col("game_number"),
            F.col("game_type"),
            F.col("home_score"),
            F.col("visiting_score"),
            F.col("period"),
            "_source_is_deleted",
            "_data_hash",
            "_version",
        )
        .agg(
            F.min(F.col("effective_from")).alias("effective_from"),
            F.max(F.col("effective_to")).alias("effective_to"),
            F.min_by("_source", "effective_from").alias("_source"),
        )
        .withF.Column(
            "is_active",
            F.when(F.col("effective_to") == "2040-01-01 00:00:00", "True").otherwise(
                "False"
            ),
        )
        .drop("_version")
        .withF.Column(
            "_version",
            F.sha1(F.concat_ws("_", F.col("_data_hash"), F.col("effective_from"))),
        )
    )

    try:
        union = state.union(scd2).orderBy("game_id", "effective_from")
    except:
        union = scd2.orderBy("game_id", "effective_from")

    union.repartition(1).write.mode("overwrite").parquet(DETAILED_PATH + f"sat_games")


def tl_teams_games(**kwargs):
    spark = (
        SparkSession.builder.master("local[*]").appName("teams_to_dwh").getOrCreate()
    )

    df = spark.read.parquet(OPERATIONAL_PATH + f"games")

    window_spec = Window.partitionBy(
        "game_id", "home_team_id", "visiting_team_id"
    ).orderBy(F.F.col("_source_load_datetime").desc())

    result_df = (
        df.select(
            "game_date",
            "game_id",
            "home_team_id",
            "visiting_team_id",
            "_source_load_datetime",
        )
        .withF.Column("rank", F.row_number().over(window_spec))
        .filter("rank = 1")
        .drop("rank")
    )

    result_df = result_df.dropDuplicates(
        ["game_id", "home_team_id", "visiting_team_id"]
    ).orderBy("game_date", "game_id")

    result_df.repartition(1).write.mode("overwrite").parquet(
        DETAILED_PATH + f"tl_teams_games"
    )


def pit_games(**kwargs):
    spark = (
        SparkSession.builder.master("local[*]").appName("teams_to_dwh").getOrCreate()
    )

    df_sat = spark.read.parquet(DETAILED_PATH + f"sat_games")

    distinct_dates = df_sat.select(F.col("game_id"), F.col("effective_from")).distinct()

    window_spec = Window.partitionBy("game_id").orderBy("effective_from")
    effective_to = F.lead("effective_from").over(window_spec)

    date_grid = distinct_dates.withF.Column(
        "effective_to",
        F.when(effective_to.isNull(), "2040-01-01 00:00:00").otherwise(effective_to),
    )

    data_versions = date_grid.join(
        df_sat,
        (date_grid.effective_from == df_sat.effective_from)
        & (date_grid.game_id == df_sat.game_id),
        "left",
    ).select(
        date_grid.game_id,
        date_grid.effective_from,
        date_grid.effective_to,
        F.when(date_grid.effective_to == "2040-01-01 00:00:00", True)
            .otherwise(False)
            .alias("is_active"),
        F.col("_version").alias("sat_game_version"),
    )

    fill = (
        Window.partitionBy("game_id")
        .orderBy("effective_from")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )

    result = (
        data_versions.withF.Column(
            "sat_teams_name_version",
            F.last("sat_game_version", ignorenulls=True).over(fill),
        )
        .withF.Column("is_active", F.when(F.col("is_active"), "True").otherwise("False"))
        .select(
            "game_id", "effective_from", "effective_to", "sat_game_version", "is_active"
        )
        .orderBy("game_id", "effective_from")
    )

    result.repartition(1).write.mode("overwrite").parquet(COMMON_PATH + f"pit_games")


def dm_games(**kwargs):
    spark = (
        SparkSession.builder.master("local[*]").appName("teams_to_dwh").getOrCreate()
    )

    df_hub = spark.read.parquet(DETAILED_PATH + f"hub_games")
    df_sat = spark.read.parquet(DETAILED_PATH + f"sat_games")
    df_pit = spark.read.parquet(COMMON_PATH + f"pit_games")

    df_dm = (
        df_hub.join(df_pit, df_hub.game_id == df_pit.game_id, "inner")
        .join(
            df_sat,
            (df_pit.sat_game_version == df_sat._version)
            & (df_pit.game_id == df_sat.game_id),
            "left",
        )
        .select(
            df_hub.game_id,
            df_hub.game_source_id,
            df_sat.game_date,
            df_sat.season,
            df_sat.eastern_start_time,
            df_sat.game_number,
            df_sat.game_type,
            df_sat.home_score,
            df_sat.visiting_score,
            df_sat.period,
            df_sat._source_is_deleted,
            df_pit.effective_from,
            df_pit.effective_to,
            df_pit.is_active,
        )
        .orderBy("game_id", "effective_from")
        .distinct()
    )

    df_dm.repartition(1).write.mode("overwrite").parquet(COMMON_PATH + f"dm_games")


task_get_games_to_source = PythonOperator(
    task_id="get_games_to_source",
    python_callable=get_games_to_source,
    dag=dag,
)

task_get_games_to_staging = PythonOperator(
    task_id="get_games_to_staging",
    python_callable=get_games_to_staging,
    dag=dag,
)

task_get_games_to_operational = PythonOperator(
    task_id="get_games_to_operational",
    python_callable=get_games_to_operational,
    dag=dag,
)

task_hub_games = PythonOperator(
    task_id="hub_games",
    python_callable=hub_games,
    dag=dag,
)

task_sat_games = PythonOperator(
    task_id="sat_games",
    python_callable=sat_games,
    dag=dag,
)

task_tl_teams_games = PythonOperator(
    task_id="tl_teams_games",
    python_callable=tl_teams_games,
    dag=dag,
)

task_pit_games = PythonOperator(
    task_id="pit_games",
    python_callable=pit_games,
    dag=dag,
)

task_dm_games = PythonOperator(
    task_id="dm_games",
    python_callable=dm_games,
    dag=dag,
)

task_get_games_to_source >> task_get_games_to_staging >> task_get_games_to_operational
task_get_games_to_operational >> [task_hub_games, task_sat_games, task_tl_teams_games]
task_sat_games >> task_pit_games >> task_dm_games
task_hub_games >> task_dm_games