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
    dag_id="nhl_games",
    schedule_interval="10 12 * * *",
    start_date=days_ago(2),
    catchup=False,
    tags=["hse_nhl_ml_project"],
    default_args=DEFAULT_ARGS,
    description="ETL process for getting list of NHL games",
)


def get_games_to_source(**kwargs):
    current_date = kwargs["ds"]

    ts = kwargs['ts']
    ts_datetime = datetime.fromisoformat(ts)
    dt = ts_datetime.strftime('%Y-%m-%d %H:%M:%S')

    spark = (
        SparkSession.builder.config(
            "spark.jars",
            "~/airflow_venv/lib/python3.10/site-packages/pyspark/jars/postgresql-42.3.1.jar",
        )
        .master("local[*]")
        .appName("parse_teams")
        .getOrCreate()
    )

    data_games = get_information("/stats/rest/en/game", base_url="https://api.nhle.com")

    df_games_pd = pd.DataFrame(data_games["data"])

    df_games = (
        spark.createDataFrame(df_games_pd)
        .withColumn("_source_load_datetime", F.lit(dt))
        .withColumn("_source", F.lit("API_NHL"))
    )

    table_name = f"dwh_source.games_{current_date.replace('-', '_')}"
    write_table_to_pg(df_games, spark, "overwrite", table_name)

    data = [("dwh_source.games", dt)]
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


def get_games_to_staging(**kwargs):
    current_date = kwargs["ds"]

    spark = (
        SparkSession.builder.config(
            "spark.jars",
            "~/airflow_venv/lib/python3.10/site-packages/pyspark/jars/postgresql-42.3.1.jar",
        )
        .master("local[*]")
        .appName("parse_teams")
        .getOrCreate()
    )

    df_new = read_table_from_pg(spark, f"dwh_source.games_{current_date.replace('-', '_')}")
    df_new = df_new.withColumn("_source_is_deleted", F.lit("False"))

    prev_table_name = get_penultimate_table_name(spark, "dwh_source.games")

    if prev_table_name:
        df_prev = read_table_from_pg(
            spark, f"dwh_source.games_{prev_table_name[:10].replace('-', '_')}"
        )
        df_prev = df_prev.withColumn("_source_is_deleted", F.lit("True"))

        df_deleted = df_prev.join(df_new, "id", "leftanti").withColumn(
            "_source_load_datetime", F.lit(df_new.select("_source_load_datetime").first()[0])
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

        df_final = df_changed.unionByName(df_deleted).withColumn("_batch_id", F.lit(current_date))
    else:
        df_final = df_new.withColumn("_batch_id", F.lit(current_date))

    write_table_to_pg(df_final, spark, "overwrite", "dwh_staging.games")


def get_games_to_operational(**kwargs):
    current_date = kwargs["ds"]

    spark = (
        SparkSession.builder.config(
            "spark.jars",
            "~/airflow_venv/lib/python3.10/site-packages/pyspark/jars/postgresql-42.3.1.jar",
        )
        .master("local[*]")
        .appName("parse_teams")
        .getOrCreate()
    )

    df = read_table_from_pg(spark, f"dwh_staging.games")

    df = (
        df.select(
            F.col("id").alias("game_source_id"),
            F.concat_ws("_", F.col("game_source_id"), F.col("_source")).alias("game_business_id"),
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
        .withColumn("game_id", F.sha1(F.col("game_business_id")))
        .withColumn(
            "home_team_id", F.sha1(F.concat_ws("_", F.col("home_team_source_id"), F.col("_source")))
        )
        .withColumn(
            "visiting_team_id",
            F.sha1(F.concat_ws("_", F.col("visiting_team_source_id"), F.col("_source"))),
        )
    )

    write_table_to_pg(df, spark, "append", "dwh_operational.games")


def hub_games(**kwargs):
    current_date = kwargs["ds"]

    spark = (
        SparkSession.builder.config(
            "spark.jars",
            "~/airflow_venv/lib/python3.10/site-packages/pyspark/jars/postgresql-42.3.1.jar",
        )
        .master("local[*]")
        .appName("parse_teams")
        .getOrCreate()
    )

    df_new = read_table_from_pg(spark, f"dwh_operational.games").filter(
        F.col("_batch_id") == F.lit(current_date)
    )

    windowSpec = (
        Window.partitionBy("game_id")
        .orderBy(F.col("_source_load_datetime").desc())
        .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    )

    df_new = (
        df_new.withColumn(
            "_source_load_datetime", F.last("_source_load_datetime").over(windowSpec)
        )
        .withColumn("_source", F.last("_source").over(windowSpec))
        .select(
            F.col("game_id"),
            F.col("game_business_id"),
            F.col("game_source_id"),
            F.col("_source_load_datetime"),
            F.col("_source"),
        )
    )

    try:
        df_old = read_table_from_pg(spark, f"dwh_detailed.hub_games")

        df_new = df_new.join(df_old, "game_id", "leftanti")
        df_final = df_new.unionByName(df_old).orderBy("_source_load_datetime", "game_id")
    except:
        df_final = df_new.orderBy("_source_load_datetime", "game_id")

    write_table_to_pg(df_final, spark, "overwrite", "dwh_detailed.hub_games")


def sat_games(**kwargs):
    current_date = kwargs["ds"]

    spark = (
        SparkSession.builder.config(
            "spark.jars",
            "~/airflow_venv/lib/python3.10/site-packages/pyspark/jars/postgresql-42.3.1.jar",
        )
        .master("local[*]")
        .appName("parse_teams")
        .getOrCreate()
    )

    increment = (
        read_table_from_pg(spark, f"dwh_operational.games")
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
        .withColumn(
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
        sat_games = read_table_from_pg(spark, f"dwh_detailed.sat_games")
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
            .unionByName(increment)
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
    ).withColumn(
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
        .withColumn(
            "is_active",
            F.when(F.col("effective_to") == "2040-01-01 00:00:00", "True").otherwise(
                "False"
            ),
        )
        .drop("_version")
        .withColumn(
            "_version",
            F.sha1(F.concat_ws("_", F.col("_data_hash"), F.col("effective_from"))),
        )
    )

    try:
        union = state.unionByName(scd2).orderBy("game_id", "effective_from")
    except:
        union = scd2.orderBy("game_id", "effective_from")

    write_table_to_pg(union, spark, "overwrite", "dwh_detailed.sat_games")


def tl_teams_games(**kwargs):
    spark = (
        SparkSession.builder.config(
            "spark.jars",
            "~/airflow_venv/lib/python3.10/site-packages/pyspark/jars/postgresql-42.3.1.jar",
        )
        .master("local[*]")
        .appName("parse_teams")
        .getOrCreate()
    )

    df = read_table_from_pg(spark, f"dwh_operational.games")

    window_spec = Window.partitionBy(
        "game_id", "home_team_id", "visiting_team_id"
    ).orderBy(F.col("_source_load_datetime").desc())

    result_df = (
        df.select(
            "game_date",
            "game_id",
            "home_team_id",
            "visiting_team_id",
            "_source_load_datetime",
        )
        .withColumn("rank", F.row_number().over(window_spec))
        .filter("rank = 1")
        .drop("rank")
    )

    result_df = result_df.dropDuplicates(
        ["game_id", "home_team_id", "visiting_team_id"]
    ).orderBy("game_date", "game_id")

    write_table_to_pg(result_df, spark, "overwrite", "dwh_detailed.tl_teams_games")


def pit_games(**kwargs):
    spark = (
        SparkSession.builder.config(
            "spark.jars",
            "~/airflow_venv/lib/python3.10/site-packages/pyspark/jars/postgresql-42.3.1.jar",
        )
        .master("local[*]")
        .appName("parse_teams")
        .getOrCreate()
    )

    df_sat = read_table_from_pg(spark, f"dwh_detailed.sat_games")
    distinct_dates = df_sat.select(F.col("game_id"), F.col("effective_from")).distinct()

    window_spec = Window.partitionBy("game_id").orderBy("effective_from")
    effective_to = F.lead("effective_from").over(window_spec)

    date_grid = distinct_dates.withColumn(
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
        data_versions.withColumn(
            "sat_teams_name_version",
            F.last("sat_game_version", ignorenulls=True).over(fill),
        )
        .withColumn("is_active", F.when(F.col("is_active"), "True").otherwise("False"))
        .select(
            "game_id", "effective_from", "effective_to", "sat_game_version", "is_active"
        )
        .orderBy("game_id", "effective_from")
    )

    write_table_to_pg(result, spark, "overwrite", "dwh_common.pit_games")


def dm_games(**kwargs):
    spark = (
        SparkSession.builder.config(
            "spark.jars",
            "~/airflow_venv/lib/python3.10/site-packages/pyspark/jars/postgresql-42.3.1.jar",
        )
        .master("local[*]")
        .appName("parse_teams")
        .getOrCreate()
    )

    df_hub = read_table_from_pg(spark, f"dwh_detailed.hub_games")
    df_sat = read_table_from_pg(spark, f"dwh_detailed.sat_games")
    df_pit = read_table_from_pg(spark, f"dwh_common.pit_games")

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

    write_table_to_pg(df_dm, spark, "overwrite", "dwh_common.dm_games")


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