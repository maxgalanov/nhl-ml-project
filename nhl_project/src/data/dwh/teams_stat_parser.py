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
    dag_id="nhl_teams_stat",
    schedule_interval="10 12 * * *",
    start_date=days_ago(2),
    catchup=False,
    tags=["hse_nhl_ml_project"],
    default_args=DEFAULT_ARGS,
    description="ETL process for getting list of NHL teams daily statistics",
)


def get_teams_stat_to_source(**kwargs):
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

    current_dt = datetime.strptime(current_date, "%Y-%m-%d").date()
    dates_last_week = []

    for i in range(14):
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

    df_teams_stat = (
        spark.createDataFrame(teams_stat)
        .withColumn("_source_load_datetime", F.lit(dt))
        .withColumn("_source", F.lit("API_NHL"))
    )

    table_name = f"dwh_source.teams_stat_{current_date.replace('-', '_')}"
    write_table_to_pg(df_teams_stat, spark, "overwrite", table_name)

    data = [("dwh_source.teams_stat", dt)]
    df_meta = spark.createDataFrame(data, schema=["table_name", "updated_at"])
    df_meta.show()

    write_table_to_pg(df_meta, spark, "append", "dwh_source.metadata_table")


def get_teama_stat_to_staging(**kwargs):
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

    df_new = read_table_from_pg(spark, f"dwh_source.teams_stat_{current_date.replace('-', '_')}")
    df_new = df_new.withColumn("_source_is_deleted", F.lit("False"))

    df_final = df_new.withColumn("_batch_id", F.lit(current_date))

    write_table_to_pg(df_final, spark, "overwrite", "dwh_staging.teams_stat")


def get_teama_stat_to_operational(**kwargs):
    spark = (
        SparkSession.builder.config(
            "spark.jars",
            "~/airflow_venv/lib/python3.10/site-packages/pyspark/jars/postgresql-42.3.1.jar",
        )
        .master("local[*]")
        .appName("parse_teams")
        .getOrCreate()
    )

    df = read_table_from_pg(spark, "dwh_staging.teams_stat")
    hub_teams = read_table_from_pg(spark, "dwh_detailed.hub_teams")

    df = df.select(
        F.col("date").alias("date"),
        F.col("seasonId").alias("season_id"),
        F.col("teamName").alias("team_name"),
        F.col("teamCommonName").alias("team_common_name"),
        F.col("teamAbbrev").alias("team_business_id"),
        F.col("conferenceAbbrev").alias("conference_abbrev"),
        F.col("conferenceHomeSequence").cast("int").alias("conference_home_sequence"),
        F.col("conferenceL10Sequence").cast("int").alias("conference_l10_sequence"),
        F.col("conferenceName").alias("conference_name"),
        F.col("conferenceRoadSequence").cast("int").alias("conference_road_sequence"),
        F.col("conferenceSequence").cast("int").alias("conference_sequence"),
        F.col("divisionAbbrev").alias("division_abbrev"),
        F.col("divisionHomeSequence").cast("int").alias("division_home_sequence"),
        F.col("divisionL10Sequence").cast("int").alias("division_l10_sequence"),
        F.col("divisionName").alias("division_name"),
        F.col("divisionRoadSequence").cast("int").alias("division_road_sequence"),
        F.col("divisionSequence").cast("int").alias("division_sequence"),
        F.col("gameTypeId").alias("game_type_id"),
        F.col("gamesPlayed").cast("int").alias("games_played"),
        F.col("goalDifferential").cast("int").alias("goal_differential"),
        F.col("goalDifferentialPctg").cast("double").alias("goal_differential_pctg"),
        F.col("goalAgainst").cast("int").alias("goal_against"),
        F.col("goalFor").cast("int").alias("goal_for"),
        F.col("goalsForPctg").cast("double").alias("goals_for_pctg"),
        F.col("homeGamesPlayed").cast("int").alias("home_games_played"),
        F.col("homeGoalDifferential").cast("int").alias("home_goal_differential"),
        F.col("homeGoalsAgainst").cast("int").alias("home_goals_against"),
        F.col("homeGoalsFor").cast("int").alias("home_goals_for"),
        F.col("homeLosses").cast("int").alias("home_losses"),
        F.col("homeOtLosses").cast("int").alias("home_ot_losses"),
        F.col("homePoints").cast("int").alias("home_points"),
        F.col("homeRegulationPlusOtWins").cast("int").alias("home_regulation_plus_ot_wins"),
        F.col("homeRegulationWins").cast("int").alias("home_regulation_wins"),
        F.col("homeTies").cast("int").alias("home_ties"),
        F.col("homeWins").cast("int").alias("home_wins"),
        F.col("l10GamesPlayed").cast("int").alias("l10_games_played"),
        F.col("l10GoalDifferential").cast("int").alias("l10_goal_differential"),
        F.col("l10GoalsAgainst").cast("int").alias("l10_goals_against"),
        F.col("l10GoalsFor").cast("int").alias("l10_goals_for"),
        F.col("l10Losses").cast("int").alias("l10_losses"),
        F.col("l10OtLosses").cast("int").alias("l10_ot_losses"),
        F.col("l10Points").cast("int").alias("l10_points"),
        F.col("l10RegulationPlusOtWins").cast("int").alias("l10_regulation_plus_ot_wins"),
        F.col("l10RegulationWins").cast("int").alias("l10_regulation_wins"),
        F.col("l10Ties").cast("int").alias("l10_ties"),
        F.col("l10Wins").cast("int").alias("l10_wins"),
        F.col("leagueHomeSequence").cast("int").alias("league_home_sequence"),
        F.col("leagueL10Sequence").cast("int").alias("league_l10_sequence"),
        F.col("leagueRoadSequence").cast("int").alias("league_road_sequence"),
        F.col("leagueSequence").cast("int").alias("league_sequence"),
        F.col("losses").cast("int").alias("losses"),
        F.col("otLosses").cast("int").alias("ot_losses"),
        F.col("placeName").alias("place_name"),
        F.col("pointPctg").cast("double").alias("point_pctg"),
        F.col("points").cast("int").alias("points"),
        F.col("regulationPlusOtWinPctg").cast("double").alias("regulation_plus_ot_win_pctg"),
        F.col("regulationPlusOtWins").cast("int").alias("regulation_plus_ot_wins"),
        F.col("regulationWinPctg").cast("double").alias("regulation_win_pctg"),
        F.col("regulationWins").cast("int").alias("regulation_wins"),
        F.col("roadGamesPlayed").cast("int").alias("road_games_played"),
        F.col("roadGoalDifferential").cast("int").alias("road_goal_differential"),
        F.col("roadGoalsAgainst").cast("int").alias("road_goals_against"),
        F.col("roadGoalsFor").cast("int").alias("road_goals_for"),
        F.col("roadLosses").cast("int").alias("road_losses"),
        F.col("roadOtLosses").cast("int").alias("road_ot_losses"),
        F.col("roadPoints").cast("int").alias("road_points"),
        F.col("roadRegulationPlusOtWins").cast("int").alias("road_regulation_plus_ot_wins"),
        F.col("roadRegulationWins").cast("int").alias("road_regulation_wins"),
        F.col("roadTies").cast("int").alias("road_ties"),
        F.col("roadWins").cast("int").alias("road_wins"),
        F.col("shootoutLosses").cast("int").alias("shootout_losses"),
        F.col("shootoutWins").cast("int").alias("shootout_wins"),
        F.col("streakCode").alias("streak_code"),
        F.col("streakCount").cast("int").alias("streak_count"),
        F.col("teamLogo").alias("team_logo"),
        F.col("ties").cast("int").alias("ties"),
        F.col("waiversSequence").cast("int").alias("waivers_sequence"),
        F.col("wildcardSequence").cast("int").alias("wildcard_sequence"),
        F.col("winPctg").cast("double").alias("win_pctg"),
        F.col("wins").cast("int").alias("wins"),
        F.col("_source_load_datetime"),
        F.col("_source_is_deleted"),
        F.col("_source"),
        F.col("_batch_id"),
    )

    df = df.join(hub_teams, "team_business_id", "left") \
        .select(df["*"], hub_teams["team_id"], hub_teams["team_source_id"])

    write_table_to_pg(df, spark, "append", "dwh_operational.teams_stat")


def sat_teams_core(**kwargs):
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

    increment = read_table_from_pg(spark, "dwh_operational.teams_stat")
    increment = (
        increment.filter(F.col("_batch_id") == F.lit(current_date))
        .select(
            F.col("team_id"),
            F.col("team_name"),
            F.col("team_common_name"),
            F.col("conference_abbrev"),
            F.col("conference_name"),
            F.col("division_abbrev"),
            F.col("division_name"),
            F.col("place_name"),
            F.col("team_logo"),
            F.col("_source_is_deleted"),
            F.col("_source_load_datetime").alias("effective_from"),
            F.col("_source"),
        )
        .withColumn(
            "_data_hash",
            F.sha1(
                F.concat_ws(
                    "_",
                    F.col("team_name"),
                    F.col("team_common_name"),
                    F.col("conference_abbrev"),
                    F.col("conference_name"),
                    F.col("division_abbrev"),
                    F.col("division_name"),
                    F.col("place_name"),
                    F.col("team_logo"),
                    F.col("_source_is_deleted"),
                )
            ),
        )
    )

    try:
        sat_teams_core = read_table_from_pg(spark, "dwh_detailed.sat_teams_core")
        state = sat_teams_core.filter(F.col("is_active") == "False")

        active = (
            sat_teams_core.filter(F.col("is_active") == "True")
            .select(
                F.col("team_id"),
                F.col("team_name"),
                F.col("team_common_name"),
                F.col("conference_abbrev"),
                F.col("conference_name"),
                F.col("division_abbrev"),
                F.col("division_name"),
                F.col("place_name"),
                F.col("team_logo"),
                F.col("_source_is_deleted"),
                F.col("effective_from"),
                F.col("_source"),
                F.col("_data_hash"),
            )
            .unionByName(increment)
        )
    except:
        active = increment

    scd_window = Window.partitionBy("team_id").orderBy("effective_from")
    is_change = F.when(
        F.lag("_data_hash").over(scd_window) != F.col("_data_hash"), "True"
    ).otherwise("False")

    row_changes = active.select(
        F.col("team_id"),
        F.col("team_name"),
        F.col("team_common_name"),
        F.col("conference_abbrev"),
        F.col("conference_name"),
        F.col("division_abbrev"),
        F.col("division_name"),
        F.col("place_name"),
        F.col("team_logo"),
        F.col("effective_from"),
        F.coalesce(is_change.cast("string"), F.lit("True")).alias("is_change"),
        F.col("_data_hash"),
        F.col("_source_is_deleted"),
        F.col("_source"),
    )

    scd_window = Window.partitionBy("team_id").orderBy(
        F.col("effective_from").asc(), F.col("is_change").asc()
    )

    next_effective_from = F.lead("effective_from").over(scd_window)
    version_count = F.sum(F.when(F.col("is_change") == "True", 1).otherwise(0)).over(
        scd_window.rangeBetween(Window.unboundedPreceding, 0)
    )

    row_versions = row_changes.select(
        F.col("team_id"),
        F.col("team_name"),
        F.col("team_common_name"),
        F.col("conference_abbrev"),
        F.col("conference_name"),
        F.col("division_abbrev"),
        F.col("division_name"),
        F.col("place_name"),
        F.col("team_logo"),
        F.col("effective_from"),
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

    scd2 = row_versions.groupBy(
            F.col("team_id"),
            F.col("team_name"),
            F.col("team_common_name"),
            F.col("conference_abbrev"),
            F.col("conference_name"),
            F.col("division_abbrev"),
            F.col("division_name"),
            F.col("place_name"),
            F.col("team_logo"),
            F.col("_source_is_deleted"),
            F.col("_data_hash"),
            F.col("_version"),
        ).agg(
            F.min(F.col("effective_from")).alias("effective_from"),
            F.max(F.col("effective_to")).alias("effective_to"),
            F.min_by("_source", "effective_from").alias("_source"),
        ).withColumn(
            "is_active",
            F.when(F.col("effective_to") == "2040-01-01 00:00:00", "True").otherwise(
                "False"
            ),
        ).drop("_version") \
        .withColumn(
            "_version",
            F.sha1(F.concat_ws("_", F.col("_data_hash"), F.col("effective_from"))),
        )

    try:
        union = state.unionByName(scd2).orderBy("team_id", "effective_from")
    except:
        union = scd2.orderBy("team_id", "effective_from")

    write_table_to_pg(union, spark, "overwrite", "dwh_detailed.sat_teams_core")


def tl_teams_stat(**kwargs):
    spark = (
        SparkSession.builder.config(
            "spark.jars",
            "~/airflow_venv/lib/python3.10/site-packages/pyspark/jars/postgresql-42.3.1.jar",
        )
        .master("local[*]")
        .appName("parse_teams")
        .getOrCreate()
    )

    df = read_table_from_pg(spark, "dwh_operational.teams_stat").drop(
        "team_name",
        "team_common_name",
        "conference_abbrev",
        "conference_name",
        "division_abbrev",
        "division_name",
        "place_name",
        "team_logo",
    )

    window_spec = Window.partitionBy("team_id", "date").orderBy(
        F.col("_source_load_datetime").desc()
    )

    result_df = df.withColumn("rank", F.row_number().over(window_spec)) \
        .filter("rank = 1") \
        .drop("rank")

    result_df = result_df.dropDuplicates(["team_id", "date"]).orderBy("date", "team_id")

    write_table_to_pg(result_df, spark, "overwrite", "dwh_detailed.tl_teams_stat")


def pit_teams(**kwargs):
    spark = (
        SparkSession.builder.config(
            "spark.jars",
            "~/airflow_venv/lib/python3.10/site-packages/pyspark/jars/postgresql-42.3.1.jar",
        )
        .master("local[*]")
        .appName("parse_teams")
        .getOrCreate()
    )

    df_sat = read_table_from_pg(spark, "dwh_detailed.sat_teams_core")
    distinct_dates = df_sat.select(F.col("team_id"), F.col("effective_from")).distinct()

    window_spec = Window.partitionBy("team_id").orderBy("effective_from")
    effective_to = F.lead("effective_from").over(window_spec)

    date_grid = distinct_dates.withColumn(
        "effective_to",
        F.when(effective_to.isNull(), "2040-01-01 00:00:00").otherwise(effective_to),
    )

    data_versions = date_grid.join(
        df_sat,
        (date_grid.effective_from == df_sat.effective_from)
        & (date_grid.team_id == df_sat.team_id),
        "left",
    ).select(
        date_grid.team_id,
        date_grid.effective_from,
        date_grid.effective_to,
        F.when(date_grid.effective_to == "2040-01-01 00:00:00", True)
        .otherwise(False)
        .alias("is_active"),
        F.col("_version").alias("sat_teams_core_version"),
    )

    fill = (
        Window.partitionBy("team_id")
        .orderBy("effective_from")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )

    result = (
        data_versions.withColumn(
            "sat_teams_core_version",
            F.last("sat_teams_core_version", ignorenulls=True).over(fill),
        )
        .withColumn("is_active", F.when(F.col("is_active"), "True").otherwise("False"))
        .select(
            "team_id",
            "effective_from",
            "effective_to",
            "sat_teams_core_version",
            "is_active",
        )
        .orderBy("team_id", "effective_from")
    )

    write_table_to_pg(result, spark, "overwrite", "dwh_common.pit_teams")


def dm_teams(**kwargs):
    spark = (
        SparkSession.builder.config(
            "spark.jars",
            "~/airflow_venv/lib/python3.10/site-packages/pyspark/jars/postgresql-42.3.1.jar",
        )
        .master("local[*]")
        .appName("parse_teams")
        .getOrCreate()
    )

    df_hub = read_table_from_pg(spark, "dwh_detailed.hub_teams")
    df_sat = read_table_from_pg(spark, "dwh_detailed.sat_teams_core")
    df_pit = read_table_from_pg(spark, "dwh_common.pit_teams")

    df_dm = df_hub.join(df_pit, df_hub.team_id == df_pit.team_id, "inner") \
        .join(
            df_sat,
            (df_pit.sat_teams_core_version == df_sat._version)
            & (df_pit.team_id == df_sat.team_id),
            "left",
        ).select(
            df_hub.team_id,
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
            df_pit.is_active,
        ).orderBy("team_id", "effective_from")

    write_table_to_pg(df_dm, spark, "overwrite", "dwh_common.dm_teams")


task_get_teams_stat_to_source = PythonOperator(
    task_id="get_teams_stat_to_source",
    python_callable=get_teams_stat_to_source,
    dag=dag,
)

task_get_teama_stat_to_staging = PythonOperator(
    task_id="get_teama_stat_to_staging",
    python_callable=get_teama_stat_to_staging,
    dag=dag,
)

task_get_teama_stat_to_operational = PythonOperator(
    task_id="get_teama_stat_to_operational",
    python_callable=get_teama_stat_to_operational,
    dag=dag,
)

task_sat_teams_core = PythonOperator(
    task_id="sat_teams_core",
    python_callable=sat_teams_core,
    dag=dag,
)

task_tl_teams_stat = PythonOperator(
    task_id="tl_teams_stat",
    python_callable=tl_teams_stat,
    dag=dag,
)

task_pit_teams = PythonOperator(
    task_id="pit_teams",
    python_callable=pit_teams,
    dag=dag,
)

task_dm_teams = PythonOperator(
    task_id="dm_teams",
    python_callable=dm_teams,
    dag=dag,
)

task_get_teams_stat_to_source >> task_get_teama_stat_to_staging >> task_get_teama_stat_to_operational
task_get_teama_stat_to_operational >> [task_sat_teams_core, task_tl_teams_stat]
task_sat_teams_core >> task_pit_teams >> task_dm_teams