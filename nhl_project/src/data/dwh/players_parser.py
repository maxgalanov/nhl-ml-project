import pandas as pd
import requests
from datetime import timedelta, datetime

from airflow.models import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

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
    dag_id="nhl_players",
    schedule_interval="45 5 1 10 *",
    start_date=days_ago(2),
    catchup=False,
    tags=["hse_nhl_ml_project"],
    default_args=DEFAULT_ARGS,
    description="ETL process for getting list of NHL teams roaster",
)


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


def read_table_from_pg(spark, table_name):
    password = Variable.get("HSE_DB_PASSWORD")

    df_table = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://rc1b-diwt576i60sxiqt8.mdb.yandexcloud.net:6432/hse_db") \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", table_name) \
        .option("user", "maxglnv") \
        .option("password", password) \
        .load()

    return df_table


def write_table_to_pg(df, spark, write_mode, table_name):
    password = Variable.get("HSE_DB_PASSWORD")
    df.cache() 
    print("Initial count:", df.count())
    print("SparkContext active:", spark.sparkContext._jsc.sc().isStopped())

    try:
        df.write \
            .mode(write_mode) \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://rc1b-diwt576i60sxiqt8.mdb.yandexcloud.net:6432/hse_db") \
            .option("driver", "org.postgresql.Driver") \
            .option("dbtable", table_name) \
            .option("user", "maxglnv") \
            .option("password", password) \
            .save()
        print("Data written to PostgreSQL successfully")
    except Exception as e:
        print("Error during saving data to PostgreSQL:", e)

    print("Count after write:", df.count())
    df.unpersist()


def get_players_to_source(**kwargs):
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

    df_teams = (
        read_table_from_pg(spark, "dwh_detailed.hub_teams")
        .select(F.col("team_business_id"))
        .distinct()
    )
    teams_lst = df_teams.rdd.map(lambda x: x[0]).collect()
    teams_roster = pd.DataFrame()

    for code in teams_lst:
        try:
            team = get_information(f"/v1/roster/{code}/current")
            players_lst = []

            for key, value in team.items():
                players_lst.extend(value)

            df_team = pd.DataFrame(players_lst)
            df_team["triCodeCurrent"] = code

            teams_roster = pd.concat([teams_roster, df_team], ignore_index=True)
        except:
            continue

    teams_roster["firstName"] = teams_roster["firstName"].apply(
        lambda x: x.get("default", "") if type(x) == dict else ""
    )
    teams_roster["lastName"] = teams_roster["lastName"].apply(
        lambda x: x.get("default", "") if type(x) == dict else ""
    )
    teams_roster["birthCity"] = teams_roster["birthCity"].apply(
        lambda x: x.get("default", "") if type(x) == dict else ""
    )
    teams_roster["birthStateProvince"] = teams_roster["birthStateProvince"].apply(
        lambda x: x.get("default", "") if type(x) == dict else ""
    )

    dt = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df_teams_roster = spark.createDataFrame(teams_roster) \
        .withColumn("_source_load_datetime", F.lit(dt)) \
        .withColumn("_source", F.lit("API_NHL"))

    table_name = f"dwh_source.teams_roster_{current_date.replace('-', '_')}"
    write_table_to_pg(df_teams_roster, spark, "overwrite", table_name)

    data = [("dwh_source.teams_roster", dt)]
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


def get_players_to_staging(**kwargs):
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

    df_new = read_table_from_pg(spark, f"dwh_source.teams_roster_{current_date.replace('-', '_')}")
    df_new = df_new.withColumn("_source_is_deleted", F.lit("False"))

    prev_table_name = get_penultimate_table_name(spark, "dwh_source.teams_roster")

    if prev_table_name:
        df_prev = read_table_from_pg(spark, f"dwh_source.teams_roster_{prev_table_name[:10].replace('-', '_')}")
        df_prev = df_prev.withColumn("_source_is_deleted", F.lit("True"))

        df_deleted = df_prev.join(df_new, "id", "leftanti") \
            .withColumn(
                "_source_load_datetime",
                F.lit(df_new.select("_source_load_datetime").first()[0]),
            )

        df_changed = df_new.select(
            F.col("id"),
            F.col("headshot"),
            F.col("firstName"),
            F.col("lastName"),
            F.col("sweaterNumber"),
            F.col("positionCode"),
            F.col("shootsCatches"),
            F.col("heightInInches"),
            F.col("weightInPounds"),
            F.col("heightInCentimeters"),
            F.col("weightInKilograms"),
            F.col("birthDate"),
            F.col("birthCity"),
            F.col("birthCountry"),
            F.col("birthStateProvince"),
            F.col("triCodeCurrent"),
        ).subtract(
            df_prev.select(
                F.col("id"),
                F.col("headshot"),
                F.col("firstName"),
                F.col("lastName"),
                F.col("sweaterNumber"),
                F.col("positionCode"),
                F.col("shootsCatches"),
                F.col("heightInInches"),
                F.col("weightInPounds"),
                F.col("heightInCentimeters"),
                F.col("weightInKilograms"),
                F.col("birthDate"),
                F.col("birthCity"),
                F.col("birthCountry"),
                F.col("birthStateProvince"),
                F.col("triCodeCurrent"),
            )
        )
        df_changed = df_new.join(df_changed, "id", "inner").select(df_new["*"])

        df_final = df_changed.union(df_deleted) \
            .withColumn("_batch_id", F.lit(current_date))
    else:
        df_final = df_new.withColumn("_batch_id", F.lit(current_date))

    write_table_to_pg(df_final, spark, "overwrite", "dwh_staging.teams_roster")


def get_players_to_operational(**kwargs):
    spark = (
        SparkSession.builder.config(
            "spark.jars",
            "~/airflow_venv/lib/python3.10/site-packages/pyspark/jars/postgresql-42.3.1.jar",
        )
        .master("local[*]")
        .appName("parse_teams")
        .getOrCreate()
    )

    df = read_table_from_pg(spark, "dwh_staging.teams_roster")
    hub_teams = read_table_from_pg(spark, "dwh_detailed.hub_teams")

    df = df.select(
            F.col("id").alias("player_source_id"),
            F.col("headshot"),
            F.col("firstName").alias("first_name"),
            F.col("lastName").alias("last_name"),
            F.col("sweaterNumber").alias("sweater_number"),
            F.col("positionCode").alias("position_code"),
            F.col("shootsCatches").alias("shoots_catches"),
            F.col("heightInInches").alias("height_in_inches"),
            F.col("weightInPounds").alias("weight_in_pounds"),
            F.col("heightInCentimeters").alias("height_in_centimeters"),
            F.col("weightInKilograms").alias("weight_in_kilograms"),
            F.col("birthDate").alias("birth_date"),
            F.col("birthCity").alias("birth_city"),
            F.col("birthCountry").alias("birth_country"),
            F.col("birthStateProvince").alias("birth_state_province"),
            F.col("triCodeCurrent").alias("team_business_id"),
            F.col("_source_load_datetime"),
            F.col("_source_is_deleted"),
            F.col("_source"),
            F.col("_batch_id"),
        ).withColumn(
            "player_business_id",
            F.concat_ws("_", F.col("player_source_id"), F.col("_source")),
        ).withColumn("player_id", F.sha1(F.col("player_business_id")))

    df = df.join(hub_teams, "team_business_id", "left").select(
        df["*"], hub_teams["team_id"], hub_teams["team_source_id"]
    )

    write_table_to_pg(df, spark, "append", "dwh_operational.teams_roster")


def hub_players(**kwargs):
    current_date = kwargs["ds"]

    spark = (
        SparkSession.builder.master("local[*]").appName("teams_to_dwh").getOrCreate()
    )

    df_new = read_table_from_pg(spark, "dwh_operational.teams_roster").filter(
        F.col("_batch_id") == F.lit(current_date)
    )

    windowSpec = (
        Window.partitionBy("player_id")
        .orderBy(F.col("_source_load_datetime").desc())
        .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    )

    df_new = (
        df_new.withColumn(
            "_source_load_datetime", F.last("_source_load_datetime").over(windowSpec)
        )
        .withColumn("_source", F.last("_source").over(windowSpec))
        .select(
            F.col("player_id"),
            F.col("player_business_id"),
            F.col("player_source_id"),
            F.col("_source_load_datetime"),
            F.col("_source"),
        )
        .distinct()
    )

    try:
        df_old = read_table_from_pg(spark, "dwh_detailed.hub_players")

        df_new = df_new.join(df_old, "player_id", "leftanti")
        df_final = (
            df_new.union(df_old)
            .orderBy("_source_load_datetime", "player_id")
            .distinct()
        )
    except:
        df_final = df_new.orderBy("player_id", "_source_load_datetime").distinct()

    write_table_to_pg(df_final, spark, "overwrite", "dwh_detailed.hub_players")


def sat_players(**kwargs):
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
        read_table_from_pg(spark, "dwh_operational.teams_roster")
        .filter(F.col("_batch_id") == F.lit(current_date))
        .select(
            F.col("player_id"),
            F.col("team_business_id").alias("team_tri_code"),
            F.col("headshot"),
            F.col("first_name"),
            F.col("last_name"),
            F.col("sweater_number"),
            F.col("position_code"),
            F.col("shoots_catches"),
            F.col("height_in_inches"),
            F.col("weight_in_pounds"),
            F.col("height_in_centimeters"),
            F.col("weight_in_kilograms"),
            F.col("birth_date"),
            F.col("birth_city"),
            F.col("birth_country"),
            F.col("birth_state_province"),
            F.col("_source_is_deleted"),
            F.col("_source_load_datetime").alias("effective_from"),
            F.col("_source"),
        )
        .withColumn(
            "_data_hash",
            F.sha1(
                F.concat_ws(
                    "_",
                    F.col("team_tri_code"),
                    F.col("headshot"),
                    F.col("first_name"),
                    F.col("last_name"),
                    F.col("sweater_number"),
                    F.col("position_code"),
                    F.col("shoots_catches"),
                    F.col("height_in_inches"),
                    F.col("weight_in_pounds"),
                    F.col("height_in_centimeters"),
                    F.col("weight_in_kilograms"),
                    F.col("birth_date"),
                    F.col("birth_city"),
                    F.col("birth_country"),
                    F.col("birth_state_province"),
                    F.col("_source_is_deleted"),
                )
            ),
        )
    )

    try:
        sat_players = read_table_from_pg(spark, "dwh_detailed.sat_players")
        state = sat_players.filter(F.col("is_active") == "False")

        active = (
            sat_players.filter(F.col("is_active") == "True")
            .select(
                F.col("player_id"),
                F.col("team_tri_code"),
                F.col("headshot"),
                F.col("first_name"),
                F.col("last_name"),
                F.col("sweater_number"),
                F.col("position_code"),
                F.col("shoots_catches"),
                F.col("height_in_inches"),
                F.col("weight_in_pounds"),
                F.col("height_in_centimeters"),
                F.col("weight_in_kilograms"),
                F.col("birth_date"),
                F.col("birth_city"),
                F.col("birth_country"),
                F.col("birth_state_province"),
                F.col("_source_is_deleted"),
                F.col("effective_from"),
                F.col("_source"),
                F.col("_data_hash"),
            )
            .union(increment)
        )
    except:
        active = increment

    scd_window = Window.partitionBy("player_id").orderBy("effective_from")
    is_change = F.when(
        F.lag("_data_hash").over(scd_window) != F.col("_data_hash"), "True"
    ).otherwise("False")

    row_changes = active.select(
        F.col("player_id"),
        F.col("team_tri_code"),
        F.col("headshot"),
        F.col("first_name"),
        F.col("last_name"),
        F.col("sweater_number"),
        F.col("position_code"),
        F.col("shoots_catches"),
        F.col("height_in_inches"),
        F.col("weight_in_pounds"),
        F.col("height_in_centimeters"),
        F.col("weight_in_kilograms"),
        F.col("birth_date"),
        F.col("birth_city"),
        F.col("birth_country"),
        F.col("birth_state_province"),
        F.col("effective_from"),
        F.coalesce(is_change.cast("string"), F.lit("True")).alias("is_change"),
        F.col("_data_hash"),
        F.col("_source_is_deleted"),
        F.col("_source"),
    )

    scd_window = Window.partitionBy("player_id").orderBy(
        F.col("effective_from").asc(), F.col("is_change").asc()
    )

    next_effective_from = F.lead("effective_from").over(scd_window)
    version_count = F.sum(F.when(F.col("is_change") == "True", 1).otherwise(0)).over(
        scd_window.rangeBetween(Window.unboundedPreceding, 0)
    )

    row_versions = row_changes.select(
        F.col("player_id"),
        F.col("team_tri_code"),
        F.col("headshot"),
        F.col("first_name"),
        F.col("last_name"),
        F.col("sweater_number"),
        F.col("position_code"),
        F.col("shoots_catches"),
        F.col("height_in_inches"),
        F.col("weight_in_pounds"),
        F.col("height_in_centimeters"),
        F.col("weight_in_kilograms"),
        F.col("birth_date"),
        F.col("birth_city"),
        F.col("birth_country"),
        F.col("birth_state_province"),
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
            F.col("player_id"),
            F.col("team_tri_code"),
            F.col("headshot"),
            F.col("first_name"),
            F.col("last_name"),
            F.col("sweater_number"),
            F.col("position_code"),
            F.col("shoots_catches"),
            F.col("height_in_inches"),
            F.col("weight_in_pounds"),
            F.col("height_in_centimeters"),
            F.col("weight_in_kilograms"),
            F.col("birth_date"),
            F.col("birth_city"),
            F.col("birth_country"),
            F.col("birth_state_province"),
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
        union = state.union(scd2).orderBy("player_id", "effective_from")
    except:
        union = scd2.orderBy("player_id", "effective_from")

    write_table_to_pg(union, spark, "overwrite", "dwh_detailed.sat_players")


def pit_players(**kwargs):
    spark = (
        SparkSession.builder.config(
            "spark.jars",
            "~/airflow_venv/lib/python3.10/site-packages/pyspark/jars/postgresql-42.3.1.jar",
        )
        .master("local[*]")
        .appName("parse_teams")
        .getOrCreate()
    )

    df_sat = read_table_from_pg(spark, "dwh_detailed.sat_players")

    distinct_dates = df_sat.select(F.col("player_id"), F.col("effective_from")).distinct()

    window_spec = Window.partitionBy("player_id").orderBy("effective_from")
    effective_to = F.lead("effective_from").over(window_spec)

    date_grid = distinct_dates.withColumn(
        "effective_to",
        F.when(effective_to.isNull(), "2040-01-01 00:00:00").otherwise(effective_to),
    )

    data_versions = date_grid.join(
            df_sat.alias("df1"),
            (date_grid.effective_from == df_sat.effective_from)
            & (date_grid.player_id == df_sat.player_id),
            "left",
        ).select(
            date_grid.player_id,
            date_grid.effective_from,
            date_grid.effective_to,
            F.when(date_grid.effective_to == "2040-01-01 00:00:00", True)
            .otherwise(False)
            .alias("is_active"),
            F.col("df1._version").alias("sat_players_version"),
        )

    fill = (
        Window.partitionBy("player_id")
        .orderBy("effective_from")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )

    result = (
        data_versions.withColumn(
            "sat_players_version",
            F.last("sat_players_version", ignorenulls=True).over(fill),
        )
        .withColumn("is_active", F.when(F.col("is_active"), "True").otherwise("False"))
        .select(
            "player_id",
            "effective_from",
            "effective_to",
            "sat_players_version",
            "is_active",
        )
        .orderBy("player_id", "effective_from")
    )

    write_table_to_pg(result, spark, "overwrite", "dwh_common.pit_players")


def dm_players(**kwargs):
    spark = (
        SparkSession.builder.config(
            "spark.jars",
            "~/airflow_venv/lib/python3.10/site-packages/pyspark/jars/postgresql-42.3.1.jar",
        )
        .master("local[*]")
        .appName("parse_teams")
        .getOrCreate()
    )

    df_hub = read_table_from_pg(spark, "dwh_detailed.hub_players")
    df_sat = read_table_from_pg(spark, "dwh_detailed.sat_players")
    df_pit = read_table_from_pg(spark, "dwh_common.pit_players")

    df_dm = (
        df_hub.join(df_pit, df_hub.player_id == df_pit.player_id, "inner")
        .join(
            df_sat,
            (df_pit.sat_players_version == df_sat._version)
            & (df_pit.player_id == df_sat.player_id),
            "left",
        )
        .select(
            df_hub.player_id,
            df_hub.player_source_id,
            df_sat.headshot,
            df_sat.first_name,
            df_sat.last_name,
            df_sat.sweater_number,
            df_sat.position_code,
            df_sat.shoots_catches,
            df_sat.height_in_inches,
            df_sat.weight_in_pounds,
            df_sat.height_in_centimeters,
            df_sat.weight_in_kilograms,
            df_sat.birth_date,
            df_sat.birth_city,
            df_sat.birth_country,
            df_sat.birth_state_province,
            df_sat._source_is_deleted,
            df_pit.effective_from,
            df_pit.effective_to,
            df_pit.is_active,
        )
        .orderBy("player_id", "effective_from")
    )

    write_table_to_pg(df_dm, spark, "overwrite", "dwh_common.dm_players")


task_get_players_to_source = PythonOperator(
    task_id="get_players_to_source",
    python_callable=get_players_to_source,
    dag=dag,
)

task_get_players_to_staging = PythonOperator(
    task_id="get_players_to_staging",
    python_callable=get_players_to_staging,
    dag=dag,
)

task_get_players_to_operational = PythonOperator(
    task_id="get_players_to_operational",
    python_callable=get_players_to_operational,
    dag=dag,
)

task_hub_players = PythonOperator(
    task_id="hub_players",
    python_callable=hub_players,
    dag=dag,
)

task_sat_players = PythonOperator(
    task_id="sat_players",
    python_callable=sat_players,
    dag=dag,
)

task_pit_players = PythonOperator(
    task_id="pit_players",
    python_callable=pit_players,
    dag=dag,
)

task_dm_players = PythonOperator(
    task_id="dm_players",
    python_callable=dm_players,
    dag=dag,
)

task_get_players_to_source >> task_get_players_to_staging >> task_get_players_to_operational
task_get_players_to_operational >> [task_hub_players, task_sat_players]
task_sat_players >> task_pit_players >> task_dm_players
task_hub_players >> task_dm_players