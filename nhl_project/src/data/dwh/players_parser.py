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
    description="ETL process for getting list of NHL teams roaster",
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


def get_players_to_source(**kwargs):
    current_date = kwargs["ds"]

    spark = SparkSession.builder.master("local[*]").appName("parse_teams").getOrCreate()

    df_teams = spark.read.parquet(DETAILED_PATH + f"hub_teams").select("team_business_id")
    teams_lst = df_teams.distinct().rdd.map(lambda x: x[0]).collect()
    teams_roster = pd.DataFrame()

    for code in teams_lst:
        try:
            team = get_information(f'/v1/roster/{code}/current')
            players_lst = []
            
            for key, value in team.items():
                players_lst.extend(value)

            df_team = pd.DataFrame(players_lst)
            df_team['triCodeCurrent'] = code

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
    df_teams_roster = spark.createDataFrame(teams_roster)\
        .withColumn("_source_load_datetime", lit(dt))\
        .withColumn("_source", lit("API_NHL"))

    df_teams_roster.repartition(1).write.mode("overwrite").parquet(SOURCE_PATH + f"teams_roster/{current_date}")
    

def get_penultimate_file_name(directory):
    files = os.listdir(directory)
    files = [f for f in files if os.path.isdir(os.path.join(directory, f))]
    
    if len(files) < 2:
        return None
    
    files.sort(key=lambda x: os.path.getmtime(os.path.join(directory, x)), reverse=True)
    penultimate_file = files[1]
    
    return penultimate_file


def get_players_to_staging(**kwargs):
    current_date = kwargs["ds"]

    spark = SparkSession.builder.master("local[*]").appName("parse_teams").getOrCreate()

    df_new = spark.read.parquet(SOURCE_PATH + f"teams_roster/{current_date}")
    df_new = df_new.withColumn("_source_is_deleted", lit("False"))

    prev_file_name = get_penultimate_file_name(SOURCE_PATH + "teams_roster")

    if prev_file_name:
        df_prev = spark.read.parquet(SOURCE_PATH + f"teams_roster/{prev_file_name}")
        df_prev = df_prev.withColumn("_source_is_deleted", lit("True"))
        
        df_deleted = df_prev.join(df_new, "id", "leftanti")\
                                    .withColumn("_source_load_datetime", 
                                                lit(df_new.select("_source_load_datetime").first()[0]))
        
        df_changed = df_new.select(col("id"),
                                col("headshot"),
                                col("firstName"),
                                col("lastName"),
                                col("sweaterNumber"),
                                col("positionCode"),
                                col("shootsCatches"),
                                col("heightInInches"),
                                col("weightInPounds"),
                                col("heightInCentimeters"),
                                col("weightInKilograms"),
                                col("birthDate"),
                                col("birthCity"),
                                col("birthCountry"),
                                col("birthStateProvince"),
                                col("triCodeCurrent"))\
                            .subtract(df_prev.select(col("id"),
                                                    col("headshot"),
                                                    col("firstName"),
                                                    col("lastName"),
                                                    col("sweaterNumber"),
                                                    col("positionCode"),
                                                    col("shootsCatches"),
                                                    col("heightInInches"),
                                                    col("weightInPounds"),
                                                    col("heightInCentimeters"),
                                                    col("weightInKilograms"),
                                                    col("birthDate"),
                                                    col("birthCity"),
                                                    col("birthCountry"),
                                                    col("birthStateProvince"),
                                                    col("triCodeCurrent")))
        df_changed = df_new.join(df_changed, "id", "inner").select(df_new["*"])

        df_final = df_changed.union(df_deleted)\
                            .withColumn("_batch_id", lit(current_date))
    else:
        df_final = df_new.withColumn("_batch_id", lit(current_date))

    df_final.repartition(1).write.mode("overwrite").parquet(STAGING_PATH + f"teams_roster/{current_date}")


def get_players_to_operational(**kwargs):
    current_date = kwargs["ds"]

    spark = SparkSession.builder.master("local[*]").appName("parse_teams").getOrCreate()

    df = spark.read.parquet(STAGING_PATH + f"teams_roster/{current_date}")
    hub_teams = spark.read.parquet(DETAILED_PATH + f"hub_teams")

    df = df.select(col("id").alias("player_source_id"),
                    col("headshot"),
                    col("firstName").alias("first_name"),
                    col("lastName").alias("last_name"),
                    col("sweaterNumber").alias("sweater_number"),
                    col("positionCode").alias("position_code"),
                    col("shootsCatches").alias("shoots_catches"),
                    col("heightInInches").alias("height_in_inches"),
                    col("weightInPounds").alias("weight_in_pounds"),
                    col("heightInCentimeters").alias("height_in_centimeters"),
                    col("weightInKilograms").alias("weight_in_kilograms"),
                    col("birthDate").alias("birth_date"),
                    col("birthCity").alias("birth_city"),
                    col("birthCountry").alias("birth_country"),
                    col("birthStateProvince").alias("birth_state_province"),
                    col("triCodeCurrent").alias("team_business_id"),

                    col("_source_load_datetime"),
                    col("_source_is_deleted"),
                    col("_source"),
                    col("_batch_id"))\
                .withColumn("player_business_id", concat_ws("_", col("player_source_id"), col("_source")))\
                .withColumn("player_id", sha1(col("player_business_id")))

    df = df.join(hub_teams, "team_business_id", "left")\
                .select(df["*"], hub_teams["team_id"], hub_teams["team_source_id"])
    
    df.repartition(1).write.mode("append").parquet(OPERATIONAL_PATH + f"teams_roster")


def hub_players(**kwargs):
    current_date = kwargs["ds"]

    spark = SparkSession.builder.master("local[*]").appName("teams_to_dwh").getOrCreate()

    df_new = spark.read.parquet(OPERATIONAL_PATH + f"teams_roster")\
        .filter(col("_batch_id") == lit(current_date))\
        .select(col("player_id"),
                col("player_business_id"),
                col("player_source_id"),

                col("_source_load_datetime"),
                col("_source"))

    try:
        df_old = spark.read.parquet(DETAILED_PATH + f"hub")

        df_new = df_new.join(df_old, "player_id", "leftanti")
        df_final = df_new.union(df_old).orderBy("_source_load_datetime", "player_id")
    except pyspark.errors.AnalysisException:
        df_final = df_new.orderBy("_source_load_datetime", "player_id")

    df_final.repartition(1).write.mode("overwrite").parquet(DETAILED_PATH + f"hub_players")


def sat_players(**kwargs):
    current_date = kwargs["ds"]

    spark = SparkSession.builder.master("local[*]").appName("teams_to_dwh").getOrCreate()

    increment = spark.read.parquet(OPERATIONAL_PATH + f"teams_roster")\
        .filter(col("_batch_id") == lit(current_date))\
        .select(col("player_id"),
                col("team_business_id").alias("team_tri_code"),
                col("headshot"),
                col("first_name"),
                col("last_name"),
                col("sweater_number"),
                col("position_code"),
                col("shoots_catches"),
                col("height_in_inches"),
                col("weight_in_pounds"),
                col("height_in_centimeters"),
                col("weight_in_kilograms"),
                col("birth_date"),
                col("birth_city"),
                col("birth_country"),
                col("birth_state_province"),

                col("_source_is_deleted"),
                col("_source_load_datetime").alias("effective_from"),
                col("_source"))\
        .withColumn("_data_hash", sha1(concat_ws("_", col("team_tri_code"),
                                            col("headshot"),
                                            col("first_name"),
                                            col("last_name"),
                                            col("sweater_number"),
                                            col("position_code"),
                                            col("shoots_catches"),
                                            col("height_in_inches"),
                                            col("weight_in_pounds"),
                                            col("height_in_centimeters"),
                                            col("weight_in_kilograms"),
                                            col("birth_date"),
                                            col("birth_city"),
                                            col("birth_country"),
                                            col("birth_state_province"),
                                            col("_source_is_deleted"))))
    
    try:    
        sat_players = spark.read.parquet(DETAILED_PATH + f"sat_players")
        state = sat_players.filter(col("is_active") == "False")

        active = sat_players.filter(col("is_active") == "True")\
            .select(col("player_id"),
                    col("team_tri_code"),
                    col("headshot"),
                    col("first_name"),
                    col("last_name"),
                    col("sweater_number"),
                    col("position_code"),
                    col("shoots_catches"),
                    col("height_in_inches"),
                    col("weight_in_pounds"),
                    col("height_in_centimeters"),
                    col("weight_in_kilograms"),
                    col("birth_date"),
                    col("birth_city"),
                    col("birth_country"),
                    col("birth_state_province"),

                    col("_source_is_deleted"),
                    col("effective_from"),
                    col("_source"),
                    col("_data_hash")) \
            .union(increment)
    except pyspark.errors.AnalysisException:
        active = increment


    scd_window = Window.partitionBy("player_id").orderBy("effective_from")
    is_change = when(lag("_data_hash").over(scd_window) != col("_data_hash"), "True").otherwise("False")

    row_changes = active.select(
        col("player_id"),
        col("team_tri_code"),
        col("headshot"),
        col("first_name"),
        col("last_name"),
        col("sweater_number"),
        col("position_code"),
        col("shoots_catches"),
        col("height_in_inches"),
        col("weight_in_pounds"),
        col("height_in_centimeters"),
        col("weight_in_kilograms"),
        col("birth_date"),
        col("birth_city"),
        col("birth_country"),
        col("birth_state_province"),

        col("effective_from"),
        coalesce(is_change.cast("string"), lit("True")).alias("is_change"),
        col("_data_hash"),
        col("_source_is_deleted"),
        col("_source")
    )


    scd_window = Window.partitionBy("player_id").orderBy(col("effective_from").asc(), col("is_change").asc())

    next_effective_from = lead("effective_from").over(scd_window)
    version_count = F.sum(when(col("is_change") == "True", 1).otherwise(0)).over(scd_window.rangeBetween(Window.unboundedPreceding, 0))

    row_versions = row_changes.select(
                    col("player_id"),
                    col("team_tri_code"),
                    col("headshot"),
                    col("first_name"),
                    col("last_name"),
                    col("sweater_number"),
                    col("position_code"),
                    col("shoots_catches"),
                    col("height_in_inches"),
                    col("weight_in_pounds"),
                    col("height_in_centimeters"),
                    col("weight_in_kilograms"),
                    col("birth_date"),
                    col("birth_city"),
                    col("birth_country"),
                    col("birth_state_province"),

                    col("effective_from"),
                    next_effective_from.cast("string").alias("effective_to"),
                    version_count.alias("_version"),
                    "_data_hash",
                    "_source_is_deleted",
                    "_source"
                )\
        .withColumn("effective_to", coalesce(expr("effective_to - interval 1 second"), lit("2040-01-01 00:00:00")))


    scd2 = row_versions.groupBy(
            col("player_id"),
            col("team_tri_code"),
            col("headshot"),
            col("first_name"),
            col("last_name"),
            col("sweater_number"),
            col("position_code"),
            col("shoots_catches"),
            col("height_in_inches"),
            col("weight_in_pounds"),
            col("height_in_centimeters"),
            col("weight_in_kilograms"),
            col("birth_date"),
            col("birth_city"),
            col("birth_country"),
            col("birth_state_province"),

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
        union = state.union(scd2).orderBy("player_id", "effective_from")
    except:
        union = scd2.orderBy("player_id", "effective_from")

    union.repartition(1).write.mode("overwrite").parquet(DETAILED_PATH + f"sat_players")


def el_teams_roaster(**kwargs):
    current_date = kwargs["ds"]

    spark = SparkSession.builder.master("local[*]").appName("teams_to_dwh").getOrCreate()

    increment = spark.read.parquet(OPERATIONAL_PATH + f"teams_roster")\
        .filter(col("_batch_id") == lit(current_date))\
        .select(col("player_id"),
                col("team_id"),

                col("_source_is_deleted"),
                col("_source_load_datetime").alias("effective_from"),
                col("_source"))\
        .withColumn("_data_hash", sha1(concat_ws("_", col("team_id"),
                                                    col("_source_is_deleted"))))
    
    try:    
        el_teams_roaster = spark.read.parquet(DETAILED_PATH + f"el_teams_roaster")
        state = el_teams_roaster.filter(col("is_active") == "False")

        active = el_teams_roaster.filter(col("is_active") == "True")\
            .select(col("player_id"),
                    col("team_id"),

                    col("_source_is_deleted"),
                    col("effective_from"),
                    col("_source"),
                    col("_data_hash"))\
            .union(increment)
    except:
        active = increment

    scd_window = Window.partitionBy("player_id").orderBy("effective_from")
    is_change = when(lag("_data_hash").over(scd_window) != col("_data_hash"), "True").otherwise("False")

    row_changes = active.select(
        col("player_id"),
        col("team_id"),

        col("effective_from"),
        coalesce(is_change.cast("string"), lit("True")).alias("is_change"),
        col("_data_hash"),
        col("_source_is_deleted"),
        col("_source")
    )


    scd_window = Window.partitionBy("player_id").orderBy(col("effective_from").asc(), col("is_change").asc())

    next_effective_from = lead("effective_from").over(scd_window)
    version_count = F.sum(when(col("is_change") == "True", 1).otherwise(0)).over(scd_window.rangeBetween(Window.unboundedPreceding, 0))

    row_versions = row_changes.select(
        col("player_id"),
        col("team_id"),
        col("effective_from"),
        next_effective_from.cast("string").alias("effective_to"),
        version_count.alias("_version"),
        "_data_hash",
        "_source_is_deleted",
        "_source"
    ).withColumn("effective_to", coalesce(expr("effective_to - interval 1 second"), lit("2040-01-01 00:00:00")))

    scd2 = row_versions.groupBy(
        col("player_id"),
        col("team_id"),

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
        union = state.union(scd2).orderBy("player_id", "effective_from")
    except:
        union = scd2.orderBy("player_id", "effective_from")

    union.repartition(1).write.mode("overwrite").parquet(DETAILED_PATH + f"el_teams_roaster")


def pit_players(**kwargs):
    current_date = kwargs["ds"]

    spark = SparkSession.builder.master("local[*]").appName("teams_to_dwh").getOrCreate()

    df_sat = spark.read.parquet(DETAILED_PATH + f"sat_players")
    df_el = spark.read.parquet(DETAILED_PATH + f"el_teams_roaster")

    distinct_dates = df_sat.select(col("player_id"), col("effective_from"))\
        .union(df_el.select(col("player_id"), col("effective_from")))\
        .distinct()
    
    window_spec = Window.partitionBy("player_id").orderBy("effective_from")
    effective_to = lead("effective_from").over(window_spec)

    date_grid = distinct_dates.withColumn("effective_to",
        when(effective_to.isNull(), "2040-01-01 00:00:00").otherwise(effective_to)
    )

    data_versions = date_grid.join(df_sat.alias("df1"), (date_grid.effective_from == df_sat.effective_from) & (date_grid.player_id == df_sat.player_id), "left")\
        .join(df_el.alias("df2"), (date_grid.effective_from == df_el.effective_from) & (date_grid.player_id == df_el.player_id), "left")\
        .select(
            date_grid.player_id,
            date_grid.effective_from,
            date_grid.effective_to,
            when(date_grid.effective_to == "2040-01-01 00:00:00", True).otherwise(False).alias("is_active"),
            col("df1._version").alias("sat_players_version"),
            col("df2._version").alias("el_teams_roaster_version")
        )
    

    fill = Window.partitionBy("player_id").orderBy("effective_from").rowsBetween(Window.unboundedPreceding, Window.currentRow)

    result = data_versions.withColumn("sat_players_version", last("sat_players_version", ignorenulls=True).over(fill)) \
        .withColumn("el_teams_roaster_version", last("el_teams_roaster_version", ignorenulls=True).over(fill)) \
        .withColumn("is_active", when(col("is_active"), "True").otherwise("False")) \
        .select("player_id",
                "effective_from",
                "effective_to",
                "sat_players_version",
                "el_teams_roaster_version",
                "is_active"
        ).orderBy("player_id", "effective_from")
    
    result.repartition(1).write.mode("overwrite").parquet(COMMON_PATH + f"pit_players")


def dm_players(**kwargs):
    current_date = kwargs["ds"]

    spark = SparkSession.builder.master("local[*]").appName("teams_to_dwh").getOrCreate()

    df_hub = spark.read.parquet(DETAILED_PATH + f"hub_players")
    df_hub_teams = spark.read.parquet(DETAILED_PATH + f"hub_teams")
    df_sat = spark.read.parquet(DETAILED_PATH + f"sat_players")
    df_el = spark.read.parquet(DETAILED_PATH + f"el_teams_roaster")
    df_pit = spark.read.parquet(COMMON_PATH + f"pit_players")


    df_dm = df_hub.join(df_pit, df_hub.player_id == df_pit.player_id, "inner")\
        .join(df_sat, (df_pit.sat_players_version == df_sat._version) & (df_pit.player_id == df_sat.player_id), "left")\
        .join(df_el, (df_pit.el_teams_roaster_version == df_el._version) & (df_pit.player_id == df_el.player_id), "left")\
        .join(df_hub_teams, df_hub_teams.team_id == df_el.team_id, "left")\
        .select(df_hub.player_id,
                df_hub.player_source_id,
                df_hub_teams.team_business_id,

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
                df_pit.is_active
                )\
            .orderBy("player_id", "effective_from")
    
    df_dm.repartition(1).write.mode("overwrite").parquet(COMMON_PATH + f"dm_players")

