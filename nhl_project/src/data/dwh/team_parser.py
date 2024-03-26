import pandas as pd
import requests
import os
from datetime import timedelta, datetime

from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, concat_ws, when, isnan, hash, sha1, coalesce, lag, lead, date_sub
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

SOURCE_PATH = "/Users/shiryaevva/HSE/2-nd_year/nhl-ml-project/dwh/source/"
STAGING_PATH = "/Users/shiryaevva/HSE/2-nd_year/nhl-ml-project/dwh/vault/staging/"
OPERATIONAL_PATH = "/Users/shiryaevva/HSE/2-nd_year/nhl-ml-project/dwh/vault/operational/"
DETAILED_PATH = "/Users/shiryaevva/HSE/2-nd_year/nhl-ml-project/dwh/vault/detailed/"
COMMON_PATH = "/Users/shiryaevva/HSE/2-nd_year/nhl-ml-project/dwh/vault/common/"


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


def get_teams_to_source(**kwargs):
    current_date = kwargs["ds"]

    spark = SparkSession.builder.master("local[*]").appName("parse_teams").getOrCreate()

    dt = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    data_teams = get_information("en/team", "https://api.nhle.com/stats/rest/")
    df_teams_pd = pd.DataFrame(data_teams["data"])

    df_teams = spark.createDataFrame(df_teams_pd)\
        .withColumn("_source_load_datetime", lit(dt))\
        .withColumn("_source", lit("API_NHL"))

    df_teams.repartition(1).write.mode("overwrite").parquet(SOURCE_PATH + f"teams/{current_date}")
    

def get_penultimate_file_name(directory):
    files = os.listdir(directory)
    files = [f for f in files if os.path.isdir(os.path.join(directory, f))]
    
    if len(files) < 2:
        return None
    
    files.sort(key=lambda x: os.path.getmtime(os.path.join(directory, x)), reverse=True)
    penultimate_file = files[1]
    
    return penultimate_file


def get_teams_to_staging(**kwargs):
    current_date = kwargs["ds"]

    spark = SparkSession.builder.master("local[*]").appName("parse_teams").getOrCreate()

    df_new = spark.read.parquet(SOURCE_PATH + f"teams/{current_date}")
    df_new = df_new.withColumn("_source_is_deleted", lit("False"))

    prev_file_name = get_penultimate_file_name(SOURCE_PATH + "teams")

    if prev_file_name:
        df_prev = spark.read.parquet(SOURCE_PATH + f"teams/{prev_file_name}")
        df_prev = df_prev.withColumn("_source_is_deleted", lit("True"))
        
        df_deleted = df_prev.join(df_new, "id", "leftanti")\
                            .withColumn("_source_load_datetime", 
                                        lit(df_new.select("_source_load_datetime").first()[0]))
        
        df_changed = df_new.select(col("id"),
                                   col("fullName"),
                                   col("triCode"))\
                            .subtract(df_prev.select(col("id"),
                                                     col("fullName"),
                                                     col("triCode")))
        df_changed = df_new.join(df_changed, "id", "inner").select(df_new["*"])

        df_final = df_changed.union(df_deleted)\
                                        .withColumn("_batch_id", lit(current_date))
    else:
        df_final = df_new.withColumn("_batch_id", lit(current_date))

    df_final.repartition(1).write.mode("overwrite").parquet(STAGING_PATH + f"teams/{current_date}")
    

def get_teams_to_operational(**kwargs):
    current_date = kwargs["ds"]

    spark = SparkSession.builder.master("local[*]").appName("parse_teams").getOrCreate()

    df = spark.read.parquet(STAGING_PATH + f"teams/{current_date}")
    df = df.select(col("id").alias("team_source_id"),
                    col("fullName").alias("team_full_name"),
                    col("triCode").alias("team_business_id"),
                    
                    col("_source_load_datetime"),
                    col("_source_is_deleted"),
                    col("_source"),
                    col("_batch_id"))\
            .withColumn("team_id", sha1(concat_ws("_", col("team_source_id"), col("_source"))))
    
    df.repartition(1).write.mode("append").parquet(OPERATIONAL_PATH + f"teams")
    

def hub_teams(**kwargs):
    current_date = kwargs["ds"]

    spark = (
        SparkSession.builder.master("local[*]").appName("teams_to_dwh").getOrCreate()
    )

    df_new = spark.read.parquet(OPERATIONAL_PATH + f"teams")\
        .filter(col("_batch_id") == lit(current_date))\
        .select(col("team_id"),
                col("team_business_id"),
                col("team_source_id"),

                col("_source_load_datetime"),
                col("_source"))

    try:
        df_old = spark.read.parquet(DETAILED_PATH + f"hub_teams")

        df_new = df_new.join(df_old, "team_id", "leftanti")
        df_final = df_new.union(df_old).orderBy("_source_load_datetime", "team_id")
    except pyspark.errors.AnalysisException:
        df_final = df_new.orderBy("_source_load_datetime", "team_id")

    df_final.repartition(1).write.mode("overwrite").parquet(DETAILED_PATH + f"hub_teams")


