import pandas as pd
import requests
import os
from datetime import timedelta, datetime

from airflow.models import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
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

    postgres_hook = PostgresHook(postgres_conn_id='hse_postgres')
    connection = postgres_hook.get_connection('hse_postgres')

    jdbc_url = f"jdbc:postgresql://{connection.host}:{connection.port}/{connection.schema}"
    properties = {
        "user": connection.login,
        "password": connection.password
    }

    spark = (
        SparkSession.builder
        .config("spark.jars", "~/airflow_venv/lib/python3.10/site-packages/pyspark/jars/postgresql-42.3.1.jar")
        .master("local[*]")
        .appName("parse_teams")
        .getOrCreate()
    )

    dt = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    data_teams = get_information("en/team", "https://api.nhle.com/stats/rest/")
    df_teams_pd = pd.DataFrame(data_teams["data"])

    df_teams = (
        spark.createDataFrame(df_teams_pd)
        .withColumn("_source_load_datetime", F.lit(dt))
        .withColumn("_source", F.lit("API_NHL"))
    )

    df_teams.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", f"dwh_source.teams_{current_date}") \
        .option("properties", properties) \
        .mode("overwrite") \
        .save()
    
    create_metadata_table_sql = """
    CREATE TABLE IF NOT EXISTS public.metadata_table (
        table_name VARCHAR(255),
        updated_at TIMESTAMP
    );
    """
    postgres_hook.run(create_metadata_table_sql)

    insert_metadata_sql = f"""
    INSERT INTO public.metadata_table (table_name, updated_at)
    VALUES ('dwh_source.teams', '{dt}');
    """
    postgres_hook.run(insert_metadata_sql)


def get_penultimate_table_name(postgres_hook, table_name):
    penultimate_table_query = f"""
    SELECT updated_at
    FROM public.metadata_table
    WHERE table_name = '{table_name}'
    ORDER BY updated_at DESC
    OFFSET 1 LIMIT 1;
    """

    connection = postgres_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(penultimate_table_query)
    result = cursor.fetchone()
    cursor.close()
    connection.close()

    return result[0] if result else None


def get_teams_to_staging(**kwargs):
    current_date = kwargs["ds"]

    postgres_hook = PostgresHook(postgres_conn_id='your_connection_id')
    connection = postgres_hook.get_connection('your_connection_id')

    jdbc_url = f"jdbc:postgresql://{connection.host}:{connection.port}/{connection.schema}"
    properties = {
        "user": connection.login,
        "password": connection.password
    }

    spark = (
        SparkSession.builder
        .config("spark.jars", "~/airflow_venv/lib/python3.10/site-packages/pyspark/jars/postgresql-42.3.1.jar")
        .master("local[*]")
        .appName("parse_teams")
        .getOrCreate()
    )

    df_new = spark.read \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", f"dwh_source.teams_{current_date}") \
        .option("properties", properties) \
        .load()

    df_new = df_new.withColumn("_source_is_deleted", F.lit("False"))

    prev_table_date = get_penultimate_table_name(postgres_hook, 'dwh_source.teams')

    if prev_table_date:

        df_prev = spark.read \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", f"dwh_source.teams_{prev_table_date[:10]}") \
            .option("properties", properties) \
            .load()

        df_prev = df_prev.withColumn("_source_is_deleted", F.lit("True"))

        df_deleted = df_prev.join(df_new, "id", "leftanti").withColumn(
            "_source_load_datetime",
            F.lit(df_new.select("_source_load_datetime").first()[0]),
        )

        df_changed = df_new.select(
            F.col("id"), F.col("fullName"), F.col("triCode")
        ).subtract(df_prev.select(F.col("id"), F.col("fullName"), F.col("triCode")))
        df_changed = df_new.join(df_changed, "id", "inner").select(df_new["*"])

        df_final = df_changed.union(df_deleted).withColumn(
            "_batch_id", F.lit(current_date)
        )
    else:
        df_final = df_new.withColumn("_batch_id", F.lit(current_date))

    df_final.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", f"dwh_staging.teams_{current_date}") \
        .option("properties", properties) \
        .mode("overwrite") \
        .save()


def get_teams_to_operational(**kwargs):
    current_date = kwargs["ds"]

    postgres_hook = PostgresHook(postgres_conn_id='your_connection_id')
    connection = postgres_hook.get_connection('your_connection_id')

    jdbc_url = f"jdbc:postgresql://{connection.host}:{connection.port}/{connection.schema}"
    properties = {
        "user": connection.login,
        "password": connection.password
    }

    spark = (
        SparkSession.builder
        .config("spark.jars", "~/airflow_venv/lib/python3.10/site-packages/pyspark/jars/postgresql-42.3.1.jar")
        .master("local[*]")
        .appName("parse_teams")
        .getOrCreate()
    )

    df = spark.read \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", f"dwh_staging.teams_{current_date}") \
        .option("properties", properties) \
        .load()

    df = df.select(
        F.col("id").alias("team_source_id"),
        F.col("fullName").alias("team_full_name"),
        F.col("triCode").alias("team_business_id"),
        F.col("_source_load_datetime"),
        F.col("_source_is_deleted"),
        F.col("_source"),
        F.col("_batch_id"),
    ).withColumn(
        "team_id", F.sha1(F.concat_ws("_", F.col("team_source_id"), F.col("_source")))
    )

    df.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", "dwh_operational.teams") \
        .option("properties", properties) \
        .mode("append") \
        .save()


def hub_teams(**kwargs):
    current_date = kwargs["ds"]

    postgres_hook = PostgresHook(postgres_conn_id='your_connection_id')
    connection = postgres_hook.get_connection('your_connection_id')

    jdbc_url = f"jdbc:postgresql://{connection.host}:{connection.port}/{connection.schema}"
    properties = {
        "user": connection.login,
        "password": connection.password
    }

    spark = (
        SparkSession.builder
        .config("spark.jars", "~/airflow_venv/lib/python3.10/site-packages/pyspark/jars/postgresql-42.3.1.jar")
        .master("local[*]")
        .appName("parse_teams")
        .getOrCreate()
    )

    df_new = spark.read \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", "dwh_operational.teams") \
        .option("properties", properties) \
        .load()

    df_new = df_new.filter(
        F.col("_batch_id") == F.lit(current_date)
    )

    windowSpec = (
        Window.partitionBy("team_id")
        .orderBy(F.col("_source_load_datetime").desc())
        .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    )

    df_new = (
        df_new.withColumn(
            "_source_load_datetime", F.last("_source_load_datetime").over(windowSpec)
        )
        .withColumn("_source", F.last("_source").over(windowSpec))
        .select(
            F.col("team_id"),
            F.col("team_business_id"),
            F.col("team_source_id"),
            F.col("_source_load_datetime"),
            F.col("_source"),
        )
    )

    try:
        df_old = spark.read \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", "dwh_detailed.hub_teams") \
            .option("properties", properties) \
            .load()

        df_new = df_new.join(df_old, "team_id", "leftanti")
        df_final = df_new.union(df_old).orderBy("_source_load_datetime", "team_id")
    except pyspark.errors.AnalysisException:
        df_final = df_new.orderBy("_source_load_datetime", "team_id")

    df_final.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", "dwh_detailed.hub_teams") \
        .option("properties", properties) \
        .mode("overwrite") \
        .save()


task_get_teams_to_source = PythonOperator(
    task_id="get_teams_to_source",
    python_callable=get_teams_to_source,
    dag=dag,
)

task_get_teams_to_staging = PythonOperator(
    task_id="get_teams_to_staging",
    python_callable=get_teams_to_staging,
    dag=dag,
)

task_get_teams_to_operational = PythonOperator(
    task_id="get_teams_to_operational",
    python_callable=get_teams_to_operational,
    dag=dag,
)

task_hub_teams = PythonOperator(
    task_id="hub_teams",
    python_callable=hub_teams,
    dag=dag,
)

task_get_teams_to_source >> task_get_teams_to_staging >> task_get_teams_to_operational
task_get_teams_to_operational >> task_hub_teams
