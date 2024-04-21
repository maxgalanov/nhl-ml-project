import pandas as pd
import requests
from datetime import datetime

from airflow.models import Variable
from sqlalchemy import create_engine
from contextlib import contextmanager


@contextmanager
def get_time():
    """
    Генератор для измерения времени выполнения кода внутри блока `with`.
    """
    try:
        start_time = datetime.now()
        print(f"Started at: {start_time}")
        yield
    finally:
        end_time = datetime.now()
        print(f"Ended at: {end_time}")
        print(f"Duration: {end_time - start_time}", end="\n\n")


def get_information(endpoint, base_url="https://api-web.nhle.com"):
    """
    Отправляет GET запрос к заданному API-эндпоинту и возвращает данные в формате JSON.

    Параметры:
    endpoint (str): Эндпоинт API, к которому производится запрос (например, '/players/stats').
    base_url (str): Базовый URL API (по умолчанию "https://api-web.nhle.com").

    Возвращает:
    dict: Данные, полученные от API в формате JSON, если запрос успешен.
    None: Возвращает None, если запрос не удался.
    """
    with get_time():
        base_url = f"{base_url}"
        endpoint = f"{endpoint}"
        full_url = f"{base_url}{endpoint}"

        response = requests.get(full_url)

        if response.status_code == 200:
            player_data = response.json()
            return player_data
        else:
            print(f"Error: Unable to fetch data. Status code: {response.status_code}")


def read_table_from_pg(
    spark,
    table_name,
    username="maxglnv",
    password=Variable.get("HSE_DB_PASSWORD"),
    host="rc1b-diwt576i60sxiqt8.mdb.yandexcloud.net",
    port="6432",
    database="hse_db",
):
    """
    Загружает таблицу из PostgreSQL в Spark DataFrame.

    Параметры:
    spark (SparkSession): Сессия Spark, используемая для выполнения операций.
    table_name (str): Имя таблицы, которую необходимо загрузить из базы данных.

    Возвращает:
    pyspark.sql.DataFrame: Spark DataFrame, содержащий данные из указанной таблицы базы данных.
    """
    with get_time():
        db_url = f"jdbc:postgresql://{host}:{port}/{database}"
        try:
            df_table = (
                spark.read.format("jdbc")
                .option("url", db_url)
                .option("driver", "org.postgresql.Driver")
                .option("dbtable", table_name)
                .option("user", username)
                .option("password", password)
                .load()
            )

            print(
                f"Данные успешно загружены из таблицы {table_name} PostgreSQL в Spark DataFrame."
            )
            return df_table

        except Exception as e:
            raise Exception(
                f"Произошла ошибка при чтении таблицы {table_name} из базы данных: {e}"
            ) from e


def write_table_to_pg(
    df,
    spark,
    write_mode,
    table_name,
    username="maxglnv",
    password=Variable.get("HSE_DB_PASSWORD"),
    host="rc1b-diwt576i60sxiqt8.mdb.yandexcloud.net",
    port="6432",
    database="hse_db",
):
    """
    Записывает данные из Spark DataFrame в таблицу PostgreSQL.

    Параметры:
    df (pyspark.sql.DataFrame): DataFrame, который будет записан в базу данных.
    spark (SparkSession): Сессия Spark, используемая для выполнения операций.
    write_mode (str): Режим записи (например, 'append', 'overwrite', 'ignore', 'error').
    table_name (str): Имя целевой таблицы в базе данных, куда будут записаны данные.
    username (str): Имя пользователя для подключения к базе данных.
    password (str): Пароль пользователя.
    host (str): Хост, на котором расположена база данных.
    port (str): Порт для подключения к базе данных.
    database (str): Имя базы данных.
    """
    with get_time():
        db_url = f"jdbc:postgresql://{host}:{port}/{database}"
        df.cache()
        print("Количество строк:", df.count())

        try:
            df.write.mode(write_mode).format("jdbc").option("url", db_url).option(
                "driver", "org.postgresql.Driver"
            ).option("dbtable", table_name).option("user", username).option(
                "password", password
            ).save()
            print(
                f"Spark DataFrame успешно записан в PostgreSQL в таблицу {table_name}."
            )
        except Exception as e:
            raise Exception(
                f"Произошла ошибка при записи таблицы {table_name} в базу данных: {e}"
            ) from e

        print("Количество строк:", df.count())
        df.unpersist()


def write_df_to_pg(
    df,
    table_name,
    schema="public",
    username="maxglnv",
    password=Variable.get("HSE_DB_PASSWORD"),
    host="rc1b-diwt576i60sxiqt8.mdb.yandexcloud.net",
    port="6432",
    database="hse_db",
):
    """
    Записывает DataFrame в таблицу PostgreSQL.

    Параметры:
    df (pandas.DataFrame): DataFrame для записи.
    table_name (str): Имя таблицы для записи данных.
    schema (str): Схема базы данных, в которой находится таблица.
    username (str): Имя пользователя для подключения к базе данных.
    password (str): Пароль пользователя.
    host (str): Хост, на котором расположена база данных.
    port (str): Порт для подключения к базе данных.
    database (str): Имя базы данных.

    Описание:
    Функция подключается к указанной базе данных PostgreSQL и записывает данные из
    DataFrame в таблицу. Если таблица уже существует, она будет заменена.
    Время выполнения операции записи измеряется и выводится.
    """
    with get_time():
        connection_string = (
            f"postgresql://{username}:{password}@{host}:{port}/{database}"
        )
        engine = create_engine(connection_string)

        with engine.connect() as connection:
            try:
                df.to_sql(
                    table_name, con=connection, schema=schema, index=False, if_exists="replace"
                )
                print(
                    f"DataFrame успешно записан в PostgreSQL в таблицу {schema}.{table_name}."
                )
            except Exception as e:
                raise Exception(
                    f"Произошла ошибка при записи таблицы {schema}.{table_name} в базу данных: {e}"
                ) from e


def read_df_from_pg(
    table_name,
    schema="public",
    username="maxglnv",
    password=Variable.get("HSE_DB_PASSWORD"),
    host="rc1b-diwt576i60sxiqt8.mdb.yandexcloud.net",
    port="6432",
    database="hse_db",
):
    """
    Функция для чтения данных из таблицы базы данных PostgreSQL в pandas DataFrame.

    Параметры:
    table_name (str): Имя таблицы для чтения данных.
    schema (str): Схема базы данных.
    username (str): Имя пользователя для подключения к базе данных.
    password (str): Пароль пользователя.
    host (str): Хост, на котором расположена база данных.
    port (str): Порт для подключения к базе данных.
    database (str): Имя базы данных.

    Возвращает:
    pandas.DataFrame: Данные из указанной таблицы.
    """
    with get_time():
        connection_string = (
            f"postgresql://{username}:{password}@{host}:{port}/{database}"
        )
        engine = create_engine(connection_string)

        with engine.connect() as connection:
            try:
                sql_query = f"SELECT * FROM {schema}.{table_name}"

                df = pd.read_sql_query(sql_query, con=connection)
                print(
                    f"Данные успешно загружены из таблицы {schema}.{table_name} PostgreSQL в DataFrame."
                )
                return df
            except Exception as e:
                raise Exception(
                    f"Произошла ошибка при чтении таблицы {schema}.{table_name} из базы данных: {e}"
                ) from e