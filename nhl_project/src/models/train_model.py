import pandas as pd
from datetime import datetime, timedelta
import numpy as np

from catboost import CatBoostClassifier
from pyspark.sql import SparkSession

from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from nhl_project.src.data.functions import read_table_from_pg, save_object, load_object


DEFAULT_ARGS = {
    "owner": "Galanov, Shiryeava",
    "email": "maxglnv@gmail.com",
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(seconds=15),
}

dag = DAG(
    dag_id="model_train",
    schedule_interval="30 13 * * *",
    start_date=days_ago(2),
    catchup=False,
    tags=["hse_nhl_ml_project"],
    default_args=DEFAULT_ARGS,
    description="Train model",
)


def fill_na_by_team(group):
    return group.fillna(method="ffill").fillna(method="bfill")


def make_dataset():
    spark = (
        SparkSession.builder.config(
            "spark.jars",
            "~/airflow_venv/lib/python3.10/site-packages/pyspark/jars/postgresql-42.3.1.jar",
        )
        .master("local[*]")
        .appName("parse_teams")
        .getOrCreate()
    )

    df_games = read_table_from_pg(spark, "public.games_wide_datamart")
    teams_stat = read_table_from_pg(spark, "public.teams_stat_wide_datamart")
    nhl_games_odds = read_table_from_pg(spark, "public.nhl_games_odds")
    games_winner_prediction = read_table_from_pg(
        spark, "public.games_winner_prediction"
    )

    df_games = df_games.toPandas()
    teams_stat = teams_stat.toPandas()
    nhl_games_odds = nhl_games_odds.toPandas()
    games_winner_prediction = games_winner_prediction.toPandas()

    df_games = df_games.sort_values("eastern_start_time")
    df_games["game_date"] = df_games.game_date.astype(str)

    df_games["home_team_name"] = df_games["home_team_name"].replace(
        "Phoenix Coyotes", "Arizona Coyotes"
    )
    df_games["visiting_team_name"] = df_games["visiting_team_name"].replace(
        "Phoenix Coyotes", "Arizona Coyotes"
    )

    df_games["home_team_code"] = df_games["home_team_code"].replace("PHI", "ARI")
    df_games["visiting_team_code"] = df_games["visiting_team_code"].replace(
        "PHI", "ARI"
    )

    df_games["home_matches"] = df_games.groupby(["home_team_code", "season"]).cumcount()
    df_games["home_wins"] = (
        df_games.groupby(["home_team_code", "season"])["home_team_winner"].cumsum()
        - df_games["home_team_winner"]
    )
    df_games["h_home_wins"] = df_games["h_home_wins"].fillna(df_games["home_wins"])
    df_games["h_home_games_played"] = df_games["h_home_games_played"].fillna(
        df_games["home_matches"]
    )
    df_games["h_home_win_pctg"] = df_games["home_wins"] / df_games["home_matches"]
    df_games.drop(columns=["home_matches", "home_wins"], inplace=True)

    df_games["visiting_team_winner"] = np.abs(1 - df_games["home_team_winner"])
    df_games["road_matches"] = df_games.groupby(
        ["visiting_team_code", "season"]
    ).cumcount()
    df_games["road_wins"] = (
        df_games.groupby(["visiting_team_code", "season"])[
            "visiting_team_winner"
        ].cumsum()
        - df_games["visiting_team_winner"]
    )
    df_games["v_road_wins"] = df_games["v_road_wins"].fillna(df_games["road_wins"])
    df_games["v_road_games_played"] = df_games["v_road_games_played"].fillna(
        df_games["road_matches"]
    )
    df_games["v_road_win_pctg"] = df_games["road_wins"] / df_games["road_matches"]
    df_games.drop(
        columns=["road_matches", "road_wins", "visiting_team_winner"], inplace=True
    )

    home_games = df_games[
        ["game_date", "season", "home_team_code", "home_team_winner"]
    ].rename(columns={"home_team_code": "team_code", "home_team_winner": "win"})
    away_games = df_games[["game_date", "season", "visiting_team_code"]].rename(
        columns={"visiting_team_code": "team_code"}
    )
    away_games["win"] = np.abs(1 - df_games["home_team_winner"])

    all_games = pd.concat([home_games, away_games])
    all_games = all_games.sort_values("game_date")
    all_games["prev_cumulative_matches"] = all_games.groupby(
        ["team_code", "season"]
    ).cumcount()
    all_games["prev_cumulative_wins"] = (
        all_games.groupby(["team_code", "season"])["win"].cumsum() - all_games["win"]
    )
    all_games["prev_cumulative_win_pctg"] = (
        all_games["prev_cumulative_wins"] / all_games["prev_cumulative_matches"]
    )

    df_games = (
        df_games.merge(
            all_games[
                [
                    "game_date",
                    "team_code",
                    "prev_cumulative_win_pctg",
                    "prev_cumulative_matches",
                    "prev_cumulative_wins",
                ]
            ],
            left_on=["game_date", "home_team_code"],
            right_on=["game_date", "team_code"],
        )
        .rename(
            columns={
                "prev_cumulative_win_pctg": "h_wins_pctg_cumsum",
                "prev_cumulative_matches": "h_games_played_cumsum",
                "prev_cumulative_wins": "h_wins_cumsum",
            }
        )
        .drop(columns=["team_code"])
    )

    df_games = (
        df_games.merge(
            all_games[
                [
                    "game_date",
                    "team_code",
                    "prev_cumulative_win_pctg",
                    "prev_cumulative_matches",
                    "prev_cumulative_wins",
                ]
            ],
            left_on=["game_date", "visiting_team_code"],
            right_on=["game_date", "team_code"],
        )
        .rename(
            columns={
                "prev_cumulative_win_pctg": "v_wins_pctg_cumsum",
                "prev_cumulative_matches": "v_games_played_cumsum",
                "prev_cumulative_wins": "v_wins_cumsum",
            }
        )
        .drop(columns=["team_code"])
    )

    df_games["h_win_pctg"] = df_games["h_win_pctg"].fillna(
        df_games["h_wins_pctg_cumsum"]
    )
    df_games["h_games_played"] = df_games["h_games_played"].fillna(
        df_games["h_games_played_cumsum"]
    )
    df_games["h_wins"] = df_games["h_wins"].fillna(df_games["h_wins_cumsum"])

    df_games["v_win_pctg"] = df_games["v_win_pctg"].fillna(
        df_games["v_wins_pctg_cumsum"]
    )
    df_games["v_games_played"] = df_games["v_games_played"].fillna(
        df_games["v_games_played_cumsum"]
    )
    df_games["v_wins"] = df_games["v_wins"].fillna(df_games["v_wins_cumsum"])

    df_games.drop(
        columns=[
            "h_wins_pctg_cumsum",
            "h_games_played_cumsum",
            "h_wins_cumsum",
            "v_wins_pctg_cumsum",
            "v_games_played_cumsum",
            "v_wins_cumsum",
        ],
        inplace=True,
    )

    columns_to_fill = sorted(df_games.columns[df_games.isna().any()].tolist())
    columns_without_h = [col for col in columns_to_fill if col.startswith("h_")]
    columns_without_v = [col for col in columns_to_fill if col.startswith("v_")]

    df_games[columns_without_h] = df_games.groupby("home_team_code")[
        columns_without_h
    ].transform(fill_na_by_team)
    df_games[columns_without_v] = df_games.groupby("visiting_team_code")[
        columns_without_v
    ].transform(fill_na_by_team)

    df_games = df_games[(df_games.game_date >= "2012-01-01")]
    df_games["h_avg_home_goals_for"] = (
        df_games["h_home_goals_for"] / df_games["h_home_games_played"]
    )
    df_games["v_avg_road_goals_against"] = (
        df_games["v_road_goals_against"] / df_games["v_road_games_played"]
    )
    df_games["v_avg_road_goals_for"] = (
        df_games["v_road_goals_for"] / df_games["v_road_games_played"]
    )
    df_games["h_avg_home_goals_against"] = (
        df_games["h_home_goals_against"] / df_games["h_home_games_played"]
    )

    games_merged = (
        pd.merge(
            df_games,
            nhl_games_odds,
            on=["home_team_name", "game_date", "visiting_team_name"],
            how="inner",
        )
        .sort_values(by="game_date")
        .fillna(0)
    )
    games_merged.drop(columns=["eastern_dt"], inplace=True)

    numeric_cols = games_merged.select_dtypes(include=["number"]).columns
    for col in numeric_cols:
        games_merged[col] = games_merged.groupby(
            ["season", "home_team_code", "visiting_team_code"]
        )[col].transform(lambda x: x.fillna(x.mean()))

    string_cols = games_merged.select_dtypes(include=["object"]).columns
    for col in string_cols:
        games_merged[col] = games_merged.groupby(
            ["season", "home_team_code", "visiting_team_code"]
        )[col].transform(lambda x: x.fillna(x.mode()[0]))

    save_object(
        games_merged,
        "home/airflow/nhl-ml-project/nhl_project/data/processed/model_dataset.pkl",
    )


def train_model():
    model_dataset = load_object(
        "home/airflow/nhl-ml-project/nhl_project/data/processed/model_dataset.pkl"
    )
    top_features = load_object(
        "home/airflow/nhl-ml-project/nhl_project/data/processed/top_features.pkl"
    )

    today = datetime.now()
    yesterday = (today - timedelta(days=2)).strftime("%Y-%m-%d")

    df_train = model_dataset[model_dataset.game_date < yesterday].drop(
        columns=[
            "game_source_id",
            "eastern_start_time",
            "game_date",
            "game_type",
            "score_delta",
            "home_team_name",
            "visiting_team_name",
            "home_score",
            "visiting_score",
            "v_team_name",
            "h_team_name",
        ]
    )

    X_train_top = df_train[top_features]
    y_train = df_train.home_team_winner

    categorical_features_top = [
        "season",
        "home_team_code",
        "visiting_team_code",
        "h_division_name",
        "h_streak_code",
        "v_division_name",
        "v_streak_code",
        "game_month",
    ]

    catboost_cl_top = CatBoostClassifier(
        iterations=5000,
        random_state=17,
        learning_rate=0.005,
        depth=6,
        l2_leaf_reg=7,
        cat_features=categorical_features_top,
        train_dir="/home/airflow/nhl-ml-project/nhl_project/src/models/catboost_info",
    )

    catboost_cl_top.fit(X_train_top, y_train, verbose=500)

    save_object(
        catboost_cl_top,
        "home/airflow/nhl-ml-project/nhl_project/models/trained_model.pkl",
    )


task_make_dataset = PythonOperator(
    task_id="make_dataset",
    python_callable=make_dataset,
    dag=dag,
)

task_train_model = PythonOperator(
    task_id="train_model",
    python_callable=train_model,
    dag=dag,
)

task_make_dataset >> task_train_model
