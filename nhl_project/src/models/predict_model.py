import pandas as pd
from datetime import datetime, timedelta
import numpy as np

from sklearn.linear_model import CatBoostClassifier

from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from nhl_project.src.data.functions import read_df_from_pg, write_into_database, load_object


DEFAULT_ARGS = {
    "owner": "Galanov, Shiryeava",
    "email": "maxglnv@gmail.com",
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(seconds=15),
}

dag = DAG(
    dag_id="create_datamarts",
    schedule_interval="30 13 * * *",
    start_date=days_ago(2),
    catchup=False,
    tags=["hse_nhl_ml_project"],
    default_args=DEFAULT_ARGS,
    description="Create datamart with games statistics",
)


def model_predict():
    games_winner_prediction = read_df_from_pg("games_winner_prediction")
    model_dataset = load_object("home/airflow/nhl-ml-project/nhl_project/data/processed/model_dataset.pkl")
    top_features = load_object("home/airflow/nhl-ml-project/nhl_project/data/processed/top_features.pkl")
    model = load_object("home/airflow/nhl-ml-project/nhl_project/models/trained_model.pkl")

    today = datetime.now()
    yesterday = (today - timedelta(days=2)).strftime("%Y-%m-%d")

    df_test = model_dataset[model_dataset.game_date >= yesterday].drop(
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

    X_test_top = df_test[top_features]

    y_proba_lg = model.predict_proba(X_test_top)
    y_proba_win_lg = y_proba_lg[:, 1]

    y_pred = np.where(y_proba_win_lg >= 0.48, 1, 0)

    df_prdeiction = model_dataset.iloc[-(len(y_pred)) :, :9]
    df_prdeiction["home_team_win_proba"] = y_proba_win_lg
    df_prdeiction["home_team_win"] = y_pred
    df_prdeiction.reset_index(inplace=True, drop=True)

    df_old = games_winner_prediction[games_winner_prediction.game_date < yesterday]
    df_prdeiction_new = pd.concat(
        [df_old, df_prdeiction], axis=0, ignore_index=True, sort=False
    ).sort_values(by="game_date")


    write_query = """ 
        INSERT INTO public.games_winner_prediction(
            game_source_id
            , game_date
            , eastern_start_time
            , season
            , home_team_code
            , home_team_name
            , visiting_team_code
            , visiting_team_name
            , game_type
            , home_team_win_proba
            , home_team_win
        ) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) 
        """
    
    write_into_database(df_prdeiction_new, 'games_winner_prediction', write_query)


task_model_predict = PythonOperator(
    task_id="model_predict",
    python_callable=model_predict,
    dag=dag,
)

task_model_predict