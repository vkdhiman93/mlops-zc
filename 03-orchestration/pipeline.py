
from pathlib import Path
import pickle
import pandas as pd
from datetime import datetime
import numpy as np

from prefect import flow, task
from sklearn.linear_model import LinearRegression
from sklearn.feature_extraction import DictVectorizer
from sklearn.metrics import mean_squared_error
import mlflow
from mlflow.tracking import MlflowClient
from mlflow.entities import ViewType


EXPERIMENT_NAME = "nyc-taxi-experiment"
mlflow.set_tracking_uri("http://localhost:5000")
mlflow.set_experiment(EXPERIMENT_NAME)


@task
def read_dataframe(year, month):
    url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month:02d}.parquet'
    df = pd.read_parquet(url)
    print(f"Loaded records for {year}-{month}: {len(df)}")
    df['duration'] = df.tpep_dropoff_datetime - df.tpep_pickup_datetime
    df.duration = df.duration.dt.total_seconds() / 60
    df = df[(df.duration >= 1) & (df.duration <= 60)]
    df[['PULocationID', 'DOLocationID']] = df[[
        'PULocationID', 'DOLocationID']].astype(str)
    print(f"Records after processing for {year}-{month}: {len(df)}")
    return df


@task
def create_features(df_train, df_val):
    categorical = ['PULocationID', 'DOLocationID']
    dicts_train = df_train[categorical].to_dict(orient='records')
    dicts_val = df_val[categorical].to_dict(orient='records')

    dv = DictVectorizer(sparse=True)
    X_train = dv.fit_transform(dicts_train)
    X_val = dv.transform(dicts_val)

    return X_train, X_val, dv, df_train['duration'].values, df_val['duration'].values


@task
def already_ran(params):
    client = MlflowClient()
    experiment = client.get_experiment_by_name(EXPERIMENT_NAME)
    runs = client.search_runs(
        experiment_ids=[experiment.experiment_id],
        filter_string=f"params.year = '{params['year']}' and params.month = '{params['month']}'",
        run_view_type=ViewType.ACTIVE_ONLY,
    )
    return len(runs) > 0


@task
def train_model(X_train, y_train, X_val, y_val, dv, params):
    if already_ran(params):
        print(
            f"Run for year={params['year']} month={params['month']} already exists. Skipping.")
        return None

    with mlflow.start_run() as run:
        mlflow.log_params(params)
        lr = LinearRegression()
        lr.fit(X_train, y_train)
        y_pred = lr.predict(X_val)
        rmse = np.sqrt(mean_squared_error(y_val, y_pred))
        mlflow.log_metric("rmse", rmse)

        Path("models").mkdir(exist_ok=True)
        with open("models/preprocessor.b", "wb") as f_out:
            pickle.dump(dv, f_out)

        mlflow.log_artifact("models/preprocessor.b",
                            artifact_path="preprocessor")
        mlflow.sklearn.log_model(lr, artifact_path="model")

        print(f"Intercept of the model: {lr.intercept_}")
        return run.info.run_id


@task
def register_best_model():
    client = MlflowClient()
    experiment = client.get_experiment_by_name(EXPERIMENT_NAME)
    runs = client.search_runs(
        experiment_ids=[experiment.experiment_id],
        run_view_type=ViewType.ACTIVE_ONLY,
        max_results=1,
        order_by=["metrics.rmse ASC"]
    )
    if not runs:
        print("No runs to register.")
        return
    best_run = runs[0]
    run_id = best_run.info.run_id
    model_uri = f"runs:/{run_id}/model"
    mlflow.register_model(model_uri=model_uri, name=EXPERIMENT_NAME)


@task
def save_run_id(run_id: str):
    if run_id:
        with open("run_id.txt", "w") as f:
            f.write(run_id)
        print(f"Saved run_id: {run_id}")


@flow(name="mlops_taxi_flow")
def mlops_pipeline(year: int = 2023, month: int = 1):
    params = {'year': str(year), 'month': str(month)}

    df_train = read_dataframe(year, month)
    next_year, next_month = (year, month + 1) if month < 12 else (year + 1, 1)
    df_val = read_dataframe(next_year, next_month)

    X_train, X_val, dv, y_train, y_val = create_features(df_train, df_val)

    run_id = train_model(X_train, y_train, X_val, y_val, dv, params)

    register_best_model()
    save_run_id(run_id)


if __name__ == "__main__":
    mlops_pipeline(year=2023, month=3)
