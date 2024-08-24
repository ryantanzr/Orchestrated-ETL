import pandas as pd
from airflow.decorators import task

@task
def extract_from_csv(name) -> pd.DataFrame:
    return pd.read_csv(name)

@task
def transformation(df: pd.DataFrame) -> pd.DataFrame:
    return df

@task
def load_to_warehouse(df: pd.DataFrame, name: str) -> None:
    df.to_csv(name, index=False)