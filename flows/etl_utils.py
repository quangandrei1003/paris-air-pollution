import os
from dotenv import load_dotenv
import pandas as pd
from prefect import task

load_dotenv()


def load_open_weather_api_key() -> str:
    """
    Load open weather api key from enviroment file
    """
    api_key = os.environ.get("API_KEY")
    # api_key = "aa088b981699d5336af08cfe9e9c7365"
    return api_key


@task(retries=3, log_prints=True)
def rename_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Rename the columns of a pandas DataFrame.

    Parameters
    ----------
    df : pd.DataFrame
        The DataFrame to rename the columns of.

    Returns
    -------
    df : pd.DataFrame
        The DataFrame with renamed columns.
    """
    df = df.rename(
        columns={
            "components.co": "Carbon_Monoxide_CO",
            "components.no": "Nitric_oxide_NO",
            "components.no2": "Nitrogen_Dioxide_NO2",
            "components.o3": "Ozone_O3",
            "components.so2": "Sulfur_Dioxide_SO2",
            "components.pm2_5": "PM2_5",
            "components.pm10": "PM10",
            "components.nh3": "NH3",
        }
    )
    return df


@task(retries=3, log_prints=True)
def cleaning_columns(df: pd.DataFrame, columns=[None]) -> pd.DataFrame:
    """
    Clean and transform the columns of a pandas DataFrame.

    Parameters
    ----------
    df : pd.DataFrame
        The DataFrame to clean and transform.
    columns : list of str or None, optional
        The columns to drop from the DataFrame. If None, no columns are dropped.
        Default is None.

    Returns
    -------
    df : pd.DataFrame
        The cleaned and transformed DataFrame.
    """
    if columns:
        df.drop(columns=columns, inplace=True)
    df["dt"] = pd.to_datetime(df["dt"], unit="s")
    return df
