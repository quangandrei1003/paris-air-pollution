import pandas as pd
import os
import requests
from prefect import task, flow
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials
from etl_utils import load_open_weather_api_key, cleaning_columns, rename_columns
from pathlib import Path


@flow()
def get_air_pollution_data(
    lat: float, lon: float, start_date=None, end_date=None
) -> pd.DataFrame:
    """
    Get air pollution data for the current time for a specific latitude and longitude in Ile de France region.

    Parameters
    ----------
    lat : float
        The latitude of the location.
    lon : float
        The longitude of the location.

    Returns
    -------
    df : pd.DataFrame
        A pandas DataFrame containing the retrieved air pollution data.
    """
    # API endpoint and API key
    api_endpoint = "https://api.openweathermap.org/data/2.5/air_pollution"
    api_key = load_open_weather_api_key()

    # Make a request to the OpenWeatherMap API
    response = requests.get(
        api_endpoint,
        params={
            "lat": lat,
            "lon": lon,
            "appid": api_key,
        },
    )

    if response.status_code != 200:
        print("Error: API request failed with status code", response.status_code)
        exit()

    # Parse the API response
    data = response.json()

    # Convert the data to a dataframe
    df = pd.json_normalize(data, "list")
    df_clean = cleaning_columns(df, columns=["main.aqi"])
    df_renamed = rename_columns(df_clean)
    return df_renamed


@task(retries=3, log_prints=True)
def get_pollution_data(
    start_time: int, end_time: int, lat: float, lon: float
) -> pd.DataFrame:
    """
    Retrieve air pollution data from the OpenWeatherMap API for a specified time range and location.
    Parameters:
        start_time (int): The start time for the data range, SECONDS SINCE JAN 01 1970. (UTC).
        end_time (int): The end time for the data range, SECONDS SINCE JAN 01 1970. (UTC).
        lat (float): The latitude of the location for which to retrieve data.
        lon (float): The longitude of the location for which to retrieve data.
    Returns:
        pandas.DataFrame: A dataframe containing the API response data.
    """

    # API endpoint and API key
    api_endpoint = "https://api.openweathermap.org/data/2.5/air_pollution/history"
    api_key = os.environ["API_KEY"]

    # Send the API request
    response = requests.get(
        api_endpoint,
        params={
            "lat": lat,
            "lon": lon,
            "start": start_time,
            "end": end_time,
            "appid": api_key,
        },
    )

    # Check for errors
    if response.status_code != 200:
        print("Error: API request failed with status code", response.status_code)
        exit()

    # Parse the API response
    data = response.json()

    # Convert the data to a dataframe
    df = pd.json_normalize(data["list"])

    return df


@task(retries=3, log_prints=True)
def write_gcs(df: pd.DataFrame, path: str) -> None:
    gcp_cloud_storage_bucket_block = GcsBucket.load("paris-air-pollution-gcs")
    gcp_cloud_storage_bucket_block.upload_from_dataframe(
        df, to_path=path, serialization_format="parquet"
    )
    return


@task(retries=3, log_prints=True)
def write_local(df: pd.DataFrame, path: str) -> None:
    Path(f"data/air_pollution").mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, compression="gzip")
    return path


@task(retries=3, log_prints=True)
def extract_from_gcs(city: str) -> pd.DataFrame:
    """
    Extracts air pollution data for a given city from a Google Cloud Storage (GCS) Parquet file.

    Parameters
    ----------
    city : str
        The name of the city for which to extract air pollution data. The city name should match the name of the
        corresponding GCS Parquet file, without the file extension.

    Returns
    -------
    pd.DataFrame
        A Pandas DataFrame containing the extracted air pollution data.
    """
    gcs_path = f"data/air_pollution/{city}.parquet"
    gcs_block = GcsBucket.load("paris-air-pollution-gcs")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"../data/")
    df = pd.read_parquet(f"../data/{gcs_path}")
    return df


@task(retries=3, log_prints=True)
def write_bq(
    df: pd.DataFrame,
    table: str,
    credentials_path: str = "paris-air-pollution-gcp-creds",
) -> None:
    """
    Writes a Pandas DataFrame to BigQuery.

    Args:
        df (pd.DataFrame): The DataFrame to write to BigQuery.
        table (str): The name of the destination table in BigQuery.
        credentials_path (str, optional): The path to the Google Cloud Platform
            service account credentials file. Defaults to 'airpollution-credential'.

    Raises:
        prefect.engine.signals.FAIL: If the function fails to write to BigQuery.
    """
    try:
        gcp_credentials_block = GcpCredentials.load(credentials_path)
        df.to_gbq(
            destination_table=table,
            project_id="paris-air-pollution-quangnc",
            credentials=gcp_credentials_block.get_credentials_from_service_account(),
            chunksize=500_000,
            if_exists="append",
        )
    except Exception as e:
        print(f"Failed to write to BigQuery: {str(e)}")


@task(retries=3, log_prints=True)
def get_current_pollution(lat: float, lon: float) -> pd.DataFrame:
    """
    Retrieve air pollution data for the current time for a specific latitude and longitude.

    Parameters
    ----------
    lat : float
        The latitude of the location for which to retrieve data.
    lon : float
        The longitude of the location for which to retrieve data.

    Returns
    -------
    df : pd.DataFrame
        A pandas DataFrame containing the retrieved air pollution data.
    """
    # API endpoint and API key
    api_endpoint = "https://api.openweathermap.org/data/2.5/air_pollution"
    api_key = os.environ["API_KEY"]

    # Make a request to the OpenWeatherMap API
    response = requests.get(
        api_endpoint,
        params={
            "lat": lat,
            "lon": lon,
            "appid": api_key,
        },
    )

    # Check for errors
    if response.status_code != 200:
        print("Error: API request failed with status code", response.status_code)
        exit()

    # Parse the API response
    data = response.json()

    # Convert the data to a dataframe
    df = pd.json_normalize(data, "list", [["coord", "lon"], ["coord", "lat"]])

    return df
