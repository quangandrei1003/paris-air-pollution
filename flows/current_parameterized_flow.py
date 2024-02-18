from prefect import flow
from air_pollution_etl_tasks import (
    get_air_pollution_data,
    extract_from_gcs,
    write_local,
    write_gcs,
    write_bq,
)
import pandas as pd


@flow
def etl_current_data_main_flow(locations_path="./data/idf_major_cities.json") -> None:
    cities = pd.read_json(locations_path)
    for index, city in cities.iterrows():
        try:
            df_pollution = get_air_pollution_data(city["Latitude"], city["Longitude"])
            df_pollution["City_index"] = index

            # etl_to_local(df_pollution, city["City"])
            etl_to_gcs(df_pollution, city["City"])

        except Exception as e:
            print(f"Failed to process city {city['City']}: {str(e)}")


@flow()
def etl_gcs_to_bq(locations_path: str) -> None:
    """
    Extracts data from Google Cloud Storage (GCS), processes it, and loads it into BigQuery (BQ).

    Parameters
    ----------
    locations_path : str
        The path to the JSON file containing the list of cities to extract data for.

    Returns
    -------
    None

    Raises
    ------
    Any exceptions raised during the execution of the function are propagated to the caller.

    Notes
    -----
    This function iterates over the list of cities contained in the JSON file at `locations_path`.
    For each city, it extracts data from GCS using the `extract_from_gcs` context manager,
    and loads it into the `raw.airpollution` table in BQ using the `write_bq` function.
    If an exception is raised during the processing of a city, an error message is printed
    to the console and processing continues with the next city.
    """
    cities = pd.read_json(locations_path)
    for index, city in cities.iterrows():
        try:
            df = extract_from_gcs(city["City"])
            write_bq(df, "raw.airpollution")
        except Exception as e:
            print(f"Failed to process city {city.City}: {str(e)}")


@flow()
def etl_to_local(df, city) -> None:
    """
    A Prefect flow that writes a pandas DataFrame to local.

    Args:
        df (pd.DataFrame): The DataFrame to write.
        city (str): The name of the city associated with the DataFrame.
    """
    path = f"data/air_pollution/{city}.parquet"
    write_local(df, path)


@flow()
def etl_cities_flow(locations_path: str):
    """
    A Prefect flow that processes a pandas DataFrame containing information about cities.

    Parameters
    ----------
    locations_path : str
        The path to the JSON file containing the list of cities to extract data for.

    Raises
    ------
    Exception
        If there is an error processing the cities table.

    Returns
    -------
    None

    Notes
    -----
    This function reads in the city information from a JSON file at `locations_path` using pandas,
    and then writes the data to the `raw.cities` table in BigQuery using the `write_bq` function.
    If an exception is raised during the processing of the cities table, an error message is printed
    to the console.

    """
    cities = pd.read_json(locations_path)
    try:
        write_bq(cities, "raw.cities")
    except Exception as e:
        print(f"Failed to process cities table: {str(e)}")


@flow()
def etl_to_gcs(df, city) -> None:
    """
    A Prefect flow that writes a pandas DataFrame to a GCS bucket.

    Args:
        df (pd.DataFrame): The DataFrame to write.
        city (str): The name of the city associated with the DataFrame.
    """
    path = f"data/air_pollution/{city}.parquet"
    write_gcs(df, path)


if __name__ == "__main__":
    etl_current_data_main_flow("./data/idf_major_cities.json")
    etl_cities_flow("./data/idf_major_cities.json")
    etl_gcs_to_bq("./data/idf_major_cities.json")
