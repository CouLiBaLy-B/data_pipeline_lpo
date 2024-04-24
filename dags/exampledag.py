from airflow.decorators import dag, task
from datetime import datetime
import requests
from bs4 import BeautifulSoup
from google.oauth2 import service_account
import pandas as pd
import pandas_gbq
from geopy.geocoders import Nominatim
import os
from dotenv import load_dotenv

load_dotenv()

API_URL = os.getenv("API_URL")
PROJECT_ID = os.getenv("PROJECT_ID")
DATASET_NAME = os.getenv("DATASET_NAME")


geolocator = Nominatim(user_agent="myApplication")


def get_location_info(latitude, longitude) -> tuple[str, str, str]:
    location = geolocator.reverse((latitude, longitude), exactly_one=True)
    address = location.raw["address"]
    region = address.get("state", "")
    country = address.get("country", "")
    departement = address.get("county", "")
    return region, departement, country


def apply_function_to_sublists(func, nested_list):
    return [func(*pair) for pair in nested_list]


def type_conversion(df):
    str_cols = [
        "id",
        "description",
        "name",
        "structure",
        "region",
        "departement",
        "Pays",
    ]
    date_cols = ["update_datetime", "create_datetime"]

    df[str_cols] = df[str_cols].apply(lambda col: col.astype(str))
    df[date_cols] = df[date_cols].apply(lambda col: pd.to_datetime(col))

    return df


def convertir_date(date_string):
    try:
        date_obj = datetime.strptime(date_string, "%Y-%m-%dT%H:%M:%S.%f%z")
        formatted_date = date_obj.strftime("%Y-%m-%d")
        return formatted_date
    except ValueError:
        return """Format de date invalide. Assurez-vous que la chaîne est
                  au format 'aaaa-mm-jjThh:mm:ss.ssssss+hh:mm'."""


def add_months_names(df):
    months_list = [
        "janvier",
        "fevrier",
        "mars",
        "avril",
        "mai",
        "juin",
        "juillet",
        "août",
        "septembre",
        "octobre",
        "novembre",
        "decembre",
    ]
    df_with_months = df.assign(
        months=df["period"].apply(
            lambda x: [months_list[i] for i, value in enumerate(x)
                       if value == 1]
        )
    )
    return df_with_months


def remove_html_tags(text):
    soup = BeautifulSoup(text, "html.parser")
    cleaned_text = soup.get_text()
    return cleaned_text


# Défini les paramètres de notre DAG, comme schedule et start_date
@dag(
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    doc_md=__doc__,
    default_args={"owner": "biodiv-sport", "retries": 3},
    tags=["ETL-V1"],
)
def data_process():
    @task
    def data_extract(api_url) -> list:

        # Récupération des données depuis l'API
        response = requests.get(api_url)
        res = response.json()
        res = res["results"]
        return res

    @task
    def data_transformation(res):
        data = [
            {
                "create_datetime": convertir_date(item["create_datetime"]),
                "id": item["id"],
                "description": remove_html_tags(item["description"]["fr"]),
                "name": item["name"]["fr"],
                "structure": item["structure"],
                "species_id": item.get("species_id", "zr"),
                "practices": item["practices"],
                "coordonnees": (
                    item["geometry"]["coordinates"][0][0][0]
                    if item["geometry"]["type"] == "MultiPolygon"
                    else item["geometry"]["coordinates"][0][0]
                ),
                "period": (
                    [int(bool_) for bool_ in item["period"]]
                    if isinstance(item["period"], list)
                    else item["period"]
                ),
                "update_datetime": convertir_date(item["update_datetime"]),
            }
            for item in res
        ]

        df = pd.DataFrame(data)
        df[["region", "departement", "Pays"]] = list(
            df["coordonnees"].apply(lambda raw: get_location_info(raw[1],
                                                                  raw[0]))
        )
        df = add_months_names(df)
        df = df.explode("months")
        df = df.explode("practices")
        df.reset_index(inplace=True)
        df = type_conversion(df)
        df.drop(["period", "coordonnees", "index"], axis=1, inplace=True)
        return df

    @task
    def data_load(df: pd.DataFrame) -> None:
        credentials = service_account.Credentials.from_service_account_file(
            "/usr/local/airflow/.secret/service_account.json"
        )

        pandas_gbq.to_gbq(
            df,
            f"{DATASET_NAME}.data",
            project_id=PROJECT_ID,
            credentials=credentials,
            if_exists="replace",
        )

    res = data_extract(API_URL)
    df = data_transformation(res)
    data_load(df)


# Instantiate the DAG
data_process()
