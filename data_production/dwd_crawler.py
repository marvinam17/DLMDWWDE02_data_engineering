"""
This module provides data crawling for the Kafka Producer.
"""
import re
from zipfile import ZipFile
from io import BytesIO
import requests
import pandas as pd
from bs4 import BeautifulSoup

def rename_dataframe_columns(df):
    """
    Ensure to process equal column names and return only required columns.
    """
    rename_dict = {
        "STATIONS_ID":"STATION_ID",
        "MESS_DATUM":"MEASUREMENT_DATE",
        "QN":"QUALITY_LEVEL",
        "PP_10":"AIR_PRESSURE",
        "TT_10":"AIR_TEMPERATURE_200CM",
        "TM5_10":"AIR_TEMPERATURE_5CM",
        "RF_10":"REL_HUMIDITY",
        "TD_10":"DEWPOINT_TEMPERATURE"
    }
    df.rename(mapper=rename_dict, axis=1, inplace=True)
    return df[list(rename_dict.values())]

def reformat_timestamp(df):
    df["MEASUREMENT_DATE"] = pd.to_datetime(df["MEASUREMENT_DATE"],
                                            format='%Y%m%d%H%M')
    return df

def get_and_unzip_files(url, list_of_filenames):
    """
    This function gets the filesnames of historic temperature data
    for a specific station from DWD and returns them in a list
    """
    dfs=[]
    for file in list_of_filenames:
        r = requests.get(url+file, stream=True)
        z=ZipFile(BytesIO(r.content))
        for file in z.namelist():
            df = pd.read_csv(z.open(file),sep=';')
            df.columns = [s.strip() for s in df.columns]
            dfs.append(rename_dataframe_columns(df))
    return pd.concat(dfs).sort_values("MEASUREMENT_DATE").drop_duplicates(
        subset=["MEASUREMENT_DATE"])

def get_station_dwd_file_storage(url, station_id):
    """
    This function crawls historic data from DWD
    The data is splitted in histroic, recent and now data.
    """
    all_data = []
    for folder in ["historical/","recent/","now/"]:
        filtered = []
        r = requests.get(url+folder)
        soup = BeautifulSoup(r.content,'html.parser')
        a_tags_list = []
        for link in soup.find_all('a'):
            a_tags_list.append(link.get('href'))
        regex = re.compile(f'10minutenwerte_TU_{station_id}_*')
        filtered = [(folder+i) for i in a_tags_list if regex.match(str(i))]
        all_data.extend(filtered)
    return all_data
