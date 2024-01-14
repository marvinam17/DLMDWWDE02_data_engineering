import re
import requests
import pandas as pd
from bs4 import BeautifulSoup
from zipfile import ZipFile
from io import BytesIO


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
            dfs.append(pd.read_csv(z.open(file),sep=';'))
    return pd.concat(dfs)

def get_station_dwd_file_storage(url, station_id):
    """
    This function crawls historic data from DWD
    """
    r = requests.get(url)
    soup = BeautifulSoup(r.content,'html.parser') 
    a_tags_list = []
    for link in soup.find_all('a'):
        a_tags_list.append(link.get('href'))
    regex = re.compile(f'10minutenwerte_TU_{station_id}_*')
    filtered = [i for i in a_tags_list if regex.match(str(i))]
    return filtered