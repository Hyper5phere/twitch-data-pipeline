import os
import requests
import logging
import config
import pandas as pd
import sqlite3
import py7zr
import zipfile

# setup logging first
logging.basicConfig(format='%(asctime)s :: %(levelname)-8s :: %(name)-20s :: %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    level="DEBUG")
logger = logging.getLogger("twitch.data.downloader")

if not os.path.exists(config.ZIP_PATH):
    # download the dataset file from dataverse
    logger.info("Downloading data file: %s", config.TWITCH_DATA_URL)
    with requests.get(config.TWITCH_DATA_URL, stream=True) as r:
        r.raise_for_status()
        with open(config.ZIP_PATH, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)

# Extract the data
inner_zip_file = os.path.join(config.DATA_FOLDER, "Twitch_data.zip")
if not os.path.exists(inner_zip_file):
    logger.info("Extracting data 7z file: %s", config.ZIP_PATH)
    with py7zr.SevenZipFile(config.ZIP_PATH, mode='r') as z:
        z.extractall(config.DATA_FOLDER)

data_folder = os.path.join(config.DATA_FOLDER, "ICWSM19_data")
if not os.path.exists(data_folder):
    logger.info("Extracting data ZIP file: %s", inner_zip_file)
    with zipfile.ZipFile(inner_zip_file, 'r') as zip_ref:
        zip_ref.extractall(config.DATA_FOLDER)

# load the pickle files into pandas dataframes and write into local SQL database
with sqlite3.connect(config.DB_PATH) as conn:
    for f in os.listdir(data_folder):
        fpath = os.path.join(data_folder, f)
        logger.info("Reading data file: %s", fpath)
        data_frame = pd.read_pickle(fpath)
        table_name = os.path.basename(fpath).replace(".pkl", "")
        # Identify datetime columns
        datetime_cols = data_frame.select_dtypes(include=['datetime64[ns]', 'datetime64[ns, UTC]']).columns
        # Convert datetime columns to ISO 8601 strings
        for col in datetime_cols:
            data_frame[col] = data_frame[col].astype(str)
        data_frame.to_sql(table_name, conn,
                          if_exists='replace', index=False)
