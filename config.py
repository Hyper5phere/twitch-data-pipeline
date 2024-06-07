import os
script_dir = os.path.dirname(os.path.abspath(__file__))

TWITCH_DATA_URL = "https://dataverse.harvard.edu/api/access/datafile/:persistentId?persistentId=doi:10.7910/DVN/VE0IVQ/5VNGY6"
DATA_FOLDER = os.path.join(script_dir, "data")
DB_PATH = os.path.join(DATA_FOLDER, "twitch_data.db")
ZIP_PATH = os.path.join(DATA_FOLDER, "twitch_data.7z")
