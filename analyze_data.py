import os
import sqlite3
import pandas as pd
import config
import matplotlib.pyplot as plt
import logging

# setup logging first
logging.basicConfig(format='%(asctime)s :: %(levelname)-8s :: %(name)-20s :: %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    level="DEBUG")
logger = logging.getLogger("twitch.data.analyzer")

data_folder = os.path.join(config.DATA_FOLDER, "ICWSM19_data")
all_tables = [f.replace(".pkl", "") for f in os.listdir(data_folder)]

post_counts = pd.DataFrame({"commenter_id": [-1], "count": 0})
logger.info("Reading database: %s", config.DB_PATH)
with sqlite3.connect(config.DB_PATH) as conn:
    for table in all_tables:
        df = pd.read_sql(f"SELECT * FROM {table}", conn)
        streamer_stats = df["commenter_id"].value_counts().head(10).to_frame()
        for idx, row in streamer_stats.iterrows():
            if idx not in post_counts["commenter_id"]:
                post_counts["commenter_id"].append(idx)
                post_counts["count"].append(row["count"])
            else:
                post_counts[idx == "commenter_id"] += row["count"]
        # post_counts = pd.concat(
        #     [post_counts, streamer_stats], names=["commenter_id"], ignore_index=True)
        logger.info(table + " post counts:\n%s", post_counts)

logger.info("Total post counts:\n%s", post_counts)
