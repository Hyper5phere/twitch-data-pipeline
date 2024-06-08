import os
import sqlite3
import pandas as pd
import config
import matplotlib.pyplot as plt
import logging

# setup logging first
logging.basicConfig(format='%(asctime)s :: %(levelname)-8s :: %(name)-20s :: %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    level="INFO")
logger = logging.getLogger("twitch.data.analyzer")

data_folder = os.path.join(config.DATA_FOLDER, "ICWSM19_data")
all_tables = [f.replace(".pkl", "") for f in os.listdir(data_folder)]
# all_tables = ["scarra", "tfue"]

chat_data = None
logger.info("Reading database: %s", config.DB_PATH)
with sqlite3.connect(config.DB_PATH) as conn:
    for table in all_tables:
        df = pd.read_sql(f"SELECT commenter_id, created_at FROM {table}", conn)
        if chat_data is None:
            chat_data = df
        else:
            chat_data = pd.concat([chat_data, df], axis=0, ignore_index=True)
        logger.info("Read data table: %s", table)

# Convert ISO 8601 time strings to datetime objects
chat_data["created_at"] = pd.to_datetime(chat_data["created_at"], format="ISO8601")

top_commenters = chat_data["commenter_id"].value_counts()
logger.info("Top 50 commenter IDs:\n%s", top_commenters.head(10))

plt.figure(figsize=(10, 6))
top_commenters.head(50).plot(kind='bar', color='skyblue', edgecolor='black')

# Add titles and labels
plt.title("Top 50 most active users' message counts", fontsize=15)
plt.xlabel('Commenter ID', fontsize=12)
plt.ylabel('Message Count', fontsize=12)
plt.xticks(rotation=90, fontsize=8)
plt.yticks(fontsize=10)

# Save the message count plot
output_path = os.path.join(config.DATA_FOLDER, "message_counts_all_time.png")
logger.info("Writing output plot to: %s", output_path)
plt.savefig(output_path)

chat_data['hour'] = chat_data['created_at'].dt.hour
chat_data['day_of_week'] = chat_data['created_at'].dt.dayofweek

plt.figure(figsize=(12, 6))
chat_data['hour'].plot(kind='hist', bins=24, rwidth=0.8, color='magenta', edgecolor='black')
plt.title('Distribution of Chat Messages by Time of Day')
plt.xlabel('Hour of the Day')
plt.ylabel('Number of Messages')
plt.xticks(range(24))
plt.grid(axis='y', linestyle='--', alpha=0.7)

# Save the time of day plot
output_path = os.path.join(config.DATA_FOLDER, "message_counts_daily.png")
logger.info("Writing output plot to: %s", output_path)
plt.savefig(output_path)

plt.figure(figsize=(12, 6))
chat_data['day_of_week'].value_counts().sort_index().plot(kind='bar', color='cyan', edgecolor='black')
plt.title('Distribution of Chat Messages by Day of the Week')
plt.xlabel('Day of the Week')
plt.ylabel('Number of Messages')
plt.xticks(ticks=range(7), labels=['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'])
plt.grid(axis='y', linestyle='--', alpha=0.7)

# Save the day of week plot
output_path = os.path.join(config.DATA_FOLDER, "message_counts_weekly.png")
logger.info("Writing output plot to: %s", output_path)
plt.savefig(output_path)
