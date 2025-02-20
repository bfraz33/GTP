import schedule
import time
from nba_data_extract import fetch_all_live_games  # Your data extraction function

# Function to run periodically (insert data from API to DB)
def update_database():
    print("Running database update task...")
    fetch_all_live_games()  # This will fetch live data and insert into the database

# Schedule to run the task every 15 seconds
schedule.every(15).seconds.do(update_database)

# Keep the scheduler running in main.py
def run_scheduler():
    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == "__main__":
    print("Starting the task scheduler...")
    run_scheduler()  # This will run the scheduled task in the background
