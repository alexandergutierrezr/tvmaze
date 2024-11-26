import sqlite3
import pandas as pd

def store_to_db():
    df = pd.read_parquet("../tvmaze-analysis/data/tvmaze_cleaned.snappy.parquet")
    conn = sqlite3.connect("../tvmaze-analysis/db/tvmaze.db")
    df.to_sql("shows", conn, if_exists="replace", index=False)
    print("Data stored in SQLite database.")

if __name__ == "__main__":
    store_to_db()
