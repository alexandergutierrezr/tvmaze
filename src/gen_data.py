import pandas as pd
import json

def load_json_to_dataframe():
    with open("../tvmaze-analysis/json/tvmaze_data.json", "r") as f:
        data = json.load(f)
    df = pd.json_normalize(data)
    df.to_parquet("../tvmaze-analysis/data/tvmaze_raw.parquet", compression="snappy")
    return df

if __name__ == "__main__":
    df = load_json_to_dataframe()
    print(f"DataFrame with {len(df)} records created.")
