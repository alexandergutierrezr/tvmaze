import os
import requests
import json
from datetime import datetime, timedelta
import time

def fetch_tvmaze_data():
    base_url = "http://api.tvmaze.com/schedule/web"
    start_date = datetime(2024, 1, 1)
    end_date = datetime(2024, 1, 31)
    all_data = []
    
    # Crear directorio de salida si no existe
    output_dir = "../tvmaze-analysis/json"
    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, "tvmaze_data.json")

    while start_date <= end_date:
        date_str = start_date.strftime("%Y-%m-%d")
        print(f"Fetching data for {date_str}")
        try:
            response = requests.get(f"{base_url}?date={date_str}")
            response.raise_for_status()
            data = response.json()
            all_data.extend(data)
        except requests.RequestException as e:
            print(f"Failed to fetch data for {date_str}: {e}")
        time.sleep(1)  # Retardo para evitar bloqueos
        start_date += timedelta(days=1)

    # Guardar datos en el archivo JSON
    with open(output_file, "w") as f:
        json.dump(all_data, f, indent=4)
    print(f"Data saved to {output_file}")

if __name__ == "__main__":
    fetch_tvmaze_data()

