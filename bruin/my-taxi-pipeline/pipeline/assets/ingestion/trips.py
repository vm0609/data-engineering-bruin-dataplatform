"""@bruin
name: ingestion.trips
type: python
image: python:3.11

connection: duckdb-default

materialization:
  type: table
  strategy: append

columns:
  - name: pickup_datetime
    type: timestamp
    description: "When the meter was engaged"
  - name: dropoff_datetime
    type: timestamp
    description: "When the meter was disengaged"
@bruin"""

import os
import json
import pandas as pd

def materialize():
    start_date = os.environ["BRUIN_START_DATE"]
    end_date = os.environ["BRUIN_END_DATE"]
    taxi_types = json.loads(os.environ["BRUIN_VARS"]).get("taxi_types", ["yellow"])
    raw_vars = os.environ.get("BRUIN_VARS", "{}")
    taxi_types = json.loads(raw_vars).get("taxi_types", ["yellow"])

    # Generate list of months between start and end dates
    months = pd.date_range(start=start_date, end=end_date, freq='MS')

    all_dfs = []
    for taxi_type in taxi_types:
        for month_start in months:
            year = month_start.year
            month = f"{month_start.month:02d}"

            url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{taxi_type}_tripdata_{year}-{month}.parquet"
            
            try:
                print(f"Fetching data from: {url}")
                df = pd.read_parquet(url, engine='pyarrow')

                # Normalize column names to match the asset definition
                df.rename(columns={
                    'tpep_pickup_datetime': 'pickup_datetime',
                    'tpep_dropoff_datetime': 'dropoff_datetime',
                    'lpep_pickup_datetime': 'pickup_datetime',
                    'lpep_dropoff_datetime': 'dropoff_datetime',
                }, inplace=True)

                all_dfs.append(df)
            except Exception as e:
                print(f"Could not fetch data from {url}. Reason: {e}")

    if not all_dfs:
        print("Warning: No data was fetched. Returning an empty DataFrame.")
        return pd.DataFrame()

    final_dataframe = pd.concat(all_dfs, ignore_index=True)

    return final_dataframe