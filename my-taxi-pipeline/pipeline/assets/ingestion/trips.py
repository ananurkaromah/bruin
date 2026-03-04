"""@bruin
name: ingestion.trips
type: python
connection: duckdb-default
image: python:3.11

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
from datetime import datetime
from dateutil.relativedelta import relativedelta

def materialize():
    start_date = datetime.fromisoformat(os.environ["BRUIN_START_DATE"].replace("Z", ""))
    end_date = datetime.fromisoformat(os.environ["BRUIN_END_DATE"].replace("Z", ""))

    taxi_types = json.loads(os.environ["BRUIN_VARS"]).get("taxi_types", ["yellow"])

    all_dfs = []

    current = start_date
    while current < end_date:
        year = current.year
        month = str(current.month).zfill(2)

        for taxi_type in taxi_types:
            url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{taxi_type}_tripdata_{year}-{month}.parquet"

            try:
                df = pd.read_parquet(url)
                df["taxi_type"] = taxi_type
                all_dfs.append(df)
            except Exception as e:
                print(f"Failed to load {url}: {e}")

        current += relativedelta(months=1)

    if not all_dfs:
        return pd.DataFrame()

    final_dataframe = pd.concat(all_dfs, ignore_index=True)

    return final_dataframe