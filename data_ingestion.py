import yaml
import requests
import psycopg2

def load_config(config_file="config.yaml"):
    """Load configuration from a YAML file."""
    with open(config_file, "r") as file:
        config = yaml.safe_load(file)
    return config

def fetch_timestamps(filter_value, region, resolution):
    """Fetch available timestamps from the SMARD API."""
    timestamp_url = f"https://www.smard.de/app/chart_data/{filter_value}/{region}/index_{resolution}.json"
    print(f"Requesting timestamps from: {timestamp_url}")
    response = requests.get(timestamp_url, timeout=60)
    response.raise_for_status()
    data = response.json()
    print("Timestamps raw response:", data)
    # Use the list directly if that's what the API returns; otherwise, try to get the 'timestamps' key.
    return data if isinstance(data, list) else data.get("timestamps", [])

def fetch_timeseries(filter_value, region, resolution, timestamp):
    """Fetch time series data from the SMARD API using a timestamp."""
    timeseries_url = (
        f"https://www.smard.de/app/chart_data/{filter_value}/{region}/"
        f"{filter_value}_{region}_{resolution}_{timestamp}.json"
    )
    print(f"Requesting time series data from: {timeseries_url}")
    response = requests.get(timeseries_url, timeout=60)
    response.raise_for_status()
    data = response.json()
    print("Time series raw response:", data)
    # Use the list directly if that's what the API returns; otherwise, try to get the 'series' key.
    return data if isinstance(data, list) else data.get("series", [])

def prepare_data(raw_data) -> list:
    """
    Prepare raw data into a list of dictionaries with keys 'timestamp' and 'value'.
    This makes it easier to iterate over and insert into the database.
    """
    prepared_data = []
    for raw_data_pair in raw_data:
        # Ensure raw_data_pair is a list/tuple with at least two elements.
        if isinstance(raw_data_pair, (list, tuple)) and len(raw_data_pair) >= 2:
            prepared_data.append({
                "timestamp": raw_data_pair[0],
                "value": raw_data_pair[1]
            })
        else:
            print("Unexpected data format:", raw_data_pair)
    print(f"Prepared {len(prepared_data)} data entries for insertion.")
    return prepared_data

def insert_data_into_db(timeseries_data):
    """Insert or update time series data into a PostgreSQL database."""
    conn = psycopg2.connect(
        host="db",           # Docker Compose service name for the database
        database="energydata",
        user="user",
        password="password"
    )
    cur = conn.cursor()

    # Create table if it doesn't exist with a UNIQUE constraint on timestamp.
    cur.execute("""
        CREATE TABLE IF NOT EXISTS energy_timeseries (
            id SERIAL PRIMARY KEY,
            timestamp BIGINT UNIQUE,
            value FLOAT
        );
    """)
    conn.commit()

    inserted_count = 0
    updated_count = 0

    # Use an UPSERT statement to insert new data or update existing rows if the timestamp exists.
    for entry in timeseries_data:
        cur.execute(
            """
            INSERT INTO energy_timeseries (timestamp, value)
            VALUES (%s, %s)
            ON CONFLICT (timestamp)
            DO UPDATE SET value = EXCLUDED.value;
            """,
            (entry["timestamp"], entry["value"])
        )
        # Note: You could optionally count rows affected by checking cur.rowcount if needed.
        inserted_count += 1

    conn.commit()
    cur.close()
    conn.close()
    print(f"Upserted {inserted_count} records into the database.")

def main():
    # Load configuration from config.yaml
    config = load_config()
    filter_ids = config.get("FILTER_IDS", [])
    regions = config.get("REGIONS", [])
    resolutions = config.get("RESOLUTIONS", [])
    
    if not filter_ids or not regions or not resolutions:
        print("Missing configuration values in config.yaml.")
        return

    # For simplicity, use the first value from each list.
    filter_value = filter_ids[0]
    region = regions[0]
    resolution = resolutions[0]

    # Fetch available timestamps from the SMARD API.
    timestamps = fetch_timestamps(filter_value, region, resolution)
    if not timestamps:
        print("No timestamps found.")
        return

    # Use the latest timestamp for fetching time series data.
    latest_timestamp = timestamps[-1]
    print(f"Latest timestamp: {latest_timestamp}")
    
    timeseries_data = fetch_timeseries(filter_value, region, resolution, latest_timestamp)
    print("Time series data sample:", timeseries_data)

    # Prepare data for database insertion/updating.
    prepared_timeseries_data = prepare_data(timeseries_data)
    
    # Upsert the prepared data into the PostgreSQL database.
    insert_data_into_db(prepared_timeseries_data)
    
if __name__ == "__main__":
    main()
