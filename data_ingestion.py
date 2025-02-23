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
    # Return list directly if API returns a list, or the value of the "timestamps" key.
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
    # Return list directly if API returns a list, or the value of the "series" key.
    return data if isinstance(data, list) else data.get("series", [])

def prepare_data(raw_data) -> list:
    """
    Prepare raw data into a list of dictionaries with keys 'timestamp' and 'value'.
    Each raw data entry is expected to be a list/tuple with at least two elements.
    """
    prepared_data = []
    for raw_data_pair in raw_data:
        if isinstance(raw_data_pair, (list, tuple)) and len(raw_data_pair) >= 2:
            prepared_data.append({
                "timestamp": raw_data_pair[0],
                "value": raw_data_pair[1]
            })
        else:
            print("Unexpected data format:", raw_data_pair)
    print(f"Prepared {len(prepared_data)} data entries for insertion.")
    return prepared_data

def insert_data_into_db(timeseries_data, filter_value, region, resolution):
    """Upsert time series data into a PostgreSQL database with composite keys."""
    conn = psycopg2.connect(
        host="db",           # Docker Compose service name for the database
        database="energydata",
        user="user",
        password="password"
    )
    cur = conn.cursor()

    # Create table if it doesn't exist with a composite UNIQUE constraint.
    cur.execute("""
        CREATE TABLE IF NOT EXISTS energy_timeseries (
            id SERIAL PRIMARY KEY,
            filter_value TEXT,
            region TEXT,
            resolution TEXT,
            timestamp BIGINT,
            value FLOAT,
            UNIQUE (filter_value, region, resolution, timestamp)
        );
    """)
    conn.commit()

    inserted_count = 0
    # Use an UPSERT statement: insert new row or update the value if conflict occurs.
    for entry in timeseries_data:
        cur.execute(
            """
            INSERT INTO energy_timeseries (filter_value, region, resolution, timestamp, value)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (filter_value, region, resolution, timestamp)
            DO UPDATE SET value = EXCLUDED.value;
            """,
            (filter_value, region, resolution, entry["timestamp"], entry["value"])
        )
        inserted_count += 1

    conn.commit()
    cur.close()
    conn.close()
    print(f"Upserted {inserted_count} records for combination {filter_value}, {region}, {resolution}.")

def main():
    # Load configuration from config.yaml
    config = load_config()
    filter_ids = config.get("FILTER_IDS", [])
    regions = config.get("REGIONS", [])
    resolutions = config.get("RESOLUTIONS", [])
    
    # New options for timestamp selection:
    timestamp_mode = config.get("TIMESTAMP_MODE", "newest")  # options: "newest", "all", "specific"
    specific_timestamp = config.get("SPECIFIC_TIMESTAMP", None)  # used only if mode is "specific"

    if not filter_ids or not regions or not resolutions:
        print("Missing configuration values in config.yaml.")
        return

    # Iterate over all combinations of FILTER_IDS, REGIONS, and RESOLUTIONS.
    for filter_value in filter_ids:
        for region in regions:
            for resolution in resolutions:
                print(f"\nProcessing combination: Filter {filter_value}, Region {region}, Resolution {resolution}")
                timestamps = fetch_timestamps(filter_value, region, resolution)
                if not timestamps:
                    print(f"No timestamps found for combination {filter_value}, {region}, {resolution}.")
                    continue

                # Choose which timestamps to process based on TIMESTAMP_MODE.
                if timestamp_mode == "all":
                    timestamps_to_process = timestamps
                elif timestamp_mode == "specific":
                    if specific_timestamp:
                        timestamps_to_process = [specific_timestamp]
                    else:
                        print("No SPECIFIC_TIMESTAMP provided in config, defaulting to newest.")
                        timestamps_to_process = [timestamps[-1]]
                else:  # default to newest
                    timestamps_to_process = [timestamps[-1]]
                
                # Process each selected timestamp.
                for ts in timestamps_to_process:
                    print(f"Processing timestamp: {ts}")
                    timeseries_data = fetch_timeseries(filter_value, region, resolution, ts)
                    if not timeseries_data:
                        print(f"No time series data for timestamp {ts} for combination {filter_value}, {region}, {resolution}.")
                        continue
                    prepared_timeseries_data = prepare_data(timeseries_data)
                    if prepared_timeseries_data:
                        insert_data_into_db(prepared_timeseries_data, filter_value, region, resolution)
                    else:
                        print("No prepared data to insert for this timestamp.")
    
if __name__ == "__main__":
    main()
