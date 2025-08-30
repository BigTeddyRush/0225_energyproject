import yaml
import requests
import psycopg2
import time

def load_config(config_file="config.yaml"):
    """Load configuration from a YAML file."""
    with open(config_file, "r") as file:
        config = yaml.safe_load(file)
    return config

def wait_for_db(host, database, user, password, retries=10, delay=5):
    """
    Wait for the database to become available.
    Retries the connection for a given number of times with a delay.
    """
    for attempt in range(1, retries + 1):
        try:
            conn = psycopg2.connect(host=host, database=database, user=user, password=password)
            conn.close()
            return True
        except psycopg2.OperationalError as e:
            print(f"Database not ready (attempt {attempt}/{retries}). Retrying in {delay} seconds...")
            time.sleep(delay)
    print("Failed to connect to the database after several attempts.")
    return False

def fetch_timestamps(filter_id, region, resolution):
    """Fetch available timestamps from the SMARD API using the numeric filter id."""
    timestamp_url = f"https://smard.api.proxy.bund.dev/app/chart_data/{filter_id}/{region}/index_{resolution}.json"
    print(f"Requesting timestamps from: {timestamp_url}")
    try:
        response = requests.get(timestamp_url, timeout=60)
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        print(f"HTTP error occurred while fetching timestamps for filter {filter_id}, region {region}, resolution {resolution}: {e}")
        return []  # Return an empty list so processing can continue.
    data = response.json()
    return data if isinstance(data, list) else data.get("timestamps", [])

def fetch_timeseries(filter_id, region, resolution, timestamp):
    """Fetch time series data from the SMARD API using a timestamp."""
    timeseries_url = (
        f"hhttps://smard.api.proxy.bund.dev/app/chart_data/{filter_id}/{region}/"
        f"{filter_id}_{region}_{resolution}_{timestamp}.json"
    )
    print(f"Requesting time series data from: {timeseries_url}")
    try:
        response = requests.get(timeseries_url, timeout=60)
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        print(f"HTTP error occurred while fetching time series for filter {filter_id}, region {region}, resolution {resolution}, timestamp {timestamp}: {e}")
        return []  # Return an empty list to indicate no data.
    data = response.json()
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

def insert_data_into_db(timeseries_data, filter_label, filter_id, region, resolution):
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
            filter_label TEXT,
            filter_id INTEGER,
            region TEXT,
            resolution TEXT,
            timestamp BIGINT,
            value FLOAT,
            UNIQUE (filter_id, region, resolution, timestamp)
        );
    """)
    conn.commit()

    inserted_count = 0
    # Use an UPSERT statement: insert new row or update the value if conflict occurs.
    for entry in timeseries_data:
        cur.execute(
            """
            INSERT INTO energy_timeseries (filter_label, filter_id, region, resolution, timestamp, value)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (filter_id, region, resolution, timestamp)
            DO UPDATE SET value = EXCLUDED.value, filter_label = EXCLUDED.filter_label;
            """,
            (filter_label, filter_id, region, resolution, entry["timestamp"], entry["value"])
        )
        inserted_count += 1

    conn.commit()
    cur.close()
    conn.close()
    print(f"Upserted {inserted_count} records for combination {filter_label} ({filter_id}), {region}, {resolution}.")

def main():
    # Load configuration from config.yaml
    config = load_config()
    filter_ids = config.get("FILTER_IDS", [])
    regions = config.get("REGIONS", [])
    resolutions = config.get("RESOLUTIONS", [])
    
    # Options for timestamp selection.
    timestamp_mode = config.get("TIMESTAMP_MODE", "newest")  # "newest", "all", or "specific"
    specific_timestamp = config.get("SPECIFIC_TIMESTAMP", None)  # used only if mode is "specific"

    if not filter_ids or not regions or not resolutions:
        print("Missing configuration values in config.yaml.")
        return

    # Wait for the database to be ready before processing.
    if not wait_for_db(host="db", database="energydata", user="user", password="password"):
        return

    # Iterate over all combinations of FILTER_IDS (each now a tuple), REGIONS, and RESOLUTIONS.
    for filter_tuple in filter_ids:
        # Unpack the tuple: filter_label is a descriptive name; filter_id is the numeric id for API calls.
        filter_label, filter_id = filter_tuple
        for region in regions:
            for resolution in resolutions:
                timestamps = fetch_timestamps(filter_id, region, resolution)
                if not timestamps:
                    print(f"No timestamps found for combination {filter_label} ({filter_id}), {region}, {resolution}.")
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
                    timeseries_data = fetch_timeseries(filter_id, region, resolution, ts)
                    if not timeseries_data:
                        print(f"No time series data for timestamp {ts} for combination {filter_label} ({filter_id}), {region}, {resolution}.")
                        continue
                    prepared_timeseries_data = prepare_data(timeseries_data)
                    if prepared_timeseries_data:
                        insert_data_into_db(prepared_timeseries_data, filter_label, filter_id, region, resolution)
                    else:
                        print("No prepared data to insert for this timestamp.")
    
if __name__ == "__main__":
    main()
