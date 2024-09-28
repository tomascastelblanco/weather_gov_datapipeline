import requests
import psycopg2
from datetime import datetime, timedelta
import pytz
import os
from dotenv import load_dotenv

# load environment variables from .env file
load_dotenv()

# catabase connection details from .env
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')

# station ID and Base URL for the API
STATION_ID = '032HE'  # replace with another station ID if needed
BASE_URL = 'https://api.weather.gov'

# function to round values
def round_value(value):
    """rounds a value to two decimal places or returns None if invalid."""
    try:
        return round(float(value), 2)
    except (TypeError, ValueError):
        return None

# establish connection to the PostgreSQL database
def get_db_connection():
    """connects to the PostgreSQL database using the credentials from .env file."""
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )
    return conn

# create necessary tables if they do not already exist
def create_tables_if_not_exist(conn):
    """creates the 'stations' and 'observations' tables if they do not already exist."""
    with conn.cursor() as cur:
        # create 'stations' table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS stations (
                station_id VARCHAR(255) PRIMARY KEY,
                name VARCHAR(255),
                timezone VARCHAR(255),
                latitude FLOAT,
                longitude FLOAT
            );
        """)
        
        # create 'observations' table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS observations (
                station_id VARCHAR(255),
                timestamp TIMESTAMPTZ,
                temperature FLOAT,
                wind_speed FLOAT,
                humidity FLOAT,
                PRIMARY KEY (station_id, timestamp),
                FOREIGN KEY (station_id) REFERENCES stations (station_id)
            );
        """)
        
        conn.commit()

# fetch station information from the API
def fetch_station_info(station_id):
    """fetches station details such as name, timezone, latitude, and longitude from the API."""
    url = f"{BASE_URL}/stations/{station_id}"
    response = requests.get(url)
    
    if response.status_code == 200:
        data = response.json()
        properties = data.get('properties', {})
        
        if not properties:
            print("no properties found for station.")
            return None
        
        # Extract coordinates and properties
        geometry = data.get('geometry', {})
        coordinates = geometry.get('coordinates', [None, None])
        latitude = coordinates[1]
        longitude = coordinates[0]

        # handle missing coordinates
        if latitude is None or longitude is None:
            print("coordinates not available.")
        
        station_info = {
            'station_id': station_id,
            'name': properties.get('name'),
            'timezone': properties.get('timeZone'),
            'latitude': latitude,
            'longitude': longitude
        }
        return station_info
    else:
        print(f"failed to fetch station info: {response.status_code}")
        return None

# insert station data into the database
def insert_station(conn, station_info):
    """inserts station details into the 'stations' table."""
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO stations (station_id, name, timezone, latitude, longitude)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (station_id) DO NOTHING;
        """, (
            station_info['station_id'],
            station_info['name'],
            station_info['timezone'],
            station_info['latitude'],
            station_info['longitude']
        ))
        conn.commit()

# fetch weather observations (try fetching all available data)
def fetch_all_available_observations(station_id):
    """
    attempts to fetch all available weather data from the API.
    since the API might only return recent data (limited to 7 days as mention before), it will demonstrate the limitation.
    """
    end_time = datetime.now(tz=pytz.UTC)  # current date and time
    # set start time far in the past to attempt fetching all available data
    start_time = end_time - timedelta(days=365*10)  # attempt to fetch up to 10 years of data

    print(f"Attempting to fetch data from {start_time.date()} to {end_time.date()}")

    url = f"{BASE_URL}/stations/{station_id}/observations"
    params = {
        'start': start_time.isoformat(),
        'end': end_time.isoformat()
    }
    response = requests.get(url, params=params)
    
    if response.status_code == 200:
        observations = response.json().get('features', [])
        print(f"observations fetched: {len(observations)} entries (API likely limited).")
        return observations
    else:
        print(f"failed to fetch observations: {response.status_code}")
        return []

# insert observations into the database
def process_and_insert_observations(conn, station_id, observations):
    """inserts weather observations into the 'observations' table."""
    with conn.cursor() as cur:
        for obs in observations:
            properties = obs['properties']
            timestamp = properties['timestamp']
            temperature = round_value(properties.get('temperature', {}).get('value'))
            wind_speed = round_value(properties.get('windSpeed', {}).get('value'))
            humidity = round_value(properties.get('relativeHumidity', {}).get('value'))

            print(f"Observations: Timestamp: {timestamp}, Temperature: {temperature}, Wind: {wind_speed}, Humidity: {humidity}")

            cur.execute("""
                INSERT INTO observations (station_id, timestamp, temperature, wind_speed, humidity)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (station_id, timestamp) DO NOTHING;
            """, (
                station_id,
                timestamp,
                temperature,
                wind_speed,
                humidity
            ))
        conn.commit()

# fnction to calculate average temperatur for the last 7 days
def calculate_average_temperature(conn, station_id):
    """calculates and prints the average temperature for the last 7 days for a specific station."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT AVG(temperature) AS average_temperature
            FROM observations
            WHERE station_id = %s
            AND timestamp >= NOW() - INTERVAL '7 days';
        """, (station_id,))
        result = cur.fetchone()
        print(f"Average temperature for the last 7 days: {result[0]:.2f}°C" if result[0] is not None else "No temperature data available.")

# function to calculate max wind speed change between two consecutive observations for the last 7 days
def calculate_max_wind_speed_change(conn, station_id):
    """calculates and prints the maximum wind speed change between two consecutive observations in the last 7 days."""
    with conn.cursor() as cur:
        cur.execute("""
            WITH wind_speed_changes AS (
                SELECT timestamp, wind_speed,
                       LAG(wind_speed) OVER (ORDER BY timestamp) AS previous_wind_speed
                FROM observations
                WHERE station_id = %s
                AND timestamp >= NOW() - INTERVAL '7 days'
            )
            SELECT MAX(ABS(wind_speed - previous_wind_speed)) AS max_wind_speed_change
            FROM wind_speed_changes
            WHERE previous_wind_speed IS NOT NULL;
        """, (station_id,))
        result = cur.fetchone()
        print(f"maximum wind speed change in the last 7 days: {result[0]:.2f} m/s" if result[0] is not None else "no wind speed data available.")

# main function to call all functions
def main():
    """main function to handle the entire process: create tables, fetch data, and insert it into the database."""
    # connect to the database
    conn = get_db_connection()

    # ensure the necessary tables are created
    create_tables_if_not_exist(conn)

    # fetch and insert station info
    station_info = fetch_station_info(STATION_ID)
    if station_info:
        insert_station(conn, station_info)

    # fetch all available observations and insert into the database
    observations = fetch_all_available_observations(STATION_ID)
    if observations:
        process_and_insert_observations(conn, STATION_ID, observations)

    # after fetching and inserting data, calculate and print the required metrics
    print("\n--- calculating metrics for the last 7 days ---\n")
    calculate_average_temperature(conn, STATION_ID)
    calculate_max_wind_speed_change(conn, STATION_ID)

    # close the database connection
    conn.close()

if __name__ == "__main__":
    main()
