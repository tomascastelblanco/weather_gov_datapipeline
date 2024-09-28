# Weather Data Pipeline

## Overview

This project is a weather data pipeline that retrieves weather information from the [**National Weather Service public API**](https://www.weather.gov/documentation/services-web-api) for a selected station and stores it in a PostgreSQL database. The pipeline processes the data and allows you to analyze weather metrics like **average temperature** and **maximum wind speed change** over time.

## Key Features:
- Fetches weather observations from the **National Weather Service public API** for a single station.
- Automatically **creates the required tables** (`stations`, `observations`) if they don't exist.
- The pipeline ensures no duplicate records are inserted during re-runs.
- Demonstrates the **7-day limitation** of the weather data retrieval from the API.
- Displays results for average observed temperature and maximum wind speed change in the Python terminal, with the option to also retrieve this data using SQL queries on the database.

---
## Assumptions:
- The National Weather Service public API allows retrieving data only for the last 7 days, as shown on the API website and demonstrated by the script. 
- The original request for "Average observed temperature for last week (Mon-Sun)" requires data beyond 7 days. Due to the API limitation, we calculate the "Average observed temperature for the last 7 days" instead.
- The "Maximum wind speed change between two consecutive observations" is calculated based on the last 7 days of available data, as this is the maximum data we can retrieve from the API.

## Project Structure

- **`weather_pipeline.py`**: Main script that fetches weather data, processes it, and stores it in the database.
- **`check_Stations.py`**: A helper script that retrieves **3 random station IDs** from the API. You can use any of these IDs to update the station ID in the main script, or you can use an ID you already have.
- **`.env`**: Environment file where you configure the database connection.
- **Database**: The script assumes you will create a **PostgreSQL database** named `weather_db`. The script will create the necessary tables, but the database itself must be created manually.

---

## Prerequisites

1. **PostgreSQL**: Install PostgreSQL on your system.
2. **Python 3.x**: Install Python 3.
3. **Required Python Packages**: Install the necessary Python packages by running:

    ```bash
    pip install -r requirements.txt
    ```

4. **.env file**: Edit the `.env` file in the project directory with the following variables to set up the connection to your PostgreSQL database:
```bash
    DB_HOST=your_host
    DB_PORT=your_port
    DB_NAME=weather_db
    DB_USER=your_user
    DB_PASSWORD=your_password
 ```

---

## Setup Instructions

### 1. Create the Database
- Before running the pipeline, you must create a PostgreSQL database named **`weather_db`**:

    ```sql
    CREATE DATABASE weather_db;
    ```

- You do **not** need to create the tables (`stations` and `observations`). The script will create them automatically if they don't exist.

### 2. Configure the `.env` File
- Make sure to fill in your database credentials in the `.env` file (as shown above). This is crucial for the script to connect to your PostgreSQL instance.

---

## Running the Pipeline

1. **Run the weather pipeline** to fetch and store weather data, it already include an station ID (032HE). Other example IDs: 048HI, 040SE.
   
   ```bash
   python weather_pipeline.py

2. **Retrieving the Data** After running the pipeline and storing the weather data in the PostgreSQL database, you can retrieve the metrics using the SQL queries provided in the `metrics.sql` file.
