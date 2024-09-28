import requests
import random

# base URL to fetch weather stations from the national weather API
BASE_URL = 'https://api.weather.gov/stations'

# function to fetch all weather stations
def fetch_stations():
    # make a GET request to the weather station endpoint
    response = requests.get(BASE_URL)
    # check if the response is successful (status code 200)
    if response.status_code == 200:
        #return the list of station features from the JSON response
        return response.json()['features']
    else:
        # if the request fails, print an error message and return an empty list
        print(f"failed to fetch stations: {response.status_code}")
        return []

# function to check if a given station has latest observations data
def check_observations(station_id):
    # build the URL for fetching the latest observations for a specific station
    url = f"{BASE_URL}/{station_id}/observations/latest"
    # make a GET request to the station's observations endpoint
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        # check if the 'properties' field is present, indicating valid data
        if 'properties' in data:
            return True
        else:
            return False
    else:
        print(f"failed to check observations for station {station_id}: {response.status_code}")
        return False

# main function that handles the process of fetching and checking stations
def main():
    # fetch all available stations
    stations = fetch_stations()
    
    # if no stations were fetched, print a message
    if not stations:
        print("no stations found")
        return

    # randomly select 3 stations from the fetched list 
    random_stations = random.sample(stations, 3) if len(stations) > 3 else stations

    # loop through each random station and check if it has observations data, this is just to double check and not get stations with no data.
    for station in random_stations:
        station_id = station['properties']['stationIdentifier']  # to get the station ID
        station_name = station['properties']['name']  # to get the station name
        has_data = check_observations(station_id)  # to check if the station has observations data
        print(f"station ID: {station_id}, name: {station_name}, has observations data: {has_data}")

if __name__ == "__main__":
    main()
