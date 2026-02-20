# Import libraries
import requests
import json

# Getting secret value from key vault
weatherapikey = dbutils.secrets.get(scope = "key-vault-scope-name", key = "API_key-name_from_Key-Vault")
location = "location-name"

# Base URL from the official documentation
base_url = "http://api.weatherapi.com/v1/"

# Current weather endpoint = f"{base_url}/API-method"
current_weather_url = f"{base_url}/current.json"


# Define the parameters for the API request
params = {
    'key': weatherapikey,
    'q': location,
}

# Make the API request
response = requests.get(current_weather_url, params=params)

# Check if the request was successful
if response.status_code == 200:
    current_weather = response.json()
    print("Current weather:")
    print(json.dumps(current_weather, indent=3))
else:
    print(f"Error: {response.status_code}, {response.text}")