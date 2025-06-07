from pydantic import BaseModel, Field
from dateutil import parser
from typing import Any
import os
import requests


class Tools:
    """
    This is a class for creating a tool for
    retrieving weather information for
    a given location using Open Meteo API.
    """

    class Valves(BaseModel):
        """
        Defines:
        - Open Meteo API key string
        """
        OPEN_METEO_API_KEY: str = Field(
            default=os.getenv(
                "OPEN_METEO_API_KEY",
                "",
            ),
            description="Open Meteo API key string.",
        )
        LIMIT: int = Field(
            default=os.getenv(
                "LIMIT",
                4096,
            ),
            description="Search results character limit.",
        )

    def __init__(self):
        """
        Constructor method
        """
        self.wmo_codes_to_descriptions = {
            0: "Clear sky",
            1: "Mainly clear",
            2: "Partly cloudy",
            3: "Overcast",
            45: "Fog",
            48: "Depositing rime fog",
            51: "Light drizzle",
            53: "Moderate drizzle",
            55: "Dense drizzle",
            56: "Light freezing drizzle",
            57: "Dense freezing drizzle",
            61: "Slight rain",
            63: "Moderate rain",
            65: "Heavy rain",
            66: "Light freezing rain",
            67: "Heavy freezing rain",
            71: "Slight snow fall",
            73: "Moderate snow fall",
            75: "Heavy snow fall",
            77: "Snow grains",
            80: "Slight rain showers",
            81: "Moderate rain showers",
            82: "Violent rain showers",
            85: "Slight snow showers and heavy",
            86: "Heavy snow showers",
            95: "Thunderstorm",
            96: "Thunderstorm with slight hail",
            99: "Thunderstorm with heavy hail",
        }
        self.valves = self.Valves()

    def _translate_location_name(self, name: str) -> str:
        """
        Translates a location name to its English equivalent for API requests.
        :param name: The name of the location to translate.
        :return: The translated location name in English.
        """
        url = "https://translate.google.com/translate_a/single"
        payload = {
            "q": name,
            "client": "gtx",
            "sl": "auto",
            "tl": "en",
            "dt": "t",
        }
        headers = {"Content-Type": "application/json"}

        translation = ""

        try:
            response = requests.get(url, params=payload, headers=headers)
            response.raise_for_status()
            result = response.json()

            # Google Translate returns a list
            if result and isinstance(result, list):
                # Check if the first element is a list and has at least one item
                if result[0] and isinstance(result[0], list):
                    # Check if the first element is a list and has at least one item
                    if result[0][0] and isinstance(result[0][0], list):
                        # Extract the translated text
                        if isinstance(result[0][0][0], str):
                            translation = result[0][0][0]
        except requests.RequestException as e:
            print(f"Error translating location name: {str(e)}")

        return translation

    def _get_location_coordinates(self, location: str) -> tuple[str, str, str]:
        """
        Gets the latitude, longitude, and timezone of a given location.
        :param location: The name of the location to get coordinates for (in any language).
        :return: A tuple containing latitude, longitude, and timezone of the location.
        :raises: Exception if the location cannot be found or if there is an error in the request.
        """
        geocoding_base_url = "https://geocoding-api.open-meteo.com/v1/search"
        geocoding_params = {
            "apikey": self.valves.OPEN_METEO_API_KEY,
            "name": location,
            "count": "1",
            "language": "en",
            "format": "json",
        }

        latitude = ""
        longitude = ""
        timezone = ""

        try:
            response = requests.get(geocoding_base_url, params=geocoding_params)
            response.raise_for_status()  # Raise HTTPError for bad responses (4xx and 5xx)

            if response.status_code != 200 or len(response.json()) == 1:
                print(f"Error fetching geolocation data for {location}")
            else:
                data = response.json()["results"][0]

                latitude = data["latitude"]
                longitude = data["longitude"]
                timezone = data["timezone"]
        except requests.RequestException as e:
            print(f"Error fetching location data: {str(e)}")

        return latitude, longitude, timezone

    def _normalize_date(self, date_str: str) -> str:
        """
        Converts any date format to YYYY-MM-DD format.
        :param date_str: The date string to normalize.
        :return: The date string normalized in YYYY-MM-DD format or None if parsing fails.
        """
        norm_date = ""

        if date_str.strip():
            try:
                # Parse the date string automatically
                parsed_date = parser.parse(date_str.strip())
                # Format as YYYY-MM-DD
                norm_date = parsed_date.strftime("%Y-%m-%d")
            except (ValueError, TypeError):
                print(f"Could not parse date: {date_str}")

        return norm_date

    def get_weather_info(self, location: str, date: str) -> dict[str, Any]:
        """
        Gets weather information for a given location on a specific date.
        :param location: The name of the location to get the weather for (in any language).
        :param date: The date for which to get the weather information (in any format).
        :return: The weather in the given location on the date of interest or an error message.
        """
        print(f"Tool: {__name__}")
        print(f"Location: {location}")
        print(f"Date: {date}")

        weather_info = {
            "location": location,
            "date": date,
            "context": {
                "hours": [],
                "weather_descriptions": [],
                "temperatures": [],
                "humidities": [],
                "wind_speeds": [],
            }
        }

        latitude, longitude, timezone = self._get_location_coordinates(self._translate_location_name(location))

        if latitude and longitude and timezone:
            base_url = "http://api.open-meteo.com/v1/forecast"
            params = {
                "latitude": latitude,
                "longitude": longitude,
                "timezone": timezone,
                "start_date": self._normalize_date(date),
                "end_date": self._normalize_date(date),
                "hourly": "weather_code,temperature_2m,relative_humidity_2m,wind_speed_10m",
            }

            if self.valves.OPEN_METEO_API_KEY:
                params["apikey"] = self.valves.OPEN_METEO_API_KEY

            try:
                response = requests.get(base_url, params=params)
                response.raise_for_status()  # Raise HTTPError for bad responses (4xx and 5xx)

                if response.status_code != 200 or len(response.json()) == 1:
                    print(
                        f"Error fetching weather data for coordinates {latitude}, {longitude}"
                    )
                else:
                    print(response.json())
                    units = response.json()["hourly_units"]
                    data = response.json()["hourly"]

                    weather_info["context"]["hours"] = data["time"]
                    weather_info["context"]["weather_descriptions"] = [
                        self.wmo_codes_to_descriptions[code] for code in data["weather_code"]
                    ]
                    weather_info["context"]["temperatures"] = [
                        f'{temp}{units["temperature_2m"]}' for temp in data["temperature_2m"]
                    ]
                    weather_info["context"]["humidities"] = [
                        f'{humidity}{units["relative_humidity_2m"]}' for humidity in data["relative_humidity_2m"]
                    ]
                    weather_info["context"]["wind_speeds"] = [
                        f'{wind_speed}{units["wind_speed_10m"]}' for wind_speed in data["wind_speed_10m"]
                    ]
            except requests.RequestException as e:
                print(f"Error fetching weather data: {str(e)}")

        return weather_info

    def get_current_weather_info(self, location: str) -> dict[str, Any]:
        """
        Gets current weather for a given location.
        :param location: The name of the location to get the weather for (in any language).
        :return: The current weather in the given location or an error message.
        """
        print(f"Tool: {__name__}")
        print(f"Location: {location}")

        weather_info = {
            "location": location,
            "context": {
                "weather_description": "",
                "temperature": "",
                "humidity": "",
                "wind_speed": "",
            }
        }

        latitude, longitude, timezone = self._get_location_coordinates(self._translate_location_name(location))

        if latitude and longitude and timezone:
            base_url = "http://api.open-meteo.com/v1/forecast"
            params = {
                "latitude": latitude,
                "longitude": longitude,
                "timezone": timezone,
                "current": "weather_code,temperature_2m,relative_humidity_2m,wind_speed_10m",
            }

            if self.valves.OPEN_METEO_API_KEY:
                params["apikey"] = self.valves.OPEN_METEO_API_KEY

            try:
                response = requests.get(base_url, params=params)
                response.raise_for_status()  # Raise HTTPError for bad responses (4xx and 5xx)

                if response.status_code != 200 or len(response.json()) == 1:
                    print(
                        f"Error fetching weather data for coordinates {latitude}, {longitude}"
                    )
                else:
                    units = response.json()["current_units"]
                    data = response.json()["current"]

                    weather_info["context"]["weather_description"] = self.wmo_codes_to_descriptions.get(
                        data["weather_code"], "Unknown"
                    )
                    weather_info["context"]["temperature"] = f'{data["temperature_2m"]}{units["temperature_2m"]}'
                    weather_info["context"]["humidity"] = f'{data["relative_humidity_2m"]}{units["relative_humidity_2m"]}'
                    weather_info["context"]["wind_speed"] = f'{data["wind_speed_10m"]}{units["wind_speed_10m"]}'
            except requests.RequestException as e:
                print(f"Error fetching weather data: {str(e)}")

        return weather_info

if __name__ == "__main__":
    tool = Tools()
    # print(f"City name to get weather for: {tool._translate_location_name('Αθήνα')}")
    # print(tool.get_weather_info("Βόλο", "31 Μαρτίου 2023"))
    print(tool.get_current_weather_info("Βόλο"))