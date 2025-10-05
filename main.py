# main.py
# Resilient CleanSky backend: FastAPI + OpenAQ v3 + Open-Meteo (weather & 14-day forecast)
# Endpoints:
#  - GET /                         -> hello
#  - GET /countries                -> list countries (cached, fallback sample)
#  - GET /cities?country=XX        -> list cities (cached, fallback)
#  - GET /air-quality?city=Name    -> aggregated + station list (OpenAQ v3, fallback)
#  - GET /measurements?location=.. -> time series for a station
#  - GET /weather?lat=..&lon=..    -> current weather (Open-Meteo)
#  - GET /forecast?lat=..&lon=..&days=14 -> daily forecast (Open-Meteo) or naive if offline
#
# Run: uvicorn main:app --reload
import random
import os
from datetime import datetime, timedelta
import requests
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from dotenv import load_dotenv

load_dotenv()

# --- Configuration and setup ---
OPENAQ_API_KEY = os.getenv("OPENAQ_API_KEY")
HEADERS = {"X-API-Key": OPENAQ_API_KEY} if OPENAQ_API_KEY else {}
OPENMETEO_BASE = "https://api.open-meteo.com/v1/forecast"
CACHE = {}
CACHE_TTL = 3600  # Cache Time-to-Live in seconds

app = FastAPI()

# CORS Middleware
app.add_middleware(
	CORSMiddleware,
	allow_origins=["*"],
	allow_methods=["*"],
	allow_headers=["*"],
)


def log(*args):
	"""Simple logger function for consistent output."""
	print("[clean_sky]", *args)


def fetch_and_cache(url: str, headers: dict = None, fallback_data=None, cache_key: str = None):
	"""
    Fetches data from a URL with caching and error handling.
    If the request fails, it returns the provided fallback data.
    """
	cache_key = cache_key or url
	now = datetime.now()

	# Check cache
	if cache_key in CACHE and (now - CACHE[cache_key]["timestamp"]).total_seconds() < CACHE_TTL:
		return CACHE[cache_key]["data"]

	# Fetch new data
	try:
		resp = requests.get(url, headers=headers, timeout=10)
		resp.raise_for_status()
		data = resp.json()

		# Store in cache
		CACHE[cache_key] = {"data": data, "timestamp": now}
		return data
	except requests.RequestException as e:
		log(f"API request to {url} failed: {e}")
		return fallback_data


# --- Endpoints ---
@app.get("/")
def read_root():
	"""Endpoint for a simple 'hello' message."""
	return {"message": "Welcome to the CleanSky API!"}


@app.get("/countries")
def get_countries():
	"""Lists all countries with air quality data."""
	url = "https://api.openaq.org/v3/countries"
	fallback = {"results": [{"code": "UZ", "name": "Uzbekistan"}, {"code": "US", "name": "United States"}]}
	data = fetch_and_cache(url, headers=HEADERS, fallback_data=fallback, cache_key="countries")

	countries = [{"code": c["code"], "name": c["name"]} for c in data.get("results", [])]
	return {"countries": countries}


@app.get("/cities")
def get_cities(country: str = Query(..., description="ISO 2-letter country code")):
	"""
    Lists cities for a given country with air quality data.
    """
	url = f"https://api.openaq.org/v3/locations?country={country}&limit=1000"
	fallback_results = [{"city": "Tashkent"}, {"city": "New York"}, {"city": "Delhi"}]
	data = fetch_and_cache(url, headers=HEADERS, fallback_data={"results": []}, cache_key=f"cities_{country}")

	cities = set()
	for loc in data.get("results", []):
		if loc.get("boundary") and loc["boundary"].get("city"):
			cities.add(loc["boundary"]["city"])

	sorted_cities = sorted(list(cities))

	if not sorted_cities:
		# If API returns no data, use a fallback
		return {"cities": fallback_results}

	return {"cities": [{"city": c} for c in sorted_cities]}


# -------- Air Quality ----------
@app.get("/air-quality")
def air_quality(city: str = Query(..., description="Name of the city")):
    """
    Returns aggregated air quality data and a list of stations for a given city.
    Uses caching and fetches data from OpenAQ v3.
    """
    cache_key = f"air_quality_{city}"

    # Check for cached data
    now = datetime.now()
    if cache_key in CACHE and (now - CACHE[cache_key]["timestamp"]).total_seconds() < CACHE_TTL:
        return CACHE[cache_key]["data"]

    try:
        # Step 1: find locations for this city
        # Note: OpenAQ v3's location names are not always consistent for a city, so we must fetch and handle each separately.
        loc_url = f"https://api.openaq.org/v3/locations?city={city}&limit=5"
        loc_resp = requests.get(loc_url, headers=HEADERS, timeout=10)
        loc_resp.raise_for_status()
        loc_data = loc_resp.json().get("results", [])

        if not loc_data:
            raise HTTPException(status_code=404, detail="No locations found for this city.")

        aggregated_values = {}
        all_stations = []

        # Step 2: Fetch latest data for each location individually
        for loc in loc_data:
            # The 'location' name is required for the /latest endpoint
            location_name = loc.get("name") or loc.get("location")
            if not location_name:
                continue

            try:
                latest_url = f"https://api.openaq.org/v3/latest?location={location_name}&limit=200"
                latest_resp = requests.get(latest_url, headers=HEADERS, timeout=10)
                latest_resp.raise_for_status()
                latest_data = latest_resp.json().get("results", [])

                for station in latest_data:
                    all_stations.append(station)
                    for m in station.get("measurements", []):
                        param = m["parameter"]
                        value = m["value"]
                        aggregated_values.setdefault(param, []).append(value)
            except requests.RequestException as e:
                log(f"Failed to fetch latest data for location {location_name}: {e}")
                # Continue to the next location if one request fails

        # Step 3: Average pollutant values
        avg = {param: sum(vals) / len(vals) for param, vals in aggregated_values.items() if vals}

        result = {
            "aggregated": avg,
            "last_updated": datetime.utcnow().isoformat() + "Z",
            "locations": all_stations,
        }

        # Cache the result
        CACHE[cache_key] = {"data": result, "timestamp": now}

        return result

    except requests.RequestException as e:
        log(f"OpenAQ /air-quality failed for city {city}: {e}")
        # Fallback with dummy data
        fallback_data = {
            "aggregated": {"pm25": 20, "pm10": 35, "no2": 15},
            "last_updated": datetime.utcnow().isoformat() + "Z",
            "locations": [],
        }
        return fallback_data
    except HTTPException as e:
        return JSONResponse(status_code=e.status_code, content={"detail": e.detail})


@app.get("/measurements")
def measurements(location: str = Query(...), parameter: str = "pm25", limit: int = 100):
	"""
    Returns time-series measurements for a specific location and parameter.
    """
	url = f"https://api.openaq.org/v3/measurements?location={location}&parameter={parameter}&limit={limit}&sort=desc"
	fallback = {
		"results": [{"date": {"utc": (datetime.utcnow() - timedelta(hours=i)).isoformat() + "Z"}, "value": 10 + i % 5}
					for i in range(limit)]
	}
	data = fetch_and_cache(url, headers=HEADERS, fallback_data=fallback,
						   cache_key=f"measurements_{location}_{parameter}")

	return data


@app.get("/weather")
def current_weather(lat: float = Query(...), lon: float = Query(...)):
	"""Returns current weather (temperature, windspeed, weathercode) from Open-Meteo."""
	url = f"{OPENMETEO_BASE}?latitude={lat}&longitude={lon}&current_weather=true&timezone=auto"
	try:
		resp = requests.get(url, timeout=8)
		resp.raise_for_status()
		data = resp.json()
		return {"source": "open-meteo", "current_weather": data.get("current_weather")}
	except requests.RequestException as e:
		log(f"Open-Meteo current weather failed for {lat},{lon}: {e}")
		raise HTTPException(status_code=502, detail=f"Open-Meteo request failed: {e}")


@app.get("/forecast")
def forecast(lat: float = Query(...), lon: float = Query(...), days: int = Query(14, ge=1, le=16)):
	"""Returns daily forecast for 'days' days (default 14) using Open-Meteo daily forecast fields."""
	url = f"{OPENMETEO_BASE}?latitude={lat}&longitude={lon}&daily=temperature_2m_max,temperature_2m_min,precipitation_sum&timezone=auto&forecast_days={days}"
	try:
		resp = requests.get(url, timeout=10)
		resp.raise_for_status()
		data = resp.json()
		return {"source": "open-meteo", "daily": data.get("daily", {})}
	except requests.RequestException as e:
		log(f"Open-Meteo forecast failed for {lat},{lon}: {e}")

		# Fallback: naive forecast
		try:
			cw_url = f"{OPENMETEO_BASE}?latitude={lat}&longitude={lon}&current_weather=true&timezone=auto"
			cw_resp = requests.get(cw_url, timeout=6)
			cw_resp.raise_for_status()
			temp = cw_resp.json().get("current_weather", {}).get("temperature", 20)
		except Exception:
			temp = 20

		base = datetime.utcnow()
		dates = [(base + timedelta(days=i)).date().isoformat() for i in range(days)]
		tmax = [round(temp + 5 + random.uniform(-3, 3), 1) for _ in range(days)]
		tmin = [round(temp - 2 + random.uniform(-3, 3), 1) for _ in range(days)]
		precip = [round(max(0, random.uniform(0, 5)), 1) for _ in range(days)]

		return {"source": "fallback", "daily": {"time": dates, "temperature_2m_max": tmax, "temperature_2m_min": tmin,
												"precipitation_sum": precip}}


# Serve static files (frontend)
app.mount("/", StaticFiles(directory="static", html=True), name="static")
