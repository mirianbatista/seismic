import websocket
import json
import logging
import pandas as pd
import os
import time
import sys
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut

websocket_url = 'wss://www.seismicportal.eu/standing_order/websocket'
PING_INTERVAL = 15  
CSV_FILE = "seismic_events.csv"  

logging.basicConfig(stream=sys.stdout, level=logging.INFO)

geolocator = Nominatim(user_agent="seismic_websocket_app")

if not os.path.exists(CSV_FILE):
    pd.DataFrame(columns=["action", "auth", "unid", "time", "mag", "flynn_region", "latitude", "longitude", "city", "state", "country"]).to_csv(CSV_FILE, index=False)

def get_location(lat, lon):
    """
    Get the city, state, and country based on latitude and longitude.
    """
    try:
        location = geolocator.reverse((lat, lon), exactly_one=True, timeout=10)
        if location and location.raw:
            address = location.raw.get("address", {})
            city = address.get("city", address.get("town", address.get("village", "")))
            state = address.get("state", "")
            country = address.get("country", "")
            return city, state, country
    except GeocoderTimedOut:
        logging.warning(f"Geocoder timed out for coordinates ({lat}, {lon})")
    except Exception as e:
        logging.error(f"Error fetching location for ({lat}, {lon}): {e}")
    return "", "", ""

def save_to_csv(event_data):
    """
    Save event data to a CSV file
    """
    try:
        df = pd.DataFrame([event_data])
        df.to_csv(CSV_FILE, mode='a', header=False, index=False)
        logging.info(f"Event saved to CSV: {event_data}")
    except Exception as e:
        logging.error(f"Error saving to CSV: {e}")


def process_message(message):
    """
    Process incoming WebSocket messages and save to CSV
    """
    try:
        data = json.loads(message)
        info = data['data']['properties']
        info['action'] = data['action']
        latitude = data['data']['geometry']['coordinates'][1]
        longitude = data['data']['geometry']['coordinates'][0]

        city, state, country = get_location(latitude, longitude)

        event_data = {
            "action": info.get("action"),
            "auth": info.get("auth"),
            "unid": info.get("unid"),
            "time": info.get("time"),
            "mag": info.get("mag"),
            "flynn_region": info.get("flynn_region"),
            "latitude": latitude,
            "longitude": longitude,
            "city": city,
            "state": state,
            "country": country,
        }
        
        save_to_csv(event_data)
    except Exception:
        logging.exception("Error processing JSON message")


def on_message(ws, message):
    """
    Callback for when a message is received
    """
    process_message(message)


def on_error(ws, error):
    """
    Callback for WebSocket errors
    """
    logging.error(f"WebSocket error: {error}")


def on_close(ws, close_status_code, close_msg):
    """
    Callback for when the WebSocket connection is closed
    """
    logging.info(f"WebSocket connection closed: {close_status_code} - {close_msg}")


def on_open(ws):
    """
    Callback for when the WebSocket connection is opened
    """
    logging.info("WebSocket connection opened.")


def start_websocket():
    """
    Initialize the WebSocket connection
    """
    while True:
        try:
            ws = websocket.WebSocketApp(
                websocket_url,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close,
                on_open=on_open,
            )
            ws.run_forever(ping_interval=PING_INTERVAL)
        except KeyboardInterrupt:
            logging.info("Shutting down WebSocket client...")
            break
        except Exception as e:
            logging.exception("Unexpected error, reconnecting in 5 seconds...")
            time.sleep(5)


if __name__ == "__main__":
    start_websocket()
