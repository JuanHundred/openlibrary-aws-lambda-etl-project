import requests 
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def fetch_api(api_url):
    """Returns the GET response of the API url."""
    response = None
    try:
        # GET
        logger.info(f"GET {api_url}")
        response = requests.get(api_url, timeout=120)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to GET from {api_url}: {e}")

    logger.info(f"GET {api_url} — status: {response.status_code}, size: {len(response.content)} bytes")   
    return response