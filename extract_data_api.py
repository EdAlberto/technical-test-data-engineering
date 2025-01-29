import requests
import logging
import json
import time
from typing import List, Dict
import unittest

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

BASE_URL = "https://api.spaceflightnewsapi.net/v4"
ENDPOINTS = ["articles", "blogs", "reports", "info"]
RATE_LIMIT_SLEEP = 1  # Time to sleep between requests to avoid rate limits
MAX_RECORDS = 30  # Maximum records to fetch per endpoint

# Function to fetch paginated data
def fetch_paginated_data(endpoint: str) -> List[Dict]:
    data = []
    url = f"{BASE_URL}/{endpoint}"
    page = 1
    while url and len(data) < MAX_RECORDS:
        try:
            logger.info(f"Fetching page {page} from {endpoint}...")
            response = requests.get(url)
            response.raise_for_status()
            result = response.json()
            
            logger.debug(f"API response structure: {result}")

            if "results" in result:
                data.extend(result["results"])
                url = result.get("next")  # Get the next page URL
            else:
                data.extend(result)
                url = None  # No pagination

            if len(data) >= MAX_RECORDS:
                data = data[:MAX_RECORDS]  # Trim data to MAX_RECORDS if exceeded
                break

            page += 1
            time.sleep(RATE_LIMIT_SLEEP)
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching data from {endpoint}: {e}")
            break

    logger.info(f"Completed fetching data from {endpoint}. Total records: {len(data)}")
    return data


# Function to remove duplicates from fetched data
def deduplicate_data(data: List[Dict], key: str = "id") -> List[Dict]:
    seen = set()
    unique_data = []
    for item in data:
        if not isinstance(item, dict):
            logger.warning(f"Skipping non-dict item: {item}")
            continue
        if key in item and item[key] not in seen:
            unique_data.append(item)
            seen.add(item[key])
    logger.info(f"Deduplicated data: {len(unique_data)} unique records remaining.")
    return unique_data


# Main extraction function
def extract_data() -> Dict[str, List[Dict]]:
    all_data = {}
    for endpoint in ENDPOINTS:
        raw_data = fetch_paginated_data(endpoint)
        deduplicated = deduplicate_data(raw_data)
        all_data[endpoint] = deduplicated

    logger.info("Data extraction completed for all endpoints.")
    return all_data

# Unit tests
class TestPipeline(unittest.TestCase):
    def test_deduplicate_data(self):
        test_data = [
            {"id": 1, "name": "Item 1"},
            {"id": 2, "name": "Item 2"},
            {"id": 1, "name": "Item 1"},
        ]
        expected = [
            {"id": 1, "name": "Item 1"},
            {"id": 2, "name": "Item 2"},
        ]
        self.assertEqual(deduplicate_data(test_data), expected)

    def test_fetch_paginated_data(self):
        # Test with a mock endpoint or a real endpoint if available
        # This is more complex and might require mocking requests.get
        pass

if __name__ == "__main__":
    # Run extraction
    extracted_data = extract_data()

    # Save data to JSON files for each endpoint in a PySpark-compatible format
    for endpoint, data in extracted_data.items():
        # Save each record as a JSON object in a list
        with open(f"{endpoint}.json", "w") as f:
            json.dump(data, f, indent=4)
        logger.info(f"Saved data for {endpoint} to {endpoint}.json")

    # Run unit tests
    unittest.main(argv=[''], exit=False)
