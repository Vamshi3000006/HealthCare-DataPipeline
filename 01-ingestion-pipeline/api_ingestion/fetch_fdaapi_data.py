import requests
import json
import os
from datetime import datetime

# ✅ Define the API endpoint
API_URL = "https://api.fda.gov/drug/label.json?limit=10"

# ✅ Output directory (local)
OUTPUT_DIR = "api_ingestion/data"
os.makedirs(OUTPUT_DIR, exist_ok=True)

def fetch_openfda_data():
    try:
        print("📡 Fetching data from OpenFDA...")
        response = requests.get(API_URL)
        response.raise_for_status()  # Raises an HTTPError if the status != 200

        json_data = response.json()
        results = json_data.get("results", [])

        if not results:
            print("⚠️ No results found in API response.")
            return

        # ✅ Create a timestamped filename
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")
        filename = f"openfda_drugs_{timestamp}.json"
        filepath = os.path.join(OUTPUT_DIR, filename)

        # ✅ Save only the 'results' list
        with open(filepath, "w") as f:
            json.dump(results, f, indent=2)

        print(f"✅ Data saved to {filepath} ({len(results)} records)")

    except Exception as e:
        print(f"❌ Error occurred: {e}")

# ✅ Run the script
if __name__ == "__main__":
    fetch_openfda_data()
