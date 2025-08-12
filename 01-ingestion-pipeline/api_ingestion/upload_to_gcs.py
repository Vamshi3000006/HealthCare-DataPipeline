import os
from google.cloud import storage
from datetime import datetime

# ✅ Set your GCS bucket name here
BUCKET_NAME = "healthcare-raw-bucket"

# ✅ Local folder where OpenFDA JSON files are saved
LOCAL_FOLDER = "data"

def upload_latest_file_to_gcs():
    # 🔍 Get list of .json files in the folder
    files = [f for f in os.listdir(LOCAL_FOLDER) if f.endswith(".json")]
    if not files:
        print("❌ No .json files found in folder.")
        return

    # 📌 Get the most recently created .json file
    latest_file = max(files, key=lambda x: os.path.getctime(os.path.join(LOCAL_FOLDER, x)))
    local_path = os.path.join(LOCAL_FOLDER, latest_file)

    # 🧠 Extract timestamp from filename
    timestamp_str = latest_file.replace("openfda_drugs_", "").replace(".json", "")
    dt = datetime.strptime(timestamp_str, "%Y-%m-%d_%H-%M")

    # 📁 Construct GCS object path with partitioning
    gcs_path = f"raw/api/openfda_drugs/year={dt.year}/month={dt.month:02}/day={dt.day:02}/{latest_file}"

    # ☁️ Upload to GCS
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(gcs_path)

    blob.upload_from_filename(local_path)
    print(f"✅ Uploaded: {local_path} → gs://{BUCKET_NAME}/{gcs_path}")

if __name__ == "__main__":
    upload_latest_file_to_gcs()
