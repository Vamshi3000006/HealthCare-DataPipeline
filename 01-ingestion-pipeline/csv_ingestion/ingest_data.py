import os
import json
import requests
import zipfile
import pandas as pd
from datetime import datetime
from google.cloud import storage

# List of CMS ZIP file URLs (you can add more later)
zip_urls = [
    "https://data.cms.gov/sites/default/files/2023-04/c3d8a962-c6b8-4a59-adb5-f0495cc81fda/Outpatient.zip"
]

# GCS bucket name
raw_bucket = "healthcare-raw-bucket"

# Path to processed files tracker
TRACKER_PATH = "metadata/processed_files.json"

# ----------------------------
# TRACKER FUNCTIONS
# ----------------------------

def load_processed_tracker():
    if not os.path.exists(TRACKER_PATH):
        return []
    with open(TRACKER_PATH, "r") as f:
        data = json.load(f)
    return data.get("processed", [])

def update_processed_tracker(filename):
    processed = load_processed_tracker()
    if filename not in processed:
        processed.append(filename)
        with open(TRACKER_PATH, "w") as f:
            json.dump({"processed": processed}, f, indent=4)

# ----------------------------
# INGESTION FUNCTIONS
# ----------------------------

def download_zip_file(url, download_dir="downloads"):
    os.makedirs(download_dir, exist_ok=True)
    local_zip_path = os.path.join(download_dir, url.split("/")[-1])
    print(f"🔽 Downloading: {url}")
    
    response = requests.get(url)
    if response.status_code == 200:
        with open(local_zip_path, "wb") as f:
            f.write(response.content)
        print(f"✅ Saved to: {local_zip_path}")
        return local_zip_path
    else:
        print(f"❌ Failed to download {url}")
        return None

def extract_csv_from_zip(zip_path, extract_dir="downloads/extracted"):
    os.makedirs(extract_dir, exist_ok=True)
    extracted_files = []
    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        zip_ref.extractall(extract_dir)
        extracted_files = zip_ref.namelist()
    full_paths = [os.path.join(extract_dir, f) for f in extracted_files]
    print(f"📂 Extracted files: {full_paths}")
    return full_paths

def generate_metadata(csv_path, source_url):
    df = pd.read_csv(csv_path, sep="|", low_memory=False)
    metadata = {
        "filename": os.path.basename(csv_path),
        "filesize_bytes": os.path.getsize(csv_path),
        "num_records": len(df),
        "num_columns": df.shape[1],
        "ingestion_timestamp": datetime.utcnow().isoformat() + "Z",
        "source_url": source_url
    }
    print(f"📝 Metadata for {csv_path}:")
    print(metadata)
    metadata_path = csv_path + ".json"
    with open(metadata_path, "w") as f:
        json.dump(metadata, f, indent=4)
    return metadata_path

def upload_to_gcs(bucket_name, local_file_path, destination_blob_name):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(local_file_path)
    print(f"☁️ Uploaded {local_file_path} → gs://{bucket_name}/{destination_blob_name}")

# ----------------------------
# MAIN FLOW
# ----------------------------

if __name__ == "__main__":
    processed_files = load_processed_tracker()

    for url in zip_urls:
        zip_path = download_zip_file(url)
        if zip_path:
            extracted_csvs = extract_csv_from_zip(zip_path)
            for csv_file in extracted_csvs:
                base_name = os.path.basename(csv_file)
                if base_name in processed_files:
                    print(f"⏭️ Skipping already processed file: {base_name}")
                    continue

                # 1. Create metadata
                metadata_path = generate_metadata(csv_file, url)

                # 2. Upload CSV to GCS
                csv_blob_name = f"raw/{base_name}"
                upload_to_gcs(raw_bucket, csv_file, csv_blob_name)

                # 3. Upload metadata to GCS
                metadata_blob_name = f"metadata/{os.path.basename(metadata_path)}"
                upload_to_gcs(raw_bucket, metadata_path, metadata_blob_name)

                # 4. Update tracker
                update_processed_tracker(base_name)
