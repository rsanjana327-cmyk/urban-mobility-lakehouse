# scripts/download_data.py
import os
import requests
import hashlib
from datetime import datetime

RAW_DATA_DIR = "data/raw"
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"

DATASETS = [
    {"year": 2023, "month": 1, "type": "yellow"},
    {"year": 2023, "month": 2, "type": "yellow"},
    {"year": 2023, "month": 3, "type": "yellow"},
]

def calculate_checksum(filepath):
    md5 = hashlib.md5()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            md5.update(chunk)
    return md5.hexdigest()

def download_file(url, filepath):
    print(f"  Downloading: {url}")
    response = requests.get(url, stream=True)

    if response.status_code != 200:
        print(f"  ERROR: HTTP {response.status_code}")
        return False

    total = int(response.headers.get("content-length", 0))
    downloaded = 0

    with open(filepath, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
            downloaded += len(chunk)
            if total:
                pct = (downloaded / total) * 100
                print(f"  Progress: {pct:.1f}% ({downloaded/1024/1024:.1f} MB)", end="\r")

    print(f"\n  Done: {filepath}")
    return True

def main():
    print("=" * 60)
    print("NYC TAXI DATA DOWNLOAD")
    print("=" * 60)

    os.makedirs(RAW_DATA_DIR, exist_ok=True)
    metadata_log = []

    for dataset in DATASETS:
        year  = dataset["year"]
        month = dataset["month"]
        dtype = dataset["type"]

        filename = f"{dtype}_tripdata_{year}-{month:02d}.parquet"
        url      = f"{BASE_URL}/{filename}"

        dest_dir = os.path.join(RAW_DATA_DIR, f"year={year}", f"month={month:02d}")
        os.makedirs(dest_dir, exist_ok=True)
        filepath = os.path.join(dest_dir, filename)

        print(f"\n{'='*60}")
        print(f"Dataset: {dtype} taxi {year}-{month:02d}")
        print(f"{'='*60}")

        if os.path.exists(filepath):
            print(f"  Already exists — skipping")
            checksum = calculate_checksum(filepath)
        else:
            success = download_file(url, filepath)
            if not success:
                continue
            checksum = calculate_checksum(filepath)

        file_size_mb = os.path.getsize(filepath) / (1024 * 1024)

        metadata_log.append({
            "filename"     : filename,
            "filepath"     : filepath,
            "file_size_mb" : round(file_size_mb, 2),
            "checksum_md5" : checksum,
            "downloaded_at": datetime.now().isoformat(),
            "status"       : "success"
        })

        print(f"  Size    : {file_size_mb:.2f} MB")
        print(f"  Checksum: {checksum}")

    print(f"\n{'='*60}")
    print(f"SUMMARY")
    print(f"{'='*60}")
    print(f"Total files : {len(metadata_log)}")
    total_size = sum(f["file_size_mb"] for f in metadata_log)
    print(f"Total size  : {total_size:.2f} MB")
    for f in metadata_log:
        print(f"  checkmark {f['filename']} ({f['file_size_mb']} MB)")

if __name__ == "__main__":
    main()