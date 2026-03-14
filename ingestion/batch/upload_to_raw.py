# ingestion/batch/upload_to_raw.py
import os
import boto3
import hashlib
import json
from datetime import datetime
from botocore.client import Config

# MinIO Connection
MINIO_ENDPOINT   = "http://localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin123"
BUCKET_NAME      = "lakehouse"
RAW_PREFIX       = "raw"
LOCAL_RAW_DIR    = "data/raw"
METADATA_FILE    = "data/ingestion_metadata.json"

def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url          = MINIO_ENDPOINT,
        aws_access_key_id     = MINIO_ACCESS_KEY,
        aws_secret_access_key = MINIO_SECRET_KEY,
        config                = Config(signature_version="s3v4"),
        region_name           = "us-east-1"
    )

def load_metadata():
    if os.path.exists(METADATA_FILE):
        with open(METADATA_FILE, "r") as f:
            return json.load(f)
    return {"ingested_files": []}

def save_metadata(metadata):
    os.makedirs(os.path.dirname(METADATA_FILE), exist_ok=True)
    with open(METADATA_FILE, "w") as f:
        json.dump(metadata, f, indent=2)

def calculate_checksum(filepath):
    md5 = hashlib.md5()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            md5.update(chunk)
    return md5.hexdigest()

def file_already_ingested(metadata, filename, checksum):
    for record in metadata["ingested_files"]:
        if record["filename"] == filename and record["checksum"] == checksum:
            return True
    return False

def main():
    print("=" * 60)
    print("RAW LAYER INGESTION — Urban Mobility Lakehouse")
    print("=" * 60)

    s3       = get_s3_client()
    metadata = load_metadata()
    uploaded = 0
    skipped  = 0

    for root, dirs, files in os.walk(LOCAL_RAW_DIR):
        for filename in files:
            if not filename.endswith(".parquet"):
                continue

            local_path = os.path.join(root, filename)
            checksum   = calculate_checksum(local_path)

            # Idempotency check
            if file_already_ingested(metadata, filename, checksum):
                print(f"\n  SKIP (already ingested): {filename}")
                skipped += 1
                continue

            # Build S3 key with Hive partitioning
            rel_path = os.path.relpath(local_path, LOCAL_RAW_DIR)
            s3_key   = f"{RAW_PREFIX}/{rel_path}".replace("\\", "/")

            print(f"\n  Uploading : {filename}")
            print(f"  S3 Key    : s3://{BUCKET_NAME}/{s3_key}")

            try:
                s3.upload_file(
                    local_path,
                    BUCKET_NAME,
                    s3_key,
                    ExtraArgs={
                        "Metadata": {
                            "source"      : "nyc_tlc",
                            "ingested_at" : datetime.now().isoformat(),
                            "pipeline_ver": "1.0.0"
                        }
                    }
                )

                file_size_mb = os.path.getsize(local_path) / (1024 * 1024)

                metadata["ingested_files"].append({
                    "filename"    : filename,
                    "s3_key"      : s3_key,
                    "checksum"    : checksum,
                    "file_size_mb": round(file_size_mb, 2),
                    "ingested_at" : datetime.now().isoformat(),
                    "status"      : "success",
                    "source"      : "nyc_tlc_yellow_taxi"
                })

                save_metadata(metadata)
                print(f"  Status    : uploaded successfully")
                uploaded += 1

            except Exception as e:
                print(f"  ERROR: {e}")

    print(f"\n{'='*60}")
    print(f"INGESTION SUMMARY")
    print(f"{'='*60}")
    print(f"  Uploaded : {uploaded} files")
    print(f"  Skipped  : {skipped} files (already ingested)")
    print(f"  Metadata : {METADATA_FILE}")
    print(f"  View in MinIO: http://localhost:9001")

if __name__ == "__main__":
    main()