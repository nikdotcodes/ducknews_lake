import os
import requests
import zipfile
import tempfile
from pathlib import Path
import boto3
from dotenv import load_dotenv


print("🚀 Starting EL pipeline...")
print("🔎 Loading environment variables...")

load_dotenv()

# Detect environment
running_in_docker = os.path.exists("/.dockerenv")

# Setting up MinIO/S3 connection
MINIO_ENDPOINT = (
    os.getenv("MINIO_ENDPOINT_DOCKER")
    if running_in_docker
    else os.getenv("MINIO_ENDPOINT_LOCAL")
)
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")

RAW_BUCKET = os.getenv("RAW_BUCKET")
BUCKET_PREFIX = os.getenv("BUCKET_PREFIX")
GITHUB_INDEX = os.getenv("GITHUB_ZIP_INDEX")
GITHUB_API_INDEX = os.getenv("GITHUB_API_INDEX")

# temp location for processed zips (will move to s3 later)
PROCESSED_FILE = Path(".processed_zips")


def get_processed_set():
    if PROCESSED_FILE.exists():
        print("🗄 Loading processed set...")
        return set(PROCESSED_FILE.read_text().splitlines())
    return set()


def add_to_processed_file(zip_name: str):
    with open(PROCESSED_FILE, "a") as f:
        f.write(zip_name + "\n")
    print("🗄️Logging processed file...")


def list_github_zip_links():
    print("🏗️Gathering zip links from GitHub...")
    r = requests.get(GITHUB_API_INDEX)
    r.raise_for_status()

    files = [item["download_url"] for item in r.json() if item["name"].endswith(".zip")]
    return files


def upload_file_to_bucket(file_path: Path, key: str):
    s3_client = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )
    s3_client.upload_file(str(file_path), RAW_BUCKET, key)
    print(f"Uploaded {file_path} to s3://{RAW_BUCKET}/{key}")


def pull_fake_news_from_github():
    processed = get_processed_set()
    zip_links = list_github_zip_links()

    for link in zip_links:
        zip_name = link.split("/")[-1]
        if zip_name in processed:
            continue

        print(f"⤵️ Downloading {zip_name} from GitHub...")
        r = requests.get(link)
        r.raise_for_status()

        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            tmp.write(r.content)
            tmp_path = tmp.name

        with zipfile.ZipFile(tmp_path, "r") as zip:
            for member in zip.namelist():
                with zip.open(member) as f:
                    tmp_file_path = Path(tempfile.gettempdir()) / member.split("/")[0]
                    tmp_file_path.write_bytes(f.read())
                    s3_key = f"{BUCKET_PREFIX}/{member}"
                    upload_file_to_bucket(tmp_file_path, s3_key)
                    print(f"📤 Uploaded {s3_key} to s3")

        add_to_processed_file(zip_name)
    print("🎉 Raw pipeline completed successfully!")
