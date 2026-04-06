import os
import time
import requests
import boto3
import logging
from botocore.config import Config
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

DISCOGS_TOKEN = os.environ.get("DISCOGS_TOKEN")
DISCOGS_USER_AGENT = os.environ.get("DISCOGS_USER_AGENT", "OwnSoundArtistScanner/1.0")
ARTIST_PATH_PREFIX = "public/artists/"

def get_s3_client_for_library(library):
    url = library.get("url")
    ids = library.get("identifiers", {})
    
    bucket_name = ids.get("bucket_name")
    if not bucket_name and url:
        try: bucket_name = url.rstrip("/").split("/")[-1]
        except: pass
    
    endpoint_url = url
    if bucket_name and bucket_name in url:
        endpoint_url = url.split(bucket_name)[0]

    client = boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=ids.get("aws_access_key_id"),
        aws_secret_access_key=ids.get("aws_secret_access_key"),
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
        region_name="us-east-1"
    )
    return client, bucket_name

def search_artist_discogs(artist_name):
    if not DISCOGS_TOKEN or DISCOGS_TOKEN == "YOUR_DISCOGS_TOKEN_HERE":
        logger.warning("Discogs token not configured.")
        return None

    headers = {
        "User-Agent": DISCOGS_USER_AGENT,
        "Authorization": f"Discogs token={DISCOGS_TOKEN}"
    }

    try:
        search_url = f"https://api.discogs.com/database/search?q={artist_name}&type=artist"
        response = requests.get(search_url, headers=headers)
        response.raise_for_status()
        results = response.json().get("results", [])

        if not results:
            return None

        artist_id = results[0]["id"]
        time.sleep(1) 

        artist_url = f"https://api.discogs.com/artists/{artist_id}"
        response = requests.get(artist_url, headers=headers)
        response.raise_for_status()
        artist_data = response.json()

        images = artist_data.get("images", [])
        if not images:
            return None

        return images[0].get("resource_url")

    except Exception as e:
        logger.error(f"Error fetching artist from Discogs ({artist_name}): {e}")
        return None

def download_and_upload_image(s3_client, bucket_name, image_url, artist_name):
    try:
        headers = {"User-Agent": DISCOGS_USER_AGENT}
        response = requests.get(image_url, headers=headers)
        response.raise_for_status()

        safe_filename = "".join(e for e in artist_name if e.isalnum() or e in (' ', '.', '_')).rstrip()
        key = f"{ARTIST_PATH_PREFIX}{safe_filename}.jpg"

        s3_client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=response.content,
            ContentType="image/jpeg"
        )
        return key
    except Exception as e:
        logger.error(f"Error uploading artist image to S3 ({artist_name}): {e}")
        return None

def scan_artists_for_images(repo):
    libraries = repo.get_libraries()
    if not libraries:
        logger.error("No libraries found in database.")
        return 0
    
    target_library = libraries[0]
    s3_client, bucket_name = get_s3_client_for_library(target_library)
    
    if not bucket_name:
        logger.error("Could not determine bucket name for the first library.")
        return 0

    artists = repo.all_artists()
    updated_count = 0

    for artist in artists:
        if not artist.get("image") or "500x500" in artist.get("image"):
            logger.info(f"Fetching image for artist: {artist['name']}")
            image_url = search_artist_discogs(artist["name"])
            
            if image_url:
                s3_key = download_and_upload_image(s3_client, bucket_name, image_url, artist["name"])
                if s3_key:
                    repo.update_artist(artist["id"], {"image": s3_key, "bucket": bucket_name})
                    updated_count += 1
                    logger.info(f"Successfully updated image for {artist['name']}")
            
            time.sleep(1)

    return updated_count
