
import boto3
from mutagen.flac import FLAC, Picture
from mutagen import File
from io import BytesIO
from botocore.config import Config
import base64
import json
from PIL import Image
from tqdm import tqdm
import os
import logging

logger = logging.getLogger(__name__)

# =====================
# CONFIGURATION
# =====================
# Moved to API environment variables or configuration if possible, but hardcoding for now based on scan_2_json.py
ENDPOINT = os.environ.get("S3_ENDPOINT", "https://bucket.morgan-coulm.fr")
ACCESS_KEY = os.environ.get("S3_ACCESS_KEY", "Xx2z3G6Yhl1ABsPa4Eic")
SECRET_KEY = os.environ.get("S3_SECRET_KEY", "LnEdVzwA5ve8rmI1tk4Q3MUqpNb6ujOy0RBG9YaZ")
BUCKET_NAME = os.environ.get("S3_BUCKET_NAME", "music2")
COVER_PATH_PREFIX = "public/covers/" # Prefix for storing covers in S3

AUDIO_EXTENSIONS = [".flac", ".mp3", ".wav", ".m4a", ".aac", ".ogg", ".aiff", ".wma"]

# =====================
# UTILITAIRES
# =====================
def get_first_artist(artist_value):
    if not artist_value:
        return "Unknown"
    artist = artist_value[0]
    for sep in [";", ",", " feat. ", " ft. ", " & ", " / "]:
        if sep in artist:
            artist = artist.split(sep)[0]
            break
    return artist.strip()

def extract_cover_image(audio):
    try:
        name = audio.__class__.__name__

        if name == "MP3" and audio.tags:
            for tag in audio.tags.values():
                if getattr(tag, "FrameID", None) == "APIC":
                    return base64.b64encode(tag.data).decode()

        if name == "FLAC" and audio.pictures:
            return base64.b64encode(audio.pictures[0].data).decode()

        if name == "MP4" and audio.tags:
            covr = audio.tags.get("covr")
            if covr:
                return base64.b64encode(covr[0]).decode()

        if audio.tags and "metadata_block_picture" in audio.tags:
            raw = base64.b64decode(audio.tags["metadata_block_picture"][0])
            pic = Picture(raw)
            return base64.b64encode(pic.data).decode()
    except Exception as e:
        logger.warning(f"Could not extract cover image: {e}")
        pass

    return None

def list_s3_music_files(s3_client, bucket):
    logger.info(f"Listing music files in bucket: {bucket}")
    paginator = s3_client.get_paginator("list_objects_v2")
    out = []
    try:
        for page in paginator.paginate(Bucket=bucket):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if any(key.lower().endswith(ext) for ext in AUDIO_EXTENSIONS):
                    out.append(key)
        logger.info(f"Found {len(out)} music files.")
    except Exception as e:
        logger.error(f"Error listing S3 files in {bucket}: {e}")
    return out

def read_audio_metadata_s3(s3_client, bucket, key):
    logger.debug(f"Reading metadata for: {key}")
    try:
        obj = s3_client.get_object(Bucket=bucket, Key=key)
        data = obj["Body"].read()

        audio_easy = File(BytesIO(data), easy=True)
        audio_full = File(BytesIO(data), easy=False)

        duration = "00:00"
        if audio_full and hasattr(audio_full.info, "length"):
            m = int(audio_full.info.length // 60)
            s = int(audio_full.info.length % 60)
            duration = f"{m:02d}:{s:02d}"

        raw_genres = audio_easy.get("genre", []) if audio_easy else []
        genres = []
        for g in raw_genres:
            parts = [p.strip() for p in g.split(",") if p.strip()]
            genres.extend(parts)

        return {
            "title": audio_easy.get("title", ["Unknown"])[0] if audio_easy else "Unknown",
            "artist": get_first_artist(audio_easy.get("artist")) if audio_easy else "Unknown",
            "album": audio_easy.get("album", ["Unknown"])[0] if audio_easy else "Unknown",
            "genre": genres,
            "duration": duration,
            "cover_base64": extract_cover_image(audio_full),
            "path": key,
            "bucket": bucket
        }
    except Exception as e:
        logger.warning(f"Error reading metadata for {key}: {e}")
    
    return {
        "title": os.path.basename(key),
        "artist": "Unknown",
        "album": "Unknown",
        "genre": [],
        "duration": "00:00",
        "cover_base64": None,
        "path": key,
        "bucket": bucket
    }

def upload_cover_s3(s3_client, base64_cover, filename, bucket, size=None):
    if not base64_cover:
        return ""

    try:
        image_data = base64.b64decode(base64_cover)
        img = Image.open(BytesIO(image_data))

        if size:
            img = img.resize(size, Image.Resampling.LANCZOS)

        buffer = BytesIO()
        img.save(buffer, format="WEBP")
        buffer.seek(0)

        safe_filename = "".join(e for e in filename if e.isalnum() or e in (' ', '.', '_')).rstrip()
        key = f"{COVER_PATH_PREFIX}{safe_filename}.webp"

        s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=buffer,
            ContentType="image/webp"
        )
        return key
    except Exception as e:
        logger.error(f"Error uploading cover {filename}: {e}")
        return ""

def scan_bucket_for_music_metadata(endpoint, access_key, secret_key, bucket_name):
    s3_client = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
        region_name="us-east-1"
    )

    music_files_keys = list_s3_music_files(s3_client, bucket_name)
    processed_data = {"artists": {}, "albums": {}, "tracks": {}, "genres": {}}

    artist_map = {}
    album_map = {}
    genre_map = {}
    current_artist_id = 1
    current_album_id = 1
    current_track_id = 1
    current_genre_id = 1

    for key in tqdm(music_files_keys, desc=f"Scanning {bucket_name}"):
        meta = read_audio_metadata_s3(s3_client, bucket_name, key)
        artist_name = meta["artist"]
        album_name = meta["album"]
        genre_names = meta["genre"]

        if artist_name not in artist_map:
            artist_id_str = str(current_artist_id)
            artist_map[artist_name] = artist_id_str
            processed_data["artists"][artist_id_str] = {"id": current_artist_id, "name": artist_name, "image": "", "listAlbums": []}
            current_artist_id += 1
        artist_id_str = artist_map[artist_name]

        album_key = f"{artist_name} - {album_name}"
        if album_key not in album_map:
            album_id_str = str(current_album_id)
            album_map[album_key] = album_id_str
            
            cover_full_key = ""
            cover_small_key = ""
            if meta["cover_base64"]:
                cover_full_key = upload_cover_s3(s3_client, meta["cover_base64"], f"{album_name}_cover", bucket_name)
                cover_small_key = upload_cover_s3(s3_client, meta["cover_base64"], f"{album_name}_cover_small", bucket_name, size=(40, 40))
                if not processed_data["artists"][artist_id_str]["image"]:
                     processed_data["artists"][artist_id_str]["image"] = cover_full_key

            processed_data["albums"][album_id_str] = {
                "id": current_album_id,
                "name": album_name,
                "artistId": [int(artist_id_str)],
                "genreIds": [],
                "cover": cover_full_key,
                "coverSmall": cover_small_key,
                "coverBucket": bucket_name,
                "listMusique": []
            }
            processed_data["artists"][artist_id_str]["listAlbums"].append(current_album_id)
            current_album_id += 1
        album_id_str = album_map[album_key]

        for gname in genre_names:
            if gname not in genre_map:
                genre_id_str = str(current_genre_id)
                genre_map[gname] = genre_id_str
                processed_data["genres"][genre_id_str] = {"id": current_genre_id, "name": gname}
                current_genre_id += 1
            
            gid = int(genre_map[gname])
            if gid not in processed_data["albums"][album_id_str]["genreIds"]:
                processed_data["albums"][album_id_str]["genreIds"].append(gid)

        track_id_str = str(current_track_id)
        processed_data["tracks"][track_id_str] = {
            "id": current_track_id,
            "title": meta["title"],
            "duration": meta["duration"],
            "artistId": int(artist_id_str),
            "albumId": int(album_id_str),
            "albumTrack": len(processed_data["albums"][album_id_str]["listMusique"]) + 1,
            "path": meta["path"],
            "bucket": bucket_name
        }
        processed_data["albums"][album_id_str]["listMusique"].append(current_track_id)
        current_track_id += 1

    return processed_data

if __name__ == '__main__':
    # Example of how to run this script directly for testing
    # This part will not be executed when imported as a module
    
    # Configure logging for standalone execution
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    logger.info("Starting standalone bucket scan...")
    
    try:
        scanned_data = scan_bucket_for_music_metadata()
        
        # For standalone testing, print or save the output
        output_filename = "scanned_music_data.json"
        with open(output_filename, "w", encoding="utf-8") as f:
            json.dump(scanned_data, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Scanned data saved to {output_filename}")
        
        # Print summary
        print("--- Scan Summary ---")
        print(f"Artists found: {len(scanned_data['artists'])}")
        print(f"Albums found: {len(scanned_data['albums'])}")
        print(f"Tracks found: {len(scanned_data['tracks'])}")
        print("--------------------")

    except Exception as e:
        logger.exception("An error occurred during the standalone scan.")
