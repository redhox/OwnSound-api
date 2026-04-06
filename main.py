
from fastapi import FastAPI, Depends, HTTPException, Body
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Literal, Dict, Any
import os
from passlib.context import CryptContext
from auth import verify_token, create_token
from repositories.json_repo import JsonRepository
from repositories.bucket_repo import S3ContactRepository
from dotenv import load_dotenv
import logging

# Import the bucket scanner
from bucket_scanner import scan_bucket_for_music_metadata
from artist_image_scanner import scan_artists_for_images

import asyncio
from contextlib import asynccontextmanager

load_dotenv() 

logger = logging.getLogger(__name__)

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

def get_password_hash(password):
    return pwd_context.hash(password)

repo = JsonRepository()
bucketS3 = S3ContactRepository(json_repo=repo)

async def top_genres_job():
    while True:
        try:
            logger.info("Running background job: updating top genres...")
            repo.update_user_top_genres()
            logger.info("Background job completed: top genres updated.")
        except Exception as e:
            logger.error(f"Error in top_genres_job: {e}")
        
        # Sleep for 24 hours (24 * 60 * 60 seconds)
        await asyncio.sleep(24 * 60 * 60)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Run the job immediately on startup
    asyncio.create_task(top_genres_job())
    yield

app = FastAPI(lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    # Allow requests from the frontend running on localhost:5173 and the backend's default origin
    allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"], 
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def get_base_url_for_bucket(identifier: int | str | None = None) -> str | None:
    libraries = repo.get_libraries()
    if not libraries:
        return None
    target = None
    if isinstance(identifier, int):
        for lib in libraries:
            if lib.get("id") == identifier:
                target = lib
                break
    elif isinstance(identifier, str):
        for lib in libraries:
            if lib.get("identifiers", {}).get("bucket_name") == identifier:
                target = lib
                break
            if identifier in lib.get("url", ""):
                target = lib
                break
    if not target:
        target = libraries[0]
    if target and target.get("url"):
        return target["url"].rstrip("/") + "/"
    return None
# ======================
# AUTH
# ======================
class LoginPayload(BaseModel):
    username: str
    password: str

@app.post("/login")
def login(payload: LoginPayload):
    user = repo.get_user_by_username(payload.username)

    if not user:
        raise HTTPException(status_code=401, detail="Invalid credentials")

    # Vérification du mot de passe
    # Note: On utilise pwd_context défini plus haut
    is_valid = False
    try:
        is_valid = pwd_context.verify(payload.password, user["password"])
    except Exception:
        # Fallback pour les anciens mots de passe en clair
        if user["password"] == payload.password:
            is_valid = True

    if not is_valid:
        raise HTTPException(status_code=401, detail="Invalid credentials")

    token = create_token(user)

    return {
        "token": token,
        "type": "bearer",
        "user": {
            "id": user["id"],
            "username": user["username"],
            "email": user.get("email"),
            "role":user.get("role"),
        }
    }

# ======================
# HELPERS
# ======================

def get_fresh_user(user):
    return repo.get_user_by_id(user["id"])

def build_album(album_id: int, user):
    album = repo.get_album(album_id)
    if not album:
        return None

    current_user = repo.get_user_by_id(user["id"])
    liked_tracks = set(current_user.get("like", {}).get("track", []))
    liked_albums = set(current_user.get("like", {}).get("album", []))

    album_artist_ids = album.get("artistId", [])
    album_artists = []
    for aid in album_artist_ids:
        artist = repo.get_artist(aid)
        if artist:
            album_artists.append({
                "id": artist["id"],
                "name": artist["name"]
            })

    genre_data = []
    for gid in album.get("genreIds", []):
        genre = repo.get_genre(gid)
        if genre:
            genre_data.append({
                "id": genre["id"],
                "name": genre["name"]
            })

    tracks = []
    for tid in album.get("listMusique", []):
        t = repo.get_track(tid)
        if not t:
            continue

        track_artist = repo.get_artist(t.get("artistId"))
        track_artist_name = track_artist["name"] if track_artist else None

        tracks.append({
            "id": t["id"],
            "title": t["title"],
            "duration": t.get("duration"),
            "albumName": album.get("name"),
            "artistName": track_artist_name,
            "artistId": t.get("artistId"),
            "albumId": t.get("albumId"),
            "albumTrack": t.get("albumTrack"),
            "like": t["id"] in liked_tracks,
            "coverSmall": (get_base_url_for_bucket(album.get("coverBucket") or 1) + album.get("coverSmall")) if album and album.get("coverSmall") else None,
            "path": None # URL fetched on demand
        })

    tracks.sort(key=lambda x: x["albumTrack"])

    return {
        "id": album["id"],
        "name": album.get("name"),
        "artist": album_artists,
        "artistName": album_artists[0]["name"] if album_artists else None, 
        "artistId": album_artists[0]["id"] if album_artists else None,
        "genres": genre_data,
        "cover": (get_base_url_for_bucket(album.get("coverBucket") or 1) + album.get("cover")) if album and album.get("cover") else None,
        "like": album["id"] in liked_albums,
        "listMusique": tracks
    }


def build_playlist(pid: int, user):
    playlist = repo.get_playlist(pid)
    if not playlist:
        return None

    current_user = repo.get_user_by_id(user["id"])
    liked_tracks = set(current_user.get("like", {}).get("track", []))

    tracks = []
    for tid in playlist.get("listMusique", []):
        t = repo.get_track(tid)
        if not t:
            continue

        album = repo.get_album(t["albumId"])
        artist = repo.get_artist(t["artistId"])

        tracks.append({
            "id": t["id"],
            "title": t["title"],
            "duration": t.get("duration"),
            "albumName": album.get("name") if album else None,
            "artistName": artist.get("name") if artist else None,
            "artistId": t.get("artistId"),
            "albumId": t.get("albumId"),
            "albumTrack": t.get("albumTrack"),
            "like": t["id"] in liked_tracks,
            "coverSmall": (get_base_url_for_bucket(album.get("coverBucket") or 1) + album.get("coverSmall")) if album and album.get("coverSmall") else None,
            "path": None # URL fetched on demand
        })

    return {
        "id": playlist["id"],
        "name": playlist["name"],
        "listMusique": tracks
    }


# ======================
# ROUTES 
# ======================

# ======================
# USER
# ======================
class ChangeUsernamePayload(BaseModel):
    username: str

@app.post("/user/username")
def change_username(payload: ChangeUsernamePayload, user=Depends(verify_token)):
    current_user = repo.get_user_by_id(user["id"])
    if not current_user:
        raise HTTPException(401, "USER_NOT_FOUND")
    try:
        repo.set_username(user["id"], payload.username)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

    return {"username": payload.username}

class ChangePasswordPayload(BaseModel):
    password: str

@app.post("/user/password")
def change_password(payload: ChangePasswordPayload, user=Depends(verify_token)):
    current_user = repo.get_user_by_id(user["id"])
    if not current_user:
        raise HTTPException(401, "USER_NOT_FOUND")
    try:
        repo.set_user_password(user["id"], get_password_hash(payload.password))
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

class SetRolePayload(BaseModel):
    user_id: str
    role: Literal["user", "admin"]

@app.post("/admin/user/role")
def set_role(payload: SetRolePayload, user=Depends(verify_token)):
    current = repo.get_user_by_id(user["id"])
    if not current or current.get("role") != "admin":
        raise HTTPException(403, "ADMIN_REQUIRED")

    try:
        repo.set_user_role(payload.user_id, payload.role)
    except Exception as e:
        raise HTTPException(400, str(e))

    return {
        "user_id": payload.user_id,
        "role": payload.role
    }
@app.post("/admin/user/listUser")
def list_users(user=Depends(verify_token)): # Renamed to list_users for clarity
    current = repo.get_user_by_id(user["id"])
    if not current or current.get("role") != "admin":
        raise HTTPException(403, "ADMIN_REQUIRED")
    try:
        users = repo.get_user_all()
    except Exception as e:
        logger.error(f"Error listing users: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve users")

    return {"users": users}

class CreateUserPayload(BaseModel):
    username: str
    password: str
    email: str
    role: Literal["user", "admin"] = "user"

@app.post("/admin/user/create")
def create_user(payload: CreateUserPayload, user=Depends(verify_token)):
    current = repo.get_user_by_id(user["id"])
    if not current or current.get("role") != "admin":
        raise HTTPException(403, "ADMIN_REQUIRED")

    try:
        user_id = repo.create_user(
            username=payload.username,
            password=get_password_hash(payload.password),
            email=payload.email,
            role=payload.role
        )
    except Exception as e:
        logger.error(f"Error creating user {payload.username}: {e}")
        raise HTTPException(status_code=400, detail=str(e))

    return {
        "id": user_id,
        "username": payload.username,
        "email": payload.email,
        "role": payload.role
    }

class RegisterPayload(BaseModel):
    username: str
    password: str
    email: str
    token: str

@app.post("/register")
def register(payload: RegisterPayload):
    if not repo.verify_registration_token(payload.token):
        raise HTTPException(status_code=401, detail="Invalid registration token")

    try:
        user_id = repo.create_user(
            username=payload.username,
            password=get_password_hash(payload.password),
            email=payload.email,
            role="user"
        )
        repo.consume_registration_token(payload.token)
    except Exception as e:
        logger.error(f"Error during registration for {payload.username}: {e}")
        raise HTTPException(status_code=400, detail=str(e))

    return {
        "id": user_id,
        "username": payload.username,
        "email": payload.email,
        "role": "user"
    }

@app.post("/admin/generateToken")
def generate_token(user=Depends(verify_token)):
    current = repo.get_user_by_id(user["id"])
    if not current or current.get("role") != "admin":
        raise HTTPException(status_code=403, detail="ADMIN_REQUIRED")

    token = repo.create_registration_token()
    return {"token": token}


# --- Library Management ---
# Define Pydantic model for library payload, matching frontend expectations
class LibraryPayload(BaseModel):
    name: str
    type: str # e.g., "bucket"
    config: Dict[str, Any]

@app.get("/admin/libraries")
def get_libraries(user=Depends(verify_token)):
    current = repo.get_user_by_id(user["id"])
    if not current or current.get("role") != "admin":
        raise HTTPException(status_code=403, detail="ADMIN_REQUIRED")
    
    raw_libraries = repo.get_libraries() # This comes directly from database.json
    formatted_libraries = []
    for lib in raw_libraries:
        formatted_lib = {
            "id": lib.get("id"),
            "name": lib.get("name"),
            "type": None, # Initialize type
            "config": {} # Initialize config
        }
        # Transform backend data to frontend expected format
        if lib.get("url") and lib.get("identifiers"):
            formatted_lib["type"] = "bucket" # Assuming it's always a bucket type if url/identifiers exist
            
            # Extract bucket_name from URL or identifiers. Prioritize identifiers if available.
            # Infer bucket_name from URL if not explicitly in identifiers.
            bucket_name_from_url = ""
            if lib.get("url"):
                try:
                    # Simple parsing: assumes URL ends with bucket name or path
                    parsed_url = lib["url"].rstrip("/").split('/')
                    if len(parsed_url) > 1:
                        bucket_name_from_url = parsed_url[-1]
                except Exception:
                    logger.warning(f"Could not reliably extract bucket name from URL: {lib.get('url')}")

            formatted_lib["config"] = {
                "bucket_name": lib.get("identifiers", {}).get("bucket_name", bucket_name_from_url), 
                "aws_endpoint_url": lib.get("url"), # Using the provided URL as endpoint
                "aws_access_key_id": lib.get("identifiers", {}).get("aws_access_key_id"),
                "aws_secret_access_key": lib.get("identifiers", {}).get("aws_secret_access_key"),
                "url_expiration": lib.get("identifiers", {}).get("url_expiration", 3600) # Default expiration
            }
        # Add logic here for other library types if they exist and are supported
        
        formatted_libraries.append(formatted_lib)

    logger.info(f"Returning {len(formatted_libraries)} formatted libraries.")
    return {"libraries": formatted_libraries}

@app.post("/admin/libraries")
def create_library(payload: LibraryPayload, user=Depends(verify_token)):
    current = repo.get_user_by_id(user["id"])
    if not current or current.get("role") != "admin":
        raise HTTPException(status_code=403, detail="ADMIN_REQUIRED")
    
    try:
        # Convert frontend payload (type, config) back to backend DB schema (url, identifiers)
        library_data_for_db = {
            "name": payload.name,
            # Store type if it's to be saved in DB, otherwise it's inferred on GET
            "url": payload.config.get("aws_endpoint_url"),
            "identifiers": {
                "bucket_name": payload.config.get("bucket_name"),
                "aws_access_key_id": payload.config.get("aws_access_key_id"),
                "aws_secret_access_key": payload.config.get("aws_secret_access_key"),
                "url_expiration": payload.config.get("url_expiration")
            }
        }
        # Basic validation for 'bucket' type
        if payload.type != "bucket":
             raise ValueError("Unsupported library type. Only 'bucket' is currently supported.")
        if not library_data_for_db["url"] or not library_data_for_db["identifiers"].get("aws_access_key_id"):
             raise ValueError("Endpoint URL and Access Key ID are required for bucket type.")

        new_lib = repo.add_library(library_data_for_db)
        # Refresh S3 client configurations after adding a new one
        bucketS3.refresh_configs() 
        logger.info(f"Library '{payload.name}' created successfully.")
        return new_lib
    except Exception as e:
        logger.error(f"Error creating library '{payload.name}': {e}")
        raise HTTPException(status_code=400, detail=str(e))

@app.put("/admin/libraries/{index}")
def update_library(index: int, payload: LibraryPayload, user=Depends(verify_token)):
    current = repo.get_user_by_id(user["id"])
    if not current or current.get("role") != "admin":
        raise HTTPException(status_code=403, detail="ADMIN_REQUIRED")
    
    try:
        # Convert frontend payload back to backend DB schema
        library_data_for_db = {
            "name": payload.name,
            "url": payload.config.get("aws_endpoint_url"),
            "identifiers": {
                "bucket_name": payload.config.get("bucket_name"),
                "aws_access_key_id": payload.config.get("aws_access_key_id"),
                "aws_secret_access_key": payload.config.get("aws_secret_access_key"),
                "url_expiration": payload.config.get("url_expiration")
            }
        }
        if payload.type != "bucket":
             raise ValueError("Unsupported library type. Only 'bucket' is currently supported.")
        if not library_data_for_db["url"] or not library_data_for_db["identifiers"].get("aws_access_key_id"):
             raise ValueError("Endpoint URL and Access Key ID are required for bucket type.")

        updated = repo.update_library(index, library_data_for_db)
        # Refresh S3 client configurations after updating one
        bucketS3.refresh_configs()
        logger.info(f"Library at index {index} updated successfully.")
        return updated
    except IndexError:
        logger.error(f"Update failed: Library index {index} out of range.")
        raise HTTPException(status_code=404, detail="Library not found.")
    except Exception as e:
        logger.error(f"Error updating library at index {index}: {e}")
        raise HTTPException(status_code=400, detail=str(e))

class ScanRequest(BaseModel):
    library_id: int | None = None

# --- New endpoint for scanning bucket ---
@app.post("/admin/scan-bucket")
def trigger_bucket_scan(req: ScanRequest = Body(default=ScanRequest()), user=Depends(verify_token)):
    current = repo.get_user_by_id(user["id"])
    if not current or current.get("role") != "admin":
        raise HTTPException(status_code=403, detail="ADMIN_REQUIRED")

    try:
        logger.info(f"Received request to scan. Body library_id: {req.library_id}")
        all_libraries = repo.get_libraries()
        
        libraries_to_scan = []
        if req.library_id:
            libraries_to_scan = [lib for lib in all_libraries if lib.get("id") == req.library_id]
            logger.info(f"Filtered libraries to scan. Found {len(libraries_to_scan)} matches for ID {req.library_id}")
            if not libraries_to_scan:
                raise HTTPException(status_code=404, detail=f"Library with ID {req.library_id} not found.")
        else:
            libraries_to_scan = all_libraries
            logger.info(f"No library_id provided. Scanning all {len(libraries_to_scan)} libraries.")

        total_added = {"artists": 0, "albums": 0, "tracks": 0, "genres": 0}
        
        current_db_data = repo.data
        artists_db = current_db_data.setdefault("artists", {})
        albums_db = current_db_data.setdefault("albums", {})
        tracks_db = current_db_data.setdefault("tracks", {})
        genres_db = current_db_data.setdefault("genres", {})

        for lib in libraries_to_scan:
            # ... (previous code for library setup)
            url = lib.get("url")
            ids = lib.get("identifiers", {})
            lib_id = lib.get("id")
            
            bucket_name = ids.get("bucket_name")
            if not bucket_name and url:
                try: bucket_name = url.rstrip("/").split("/")[-1]
                except: continue
            
            if not bucket_name or not ids.get("aws_access_key_id"):
                continue

            endpoint_url = url
            if bucket_name in url:
                endpoint_url = url.split(bucket_name)[0]

            logger.info(f"Scanning library: {lib.get('name')} (Bucket: {bucket_name})")
            
            scanned_data = scan_bucket_for_music_metadata(
                endpoint=endpoint_url,
                access_key=ids.get("aws_access_key_id"),
                secret_key=ids.get("aws_secret_access_key"),
                bucket_name=bucket_name
            )

            # Re-generate maps
            artist_name_to_id = {v["name"]: str(v["id"]) for v in artists_db.values()}
            genre_name_to_id = {v["name"]: str(v["id"]) for v in genres_db.values()}
            album_map = {}
            for a_id, alb in albums_db.items():
                art_id = alb.get("artistId", [None])[0]
                art = repo.get_artist(art_id) if art_id else None
                if art and alb.get("name"):
                    album_map[f"{art['name']} - {alb['name']}"] = str(a_id)

            current_artist_id = max(map(int, artists_db.keys()), default=0) + 1
            current_album_id = max(map(int, albums_db.keys()), default=0) + 1
            current_track_id = max(map(int, tracks_db.keys()), default=0) + 1
            current_genre_id = max(map(int, genres_db.keys()), default=0) + 1

            # Process Scanned Genres
            for s_gen_id, s_gen in scanned_data.get("genres", {}).items():
                name = s_gen["name"]
                if name not in genre_name_to_id:
                    new_id = str(current_genre_id)
                    genres_db[new_id] = {
                        "id": current_genre_id,
                        "name": name
                    }
                    genre_name_to_id[name] = new_id
                    current_genre_id += 1
                    total_added["genres"] += 1

            # Process Scanned Artists
            for s_art_id, s_art in scanned_data["artists"].items():
                name = s_art["name"]
                if name not in artist_name_to_id:
                    new_id = str(current_artist_id)
                    artists_db[new_id] = {
                        "id": current_artist_id,
                        "name": name,
                        "image": s_art["image"],
                        "listAlbums": [],
                        "library_id": lib_id
                    }
                    artist_name_to_id[name] = new_id
                    current_artist_id += 1
                    total_added["artists"] += 1
                elif s_art["image"]:
                    artists_db[artist_name_to_id[name]]["image"] = s_art["image"]

            # Process Scanned Albums
            for s_alb_id, s_alb in scanned_data["albums"].items():
                art_name = scanned_data["artists"][str(s_alb["artistId"][0])]["name"]
                key = f"{art_name} - {s_alb['name']}"
                art_id = artist_name_to_id.get(art_name)

                if key not in album_map:
                    new_id = str(current_album_id)
                    albums_db[new_id] = {
                        "id": current_album_id,
                        "name": s_alb["name"],
                        "artistId": [int(art_id)],
                        "genreIds": [],
                        "cover": s_alb["cover"],
                        "coverSmall": s_alb["coverSmall"],
                        "coverBucket": s_alb["coverBucket"],
                        "listMusique": [],
                        "library_id": lib_id
                    }
                    album_map[key] = new_id
                    if art_id:
                        artists_db[art_id].setdefault("listAlbums", []).append(current_album_id)
                    current_album_id += 1
                    total_added["albums"] += 1

            # Process Scanned Tracks
            for s_trk_id, s_trk in scanned_data["tracks"].items():
                art_name = scanned_data["artists"][str(s_trk["artistId"])]["name"]
                alb_name = scanned_data["albums"][str(s_trk["albumId"])]["name"]
                alb_id = album_map.get(f"{art_name} - {alb_name}")
                art_id = artist_name_to_id.get(art_name)
                
                # Retrieve and aggregate genres for the album
                s_genre_ids = scanned_data["albums"][str(s_trk["albumId"])].get("genreIds", [])
                if alb_id:
                    album_entry = albums_db[alb_id]
                    if "genreIds" not in album_entry:
                        album_entry["genreIds"] = []
                    
                    for sgid in s_genre_ids:
                        gname = scanned_data["genres"][str(sgid)]["name"]
                        if gname in genre_name_to_id:
                            db_gid = int(genre_name_to_id[gname])
                            if db_gid not in album_entry["genreIds"]:
                                album_entry["genreIds"].append(db_gid)

                if alb_id and not any(t.get("path") == s_trk["path"] for t in tracks_db.values() if t.get("albumId") == int(alb_id)):
                    new_id = str(current_track_id)
                    tracks_db[new_id] = {
                        "id": current_track_id,
                        "title": s_trk["title"],
                        "duration": s_trk["duration"],
                        "artistId": int(art_id),
                        "albumId": int(alb_id),
                        "albumTrack": len(albums_db[alb_id]["listMusique"]) + 1,
                        "path": s_trk["path"],
                        "bucket": s_trk["bucket"],
                        "library_id": lib_id
                    }
                    albums_db[alb_id]["listMusique"].append(current_track_id)
                    current_track_id += 1
                    total_added["tracks"] += 1

        repo._save()
        bucketS3.refresh_configs()
        
        return {
            "message": "Scan completed.",
            "artists_added": total_added["artists"],
            "albums_added": total_added["albums"],
            "tracks_added": total_added["tracks"],
            "genres_added": total_added["genres"],
            "total_artists": len(artists_db),
            "total_albums": len(albums_db),
            "total_tracks": len(tracks_db),
            "total_genres": len(genres_db)
        }
    except Exception as e:
        logger.exception("Scan failed")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/admin/scan-artist-images")
def trigger_artist_image_scan(user=Depends(verify_token)):
    current = repo.get_user_by_id(user["id"])
    if not current or current.get("role") != "admin":
        raise HTTPException(status_code=403, detail="ADMIN_REQUIRED")

    try:
        updated_count = scan_artists_for_images(repo)
        return {"message": "Artist image scan completed.", "updated_count": updated_count}
    except Exception as e:
        logger.exception("Artist image scan failed")
        raise HTTPException(status_code=500, detail=str(e))


# Track

@app.get("/track/{track_id}/url")
def get_track_url(track_id: int, user=Depends(verify_token)):
    t = repo.get_track(track_id)
    if not t:
        raise HTTPException(404, "Track not found")
    
    # Add to history
    repo.add_track_to_history(user["id"], track_id)
    
    album = repo.get_album(t["albumId"])
    track_bucket_name = t.get("bucket")
    track_path = t.get("path")
    track_lib_id = t.get("library_id") or (album.get("library_id") if album else None)
    
    if not track_path:
        return {"url": None}
        
    try:
        track_url = bucketS3.get_temporary_link(track_path, bucket_name=track_bucket_name, library_id=track_lib_id)
        return {"url": track_url}
    except (RuntimeError, ValueError) as e:
        logger.error(f"Could not get temporary link for track {track_id}: {e}")
        return {"url": None}

@app.get("/user/history")
def get_user_history(user=Depends(verify_token)):
    current_user = repo.get_user_by_id(user["id"])
    if not current_user:
        raise HTTPException(401, "USER_NOT_FOUND")
    
    history_ids = current_user.get("history", [])
    
    # We can reuse track_by_list_id logic but we want to maintain history order
    user_likes = set(current_user.get("like", {}).get("track", []))
    out = []
    for tid in history_ids:
        t = repo.get_track(tid)
        if not t:
            continue
        album = repo.get_album(t["albumId"])
        artist = repo.get_artist(t["artistId"])
        
        out.append({
            "id": t["id"],
            "title": t["title"],
            "duration": t.get("duration"),
            "albumName": album.get("name") if album else None,
            "artistName": artist.get("name") if artist else None,
            "artistId": t.get("artistId"),
            "albumId": t.get("albumId"),
            "albumTrack": t.get("albumTrack"),
            "like": t["id"] in user_likes,
            "coverSmall": (get_base_url_for_bucket(album.get("coverBucket") or 1) + album.get("coverSmall")) if album and album.get("coverSmall") else None,
            "path": None 
        })
    return out

@app.post("/trackByListID")
def track_by_list_id(ids: List[int] = Body(...), user=Depends(verify_token)):
    current_user = repo.get_user_by_id(user["id"])
    if not current_user:
        raise HTTPException(401, "USER_NOT_FOUND")
        
    user_likes = set(current_user.get("like", {}).get("track", []))
    out = []
    for tid in ids:
        t = repo.get_track(tid)
        if not t:
            continue
        album = repo.get_album(t["albumId"])
        artist = repo.get_artist(t["artistId"])
        
        out.append({
            "id": t["id"],
            "title": t["title"],
            "duration": t.get("duration"),
            "albumName": album.get("name") if album else None,
            "artistName": artist.get("name") if artist else None,
            "artistId": t.get("artistId"),
            "albumId": t.get("albumId"),
            "albumTrack": t.get("albumTrack"),
            "like": t["id"] in user_likes,
            "coverSmall": (get_base_url_for_bucket(album.get("coverBucket") or 1) + album.get("coverSmall")) if album and album.get("coverSmall") else None,
            "path": None # URL will be fetched on demand
        })
    return out

@app.get("/albumLike")
def album_like(user=Depends(verify_token)):
    current_user = repo.get_user_by_id(user["id"])
    if not current_user:
        raise HTTPException(401, "USER_NOT_FOUND")
        
    liked_albums = set(current_user.get("like", {}).get("album", []))

    result = []
    for a in repo.all_albums():
        if a["id"] in liked_albums:
            primary_artist_id = a.get("artistId", [None])[0]
            main_artist = repo.get_artist(primary_artist_id) if primary_artist_id else None
            
            result.append({
                "id": a["id"],
                "name": a.get("name"),
                "like": True,
                "artistName": main_artist["name"] if main_artist else None,
                "artistId": main_artist["id"] if main_artist else None,
                "cover": (get_base_url_for_bucket(a.get("coverBucket") or 1) + a.get("cover")) if get_base_url_for_bucket(a.get("coverBucket") or 1) and a and a.get("cover") else None,
            })
    return result


# Album

class AlbumRequest(BaseModel):
    album_id: int

@app.post("/get_album")
def get_album(req: AlbumRequest, user=Depends(verify_token)):
    album = build_album(req.album_id, user=user)
    if not album:
        raise HTTPException(404)
    return album


@app.get("/allAlbum")
def all_album(user=Depends(verify_token)):
    current_user = repo.get_user_by_id(user["id"])
    if not current_user:
        raise HTTPException(401, "USER_NOT_FOUND")
        
    liked_albums = set(current_user.get("like", {}).get("album", []))

    result = []
    for a in repo.all_albums():
        primary_artist_id = a.get("artistId", [None])[0]
        main_artist = repo.get_artist(primary_artist_id) if primary_artist_id else None

        result.append({
            "id": a["id"],
            "name": a.get("name"),
            "like": a["id"] in liked_albums,
            "artistName": main_artist["name"] if main_artist else None,
            "artistId": main_artist["id"] if main_artist else None,
            "cover": (get_base_url_for_bucket(a.get("coverBucket") or 1) + a.get("cover")) if get_base_url_for_bucket(a.get("coverBucket") or 1) and a and a.get("cover") else None,
        })
    return result


@app.get("/trackLike")
def track_like(user=Depends(verify_token)):
    current_user = repo.get_user_by_id(user["id"])
    if not current_user:
        raise HTTPException(401, "USER_NOT_FOUND")
        
    user_likes = current_user.get("like", {}).get("track", [])
    out = []
    for tid in user_likes:
        t = repo.get_track(tid)
        if not t:
            continue
        album = repo.get_album(t["albumId"])
        artist = repo.get_artist(t["artistId"])
        
        out.append({
            "id": t["id"],
            "title": t["title"],
            "duration": t.get("duration"),
            "albumName": album.get("name") if album else None,
            "artistName": artist.get("name") if artist else None,
            "artistId": t.get("artistId"),
            "albumId": t.get("albumId"),
            "albumTrack": t.get("albumTrack"),
            "like": True,
            "coverSmall": (get_base_url_for_bucket(album.get("coverBucket") or 1) + album.get("coverSmall")) if album and album.get("coverSmall") else None,
            "path": None # URL will be fetched on demand
        })
    return {"listMusique": out}

class ArtistID(BaseModel):
    artist_id: int

@app.post("/albumByArtistID")
def album_by_artist(payload: ArtistID, user=Depends(verify_token)):
    artist = repo.get_artist(payload.artist_id)
    if not artist:
        raise HTTPException(404)

    current_user = repo.get_user_by_id(user["id"])
    if not current_user:
        raise HTTPException(401, "USER_NOT_FOUND")
        
    liked_albums = set(current_user.get("like", {}).get("album", []))

    albums = []
    for aid in artist.get("listAlbums", []):
        album = repo.get_album(aid)
        if album:
            albums.append({
                "id": album["id"],
                "name": album.get("name"),
                "like": album["id"] in liked_albums,
                "artistName": album.get("artistName"),
                "artistId": album.get("artistId"),
                "date": album.get("date"),
                "cover": (get_base_url_for_bucket(album.get("coverBucket") or 1) + album.get("cover")) if album and album.get("cover") else None,
            })

    return {
        "id": artist["id"],
        "name": artist.get("name"),
        "image": get_base_url_for_bucket(artist.get("bucket") or 1) + artist.get("image") if artist.get("image") else None,
        "listAlbums": albums
    }


class AlbumListRequest(BaseModel):
    album_ids: List[int]

@app.post("/albumByListId")
def album_by_list_id(req: AlbumListRequest, user=Depends(verify_token)):
    current_user = repo.get_user_by_id(user["id"])
    if not current_user:
        raise HTTPException(401, "USER_NOT_FOUND")
        
    liked_albums = set(current_user.get("like", {}).get("album", []))
    album_list = []

    # Define get_base_url_for_bucket locally to ensure it's in scope
    def get_base_url_for_bucket(identifier: int | str | None = None) -> str | None:
        libraries = repo.get_libraries()
        if not libraries:
            return None
        target = None
        if isinstance(identifier, int):
            for lib in libraries:
                if lib.get("id") == identifier:
                    target = lib
                    break
        elif isinstance(identifier, str):
            for lib in libraries:
                if lib.get("identifiers", {}).get("bucket_name") == identifier:
                    target = lib
                    break
                if identifier in lib.get("url", ""):
                    target = lib
                    break
        if not target:
            target = libraries[0]
        if target and target.get("url"):
            return target["url"].rstrip("/") + "/"
        return None

    for aid in req.album_ids:
        a = repo.get_album(aid)
        if a:
            album_list.append({
                "id": a["id"],
                "name": a.get("name"),
                "like": a["id"] in liked_albums,
                "artistName": a.get("artistName"),
                "artistId": a.get("artistId"),
                "date": a.get("date"),
                "cover": (get_base_url_for_bucket(a.get("coverBucket") or 1) + a.get("cover")) if get_base_url_for_bucket(a.get("coverBucket") or 1) and a and a.get("cover") else None,
            })

    return {"albums": album_list}



# Artist

@app.get("/artistLike")
def artist_like(user=Depends(verify_token)):
    current_user = repo.get_user_by_id(user["id"])
    if not current_user:
        raise HTTPException(401, "USER_NOT_FOUND")
        
    liked_ids = set(current_user.get("like", {}).get("artist", []))
    
    artist_list = []
    for aid in liked_ids:
        a = repo.get_artist(aid)
        if a:
            artist_list.append({
                "id": a["id"],
                "name": a["name"],
                "like": True,
                "image": get_base_url_for_bucket(a.get("bucket") or 1) + a.get("image") if a.get("image") else None,
            })
    return artist_list

@app.get("/allGenres")
def get_all_genres(user=Depends(verify_token)):
    genres = repo.all_genres()
    return sorted(list(genres), key=lambda x: x["name"])

class GenresRequest(BaseModel):
    genre_names: List[str]

@app.post("/tracksByGenres")
def get_tracks_by_genres(payload: GenresRequest, user=Depends(verify_token)):
    current_user = repo.get_user_by_id(user["id"])
    if not current_user:
        raise HTTPException(401, "USER_NOT_FOUND")
    
    user_likes = set(current_user.get("like", {}).get("track", []))
    
    all_genres = {g["name"].lower(): g["id"] for g in repo.all_genres()}
    target_genre_ids = [all_genres.get(name.lower()) for name in payload.genre_names if all_genres.get(name.lower())]
    
    if not target_genre_ids:
        return []

    final_tracks = []
    
    # Étape 1 : Essayer l'intersection (Albums qui possèdent TOUS les genres demandés)
    matching_albums_and = []
    for album in repo.all_albums():
        album_genre_ids = set(album.get("genreIds", []))
        if all(gid in album_genre_ids for gid in target_genre_ids):
            matching_albums_and.append(album)

    if matching_albums_and:
        for alb in matching_albums_and:
            t_ids = alb.get("listMusique", [])
            if t_ids:
                random_tid = random.choice(t_ids)
                t = repo.get_track(random_tid)
                if t:
                    final_tracks.append(t)
    else:
        # Étape 2 : Fallback (Intersection vide -> On prend des morceaux de chaque genre)
        seen_album_ids = set()
        temp_tracks = []
        for gid in target_genre_ids:
            # Pour chaque genre, on récupère les albums correspondants
            genre_albums = [alb for alb in repo.all_albums() if gid in alb.get("genreIds", [])]
            # Mélanger pour avoir de la variété si on limite
            random.shuffle(genre_albums)
            
            # On prend un échantillon d'albums pour ce genre (ex: max 20 par genre pour éviter une liste géante)
            count = 0
            for alb in genre_albums:
                if alb["id"] in seen_album_ids:
                    continue
                seen_album_ids.add(alb["id"])
                
                t_ids = alb.get("listMusique", [])
                if t_ids:
                    random_tid = random.choice(t_ids)
                    t = repo.get_track(random_tid)
                    if t:
                        temp_tracks.append(t)
                        count += 1
                if count >= 20: # Limite par genre dans le fallback
                    break
        
        final_tracks = temp_tracks
        # On mélange le tout pour que les genres soient entremêlés
        random.shuffle(final_tracks)

    # Formater pour le frontend
    out = []
    for t in final_tracks:
        album = repo.get_album(t["albumId"])
        artist = repo.get_artist(t["artistId"])
        out.append({
            "id": t["id"],
            "title": t["title"],
            "duration": t.get("duration"),
            "albumName": album.get("name") if album else None,
            "artistName": artist.get("name") if artist else None,
            "artistId": t.get("artistId"),
            "albumId": t.get("albumId"),
            "albumTrack": t.get("albumTrack"),
            "like": t["id"] in user_likes,
            "coverSmall": (get_base_url_for_bucket(album.get("coverBucket") or 1) + album.get("coverSmall")) if album and album.get("coverSmall") else None,
            "path": None 
        })
    
    return out

class GenreID(BaseModel):
    genre_id: int

@app.post("/albumByGenreID")
def album_by_genre(payload: GenreID, user=Depends(verify_token)):
    genre = repo.get_genre(payload.genre_id)
    if not genre:
        raise HTTPException(404, "Genre not found")

    current_user = repo.get_user_by_id(user["id"])
    liked_albums = set(current_user.get("like", {}).get("album", []))

    albums = []
    for a in repo.all_albums():
        if payload.genre_id in a.get("genreIds", []):
            primary_artist_id = a.get("artistId", [None])[0]
            main_artist = repo.get_artist(primary_artist_id) if primary_artist_id else None
            
            albums.append({
                "id": a["id"],
                "name": a.get("name"),
                "like": a["id"] in liked_albums,
                "artistName": main_artist["name"] if main_artist else None,
                "artistId": main_artist["id"] if main_artist else None,
                "cover": (get_base_url_for_bucket(a.get("coverBucket") or 1) + a.get("cover")) if get_base_url_for_bucket(a.get("coverBucket") or 1) and a and a.get("cover") else None,
            })

    return {
        "id": genre["id"],
        "name": genre.name if hasattr(genre, 'name') else (genre.get("name") if isinstance(genre, dict) else str(genre)),
        "listAlbums": albums
    }

import random

@app.get("/recommend/albums")
def recommend_albums(user=Depends(verify_token)):
    current_user = repo.get_user_by_id(user["id"])
    if not current_user:
        raise HTTPException(401, "USER_NOT_FOUND")
    
    liked_album_ids = current_user.get("like", {}).get("album", [])
    top_genres = current_user.get("top_genres", [])
    top_genre_ids = [g["id"] for g in top_genres]
    
    recommendations = []

    # 1. 1 album parmi les 3 derniers likés (seulement si > 3 likés)
    if len(liked_album_ids) > 3:
        last_3 = liked_album_ids[-3:]
        recommendations.append(random.choice(last_3))

    # 2. 1 album parmi tous les likés hors sélection précédente (seulement si > 3 likés)
    if len(liked_album_ids) > 3:
        remaining_liked = [aid for aid in liked_album_ids if aid not in recommendations]
        if remaining_liked:
            recommendations.append(random.choice(remaining_liked))

    # 3. Compléter jusqu'à 5 avec des albums aléatoires liés aux genres
    needed = 5 - len(recommendations)
    if needed > 0:
        genre_candidates = [
            a["id"] for a in repo.all_albums()
            if a["id"] not in recommendations
            and a["id"] not in liked_album_ids
            and any(gid in top_genre_ids for gid in a.get("genreIds", []))
        ]

        if genre_candidates:
            picks = random.sample(genre_candidates, min(needed, len(genre_candidates)))
            recommendations.extend(picks)

        # Si toujours pas assez, compléter avec n'importe quel album non liké
        needed = 5 - len(recommendations)
        if needed > 0:
            fallback = [
                a["id"] for a in repo.all_albums()
                if a["id"] not in recommendations
                and a["id"] not in liked_album_ids
            ]
            if fallback:
                picks = random.sample(fallback, min(needed, len(fallback)))
                recommendations.extend(picks)

    # Convertir en objets album complets
    liked_set = set(liked_album_ids)
    result = []
    for aid in recommendations:
        a = repo.get_album(aid)
        if a:
            primary_artist_id = a.get("artistId", [None])[0]
            main_artist = repo.get_artist(primary_artist_id) if primary_artist_id else None
            result.append({
                "id": a["id"],
                "name": a.get("name"),
                "like": a["id"] in liked_set,
                "artistName": main_artist["name"] if main_artist else None,
                "artistId": main_artist["id"] if main_artist else None,
                "cover": (get_base_url_for_bucket(a.get("coverBucket") or 1) + a.get("cover"))
                         if get_base_url_for_bucket(a.get("coverBucket") or 1) and a.get("cover") else None,
            })

    return result

@app.get("/recommend/genres-albums")
def recommend_genres_albums(user=Depends(verify_token)):
    current_user = repo.get_user_by_id(user["id"])
    if not current_user:
        raise HTTPException(401, "USER_NOT_FOUND")
    
    top_genres = current_user.get("top_genres", [])
    if not top_genres:
        return []

    result = []
    all_albums = list(repo.all_albums())
    liked_album_ids = set(current_user.get("like", {}).get("album", []))

    for g in top_genres:
        gid = g["id"]
        candidates = [a for a in all_albums if gid in a.get("genreIds", [])]
        if candidates:
            chosen = random.choice(candidates)
            primary_artist_id = chosen.get("artistId", [None])[0]
            main_artist = repo.get_artist(primary_artist_id) if primary_artist_id else None
            
            result.append({
                "id": chosen["id"],
                "name": chosen.get("name"),
                "like": chosen["id"] in liked_album_ids,
                "artistName": main_artist["name"] if main_artist else None,
                "artistId": main_artist["id"] if main_artist else None,
                "cover": (get_base_url_for_bucket(chosen.get("coverBucket") or 1) + chosen.get("cover"))
                         if get_base_url_for_bucket(chosen.get("coverBucket") or 1) and chosen.get("cover") else None,
                "based_on_genre": g["name"]
            })
            
    return result

@app.post("/allArtist")
def all_artist(user=Depends(verify_token)):
    current_user = repo.get_user_by_id(user["id"])
    if not current_user:
        raise HTTPException(401, "USER_NOT_FOUND")
        
    liked_ids = set(current_user.get("like", {}).get("artist", []))
    
    artist_list = []
    for a in repo.all_artists():
        artist_list.append({
            "id": a["id"],
            "name": a["name"],
            "like": a["id"] in liked_ids,
            "image": get_base_url_for_bucket(a.get("bucket") or 1) + a.get("image") if a.get("image") else None,
        })
    return artist_list

@app.post("/artistByListId")
def artist_by_list_id(ids: List[int], user=Depends(verify_token)):
    current_user = repo.get_user_by_id(user["id"])
    if not current_user:
        raise HTTPException(401, "USER_NOT_FOUND")
        
    liked_ids = set(current_user.get("like", {}).get("artist", []))
    
    artist_list = []
    for aid in ids:
        a = repo.get_artist(aid)
        if a:
            artist_list.append({
                "id": a["id"],
                "name": a["name"],
                "like": a["id"] in liked_ids,
                "image": get_base_url_for_bucket(a.get("bucket") or 1) + a.get("image") if a.get("image") else None,
            })
    return artist_list



# Playlist

class PlaylistRequest(BaseModel):
    playlist_id: int

@app.post("/get_playlist")
def get_playlist(req: PlaylistRequest, user=Depends(verify_token)):
    playlist = build_playlist(req.playlist_id, user)
    if not playlist:
        raise HTTPException(404)
    return playlist



@app.get("/listplaylists")
def list_playlists(user=Depends(verify_token)):
    current_user = repo.get_user_by_id(user["id"])
    if not current_user:
        raise HTTPException(401, "USER_NOT_FOUND")
        
    playlist_ids = set(current_user.get("like", {}).get("playlist", []))

    return [
        {"id": p["id"], "name": p["name"]}
        for pid in playlist_ids
        if (p := repo.get_playlist(pid))
    ]



class CreatePlaylist(BaseModel):
    name: str

@app.post("/playlist/create")
def create_playlist(payload: CreatePlaylist, user=Depends(verify_token)):
    playlist_id = repo.create_playlist(user["id"], payload.name)
    if playlist_id is None:
        raise HTTPException(400, "Impossible de créer la playlist")
    return {"playlist_id": playlist_id}


class UpdatePlaylistTracks(BaseModel):
    playlist_id: int
    track_ids: List[int]
    action: Literal["add", "del"]

@app.post("/playlist/update_tracks")
def update_playlist(payload: UpdatePlaylistTracks, user=Depends(verify_token)):
    playlist = repo.get_playlist(payload.playlist_id)
    if not playlist or playlist.get("owner") != user["id"]:
        raise HTTPException(403)

    result = repo.update_playlist_tracks(
        payload.playlist_id,
        payload.track_ids,
        payload.action
    )

    if result == "EMPTY":
        repo.delete_playlist(user["id"], payload.playlist_id)
        return {"deleted": True}

    return result






class SearchPayload(BaseModel):
    q: str

@app.post("/search")
def search(payload: SearchPayload, user=Depends(verify_token)):
    q = payload.q.lower().strip()
    if not q:
        return {"tracks": [], "albums": [], "artists": []}

    tracks = [t for t in repo.all_tracks() if q in t["title"].lower()]
    albums = [a for a in repo.all_albums() if q in a["name"].lower()]
    artists = [a for a in repo.all_artists() if q in a["name"].lower()]

    return {"tracks": tracks, "albums": albums, "artists": artists}

class LikeUpdate(BaseModel):
    id: int
    like: bool
    type: Literal["track", "album", "artist"]

@app.post("/updateLike")
def update_like(payload: LikeUpdate, user=Depends(verify_token)):
    current_user = repo.get_user_by_id(user["id"])
    if not current_user:
        raise HTTPException(401, "USER_NOT_FOUND")
        
    repo.update_user_like(
        user_id=user["id"],
        obj_type=payload.type,
        obj_id=payload.id,
        like=payload.like
    )
    return payload
