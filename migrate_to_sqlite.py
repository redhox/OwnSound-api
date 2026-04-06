import json
import os
from repositories.sqlite_repo import SqliteRepository
from passlib.context import CryptContext

DB_JSON = "database.json"
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

def migrate():
    if not os.path.exists(DB_JSON):
        print(f"{DB_JSON} not found. Skipping migration.")
        return

    print("Starting migration from JSON to SQLite...")
    with open(DB_JSON, "r", encoding="utf-8") as f:
        data = json.load(f)

    repo = SqliteRepository()
    
    # 1. Genres
    genre_id_map = {} # old_id -> new_id
    for old_id, g in data.get("genres", {}).items():
        new_id = repo.ensure_genre(g["name"])
        genre_id_map[int(old_id)] = new_id
    print(f"Migrated {len(genre_id_map)} genres.")

    # 2. Artists
    artist_id_map = {}
    for old_id, a in data.get("artists", {}).items():
        new_id = repo.ensure_artist(a["name"], image=a.get("image"), bucket=a.get("bucket"), library_id=a.get("library_id"))
        artist_id_map[int(old_id)] = new_id
    print(f"Migrated {len(artist_id_map)} artists.")

    # 3. Albums
    album_id_map = {}
    for old_id, alb in data.get("albums", {}).items():
        # Map genres
        new_genre_ids = [genre_id_map[gid] for gid in alb.get("genreIds", []) if gid in genre_id_map]
        
        # We need an artist_id. Take the first one.
        old_artist_ids = alb.get("artistId", [])
        new_artist_id = artist_id_map[old_artist_ids[0]] if old_artist_ids and old_artist_ids[0] in artist_id_map else None
        
        if new_artist_id:
            new_id = repo.ensure_album(
                alb["name"], 
                new_artist_id, 
                genre_ids=new_genre_ids,
                cover=alb.get("cover"),
                coverSmall=alb.get("coverSmall"),
                coverBucket=alb.get("coverBucket"),
                library_id=alb.get("library_id")
            )
            album_id_map[int(old_id)] = new_id
    print(f"Migrated {len(album_id_map)} albums.")

    # 4. Tracks
    track_id_map = {}
    for old_id, t in data.get("tracks", {}).items():
        new_album_id = album_id_map.get(t.get("albumId"))
        new_artist_id = artist_id_map.get(t.get("artistId"))
        
        if new_album_id and new_artist_id:
            new_id = repo.add_track(
                t["title"],
                t.get("duration"),
                new_artist_id,
                new_album_id,
                t.get("albumTrack", 0),
                t["path"],
                t.get("bucket"),
                t.get("library_id")
            )
            track_id_map[int(old_id)] = new_id
    print(f"Migrated {len(track_id_map)} tracks.")

    # 5. Libraries
    for lib in data.get("libraries", []):
        repo.add_library(lib)
    print(f"Migrated libraries.")

    # 6. Users
    for old_id, u in data.get("users", {}).items():
        try:
            # Check if user exists
            if repo.get_user_by_username(u["username"]):
                print(f"User {u['username']} already exists, skipping.")
                continue
                
            new_user_id_str = repo.create_user(u["username"], u["password"], u["email"], role=u.get("role", "user"))
            new_user_id = int(new_user_id_str)
            
            # Migrate likes
            likes = u.get("like", {})
            for tid in likes.get("track", []):
                if tid in track_id_map:
                    repo.update_user_like(str(new_user_id), "track", track_id_map[tid], True)
            for aid in likes.get("album", []):
                if aid in album_id_map:
                    repo.update_user_like(str(new_user_id), "album", album_id_map[aid], True)
            for art_id in likes.get("artist", []):
                if art_id in artist_id_map:
                    repo.update_user_like(str(new_user_id), "artist", artist_id_map[art_id], True)
            
            # Migrate history
            for tid in reversed(u.get("history", [])): # reversed to maintain order if add_track_to_history prepends
                if tid in track_id_map:
                    repo.add_track_to_history(str(new_user_id), track_id_map[tid])
                    
        except Exception as e:
            print(f"Error migrating user {u.get('username')}: {e}")

    print("Migration completed.")

if __name__ == "__main__":
    migrate()
