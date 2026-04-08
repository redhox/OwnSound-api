import os
import json
from repositories.sqlite_repo import SqliteRepository
from repositories.postgres_repo import PostgresRepository
from dotenv import load_dotenv

load_dotenv()

def migrate():
    print("Starting migration from SQLite to PostgreSQL...")
    
    sqlite_repo = SqliteRepository()
    
    pg_host = os.getenv("PGHOST", "localhost")
    pg_port = os.getenv("PGPORT", "5432")
    pg_db = os.getenv("PGDATABASE", "musique")
    pg_user = os.getenv("PGUSER", "postgres")
    pg_pass = os.getenv("PGPASSWORD", "postgres")
    dsn = f"dbname={pg_db} user={pg_user} password={pg_pass} host={pg_host} port={pg_port}"
    
    pg_repo = PostgresRepository(dsn)
    
    # 1. Libraries
    print("Migrating libraries...")
    libs = sqlite_repo.get_libraries()
    lib_id_map = {}
    for lib in libs:
        old_id = lib['id']
        new_lib = pg_repo.add_library(lib)
        lib_id_map[old_id] = new_lib['id']
    
    # 2. Genres
    print("Migrating genres...")
    genres = sqlite_repo.all_genres()
    genre_id_map = {}
    for g in genres:
        new_id = pg_repo.ensure_genre(g['name'])
        genre_id_map[g['id']] = new_id
        
    # 3. Artists
    print("Migrating artists...")
    artists = sqlite_repo.all_artists()
    artist_id_map = {}
    for a in artists:
        new_id = pg_repo.ensure_artist(
            a['name'], 
            image=a.get('image'), 
            bucket=a.get('bucket'), 
            library_id=lib_id_map.get(a.get('library_id'))
        )
        artist_id_map[a['id']] = new_id
        
    # 4. Albums
    print("Migrating albums...")
    albums = sqlite_repo.all_albums()
    album_id_map = {}
    for alb in albums:
        # We need an artist_id. Take the first one.
        old_artist_ids = alb.get("artistId", [])
        new_artist_id = artist_id_map.get(old_artist_ids[0]) if old_artist_ids else None
        
        if new_artist_id:
            new_genre_ids = [genre_id_map[gid] for gid in alb.get("genreIds", []) if gid in genre_id_map]
            new_id = pg_repo.ensure_album(
                alb["name"], 
                new_artist_id, 
                genre_ids=new_genre_ids,
                cover=alb.get("cover"),
                coverSmall=alb.get("coverSmall"),
                coverBucket=alb.get("coverBucket"),
                library_id=lib_id_map.get(alb.get("library_id"))
            )
            album_id_map[alb["id"]] = new_id
            
    # 5. Tracks
    print("Migrating tracks...")
    tracks = sqlite_repo.all_tracks()
    track_id_map = {}
    for t in tracks:
        new_album_id = album_id_map.get(t.get("albumId"))
        new_artist_id = artist_id_map.get(t.get("artistId"))
        
        if new_album_id and new_artist_id:
            new_id = pg_repo.add_track(
                t["title"],
                t.get("duration"),
                new_artist_id,
                new_album_id,
                t.get("albumTrack", 0),
                t["path"],
                t.get("bucket"),
                lib_id_map.get(t.get("library_id"))
            )
            track_id_map[t["id"]] = new_id
            
    # Update album track_ids in PG
    print("Updating album track arrays...")
    with pg_repo.pool.getconn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE albums a
                SET track_ids = sub.ids
                FROM (
                    SELECT album_id, array_agg(id ORDER BY album_track) ids
                    FROM tracks
                    GROUP BY album_id
                ) sub
                WHERE a.id = sub.album_id
            """)
            conn.commit()
        pg_repo.pool.putconn(conn)

    # 6. Users
    print("Migrating users...")
    users = sqlite_repo.get_user_all()
    user_id_map = {}
    for u in users:
        # Check if user exists
        if pg_repo.get_user_by_username(u["username"]):
            print(f"User {u['username']} already exists, skipping.")
            continue
            
        new_uid_str = pg_repo.create_user(u["username"], u["password"], u["email"], role=u.get("role", "user"))
        new_uid = int(new_uid_str)
        user_id_map[int(u['id'])] = new_uid
        
        # Migrate likes
        likes = u.get("like", {})
        for tid in likes.get("track", []):
            if tid in track_id_map:
                pg_repo.update_user_like(str(new_uid), "track", track_id_map[tid], True)
        for aid in likes.get("album", []):
            if aid in album_id_map:
                pg_repo.update_user_like(str(new_uid), "album", album_id_map[aid], True)
        for art_id in likes.get("artist", []):
            if art_id in artist_id_map:
                pg_repo.update_user_like(str(new_uid), "artist", artist_id_map[art_id], True)
        for pid in likes.get("playlist", []):
             # Playlists will be migrated later, skip for now or handle after
             pass
        
        # Migrate history
        history = u.get("history", [])
        for tid in reversed(history):
            if tid in track_id_map:
                pg_repo.add_track_to_history(str(new_uid), track_id_map[tid])
                
    # 7. Playlists
    print("Migrating playlists...")
    playlists = sqlite_repo.all_playlists()
    for p in playlists:
        new_owner_id = user_id_map.get(int(p['owner']))
        if new_owner_id:
            new_pid = pg_repo.create_playlist(str(new_owner_id), p['name'])
            track_ids = p.get('listMusique', [])
            new_track_ids = [track_id_map[tid] for tid in track_ids if tid in track_id_map]
            pg_repo.update_playlist_tracks(new_pid, new_track_ids, "add")

    print("Migration completed.")

if __name__ == "__main__":
    migrate()
