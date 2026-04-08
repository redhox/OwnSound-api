# /repositories/postgres_repo.py
import logging
import uuid
import json
import psycopg2
from psycopg2 import pool, extras
from datetime import datetime
from typing import List, Dict, Any
from repositories.base import BaseRepository
from passlib.context import CryptContext

logger = logging.getLogger(__name__)
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

class PostgresRepository(BaseRepository):
    def __init__(self, dsn: str):
        self.dsn = dsn
        try:
            self.pool = pool.SimpleConnectionPool(1, 10, dsn)
            self._initialize_db()
            self._initialize_admin()
        except Exception as e:
            logger.error(f"Failed to connect to PostgreSQL: {e}")
            raise

    def _initialize_db(self):
        with self.pool.getconn() as conn:
            with conn.cursor() as cur:
                # Enable extensions
                cur.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm;")
                
                # Performance settings for session (init only)
                cur.execute("SET synchronous_commit TO OFF;")
                
                # Tables
                cur.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    id SERIAL PRIMARY KEY,
                    username TEXT UNIQUE NOT NULL,
                    password TEXT NOT NULL,
                    email TEXT,
                    role TEXT DEFAULT 'user',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    top_genres JSONB DEFAULT '[]'
                );
                
                CREATE TABLE IF NOT EXISTS libraries (
                    id SERIAL PRIMARY KEY,
                    name TEXT NOT NULL,
                    url TEXT,
                    identifiers JSONB
                );

                CREATE TABLE IF NOT EXISTS artists (
                    id BIGSERIAL PRIMARY KEY,
                    name TEXT NOT NULL,
                    image TEXT,
                    bucket TEXT,
                    library_id INTEGER REFERENCES libraries(id),
                    UNIQUE(name)
                );

                CREATE TABLE IF NOT EXISTS genres (
                    id SERIAL PRIMARY KEY,
                    name TEXT UNIQUE NOT NULL
                );

                CREATE TABLE IF NOT EXISTS albums (
                    id BIGSERIAL PRIMARY KEY,
                    name TEXT NOT NULL,
                    cover TEXT,
                    cover_small TEXT,
                    cover_bucket TEXT,
                    date TEXT,
                    library_id INTEGER REFERENCES libraries(id),
                    track_ids BIGINT[] DEFAULT '{}',
                    UNIQUE(name, library_id)
                );

                CREATE TABLE IF NOT EXISTS tracks (
                    id BIGSERIAL PRIMARY KEY,
                    title TEXT NOT NULL,
                    duration TEXT,
                    album_id BIGINT REFERENCES albums(id),
                    artist_id BIGINT REFERENCES artists(id),
                    album_track INTEGER,
                    path TEXT NOT NULL,
                    bucket TEXT,
                    library_id INTEGER REFERENCES libraries(id),
                    UNIQUE(path, library_id)
                );

                CREATE TABLE IF NOT EXISTS album_artists (
                    album_id BIGINT REFERENCES albums(id),
                    artist_id BIGINT REFERENCES artists(id),
                    PRIMARY KEY (album_id, artist_id)
                );

                CREATE TABLE IF NOT EXISTS album_genres (
                    album_id BIGINT REFERENCES albums(id),
                    genre_id INTEGER REFERENCES genres(id),
                    PRIMARY KEY (album_id, genre_id)
                );

                CREATE TABLE IF NOT EXISTS user_like_tracks (
                    user_id INTEGER REFERENCES users(id),
                    track_id BIGINT REFERENCES tracks(id),
                    PRIMARY KEY (user_id, track_id)
                );

                CREATE TABLE IF NOT EXISTS user_like_albums (
                    user_id INTEGER REFERENCES users(id),
                    album_id BIGINT REFERENCES albums(id),
                    PRIMARY KEY (user_id, album_id)
                );

                CREATE TABLE IF NOT EXISTS user_like_artists (
                    user_id INTEGER REFERENCES users(id),
                    artist_id BIGINT REFERENCES artists(id),
                    PRIMARY KEY (user_id, artist_id)
                );

                CREATE TABLE IF NOT EXISTS user_like_playlists (
                    user_id INTEGER REFERENCES users(id),
                    playlist_id INTEGER, -- defined later
                    PRIMARY KEY (user_id, playlist_id)
                );

                CREATE TABLE IF NOT EXISTS user_history (
                    id BIGSERIAL PRIMARY KEY,
                    user_id INTEGER REFERENCES users(id),
                    track_id BIGINT REFERENCES tracks(id),
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS playlists (
                    id SERIAL PRIMARY KEY,
                    name TEXT NOT NULL,
                    owner_id INTEGER REFERENCES users(id)
                );

                CREATE TABLE IF NOT EXISTS playlist_tracks (
                    playlist_id INTEGER REFERENCES playlists(id),
                    track_id BIGINT REFERENCES tracks(id),
                    position INTEGER,
                    PRIMARY KEY (playlist_id, track_id)
                );

                CREATE TABLE IF NOT EXISTS registration_tokens (
                    token TEXT PRIMARY KEY,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );

                CREATE UNLOGGED TABLE IF NOT EXISTS tracks_staging (
                    artist_name TEXT,
                    album_name TEXT,
                    genre_names TEXT,
                    title TEXT,
                    duration TEXT,
                    album_track INTEGER,
                    path TEXT,
                    bucket TEXT,
                    cover TEXT,
                    cover_small TEXT,
                    cover_bucket TEXT,
                    date TEXT,
                    library_id INTEGER
                );

                -- Indexes
                CREATE INDEX IF NOT EXISTS idx_tracks_title_trgm ON tracks USING GIN(title gin_trgm_ops);
                CREATE INDEX IF NOT EXISTS idx_albums_name_trgm ON albums USING GIN(name gin_trgm_ops);
                CREATE INDEX IF NOT EXISTS idx_artists_name_trgm ON artists USING GIN(name gin_trgm_ops);
                CREATE INDEX IF NOT EXISTS idx_tracks_album_id ON tracks(album_id);
                CREATE INDEX IF NOT EXISTS idx_tracks_artist_id ON tracks(artist_id);
                """)
                
                # Ensure types are correct if they were created with wrong types before
                try:
                    cur.execute("ALTER TABLE artists ALTER COLUMN bucket TYPE TEXT;")
                    cur.execute("ALTER TABLE albums ALTER COLUMN cover_bucket TYPE TEXT;")
                    cur.execute("ALTER TABLE tracks_staging ALTER COLUMN cover_bucket TYPE TEXT;")
                except Exception as e:
                    logger.warning(f"Failed to alter columns (might be normal if already correct): {e}")
                
            conn.commit()
            self.pool.putconn(conn)

    def _initialize_admin(self):
        with self.pool.getconn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1 FROM users WHERE role = 'admin' LIMIT 1")
                if not cur.fetchone():
                    logger.info("Initializing default admin user for PostgreSQL...")
                    hashed_password = pwd_context.hash("admin")
                    cur.execute("""
                        INSERT INTO users (username, password, email, role)
                        VALUES (%s, %s, %s, %s)
                    """, ("admin", hashed_password, "admin@admin.com", "admin"))
                    conn.commit()
                    logger.info("Default admin user created.")
            self.pool.putconn(conn)

    def _get_conn(self):
        return self.pool.getconn()

    def _put_conn(self, conn):
        self.pool.putconn(conn)

    def get_user_by_id(self, user_id: str):
        conn = self._get_conn()
        try:
            with conn.cursor(cursor_factory=extras.RealDictCursor) as cur:
                cur.execute("SELECT * FROM users WHERE id = %s", (int(user_id),))
                user = cur.fetchone()
                if user:
                    # Fetch likes and history
                    cur.execute("SELECT track_id FROM user_like_tracks WHERE user_id = %s", (int(user_id),))
                    user['like'] = {'track': [r['track_id'] for r in cur.fetchall()]}
                    cur.execute("SELECT album_id FROM user_like_albums WHERE user_id = %s", (int(user_id),))
                    user['like']['album'] = [r['album_id'] for r in cur.fetchall()]
                    cur.execute("SELECT artist_id FROM user_like_artists WHERE user_id = %s", (int(user_id),))
                    user['like']['artist'] = [r['artist_id'] for r in cur.fetchall()]
                    cur.execute("SELECT playlist_id FROM user_like_playlists WHERE user_id = %s", (int(user_id),))
                    user['like']['playlist'] = [r['playlist_id'] for r in cur.fetchall()]
                    
                    cur.execute("SELECT track_id FROM user_history WHERE user_id = %s ORDER BY timestamp DESC", (int(user_id),))
                    user['history'] = [r['track_id'] for r in cur.fetchall()]
                    
                    if isinstance(user['top_genres'], str):
                        user['top_genres'] = json.loads(user['top_genres'])
                    
                    user['id'] = str(user['id'])
                return dict(user) if user else None
        finally:
            self._put_conn(conn)

    def get_user_all(self):
        conn = self._get_conn()
        try:
            with conn.cursor(cursor_factory=extras.RealDictCursor) as cur:
                cur.execute("SELECT * FROM users")
                users = cur.fetchall()
                result = []
                for user in users:
                    user_id = user['id']
                    # Fetch likes and history
                    cur.execute("SELECT track_id FROM user_like_tracks WHERE user_id = %s", (user_id,))
                    user['like'] = {'track': [r['track_id'] for r in cur.fetchall()]}
                    cur.execute("SELECT album_id FROM user_like_albums WHERE user_id = %s", (user_id,))
                    user['like']['album'] = [r['album_id'] for r in cur.fetchall()]
                    cur.execute("SELECT artist_id FROM user_like_artists WHERE user_id = %s", (user_id,))
                    user['like']['artist'] = [r['artist_id'] for r in cur.fetchall()]
                    cur.execute("SELECT playlist_id FROM user_like_playlists WHERE user_id = %s", (user_id,))
                    user['like']['playlist'] = [r['playlist_id'] for r in cur.fetchall()]
                    
                    cur.execute("SELECT track_id FROM user_history WHERE user_id = %s ORDER BY timestamp DESC", (user_id,))
                    user['history'] = [r['track_id'] for r in cur.fetchall()]
                    
                    if isinstance(user['top_genres'], str):
                        user['top_genres'] = json.loads(user['top_genres'])
                    
                    user['id'] = str(user['id'])
                    result.append(dict(user))
                return result
        finally:
            self._put_conn(conn)

    def get_album(self, album_id: int):
        conn = self._get_conn()
        try:
            with conn.cursor(cursor_factory=extras.RealDictCursor) as cur:
                cur.execute("""
                    SELECT a.*, 
                           COALESCE(array_agg(DISTINCT aa.artist_id) FILTER (WHERE aa.artist_id IS NOT NULL), '{}') as "artistId",
                           COALESCE(array_agg(DISTINCT ag.genre_id) FILTER (WHERE ag.genre_id IS NOT NULL), '{}') as "genreIds"
                    FROM albums a
                    LEFT JOIN album_artists aa ON a.id = aa.album_id
                    LEFT JOIN album_genres ag ON a.id = ag.album_id
                    WHERE a.id = %s
                    GROUP BY a.id
                """, (album_id,))
                album = cur.fetchone()
                if album:
                    album['listMusique'] = album.pop('track_ids') or []
                    album['coverSmall'] = album.pop('cover_small')
                    album['coverBucket'] = album.pop('cover_bucket')
                return dict(album) if album else None
        finally:
            self._put_conn(conn)

    def get_track(self, track_id: int):
        conn = self._get_conn()
        try:
            with conn.cursor(cursor_factory=extras.RealDictCursor) as cur:
                cur.execute("SELECT * FROM tracks WHERE id = %s", (track_id,))
                track = cur.fetchone()
                if track:
                    track['artistId'] = track.pop('artist_id')
                    track['albumId'] = track.pop('album_id')
                    track['albumTrack'] = track.pop('album_track')
                return dict(track) if track else None
        finally:
            self._put_conn(conn)

    def get_artist(self, artist_id: int):
        conn = self._get_conn()
        try:
            with conn.cursor(cursor_factory=extras.RealDictCursor) as cur:
                cur.execute("""
                    SELECT art.*, 
                           COALESCE(array_agg(DISTINCT aa.album_id) FILTER (WHERE aa.album_id IS NOT NULL), '{}') as "listAlbums"
                    FROM artists art
                    LEFT JOIN album_artists aa ON art.id = aa.artist_id
                    WHERE art.id = %s
                    GROUP BY art.id
                """, (artist_id,))
                artist = cur.fetchone()
                return dict(artist) if artist else None
        finally:
            self._put_conn(conn)

    def get_genre(self, genre_id: int):
        conn = self._get_conn()
        try:
            with conn.cursor(cursor_factory=extras.RealDictCursor) as cur:
                cur.execute("SELECT * FROM genres WHERE id = %s", (genre_id,))
                res = cur.fetchone()
                return dict(res) if res else None
        finally:
            self._put_conn(conn)

    def get_playlist(self, playlist_id: int):
        conn = self._get_conn()
        try:
            with conn.cursor(cursor_factory=extras.RealDictCursor) as cur:
                cur.execute("""
                    SELECT p.*, 
                           COALESCE(array_agg(pt.track_id ORDER BY pt.position) FILTER (WHERE pt.track_id IS NOT NULL), '{}') as "listMusique"
                    FROM playlists p
                    LEFT JOIN playlist_tracks pt ON p.id = pt.playlist_id
                    WHERE p.id = %s
                    GROUP BY p.id
                """, (playlist_id,))
                playlist = cur.fetchone()
                if playlist:
                    playlist['owner'] = str(playlist.pop('owner_id'))
                return dict(playlist) if playlist else None
        finally:
            self._put_conn(conn)

    def all_albums(self):
        conn = self._get_conn()
        try:
            with conn.cursor(cursor_factory=extras.RealDictCursor) as cur:
                cur.execute("""
                    SELECT a.*, 
                           COALESCE(array_agg(DISTINCT aa.artist_id) FILTER (WHERE aa.artist_id IS NOT NULL), '{}') as "artistId",
                           COALESCE(array_agg(DISTINCT ag.genre_id) FILTER (WHERE ag.genre_id IS NOT NULL), '{}') as "genreIds"
                    FROM albums a
                    LEFT JOIN album_artists aa ON a.id = aa.album_id
                    LEFT JOIN album_genres ag ON a.id = ag.album_id
                    GROUP BY a.id
                """)
                albums = cur.fetchall()
                for a in albums:
                    a['listMusique'] = a.pop('track_ids') or []
                    a['coverSmall'] = a.pop('cover_small')
                    a['coverBucket'] = a.pop('cover_bucket')
                return [dict(a) for a in albums]
        finally:
            self._put_conn(conn)

    def all_artists(self):
        conn = self._get_conn()
        try:
            with conn.cursor(cursor_factory=extras.RealDictCursor) as cur:
                cur.execute("""
                    SELECT art.*,
                           COALESCE(array_agg(DISTINCT aa.album_id) FILTER (WHERE aa.album_id IS NOT NULL), '{}') as "listAlbums"
                    FROM artists art
                    LEFT JOIN album_artists aa ON art.id = aa.artist_id
                    GROUP BY art.id
                """)
                return [dict(a) for a in cur.fetchall()]
        finally:
            self._put_conn(conn)

    def all_genres(self):
        conn = self._get_conn()
        try:
            with conn.cursor(cursor_factory=extras.RealDictCursor) as cur:
                cur.execute("SELECT * FROM genres")
                return [dict(g) for g in cur.fetchall()]
        finally:
            self._put_conn(conn)

    def all_playlists(self):
        conn = self._get_conn()
        try:
            with conn.cursor(cursor_factory=extras.RealDictCursor) as cur:
                cur.execute("""
                    SELECT p.*, 
                           COALESCE(array_agg(pt.track_id ORDER BY pt.position) FILTER (WHERE pt.track_id IS NOT NULL), '{}') as "listMusique"
                    FROM playlists p
                    LEFT JOIN playlist_tracks pt ON p.id = pt.playlist_id
                    GROUP BY p.id
                """)
                playlists = cur.fetchall()
                for p in playlists:
                    p['owner'] = str(p.pop('owner_id'))
                return [dict(p) for p in playlists]
        finally:
            self._put_conn(conn)

    def all_tracks(self):
        conn = self._get_conn()
        try:
            with conn.cursor(cursor_factory=extras.RealDictCursor) as cur:
                cur.execute("SELECT * FROM tracks")
                tracks = cur.fetchall()
                for t in tracks:
                    t['artistId'] = t.pop('artist_id')
                    t['albumId'] = t.pop('album_id')
                    t['albumTrack'] = t.pop('album_track')
                return [dict(t) for t in tracks]
        finally:
            self._put_conn(conn)

    def create_playlist(self, user_id: str, name: str) -> int:
        conn = self._get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("INSERT INTO playlists (name, owner_id) VALUES (%s, %s) RETURNING id", (name, int(user_id)))
                pid = cur.fetchone()[0]
                conn.commit()
                return pid
        finally:
            self._put_conn(conn)

    def update_playlist_tracks(self, playlist_id: int, track_ids: List[int], action: str):
        conn = self._get_conn()
        try:
            with conn.cursor() as cur:
                if action == "add":
                    for tid in track_ids:
                        cur.execute("INSERT INTO playlist_tracks (playlist_id, track_id) VALUES (%s, %s) ON CONFLICT DO NOTHING", (playlist_id, tid))
                elif action == "del":
                    for tid in track_ids:
                        cur.execute("DELETE FROM playlist_tracks WHERE playlist_id = %s AND track_id = %s", (playlist_id, tid))
                conn.commit()
                return self.get_playlist(playlist_id)
        finally:
            self._put_conn(conn)

    def update_user_like(self, user_id: str, obj_type: str, obj_id: int, like: bool):
        conn = self._get_conn()
        table_map = {
            "track": "user_like_tracks",
            "album": "user_like_albums",
            "artist": "user_like_artists",
            "playlist": "user_like_playlists"
        }
        id_col_map = {
            "track": "track_id",
            "album": "album_id",
            "artist": "artist_id",
            "playlist": "playlist_id"
        }
        table = table_map.get(obj_type)
        id_col = id_col_map.get(obj_type)
        if not table: return

        try:
            with conn.cursor() as cur:
                if like:
                    cur.execute(f"INSERT INTO {table} (user_id, {id_col}) VALUES (%s, %s) ON CONFLICT DO NOTHING", (int(user_id), obj_id))
                else:
                    cur.execute(f"DELETE FROM {table} WHERE user_id = %s AND {id_col} = %s", (int(user_id), obj_id))
                conn.commit()
        finally:
            self._put_conn(conn)

    def get_user_by_username(self, username: str):
        conn = self._get_conn()
        try:
            with conn.cursor(cursor_factory=extras.RealDictCursor) as cur:
                cur.execute("SELECT * FROM users WHERE username = %s", (username,))
                user = cur.fetchone()
                if user:
                    return self.get_user_by_id(user['id'])
                return None
        finally:
            self._put_conn(conn)

    def delete_playlist(self, user_id: str, playlist_id: int):
        conn = self._get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM playlists WHERE id = %s AND owner_id = %s", (playlist_id, int(user_id)))
                conn.commit()
        finally:
            self._put_conn(conn)

    def create_registration_token(self) -> str:
        token = str(uuid.uuid4())
        conn = self._get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("INSERT INTO registration_tokens (token) VALUES (%s)", (token,))
                conn.commit()
                return token
        finally:
            self._put_conn(conn)

    def verify_registration_token(self, token: str) -> bool:
        conn = self._get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT 1 FROM registration_tokens WHERE token = %s", (token,))
                return cur.fetchone() is not None
        finally:
            self._put_conn(conn)

    def consume_registration_token(self, token: str):
        conn = self._get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM registration_tokens WHERE token = %s", (token,))
                conn.commit()
        finally:
            self._put_conn(conn)

    def get_libraries(self):
        conn = self._get_conn()
        try:
            with conn.cursor(cursor_factory=extras.RealDictCursor) as cur:
                cur.execute("SELECT * FROM libraries")
                libs = cur.fetchall()
                for lib in libs:
                    if isinstance(lib['identifiers'], str):
                        lib['identifiers'] = json.loads(lib['identifiers'])
                return [dict(l) for l in libs]
        finally:
            self._put_conn(conn)

    def update_library(self, library_id: int, library_data: dict):
        conn = self._get_conn()
        try:
            with conn.cursor(cursor_factory=extras.RealDictCursor) as cur:
                cur.execute("""
                    UPDATE libraries 
                    SET name = %s, url = %s, identifiers = %s 
                    WHERE id = %s 
                    RETURNING *
                """, (library_data['name'], library_data['url'], json.dumps(library_data['identifiers']), library_id))
                res = cur.fetchone()
                conn.commit()
                if res:
                    if isinstance(res['identifiers'], str):
                        res['identifiers'] = json.loads(res['identifiers'])
                    return dict(res)
                raise KeyError("Library not found")
        finally:
            self._put_conn(conn)

    def add_library(self, library_data: dict):
        conn = self._get_conn()
        try:
            with conn.cursor(cursor_factory=extras.RealDictCursor) as cur:
                cur.execute("""
                    INSERT INTO libraries (name, url, identifiers) 
                    VALUES (%s, %s, %s) 
                    RETURNING *
                """, (library_data['name'], library_data['url'], json.dumps(library_data['identifiers'])))
                res = cur.fetchone()
                conn.commit()
                if res:
                    if isinstance(res['identifiers'], str):
                        res['identifiers'] = json.loads(res['identifiers'])
                    return dict(res)
        finally:
            self._put_conn(conn)

    def delete_library(self, library_id: int):
        conn = self._get_conn()
        try:
            with conn.cursor() as cur:
                # Use subqueries or separate deletes since we don't have cascades everywhere in this raw implementation
                cur.execute("DELETE FROM tracks WHERE library_id = %s", (library_id,))
                cur.execute("DELETE FROM albums WHERE library_id = %s", (library_id,))
                cur.execute("DELETE FROM artists WHERE library_id = %s", (library_id,))
                cur.execute("DELETE FROM libraries WHERE id = %s", (library_id,))
                conn.commit()
                return True
        finally:
            self._put_conn(conn)

    def update_user_top_genres(self):
        # Implementation of top genres update using SQL
        conn = self._get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT id FROM users")
                user_ids = [r[0] for r in cur.fetchall()]
                for uid in user_ids:
                    cur.execute("""
                        SELECT g.id, g.name, COUNT(*) as count
                        FROM user_history uh
                        JOIN tracks t ON uh.track_id = t.id
                        JOIN albums a ON t.album_id = a.id
                        JOIN album_genres ag ON a.id = ag.album_id
                        JOIN genres g ON ag.genre_id = g.id
                        WHERE uh.user_id = %s
                        GROUP BY g.id, g.name
                        ORDER BY count DESC
                        LIMIT 10
                    """, (uid,))
                    top_genres = [{"id": r[0], "name": r[1], "count": r[2]} for r in cur.fetchall()]
                    cur.execute("UPDATE users SET top_genres = %s WHERE id = %s", (json.dumps(top_genres), uid))
                conn.commit()
        finally:
            self._put_conn(conn)

    def create_user(self, username, password, email, role="user"):
        conn = self._get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("INSERT INTO users (username, password, email, role) VALUES (%s, %s, %s, %s) RETURNING id", (username, password, email, role))
                uid = cur.fetchone()[0]
                conn.commit()
                return str(uid)
        finally:
            self._put_conn(conn)

    def delete_user(self, user_id: str):
        conn = self._get_conn()
        uid = int(user_id)
        try:
            with conn.cursor() as cur:
                # 1. Delete playlists owned by the user
                cur.execute("SELECT id FROM playlists WHERE owner_id = %s", (uid,))
                playlist_ids = [r[0] for r in cur.fetchall()]
                
                if playlist_ids:
                    pids = tuple(playlist_ids)
                    if len(pids) == 1:
                        cur.execute("DELETE FROM playlist_tracks WHERE playlist_id = %s", (pids[0],))
                        cur.execute("DELETE FROM user_like_playlists WHERE playlist_id = %s", (pids[0],))
                    else:
                        cur.execute("DELETE FROM playlist_tracks WHERE playlist_id IN %s", (pids,))
                        cur.execute("DELETE FROM user_like_playlists WHERE playlist_id IN %s", (pids,))
                    
                    cur.execute("DELETE FROM playlists WHERE owner_id = %s", (uid,))

                # 2. Clean up user's likes
                cur.execute("DELETE FROM user_like_tracks WHERE user_id = %s", (uid,))
                cur.execute("DELETE FROM user_like_albums WHERE user_id = %s", (uid,))
                cur.execute("DELETE FROM user_like_artists WHERE user_id = %s", (uid,))
                cur.execute("DELETE FROM user_like_playlists WHERE user_id = %s", (uid,))
                
                # 3. Clean up history
                cur.execute("DELETE FROM user_history WHERE user_id = %s", (uid,))
                
                # 4. Finally delete the user
                cur.execute("DELETE FROM users WHERE id = %s", (uid,))
                
                conn.commit()
                return True
        finally:
            self._put_conn(conn)

    def set_user_role(self, user_id, role):
        conn = self._get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("UPDATE users SET role = %s WHERE id = %s", (role, int(user_id)))
                conn.commit()
                return role
        finally:
            self._put_conn(conn)

    def set_username(self, user_id, new_username):
        conn = self._get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("UPDATE users SET username = %s WHERE id = %s", (new_username, int(user_id)))
                conn.commit()
                return new_username
        finally:
            self._put_conn(conn)

    def set_user_password(self, user_id, new_password):
        conn = self._get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("UPDATE users SET password = %s WHERE id = %s", (new_password, int(user_id)))
                conn.commit()
                return True
        finally:
            self._put_conn(conn)

    def add_track_to_history(self, user_id: str, track_id: int):
        conn = self._get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM user_history WHERE user_id = %s AND track_id = %s", (int(user_id), track_id))
                cur.execute("INSERT INTO user_history (user_id, track_id) VALUES (%s, %s)", (int(user_id), track_id))
                # Cleanup old history
                cur.execute("""
                    DELETE FROM user_history 
                    WHERE id IN (
                        SELECT id FROM user_history 
                        WHERE user_id = %s 
                        ORDER BY timestamp DESC 
                        OFFSET 200
                    )
                """, (int(user_id),))
                conn.commit()
        finally:
            self._put_conn(conn)

    # Scanning methods optimized for PostgreSQL
    def ensure_genre(self, name):
        conn = self._get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("INSERT INTO genres (name) VALUES (%s) ON CONFLICT (name) DO UPDATE SET name=EXCLUDED.name RETURNING id", (name,))
                gid = cur.fetchone()[0]
                conn.commit()
                return gid
        finally:
            self._put_conn(conn)

    def ensure_artist(self, name, image=None, bucket=None, library_id=None):
        conn = self._get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO artists (name, image, bucket, library_id) 
                    VALUES (%s, %s, %s, %s) 
                    ON CONFLICT (name) DO UPDATE SET image = COALESCE(artists.image, EXCLUDED.image)
                    RETURNING id
                """, (name, image, bucket, library_id))
                aid = cur.fetchone()[0]
                conn.commit()
                return aid
        finally:
            self._put_conn(conn)

    def ensure_album(self, name, artist_id, genre_ids=None, cover=None, coverSmall=None, coverBucket=None, library_id=None):
        conn = self._get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO albums (name, cover, cover_small, cover_bucket, library_id) 
                    VALUES (%s, %s, %s, %s, %s) 
                    ON CONFLICT (name, library_id) DO UPDATE SET name=EXCLUDED.name 
                    RETURNING id
                """, (name, cover, coverSmall, coverBucket, library_id))
                albid = cur.fetchone()[0]
                
                # Update album_artists
                cur.execute("INSERT INTO album_artists (album_id, artist_id) VALUES (%s, %s) ON CONFLICT DO NOTHING", (albid, artist_id))
                
                # Update album_genres
                if genre_ids:
                    for gid in genre_ids:
                        cur.execute("INSERT INTO album_genres (album_id, genre_id) VALUES (%s, %s) ON CONFLICT DO NOTHING", (albid, gid))
                
                conn.commit()
                return albid
        finally:
            self._put_conn(conn)

    def add_track(self, title, duration, artist_id, album_id, album_track, path, bucket, library_id):
        conn = self._get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO tracks (title, duration, artist_id, album_id, album_track, path, bucket, library_id) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s) 
                    ON CONFLICT (path, library_id) DO UPDATE SET title=EXCLUDED.title
                    RETURNING id
                """, (title, duration, artist_id, album_id, album_track, path, bucket, library_id))
                tid = cur.fetchone()[0]
                conn.commit()
                return tid
        finally:
            self._put_conn(conn)

    def update_artist(self, artist_id: int, data: dict):
        conn = self._get_conn()
        try:
            with conn.cursor() as cur:
                if "image" in data and "bucket" in data:
                    cur.execute("UPDATE artists SET image = %s, bucket = %s WHERE id = %s", (data["image"], data["bucket"], artist_id))
                elif "image" in data:
                    cur.execute("UPDATE artists SET image = %s WHERE id = %s", (data["image"], artist_id))
                elif "bucket" in data:
                    cur.execute("UPDATE artists SET bucket = %s WHERE id = %s", (data["bucket"], artist_id))
                else:
                    return False
                conn.commit()
                return cur.rowcount > 0
        finally:
            self._put_conn(conn)

    def get_track_paths_by_library(self, library_id: int):
        conn = self._get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT path FROM tracks WHERE library_id = %s", (library_id,))
                return [r[0] for r in cur.fetchall()]
        finally:
            self._put_conn(conn)

    def delete_track_by_path(self, path: str, library_id: int):
        conn = self._get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM tracks WHERE path = %s AND library_id = %s", (path, library_id))
                conn.commit()
        finally:
            self._put_conn(conn)

    def search(self, query: str):
        conn = self._get_conn()
        q = f"%{query}%"
        try:
            with conn.cursor(cursor_factory=extras.RealDictCursor) as cur:
                cur.execute("SELECT * FROM tracks WHERE title ILIKE %s LIMIT 100", (q,))
                tracks = [dict(t) for t in cur.fetchall()]
                for t in tracks:
                    t['artistId'] = t.pop('artist_id')
                    t['albumId'] = t.pop('album_id')
                    t['albumTrack'] = t.pop('album_track')
                
                cur.execute("SELECT * FROM albums WHERE name ILIKE %s LIMIT 50", (q,))
                albums = [dict(a) for a in cur.fetchall()]
                for a in albums:
                    a['artistId'] = [] # Need proper mapping if required, but for search results this often suffices
                    a['coverSmall'] = a.pop('cover_small')
                    a['coverBucket'] = a.pop('cover_bucket')
                
                cur.execute("SELECT * FROM artists WHERE name ILIKE %s LIMIT 50", (q,))
                artists = [dict(art) for art in cur.fetchall()]
                
                return {
                    "tracks": tracks,
                    "albums": albums,
                    "artists": artists
                }
        finally:
            self._put_conn(conn)

    # --- PERFORMANCE OPTIMIZED BULK LOAD ---
    def copy_to_staging(self, file_obj):
        conn = self._get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("TRUNCATE tracks_staging")
                cur.copy_expert("COPY tracks_staging FROM STDIN WITH (FORMAT csv, DELIMITER E'\\t')", file_obj)
                conn.commit()
        finally:
            self._put_conn(conn)

    def bulk_import_from_staging(self):
        conn = self._get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("SET synchronous_commit TO OFF")
                
                # 1. Genres
                cur.execute("""
                    INSERT INTO genres (name)
                    SELECT DISTINCT unnest(string_to_array(genre_names, ','))
                    FROM tracks_staging
                    WHERE genre_names IS NOT NULL
                    ON CONFLICT (name) DO NOTHING
                """)

                # 2. Artists
                cur.execute("""
                    INSERT INTO artists (name, library_id)
                    SELECT artist_name, MIN(library_id)
                    FROM tracks_staging
                    GROUP BY artist_name
                    ON CONFLICT (name) DO NOTHING
                """)

                # 3. Albums
                cur.execute("""
                    INSERT INTO albums (name, cover, cover_small, cover_bucket, date, library_id)
                    SELECT album_name, MIN(cover), MIN(cover_small), MIN(cover_bucket), MIN(date), library_id
                    FROM tracks_staging
                    WHERE album_name IS NOT NULL
                    GROUP BY album_name, library_id
                    ON CONFLICT (name, library_id) DO NOTHING
                """)

                # 4. Tracks
                cur.execute("""
                    INSERT INTO tracks (title, duration, artist_id, album_id, album_track, path, bucket, library_id)
                    SELECT 
                        s.title, s.duration, a.id, al.id, s.album_track, s.path, s.bucket, s.library_id
                    FROM tracks_staging s
                    JOIN artists a ON a.name = s.artist_name
                    JOIN albums al ON al.name = s.album_name AND al.library_id = s.library_id
                    ON CONFLICT (path, library_id) DO NOTHING
                """)

                # 5. Junctions: album_artists
                cur.execute("""
                    INSERT INTO album_artists (album_id, artist_id)
                    SELECT DISTINCT al.id, a.id
                    FROM tracks_staging s
                    JOIN artists a ON a.name = s.artist_name
                    JOIN albums al ON al.name = s.album_name AND al.library_id = s.library_id
                    ON CONFLICT DO NOTHING
                """)

                # 6. Junctions: album_genres
                cur.execute("""
                    INSERT INTO album_genres (album_id, genre_id)
                    SELECT DISTINCT al.id, g.id
                    FROM tracks_staging s
                    JOIN albums al ON al.name = s.album_name AND al.library_id = s.library_id
                    CROSS JOIN LATERAL unnest(string_to_array(s.genre_names, ',')) AS gn
                    JOIN genres g ON g.name = gn
                    ON CONFLICT DO NOTHING
                """)

                # 7. Denormalization: update album.track_ids
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
                cur.execute("SET synchronous_commit TO ON")
        finally:
            self._put_conn(conn)
