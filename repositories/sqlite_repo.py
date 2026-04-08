# /repositories/sqlite_repo.py
import logging
import uuid
from datetime import datetime
from typing import List
from sqlalchemy import create_engine, select, delete, func, desc
from sqlalchemy.orm import sessionmaker, joinedload
from repositories.base import BaseRepository
from repositories.models import Base, User, Artist, Album, Track, Genre, Playlist, Library, RegistrationToken, UserHistory, PlaylistTrack, album_artists, album_genres, user_like_tracks, user_like_albums, user_like_artists, user_like_playlists
from passlib.context import CryptContext

logger = logging.getLogger(__name__)
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

class SqliteRepository(BaseRepository):
    def __init__(self, db_url="sqlite:///./database.db"):
        self.engine = create_engine(db_url, connect_args={"check_same_thread": False})
        Base.metadata.create_all(self.engine)
        self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
        self._initialize_admin()

    def _initialize_admin(self):
        with self.SessionLocal() as session:
            admin = session.query(User).filter(User.role == "admin").first()
            if not admin:
                logger.info("Initializing default admin user...")
                hashed_password = pwd_context.hash("admin")
                new_admin = User(
                    username="admin",
                    password=hashed_password,
                    email="admin@admin.com",
                    role="admin"
                )
                session.add(new_admin)
                session.commit()
                logger.info("Default admin user created.")

    def _to_dict(self, obj):
        if obj is None:
            return None
        
        if isinstance(obj, User):
            return {
                "id": str(obj.id),
                "username": obj.username,
                "password": obj.password,
                "email": obj.email,
                "role": obj.role,
                "created_at": obj.created_at.isoformat() if obj.created_at else None,
                "top_genres": obj.top_genres,
                "history": [h.track_id for h in obj.history],
                "like": {
                    "track": [t.id for t in obj.liked_tracks],
                    "album": [a.id for a in obj.liked_albums],
                    "artist": [art.id for art in obj.liked_artists],
                    "playlist": [p.id for p in obj.liked_playlists]
                }
            }
        
        if isinstance(obj, Album):
            return {
                "id": obj.id,
                "name": obj.name,
                "artistId": [a.id for a in obj.artists],
                "genreIds": [g.id for g in obj.genres],
                "cover": obj.cover,
                "coverSmall": obj.coverSmall,
                "coverBucket": obj.coverBucket,
                "date": obj.date,
                "library_id": obj.library_id,
                "listMusique": [t.id for t in obj.tracks]
            }
            
        if isinstance(obj, Track):
            return {
                "id": obj.id,
                "title": obj.title,
                "duration": obj.duration,
                "artistId": obj.artist_id,
                "albumId": obj.album_id,
                "albumTrack": obj.album_track,
                "path": obj.path,
                "bucket": obj.bucket,
                "library_id": obj.library_id
            }
            
        if isinstance(obj, Artist):
            return {
                "id": obj.id,
                "name": obj.name,
                "image": obj.image,
                "bucket": obj.bucket,
                "library_id": obj.library_id,
                "listAlbums": [] # Needs more complex query or relationship if needed
            }
            
        if isinstance(obj, Genre):
            return {
                "id": obj.id,
                "name": obj.name
            }
            
        if isinstance(obj, Playlist):
            return {
                "id": obj.id,
                "name": obj.name,
                "owner": str(obj.owner_id),
                "listMusique": [t.id for t in obj.tracks]
            }
            
        if isinstance(obj, Library):
            return {
                "id": obj.id,
                "name": obj.name,
                "url": obj.url,
                "identifiers": obj.identifiers
            }
            
        return None

    def get_user_by_id(self, user_id: str):
        with self.SessionLocal() as session:
            user = session.query(User).filter(User.id == int(user_id)).first()
            return self._to_dict(user)

    def get_album(self, album_id: int):
        with self.SessionLocal() as session:
            album = session.query(Album).filter(Album.id == album_id).first()
            return self._to_dict(album)

    def get_track(self, track_id: int):
        with self.SessionLocal() as session:
            track = session.query(Track).filter(Track.id == track_id).first()
            return self._to_dict(track)

    def get_artist(self, artist_id: int):
        with self.SessionLocal() as session:
            artist = session.query(Artist).filter(Artist.id == artist_id).first()
            # To match JsonRepository's Artist dict including listAlbums
            if artist:
                d = self._to_dict(artist)
                albums = session.query(Album).filter(Album.artists.any(id=artist_id)).all()
                d["listAlbums"] = [a.id for a in albums]
                return d
            return None

    def get_genre(self, genre_id: int):
        with self.SessionLocal() as session:
            genre = session.query(Genre).filter(Genre.id == genre_id).first()
            return self._to_dict(genre)

    def get_playlist(self, playlist_id: int):
        with self.SessionLocal() as session:
            playlist = session.query(Playlist).filter(Playlist.id == playlist_id).first()
            return self._to_dict(playlist)

    def all_albums(self):
        with self.SessionLocal() as session:
            albums = session.query(Album).all()
            return [self._to_dict(a) for a in albums]

    def all_artists(self):
        with self.SessionLocal() as session:
            artists = session.query(Artist).all()
            return [self._to_dict(a) for a in artists]

    def all_genres(self):
        with self.SessionLocal() as session:
            genres = session.query(Genre).all()
            return [self._to_dict(g) for g in genres]

    def all_tracks(self):
        with self.SessionLocal() as session:
            tracks = session.query(Track).all()
            return [self._to_dict(t) for t in tracks]

    def all_playlists(self):
        with self.SessionLocal() as session:
            playlists = session.query(Playlist).all()
            return [self._to_dict(p) for p in playlists]

    def create_playlist(self, user_id: str, name: str) -> int:
        with self.SessionLocal() as session:
            new_playlist = Playlist(name=name, owner_id=int(user_id))
            session.add(new_playlist)
            session.commit()
            return new_playlist.id

    def update_playlist_tracks(self, playlist_id: int, track_ids: List[int], action: str):
        with self.SessionLocal() as session:
            playlist = session.query(Playlist).filter(Playlist.id == playlist_id).first()
            if not playlist:
                return None
            
            if action == "add":
                for tid in track_ids:
                    track = session.query(Track).filter(Track.id == tid).first()
                    if track and track not in playlist.tracks:
                        playlist.tracks.append(track)
            elif action == "del":
                for tid in track_ids:
                    track = session.query(Track).filter(Track.id == tid).first()
                    if track and track in playlist.tracks:
                        playlist.tracks.remove(track)
            
            session.commit()
            if not playlist.tracks:
                return "EMPTY"
            return self._to_dict(playlist)

    def update_user_like(self, user_id: str, obj_type: str, obj_id: int, like: bool):
        with self.SessionLocal() as session:
            user = session.query(User).filter(User.id == int(user_id)).first()
            if not user:
                return
            
            if obj_type == "track":
                obj = session.query(Track).filter(Track.id == obj_id).first()
                target_list = user.liked_tracks
            elif obj_type == "album":
                obj = session.query(Album).filter(Album.id == obj_id).first()
                target_list = user.liked_albums
            elif obj_type == "artist":
                obj = session.query(Artist).filter(Artist.id == obj_id).first()
                target_list = user.liked_artists
            elif obj_type == "playlist":
                obj = session.query(Playlist).filter(Playlist.id == obj_id).first()
                target_list = user.liked_playlists
            else:
                return

            if not obj:
                return

            if like:
                if obj not in target_list:
                    target_list.append(obj)
            else:
                if obj in target_list:
                    target_list.remove(obj)
            
            session.commit()

    def get_user_by_username(self, username: str):
        with self.SessionLocal() as session:
            user = session.query(User).filter(User.username == username).first()
            return self._to_dict(user)

    def delete_playlist(self, user_id: str, playlist_id: int):
        with self.SessionLocal() as session:
            playlist = session.query(Playlist).filter(Playlist.id == playlist_id, Playlist.owner_id == int(user_id)).first()
            if playlist:
                session.delete(playlist)
                session.commit()

    def create_registration_token(self) -> str:
        with self.SessionLocal() as session:
            token = str(uuid.uuid4())
            new_token = RegistrationToken(token=token)
            session.add(new_token)
            session.commit()
            return token

    def verify_registration_token(self, token: str) -> bool:
        with self.SessionLocal() as session:
            t = session.query(RegistrationToken).filter(RegistrationToken.token == token).first()
            return t is not None

    def consume_registration_token(self, token: str):
        with self.SessionLocal() as session:
            session.query(RegistrationToken).filter(RegistrationToken.token == token).delete()
            session.commit()

    def get_libraries(self):
        with self.SessionLocal() as session:
            libraries = session.query(Library).all()
            return [self._to_dict(lib) for lib in libraries]

    def update_library(self, library_id: int, library_data: dict):
        with self.SessionLocal() as session:
            lib = session.query(Library).filter(Library.id == library_id).first()
            if lib:
                lib.name = library_data.get("name", lib.name)
                lib.url = library_data.get("url", lib.url)
                lib.identifiers = library_data.get("identifiers", lib.identifiers)
                session.commit()
                return self._to_dict(lib)
            raise KeyError("Library not found")

    def add_library(self, library_data: dict):
        with self.SessionLocal() as session:
            new_lib = Library(
                name=library_data.get("name"),
                url=library_data.get("url"),
                identifiers=library_data.get("identifiers")
            )
            session.add(new_lib)
            session.commit()
            return self._to_dict(new_lib)

    def delete_library(self, library_id: int):
        with self.SessionLocal() as session:
            library = session.query(Library).filter(Library.id == library_id).first()
            if not library:
                raise KeyError("Library not found")

            session.query(Track).filter(Track.library_id == library_id).delete(synchronize_session='fetch')
            session.query(Album).filter(Album.library_id == library_id).delete(synchronize_session='fetch')
            session.query(Artist).filter(Artist.library_id == library_id).delete(synchronize_session='fetch')

            session.delete(library)
            session.commit()
            logger.info(f"Library with ID {library_id} and its associated data deleted.")
            return True

    def update_user_top_genres(self):
        with self.SessionLocal() as session:
            users = session.query(User).all()
            for user in users:
                genre_counts = {}
                history_tracks = session.query(Track).join(UserHistory).filter(UserHistory.user_id == user.id).all()
                for track in history_tracks:
                    album = track.album
                    if album:
                        for genre in album.genres:
                            genre_counts[genre.id] = genre_counts.get(genre.id, 0) + 1

                sorted_genres = sorted(genre_counts.items(), key=lambda x: x[1], reverse=True)[:10]

                top_genres_data = []
                for g_id, count in sorted_genres:
                    genre = session.query(Genre).filter(Genre.id == g_id).first()
                    if genre:
                        top_genres_data.append({
                            "id": genre.id,
                            "name": genre.name,
                            "count": count
                        })

                user.top_genres = top_genres_data
            session.commit()
    
    def get_user_all(self):
        with self.SessionLocal() as session:
            users = session.query(User).all()
            return [self._to_dict(u) for u in users]

    def create_user(self, username, password, email, role="user"):
        with self.SessionLocal() as session:
            if session.query(User).filter(User.username == username).first():
                raise ValueError("USERNAME_ALREADY_EXISTS")
            new_user = User(username=username, password=password, email=email, role=role)
            session.add(new_user)
            session.commit()
            return str(new_user.id)

    def set_user_role(self, user_id, role):
        with self.SessionLocal() as session:
            user = session.query(User).filter(User.id == int(user_id)).first()
            if user:
                user.role = role
                session.commit()
                return role
            raise KeyError("USER_NOT_FOUND")

    def set_username(self, user_id, new_username):
        with self.SessionLocal() as session:
            if session.query(User).filter(User.username == new_username, User.id != int(user_id)).first():
                raise ValueError("USERNAME_ALREADY_EXISTS")
            user = session.query(User).filter(User.id == int(user_id)).first()
            if user:
                user.username = new_username
                session.commit()
                return new_username
            raise KeyError("USER_NOT_FOUND")

    def set_user_password(self, user_id, new_password):
        with self.SessionLocal() as session:
            user = session.query(User).filter(User.id == int(user_id)).first()
            if user:
                user.password = new_password
                session.commit()
                return True
            raise KeyError("USER_NOT_FOUND")

    def add_track_to_history(self, user_id: str, track_id: int):
        with self.SessionLocal() as session:
            # Check if entry already exists to update timestamp (move to front)
            session.query(UserHistory).filter(
                UserHistory.user_id == int(user_id), 
                UserHistory.track_id == track_id
            ).delete()
            
            new_history = UserHistory(user_id=int(user_id), track_id=track_id)
            session.add(new_history)
            
            # Limit history to 200 items per user
            history_entries = session.query(UserHistory).filter(
                UserHistory.user_id == int(user_id)
            ).order_by(UserHistory.timestamp.desc()).all()
            
            if len(history_entries) > 200:
                for old_entry in history_entries[200:]:
                    session.delete(old_entry)
            
            session.commit()

    def delete_user(self, user_id: str):
        with self.SessionLocal() as session:
            user = session.query(User).filter(User.id == int(user_id)).first()
            if not user:
                raise KeyError("USER_NOT_FOUND")
            
            # Delete playlists owned by the user
            user_playlists = session.query(Playlist).filter(Playlist.owner_id == int(user_id)).all()
            for pl in user_playlists:
                # Clean up playlist associations
                session.query(PlaylistTrack).filter(PlaylistTrack.playlist_id == pl.id).delete()
                session.execute(user_like_playlists.delete().where(user_like_playlists.c.playlist_id == pl.id))
                session.delete(pl)
            
            # Clean up user's likes
            session.execute(user_like_tracks.delete().where(user_like_tracks.c.user_id == int(user_id)))
            session.execute(user_like_albums.delete().where(user_like_albums.c.user_id == int(user_id)))
            session.execute(user_like_artists.delete().where(user_like_artists.c.user_id == int(user_id)))
            session.execute(user_like_playlists.delete().where(user_like_playlists.c.user_id == int(user_id)))
            
            # history is handled by cascade="all, delete-orphan" in User model
            
            session.delete(user)
            session.commit()
            return True
            
    # Methods for scanning
    def ensure_genre(self, name):
        with self.SessionLocal() as session:
            genre = session.query(Genre).filter(Genre.name == name).first()
            if not genre:
                genre = Genre(name=name)
                session.add(genre)
                session.commit()
            return genre.id

    def ensure_artist(self, name, image=None, bucket=None, library_id=None):
        with self.SessionLocal() as session:
            artist = session.query(Artist).filter(Artist.name == name).first()
            if not artist:
                artist = Artist(name=name, image=image, bucket=bucket, library_id=library_id)
                session.add(artist)
                session.commit()
            elif image and not artist.image: # On ne met à jour que si l'image actuelle est vide
                artist.image = image
                if bucket:
                    artist.bucket = bucket
                session.commit()
            return artist.id

    def ensure_album(self, name, artist_id, genre_ids=None, cover=None, coverSmall=None, coverBucket=None, library_id=None):
        with self.SessionLocal() as session:
            # Album uniqueness often depends on name + artist
            album = session.query(Album).filter(Album.name == name, Album.artists.any(id=artist_id)).first()
            if not album:
                album = Album(name=name, cover=cover, coverSmall=coverSmall, coverBucket=coverBucket, library_id=library_id)
                artist = session.query(Artist).filter(Artist.id == artist_id).first()
                if artist:
                    album.artists.append(artist)
                if genre_ids:
                    for gid in genre_ids:
                        genre = session.query(Genre).filter(Genre.id == gid).first()
                        if genre:
                            album.genres.append(genre)
                session.add(album)
                session.commit()
            return album.id

    def add_track(self, title, duration, artist_id, album_id, album_track, path, bucket, library_id):
        with self.SessionLocal() as session:
            # Check if track already exists by path
            existing = session.query(Track).filter(Track.path == path, Track.album_id == album_id).first()
            if not existing:
                track = Track(
                    title=title, 
                    duration=duration, 
                    artist_id=artist_id, 
                    album_id=album_id, 
                    album_track=album_track,
                    path=path,
                    bucket=bucket,
                    library_id=library_id
                )
                session.add(track)
                session.commit()
                return track.id
            return existing.id

    def update_artist(self, artist_id, data):
        with self.SessionLocal() as session:
            artist = session.query(Artist).filter(Artist.id == artist_id).first()
            if artist:
                if "image" in data:
                    artist.image = data["image"]
                if "bucket" in data:
                    artist.bucket = data["bucket"]
                session.commit()
                return True
            return False

    def get_track_paths_by_library(self, library_id: int):
        with self.SessionLocal() as session:
            tracks = session.query(Track).filter(Track.library_id == library_id).all()
            return [t.path for t in tracks]

    def delete_track_by_path(self, path: str, library_id: int):
        with self.SessionLocal() as session:
            session.query(Track).filter(Track.path == path, Track.library_id == library_id).delete()
            session.commit()

    def search(self, query: str):
        q = f"%{query.lower()}%"
        with self.SessionLocal() as session:
            tracks = session.query(Track).filter(Track.title.ilike(q)).limit(50).all()
            albums = session.query(Album).filter(Album.name.ilike(q)).limit(50).all()
            artists = session.query(Artist).filter(Artist.name.ilike(q)).limit(50).all()
            
            return {
                "tracks": [self._to_dict(t) for t in tracks],
                "albums": [self._to_dict(a) for a in albums],
                "artists": [self._to_dict(art) for art in artists]
            }

    def get_user_by_email(self, email: str):
        with self.SessionLocal() as session:
            user = session.query(User).filter(User.email == email).first()
            return self._to_dict(user)

    def set_reset_token(self, email: str, token: str, expiry: datetime):
        with self.SessionLocal() as session:
            user = session.query(User).filter(User.email == email).first()
            if user:
                user.reset_token = token
                user.reset_token_expiry = expiry
                session.commit()
                return True
            return False

    def get_user_by_reset_token(self, token: str):
        with self.SessionLocal() as session:
            user = session.query(User).filter(User.reset_token == token).first()
            if user and user.reset_token_expiry > datetime.utcnow():
                return self._to_dict(user)
            return None

    def update_password_with_token(self, token: str, hashed_password: str):
        with self.SessionLocal() as session:
            user = session.query(User).filter(User.reset_token == token).first()
            if user and user.reset_token_expiry > datetime.utcnow():
                user.password = hashed_password
                user.reset_token = None
                user.reset_token_expiry = None
                session.commit()
                return True
            return False

    def get_all_active_reset_tokens(self) -> List[dict]:
        with self.SessionLocal() as session:
            now = datetime.utcnow()
            users = session.query(User).filter(User.reset_token != None, User.reset_token_expiry > now).all()
            return [{
                "username": u.username,
                "email": u.email,
                "token": u.reset_token,
                "expiry": u.reset_token_expiry.isoformat()
            } for u in users]
