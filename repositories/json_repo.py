# /repositories/json_repo.py
import logging
import uuid
from datetime import date
import json
from repositories.base import BaseRepository
logger = logging.getLogger(__name__)
class JsonRepository(BaseRepository):

    def __init__(self, path="./database.json"):
        self.path = path
        self._load()

    def _load(self):
        with open(self.path, "r", encoding="utf-8") as f:
            self.data = json.load(f)

    def _save(self):
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(self.data, f, ensure_ascii=False, indent=2)

    def get_album(self, album_id: int):
        return self.data["albums"].get(str(album_id))

    def get_track(self, track_id: int):
        return self.data["tracks"].get(str(track_id))

    def get_genre(self, genre_id: int):
        return self.data.get("genres", {}).get(str(genre_id))

    def get_artist(self, artist_id: int):
        return self.data["artists"].get(str(artist_id))

    def get_playlist(self, playlist_id: int):
        return self.data["playlists"].get(str(playlist_id))

    def all_albums(self):
        return self.data["albums"].values()

    def all_artists(self):
        return self.data["artists"].values()

    def all_genres(self):
        return self.data.get("genres", {}).values()

    def all_tracks(self):
        return self.data["tracks"].values()

    def all_playlists(self):
        return self.data["playlists"].values()

    def create_playlist(self, user_id: str, name: str) -> int:
        user = self.data["users"].get(str(user_id))
        if not user:
            return None

        playlists = self.data.setdefault("playlists", {})
        new_id = max(map(int, playlists.keys()), default=0) + 1

        playlists[str(new_id)] = {
            "id": new_id,
            "name": name,
            "listMusique": [],
            "owner": user_id
        }

        user.setdefault("like", {}).setdefault("playlist", []).append(new_id)

        self._save()
        return new_id


    def update_playlist_tracks(self, playlist_id: int, track_ids, action: str):
        playlist = self.get_playlist(playlist_id)
        if not playlist:
            return None

        for tid in track_ids:
            if action == "add" and tid not in playlist["listMusique"]:
                playlist["listMusique"].insert(0, tid)
            elif action == "del" and tid in playlist["listMusique"]:
                playlist["listMusique"].remove(tid)

        # playlist vide → signaler suppression
        if not playlist["listMusique"]:
            self._save()
            return "EMPTY"

        self._save()
        return playlist


    def update_user_like(self, user_id: str, obj_type: str, obj_id: int, like: bool):
        user = self.data["users"].get(str(user_id))
        if not user:
            return
        print(user_id,obj_type,obj_id,like)
        likes = user.setdefault("like", {}).setdefault(obj_type, [])

        if like:
            if obj_id not in likes:
                likes.append(obj_id)
        else:
            if obj_id in likes:
                likes.remove(obj_id)

        self._save()

    def get_user_all(self):
        return list(self.data.get("users", {}).values())

    def get_user_by_username(self, username: str):
        for user in self.data.get("users", {}).values():
            if user.get("username") == username:
                return user
        return None
    def get_user_by_id(self, user_id: str):
        return self.data.get("users", {}).get(str(user_id))
    def delete_playlist(self, user_id: str, playlist_id: int):
        pid = str(playlist_id)

        playlist = self.data.get("playlists", {}).get(pid)
        if not playlist:
            return

        user = self.data.get("users", {}).get(str(user_id))
        if not user:
            return

        # retirer la playlist de l'utilisateur
        playlists = user.get("like", {}).get("playlist", [])
        if playlist_id in playlists:
            playlists.remove(playlist_id)

        # supprimer la playlist
        del self.data["playlists"][pid]

        self._save()


 # 1. créer un user
    def create_user(self, username: str, password: str, email: str, role: str = "user") -> str:
        users = self.data.setdefault("users", {})

        for u in users.values():
            if u.get("username") == username:
                logger.error(f"create_user: username déjà existant ({username})")
                raise ValueError("USERNAME_ALREADY_EXISTS")

        new_id = str(max(map(int, users.keys()), default=0) + 1)

        users[new_id] = {
            "id": new_id,
            "username": username,
            "password": password,
            "email": email,
            "role": role,
            "created_at": date.today().isoformat(),
            "like": {
                "track": [],
                "album": [],
                "artist": [],
                "playlist": []
            }
        }

        self._save()
        return new_id

    def set_user_role(self, user_id: str, role: str):
        if role not in ("user", "admin"):
            raise ValueError("INVALID_ROLE")

        user = self.data["users"].get(str(user_id))
        if not user:
            raise KeyError("USER_NOT_FOUND")

        if user.get("role") != role:
            user["role"] = role
            self._save()

        return role


    def set_username(self, user_id: str, new_username: str):
        users = self.data.get("users", {})
        user = users.get(str(user_id))
        if not user:
            logger.error(f"set_username: user introuvable ({user_id})")
            raise KeyError("USER_NOT_FOUND")

        for u in users.values():
            if u.get("username") == new_username and u["id"] != user_id:
                logger.error(f"set_username: username déjà pris ({new_username})")
                raise ValueError("USERNAME_ALREADY_EXISTS")

        if user.get("username") != new_username:
            user["username"] = new_username
            self._save()

        return new_username

    def set_user_password(self, user_id: str, new_password: str):
        user = self.data.get("users", {}).get(str(user_id))
        if not user:
            logger.error(f"set_user_password: user introuvable ({user_id})")
            raise KeyError("USER_NOT_FOUND")

        if user.get("password") != new_password:
            user["password"] = new_password
            self._save()

        return True

    def create_registration_token(self) -> str:
        token = str(uuid.uuid4())
        tokens = self.data.setdefault("registration_tokens", [])
        tokens.append(token)
        self._save()
        return token

    def verify_registration_token(self, token: str) -> bool:
        tokens = self.data.get("registration_tokens", [])
        return token in tokens

    def consume_registration_token(self, token: str):
        tokens = self.data.get("registration_tokens", [])
        if token in tokens:
            tokens.remove(token)
            self._save()

    def get_libraries(self):
        return self.data.get("libraries", [])

    def update_library(self, index: int, library_data: dict):
        libraries = self.data.get("libraries", [])
        if 0 <= index < len(libraries):
            libraries[index].update(library_data)
            self._save()
            return libraries[index]
        raise IndexError("Library index out of range")

    def add_library(self, library_data: dict):
        libraries = self.data.setdefault("libraries", [])
        new_id = max((lib.get("id", 0) for lib in libraries), default=0) + 1
        library_data["id"] = new_id
        libraries.append(library_data)
        self._save()
        return library_data

    def add_track_to_history(self, user_id: str, track_id: int):
        user = self.data["users"].get(str(user_id))
        if not user:
            return

        history = user.setdefault("history", [])
        
        # Remove if already exists to move it to the front (optional but often preferred)
        if track_id in history:
            history.remove(track_id)
        
        # Add to the beginning (index 0)
        history.insert(0, track_id)
        
        # Limit to 200
        if len(history) > 200:
            user["history"] = history[:200]
            
        self._save()

    def update_user_top_genres(self):
        users = self.data.get("users", {})
        tracks_db = self.data.get("tracks", {})
        albums_db = self.data.get("albums", {})
        genres_db = self.data.get("genres", {})

        for user_id, user in users.items():
            history = user.get("history", [])
            if not history:
                user["top_genres"] = []
                continue

            genre_counts = {}
            for track_id in history:
                track = tracks_db.get(str(track_id))
                if not track:
                    continue
                
                album_id = track.get("albumId")
                album = albums_db.get(str(album_id))
                if not album:
                    continue
                
                genre_ids = album.get("genreIds", [])
                for g_id in genre_ids:
                    genre_counts[str(g_id)] = genre_counts.get(str(g_id), 0) + 1
            
            # Sort by count and get top 10
            sorted_genres = sorted(genre_counts.items(), key=lambda x: x[1], reverse=True)[:10]
            
            top_genres_data = []
            for g_id, count in sorted_genres:
                genre = genres_db.get(g_id)
                if genre:
                    top_genres_data.append({
                        "id": int(g_id),
                        "name": genre.get("name"),
                        "count": count
                    })
            
            user["top_genres"] = top_genres_data
        
        self._save()