# /repositories/json_repo.py
import json
from repositories.base import BaseRepository

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

    def get_artist(self, artist_id: int):
        return self.data["artists"].get(str(artist_id))

    def get_playlist(self, playlist_id: int):
        return self.data["playlists"].get(str(playlist_id))

    def all_albums(self):
        return self.data["albums"].values()

    def all_artists(self):
        return self.data["artists"].values()

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
