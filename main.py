from fastapi import FastAPI, Depends, HTTPException, Body
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Literal
import os
from auth import verify_token, create_token
from repositories.json_repo import JsonRepository
from repositories.bucket_repo import S3ContactRepository
from dotenv import load_dotenv
load_dotenv() 
BUCKET_HOST=f'{os.environ["AWS_ENDPOINT_URL"]}/{os.environ["BUCKET_NAME"]}/'

repo = JsonRepository()
bucketS3 = S3ContactRepository()

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_headers=["*"],
    allow_methods=["*"],
)

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

    if user["password"] != payload.password:
        raise HTTPException(status_code=401, detail="Invalid credentials")

    token = create_token(user)

    return {
        "token": token,
        "type": "bearer",
        "user": {
            "id": user["id"],
            "username": user["username"],
            "email": user.get("email")
        }
    }

# ======================
# HELPERS
# ======================
def get_fresh_user(user):
    return repo.get_user_by_id(user["id"])

def build_album(album_id: int, user=None):
    album = repo.get_album(album_id)
    if not album:
        return None

    u = repo.get_user_by_id(user["id"]) if user else None
    liked_tracks = set(u.get("like", {}).get("track", [])) if u else set()
    liked_albums = set(u.get("like", {}).get("album", [])) if u else set()

    # Résolution des artistes de l’album
    album_artist_ids = album.get("artistIds", [])
    album_artists = []
    for aid in album_artist_ids:
        artist = repo.get_artist(aid)
        if artist:
            album_artists.append({
                "id": artist["id"],
                "name": artist["name"]
            })

    tracks = []

    for tid in album.get("listMusique", []):
        t = repo.get_track(tid)
        if not t:
            continue

        artist = repo.get_artist(t.get("artistId"))
        artist_name = artist["name"] if artist else None

        tracks.append({
            "id": t["id"],
            "title": t["title"],
            "duration": t.get("duration"),
            "albumName": album.get("name"),
            "artistName": artist_name,
            "artistId": t.get("artistId"),
            "albumId": t.get("albumId"),
            "albumTrack": t.get("albumTrack"),
            "like": t["id"] in liked_tracks,
            "coverSmall": BUCKET_HOST + album.get("coverSmall"),
        })

    tracks.sort(key=lambda x: x["albumTrack"])

    return {
        "id": album["id"],
        "name": album.get("name"),
        "artist": album_artists,
        "artistName": artist_name,
        "artistId":t.get("artistId"),
        "cover": BUCKET_HOST + album.get("cover"),
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
            "coverSmall": BUCKET_HOST+album.get("cover") if album else None,
        })

    return {
        "id": playlist["id"],
        "name": playlist["name"],
        "listMusique": tracks
    }


# ======================
# ROUTES 
# ======================


# Track

@app.post("/trackByListID")
def track_by_list_id(ids: List[int] = Body(...), user=Depends(verify_token)):
    user_likes = set(user.get("like", {}).get("track", []))
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
            "coverSmall": BUCKET_HOST+album.get("cover") if album else None,
            "path": bucketS3.get_temporary_link(t.get("path")) if t.get("path") else None
        })
    return out

@app.get("/trackLike")
def track_like(user=Depends(verify_token)):
    out = []

    current_user = repo.get_user_by_id(user["id"])
    liked_ids = set(current_user.get("like", {}).get("track", []))

    for tid in liked_ids:
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
            "coverSmall": BUCKET_HOST+album.get("cover") if album else None,
        })

    return {
        "id": 0,
        "name": "trackLike",
        "listMusique": out
    }


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
    u = get_fresh_user(user)
    liked_albums = set(u.get("like", {}).get("album", []))

    result = []
    for a in repo.all_albums():
        # artiste principal
        main_artist = None
        if a.get("artistId"):
            artist = repo.get_artist(a["artistId"][0])
            if artist:
                main_artist = artist

        result.append({
            "id": a["id"],
            "name": a.get("name"),
            "like": a["id"] in liked_albums,
            "artistName": main_artist["name"] if main_artist else None,
            "artistId": main_artist["id"] if main_artist else None,
            "cover": BUCKET_HOST + a.get("cover")
        })
    return result


@app.get("/albumLike")
def album_like(user=Depends(verify_token)):
    u = get_fresh_user(user)
    liked_albums = set(u.get("like", {}).get("album", []))

    result = []
    for a in repo.all_albums():
        if a["id"] in liked_albums:
            main_artist = None
            if a.get("artistId"):
                artist = repo.get_artist(a["artistId"][0])
                if artist:
                    main_artist = artist

            result.append({
                "id": a["id"],
                "name": a.get("name"),
                "like": True,
                "artistName": main_artist["name"] if main_artist else None,
                "artistId": main_artist["id"] if main_artist else None,
                "cover": BUCKET_HOST + a.get("cover")
            })
    return result


class ArtistID(BaseModel):
    artist_id: int

@app.post("/albumByArtistID")
def album_by_artist(payload: ArtistID, user=Depends(verify_token)):
    artist = repo.get_artist(payload.artist_id)
    if not artist:
        raise HTTPException(404)

    u = get_fresh_user(user)
    liked_albums = set(u.get("like", {}).get("album", []))

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
                "cover": BUCKET_HOST+album.get("cover")
            })

    return {
        "id": artist["id"],
        "name": artist.get("name"),
        "image": BUCKET_HOST+artist.get("image"),
        "listAlbums": albums
    }


class AlbumListRequest(BaseModel):
    album_ids: List[int]

@app.post("/albumByListId")
def album_by_list_id(req: AlbumListRequest, user=Depends(verify_token)):
    u = get_fresh_user(user)
    liked_albums = set(u.get("like", {}).get("album", []))
    return {
        "albums": [
            {
                "id": a["id"],
                "name": a.get("name"),
                "like": a["id"] in liked_albums,
                "artistName": a.get("artistName"),
                "artistId": a.get("artistId"),
                "cover": BUCKET_HOST+a.get("cover")
            }
            for aid in req.album_ids
            if (a := repo.get_album(aid))
        ]
    }



# Artist

@app.get("/artistLike")
def artist_like(user=Depends(verify_token)):
    u = get_fresh_user(user)
    liked_ids = set(u.get("like", {}).get("artist", []))
    return [
        {
            "id": a["id"],
            "name": a["name"],
            "like": True,
            "image": BUCKET_HOST+a.get("image")
        }
        for aid in liked_ids
        if (a := repo.get_artist(aid))
    ]

@app.post("/allArtist")
def all_artist(user=Depends(verify_token)):
    u = get_fresh_user(user)
    liked_ids = set(u.get("like", {}).get("artist", []))
    return [
        {
            "id": a["id"],
            "name": a["name"],
            "like": a["id"] in liked_ids,
            "image": BUCKET_HOST+a.get("image")
        }
        for a in repo.all_artists()
    ]

@app.post("/artistByListId")
def artist_by_list_id(ids: List[int], user=Depends(verify_token)):
    u = get_fresh_user(user)
    liked_ids = set(u.get("like", {}).get("artist", []))
    return [
        {
            "id": a["id"],
            "name": a["name"],
            "like": a["id"] in liked_ids,
            "image": BUCKET_HOST+a.get("image")
        }
        for aid in ids
        if (a := repo.get_artist(aid))
    ]



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
    u = repo.get_user_by_id(user["id"])
    playlist_ids = set(u.get("like", {}).get("playlist", []))

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
    repo.update_user_like(
        user_id=user["id"],
        obj_type=payload.type,
        obj_id=payload.id,
        like=payload.like
    )
    return payload
