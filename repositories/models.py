from sqlalchemy import Column, Integer, String, ForeignKey, Table, JSON, DateTime, Boolean
from sqlalchemy.orm import relationship, declarative_base
from datetime import datetime

Base = declarative_base()

# Junction tables for relationships
album_genres = Table(
    'album_genres', Base.metadata,
    Column('album_id', Integer, ForeignKey('albums.id')),
    Column('genre_id', Integer, ForeignKey('genres.id'))
)

album_artists = Table(
    'album_artists', Base.metadata,
    Column('album_id', Integer, ForeignKey('albums.id')),
    Column('artist_id', Integer, ForeignKey('artists.id'))
)

user_like_tracks = Table(
    'user_like_tracks', Base.metadata,
    Column('user_id', Integer, ForeignKey('users.id')),
    Column('track_id', Integer, ForeignKey('tracks.id'))
)

user_like_albums = Table(
    'user_like_albums', Base.metadata,
    Column('user_id', Integer, ForeignKey('users.id')),
    Column('album_id', Integer, ForeignKey('albums.id'))
)

user_like_artists = Table(
    'user_like_artists', Base.metadata,
    Column('user_id', Integer, ForeignKey('users.id')),
    Column('artist_id', Integer, ForeignKey('artists.id'))
)

user_like_playlists = Table(
    'user_like_playlists', Base.metadata,
    Column('user_id', Integer, ForeignKey('users.id')),
    Column('playlist_id', Integer, ForeignKey('playlists.id'))
)

class User(Base):
    __tablename__ = 'users'
    id = Column(Integer, primary_key=True)
    username = Column(String, unique=True, index=True)
    password = Column(String)
    email = Column(String)
    role = Column(String, default="user")
    created_at = Column(DateTime, default=datetime.utcnow)
    top_genres = Column(JSON, default=list)
    
    # Relationships
    liked_tracks = relationship('Track', secondary=user_like_tracks)
    liked_albums = relationship('Album', secondary=user_like_albums)
    liked_artists = relationship('Artist', secondary=user_like_artists)
    liked_playlists = relationship('Playlist', secondary=user_like_playlists, back_populates='liked_by_users')
    playlists = relationship('Playlist', back_populates='owner')
    history = relationship('UserHistory', back_populates='user', order_by="desc(UserHistory.timestamp)", cascade="all, delete-orphan")

class UserHistory(Base):
    __tablename__ = 'user_history'
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey('users.id'))
    track_id = Column(Integer, ForeignKey('tracks.id'))
    timestamp = Column(DateTime, default=datetime.utcnow)
    
    user = relationship('User', back_populates='history')
    track = relationship('Track')

class Artist(Base):
    __tablename__ = 'artists'
    id = Column(Integer, primary_key=True)
    name = Column(String, index=True)
    image = Column(String)
    bucket = Column(String)
    library_id = Column(Integer, ForeignKey('libraries.id'))

class Album(Base):
    __tablename__ = 'albums'
    id = Column(Integer, primary_key=True)
    name = Column(String, index=True)
    cover = Column(String)
    coverSmall = Column(String)
    coverBucket = Column(String)
    date = Column(String)
    library_id = Column(Integer, ForeignKey('libraries.id'))
    
    artists = relationship('Artist', secondary=album_artists)
    genres = relationship('Genre', secondary=album_genres)
    tracks = relationship('Track', back_populates='album', cascade="all, delete-orphan")

class Track(Base):
    __tablename__ = 'tracks'
    id = Column(Integer, primary_key=True)
    title = Column(String, index=True)
    duration = Column(String)
    album_id = Column(Integer, ForeignKey('albums.id'))
    artist_id = Column(Integer, ForeignKey('artists.id'))
    album_track = Column(Integer)
    path = Column(String)
    bucket = Column(String)
    library_id = Column(Integer, ForeignKey('libraries.id'))
    
    album = relationship('Album', back_populates='tracks')
    artist = relationship('Artist')

class Genre(Base):
    __tablename__ = 'genres'
    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True, index=True)

class Playlist(Base):
    __tablename__ = 'playlists'
    id = Column(Integer, primary_key=True)
    name = Column(String)
    owner_id = Column(Integer, ForeignKey('users.id'))
    
    owner = relationship('User', back_populates='playlists')
    liked_by_users = relationship('User', secondary=user_like_playlists, back_populates='liked_playlists')
    tracks = relationship('Track', secondary='playlist_tracks')

class PlaylistTrack(Base):
    __tablename__ = 'playlist_tracks'
    playlist_id = Column(Integer, ForeignKey('playlists.id'), primary_key=True)
    track_id = Column(Integer, ForeignKey('tracks.id'), primary_key=True)
    position = Column(Integer) # To maintain order

class Library(Base):
    __tablename__ = 'libraries'
    id = Column(Integer, primary_key=True)
    name = Column(String)
    url = Column(String)
    identifiers = Column(JSON)

class RegistrationToken(Base):
    __tablename__ = 'registration_tokens'
    token = Column(String, primary_key=True)
    created_at = Column(DateTime, default=datetime.utcnow)
