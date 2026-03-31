from sqlalchemy import Column, Integer, String, ForeignKey, SmallInteger, Table, UniqueConstraint, Float
from sqlalchemy.orm import relationship, declarative_base

# # Clase base de la que heredarán todos nuestros modelos
Base = declarative_base()

class Genre(Base):
    __tablename__ = "genres"
    
    id = Column(Integer, primary_key=True)
    name = Column(String(50), unique=True, nullable=False)
    
    # # Relación: un género tiene muchos artistas
    artists = relationship("Artist", back_populates="genre")

class Artist(Base):
    __tablename__ = "artists"
    
    id = Column(Integer, primary_key=True)
    name = Column(String(100), nullable=False)
    genre_id = Column(Integer, ForeignKey("genres.id", ondelete="SET NULL"))
    
    # # Relaciones bidireccionales para navegar los datos fácilmente
    genre = relationship("Genre", back_populates="artists")
    songs = relationship("Song", back_populates="artist", cascade="all, delete-orphan")

class Song(Base):
    __tablename__ = "songs" 
    
    id = Column(Integer, primary_key=True)
    artist_id = Column(Integer, ForeignKey("artists.id", ondelete="CASCADE"), nullable=False)
    title = Column(String(150), nullable=False)
    release_year = Column(SmallInteger)
    
 
    bpm = Column(Float, nullable=True) 
    camelot_key = Column(String(5), nullable=True) 
    

    artist = relationship("Artist", back_populates="songs")
    frequencies = relationship("WordFrequency", back_populates="song", cascade="all, delete-orphan")

class Dictionary(Base):
    __tablename__ = "dictionary"
    
    id = Column(Integer, primary_key=True)
    word_text = Column(String(100), unique=True, nullable=False)
    
    word_frequencies = relationship("WordFrequency", back_populates="word")

class WordFrequency(Base):
    __tablename__ = "word_frequencies"
    
    song_id = Column(Integer, ForeignKey("songs.id", ondelete="CASCADE"), primary_key=True)
    word_id = Column(Integer, ForeignKey("dictionary.id", ondelete="CASCADE"), primary_key=True)
    occurrence_count = Column(Integer, nullable=False, default=1)
    
    song = relationship("Song", back_populates="frequencies")
    word = relationship("Dictionary", back_populates="word_frequencies")

from sqlalchemy import Float

