import logging
from typing import List, Tuple, Dict
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError

from database.models import Genre, Artist, Song, Dictionary, WordFrequency
from core.analyzer import LyricAnalyzer

logger = logging.getLogger(__name__)

class IngestionService:
    """
    Servicio de capa empresarial para gestionar la inserción de canciones
    y sus estadísticas léxicas en la base de datos de forma transaccional.
    """

    def __init__(self, db_session: Session, analyzer: LyricAnalyzer):
        self.db = db_session
        self.analyzer = analyzer

    def get_or_create_genre(self, genre_name: str) -> Genre:
        genre_name = genre_name.strip().title()
        genre = self.db.query(Genre).filter(Genre.name == genre_name).first()
        
        if not genre:
            genre = Genre(name=genre_name)
            self.db.add(genre)
            self.db.flush() 
            
        return genre

    def get_or_create_artist(self, artist_name: str, genre_id: int) -> Artist:
        artist_name = artist_name.strip().title()
        artist = self.db.query(Artist).filter(Artist.name == artist_name).first()
        
        if not artist:
            artist = Artist(name=artist_name, genre_id=genre_id)
            self.db.add(artist)
            self.db.flush()
            
        return artist

    def _get_or_create_words_bulk(self, words: List[str]) -> Dict[str, int]:
        existing_words = self.db.query(Dictionary).filter(Dictionary.word_text.in_(words)).all()
        word_id_map = {w.word_text: w.id for w in existing_words}

        new_words_texts = set(words) - set(word_id_map.keys())
        
        if new_words_texts:
            new_word_objects = [Dictionary(word_text=text) for text in new_words_texts]
            
            self.db.add_all(new_word_objects)
            self.db.flush() 
            
            for new_word in new_word_objects:
                word_id_map[new_word.word_text] = new_word.id
                
        return word_id_map

    def process_and_save_song(
        self, artist_name: str, genre_name: str, song_title: str, release_year: int, raw_lyrics: str
    ) -> bool:
        try:
            word_ranking = self.analyzer.process(raw_lyrics, limit=100)
            
            if not word_ranking:
                logger.warning(f"La canción '{song_title}' no generó palabras válidas para analizar.")
                return False

            genre = self.get_or_create_genre(genre_name)
            artist = self.get_or_create_artist(artist_name, genre.id)
            existing_song = self.db.query(Song).filter(
                Song.title == song_title, Song.artist_id == artist.id
            ).first()
            
            if existing_song:
                logger.info(f"La canción '{song_title}' ya existe en la base de datos. Omitiendo.")
                return False

            new_song = Song(title=song_title, artist_id=artist.id, release_year=release_year)
            self.db.add(new_song)
            self.db.flush()
            just_words = [item[0] for item in word_ranking]
            word_to_id_map = self._get_or_create_words_bulk(just_words)

            frequencies_to_insert = []
            for word_text, count in word_ranking:
                word_id = word_to_id_map[word_text]
                freq = WordFrequency(
                    song_id=new_song.id,
                    word_id=word_id,
                    occurrence_count=count
                )
                frequencies_to_insert.append(freq)
            self.db.bulk_save_objects(frequencies_to_insert)

            self.db.commit()
            logger.info(f"Éxito: '{song_title}' guardada con {len(frequencies_to_insert)} palabras únicas.")
            return True

        except SQLAlchemyError as db_error:
            self.db.rollback()
            logger.error(f"Error de base de datos procesando '{song_title}'. Transacción revertida: {db_error}")
            return False
        except Exception as e:
            self.db.rollback()
            logger.critical(f"Error inesperado procesando '{song_title}': {e}")
            return False
