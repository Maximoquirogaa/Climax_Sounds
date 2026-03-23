import logging
from dataclasses import dataclass
from typing import List, Dict, Optional # 👈 1. Agregamos Optional
from sqlalchemy.orm import Session
from sqlalchemy import func, desc

from database.models import Song, Artist, Genre, WordFrequency, Dictionary

logger = logging.getLogger(__name__)

# --- DTOs (Data Transfer Objects) Estándar AAA ---
@dataclass
class BridgeResult:
    word: str
    song_title: str
    artist_name: str
    genre: str
    occurrences: int

@dataclass
class TwinResult:
    song_title: str
    artist_name: str
    genre: str
    shared_words: int
    score: int

class MatchmakerService:
    """
    Motor algorítmico para Mash-upgradde.
    Encuentra intersecciones léxicas entre obras musicales.
    """
    def __init__(self, db_session: Session):
        self.db = db_session

    # 👇 2. Modificamos la firma para recibir genre y year
    def get_acapella_bridges(self, target_word: str, genre: Optional[str] = None, year: Optional[str] = None, limit_per_genre: int = 3) -> List[BridgeResult]:
        """
        ALGORITMO 1: Búsqueda de anclas directas con filtros dinámicos.
        """
        target_word = target_word.lower().strip()
        
        # 1. Buscamos el ID de la palabra
        word_obj = self.db.query(Dictionary).filter(Dictionary.word_text == target_word).first()
        if not word_obj:
            logger.warning(f"La palabra '{target_word}' no existe en la base de datos.")
            return []

        # 2. Query Base con SQLAlchemy
        query = (
            self.db.query(
                Song.title,
                Artist.name.label("artist_name"),
                Genre.name.label("genre_name"),
                WordFrequency.occurrence_count
            )
            .join(WordFrequency, Song.id == WordFrequency.song_id)
            .join(Artist, Song.artist_id == Artist.id)
            .join(Genre, Artist.genre_id == Genre.id)
            .filter(WordFrequency.word_id == word_obj.id)
        )

        # 🚨 3. APLICAMOS LOS FILTROS DINÁMICOS (La Magia) 🚨
        if genre:
            # Filtramos comparando con la tabla Genre usando ilike (ignora mayúsculas/minúsculas)
            query = query.filter(Genre.name.ilike(f"%{genre}%"))

        if year:
            if year == 'retro':
                query = query.filter(Song.release_year < 2010)
            else:
                # Convertimos el string a entero para compararlo en la BD
                query = query.filter(Song.release_year == int(year))

        # 4. Terminamos la query agregando el ordenamiento y el límite
        query = query.order_by(desc(WordFrequency.occurrence_count)).limit(limit_per_genre * 5)

        results = query.all()
        
        # 5. Mapeo al DTO
        bridges = [
            BridgeResult(
                word=target_word,
                song_title=row.title,
                artist_name=row.artist_name,
                genre=row.genre_name,
                occurrences=row.occurrence_count
            )
            for row in results
        ]
        return bridges

    def get_harmonic_twins(self, song_title: str, top_dna_words: int = 10) -> List[TwinResult]:
        """
        ALGORITMO 2: Matchmaking Compuesto.
        Compara el 'ADN' de una canción con toda la base de datos.
        """
        # 1. Buscar la canción origen
        source_song = self.db.query(Song).filter(Song.title.ilike(f"%{song_title}%")).first()
        if not source_song:
            logger.error(f"Canción '{song_title}' no encontrada.")
            return []

        # 2. Extraer el "ADN Léxico" (Sus palabras más repetidas)
        dna_frequencies = (
            self.db.query(WordFrequency.word_id)
            .filter(WordFrequency.song_id == source_song.id)
            .order_by(desc(WordFrequency.occurrence_count))
            .limit(top_dna_words)
            .all()
        )
        dna_word_ids = [freq.word_id for freq in dna_frequencies]

        if not dna_word_ids:
            return []

        # 3. La Magia SQL: Buscar intersecciones
        # Buscamos otras canciones que tengan estos mismos word_ids, agrupamos por canción,
        # y contamos cuántas coincidencias (shared_words) tienen.
        match_query = (
            self.db.query(
                Song.title,
                Artist.name.label("artist_name"),
                Genre.name.label("genre_name"),
                func.count(WordFrequency.word_id).label("shared_words"),
                func.sum(WordFrequency.occurrence_count).label("score")
            )
            .join(Artist, Song.artist_id == Artist.id)
            .join(Genre, Artist.genre_id == Genre.id)
            .join(WordFrequency, Song.id == WordFrequency.song_id)
            .filter(WordFrequency.word_id.in_(dna_word_ids))
            .filter(Song.id != source_song.id) # Excluimos la canción original
            .group_by(Song.id, Artist.id, Genre.id)
            .having(func.count(WordFrequency.word_id) >= 3) # Condición: Deben compartir al menos 3 palabras fuertes
            .order_by(desc("shared_words"), desc("score"))
            .limit(5)
        )

        matches = match_query.all()

        return [
            TwinResult(
                song_title=row.title,
                artist_name=row.artist_name,
                genre=row.genre_name,
                shared_words=row.shared_words,
                score=row.score
            )
            for row in matches
        ]