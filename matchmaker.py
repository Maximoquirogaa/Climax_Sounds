import logging
import unicodedata
from dataclasses import dataclass
from typing import List, Optional
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

    def get_acapella_bridges(self, target_word: str, genre: Optional[str] = None, year: Optional[str] = None, limit_per_genre: int = 3) -> List[BridgeResult]:
        
        # 1. Aspiradora de Tildes
        target_word = target_word.lower().strip()
        target_word = ''.join(c for c in unicodedata.normalize('NFD', target_word) if unicodedata.category(c) != 'Mn')

        word_objs = self.db.query(Dictionary).filter(Dictionary.word_text.ilike(f"{target_word}%")).all()
        
        if not word_objs:
            logger.warning(f"La raíz léxica '{target_word}' no existe en la BD.")
            return []

        word_ids = [w.id for w in word_objs]

        # 🚨 LA SOLUCIÓN DEFINITIVA: Creamos la columna sumada como una variable 🚨
        occ_count_col = func.sum(WordFrequency.occurrence_count).label("occurrence_count")

        # 3. Query Base
        query = (
            self.db.query(
                Song.title,
                Artist.name.label("artist_name"),
                Genre.name.label("genre_name"),
                occ_count_col # La inyectamos acá
            )
            .join(WordFrequency, Song.id == WordFrequency.song_id)
            .join(Artist, Song.artist_id == Artist.id)
            .join(Genre, Artist.genre_id == Genre.id)
            .filter(WordFrequency.word_id.in_(word_ids))
            .group_by(Song.id, Song.title, Artist.id, Artist.name, Genre.id, Genre.name)
        )

        if genre and genre.strip():
            query = query.filter(Genre.name.ilike(f"%{genre.strip()}%"))

        if year and year.strip():
            if year == 'retro':
                query = query.filter(Song.release_year.isnot(None), Song.release_year < 2010)
            else:
                query = query.filter(Song.release_year == int(year))

        # 4. El ordenamiento: Usamos LA MISMA variable .desc() para que SQLAlchemy no llore
        query = query.order_by(occ_count_col.desc()).limit(limit_per_genre * 5)

        results = query.all()
        
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
        source_song = self.db.query(Song).filter(Song.title.ilike(f"%{song_title}%")).first()
        if not source_song:
            logger.error(f"Canción '{song_title}' no encontrada.")
            return []

        # Extraer ADN (Acá también actualizamos el desc() a la versión moderna)
        dna_frequencies = (
            self.db.query(WordFrequency.word_id)
            .filter(WordFrequency.song_id == source_song.id)
            .order_by(WordFrequency.occurrence_count.desc())
            .limit(top_dna_words)
            .all()
        )
        dna_word_ids = [freq.word_id for freq in dna_frequencies]

        if not dna_word_ids:
            return []

        # 🚨 LA MAGIA PARA LOS GEMELOS: Variables para conteo y suma 🚨
        shared_words_col = func.count(WordFrequency.word_id).label("shared_words")
        score_col = func.sum(WordFrequency.occurrence_count).label("score")

        match_query = (
            self.db.query(
                Song.title,
                Artist.name.label("artist_name"),
                Genre.name.label("genre_name"),
                shared_words_col,
                score_col
            )
            .join(Artist, Song.artist_id == Artist.id)
            .join(Genre, Artist.genre_id == Genre.id)
            .join(WordFrequency, Song.id == WordFrequency.song_id)
            .filter(WordFrequency.word_id.in_(dna_word_ids))
            .filter(Song.id != source_song.id)
            .group_by(Song.id, Song.title, Artist.id, Artist.name, Genre.id, Genre.name)
            .having(shared_words_col >= 3)
            # Ordenamos usando las variables limpias
            .order_by(shared_words_col.desc(), score_col.desc())
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