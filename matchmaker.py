import logging
import unicodedata
from dataclasses import dataclass
from typing import List, Optional
from sqlalchemy.orm import Session
from sqlalchemy import func

from database.models import Song, Artist, Genre, WordFrequency, Dictionary

logger = logging.getLogger(__name__)

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
    def __init__(self, db_session: Session):
        self.db = db_session

    def get_acapella_bridges(self, target_word: str, genre: Optional[str] = None, year: Optional[str] = None, limit_per_genre: int = 3) -> List[BridgeResult]:
        
        target_word = target_word.lower().strip()
        target_word = ''.join(c for c in unicodedata.normalize('NFD', target_word) if unicodedata.category(c) != 'Mn')

        word_objs = self.db.query(Dictionary).filter(Dictionary.word_text.ilike(f"{target_word}%")).all()
        if not word_objs:
            return []

        word_ids = [w.id for w in word_objs]

        query = (
            self.db.query(
                Song.title,
                Artist.name.label("artist_name"),
                Genre.name.label("genre_name"),
                func.sum(WordFrequency.occurrence_count).label("occurrences")
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

        results = query.all()
        
        bridges = [
            BridgeResult(
                word=target_word,
                song_title=row.title,
                artist_name=row.artist_name,
                genre=row.genre_name,
                occurrences=row.occurrences
            )
            for row in results
        ]
        bridges.sort(key=lambda x: x.occurrences, reverse=True)
        return bridges[:limit_per_genre * 5]


    def get_harmonic_twins(self, song_title: str, top_dna_words: int = 10) -> List[TwinResult]:
        source_song = self.db.query(Song).filter(Song.title.ilike(f"%{song_title}%")).first()
        if not source_song:
            return []

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
            .filter(Song.id != source_song.id)
            .group_by(Song.id, Song.title, Artist.id, Artist.name, Genre.id, Genre.name)
            .having(func.count(WordFrequency.word_id) >= 3)
        )

        matches = match_query.all()

        twins = [
            TwinResult(
                song_title=row.title,
                artist_name=row.artist_name,
                genre=row.genre_name,
                shared_words=row.shared_words,
                score=row.score
            )
            for row in matches
        ]

        twins.sort(key=lambda x: (x.shared_words, x.score), reverse=True)
        return twins[:5]
