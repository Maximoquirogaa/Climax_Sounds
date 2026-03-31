import os
import logging
from typing import Optional, Dict
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials

logger = logging.getLogger(__name__)

class SpotifyAudioFeaturesFetcher:
    """
    Miner de Spotify para extraer características físicas del audio.
    Convierte notación musical estándar al Sistema Camelot para DJs.
    """
    
    # Diccionarios de traducción (Mapeo de Pitch Class de Spotify a Camelot Wheel)
    # Spotify Key: 0=C, 1=C#, 2=D, 3=D#, 4=E, 5=F, 6=F#, 7=G, 8=G#, 9=A, 10=A#, 11=B
    # Spotify Mode: 1=Major (B), 0=Minor (A)
    
    CAMELOT_MAJOR = {
        0: "8B",  1: "3B",  2: "10B", 3: "5B", 
        4: "12B", 5: "7B",  6: "2B",  7: "9B", 
        8: "4B",  9: "11B", 10: "6B", 11: "1B"
    }
    
    CAMELOT_MINOR = {
        0: "5A",  1: "12A", 2: "7A",  3: "2A", 
        4: "9A",  5: "4A",  6: "11A", 7: "6A", 
        8: "1A",  9: "8A",  10: "3A", 11: "10A"
    }

    def __init__(self, client_id: str, client_secret: str):
        auth_manager = SpotifyClientCredentials(
            client_id=client_id, 
            client_secret=client_secret
        )
        self.sp = spotipy.Spotify(auth_manager=auth_manager)
        
    def _translate_to_camelot(self, key: int, mode: int) -> str:
        if key < 0 or key > 11:
            return "Unknown"
        
        if mode == 1:
            return self.CAMELOT_MAJOR.get(key, "Unknown")
        else:
            return self.CAMELOT_MINOR.get(key, "Unknown")

    def get_song_dj_features(self, artist_name: str, song_title: str) -> Optional[Dict]:
        
        clean_title = song_title.split("(")[0].strip()
        query = f"track:{clean_title} artist:{artist_name}"
        
        try:
            result = self.sp.search(q=query, type='track', limit=1)
            tracks = result.get('tracks', {}).get('items', [])
            
            if not tracks:
                logger.warning(f"Spotify no encontró: '{song_title}' de {artist_name}")
                return None
                
            track_id = tracks[0]['id']
            
            features = self.sp.audio_features([track_id])[0]
            
            if not features:
                return None

            return {
                "bpm": round(features['tempo'], 2),
                "camelot_key": self._translate_to_camelot(features['key'], features['mode'])
            }
            
        except Exception as e:
            logger.error(f"Error de API con Spotify para '{song_title}': {e}")
            return None
