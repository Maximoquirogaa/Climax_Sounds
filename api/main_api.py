import os
from fastapi import FastAPI, Depends, HTTPException
from fastapi.middleware.cors import CORSMiddleware # 👈 1. Importamos esto
from sqlalchemy.orm import Session
from dotenv import load_dotenv

from database.session import DatabaseManager
from services.matchmaker import MatchmakerService

load_dotenv()
db_manager = DatabaseManager(os.getenv("DATABASE_URL"))

app = FastAPI(
    title="Mash-upgradde API 🎧",
    description="Motor relacional de transiciones léxicas y armónicas para DJs",
    version="1.0.0"
)

# 👇 2. AGREGAMOS EL PERMISO CORS (Copia y pega este bloque)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], # En producción se pone la URL de tu frontend, por ahora "*" permite todo
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
# ---------------------------------------------------------

def get_db():
    with db_manager.get_session() as session:
        yield session

# --- ENDPOINTS (Las puertas de acceso a tu app) ---

@app.get("/")
def health_check():
    """Verifica que el servidor esté vivo."""
    return {"status": "online", "message": "El motor de Mash-upgradde está escuchando..."}

@app.get("/api/v1/mashup/bridge")
def get_acapella_bridge(word: str, db: Session = Depends(get_db)):
    """
    Busca puentes a capela (Acapella Bridges) para una palabra específica.
    Ejemplo de uso en el navegador: http://localhost:8000/api/v1/mashup/bridge?word=noche
    """
    matchmaker = MatchmakerService(db)
    resultados = matchmaker.get_acapella_bridges(target_word=word)
    
    if not resultados:
        raise HTTPException(status_code=404, detail=f"No se encontraron canciones con la palabra '{word}'")
    
    return {
        "target_word": word,
        "total_matches": len(resultados),
        "matches": resultados
    }

from database.models import Song
import math

@app.get("/api/v1/mashup/perfect")
def get_perfect_match(song_title: str, db: Session = Depends(get_db)):
    """
    Motor DJ: Busca el match perfecto basándose en la Escala Armónica (Camelot Key)
    y calcula el ajuste de tempo (Pitch Shift).
    """
    # 1. Buscamos la canción base
    cancion_base = db.query(Song).filter(Song.title.ilike(f"%{song_title}%")).first()
    
    if not cancion_base or not cancion_base.camelot_key:
        raise HTTPException(status_code=404, detail=f"No se encontró '{song_title}' o le faltan datos acústicos.")

    # 2. Buscamos pistas con la misma escala pero distinto artista
    pistas_compatibles = db.query(Song).filter(
        Song.camelot_key == cancion_base.camelot_key,
        Song.artist_id != cancion_base.artist_id
    ).all()

    resultados = []
    for pista in pistas_compatibles:
        # Cálculo matemático del Pitch Shift
        pitch_shift = ((pista.bpm - cancion_base.bpm) / cancion_base.bpm) * 100
        
        # Evaluamos la viabilidad de la mezcla
        viabilidad = "Recomendada" if abs(pitch_shift) <= 10.0 else "Extrema (Distorsión)"
        
        resultados.append({
            "target_song": pista.title,
            "artist_id": pista.artist_id,
            "match_key": pista.camelot_key,
            "target_bpm": pista.bpm,
            "pitch_shift_required": round(pitch_shift, 2),
            "mix_viability": viabilidad
        })

    return {
        "source_song": cancion_base.title,
        "source_bpm": cancion_base.bpm,
        "source_key": cancion_base.camelot_key,
        "perfect_matches": resultados
    }