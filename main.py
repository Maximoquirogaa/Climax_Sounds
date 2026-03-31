import os
import logging
from dotenv import load_dotenv

from database.session import DatabaseManager
from database.models import Base
from core.analyzer import LyricAnalyzer
from fetchers.genius_miner import GeniusDataMiner
from services.ingestion_service import IngestionService
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
)
logger = logging.getLogger("PipelinePrincipal")

def init_database(db_manager: DatabaseManager):
    logger.info("Verificando integridad del esquema de base de datos...")
    Base.metadata.create_all(bind=db_manager.engine)
    logger.info("Esquema verificado. Tablas listas.")

def main():
    load_dotenv()
    db_url = os.getenv("DATABASE_URL")
    genius_token = os.getenv("GENIUS_ACCESS_TOKEN")

    if not db_url or not genius_token:
        logger.critical("Faltan variables de entorno. Revisá tu archivo .env")
        return

    db_manager = DatabaseManager(db_url)
    init_database(db_manager)
    
    analyzer = LyricAnalyzer() 
    miner = GeniusDataMiner(genius_token)

    artistas_objetivo = [
        {"nombre": "Los Enanitos Verdes", "genero": "Rock Nacional"},
        {"nombre": "Gustavo Cerati", "genero": "Rock Nacional"},
        {"nombre": "Wos", "genero": "Rap / Trap"}
    ]
    
    canciones_por_artista = 5 
    for artista_info in artistas_objetivo:
        nombre = artista_info["nombre"]
        genero = artista_info["genero"]
        
        logger.info(f"--- INICIANDO PIPELINE PARA: {nombre} ---")
        
        catalogo = miner.fetch_artist_catalog(nombre, max_songs=canciones_por_artista)
        
        if not catalogo:
            logger.warning(f"Se omitirá a {nombre} por falta de datos.")
            continue

        with db_manager.get_session() as db_session:
            ingestion_svc = IngestionService(db_session, analyzer)
            
            exitos = 0
            for cancion in catalogo:
                logger.info(f"Procesando: {cancion['title']}...")
                
                resultado = ingestion_svc.process_and_save_song(
                    artist_name=cancion["artist"],
                    genre_name=genero,
                    song_title=cancion["title"],
                    release_year=cancion["release_year"],
                    raw_lyrics=cancion["lyrics"]
                )
                
                if resultado:
                    exitos += 1
            
            logger.info(f"Resumen para {nombre}: {exitos}/{len(catalogo)} canciones guardadas exitosamente.\n")

    logger.info("PIPELINE FINALIZADO. La base de datos está poblada.")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.warning("Proceso cancelado por el usuario de forma segura.")
    except Exception as e:
        logger.critical(f"Falla crítica en la aplicación: {e}")
