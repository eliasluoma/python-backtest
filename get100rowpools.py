import logging
from src.data.firebase_service import FirebaseService
from src.data.cache_service import DataCacheService

# Asetukset
MIN_DATA_POINTS = 600
CHECK_TIME_OFFSET = 60  # sekuntia aloituksesta
FIELD_BUYVOLUME10S_OPTIONS = [
    "buyVolume10s",
    "trade_last10Seconds_volume_buy",
    "tradeLast10Seconds_volume_buy",
    "trade_last10Seconds.volume.buy",
    "tradeLast10Seconds.volume.buy",
]

# Tietokanta-asetukset
DB_PATH = "cache/pools.db"
SCHEMA_PATH = "src/data/schema.sql"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("PoolFilter")


def analyze_and_cache_pools(firebase_service, cache_service, pool_ids):
    stats = {
        "total": 0,
        "accepted": 0,
        "rejected": 0,
        "rejected_insufficient_data": 0,
        "rejected_missing_volume": 0,
        "rejected_missing_both": 0,
        "cached_successfully": 0,
        "cache_errors": 0,
    }

    accepted_pool_ids = []

    for pool_id in pool_ids:
        stats["total"] += 1

        first_doc_id, last_doc_id = firebase_service.get_first_and_last_document_id(pool_id)

        if not first_doc_id or not last_doc_id:
            logger.warning(f"Missing first or last document for pool {pool_id}")
            stats["rejected"] += 1
            stats["rejected_insufficient_data"] += 1
            continue

        first_timestamp = int(first_doc_id.split("_")[-1])
        last_timestamp = int(last_doc_id.split("_")[-1])
        data_point_count = last_timestamp - first_timestamp + 1

        # Volumen tarkistus noin 60 sekunnin kohdalta
        volume_check_timestamp = first_timestamp + CHECK_TIME_OFFSET
        volume_doc_id = f"marketContext_{volume_check_timestamp}"
        volume_doc = (
            firebase_service.db.collection("marketContext")
            .document(pool_id)
            .collection("marketContexts")
            .document(volume_doc_id)
            .get()
        )

        has_volume = False
        if volume_doc.exists:
            volume_data = volume_doc.to_dict()
            for volume_field in FIELD_BUYVOLUME10S_OPTIONS:
                volume_value = volume_data.get(volume_field)
                if volume_value not in [0, None, "0", "0.0"]:
                    has_volume = True
                    break

        has_enough_data = data_point_count >= MIN_DATA_POINTS

        # Päätöksenteko
        if has_volume and has_enough_data:
            logger.info(f"Accepted pool {pool_id}")
            stats["accepted"] += 1
            accepted_pool_ids.append(pool_id)
        else:
            logger.info(f"Rejected pool {pool_id}")
            stats["rejected"] += 1

            if not has_enough_data and not has_volume:
                stats["rejected_missing_both"] += 1
            elif not has_enough_data:
                stats["rejected_insufficient_data"] += 1
            elif not has_volume:
                stats["rejected_missing_volume"] += 1

    # Hae ja tallenna hyväksyttyjen poolien data
    logger.info(f"\nHyväksytyt poolit: {len(accepted_pool_ids)}")
    logger.info("Aloitetaan hyväksyttyjen poolien tallennus tietokantaan...")

    for i, pool_id in enumerate(accepted_pool_ids):
        try:
            # Hae poolin data Firebasesta
            logger.info(f"Haetaan dataa poolille {pool_id} ({i+1}/{len(accepted_pool_ids)})")
            df = firebase_service.fetch_pool_data(pool_id)

            if df is not None and not df.empty:
                # Tallenna data tietokantaan
                logger.info(f"Tallennetaan {len(df)} datapistettä tietokantaan poolille {pool_id}")
                success = cache_service.update_pool_data(pool_id, df)

                if success:
                    stats["cached_successfully"] += 1
                    logger.info(f"Poolin {pool_id} tallennus onnistui!")
                else:
                    stats["cache_errors"] += 1
                    logger.error(f"Poolin {pool_id} tallennuksessa tapahtui virhe")
            else:
                stats["cache_errors"] += 1
                logger.error(f"Ei dataa saatavilla poolille {pool_id}")

        except Exception as e:
            stats["cache_errors"] += 1
            logger.error(f"Virhe käsiteltäessä poolia {pool_id}: {str(e)}")

    # Raportti
    logger.info("\nPool Analysis Report:")
    logger.info(f"Total pools processed: {stats['total']}")
    logger.info(f"Accepted pools: {stats['accepted']}")
    logger.info(f"Rejected pools: {stats['rejected']}")
    logger.info(f" - Insufficient data: {stats['rejected_insufficient_data']}")
    logger.info(f" - Missing volume data: {stats['rejected_missing_volume']}")
    logger.info(f" - Missing both: {stats['rejected_missing_both']}")
    logger.info(f"Pools cached successfully: {stats['cached_successfully']}")
    logger.info(f"Cache errors: {stats['cache_errors']}")


# Esimerkkikäyttö
if __name__ == "__main__":
    firebase_service = FirebaseService()
    cache_service = DataCacheService(DB_PATH, SCHEMA_PATH)

    # Voit säätää haettavien poolien määrää tästä
    pool_limit = 5000  # Hae 20 poolia testaukseen

    pool_ids = firebase_service.get_available_pools(limit=pool_limit)
    logger.info(f"Löydettiin {len(pool_ids)} poolia.")

    analyze_and_cache_pools(firebase_service, cache_service, pool_ids)
