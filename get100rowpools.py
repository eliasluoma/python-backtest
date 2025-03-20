import logging
from datetime import datetime
from src.data.firebase_service import FirebaseService

MIN_DATA_POINTS = 1100
CHECK_TIME_OFFSET = 60  # sekuntia aloituksesta
FIELD_BUYVOLUME10S_OPTIONS = [
    "buyVolume10s",
    "trade_last10Seconds_volume_buy",
    "tradeLast10Seconds_volume_buy",
    "trade_last10Seconds.volume.buy",
    "tradeLast10Seconds.volume.buy"
]

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("PoolFilter")

def analyze_pools(firebase_service, pool_ids):
    stats = {
        'total': 0,
        'accepted': 0,
        'rejected': 0,
        'rejected_insufficient_data': 0,
        'rejected_missing_volume': 0,
        'rejected_missing_both': 0,
    }

    for pool_id in pool_ids:
        stats['total'] += 1

        first_doc_id, last_doc_id = firebase_service.get_first_and_last_document_id(pool_id)

        if not first_doc_id or not last_doc_id:
            logger.warning(f"Missing first or last document for pool {pool_id}")
            stats['rejected'] += 1
            stats['rejected_insufficient_data'] += 1
            continue

        first_timestamp = int(first_doc_id.split('_')[-1])
        last_timestamp = int(last_doc_id.split('_')[-1])
        data_point_count = last_timestamp - first_timestamp + 1

        # Volumen tarkistus noin 60 sekunnin kohdalta
        volume_check_timestamp = first_timestamp + CHECK_TIME_OFFSET
        volume_doc_id = f"marketContext_{volume_check_timestamp}"
        volume_doc = firebase_service.db.collection("marketContext").document(pool_id).collection("marketContexts").document(volume_doc_id).get()

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
            stats['accepted'] += 1
        else:
            logger.info(f"Rejected pool {pool_id}")
            stats['rejected'] += 1

            if not has_enough_data and not has_volume:
                stats['rejected_missing_both'] += 1
            elif not has_enough_data:
                stats['rejected_insufficient_data'] += 1
            elif not has_volume:
                stats['rejected_missing_volume'] += 1

    # Raportti
    logger.info("\nPool Analysis Report:")
    logger.info(f"Total pools processed: {stats['total']}")
    logger.info(f"Accepted pools: {stats['accepted']}")
    logger.info(f"Rejected pools: {stats['rejected']}")
    logger.info(f" - Insufficient data: {stats['rejected_insufficient_data']}")
    logger.info(f" - Missing volume data: {stats['rejected_missing_volume']}")
    logger.info(f" - Missing both: {stats['rejected_missing_both']}")

# Esimerkkikäyttö
if __name__ == '__main__':
    firebase_service = FirebaseService()
    pool_ids = firebase_service.get_available_pools(limit=5000)

    analyze_pools(firebase_service, pool_ids)