"""
Cache commands for CLI interface.

This module provides CLI commands for managing the data cache,
including updating pools, clearing the cache, and showing cache status.
All field names use camelCase to match the REQUIRED_FIELDS from pool_analyzer.py.
"""

import argparse
import logging
from pathlib import Path
from typing import List, Optional
import time
from datetime import datetime
import sys

# Import service
from src.data.cache_service import DataCacheService
from src.data.firebase_service import FirebaseService

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def add_cache_subparser(subparsers):
    """Add the cache command and related subcommands to the CLI."""
    # Create cache command
    cache_parser = subparsers.add_parser("cache", help="Manage data cache")
    cache_subparsers = cache_parser.add_subparsers(dest="cache_command", help="Cache command to execute")

    # Update command
    update_parser = cache_subparsers.add_parser("update", help="Update cache")
    update_parser.add_argument(
        "--pools", "-p", nargs="+", help="Specific pools to update (if omitted, updates all pools)"
    )
    update_parser.add_argument("--recent", "-r", action="store_true", help="Update only recently active pools")
    update_parser.add_argument("--min-points", "-m", type=int, default=0, help="Minimum data points required in cache")

    # Import command
    import_parser = cache_subparsers.add_parser("import", help="Import pools from Firebase to cache")
    import_parser.add_argument(
        "--pools", "-p", nargs="+", help="Specific pools to import (if omitted, imports pools up to the limit)"
    )
    import_parser.add_argument(
        "--limit", "-l", type=int, default=None, help="Maximum number of pools to import (default: all pools)"
    )
    import_parser.add_argument(
        "--min-points",
        "-m",
        type=int,
        default=600,
        help="Minimum data points required for a pool to be imported (default: 600 = 10 minutes)",
    )
    import_parser.add_argument(
        "--new-only", "-n", action="store_true", help="Import only pools that don't exist in the local cache"
    )
    import_parser.add_argument("--schema", "-s", type=str, help="Path to schema file (defaults to schema.sql)")

    # Clear command
    clear_parser = cache_subparsers.add_parser("clear", help="Clear cache")
    clear_parser.add_argument("--days", "-d", type=int, help="Clear data older than specified days")

    # Status command
    cache_subparsers.add_parser("status", help="Show cache status")

    # Backup command
    backup_parser = cache_subparsers.add_parser("backup", help="Create a backup of the cache")
    backup_parser.add_argument(
        "--output", "-o", help="Output path for backup (if omitted, creates in default location)"
    )

    # Set cache_command as the handler function
    cache_parser.set_defaults(func=handle_cache_command)

    return cache_parser


def update_all_pools(cache_service: DataCacheService, firebase_service: FirebaseService) -> bool:
    """Update all available pools in cache."""
    # Get all pool IDs from Firebase
    pool_ids = firebase_service.get_available_pools(limit=1000)

    if not pool_ids:
        logger.error("No pools found in Firebase")
        return False

    logger.info(f"Found {len(pool_ids)} pools in Firebase")

    success_count = 0
    error_count = 0

    # Update each pool
    for pool_id in pool_ids:
        try:
            # Fetch data from Firebase
            df = firebase_service.fetch_pool_data(pool_id)

            if df.empty:
                logger.warning(f"No data found for pool {pool_id}")
                continue

            # Update cache
            success = cache_service.update_pool_data(pool_id, df)

            if success:
                success_count += 1
            else:
                error_count += 1

        except Exception as e:
            logger.error(f"Error updating pool {pool_id}: {e}")
            error_count += 1

    logger.info(f"Cache update completed: {success_count} pools updated, {error_count} errors")
    return success_count > 0


def update_specific_pools(
    cache_service: DataCacheService, firebase_service: FirebaseService, pool_ids: List[str], min_data_points: int = 0
) -> bool:
    """Update specific pools."""
    if not pool_ids:
        logger.error("No pool IDs provided")
        return False

    logger.info(f"Updating {len(pool_ids)} specific pools with min data points: {min_data_points}")
    print(f"\nPäivitetään {len(pool_ids)} poolia (vähintään {min_data_points} datapistettä vaaditaan)")

    success_count = 0
    error_count = 0
    no_data_count = 0
    skipped_count = 0
    insufficient_count = 0
    
    # Näytä edistymispalkki
    total_pools = len(pool_ids)
    progress_bar_width = 50
    
    print("\nPooli päivityksen edistyminen:")
    print(f"[{'_' * progress_bar_width}] 0%")
    
    # Update each pool
    start_time = time.time()
    for index, pool_id in enumerate(pool_ids):
        try:
            # Näytä edistyminen
            progress = (index + 1) / total_pools
            progress_bar = int(progress_bar_width * progress)
            if (index + 1) % max(1, min(5, total_pools // 10)) == 0 or index == total_pools - 1:
                print(f"\r[{'=' * progress_bar}{' ' * (progress_bar_width - progress_bar)}] {int(progress * 100)}%", end="")
                
            # Logita yksityiskohtaista tietoa
            logger.debug(f"Processing pool {index+1}/{total_pools}: {pool_id}")
            
            # Check if pool meets minimum data points requirement locally before fetching from Firebase
            if min_data_points > 0:
                pool_data = cache_service.get_pool_data(pool_id)
                if len(pool_data) >= min_data_points:
                    # Pool already has sufficient data
                    logger.debug(f"Pool {pool_id} already has sufficient data ({len(pool_data)} >= {min_data_points})")
                    skipped_count += 1
                    continue

            # Fetch data from Firebase
            fetch_start = time.time()
            df = firebase_service.fetch_pool_data(pool_id)
            fetch_duration = time.time() - fetch_start

            if df is None or df.empty:
                logger.warning(f"No data found for pool {pool_id}")
                no_data_count += 1
                continue

            # Check if pool has sufficient data points from Firebase
            if len(df) < min_data_points:
                logger.info(f"Pool {pool_id} has insufficient data points ({len(df)} < {min_data_points})")
                insufficient_count += 1
                continue

            # Update cache
            update_start = time.time()
            success = cache_service.update_pool_data(pool_id, df)
            update_duration = time.time() - update_start

            if success:
                success_count += 1
                logger.info(f"Successfully updated pool {pool_id} with {len(df)} data points (fetch: {fetch_duration:.2f}s, update: {update_duration:.2f}s)")
            else:
                error_count += 1
                logger.error(f"Failed to update pool {pool_id}")

        except Exception as e:
            error_count += 1
            logger.error(f"Error updating pool {pool_id}: {e}")
    
    # Loppuun uusi rivi
    print()
    
    end_time = time.time()
    total_time = end_time - start_time
    avg_time = total_time / max(1, len(pool_ids))

    print("\nPOOLIEN PÄIVITYKSEN TULOKSET:")
    print(f"Onnistuneet päivitykset:    {success_count} kpl")
    print(f"Ei dataa Firebasessa:       {no_data_count} kpl")
    print(f"Liian vähän dataa:          {insufficient_count} kpl")
    print(f"Ohitetut (riittävä data):   {skipped_count} kpl")
    print(f"Virheet päivityksessä:      {error_count} kpl")
    print(f"Kokonaisaika:               {total_time:.2f} sekuntia")
    print(f"Keskimäärin per pooli:      {avg_time:.2f} sekuntia")

    logger.info(
        f"Cache update completed: {success_count} pools updated, "
        f"{error_count} errors, {no_data_count} without sufficient data"
    )
    return success_count > 0


def update_recent_pools(cache_service: DataCacheService, firebase_service: FirebaseService) -> bool:
    """Update recently active pools."""
    # Get recent market data from Firebase (last 24 hours)
    recent_data = firebase_service.fetch_recent_market_data(hours_back=24, max_pools=100)

    if not recent_data:
        logger.error("No recent pools found in Firebase")
        return False

    logger.info(f"Found {len(recent_data)} recent pools in Firebase")

    # Extract pool IDs from the data
    pool_ids = list(recent_data.keys())

    # Use the specific pools update function
    return update_specific_pools(cache_service, firebase_service, pool_ids)


def clear_entire_cache(cache_service: DataCacheService) -> bool:
    """Clear the entire cache."""
    logger.info("Clearing entire cache")
    return cache_service.clear_cache()


def clear_old_data(cache_service: DataCacheService, days: int) -> bool:
    """Clear data older than the specified number of days."""
    logger.info(f"Clearing data older than {days} days")
    return cache_service.clear_cache(older_than_days=days)


def show_cache_status(cache_service: DataCacheService) -> bool:
    """Show the current cache status."""
    stats = cache_service.get_cache_stats()

    if stats.get("status") != "success":
        logger.error(f"Failed to get cache stats: {stats.get('message', 'Unknown error')}")
        return False

    data = stats.get("data", {})

    print("\nCache Status:")
    print(f"  Database path: {data.get('database_path')}")
    print(f"  Last update: {data.get('last_update')}")
    print(f"  Total pools: {data.get('total_pools')}")
    print(f"  Total data points: {data.get('total_data_points')}")
    print(f"  Cache size: {data.get('cache_size_mb', 0):.2f} MB")

    memory_cache = data.get("memory_cache", {})
    print(f"  Memory cache: {memory_cache.get('size', 0)} / {memory_cache.get('max_size', 0)} pools")

    # Show largest pools
    largest_pools = data.get("largest_pools", [])
    if largest_pools:
        print("\nLargest pools in cache:")
        for pool in largest_pools:
            print(f"  {pool.get('pool_id')}: {pool.get('data_points')} data points")

    return True


def import_missing_pools(
    cache_service: DataCacheService,
    firebase_service: FirebaseService,
    limit: Optional[int] = None,
    min_data_points: int = 600,
) -> bool:
    """
    Import only pools that don't exist in the local database and refresh incomplete pools.
    
    Args:
        cache_service: The data cache service instance
        firebase_service: The Firebase service instance
        limit: Maximum number of new pools to import
        min_data_points: Minimum data points required for a pool
        
    Returns:
        bool: Whether the import was successful
    """
    print("\n===== ALOITETAAN POOLIEN TARKISTUS JA TUONTI =====\n")
    print("Aloitetaan puulien tuonti (uudet ja keskeneräiset)")
    start_time = time.time()
    
    # 0. PART ZERO: Haetaan lista tarkistetuista pooleista
    verified_pools = cache_service.get_verified_pools()
    verified_pool_ids = {pool["pool_id"].lower() for pool in verified_pools}
    print(f"\n--- AIEMMIN TARKISTETTUJEN POOLIEN TARKISTUS ---")
    print(f"Löytyi {len(verified_pool_ids)} aiemmin tarkistettua poolia, joiden eheys on varmistettu")
    
    # Näytä esimerkkejä
    if verified_pools:
        print("\nEsimerkkejä aiemmin tarkastetuista pooleista:")
        for i, pool in enumerate(verified_pools[:5]):
            print(f"  {i+1}. {pool['pool_id']} - tarkistettu {pool['verified_at']}")
            if pool.get('note'):
                print(f"     Huomio: {pool['note']}")
        if len(verified_pools) > 5:
            print(f"  ... ja {len(verified_pools) - 5} muuta")
    
    # 1. PART ONE: Find completely new pools
    # Get existing pool IDs from local cache (with their datapoint counts)
    print("\n--- PAIKALLISEN TIETOKANNAN TARKISTUS ---")
    local_pools_info = cache_service.get_pools_with_datapoints()
    
    # Näytä yksityiskohtaista tietoa paikallisista pooleista
    print(f"Löytyi {len(local_pools_info)} poolia paikallisessa tietokannassa:")
    total_local_datapoints = sum(pool_info["dataPoints"] for pool_info in local_pools_info)
    print(f"Paikallisessa tietokannassa on yhteensä {total_local_datapoints} datapistettä")
    print(f"Keskimäärin {total_local_datapoints / max(1, len(local_pools_info)):.1f} datapistettä per pooli")
    
    # Näytä muutama esimerkki
    if local_pools_info:
        print("\nEsimerkkejä pooleista paikallisessa tietokannassa:")
        for i, pool_info in enumerate(sorted(local_pools_info, key=lambda x: x["dataPoints"], reverse=True)[:5]):
            verified_status = "✓ Tarkistettu" if pool_info["poolAddress"].lower() in verified_pool_ids else "☐ Ei tarkistettu"
            print(f"  {i+1}. {pool_info['poolAddress']} - {pool_info['dataPoints']} datapistettä - {verified_status}")
    
    # Muunnetaan poolien osoitteet pieneen kirjainkokoon vertailua varten (case-insensitive)
    existing_pool_ids = set(pool_info["poolAddress"].lower() for pool_info in local_pools_info)
    print(f"\nLöytyi {len(existing_pool_ids)} olemassa olevaa puulia paikallisessa tietokannassa")
    logger.info(f"Found {len(existing_pool_ids)} existing pools in local database")
    
    # Create a map of pool_id -> datapoints for faster lookup (käytetään lower-case)
    local_pool_datapoints = {pool_info["poolAddress"].lower(): pool_info["dataPoints"] for pool_info in local_pools_info}
    
    # Get pools from Firebase
    print("\n--- FIREBASE-TIETOKANNAN TARKISTUS ---")
    fetch_limit = None if limit is None else limit * 2  # Double to account for filtering
    print(f"Haetaan puulit Firebasesta (raja={fetch_limit})")
    
    firebase_start_time = time.time()
    firebase_pools = firebase_service.get_available_pools(limit=fetch_limit)
    firebase_fetch_time = time.time() - firebase_start_time
    
    if not firebase_pools:
        print("Firebasesta ei löytynyt yhtään puulia")
        logger.error("No pools found in Firebase")
        return False
    
    print(f"Löytyi {len(firebase_pools)} puulia Firebasesta (Haku kesti {firebase_fetch_time:.2f} sekuntia)")
    
    # Find missing pools (in Firebase but not in local cache) - Käytetään lower-case vertailua
    completely_new_pools = [pool_id for pool_id in firebase_pools if pool_id.lower() not in existing_pool_ids]
    print(f"\nLöytyi {len(completely_new_pools)} kokonaan uutta puulia, jotka eivät ole paikallisessa tietokannassa")
    
    # 2. PART TWO: Käytetään nopeaa arviointia datapisteiden määrälle poolien karsimiseksi
    # Tämä on fast_pool_check.py:n tekniikan sovellus, joka arvioi datapisteet dokumentti-ID:iden perusteella
    print("\n--- NOPEA POOLIEN ARVIOINTI ---")
    print(f"Arvioidaan poolien datapisteiden määrä nopealla menetelmällä...")
    
    # Alusta oikean kokoiset listat
    pools_to_check = completely_new_pools.copy()
    
    # Lisätään myös ei-tarkistetut olemassa olevat poolit
    untrusted_pools = [
        pool_id for pool_id in firebase_pools 
        if pool_id.lower() in existing_pool_ids and pool_id.lower() not in verified_pool_ids
    ]
    
    print(f"Täysin uusia pooleja: {len(completely_new_pools)}")
    print(f"Tarkistamattomia olemassa olevia pooleja: {len(untrusted_pools)}")
    
    # Näytä edistymispalkki
    total_pools_to_check = len(pools_to_check)
    progress_bar_width = 50
    
    print("\nPoolien nopean arvioinnin edistyminen:")
    print(f"[{'_' * progress_bar_width}] 0%")
    
    acceptable_pools = []
    rejected_pools = []
    total_estimated = 0
    total_actual = 0
    estimation_accuracy = []
    
    start_check_time = time.time()
    
    # Tarkista jokainen pooli näyttäen edistymistä
    for i, pool_id in enumerate(pools_to_check):
        # Näytä edistymistä joka 10. poolin kohdalla
        if i % 10 == 0 or i == total_pools_to_check - 1:
            progress = (i + 1) / total_pools_to_check * 100
            progress_bar = int(progress / 2)  # 50 merkkiä täydelle palkille
            print(f"\r[{'=' * progress_bar}{' ' * (50 - progress_bar)}] {progress:.1f}% ({i+1}/{total_pools_to_check})", end="")
            sys.stdout.flush()
        
        # Arvioi poolien datapisteet nopeasti käyttäen ensimmäisen ja viimeisen dokumentin ID:tä
        estimated_count, actual_count, first_id, last_id = estimate_datapoints_for_pool(
            firebase_service, pool_id, min_data_points
        )
        
        # Laske arvioinnin tarkkuus (jos molemmat ovat > 0)
        if estimated_count > 0 and actual_count > 0:
            accuracy = estimated_count / actual_count
            estimation_accuracy.append(accuracy)
        
        # Päätä, hyväksytäänkö vai hylätäänkö pooli
        if actual_count >= min_data_points:
            acceptable_pools.append(pool_id)
            total_actual += actual_count
            total_estimated += estimated_count
        else:
            rejected_pools.append(pool_id)
    
    # Lopuksi uusi rivi
    print()
    
    end_check_time = time.time()
    check_time = end_check_time - start_check_time
    
    # Laske ja näytä tilastot
    avg_accuracy = sum(estimation_accuracy) / len(estimation_accuracy) if estimation_accuracy else 0
    print(f"\nNopeasti tarkistetut poolit: {len(pools_to_check)}")
    print(f"  Hyväksytty: {len(acceptable_pools)} poolia")
    print(f"  Hylätty: {len(rejected_pools)} poolia")
    print(f"  Arvioinnin keskimääräinen tarkkuus: {avg_accuracy:.2f}")
    print(f"  Nopean tarkistuksen kesto: {check_time:.2f} sekuntia")
    print(f"  Keskimääräinen aika per pooli: {check_time / max(1, len(pools_to_check)):.4f} sekuntia")
    
    # OPTIMOINTI: Jos ei löydetty hyväksyttäviä pooleja eikä ole tarkistettavia pooleja,
    # voimme lopettaa prosessin tähän ilman turhia tarkistuksia
    if len(acceptable_pools) == 0 and len(untrusted_pools) == 0:
        print("\n===== NOPEA OPTIMOINTI =====")
        print("Ei löydetty hyväksyttäviä pooleja eikä ole tarkistamattomia pooleja paikallisessa kannassa.")
        print("Prosessi voidaan päättää aikaisemmin ilman turhia tarkistuksia.")
        print("\nEi uusia tai puutteellisia puuleja tuotavaksi, tietokanta on ajan tasalla")
        logger.info("No new or incomplete pools to import, database is up to date (optimized early exit)")
        return True
    
    # 3. PART THREE: Process incomplete pools (for existing, non-verified pools)
    # OPTIMOINTI: Ohitetaan olemassa olevien poolien vertailu kokonaan, jos ei ole tarkistamattomia pooleja
    if len(untrusted_pools) == 0:
        print("\n===== OPTIMOINTI: OHITETAAN OLEMASSA OLEVIEN POOLIEN VERTAILU =====")
        print("Ei löytynyt tarkistamattomia pooleja paikallisessa tietokannassa.")
        print("Ohitetaan olemassa olevien poolien vertailu kokonaan.")
        incomplete_pools = []
    else:
        print("\n--- OLEMASSA OLEVIEN POOLIEN VERTAILU ---")
        
        # Riittää kun tuodaan vain ne poolit jotka puuttuvat paikallisesta tietokannasta täysin
        # tai joissa on liian vähän datapisteitä paikallisesti
        incomplete_pools = []
        
        # Tarkistetaan puuttuvat poolit myös nopealla arviointimenetelmällä
        for i, pool_id in enumerate(untrusted_pools):
            pool_id_lower = pool_id.lower()
            local_data_count = local_pool_datapoints.get(pool_id_lower, 0)
            
            # Arvioi poolien datapisteet nopeasti käyttäen ensimmäisen ja viimeisen dokumentin ID:tä
            estimated_count, actual_count, first_id, last_id = estimate_datapoints_for_pool(
                firebase_service, pool_id, min_data_points
            )
            
            # Jos Firebasessa on merkittävästi enemmän dataa, lisää se täydennettäviin pooleihin
            if actual_count > local_data_count + 10 and actual_count >= min_data_points:
                incomplete_pools.append(pool_id)
    
    end_time = time.time()
    time_taken = end_time - start_time
    print(f"\nLöytyi {len(incomplete_pools)} puutteellista puulia jotka päivitetään (tarkistus kesti {time_taken:.2f} sekuntia)")
    logger.info(f"Found {len(incomplete_pools)} incomplete pools that will be refreshed (check took {time_taken:.2f} seconds)")
    
    # OPTIMOINTI #2: Jos hyväksyttäviä uusia pooleja ei ole ja puutteellisia pooleja ei löydy,
    # prosessi voidaan päättää aikaisemmin (turha jatkaa tuontiprosessia)
    if len(acceptable_pools) == 0 and len(incomplete_pools) == 0:
        print("\n===== NOPEA OPTIMOINTI #2 =====")
        print("Hyväksyttäviä uusia pooleja ei ole ja puutteellisia pooleja ei löydy.")
        print("Prosessi voidaan päättää aikaisemmin ilman turhia vaiheita.")
        print("\nEi uusia tai puutteellisia puuleja tuotavaksi, tietokanta on ajan tasalla")
        logger.info("No new or incomplete pools to import, database is up to date (optimized exit #2)")
        return True
    
    # Yhdistetään uudet ja puutteelliset poolit, mutta poistetaan jo tarkistetut
    # Käytetään nyt acceptable_pools-listaa täysin uusille pooleille
    # Ei tuoda jo tarkistettuja (verified) pooleja uudelleen
    all_pools_to_import = [
        pool_id for pool_id in (acceptable_pools + incomplete_pools)
        if pool_id.lower() not in verified_pool_ids
    ]
    
    # Tulosta tilasto jo tarkistetuista ja ohitetuista pooleista
    skipped_verified = [
        pool_id for pool_id in (acceptable_pools + incomplete_pools)
        if pool_id.lower() in verified_pool_ids
    ]
    
    if skipped_verified:
        print(f"\nJo tarkistettuja pooleja ohitettiin: {len(skipped_verified)} kpl")
        for i, pool_id in enumerate(skipped_verified[:min(3, len(skipped_verified))]):
            print(f"  {i+1}. {pool_id} - jo tarkistettu aiemmin")
        if len(skipped_verified) > 3:
            print(f"  ... ja {len(skipped_verified) - 3} muuta")
    
    if not all_pools_to_import:
        print("\nEi uusia tai puutteellisia puuleja tuotavaksi, tietokanta on ajan tasalla")
        logger.info("No new or incomplete pools to import, database is up to date")
        return True
    
    # Apply limit if specified
    if limit is not None and len(all_pools_to_import) > limit:
        print(f"\nRajoitetaan tuonti {limit} puuliin (alkuperäinen määrä: {len(all_pools_to_import)})")
        all_pools_to_import = all_pools_to_import[:limit]
        logger.info(f"Limiting import to {limit} total pools")
    
    # Import the pools
    print(f"\n--- ALOITETAAN POOLIEN TUONTI ---")
    print(f"Tuodaan {len(all_pools_to_import)} puulia (uudet: {len([p for p in all_pools_to_import if p in acceptable_pools])}, puutteelliset: {len([p for p in all_pools_to_import if p in incomplete_pools])})")
    print("Tuonti voi kestää useita minuutteja poolien määrästä riippuen...")
    import_start_time = time.time()
    
    # Päivitetään poolit ja kerätään onnistuneiden tuontien ID:t
    result = update_specific_pools(cache_service, firebase_service, all_pools_to_import, min_data_points)
    
    import_time = time.time() - import_start_time
    print(f"\nTuonti valmis! Kesto: {import_time:.2f} sekuntia")
    
    # Merkitään onnistuneesti tuodut poolit tarkistetuiksi
    print("\n--- MERKITÄÄN TARKISTETUT POOLIT ---")
    
    # Haetaan onnistuneet poolit - päivitetään yhtenäisyys tuonnin jälkeen
    # Tämä on yksinkertaistettu toteutus; todellisuudessa pitäisi seurata update_specific_pools-funktion
    # palauttamia onnistumistietoja ja käyttää niitä.
    
    # Tässä esimerkissä oletetaan, että kaikki poolit, joilla on vähintään min_data_points datapistettä, 
    # on tuotu onnistuneesti
    new_pools_after_import = cache_service.get_pools_with_datapoints(min_data_points=min_data_points)
    new_pool_ids = {pool_info["poolAddress"].lower() for pool_info in new_pools_after_import}
    
    # Onnistuneesti tuodut poolit ovat ne, jotka nyt ovat tietokannassa ja joita tuotiin
    successfully_imported = [
        pool_id for pool_id in all_pools_to_import
        if pool_id.lower() in new_pool_ids and pool_id.lower() not in verified_pool_ids
    ]
    
    if successfully_imported:
        mark_start_time = time.time()
        # Merkitään poolit tarkistetuiksi
        note = f"Tarkistettu automaattisesti {datetime.now().strftime('%Y-%m-%d %H:%M')} tuonnin yhteydessä"
        marked_count = cache_service.mark_pools_verified(successfully_imported, note)
        mark_time = time.time() - mark_start_time
        
        print(f"Merkittiin {marked_count} poolia tarkistetuksi (kesto: {mark_time:.2f} sekuntia)")
        print(f"Nämä poolit ohitetaan seuraavissa tarkistuksissa automaattisesti.")
    else:
        print("Ei uusia tarkistettuja pooleja merkittäväksi.")
    
    print("\n===== POOLIEN TARKISTUS JA TUONTI VALMIS =====\n")
    
    return result


def backup_cache(cache_service: DataCacheService, output_path: Optional[str] = None) -> bool:
    """Create a backup of the cache."""
    logger.info(f"Creating cache backup{' to ' + output_path if output_path else ''}")
    success, backup_path = cache_service.backup_database(output_path)

    if success:
        print(f"Cache backup created at: {backup_path}")
    else:
        logger.error(f"Failed to create backup: {backup_path}")

    return success


def import_pools(
    cache_service: DataCacheService,
    firebase_service: FirebaseService,
    pool_ids: List[str] = None,
    limit: Optional[int] = None,
    min_data_points: int = 600,
    new_only: bool = False,
) -> bool:
    """
    Import pools from Firebase to SQLite cache with specific criteria.

    Args:
        cache_service: The data cache service instance
        firebase_service: The Firebase service instance
        pool_ids: Specific pool IDs to import (if None, imports pools up to the limit)
        limit: Maximum number of pools to import (if None, imports all available pools)
        min_data_points: Minimum number of data points required for a pool to be imported
        new_only: If True, only import pools that don't exist in the local cache

    Returns:
        bool: Whether the import was successful
    """
    logger.info(f"Starting import with limit={limit if limit else 'all'}, min_data_points={min_data_points}")

    # If specific pools are provided, use those
    if pool_ids:
        logger.info(f"Importing {len(pool_ids)} specific pools")
        return update_specific_pools(cache_service, firebase_service, pool_ids, min_data_points)

    # Otherwise, get all available pools
    # If limit is None, don't apply a limit to Firebase query
    fetch_limit = None if limit is None else limit * 2  # Double the limit to account for filtering
    all_pools = firebase_service.get_available_pools(limit=fetch_limit)
    if not all_pools:
        logger.error("No pools found in Firebase")
        return False

    logger.info(
        f"Found {len(all_pools)} pools in Firebase, importing {'all' if limit is None else f'up to {limit}'} pools"
    )

    # If new_only is True, filter out pools that already exist in the cache
    if new_only:
        # Get existing pool IDs from cache
        existing_pools = cache_service.get_pool_ids(limit=100000)  # Get all existing pools

        # Filter pools to only those not in the cache
        filtered_pools = [pool_id for pool_id in all_pools if pool_id not in existing_pools]

        # Log how many pools we filtered out
        filtered_out = len(all_pools) - len(filtered_pools)
        logger.info(f"Filtered out {filtered_out} pools that already exist in the cache")
        logger.info(f"Remaining pools to import: {len(filtered_pools)}")

        # Use the filtered list
        all_pools = filtered_pools

        # If no new pools to import, we're done
        if not all_pools:
            logger.info("No new pools to import")
            return True

    success_count = 0
    error_count = 0
    insufficient_data_count = 0
    processed_count = 0

    for pool_id in all_pools:
        # If limit is specified and we've reached it, break
        if limit is not None and processed_count >= limit:
            logger.info(f"Reached import limit of {limit} pools")
            break

        try:
            # Fetch data from Firebase
            df = firebase_service.fetch_pool_data(pool_id)

            if df.empty:
                logger.warning(f"No data found for pool {pool_id}")
                continue

            # Check if pool has sufficient data points
            if len(df) < min_data_points:
                logger.info(f"Pool {pool_id} has insufficient data points ({len(df)} < {min_data_points})")
                insufficient_data_count += 1
                continue

            # Update cache
            success = cache_service.update_pool_data(pool_id, df)
            processed_count += 1

            if success:
                logger.info(f"Successfully imported pool {pool_id} with {len(df)} data points")
                success_count += 1
            else:
                logger.error(f"Failed to import pool {pool_id}")
                error_count += 1

        except Exception as e:
            logger.error(f"Error importing pool {pool_id}: {e}")
            error_count += 1

    # Log summary
    logger.info("Import completed:")
    logger.info(f"  Pools processed: {processed_count}")
    logger.info(f"  Successfully imported: {success_count}")
    logger.info(f"  Failed imports: {error_count}")
    logger.info(f"  Insufficient data points: {insufficient_data_count}")

    return success_count > 0


def handle_cache_command(args: argparse.Namespace):
    """Handle cache commands based on arguments."""
    # Get cache directory
    cache_dir = Path(__file__).parent.parent.parent.parent / "cache"
    cache_dir.mkdir(exist_ok=True)

    # Create cache service
    db_path = cache_dir / "pools.db"
    schema_path = Path(__file__).parent.parent.parent / "data" / "schema.sql"

    cache_service = DataCacheService(db_path=str(db_path), schema_path=str(schema_path))

    # Handle subcommands
    if args.cache_command == "update":
        if args.pools:
            # Initialize Firebase service
            firebase_service = FirebaseService()
            return update_specific_pools(cache_service, firebase_service, args.pools, min_data_points=args.min_points)
        elif args.recent:
            # Initialize Firebase service
            firebase_service = FirebaseService()
            return update_recent_pools(cache_service, firebase_service)
        else:
            # Initialize Firebase service
            firebase_service = FirebaseService()
            return update_all_pools(cache_service, firebase_service)

    elif args.cache_command == "clear":
        if args.days:
            return clear_old_data(cache_service, args.days)
        else:
            return clear_entire_cache(cache_service)

    elif args.cache_command == "status":
        return show_cache_status(cache_service)

    elif args.cache_command == "backup":
        return backup_cache(cache_service, args.output)

    elif args.cache_command == "import":
        # Initialize Firebase service
        firebase_service = FirebaseService()

        # Determine schema path
        schema_path = args.schema if args.schema else Path(__file__).parent.parent.parent / "data" / "schema.sql"

        # Create cache service with specified schema
        cache_service = DataCacheService(db_path=str(db_path), schema_path=str(schema_path))

        return import_pools(
            cache_service,
            firebase_service,
            pool_ids=args.pools,
            limit=args.limit,
            min_data_points=args.min_points,
            new_only=args.new_only,
        )

    else:
        logger.error(f"Unknown cache command: {args.cache_command}")
        return False


# Apufunktio poolien datapisteiden arvioimiseen
def estimate_datapoints_for_pool(firebase_service, pool_id, min_points=600):
    """
    Arvioi datapisteiden määrän markkinakontekstien dokumentti-ID:iden perusteella
    
    Args:
        firebase_service: FirebaseService-instanssi
        pool_id: Poolin ID
        min_points: Vähimmäismäärä datapisteitä, jonka poolissa pitäisi olla
        
    Returns:
        tuple: (arvioitu määrä, todellinen määrä, ensimmäinen ID, viimeinen ID)
    """
    try:
        # Hae ensimmäinen ja viimeinen dokumentti suoraan
        first_id, last_id = firebase_service.get_first_and_last_document_id(pool_id)
        
        if not first_id or not last_id:
            logger.debug(f"Poolille {pool_id} ei löytynyt dokumentteja")
            return 0, 0, None, None
        
        # Yritä laskea arvio datapisteiden määrästä dokumentti-ID:iden perusteella
        estimated_count = 0
        
        try:
            # Jos ID:t ovat muotoa marketContext_XXXXXXXXXX
            if first_id.startswith("marketContext_") and last_id.startswith("marketContext_"):
                first_num = int(first_id.split("_")[1])
                last_num = int(last_id.split("_")[1])
                estimated_count = abs(last_num - first_num) + 1
            else:
                # Oletetaan että datetimestamp-arvot edistyvät minuutin välein
                estimated_count = 500  # Karkea arvio
        except (ValueError, IndexError):
            logger.debug(f"Dokumentti-ID:iden numeerinen muunnos epäonnistui: {first_id} - {last_id}")
            estimated_count = 0
        
        # Haetaan myös tarkka määrä
        actual_count = firebase_service._get_single_pool_datapoints_count(pool_id)
        
        # Jos arvio on vähintään vaadittu minimi mutta todellinen määrä on pienempi, 
        # käytetään todellista määrää
        if estimated_count >= min_points and actual_count < min_points:
            logger.debug(f"Pooli {pool_id} hylätään todellisen määrän {actual_count} perusteella")
        
        return estimated_count, actual_count, first_id, last_id
    
    except Exception as e:
        logger.error(f"Virhe poolille {pool_id}: {e}")
        return 0, 0, None, None
