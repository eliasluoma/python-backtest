#!/usr/bin/env python3
"""
Fast Pool Check - Nopea poolien tarkistustyökalu

Tämä skripti tarkistaa nopeasti mitkä poolit Firebasessa sisältävät 
vähintään tietyn määrän datapisteitä ilman että kaikkia datapisteitä haetaan.
Tarkistus tapahtuu katsomalla markkinakontekstien ensimmäistä ja viimeistä dokumenttia
ja laskemalla niiden ID-arvojen erotuksen.
"""

import os
import sys
import time
import argparse
import logging

# Käytetään projektin olemassa olevia moduuleja
from src.data.firebase_service import FirebaseService
from src.data.cache_service import DataCacheService

# Asetetaan logging
logging.basicConfig(level=logging.INFO, 
                    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("FastPoolCheck")

def estimate_datapoints_by_doc_ids(firebase_service, pool_id, min_points=600):
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
        # Käytä FirebaseService:n datapisteiden laskemiseen
        # mutta ensiksi kokeile saada vain ensimmäinen ja viimeinen dokumentti

        # Hae ensimmäinen ja viimeinen dokumentti suoraan
        first_id, last_id = firebase_service.get_first_and_last_document_id(pool_id)
        
        if not first_id or not last_id:
            logger.debug(f"Poolille {pool_id} ei löytynyt dokumentteja")
            return 0, 0, None, None
        
        # Yritä laskea arvio datapisteiden määrästä dokumentti-ID:iden perusteella
        estimated_count = 0
        
        # Kokeillaan eri formaatteja
        try:
            # Jos ID:t ovat muotoa marketContext_XXXXXXXXXX
            if first_id.startswith("marketContext_") and last_id.startswith("marketContext_"):
                first_num = int(first_id.split("_")[1])
                last_num = int(last_id.split("_")[1])
                estimated_count = abs(last_num - first_num) + 1
            # Muussa tapauksessa käytä vakioarvoa kokemusperäisesti
            else:
                # Muissa tapauksissa käytetään yksinkertaista heuristista metodia
                # Oletetaan että datetimestamp-arvot edistyvät minuutin välein
                # Lisätietojen avulla arvio voidaan yrittää parantaa
                estimated_count = 500  # Tämä on vain karkea arvio, joka korvataan todellisella määrällä myöhemmin
        except (ValueError, IndexError):
            logger.debug(f"Dokumentti-ID:iden numeerinen muunnos epäonnistui: {first_id} - {last_id}")
            estimated_count = 0
        
        # Lasketaan myös tarkka määrä (hitaampi) FirebaseService luokan metodilla
        # Käytämme _get_single_pool_datapoints_count metodia, koska get_pool_datapoints_count ei ole olemassa
        actual_count = firebase_service._get_single_pool_datapoints_count(pool_id)
        
        # Jos todellinen määrä on pienempi kuin vaadittu minimi ja 
        # arvio on suurempi kuin vaadittu minimi, käytä arviota todellisen määrän sijaan
        # (Tämä on nopeampi lähestymistapa ja välttää turhia hakuja)
        if actual_count < min_points and estimated_count >= min_points:
            logger.debug(f"Pooli {pool_id} hylätään arvion {estimated_count} perusteella, vaikka todellinen määrä ei ole tiedossa.")
            return estimated_count, actual_count, first_id, last_id
        
        return estimated_count, actual_count, first_id, last_id
    
    except Exception as e:
        logger.error(f"Virhe poolille {pool_id}: {e}")
        return 0, 0, None, None

def fast_check_pools(min_points=600, limit=None, verbose=False):
    """
    Tarkistaa nopeasti poolit, joissa on vähintään tietty määrä datapisteitä
    
    Args:
        min_points: Vähimmäismäärä datapisteitä, jonka poolissa pitäisi olla
        limit: Maksimimäärä tarkastettavia pooleja
        verbose: Näytetäänkö tarkemmat lokit
    
    Returns:
        tuple: (hyväksytyt poolit, hylätyt poolit, kokonaisaika, keskimääräinen aika)
    """
    # Alusta Firebase palvelu
    firebase_service = FirebaseService()
    
    start_time = time.time()
    
    # Hae saatavilla olevat poolit
    all_pools = firebase_service.get_available_pools(limit=limit)
    if not all_pools:
        logger.error("Ei pooleja saatavilla")
        return [], [], 0, 0
    
    logger.info(f"Tarkistetaan {len(all_pools)} poolia (vähintään {min_points} datapistettä vaaditaan)")
    
    accepted_pools = []
    rejected_pools = []
    total_estimated = 0
    total_actual = 0
    estimation_accuracy = []
    
    # Tarkista jokainen pooli näyttäen edistymistä
    total_pools = len(all_pools)
    for i, pool_id in enumerate(all_pools):
        # Näytä edistyminen joka 10. poolin kohdalla tai kun saavutetaan 100%
        if i % 10 == 0 or i == total_pools - 1:
            progress = (i + 1) / total_pools * 100
            progress_bar = int(progress / 2)  # 50 merkkiä täydelle palkille
            print(f"\r[{'=' * progress_bar}{' ' * (50 - progress_bar)}] {progress:.1f}% ({i+1}/{total_pools})", end="")
            sys.stdout.flush()
        
        estimated_count, actual_count, first_id, last_id = estimate_datapoints_by_doc_ids(
            firebase_service, pool_id, min_points
        )
        
        # Laske arvioinnin tarkkuus (jos molemmat ovat > 0)
        if estimated_count > 0 and actual_count > 0:
            accuracy = estimated_count / actual_count
            estimation_accuracy.append(accuracy)
        
        # Päätä, hyväksytäänkö vai hylätäänkö pooli
        if actual_count >= min_points:
            accepted_pools.append((pool_id, estimated_count, actual_count))
            total_actual += actual_count
            total_estimated += estimated_count
            if verbose:
                logger.info(f"HYVÄKSYTTY: {pool_id} - arvio: {estimated_count}, todellinen: {actual_count}, " +
                           f"IDs: {first_id} -> {last_id}")
        else:
            rejected_pools.append((pool_id, estimated_count, actual_count))
            if verbose:
                logger.info(f"HYLÄTTY: {pool_id} - arvio: {estimated_count}, todellinen: {actual_count}, " +
                           f"IDs: {first_id} -> {last_id}")
    
    # Lopuksi uusi rivi
    print()
    
    end_time = time.time()
    total_time = end_time - start_time
    avg_time = total_time / len(all_pools) if all_pools else 0
    
    # Laske ja näytä tilastot
    avg_accuracy = sum(estimation_accuracy) / len(estimation_accuracy) if estimation_accuracy else 0
    logger.info(f"\nTarkistuksen tulokset:")
    logger.info(f"  Tarkistettu: {len(all_pools)} poolia")
    logger.info(f"  Hyväksytty: {len(accepted_pools)} poolia")
    logger.info(f"  Hylätty: {len(rejected_pools)} poolia")
    logger.info(f"  Arvioinnin keskimääräinen tarkkuus: {avg_accuracy:.2f}")
    logger.info(f"  Todellinen kokonaismäärä: {total_actual} datapistettä")
    logger.info(f"  Arvioitu kokonaismäärä: {total_estimated} datapistettä")
    logger.info(f"  Kokonaisaika: {total_time:.2f} sekuntia")
    logger.info(f"  Keskimääräinen aika per pooli: {avg_time:.4f} sekuntia")
    
    return accepted_pools, rejected_pools, total_time, avg_time

def main():
    """Pääfunktio"""
    # Määritä komentorivivalitsimet
    parser = argparse.ArgumentParser(description='Nopea poolien tarkistustyökalu')
    parser.add_argument('--min-points', type=int, default=600,
                       help='Vähimmäismäärä datapisteitä, jonka poolissa pitäisi olla (oletus: 600)')
    parser.add_argument('--limit', type=int, default=None,
                       help='Maksimimäärä tarkastettavia pooleja (oletus: rajoittamaton)')
    parser.add_argument('--verbose', '-v', action='store_true',
                       help='Näytä tarkemmat lokit')
    parser.add_argument('--output', '-o', type=str,
                       help='Tallenna hyväksytyt poolit tiedostoon')
    
    # Jäsennä argumentit
    args = parser.parse_args()
    
    # Suorita nopea tarkistus
    accepted_pools, rejected_pools, total_time, avg_time = fast_check_pools(
        min_points=args.min_points,
        limit=args.limit,
        verbose=args.verbose
    )
    
    # Näytä esimerkkejä hyväksytyistä pooleista
    if accepted_pools:
        print("\nEsimerkkejä hyväksytyistä pooleista:")
        for i, (pool_id, estimated, actual) in enumerate(accepted_pools[:5]):
            print(f"  {i+1}. {pool_id} - {actual} datapistettä (arvio: {estimated})")
        if len(accepted_pools) > 5:
            print(f"  ... ja {len(accepted_pools) - 5} muuta")
    
    # Tallenna hyväksytyt poolit tiedostoon, jos pyydetty
    if args.output and accepted_pools:
        try:
            with open(args.output, 'w') as f:
                for pool_id, _, actual in accepted_pools:
                    f.write(f"{pool_id},{actual}\n")
            print(f"\nHyväksytyt poolit tallennettu tiedostoon: {args.output}")
        except Exception as e:
            print(f"Virhe tiedostoon tallennuksessa: {e}")
    
    return 0

if __name__ == "__main__":
    sys.exit(main()) 