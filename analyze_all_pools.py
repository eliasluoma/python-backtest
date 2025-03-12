#!/usr/bin/env python
"""
Kaikkien Poolien Analyysi

Tämä skripti analysoi kaikki Firebase-poolit ja tuottaa raportin:
1. Kuinka monessa poolissa on kaikki vaaditut kentät
2. Kuinka monessa poolissa on vähintään 600 riviä (n. 10 min)
3. Kuinka monessa poolissa on vähintään 1100 riviä (n. 18 min)
4. Kuinka monessa poolissa on kaikki vaaditut kentät JA vähintään 600 riviä 
5. Kuinka monessa poolissa on kaikki vaaditut kentät JA vähintään 1100 riviä
6. Analyysi puuttuvista kentistä (mitä kenttiä puuttuu ja kuinka usein)
7. Analyysi ylimääräisistä kentistä (kentät joita ei ole vaadittujen listalla)
8. Analyysi nimeämiskäytännöistä (alaviiva vs. camelCase)
"""

import os
import sys
import logging
import json
import re
from collections import Counter
from datetime import datetime

# Aseta absoluuttinen polku juurihakemistolle
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, project_root)

# Aseta logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Tulosta debuggaustietoa ympäristöstä
print(f"Python versio: {sys.version}")
print(f"Työhakemisto: {os.getcwd()}")
print(f"sys.path: {sys.path}")
print(f"Skriptin sijainti: {__file__}")
print(f"Projektin juurihakemisto: {project_root}")

# Tuo Firebase-apuohjelmat
try:
    from src.data.firebase_service import FirebaseService
    from src.utils.firebase_utils import get_pool_ids
    print("Firebase-moduulit ladattu onnistuneesti!")
except ImportError as e:
    logger.error(f"Firebase-moduuleja ei löydy: {e}")
    logger.error("Varmista että ajat skriptin projektin juurihakemistosta.")
    sys.exit(1)


# Määritä 63 vaadittua kenttää, joita kauppastrategiamme käyttävät
REQUIRED_FIELDS = [
    # Market Cap -kentät
    "marketCap",
    "athMarketCap",
    "minMarketCap",
    "marketCapChange5s",
    "marketCapChange10s",
    "marketCapChange30s",
    "marketCapChange60s",
    "maMarketCap10s",
    "maMarketCap30s",
    "maMarketCap60s",
    # Hintakentät
    "currentPrice",
    "priceChangePercent",
    "priceChangeFromStart",
    # Omistaja-kentät
    "holdersCount",
    "initialHoldersCount",
    "holdersGrowthFromStart",
    "holderDelta5s",
    "holderDelta10s",
    "holderDelta30s",
    "holderDelta60s",
    # Volyymi-kentät
    "buyVolume5s",
    "buyVolume10s",
    "netVolume5s",
    "netVolume10s",
    # Osto-luokittelukentät
    "largeBuy5s",
    "largeBuy10s",
    "bigBuy5s",
    "bigBuy10s",
    "superBuy5s",
    "superBuy10s",
    # Kauppatiedot - 5s
    "trade_last5Seconds.volume.buy",
    "trade_last5Seconds.volume.sell",
    "trade_last5Seconds.volume.bot",
    "trade_last5Seconds.tradeCount.buy.small",
    "trade_last5Seconds.tradeCount.buy.medium",
    "trade_last5Seconds.tradeCount.buy.large",
    "trade_last5Seconds.tradeCount.buy.big",
    "trade_last5Seconds.tradeCount.buy.super",
    "trade_last5Seconds.tradeCount.sell.small",
    "trade_last5Seconds.tradeCount.sell.medium",
    "trade_last5Seconds.tradeCount.sell.large",
    "trade_last5Seconds.tradeCount.sell.big",
    "trade_last5Seconds.tradeCount.sell.super",
    "trade_last5Seconds.tradeCount.bot",
    # Kauppatiedot - 10s
    "trade_last10Seconds.volume.buy",
    "trade_last10Seconds.volume.sell",
    "trade_last10Seconds.volume.bot",
    "trade_last10Seconds.tradeCount.buy.small",
    "trade_last10Seconds.tradeCount.buy.medium",
    "trade_last10Seconds.tradeCount.buy.large",
    "trade_last10Seconds.tradeCount.buy.big",
    "trade_last10Seconds.tradeCount.buy.super",
    "trade_last10Seconds.tradeCount.sell.small",
    "trade_last10Seconds.tradeCount.sell.medium",
    "trade_last10Seconds.tradeCount.sell.large",
    "trade_last10Seconds.tradeCount.sell.big",
    "trade_last10Seconds.tradeCount.sell.super",
    "trade_last10Seconds.tradeCount.bot",
    # Metatiedot
    "poolAddress",
    "timeFromStart",
]

# Luo mappaus snake_case- ja camelCase-kenttien välillä
def create_field_name_mapping():
    """Luo mappauksen snake_case- ja camelCase-kenttien välillä kauppatiedoille"""
    mappings = {}
    
    # Luo mappaus kauppatietojen trade_lastXSeconds ja tradeLastXSeconds välillä
    for field in REQUIRED_FIELDS:
        # Jos kenttä alkaa "trade_last" ja sisältää alaviivan
        if field.startswith("trade_last") and "_" in field:
            # Luo camelCase versio
            camel_field = field.replace("trade_last", "tradeLast").replace("_", "")
            mappings[field] = camel_field
            mappings[camel_field] = field
    
    # Lisätään myös tradeLast5Seconds ja tradeLast10Seconds mappaukset,
    # vaikka ne eivät ole enää required_fields listalla
    # Tämä auttaa is_parent_field_of_required_field-funktiota
    mappings["trade_last5Seconds"] = "tradeLast5Seconds"
    mappings["tradeLast5Seconds"] = "trade_last5Seconds"
    mappings["trade_last10Seconds"] = "tradeLast10Seconds"
    mappings["tradeLast10Seconds"] = "trade_last10Seconds"
    
    # Lisää myös creationTime ja originalTimestamp
    mappings["creationTime"] = "originalTimestamp"
    mappings["originalTimestamp"] = "creationTime"
    
    return mappings

# Luo kenttänimimappaus
FIELD_NAME_MAPPING = create_field_name_mapping()

def is_snake_case(field_name):
    """Tarkistaa, onko kenttänimi alaviiva-muotoinen (snake_case)"""
    return '_' in field_name


def is_camel_case(field_name):
    """Tarkistaa, onko kenttänimi camelCase-muotoinen"""
    if '_' in field_name:  # Jos on alaviivoja, ei ole camelCase
        return False
    # CamelCase-nimessä on yleensä pieniä ja isoja kirjaimia
    return bool(re.match(r'^[a-z]+[A-Za-z0-9]*[A-Z]+[A-Za-z0-9]*$', field_name))


def field_exists(field, pool_fields):
    """
    Tarkistaa, löytyykö kenttä poolin kentistä.
    Jos kenttä ei löydy suoraan, tarkistaa myös vaihtoehtoisen nimeämiskäytännön.
    """
    if field in pool_fields:
        return True
    
    # Tarkista, onko kentälle vaihtoehtoinen nimimuoto
    alt_field = FIELD_NAME_MAPPING.get(field)
    if alt_field and alt_field in pool_fields:
        return True
    
    return False


def get_missing_fields(required_fields, pool_fields):
    """
    Tarkistaa mitä vaadittuja kenttiä puuttuu, huomioiden eri nimeämiskäytännöt.
    Palauttaa listan puuttuvista kentistä.
    """
    missing = []
    for field in required_fields:
        if not field_exists(field, pool_fields):
            missing.append(field)
    return missing


def is_parent_field_of_required_field(field, required_fields):
    """
    Tarkistaa, onko kenttä jonkin vaaditun alakentän pääkenttä.
    Esim. "tradeLast5Seconds" on pääkenttä kentälle "trade_last5Seconds.volume.buy".
    Funktio huomioi eri nimeämiskäytännöt (snake_case vs camelCase).
    """
    # Debug-tulostuksia vain kiinnostaville kentille
    interesting_fields = ["trade_last5Seconds", "trade_last10Seconds"]
    debug_mode = field in interesting_fields
    
    if debug_mode:
        print(f"Tarkistetaan onko '{field}' pääkenttä...")
    
    # Käsitellään erityistapaukset - yksinkertaistettu mappaus
    special_case_mappings = {
        "tradeLast5Seconds": "trade_last5Seconds",
        "trade_last5Seconds": "tradeLast5Seconds",
        "tradeLast10Seconds": "trade_last10Seconds",
        "trade_last10Seconds": "tradeLast10Seconds"
    }
    
    # Tarkista onko kenttä jonkin vaaditun kentän pääkenttä
    for required in required_fields:
        # Jos vaadittu kenttä alkaa tällä kentällä ja sitä seuraa piste
        prefix = field + "."
        if required.startswith(prefix):
            if debug_mode:
                print(f"  - LÖYDETTY: '{field}' on pääkenttä kentälle '{required}'")
            return True
    
    # Tarkista erityistapausmappausista, onko jokin vastaava pääkenttä
    if field in special_case_mappings:
        alt_field = special_case_mappings[field]
        if debug_mode:
            print(f"  - Tarkistetaan erityismappauksen kautta: '{alt_field}'")
        
        for required in required_fields:
            prefix = alt_field + "."
            if required.startswith(prefix):
                if debug_mode:
                    print(f"  - LÖYDETTY ERITYISMAPPAUS: '{field}' on pääkenttä kentälle '{required}'")
                return True
    
    # Tarkista myös tavalliset vaihtoehtoiset nimimuodot
    alt_field = FIELD_NAME_MAPPING.get(field)
    if alt_field:
        if debug_mode:
            print(f"  - Tarkistetaan vaihtoehtoista muotoa '{alt_field}'")
        for required in required_fields:
            prefix = alt_field + "."
            if required.startswith(prefix):
                if debug_mode:
                    print(f"  - LÖYDETTY VAIHTOEHTOINEN: '{alt_field}' on pääkenttä kentälle '{required}'")
                return True
    
    if debug_mode:
        print(f"  - '{field}' EI OLE pääkenttä millekään vaaditulle kentälle")
    return False


def main():
    """Analysoi kaikki poolit vaadittujen kenttien ja rivienmäärän osalta"""
    # Alusta Firebase
    logger.info("Alustetaan Firebase-yhteys...")
    firebase_service = FirebaseService()
    db = firebase_service.db

    if not db:
        logger.error("Firebase-yhteyden muodostaminen epäonnistui. Lopetetaan.")
        return

    # Luo aikaleima alikansiolle
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Luo output-hakemisto ja aikaleimalla nimetty alikansio
    output_dir = os.path.join(project_root, "outputs", f"pool_analysis_{timestamp}")
    os.makedirs(output_dir, exist_ok=True)
    logger.info(f"Tulokset tallennetaan hakemistoon: {output_dir}")

    # Hae kaikki pool ID:t ilman rajoitusta
    logger.info("Haetaan kaikki pool ID:t...")
    pool_ids = get_pool_ids(db)  # Poistettu rajoitus - haetaan kaikki poolit
    logger.info(f"Löydettiin {len(pool_ids)} poolia analysoitavaksi")

    # Luo analyysituloksille tietorakenteet
    valid_pools = []           # Poolit, joissa on kaikki vaaditut kentät
    invalid_pools = []         # Poolit, joissa on puutteita vaadittuissa kentissä
    invalid_pools_details = [] # Tarkemmat tiedot puutteellisista pooleista
    pools_600_rows = []        # Poolit, joissa on väh. 600 riviä
    pools_1100_rows = []       # Poolit, joissa on väh. 1100 riviä
    valid_and_5_rows = []      # Poolit, joissa on kaikki vaaditut kentät JA väh. 5 riviä
    valid_and_600_rows = []    # Poolit, joissa on kaikki vaaditut kentät JA väh. 600 riviä
    valid_and_1100_rows = []   # Poolit, joissa on kaikki vaaditut kentät JA väh. 1100 riviä
    missing_fields_count = Counter()  # Laskuri puuttuville kentille
    extra_fields_count = Counter()    # Laskuri ylimääräisille kentille
    all_pools_info = []        # Kaikki tiedot jokaisen poolin analyysista
    all_fields_seen = set()    # Kaikki olemassaolevat kentät
    
    # Nimeämiskäytäntöjen analyysiä varten
    pools_with_snake_case = set()  # Poolit, joissa on alaviiva-muotoisia kenttiä
    pools_with_camel_case = set()  # Poolit, joissa on camelCase-muotoisia kenttiä
    snake_case_fields = set()      # Löydetyt alaviiva-muotoiset kentät
    camel_case_fields = set()      # Löydetyt camelCase-muotoiset kentät
    snake_case_field_counts = Counter()  # Laskuri alaviiva-kentille
    camel_case_field_counts = Counter()  # Laskuri camelCase-kentille

    # Analyysin tilastot
    total_analyzed = 0
    total_valid = 0
    total_600_rows = 0
    total_1100_rows = 0
    total_valid_and_5 = 0
    total_valid_and_600 = 0
    total_valid_and_1100 = 0
    pool_data_counts = []      # Jokaisen poolin rivimäärät
    
    # Mappauksella korjattujen kenttien laskuri
    fields_found_with_mapping = Counter()
    pools_helped_by_mapping = 0
    originally_valid_pools = 0
    mapping_improved_valid_pools = 0

    logger.info(f"Tarkistetaan {len(pool_ids)} poolia {len(REQUIRED_FIELDS)} vaadittujen kenttien osalta...")

    # Käy läpi kaikki poolit
    for i, pool_id in enumerate(pool_ids):
        if i % 100 == 0:  # Raportoi edistymisestä 100 poolin välein
            logger.info(f"Analysoidaan poolia {i+1}/{len(pool_ids)}: {pool_id}")
        elif i % 10 == 0:  # Pienempi raportointi 10 poolin välein
            print(f"Käsitellään: {i+1}/{len(pool_ids)}", end="\r")

        # Hae data tälle poolille (käytä pientä limit_per_pool-arvoa, koska riittää nähdä kentät)
        # Mutta aseta datapisteiden vähimmäismäärä pieneksi, jotta myös vähäisen datan poolit analysoidaan
        pool_data = firebase_service.fetch_market_data(
            min_data_points=1, max_pools=1, limit_per_pool=1500, pool_address=pool_id
        ).get(pool_id)

        # Tilastoi vain jos saamme jonkinlaista dataa
        if pool_data is not None and not pool_data.empty:
            total_analyzed += 1
            row_count = len(pool_data)
            pool_data_counts.append(row_count)
            
            # Tarkista rivienmäärät
            has_600_rows = row_count >= 600
            has_1100_rows = row_count >= 1100
            has_5_rows = row_count >= 5
            
            if has_600_rows:
                total_600_rows += 1
                pools_600_rows.append(pool_id)
            
            if has_1100_rows:
                total_1100_rows += 1
                pools_1100_rows.append(pool_id)

            # Tarkista onko kaikki vaaditut kentät
            pool_fields = set(pool_data.columns)
            
            # Päivitä kaikkien nähtyjen kenttien lista
            all_fields_seen.update(pool_fields)
            
            # Tarkista ensin ilman kenttämappausta (alkuperäinen tapa)
            original_missing_fields = set(REQUIRED_FIELDS) - pool_fields
            pool_has_all_fields_originally = len(original_missing_fields) == 0
            
            if pool_has_all_fields_originally:
                originally_valid_pools += 1
            
            # Tunnista puuttuvat kentät käyttäen kenttämappausta
            missing_fields = get_missing_fields(REQUIRED_FIELDS, pool_fields)
            
            # Vertaa alkuperäisiä puuttuvia kenttiä niihin, jotka puuttuvat mappauksen jälkeen
            fields_fixed_by_mapping = list(set(original_missing_fields) - set(missing_fields))
            for field in fields_fixed_by_mapping:
                fields_found_with_mapping[field] += 1
            
            if fields_fixed_by_mapping and len(missing_fields) == 0:
                pools_helped_by_mapping += 1
            
            # Tunnista ylimääräiset kentät (ne jotka eivät ole vaadittuja)
            # Huomioi myös että vaihtoehtoisesti nimetyt (mutta vaaditut) kentät eivät ole "ylimääräisiä"
            # Ja pääkentät vaadittujen alakenttien osalta eivät myöskään ole "ylimääräisiä"
            extra_fields = set()
            for field in pool_fields:
                # Jos kenttä ei ole vaadittu JA kenttä ei ole vaihtoehtoinen nimi millekään vaaditulle kentälle
                # JA kenttä ei ole pääkenttä millekään vaaditulle kentälle
                if (field not in REQUIRED_FIELDS and 
                    field not in FIELD_NAME_MAPPING and 
                    not is_parent_field_of_required_field(field, REQUIRED_FIELDS)):
                    extra_fields.add(field)
            
            # Kerää tilastot puuttuvista kentistä
            for field in missing_fields:
                missing_fields_count[field] += 1
                
            # Kerää tilastot ylimääräisistä kentistä
            for field in extra_fields:
                extra_fields_count[field] += 1
                
            # Kerää tilastot nimeämiskäytännöistä
            has_snake_case = False
            has_camel_case = False
            
            for field in pool_fields:
                if is_snake_case(field):
                    has_snake_case = True
                    snake_case_fields.add(field)
                    snake_case_field_counts[field] += 1
                elif is_camel_case(field):
                    has_camel_case = True
                    camel_case_fields.add(field)
                    camel_case_field_counts[field] += 1
            
            if has_snake_case:
                pools_with_snake_case.add(pool_id)
            if has_camel_case:
                pools_with_camel_case.add(pool_id)

            if not missing_fields:
                # Poolissa on kaikki vaaditut kentät (huomioiden mappaukset)
                total_valid += 1
                valid_pools.append(pool_id)
                
                # Tarkista myös rivivaatimukset
                if has_5_rows:
                    total_valid_and_5 += 1
                    valid_and_5_rows.append(pool_id)
                
                if has_600_rows:
                    total_valid_and_600 += 1
                    valid_and_600_rows.append(pool_id)
                
                if has_1100_rows:
                    total_valid_and_1100 += 1
                    valid_and_1100_rows.append(pool_id)
                
                pool_info = {
                    "pool_id": pool_id,
                    "record_count": row_count,
                    "field_count": len(pool_fields),
                    "has_all_required_fields": True,
                    "has_600_rows": has_600_rows,
                    "has_1100_rows": has_1100_rows,
                    "extra_fields_count": len(extra_fields),
                    "extra_fields": list(extra_fields) if extra_fields else [],
                    "has_snake_case_fields": has_snake_case,
                    "has_camel_case_fields": has_camel_case,
                    "fields_fixed_by_mapping": fields_fixed_by_mapping
                }
            else:
                # Poolista puuttuu joitain kenttiä
                invalid_pools.append(pool_id)
                
                # Tallenna tarkemmat tiedot puutteellisesta poolista
                invalid_pool_detail = {
                    "pool_id": pool_id,
                    "record_count": row_count,
                    "missing_fields": list(missing_fields)
                }
                invalid_pools_details.append(invalid_pool_detail)
                
                pool_info = {
                    "pool_id": pool_id,
                    "record_count": row_count,
                    "field_count": len(pool_fields),
                    "has_all_required_fields": False,
                    "has_600_rows": has_600_rows,
                    "has_1100_rows": has_1100_rows,
                    "missing_field_count": len(missing_fields),
                    "missing_fields": list(missing_fields),
                    "extra_fields_count": len(extra_fields),
                    "extra_fields": list(extra_fields) if extra_fields else [],
                    "has_snake_case_fields": has_snake_case,
                    "has_camel_case_fields": has_camel_case,
                    "fields_fixed_by_mapping": fields_fixed_by_mapping
                }
            
            all_pools_info.append(pool_info)

        elif pool_data is None:
            logger.info(f"✗ Pooli {pool_id} - Dataa ei voitu hakea")
        else:
            logger.info(f"✗ Pooli {pool_id} - Ei dataa (tyhjä DataFrame)")

    # Laske kuinka paljon mappaus paransi validien poolien määrää
    mapping_improved_valid_pools = total_valid - originally_valid_pools

    # Tallenna tulokset (huomaa että timestamp on jo määritetty funktion alussa)
    valid_pools_file = os.path.join(output_dir, "valid_pools.json")
    with open(valid_pools_file, "w") as f:
        json.dump(valid_pools, f, indent=2)
        
    # Tallenna puutteelliset poolit
    invalid_pools_file = os.path.join(output_dir, "invalid_pools.json")
    with open(invalid_pools_file, "w") as f:
        json.dump(invalid_pools, f, indent=2)
        
    # Tallenna tarkemmat tiedot puutteellisista pooleista
    invalid_pools_details_file = os.path.join(output_dir, "invalid_pools_details.json")
    with open(invalid_pools_details_file, "w") as f:
        json.dump(invalid_pools_details, f, indent=2)

    pools_600_file = os.path.join(output_dir, "pools_600_rows.json") 
    with open(pools_600_file, "w") as f:
        json.dump(pools_600_rows, f, indent=2)
        
    pools_1100_file = os.path.join(output_dir, "pools_1100_rows.json")
    with open(pools_1100_file, "w") as f:
        json.dump(pools_1100_rows, f, indent=2)
        
    # Tallenna yhdistelmätulokset
    valid_and_5_file = os.path.join(output_dir, "valid_and_5_rows.json")
    with open(valid_and_5_file, "w") as f:
        json.dump(valid_and_5_rows, f, indent=2)
        
    valid_and_600_file = os.path.join(output_dir, "valid_and_600_rows.json")
    with open(valid_and_600_file, "w") as f:
        json.dump(valid_and_600_rows, f, indent=2)
        
    valid_and_1100_file = os.path.join(output_dir, "valid_and_1100_rows.json")
    with open(valid_and_1100_file, "w") as f:
        json.dump(valid_and_1100_rows, f, indent=2)
        
    all_pools_file = os.path.join(output_dir, "all_pools_analysis.json")
    with open(all_pools_file, "w") as f:
        json.dump(all_pools_info, f, indent=2)

    # Laske statistiikka puuttuvista kentistä
    total_missing_fields = len(missing_fields_count)
    most_common_missing = missing_fields_count.most_common(20)  # Top 20 puuttuvaa kenttää
    
    missing_fields_file = os.path.join(output_dir, "missing_fields_analysis.json")
    with open(missing_fields_file, "w") as f:
        json.dump({
            "missing_fields_count": dict(missing_fields_count),
            "most_common_missing": [{"field": field, "count": count} for field, count in most_common_missing]
        }, f, indent=2)
        
    # Laske statistiikka ylimääräisistä kentistä
    total_extra_fields = len(extra_fields_count)
    most_common_extra = extra_fields_count.most_common()  # Kaikki ylimääräiset kentät
    
    extra_fields_file = os.path.join(output_dir, "extra_fields_analysis.json")
    with open(extra_fields_file, "w") as f:
        json.dump({
            "extra_fields_count": dict(extra_fields_count),
            "most_common_extra": [{"field": field, "count": count} for field, count in most_common_extra],
            "total_unique_fields": len(all_fields_seen),
            "all_fields": list(all_fields_seen)
        }, f, indent=2)
        
    # Tallenna nimeämiskäytäntöjen analyysi
    naming_analysis_file = os.path.join(output_dir, "naming_convention_analysis.json")
    with open(naming_analysis_file, "w") as f:
        json.dump({
            "pools_with_snake_case": len(pools_with_snake_case),
            "pools_with_camel_case": len(pools_with_camel_case),
            "pools_with_both": len(pools_with_snake_case.intersection(pools_with_camel_case)),
            "pools_with_only_snake_case": len(pools_with_snake_case - pools_with_camel_case),
            "pools_with_only_camel_case": len(pools_with_camel_case - pools_with_snake_case),
            "unique_snake_case_fields": len(snake_case_fields),
            "unique_camel_case_fields": len(camel_case_fields),
            "snake_case_field_counts": dict(snake_case_field_counts.most_common()),
            "camel_case_field_counts": dict(camel_case_field_counts.most_common())
        }, f, indent=2)
        
    # Tallenna kenttämappauksen analyysi
    mapping_analysis_file = os.path.join(output_dir, "field_mapping_analysis.json")
    with open(mapping_analysis_file, "w") as f:
        json.dump({
            "fields_found_with_mapping": dict(fields_found_with_mapping),
            "pools_helped_by_mapping": pools_helped_by_mapping,
            "originally_valid_pools": originally_valid_pools,
            "mapping_improved_valid_pools": mapping_improved_valid_pools
        }, f, indent=2)

    # Tulosta yhteenveto
    print("\n" + "=" * 80)
    print("POOLIEN ANALYYSIN TULOKSET")
    print("=" * 80)
    print(f"Analysoituja pooleja: {total_analyzed} / {len(pool_ids)}")
    print(f"Pooleja, joissa on kaikki {len(REQUIRED_FIELDS)} vaadittua kenttää (alkuperäinen): {originally_valid_pools} ({originally_valid_pools/total_analyzed*100:.1f}%)")
    print(f"Pooleja, joissa on kaikki {len(REQUIRED_FIELDS)} vaadittua kenttää (nimimappauksen kanssa): {total_valid} ({total_valid/total_analyzed*100:.1f}%)")
    print(f"Nimimappauksen parantamia pooleja: {mapping_improved_valid_pools} ({mapping_improved_valid_pools/total_analyzed*100:.1f}%)")
    print(f"Pooleja, joissa on vähintään 600 riviä (~10 min dataa): {total_600_rows} ({total_600_rows/total_analyzed*100:.1f}%)")
    print(f"Pooleja, joissa on vähintään 1100 riviä (~18 min dataa): {total_1100_rows} ({total_1100_rows/total_analyzed*100:.1f}%)")
    print(f"Pooleja, joissa on kaikki vaaditut kentät JA väh. 5 riviä: {total_valid_and_5} ({total_valid_and_5/total_analyzed*100:.1f}%)")
    print(f"Pooleja, joissa on kaikki vaaditut kentät JA väh. 600 riviä: {total_valid_and_600} ({total_valid_and_600/total_analyzed*100:.1f}%)")
    print(f"Pooleja, joissa on kaikki vaaditut kentät JA väh. 1100 riviä: {total_valid_and_1100} ({total_valid_and_1100/total_analyzed*100:.1f}%)")
    
    if pool_data_counts:
        avg_rows = sum(pool_data_counts) / len(pool_data_counts)
        min_rows = min(pool_data_counts)
        max_rows = max(pool_data_counts)
        print(f"\nRivimäärätilastot:")
        print(f"Keskimäärin: {avg_rows:.1f} riviä per pooli")
        print(f"Minimi: {min_rows} riviä")
        print(f"Maksimi: {max_rows} riviä")
    
    # Tulosta puuttuvat kentät
    print("\nYleisimmät puuttuvat kentät:")
    for field, count in most_common_missing[:10]:
        print(f"- {field}: puuttuu {count} poolista ({count/total_analyzed*100:.1f}%)")
        
    # Tulosta ylimääräiset kentät
    print("\nYleisimmät ylimääräiset kentät (eivät vaadittuja):")
    for field, count in most_common_extra[:10]:
        print(f"- {field}: löytyy {count} poolista ({count/total_analyzed*100:.1f}%)")
    
    print(f"\nLöydetty yhteensä {len(all_fields_seen)} uniikkia kenttää, joista {len(REQUIRED_FIELDS)} on vaadittuja ja {total_extra_fields} ylimääräisiä")
    
    # Tulosta nimeämiskäytäntöjen analyysi
    snake_case_pool_count = len(pools_with_snake_case)
    camel_case_pool_count = len(pools_with_camel_case)
    both_styles_pool_count = len(pools_with_snake_case.intersection(pools_with_camel_case))
    only_snake_case_pool_count = len(pools_with_snake_case - pools_with_camel_case)
    only_camel_case_pool_count = len(pools_with_camel_case - pools_with_snake_case)
    
    print("\nNimeämiskäytännöt:")
    print(f"- Pooleja, joissa on alaviiva-muotoisia (snake_case) kenttiä: {snake_case_pool_count} ({snake_case_pool_count/total_analyzed*100:.1f}%)")
    print(f"- Pooleja, joissa on camelCase-muotoisia kenttiä: {camel_case_pool_count} ({camel_case_pool_count/total_analyzed*100:.1f}%)")
    print(f"- Pooleja, joissa on molempia nimeämiskäytäntöjä: {both_styles_pool_count} ({both_styles_pool_count/total_analyzed*100:.1f}%)")
    print(f"- Pooleja, joissa on vain alaviiva-muotoisia kenttiä: {only_snake_case_pool_count} ({only_snake_case_pool_count/total_analyzed*100:.1f}%)")
    print(f"- Pooleja, joissa on vain camelCase-muotoisia kenttiä: {only_camel_case_pool_count} ({only_camel_case_pool_count/total_analyzed*100:.1f}%)")
    print(f"- Uniikkeja alaviiva-muotoisia kenttiä: {len(snake_case_fields)}")
    print(f"- Uniikkeja camelCase-muotoisia kenttiä: {len(camel_case_fields)}")
    
    # Tulosta kenttämappauksen analyysi
    print("\nKenttämappauksen vaikutus:")
    print(f"- Pooleja joita kenttämappaus auttoi: {pools_helped_by_mapping} ({pools_helped_by_mapping/total_analyzed*100:.1f}%)")
    print(f"- Alun perin valideja pooleja: {originally_valid_pools} ({originally_valid_pools/total_analyzed*100:.1f}%)")
    print(f"- Mappauksen jälkeen valideja pooleja: {total_valid} ({total_valid/total_analyzed*100:.1f}%)")
    print(f"- Mappauksen ansiosta uusia valideja pooleja: {mapping_improved_valid_pools} (+{mapping_improved_valid_pools/originally_valid_pools*100:.1f}%)")
    
    # Tulosta yleisimmät kentät jotka löytyivät mappauksen avulla
    print("\nYleisimmät kentät jotka löytyivät vaihtoehtoisilla nimillä:")
    for field, count in fields_found_with_mapping.most_common(10):
        print(f"- {field} → {FIELD_NAME_MAPPING.get(field)}: löytyi {count} poolista")
    
    print("\nTulokset tallennettu tiedostoihin:")
    print(f"- {valid_pools_file} ({len(valid_pools)} poolia)")
    print(f"- {invalid_pools_file} ({len(invalid_pools)} poolia)")
    print(f"- {invalid_pools_details_file} ({len(invalid_pools_details)} poolin tarkemmat tiedot)")
    print(f"- {pools_600_file} ({len(pools_600_rows)} poolia)")
    print(f"- {pools_1100_file} ({len(pools_1100_rows)} poolia)")
    print(f"- {valid_and_5_file} ({len(valid_and_5_rows)} poolia)")
    print(f"- {valid_and_600_file} ({len(valid_and_600_rows)} poolia)")
    print(f"- {valid_and_1100_file} ({len(valid_and_1100_rows)} poolia)")
    print(f"- {all_pools_file}")
    print(f"- {missing_fields_file}")
    print(f"- {extra_fields_file}")
    print(f"- {naming_analysis_file}")
    print(f"- {mapping_analysis_file}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nAnalyysi keskeytetty käyttäjän toimesta.")
    except Exception as e:
        logger.exception(f"Virhe analyysissä: {str(e)}") 