#!/usr/bin/env python
"""
Hakee rivikohtaista dataa poolista ja tallentaa sen tiedostoon tutkittavaksi.
Skripti hakee sekä puutteellisesta että validista poolista tietoja ja tallentaa
erityisesti rivin 20 kaikki kentät ja niiden arvot tiedostoon helposti tutkittavassa muodossa.
"""

import os
import sys
import json
import logging
from pprint import pprint, pformat
from datetime import datetime

# Aseta absoluuttinen polku juurihakemistolle
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, project_root)

# Aseta logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Tuo Firebase-apuohjelmat
from src.data.firebase_service import FirebaseService
from src.utils.firebase_utils import fetch_market_data_for_pool

def save_to_file(data, filename):
    """Tallentaa annetun datan tiedostoon"""
    with open(filename, 'w', encoding='utf-8') as f:
        f.write(data)
    logger.info(f"Data tallennettu tiedostoon: {filename}")

def main():
    """Hae poolin dataa ja tallenna se tiedostoon tutkittavaksi"""
    # Alusta Firebase
    logger.info("Alustetaan Firebase-yhteys...")
    firebase_service = FirebaseService()
    db = firebase_service.db

    if not db:
        logger.error("Firebase-yhteyden muodostaminen epäonnistui. Lopetetaan.")
        return

    # Poolit, joita tutkitaan
    invalid_pool_id = "13UufXw2zaq4ffE16dcYhsCc6ZdxXXV5EwNq4mRJuT8F"  # Puutteellinen pooli
    valid_pool_id = "12QspooeZFsA4d41KtLz4p3e8YyLzPxG4bShsUCBbEgU"  # Validi pooli

    # Hae dataa molemmista pooleista (min 30 riviä, jos saatavilla)
    logger.info(f"Haetaan dataa puutteellisesta poolista: {invalid_pool_id}")
    invalid_pool_data = fetch_market_data_for_pool(db, invalid_pool_id, limit=30, min_data_points=1)
    
    logger.info(f"Haetaan dataa validista poolista: {valid_pool_id}")
    valid_pool_data = fetch_market_data_for_pool(db, valid_pool_id, limit=30, min_data_points=1)

    # Tarkista että data on saatu
    if invalid_pool_data is None or valid_pool_data is None:
        logger.error("Datan hakeminen epäonnistui.")
        return
    
    # Luo aikaleima tiedostoille
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Valitse rivi 20 (tai viimeinen rivi, jos ei ole 20 riviä)
    invalid_row_index = min(20, len(invalid_pool_data) - 1)
    valid_row_index = min(20, len(valid_pool_data) - 1)
    
    # Hae rivit
    invalid_row = invalid_pool_data.iloc[invalid_row_index]
    valid_row = valid_pool_data.iloc[valid_row_index]
    
    # Muunna rivit sanakirjoiksi
    invalid_row_dict = invalid_row.to_dict()
    valid_row_dict = valid_row.to_dict()
    
    # Tallenna raakadata JSON-tiedostoihin
    invalid_json_file = f"pool_data_{invalid_pool_id}_row_{invalid_row_index}_{timestamp}.json"
    valid_json_file = f"pool_data_{valid_pool_id}_row_{valid_row_index}_{timestamp}.json"
    
    with open(invalid_json_file, 'w') as f:
        json.dump(invalid_row_dict, f, indent=2, default=str)
    
    with open(valid_json_file, 'w') as f:
        json.dump(valid_row_dict, f, indent=2, default=str)
    
    logger.info(f"Puutteellisen poolin data tallennettu: {invalid_json_file}")
    logger.info(f"Validin poolin data tallennettu: {valid_json_file}")
    
    # Luo ihmisystävällinen tekstitiedosto, jossa vertaillaan pooleja
    report_file = f"pool_comparison_{timestamp}.txt"
    report = []
    
    report.append("="*80)
    report.append(f"POOLIN DATAN VERTAILU ({timestamp})")
    report.append("="*80)
    report.append("")
    
    # Puutteellisen poolin tiedot
    report.append(f"PUUTTEELLINEN POOLI: {invalid_pool_id} (rivi {invalid_row_index})")
    report.append("-"*80)
    report.append(f"Rivien määrä: {len(invalid_pool_data)}")
    report.append(f"Kenttien määrä: {len(invalid_row_dict)}")
    report.append("")
    report.append("KENTTIEN NIMET JA ARVOT:")
    report.append("")
    
    # Järjestä kentät aakkosjärjestykseen
    sorted_fields = sorted(invalid_row_dict.keys())
    
    for field in sorted_fields:
        value = invalid_row_dict[field]
        value_type = type(value).__name__
        report.append(f"{field} ({value_type}):")
        
        # Tarkempi tulostus jos kyseessä on sanakirja tai lista
        if isinstance(value, (dict, list)):
            report.append(pformat(value, indent=4))
        else:
            report.append(f"    {value}")
        report.append("")
    
    # Validin poolin tiedot
    report.append("\n" + "="*80)
    report.append(f"VALIDI POOLI: {valid_pool_id} (rivi {valid_row_index})")
    report.append("-"*80)
    report.append(f"Rivien määrä: {len(valid_pool_data)}")
    report.append(f"Kenttien määrä: {len(valid_row_dict)}")
    report.append("")
    report.append("KENTTIEN NIMET JA ARVOT:")
    report.append("")
    
    # Järjestä kentät aakkosjärjestykseen
    sorted_fields = sorted(valid_row_dict.keys())
    
    for field in sorted_fields:
        value = valid_row_dict[field]
        value_type = type(value).__name__
        report.append(f"{field} ({value_type}):")
        
        # Tarkempi tulostus jos kyseessä on sanakirja tai lista
        if isinstance(value, (dict, list)):
            report.append(pformat(value, indent=4))
        else:
            report.append(f"    {value}")
        report.append("")
    
    # Vertailu
    report.append("\n" + "="*80)
    report.append("KENTTIEN VERTAILU")
    report.append("="*80)
    
    invalid_fields = set(invalid_row_dict.keys())
    valid_fields = set(valid_row_dict.keys())
    
    # Kentät, jotka ovat validissa mutta puuttuvat puutteellisesta
    missing_in_invalid = valid_fields - invalid_fields
    report.append(f"\nKentät, jotka ovat validissa poolissa mutta puuttuvat puutteellisesta ({len(missing_in_invalid)}):")
    for field in sorted(missing_in_invalid):
        report.append(f"  - {field}")
    
    # Kentät, jotka ovat puutteellisessa mutta puuttuvat validista
    missing_in_valid = invalid_fields - valid_fields
    report.append(f"\nKentät, jotka ovat puutteellisessa poolissa mutta puuttuvat validista ({len(missing_in_valid)}):")
    for field in sorted(missing_in_valid):
        report.append(f"  - {field}")
    
    # Trade-kenttien vertailu
    report.append("\n" + "="*80)
    report.append("TRADE-KENTTIEN VERTAILU")
    report.append("="*80)
    
    # Puutteellisen poolin trade-kentät
    invalid_trade_fields = [field for field in invalid_fields if "trade" in field.lower()]
    report.append(f"\nPuutteellisen poolin trade-kentät ({len(invalid_trade_fields)}):")
    for field in sorted(invalid_trade_fields):
        value = invalid_row_dict[field]
        report.append(f"  - {field} = {value}")
    
    # Validin poolin trade-kentät
    valid_trade_fields = [field for field in valid_fields if "trade" in field.lower()]
    report.append(f"\nValidin poolin trade-kentät ({len(valid_trade_fields)}):")
    for field in sorted(valid_trade_fields):
        value = valid_row_dict[field]
        if isinstance(value, dict):
            report.append(f"  - {field}:")
            for k, v in value.items():
                report.append(f"    - {k}: {v}")
        else:
            report.append(f"  - {field} = {value}")
    
    # Kirjoita raportti tiedostoon
    with open(report_file, 'w', encoding='utf-8') as f:
        f.write("\n".join(report))
    
    logger.info(f"Vertailuraportti tallennettu: {report_file}")
    print(f"\nVERTAILU VALMIS! Tiedostot luotu:")
    print(f"1. Puutteellisen poolin JSON-data: {invalid_json_file}")
    print(f"2. Validin poolin JSON-data: {valid_json_file}")
    print(f"3. Vertailuraportti: {report_file}")

if __name__ == "__main__":
    main() 