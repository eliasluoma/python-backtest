import sys
import os
import pandas as pd
from src.data.firebase_service import FirebaseService

# Alustetaan Firebase-palvelu
firebase = FirebaseService()

# Haetaan kahden määritetyn poolin tiedot
pool1_id = '2vpAeyJCX7Wi93cXLuSaZYZb78JGSCjYML345jW3DUN2'
pool2_id = '12H7zN3gXRfUeu2fCQjooPAofbBL2wz7X7wKoS44oJkX'

print(f"Haetaan dataa poolille {pool1_id}")
pool1_data = firebase.fetch_pool_data(pool1_id)
print(f"Haetaan dataa poolille {pool2_id}")
pool2_data = firebase.fetch_pool_data(pool2_id)

# Tarkistetaan, että poolista on dataa
if pool1_data.empty:
    print(f'Poolista {pool1_id} ei löytynyt dataa')
    
if pool2_data.empty:
    print(f'Poolista {pool2_id} ei löytynyt dataa')
    
if pool1_data.empty or pool2_data.empty:
    sys.exit(1)

# Verrataan nimeämiskäytäntöjä näyttämällä sarakkeet
print('\nPool 1 (2vpAeyJCX7Wi93cXLuSaZYZb78JGSCjYML345jW3DUN2) sarakkeet:')
print(sorted(pool1_data.columns.tolist()))

print('\nPool 2 (12H7zN3gXRfUeu2fCQjooPAofbBL2wz7X7wKoS44oJkX) sarakkeet:')
print(sorted(pool2_data.columns.tolist()))

# Näytetään esimerkkirivi molemmista
print('\nPool 1 rivi 20 (tai sen lähellä oleva rivi):')
try:
    row_index = min(20, len(pool1_data) - 1)
    print(dict(pool1_data.iloc[row_index]))
except Exception as e:
    print(f'Virhe ensimmäisen poolin rivin näyttämisessä: {e}')

print('\nPool 2 rivi 20 (tai sen lähellä oleva rivi):')
try:
    row_index = min(20, len(pool2_data) - 1)
    print(dict(pool2_data.iloc[row_index]))
except Exception as e:
    print(f'Virhe toisen poolin rivin näyttämisessä: {e}')

# Tutki kenttien nimeämiseroja
pool1_fields = set(pool1_data.columns)
pool2_fields = set(pool2_data.columns)

print("\nVertailuyhteenveto:")
print(f"Pool 1 kenttiä: {len(pool1_fields)}")
print(f"Pool 2 kenttiä: {len(pool2_fields)}")

# Yhteiset ja erilaiset kentät
common_fields = pool1_fields.intersection(pool2_fields)
pool1_unique = pool1_fields - pool2_fields
pool2_unique = pool2_fields - pool1_fields

print(f"\nYhteisiä kenttiä: {len(common_fields)}")
print(f"Vain Pool 1:ssä olevia kenttiä: {len(pool1_unique)}")
if pool1_unique:
    print(f"  {sorted(list(pool1_unique))}")
    
print(f"Vain Pool 2:ssa olevia kenttiä: {len(pool2_unique)}")
if pool2_unique:
    print(f"  {sorted(list(pool2_unique))}")

# Tutki nimeämiskäytäntöjä (camelCase vs snake_case)
def count_naming_style(fields):
    snake_case = 0
    camel_case = 0
    other = 0
    
    for field in fields:
        if '_' in field:
            snake_case += 1
        elif field[0].islower() and any(c.isupper() for c in field):
            camel_case += 1
        else:
            other += 1
            
    return {'snake_case': snake_case, 'camel_case': camel_case, 'other': other}

pool1_styles = count_naming_style(pool1_fields)
pool2_styles = count_naming_style(pool2_fields)

print("\nNimeämiskäytännöt:")
print(f"Pool 1: snake_case: {pool1_styles['snake_case']}, camelCase: {pool1_styles['camel_case']}, muut: {pool1_styles['other']}")
print(f"Pool 2: snake_case: {pool2_styles['snake_case']}, camelCase: {pool2_styles['camel_case']}, muut: {pool2_styles['other']}") 