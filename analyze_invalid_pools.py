#!/usr/bin/env python3
import json
import sys
from collections import Counter

# Tarkista komentoriviargumentit
if len(sys.argv) != 3:
    print("Käyttö: python3 analyze_invalid_pools.py <all_pools_analysis.json> <valid_pools.json>")
    sys.exit(1)

all_pools_file = sys.argv[1]
valid_pools_file = sys.argv[2]

# Lataa analyysit
with open(all_pools_file, 'r') as f:
    all_pools = json.load(f)

# Lataa validit poolit vertailua varten
with open(valid_pools_file, 'r') as f:
    valid_pool_ids = json.load(f)

# Suodata epävalidit poolit
invalid_pools = [p for p in all_pools if not p["has_all_required_fields"]]

# Tilastoja
print(f"EPÄVALIDEJA POOLEJA: {len(invalid_pools)} / {len(all_pools)}")
print(f"VALIDEJA POOLEJA: {len(valid_pool_ids)} / {len(all_pools)}")

# Puuttuvien kenttien analyysi
missing_fields_counter = Counter()
for pool in invalid_pools:
    for field in pool.get("missing_fields", []):
        missing_fields_counter[field] += 1

print("\nYLEISIMMÄT PUUTTUVAT KENTÄT:")
for field, count in missing_fields_counter.most_common(10):
    print(f"- {field}: puuttuu {count} poolista ({count/len(invalid_pools)*100:.1f}%)")

# Ylimääräisten kenttien analyysi epävalideissa pooleissa
extra_fields_counter = Counter()
for pool in invalid_pools:
    for field in pool.get("extra_fields", []):
        extra_fields_counter[field] += 1

print("\nYLEISIMMÄT YLIMÄÄRÄISET KENTÄT EPÄVALIDEISSA POOLEISSA:")
for field, count in extra_fields_counter.most_common(10):
    print(f"+ {field}: löytyy {count} poolista ({count/len(invalid_pools)*100:.1f}%)")

# Poolit, joista puuttuu vähiten kenttiä (lähes validit)
print("\nPOOLIT JOISTA PUUTTUU VÄHITEN KENTTIÄ:")
nearly_valid_pools = sorted(invalid_pools, key=lambda p: len(p.get("missing_fields", [])))[:5]

for i, pool in enumerate(nearly_valid_pools):
    missing_count = len(pool.get("missing_fields", []))
    rows = pool.get("record_count", 0)
    print(f"\n{i+1}. Pooli {pool['pool_id']} - puuttuu {missing_count} kenttää, {rows} riviä:")
    for field in pool.get("missing_fields", []):
        print(f"   - {field}")
    
    if "extra_fields" in pool:
        extra_fields = pool["extra_fields"]
        print(f"   Ylimääräisiä kenttiä ({len(extra_fields)}):")
        for field in extra_fields[:5]:
            print(f"   + {field}")
        if len(extra_fields) > 5:
            print(f"   + ...ja {len(extra_fields) - 5} muuta")

# Kentät, jotka korjattiin mappauksen avulla
print("\nKENTÄT JOTKA LÖYTYIVÄT VAIHTOEHTOISILLA NIMILLÄ:")
fixed_by_mapping = Counter()
for pool in all_pools:
    for field in pool.get("fields_fixed_by_mapping", []):
        fixed_by_mapping[field] += 1

for field, count in fixed_by_mapping.most_common(10):
    print(f"* {field}: löytyi vaihtoehtoisella nimellä {count} poolista")

# Ryhmittely puuttuvien kenttien perusteella
print("\nPUUTTUVIEN KENTTIEN RYHMÄT:")
missing_patterns = {}
for pool in invalid_pools:
    pattern = frozenset(pool.get("missing_fields", []))
    if pattern not in missing_patterns:
        missing_patterns[pattern] = []
    missing_patterns[pattern].append(pool["pool_id"])

# Näytä yleisimmät puutteiden ryhmät
sorted_patterns = sorted(missing_patterns.items(), key=lambda x: len(x[1]), reverse=True)
for i, (pattern, pools) in enumerate(sorted_patterns[:3]):
    print(f"\nRyhmä {i+1} - {len(pools)} poolia, puuttuvat kentät:")
    for field in sorted(pattern)[:10]:
        print(f"  - {field}")
    if len(pattern) > 10:
        print(f"  - ...ja {len(pattern) - 10} muuta kenttää")
    print(f"  Esimerkkipoolit: {', '.join(pools[:3])}")
