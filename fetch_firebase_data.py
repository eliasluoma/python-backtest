import os
import pandas as pd
import firebase_admin
from firebase_admin import credentials, firestore
import pytz
from datetime import datetime
from dotenv import load_dotenv
import json

# Lataa ympäristömuuttujat
load_dotenv(".env.local")


def initialize_firebase():
    """Alusta Firebase"""
    if not firebase_admin._apps:
        try:
            # Käytä JSON-tiedostoa
            cred = credentials.Certificate(
                "botti-e5402-firebase-adminsdk-fbsvc-50b6327605.json"
            )
            firebase_admin.initialize_app(cred)
            return firestore.client()
        except Exception as e:
            print(f"Virhe Firebase-yhteyden alustuksessa: {str(e)}")
            print("Varmista että service account JSON-tiedosto on olemassa")
            raise


def load_market_contexts_to_csv(output_file="pool_data.csv", use_cache=True):
    """Lataa kaikki markkinadata Firebasesta CSV-tiedostoon"""

    # Kokeile ladata välimuistista ensin
    if use_cache and os.path.exists(output_file):
        try:
            print(f"\nLöydettiin välimuistitiedosto: {os.path.abspath(output_file)}")
            print("Ladataan data välimuistista...")
            df = pd.read_csv(output_file)

            # Muunna timestamp takaisin datetime-muotoon
            df["timestamp"] = pd.to_datetime(df["timestamp"], format="mixed", utc=True)

            print(f"Ladattu {len(df)} datapistettä välimuistista")
            print(f"Esimerkki datasta:\n{df.head()}\n")
            return df
        except Exception as e:
            print(f"\nVirhe välimuistin lataamisessa: {str(e)}")
            print("Jatketaan datan hakemisella Firebasesta")
    else:
        print(f"\nVälimuistitiedostoa ei löytynyt: {os.path.abspath(output_file)}")
        print("Haetaan data Firebasesta...")

    print("\nYhdistetään Firebaseen...")
    db = initialize_firebase()

    print("Haetaan poolien dataa...")
    market_contexts_ref = db.collection("marketContext")

    all_pool_data = []
    pool_docs = list(market_contexts_ref.list_documents())
    total_pools = len(pool_docs)
    print(f"Löydettiin {total_pools} poolia")

    for idx, pool_doc in enumerate(pool_docs, 1):
        try:
            # Hae poolin kaikki markkinakontekstit
            contexts = pool_doc.collection("marketContexts").order_by("timestamp").get()

            pool_contexts = []
            for context in contexts:
                data = context.to_dict()

                # Muunna string-numerot floateiksi
                for key in [
                    "marketCap",
                    "athMarketCap",
                    "minMarketCap",
                    "maMarketCap10s",
                    "maMarketCap30s",
                    "maMarketCap60s",
                    "marketCapChange5s",
                    "marketCapChange10s",
                    "marketCapChange30s",
                    "marketCapChange60s",
                    "priceChangeFromStart",
                ]:
                    if key in data and isinstance(data[key], str):
                        try:
                            data[key] = float(data[key])
                        except (ValueError, TypeError):
                            data[key] = 0.0

                # Muunna timestamp
                if "timestamp" in data:
                    try:
                        if isinstance(data["timestamp"], (int, float)):
                            data["timestamp"] = datetime.fromtimestamp(
                                data["timestamp"] / 1000
                            ).replace(tzinfo=pytz.UTC)
                        elif isinstance(data["timestamp"], str):
                            data["timestamp"] = datetime.fromtimestamp(
                                float(data["timestamp"]) / 1000
                            ).replace(tzinfo=pytz.UTC)
                    except Exception:
                        # Ohita virheelliset timestampit
                        continue

                data["poolAddress"] = pool_doc.id
                pool_contexts.append(data)

            if pool_contexts:
                df = pd.DataFrame(pool_contexts)
                all_pool_data.append(df)
                print(
                    f"[{idx}/{total_pools}] Ladattu {len(pool_contexts)} datapistettä poolille {pool_doc.id}"
                )

        except Exception as e:
            print(f"Virhe poolin {pool_doc.id} käsittelyssä: {str(e)}")
            continue

    if not all_pool_data:
        raise ValueError("Ei löydetty dataa Firebasesta")

    # Yhdistä kaikki data
    final_df = pd.concat(all_pool_data, ignore_index=True)
    print(
        f"\nYhteensä ladattu {len(final_df)} datapistettä {len(all_pool_data)} poolista"
    )

    # Tallenna CSV:ksi
    print(f"\nTallennetaan data tiedostoon {output_file}...")

    # Varmista että timestamp on yhtenäisessä muodossa ennen tallennusta
    final_df["timestamp"] = final_df["timestamp"].dt.strftime("%Y-%m-%d %H:%M:%S%z")
    final_df.to_csv(output_file, index=False)

    print(
        f"Data tallennettu! Tiedoston koko: {os.path.getsize(output_file) / (1024*1024):.2f} MB"
    )

    # Tulostetaan statistiikkaa
    print("\nDatasetin statistiikka:")
    print(f"Rivejä yhteensä: {len(final_df)}")
    print(f"Uniikkeja pooleja: {final_df['poolAddress'].nunique()}")
    print("\nDatapisteitä per pooli:")
    pool_counts = final_df["poolAddress"].value_counts()
    print(f"Keskiarvo: {pool_counts.mean():.1f}")
    print(f"Mediaani: {pool_counts.median():.1f}")
    print(f"Minimi: {pool_counts.min()}")
    print(f"Maksimi: {pool_counts.max()}")

    return final_df


if __name__ == "__main__":
    df = load_market_contexts_to_csv()
