import pandas as pd
from datetime import datetime
from typing import Dict, Optional
import psutil
import os
import time
import json
import sys
import platform
import multiprocessing as mp
from queue import Empty
import argparse
import itertools
import glob
import traceback

# Lisätään edistymisen tallennusta varten


# Määritellään parametrit yhdessä paikassa
def get_parameters():
    """
    Palauttaa parametrit simulaatiota varten.

    Jos parametrille annetaan lista arvoja, niitä käytetään grid-testissä.
    Jos parametrille annetaan vain yksi arvo, sitä käytetään normaalissa ajossa.

    Tämä versio testaa 5 tärkeintä parametria laajalla skaalalla nopeaa karkeaa hakua varten.

    Returns:
        Dict: Parametrit ja niiden arvot
    """
    return {
        # Testataan 5 tärkeintä parametria laajalla skaalalla
        "mc_growth_from_start": [
            50,
            100,
            150,
        ],  # Markkinakapitalisaation kasvu alusta (%)
        "holder_growth_from_start": [30, 60, 90],  # Holderien määrän kasvu alusta
        "holder_delta_30s": [40, 85, 130],  # Holderien määrän muutos 30 sekunnissa
        "mc_change_5s": [4],  # Markkinakapitalisaation muutos 5 sekunnissa (%)
        "buy_volume_5s": [10, 15, 20],  # Ostovolyymi 5 sekunnissa
        "large_buy_5s": [0, 1, 2],  # Suurten ostojen määrä 5 sekunnissa
        # Muut parametrit kiinteinä arvoina
        "net_volume_5s": 4,  # Nettovolyymi 5 sekunnissa
        "price_change": 3.0,  # Hinnanmuutos (%)
        "mc_change_30s": 45,  # Markkinakapitalisaation muutos 30 sekunnissa (%)
        "buy_sell_ratio_10s": 1.3,  # Osto/myynti-suhde 10 sekunnissa
    }


def get_default_parameters():
    """
    Palauttaa oletusparametrit normaaliin ajoon ottamalla ensimmäisen arvon jokaisesta parametrista.

    Returns:
        Dict: Oletusparametrit normaaliin ajoon
    """
    params = get_parameters()
    default_params = {}

    for key, value in params.items():
        if isinstance(value, list):
            # Jos parametrilla on useita arvoja, käytä ensimmäistä
            default_params[key] = value[0]
        else:
            # Jos parametrilla on vain yksi arvo, käytä sitä
            default_params[key] = value

    return default_params


class BuySimulator:
    def __init__(
        self,
        early_mc_limit: float = 400000,  # MC raja alussa
        min_delay: int = 60,  # Minimi viive sekunteina
        max_delay: int = 200,  # Maksimi viive sekunteina
        buy_params: Dict[str, float] = None,
    ):

        self.buy_params = buy_params if buy_params is not None else get_default_parameters()
        self.early_mc_limit = early_mc_limit
        self.min_delay = min_delay
        self.max_delay = max_delay

    def check_buy_conditions(
        self,
        metrics: Dict[str, float],
        initial_metrics: Dict[str, float],
        pool_data: pd.DataFrame,
    ) -> bool:
        """Tarkista täyttyvätkö ostoehdot"""

        try:
            # Tarkista MC raja 10 sekunnin kohdalla
            if len(pool_data) > 10:
                mc_at_10s = pool_data.iloc[10]["marketCap"]
                if mc_at_10s > self.early_mc_limit:
                    print(f"Pooli hylätty: MC 10s kohdalla ({mc_at_10s:.2f}) > raja ({self.early_mc_limit})")
                    return False

            # Laske MC:n kasvu alusta
            initial_mc = initial_metrics.get("marketCap", 0)
            current_mc = metrics.get("mc_at_delay", 0)
            if initial_mc > 0:
                mc_growth = ((current_mc / initial_mc) - 1) * 100
                metrics["mc_growth_from_start"] = mc_growth

            # Laske holderien kasvu alusta
            initial_holders = initial_metrics.get("holdersCount", 0)
            current_holders = metrics.get("holders_at_delay", 0)
            holder_growth = current_holders - initial_holders
            metrics["holder_growth_from_start"] = holder_growth

            # Lisää uudet parametrit
            try:
                current_idx = pool_data.index[pool_data["marketCap"] == current_mc][0]
            except (IndexError, KeyError):
                # Jos indeksiä ei löydy, käytä viimeistä riviä
                current_idx = len(pool_data) - 1

            # MC muutos 30s
            if current_idx >= 30:
                try:
                    mc_30s_ago = pool_data.iloc[current_idx - 30]["marketCap"]
                    mc_change_30s = ((current_mc / mc_30s_ago) - 1) * 100
                    metrics["mc_change_30s"] = mc_change_30s
                except Exception as e:
                    print(f"Virhe MC muutoksen laskennassa: {str(e)}")
                    metrics["mc_change_30s"] = 0

            # Holderien muutos 30s
            if current_idx >= 30:
                try:
                    holders_30s_ago = pool_data.iloc[current_idx - 30]["holdersCount"]
                    holder_delta_30s = current_holders - holders_30s_ago
                    metrics["holder_delta_30s"] = holder_delta_30s
                except Exception as e:
                    print(f"Virhe holder deltan laskennassa: {str(e)}")
                    metrics["holder_delta_30s"] = 0

            # Osto/myynti suhde 10s
            try:
                # Käytä oikeita sarakkeiden nimiä
                buy_vol_10s = pool_data.iloc[current_idx]["buyVolume10s"]
                sell_vol_10s = pool_data.iloc[current_idx]["trade_last10Seconds.volume.sell"]

                if sell_vol_10s > 0:
                    buy_sell_ratio = buy_vol_10s / sell_vol_10s
                    metrics["buy_sell_ratio_10s"] = buy_sell_ratio
                else:
                    metrics["buy_sell_ratio_10s"] = float("inf")  # Jos ei myyntejä, suhde on ääretön
            except Exception as e:
                print(f"Virhe buy/sell ration laskennassa: {str(e)}")
                metrics["buy_sell_ratio_10s"] = 0  # Aseta oletusarvo jos laskenta epäonnistuu

            # Tarkista parametrit ja laske kuinka moni täyttyy
            conditions_met = 0
            for param, threshold in self.buy_params.items():
                try:
                    if param in metrics:
                        if metrics[param] > threshold:
                            conditions_met += 1
                        else:
                            print(f"Parametri {param} ei täyty: {metrics.get(param, 0):.2f} <= {threshold}")
                except Exception as e:
                    print(f"Virhe parametrin {param} tarkistuksessa: {str(e)}")

            # Vaadi vähintään 7 parametria 9:stä täyttymään
            required_conditions = 7
            if conditions_met >= required_conditions:
                print(f"Ostoehdot täyttyvät: {conditions_met}/{len(self.buy_params)} parametria ylittää raja-arvot")
                return True
            else:
                print(
                    f"Ostoehdot eivät täyty: vain {conditions_met}/{len(self.buy_params)} parametria ylittää raja-arvot"
                )
                return False

        except Exception as e:
            print(f"Virhe ostoehtojen tarkistuksessa: {str(e)}")
            return False

    def find_buy_opportunity(self, pool_data: pd.DataFrame) -> Optional[Dict]:
        """Etsi ostomahdollisuus annetusta datasta"""
        try:
            pool_address = pool_data["poolAddress"].iloc[0]
            print(f"\nAnalysoidaan poolia: {pool_address}")

            # Resetoi indeksi
            pool_data = pool_data.reset_index(drop=True)

            try:
                # Tarkista että kaikki tarvittavat sarakkeet löytyvät
                required_columns = [
                    "marketCap",
                    "timestamp",
                    "poolAddress",
                    "marketCapChange5s",
                    "marketCapChange10s",
                    "marketCapChange30s",
                    "marketCapChange60s",
                    "holderDelta5s",
                    "holderDelta10s",
                    "holderDelta30s",
                    "holderDelta60s",
                    "holdersCount",
                    "buyVolume5s",
                    "buyVolume10s",
                    "netVolume5s",
                    "netVolume10s",
                    "priceChangePercent",
                    "trade_last10Seconds.volume.sell",
                    "currentPrice",
                    "minMarketCap",
                    "athMarketCap",
                    "maMarketCap30s",
                    "maMarketCap60s",
                ]

                missing_columns = [col for col in required_columns if col not in pool_data.columns]
                if missing_columns:
                    print(f"Puuttuvat sarakkeet poolille {pool_address}: {missing_columns}")
                    return None

                # Odota kunnes on tarpeeksi dataa
                if len(pool_data) < self.max_delay + 10:  # +10 antaa tilaa exit-logiikalle
                    print(f"Liian vähän dataa poolille {pool_address}")
                    return None

                # Tallenna alkutilanne
                initial_metrics = {
                    "marketCap": pool_data.iloc[0]["marketCap"],
                    "holdersCount": pool_data.iloc[0]["holdersCount"],
                }

                print("Parametrit:")
                for param, threshold in self.buy_params.items():
                    print(f"- {param}: > {threshold}")

                # Käy läpi mahdolliset ostokohdat viiveen jälkeen
                for delay in range(self.min_delay, self.max_delay + 1):
                    if delay >= len(pool_data):
                        print(f"Saavutettiin poolin {pool_address} datan loppu")
                        break

                    try:
                        # Valmistele metriikat
                        current_metrics = {
                            "mc_at_delay": pool_data.iloc[delay]["marketCap"],
                            "holders_at_delay": pool_data.iloc[delay]["holdersCount"],
                            "mc_change_5s": pool_data.iloc[delay]["marketCapChange5s"],
                            "holder_growth_from_start": pool_data.iloc[delay]["holdersCount"]
                            - initial_metrics["holdersCount"],
                            "mc_growth_from_start": (
                                (pool_data.iloc[delay]["marketCap"] / initial_metrics["marketCap"]) - 1
                            )
                            * 100,
                            "buy_volume_5s": pool_data.iloc[delay]["buyVolume5s"],
                            "net_volume_5s": pool_data.iloc[delay]["netVolume5s"],
                            "price_change": pool_data.iloc[delay]["priceChangePercent"],
                        }

                        # Lisää large_buy_5s jos sarake on olemassa
                        if "largeBuy5s" in pool_data.columns:
                            current_metrics["large_buy_5s"] = pool_data.iloc[delay]["largeBuy5s"]
                        else:
                            current_metrics["large_buy_5s"] = 0

                        # Tarkista ostoehdot
                        if self.check_buy_conditions(current_metrics, initial_metrics, pool_data):
                            print(f"Osto löytyi sekunnilla {delay}!")

                            # Luo ostomahdollisuuden tiedot
                            buy_opportunity = {
                                "pool_address": pool_address,
                                "entry_time": pool_data.iloc[delay]["timestamp"],
                                "entry_price": pool_data.iloc[delay]["marketCap"],
                                "entry_row": delay,
                                "entry_metrics": current_metrics,
                                "initial_metrics": initial_metrics,
                                "mc_at_10s": (pool_data.iloc[10]["marketCap"] if len(pool_data) > 10 else None),
                                "post_entry_data": pool_data.iloc[delay:].reset_index(drop=True),
                            }

                            return buy_opportunity

                    except Exception as e:
                        print(f"Virhe sekunnin {delay} analysoinnissa: {str(e)}")
                        continue

                print(f"\nPoolia {pool_address} ei ostettu - parametrit eivät täyttyneet")
                return None

            except Exception as e:
                print(f"Virhe poolin {pool_address} analysoinnissa: {str(e)}")
                return None

        except Exception as e:
            print(f"Vakava virhe find_buy_opportunity-funktiossa: {str(e)}")
            return None


def preprocess_pool_data(df):
    """Esikäsittele poolin data laskemalla tarvittavat metriikat"""
    # Varmista että data on aikajärjestyksessä
    df = df.sort_values(["poolAddress", "timestamp"])

    # Täytä puuttuvat arvot nollilla
    for col in df.columns:
        if df[col].dtype in [float, int]:
            df[col] = df[col].fillna(0)

    # Tarkista onko tarvittavat sarakkeet olemassa
    required_columns = [
        "marketCap",
        "holdersCount",
        "buyVolume5s",
        "buyVolume10s",
        "trade_last5Seconds.volume.sell",
        "trade_last10Seconds.volume.sell",
    ]
    for col in required_columns:
        if col not in df.columns:
            print(f"VAROITUS: Sarake '{col}' puuttuu datasta!")
            # Lisää puuttuva sarake nollilla
            df[col] = 0

    # Lisää myös muut mahdollisesti puuttuvat sarakkeet
    if "largeBuy5s" not in df.columns:
        df["largeBuy5s"] = 0

    if "largeBuy10s" not in df.columns:
        df["largeBuy10s"] = 0

    print(f"Esikäsitellään {df['poolAddress'].nunique()} poolia...")

    return df


def process_batch(df, pool_batch, buy_params, results_queue, progress_queue):
    """Käsittele erä pooleja erillisessä prosessissa"""
    try:
        # Luo BuySimulator-instanssi
        simulator = BuySimulator(buy_params=buy_params)

        for pool_address in pool_batch:
            try:
                # Suodata data tälle poolille
                pool_data = df[df["poolAddress"] == pool_address].copy()

                if len(pool_data) < 100:  # Varmista että on tarpeeksi dataa
                    progress_queue.put(f"SKIP:{pool_address}")
                    continue

                # Järjestä data aikajärjestykseen
                pool_data = pool_data.sort_values("timestamp")

                # Käytä BuySimulator-luokkaa ostomahdollisuuksien etsimiseen
                buy_opportunity = simulator.find_buy_opportunity(pool_data)

                if buy_opportunity:
                    # Laske tuotot
                    buy_opportunity = calculate_returns(buy_opportunity)

                    # Lähetä tulos pääprosessille
                    results_queue.put(buy_opportunity)
                    progress_queue.put(f"SUCCESS:{pool_address}")
                else:
                    progress_queue.put(f"FAIL:{pool_address}")

            except Exception as e:
                print(f"Virhe poolin {pool_address} käsittelyssä: {str(e)}")
                progress_queue.put(f"FAIL:{pool_address}")

    except Exception as e:
        progress_queue.put(f"BATCH_ERROR:{str(e)}")


def calculate_returns(buy_opportunity):
    """Laske tuotot ostomahdollisuudelle"""
    entry_price = buy_opportunity["entry_price"]
    post_entry_data = buy_opportunity["post_entry_data"]

    if not post_entry_data.empty:
        max_mc = post_entry_data["marketCap"].max()
        # Laske realistinen myyntihinta (80% maksimista)
        realistic_sell_mc = max_mc * 0.8
        max_return = max_mc / entry_price if entry_price > 0 else 0
        realistic_return = realistic_sell_mc / entry_price if entry_price > 0 else 0

        buy_opportunity["max_return"] = max_return
        buy_opportunity["max_mc"] = max_mc
        buy_opportunity["realistic_return"] = realistic_return
        buy_opportunity["realistic_sell_mc"] = realistic_sell_mc
    else:
        buy_opportunity["max_return"] = 1.0
        buy_opportunity["max_mc"] = entry_price
        buy_opportunity["realistic_return"] = 1.0
        buy_opportunity["realistic_sell_mc"] = entry_price

    return buy_opportunity


def calculate_metrics(buy_opportunities):
    """Laske metriikat ostomahdollisuuksien arviointia varten"""
    total_opportunities = len(buy_opportunities)

    if total_opportunities == 0:
        return {
            "total_opportunities": 0,
            "over_3x": 0,
            "over_6x": 0,
            "under_1_8x": 0,
            "ratio": 0,
            "over_3x_percentage": 0,
            "over_6x_percentage": 0,
            "under_1_8x_percentage": 0,
            "total_sol_return": 0,
            "total_realistic_sol_return": 0,
            "avg_sol_return": 0,
            "avg_realistic_sol_return": 0,
            "weighted_return": 0,
            "return_distribution": {
                "under_1x": 0,
                "1x_to_1_8x": 0,
                "1_8x_to_3x": 0,
                "3x_to_4x": 0,
                "4x_to_5x": 0,
                "5x_to_6x": 0,
                "6x_to_7x": 0,
                "7x_to_8x": 0,
                "8x_to_9x": 0,
                "9x_to_10x": 0,
                "over_10x": 0,
            },
            "return_distribution_percentage": {
                "under_1x": 0,
                "1x_to_1_8x": 0,
                "1_8x_to_3x": 0,
                "3x_to_4x": 0,
                "4x_to_5x": 0,
                "5x_to_6x": 0,
                "6x_to_7x": 0,
                "7x_to_8x": 0,
                "8x_to_9x": 0,
                "9x_to_10x": 0,
                "over_10x": 0,
            },
        }

    # Laske eri tuottokategorioiden määrät
    over_3x = sum(1 for opp in buy_opportunities if opp.get("return_multiple", 0) >= 3)
    over_6x = sum(1 for opp in buy_opportunities if opp.get("return_multiple", 0) >= 6)
    under_1_8x = sum(1 for opp in buy_opportunities if opp.get("return_multiple", 0) < 1.8)

    # Laske tarkempi tuottojakauma
    return_distribution = {
        "under_1x": sum(1 for opp in buy_opportunities if opp.get("return_multiple", 0) < 1),
        "1x_to_1_8x": sum(1 for opp in buy_opportunities if 1 <= opp.get("return_multiple", 0) < 1.8),
        "1_8x_to_3x": sum(1 for opp in buy_opportunities if 1.8 <= opp.get("return_multiple", 0) < 3),
        "3x_to_4x": sum(1 for opp in buy_opportunities if 3 <= opp.get("return_multiple", 0) < 4),
        "4x_to_5x": sum(1 for opp in buy_opportunities if 4 <= opp.get("return_multiple", 0) < 5),
        "5x_to_6x": sum(1 for opp in buy_opportunities if 5 <= opp.get("return_multiple", 0) < 6),
        "6x_to_7x": sum(1 for opp in buy_opportunities if 6 <= opp.get("return_multiple", 0) < 7),
        "7x_to_8x": sum(1 for opp in buy_opportunities if 7 <= opp.get("return_multiple", 0) < 8),
        "8x_to_9x": sum(1 for opp in buy_opportunities if 8 <= opp.get("return_multiple", 0) < 9),
        "9x_to_10x": sum(1 for opp in buy_opportunities if 9 <= opp.get("return_multiple", 0) < 10),
        "over_10x": sum(1 for opp in buy_opportunities if opp.get("return_multiple", 0) >= 10),
    }

    # Laske prosenttiosuudet
    return_distribution_percentage = {
        key: (value / total_opportunities) * 100 if total_opportunities > 0 else 0
        for key, value in return_distribution.items()
    }

    over_3x_percentage = (over_3x / total_opportunities) * 100 if total_opportunities > 0 else 0
    over_6x_percentage = (over_6x / total_opportunities) * 100 if total_opportunities > 0 else 0
    under_1_8x_percentage = (under_1_8x / total_opportunities) * 100 if total_opportunities > 0 else 0

    # Laske suhdeluku (yli 3x / alle 1.8x)
    ratio = over_3x / under_1_8x if under_1_8x > 0 else float("inf")

    # Laske kokonaistuotto ja keskimääräinen tuotto
    total_sol_return = sum(opp.get("return_multiple", 0) for opp in buy_opportunities)
    total_realistic_sol_return = sum(opp.get("realistic_return", 0) for opp in buy_opportunities)

    avg_sol_return = total_sol_return / total_opportunities if total_opportunities > 0 else 0
    avg_realistic_sol_return = total_realistic_sol_return / total_opportunities if total_opportunities > 0 else 0

    # Laske painotettu tuotto (huomioi sekä tuoton että ostomahdollisuuksien määrän)
    # Painotus: 70% keskimääräinen tuotto, 30% ostomahdollisuuksien määrä (max 100)
    opportunity_weight = min(total_opportunities / 100, 1)  # Skaalaa 0-1 välille
    weighted_return = (0.7 * avg_realistic_sol_return) + (
        0.3 * opportunity_weight * 10
    )  # Kerrotaan 10:llä skaalauksen vuoksi

    return {
        "total_opportunities": total_opportunities,
        "over_3x": over_3x,
        "over_6x": over_6x,
        "under_1_8x": under_1_8x,
        "ratio": ratio,
        "over_3x_percentage": over_3x_percentage,
        "over_6x_percentage": over_6x_percentage,
        "under_1_8x_percentage": under_1_8x_percentage,
        "total_sol_return": total_sol_return,
        "total_realistic_sol_return": total_realistic_sol_return,
        "avg_sol_return": avg_sol_return,
        "avg_realistic_sol_return": avg_realistic_sol_return,
        "weighted_return": weighted_return,
        "return_distribution": return_distribution,
        "return_distribution_percentage": return_distribution_percentage,
    }


def create_return_report(buy_opportunities, output_file):
    """Luo tuottoraportti ostomahdollisuuksista"""
    if not buy_opportunities:
        with open(output_file, "w") as f:
            f.write("Ei löydetty ostomahdollisuuksia.\n")
        return

    # Järjestä ostomahdollisuudet tuoton mukaan (suurin ensin)
    sorted_opportunities = sorted(buy_opportunities, key=lambda x: x.get("return_percentage", 0), reverse=True)

    with open(output_file, "w") as f:
        f.write("=== OSTOMAHDOLLISUUKSIEN TUOTTORAPORTTI ===\n\n")

        # Kirjoita yhteenveto
        total_opportunities = len(sorted_opportunities)
        f.write(f"Yhteensä {total_opportunities} ostomahdollisuutta löydetty.\n\n")

        # Laske metriikat
        metrics = calculate_metrics(sorted_opportunities)

        # Kirjoita tarkempi tuottojakauma
        f.write("=== TUOTTOJAKAUMA ===\n")
        f.write(
            f"alle 1x: {metrics['return_distribution']['under_1x']} kpl ({metrics['return_distribution_percentage']['under_1x']:.1f}%)\n"
        )
        f.write(
            f"1-1.8x: {metrics['return_distribution']['1x_to_1_8x']} kpl ({metrics['return_distribution_percentage']['1x_to_1_8x']:.1f}%)\n"
        )
        f.write(
            f"1.8-3x: {metrics['return_distribution']['1_8x_to_3x']} kpl ({metrics['return_distribution_percentage']['1_8x_to_3x']:.1f}%)\n"
        )
        f.write(
            f"3-4x: {metrics['return_distribution']['3x_to_4x']} kpl ({metrics['return_distribution_percentage']['3x_to_4x']:.1f}%)\n"
        )
        f.write(
            f"4-5x: {metrics['return_distribution']['4x_to_5x']} kpl ({metrics['return_distribution_percentage']['4x_to_5x']:.1f}%)\n"
        )
        f.write(
            f"5-6x: {metrics['return_distribution']['5x_to_6x']} kpl ({metrics['return_distribution_percentage']['5x_to_6x']:.1f}%)\n"
        )
        f.write(
            f"6-7x: {metrics['return_distribution']['6x_to_7x']} kpl ({metrics['return_distribution_percentage']['6x_to_7x']:.1f}%)\n"
        )
        f.write(
            f"7-8x: {metrics['return_distribution']['7x_to_8x']} kpl ({metrics['return_distribution_percentage']['7x_to_8x']:.1f}%)\n"
        )
        f.write(
            f"8-9x: {metrics['return_distribution']['8x_to_9x']} kpl ({metrics['return_distribution_percentage']['8x_to_9x']:.1f}%)\n"
        )
        f.write(
            f"9-10x: {metrics['return_distribution']['9x_to_10x']} kpl ({metrics['return_distribution_percentage']['9x_to_10x']:.1f}%)\n"
        )
        f.write(
            f"10x+: {metrics['return_distribution']['over_10x']} kpl ({metrics['return_distribution_percentage']['over_10x']:.1f}%)\n\n"
        )

        f.write(f"Yli 3x tuoton saavutti: {metrics['over_3x']} ({metrics['over_3x_percentage']:.1f}%)\n")
        f.write(f"Yli 6x tuoton saavutti: {metrics['over_6x']} ({metrics['over_6x_percentage']:.1f}%)\n")
        f.write(f"Alle 1.8x tuoton jäi: {metrics['under_1_8x']} ({metrics['under_1_8x_percentage']:.1f}%)\n")
        f.write(f"Yli 3x / Alle 1.8x suhdeluku: {metrics['ratio']:.2f}\n\n")

        f.write(f"Kokonaistuotto (SOL): {metrics['total_sol_return']:.2f}x\n")
        f.write(f"Realistinen kokonaistuotto (SOL): {metrics['total_realistic_sol_return']:.2f}x\n")
        f.write(f"Keskimääräinen tuotto per pooli: {metrics['avg_sol_return']:.2f}x\n")
        f.write(f"Realistinen keskimääräinen tuotto per pooli: {metrics['avg_realistic_sol_return']:.2f}x\n\n")

        # Kirjoita yksityiskohtaiset tiedot jokaisesta ostomahdollisuudesta
        f.write("=== YKSITYISKOHTAISET TIEDOT ===\n\n")

        for i, opp in enumerate(sorted_opportunities):
            f.write(f"#{i+1}: Pool {opp['pool_address']}\n")
            f.write(f"  Ostoaika: {opp.get('buy_time', 'N/A')}\n")
            f.write(f"  Ostohinta: {opp.get('buy_price', 0):.6f} SOL\n")
            f.write(f"  Myyntihinta: {opp.get('sell_price', 0):.6f} SOL\n")
            f.write(f"  Tuotto: {opp.get('return_percentage', 0):.2f}% ({opp.get('return_multiple', 0):.2f}x)\n")
            f.write(
                f"  Realistinen tuotto: {opp.get('realistic_return_percentage', 0):.2f}% ({opp.get('realistic_return', 0):.2f}x)\n"
            )

            # Lisää ostoperusteet
            f.write("  Ostoperusteet:\n")
            for param, value in opp.get("buy_conditions", {}).items():
                threshold = opp.get("thresholds", {}).get(param, "N/A")
                f.write(f"    - {param}: {value} (kynnys: {threshold})\n")

            f.write("\n")

    print(f"Tuottoraportti tallennettu: {output_file}")


def create_summary(buy_opportunities, output_file):
    """Luo yhteenvetotiedosto ostomahdollisuuksista CSV-muodossa"""
    if not buy_opportunities:
        pd.DataFrame().to_csv(output_file, index=False)
        return

    # Kerää tiedot yhteenvetoa varten
    summary_data = []

    for opp in buy_opportunities:
        # Perustiedot
        data = {
            "pool_address": opp.get("pool_address", ""),
            "buy_time": opp.get("buy_time", ""),
            "buy_price": opp.get("buy_price", 0),
            "sell_price": opp.get("sell_price", 0),
            "return_percentage": opp.get("return_percentage", 0),
            "return_multiple": opp.get("return_multiple", 0),
            "realistic_return_percentage": opp.get("realistic_return_percentage", 0),
            "realistic_return": opp.get("realistic_return", 0),
        }

        # Lisää ostoperusteet
        for param, value in opp.get("buy_conditions", {}).items():
            data[f"condition_{param}"] = value
            data[f"threshold_{param}"] = opp.get("thresholds", {}).get(param, "N/A")

        summary_data.append(data)

    # Luo DataFrame ja tallenna CSV-tiedostoon
    summary_df = pd.DataFrame(summary_data)
    summary_df.to_csv(output_file, index=False)

    print(f"Yhteenveto tallennettu: {output_file}")


def run_buy_simulation(df, buy_params=None, run_grid_test=False, continue_from=None):
    """Suorita ostosimulaatio annetuilla parametreilla tai grid-testi"""
    if run_grid_test:
        # Määritä parametriruudukko - MUOKATTU: käyttäjän määrittelemät parametrit
        param_grid = {
            "holder_delta_30s": [10, 20, 30, 40],
            "net_volume_5s": [0],
            "large_buy_5s": [0, 1, 2],
        }

        # Aseta muut parametrit kiinteisiin arvoihin
        default_params = {
            "price_change": 1,
            "mc_change_30s": 10,
            "buy_sell_ratio_10s": -999,  # Erittäin negatiivinen arvo, jotta ehto täyttyy aina
            "mc_growth_from_start": 10,
            "holder_growth_from_start": 20,
            "buy_volume_5s": 0,
        }

        # Luo kaikki parametriyhdistelmät
        param_combinations = list(itertools.product(*param_grid.values()))
        param_names = list(param_grid.keys())

        # Määritä aloituskohta
        start_index = 0
        results = []

        # Jos jatketaan edellisestä kohdasta, aseta aloituskohta ja lataa aiemmat tulokset
        if continue_from and "grid_results" in continue_from:
            results = continue_from.get("grid_results", [])
            start_index = len(results)
            print(f"Jatketaan grid-testiä kohdasta {start_index}/{len(param_combinations)}")

        # Luo aikaleima
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Luo hakemisto grid-testin tuloksille jos sitä ei ole
        grid_output_dir = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "grid_test_results",
        )
        os.makedirs(grid_output_dir, exist_ok=True)

        # Luo tiedostonimi grid-testin tuloksille
        grid_results_file = os.path.join(grid_output_dir, f"grid_results_{timestamp}.csv")

        try:
            # Käy läpi kaikki parametriyhdistelmät
            for i, combination in enumerate(param_combinations[start_index:], start=start_index):
                # Luo parametrit tälle yhdistelmälle
                current_params = default_params.copy()  # Aloita kiinteillä arvoilla
                for j, param_name in enumerate(param_names):
                    current_params[param_name] = combination[j]

                print(f"\nTestaan parametriyhdistelmää {i+1}/{len(param_combinations)}:")
                for param, value in current_params.items():
                    print(f"- {param}: {value}")

                # Suorita simulaatio tällä parametriyhdistelmällä
                buy_opportunities = run_single_simulation(df, current_params, timestamp_suffix=f"grid_{i}")

                # Laske tuotot
                avg_return = 0
                if buy_opportunities:
                    returns = [opp.get("realistic_return", 0) for opp in buy_opportunities]
                    avg_return = sum(returns) / len(returns) if returns else 0

                    # Laske tuottojakauma
                    metrics = calculate_metrics(buy_opportunities)
                    print("\n=== TUOTTOJAKAUMA ===")
                    print(
                        f"alle 1x: {metrics['return_distribution']['under_1x']} kpl ({metrics['return_distribution_percentage']['under_1x']:.1f}%)"
                    )
                    print(
                        f"1-1.8x: {metrics['return_distribution']['1x_to_1_8x']} kpl ({metrics['return_distribution_percentage']['1x_to_1_8x']:.1f}%)"
                    )
                    print(
                        f"1.8-3x: {metrics['return_distribution']['1_8x_to_3x']} kpl ({metrics['return_distribution_percentage']['1_8x_to_3x']:.1f}%)"
                    )
                    print(
                        f"3-4x: {metrics['return_distribution']['3x_to_4x']} kpl ({metrics['return_distribution_percentage']['3x_to_4x']:.1f}%)"
                    )
                    print(
                        f"4-5x: {metrics['return_distribution']['4x_to_5x']} kpl ({metrics['return_distribution_percentage']['4x_to_5x']:.1f}%)"
                    )
                    print(
                        f"5-6x: {metrics['return_distribution']['5x_to_6x']} kpl ({metrics['return_distribution_percentage']['5x_to_6x']:.1f}%)"
                    )
                    print(
                        f"6-7x: {metrics['return_distribution']['6x_to_7x']} kpl ({metrics['return_distribution_percentage']['6x_to_7x']:.1f}%)"
                    )
                    print(
                        f"7-8x: {metrics['return_distribution']['7x_to_8x']} kpl ({metrics['return_distribution_percentage']['7x_to_8x']:.1f}%)"
                    )
                    print(
                        f"8-9x: {metrics['return_distribution']['8x_to_9x']} kpl ({metrics['return_distribution_percentage']['8x_to_9x']:.1f}%)"
                    )
                    print(
                        f"9-10x: {metrics['return_distribution']['9x_to_10x']} kpl ({metrics['return_distribution_percentage']['9x_to_10x']:.1f}%)"
                    )
                    print(
                        f"10x+: {metrics['return_distribution']['over_10x']} kpl ({metrics['return_distribution_percentage']['over_10x']:.1f}%)"
                    )

                # Tallenna tulos
                result = {
                    "combination_index": i,
                    "avg_return": avg_return,
                    "opportunities_count": len(buy_opportunities),
                    **current_params,
                }
                results.append(result)

                print(
                    f"Parametriyhdistelmä {i+1}: Keskimääräinen tuotto {avg_return:.2f}%, "
                    f"Ostomahdollisuuksia: {len(buy_opportunities)}"
                )

                # Tallenna väliaikainen tulos
                pd.DataFrame(results).to_csv(grid_results_file, index=False)

                # Tallenna edistymistiedot
                progress_data = {
                    "timestamp": datetime.now().strftime("%Y%m%d_%H%M%S"),
                    "total_combinations": len(param_combinations),
                    "combinations_processed": i + 1,
                    "grid_results": results,
                    "grid_results_file": grid_results_file,
                }
                save_progress(progress_data, run_grid_test=True)

            # Merkitse grid-testi valmiiksi
            final_progress_data = {
                "timestamp": datetime.now().strftime("%Y%m%d_%H%M%S"),
                "total_combinations": len(param_combinations),
                "combinations_processed": len(param_combinations),
                "grid_results": results,
                "grid_results_file": grid_results_file,
            }
            save_progress(final_progress_data, run_grid_test=True, completed=True)

            # Etsi paras parametriyhdistelmä
            if results:
                best_result = max(results, key=lambda x: x["avg_return"])
                best_params = {param: best_result[param] for param in param_names}

                print("\nGrid-testi valmis!")
                print(f"Paras parametriyhdistelmä (indeksi {best_result['combination_index']}):")
                for param, value in best_params.items():
                    print(f"- {param}: {value}")
                print(f"Keskimääräinen tuotto: {best_result['avg_return']:.2f}%")
                print(f"Ostomahdollisuuksia: {best_result['opportunities_count']}")

                # Tallenna lopulliset tulokset
                pd.DataFrame(results).to_csv(grid_results_file, index=False)
                print(f"Grid-testin tulokset tallennettu: {grid_results_file}")

                # Luo hakemisto yhteenvetoraporteille jos sitä ei ole
                summary_dir = os.path.join(
                    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                    "summary_reports",
                )
                os.makedirs(summary_dir, exist_ok=True)

                # Luo tiedostonimi grid-testin yhteenvetoraportille
                grid_summary_file = os.path.join(summary_dir, f"grid_summary_{timestamp}.txt")

                # Tallenna grid-testin yhteenveto tiedostoon
                with open(grid_summary_file, "w") as f:
                    f.write("=== GRID-TESTIN YHTEENVETO ===\n\n")
                    f.write(f"Testattu {len(results)}/{len(param_combinations)} parametriyhdistelmää.\n\n")

                    f.write(f"Paras parametriyhdistelmä (indeksi {best_result['combination_index']}):\n")
                    for param, value in best_params.items():
                        f.write(f"- {param}: {value}\n")
                    f.write(f"Keskimääräinen tuotto: {best_result['avg_return']:.2f}%\n")
                    f.write(f"Ostomahdollisuuksia: {best_result['opportunities_count']}\n\n")

                    f.write("=== TOP 5 PARAMETRIYHDISTELMÄÄ ===\n\n")
                    top_results = sorted(results, key=lambda x: x["avg_return"], reverse=True)[:5]
                    for i, result in enumerate(top_results):
                        f.write(f"{i+1}. Indeksi {result['combination_index']}:\n")
                        for param, value in {param: result[param] for param in param_names}.items():
                            f.write(f"   - {param}: {value}\n")
                        f.write(f"   Keskimääräinen tuotto: {result['avg_return']:.2f}%\n")
                        f.write(f"   Ostomahdollisuuksia: {result['opportunities_count']}\n\n")

                print(f"Grid-testin yhteenveto tallennettu: {grid_summary_file}")

                return best_params, results
            else:
                print("Ei tuloksia grid-testistä.")
                return None, []

        except KeyboardInterrupt:
            print("\nGrid-testi keskeytetty. Tallennetaan edistyminen...")

            # Tallenna edistymistiedot
            progress_data = {
                "timestamp": datetime.now().strftime("%Y%m%d_%H%M%S"),
                "total_combinations": len(param_combinations),
                "combinations_processed": start_index + len(results),
                "grid_results": results,
                "grid_results_file": grid_results_file,
            }
            save_progress(progress_data, run_grid_test=True)

            print(
                f"Edistyminen tallennettu. Voit jatkaa myöhemmin käynnistämällä ohjelman uudelleen "
                f"tai käyttämällä --continue parametria."
            )

            # Etsi paras parametriyhdistelmä tähän mennessä
            if results:
                best_result = max(results, key=lambda x: x["avg_return"])
                best_params = {param: best_result[param] for param in param_names}

                print(f"\nParas parametriyhdistelmä tähän mennessä (indeksi {best_result['combination_index']}):")
                for param, value in best_params.items():
                    print(f"- {param}: {value}")
                print(f"Keskimääräinen tuotto: {best_result['avg_return']:.2f}%")
                print(f"Ostomahdollisuuksia: {best_result['opportunities_count']}")

                # Luo hakemisto yhteenvetoraporteille jos sitä ei ole
                summary_dir = os.path.join(
                    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                    "summary_reports",
                )
                os.makedirs(summary_dir, exist_ok=True)

                # Luo tiedostonimi keskeytetyn grid-testin yhteenvetoraportille
                interrupted_summary_file = os.path.join(summary_dir, f"interrupted_grid_summary_{timestamp}.txt")

                # Tallenna keskeytetyn grid-testin yhteenveto tiedostoon
                with open(interrupted_summary_file, "w") as f:
                    f.write("=== KESKEYTETYN GRID-TESTIN YHTEENVETO ===\n\n")
                    f.write(f"Testattu {len(results)}/{len(param_combinations)} parametriyhdistelmää.\n\n")

                    f.write(f"Paras parametriyhdistelmä tähän mennessä (indeksi {best_result['combination_index']}):\n")
                    for param, value in best_params.items():
                        f.write(f"- {param}: {value}\n")
                    f.write(f"Keskimääräinen tuotto: {best_result['avg_return']:.2f}%\n")
                    f.write(f"Ostomahdollisuuksia: {best_result['opportunities_count']}\n\n")

                    f.write("=== TOP 5 PARAMETRIYHDISTELMÄÄ TÄHÄN MENNESSÄ ===\n\n")
                    top_results = sorted(results, key=lambda x: x["avg_return"], reverse=True)[:5]
                    for i, result in enumerate(top_results):
                        f.write(f"{i+1}. Indeksi {result['combination_index']}:\n")
                        for param, value in {param: result[param] for param in param_names}.items():
                            f.write(f"   - {param}: {value}\n")
                        f.write(f"   Keskimääräinen tuotto: {result['avg_return']:.2f}%\n")
                        f.write(f"   Ostomahdollisuuksia: {result['opportunities_count']}\n\n")

                print(f"Keskeytetyn grid-testin yhteenveto tallennettu: {interrupted_summary_file}")

                return best_params, results
            else:
                return None, []
    else:
        # Normaali simulaatio yhdellä parametriyhdistelmällä
        buy_opportunities = run_single_simulation(df, buy_params, continue_from=continue_from)

        # Tulosta yhteenveto konsoliin
        if buy_opportunities:
            metrics = calculate_metrics(buy_opportunities)
            print("\n=== SIMULAATION YHTEENVETO ===")
            print(f"Yhteensä {metrics['total_opportunities']} ostomahdollisuutta löydetty.")

            # Tulosta tuottojakauma
            print("\n=== TUOTTOJAKAUMA ===")
            print(
                f"alle 1x: {metrics['return_distribution']['under_1x']} kpl ({metrics['return_distribution_percentage']['under_1x']:.1f}%)"
            )
            print(
                f"1-1.8x: {metrics['return_distribution']['1x_to_1_8x']} kpl ({metrics['return_distribution_percentage']['1x_to_1_8x']:.1f}%)"
            )
            print(
                f"1.8-3x: {metrics['return_distribution']['1_8x_to_3x']} kpl ({metrics['return_distribution_percentage']['1_8x_to_3x']:.1f}%)"
            )
            print(
                f"3-4x: {metrics['return_distribution']['3x_to_4x']} kpl ({metrics['return_distribution_percentage']['3x_to_4x']:.1f}%)"
            )
            print(
                f"4-5x: {metrics['return_distribution']['4x_to_5x']} kpl ({metrics['return_distribution_percentage']['4x_to_5x']:.1f}%)"
            )
            print(
                f"5-6x: {metrics['return_distribution']['5x_to_6x']} kpl ({metrics['return_distribution_percentage']['5x_to_6x']:.1f}%)"
            )
            print(
                f"6-7x: {metrics['return_distribution']['6x_to_7x']} kpl ({metrics['return_distribution_percentage']['6x_to_7x']:.1f}%)"
            )
            print(
                f"7-8x: {metrics['return_distribution']['7x_to_8x']} kpl ({metrics['return_distribution_percentage']['7x_to_8x']:.1f}%)"
            )
            print(
                f"8-9x: {metrics['return_distribution']['8x_to_9x']} kpl ({metrics['return_distribution_percentage']['8x_to_9x']:.1f}%)"
            )
            print(
                f"9-10x: {metrics['return_distribution']['9x_to_10x']} kpl ({metrics['return_distribution_percentage']['9x_to_10x']:.1f}%)"
            )
            print(
                f"10x+: {metrics['return_distribution']['over_10x']} kpl ({metrics['return_distribution_percentage']['over_10x']:.1f}%)"
            )

            print(f"\nYli 3x tuoton saavutti: {metrics['over_3x']} ({metrics['over_3x_percentage']:.1f}%)")
            print(f"Yli 6x tuoton saavutti: {metrics['over_6x']} ({metrics['over_6x_percentage']:.1f}%)")
            print(f"Alle 1.8x tuoton jäi: {metrics['under_1_8x']} ({metrics['under_1_8x_percentage']:.1f}%)")
            print(f"Yli 3x / Alle 1.8x suhdeluku: {metrics['ratio']:.2f}")

            print(f"\nKeskimääräinen tuotto per pooli: {metrics['avg_sol_return']:.2f}x")
            print(f"Realistinen keskimääräinen tuotto per pooli: {metrics['avg_realistic_sol_return']:.2f}x")

            # Tulosta poolien osoitteet
            print("\n=== OSTETUT POOLIT ===")
            for i, opp in enumerate(buy_opportunities):
                print(f"{i+1}. {opp['pool_address']} - Tuotto: {opp.get('return_multiple', 0):.2f}x")

            # Luo aikaleima
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

            # Luo hakemisto yhteenvetoraporteille jos sitä ei ole
            summary_dir = os.path.join(
                os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                "summary_reports",
            )
            os.makedirs(summary_dir, exist_ok=True)

            # Luo tiedostonimi yhteenvetoraportille
            summary_report_file = os.path.join(summary_dir, f"console_summary_{timestamp}.txt")

            # Tallenna yhteenveto tiedostoon
            create_console_summary_report(buy_opportunities, summary_report_file)

        return buy_opportunities


def run_single_simulation(df, buy_params, timestamp_suffix="", continue_from=None):
    """Suorita yksittäinen simulaatio annetuilla parametreilla"""
    # Luo simulator parametrit
    if buy_params is None:
        buy_params = get_default_parameters()

    # Luo aikaleima
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    if timestamp_suffix:
        timestamp = f"{timestamp}_{timestamp_suffix}"

    # Luo hakemisto ostomahdollisuuksille jos sitä ei ole
    output_dir = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "buy_opportunities1",
    )
    os.makedirs(output_dir, exist_ok=True)

    # Luo tiedostonimet
    buy_opportunities_file = os.path.join(output_dir, f"buy_opportunities_{timestamp}.csv")
    return_report_file = os.path.join(output_dir, f"return_report_{timestamp}.txt")
    summary_file = os.path.join(output_dir, f"summary_{timestamp}.csv")

    # Hae uniikit poolit
    unique_pools = df["poolAddress"].unique()
    total_pools = len(unique_pools)

    # Määritä aloituskohta
    start_index = 0
    processed_pools = []

    # Jos jatketaan edellisestä kohdasta, aseta aloituskohta
    if continue_from and "processed_pools" in continue_from:
        processed_pools = continue_from.get("processed_pools", [])
        start_index = len(processed_pools)
        print(f"Jatketaan kohdasta {start_index}/{total_pools}")

    # Luo multiprocessing-resurssit
    manager = mp.Manager()
    results_queue = manager.Queue()
    progress_queue = manager.Queue()

    # Määritä prosessorien määrä
    num_processors = mp.cpu_count()
    print(f"Käytetään {num_processors} prosessoria")

    # Laske eräkoko
    batch_size = max(1, total_pools // num_processors)

    # Luo prosessit
    processes = []

    # Luo lista käsiteltävistä pooleista (poista jo käsitellyt)
    pools_to_process = [pool for pool in unique_pools if pool not in processed_pools]

    # Jaa poolit eriin
    for i in range(0, len(pools_to_process), batch_size):
        batch_pools = pools_to_process[i : i + batch_size]
        p = mp.Process(
            target=process_batch,
            args=(df, batch_pools, buy_params, results_queue, progress_queue),
        )
        processes.append(p)
        p.start()

    # Seuraa edistymistä
    successful_pools = 0
    failed_pools = 0
    skipped_pools = 0
    buy_opportunities = []

    # Lisää jo käsitellyt poolit tuloksiin
    if continue_from and "successful_pools" in continue_from:
        successful_pools = continue_from.get("successful_pools", 0)
        failed_pools = continue_from.get("failed_pools", 0)
        skipped_pools = continue_from.get("skipped_pools", 0)

        # Lataa aiemmat ostomahdollisuudet jos ne on tallennettu
        if "buy_opportunities_file" in continue_from:
            try:
                prev_opportunities = pd.read_csv(continue_from["buy_opportunities_file"])
                buy_opportunities = prev_opportunities.to_dict("records")
                print(f"Ladattu {len(buy_opportunities)} aiempaa ostomahdollisuutta")
            except Exception as e:
                print(f"Varoitus: Aiempien ostomahdollisuuksien lataus epäonnistui: {str(e)}")

    # Kokonaismäärä käsiteltäviä pooleja
    total_to_process = len(pools_to_process)
    processed_count = 0

    # Seuraa prosessien tilaa
    active_processes = len(processes)

    # Tallenna edistyminen säännöllisesti
    last_save_time = time.time()
    save_interval = 60  # Tallenna minuutin välein

    try:
        while active_processes > 0:
            # Tarkista onko uusia tuloksia
            try:
                result = results_queue.get(timeout=0.1)
                if isinstance(result, dict) and "pool_address" in result:
                    buy_opportunities.append(result)
                    print(f"Ostomahdollisuus löydetty: {result['pool_address']}")
            except Empty:
                pass

            # Tarkista edistymisviestit
            try:
                progress_msg = progress_queue.get(timeout=0.1)
                if progress_msg.startswith("SUCCESS:"):
                    successful_pools += 1
                    processed_count += 1
                    pool_id = progress_msg.split(":", 1)[1]
                    processed_pools.append(pool_id)
            except Empty:
                pass

            # Päivitä aktiivisten prosessien määrä
            active_processes = sum(p.is_alive() for p in processes)

            # Näytä edistyminen
            total_processed = successful_pools + failed_pools + skipped_pools
            if total_processed > 0 and total_processed % 1 == 0:
                # Näytä CPU ja RAM käyttö
                cpu_percent = psutil.cpu_percent()
                ram_percent = psutil.virtual_memory().percent

                print(
                    f"\rEdistyminen: {processed_count}/{total_to_process} poolia käsitelty "
                    f"({successful_pools} onnistunutta, {failed_pools} epäonnistunutta, {skipped_pools} ohitettua). "
                    f"Löydetty {len(buy_opportunities)} ostomahdollisuutta. "
                    f"CPU: {cpu_percent:.1f}%, RAM: {ram_percent:.1f}%",
                    end="",
                )

            # Tallenna edistyminen säännöllisesti
            current_time = time.time()
            if current_time - last_save_time > save_interval:
                # Tallenna väliaikainen CSV ostomahdollisuuksista
                temp_buy_file = os.path.join(output_dir, f"temp_buy_opportunities_{timestamp}.csv")
                if buy_opportunities:
                    pd.DataFrame(buy_opportunities).to_csv(temp_buy_file, index=False)

                # Tallenna edistymistiedot
                progress_data = {
                    "timestamp": datetime.now().strftime("%Y%m%d_%H%M%S"),
                    "total_pools": total_pools,
                    "pools_processed": total_processed,
                    "processed_pools": processed_pools,
                    "successful_pools": successful_pools,
                    "failed_pools": failed_pools,
                    "skipped_pools": skipped_pools,
                    "buy_opportunities_file": (temp_buy_file if buy_opportunities else None),
                    "buy_params": buy_params,
                }
                save_progress(
                    progress_data,
                    run_grid_test=(timestamp_suffix != ""),
                    completed=False,
                )
                last_save_time = current_time

            # Pieni viive CPU:n säästämiseksi
            time.sleep(0.01)

        print("\nKaikki prosessit valmistuneet.")
    except KeyboardInterrupt:
        print("\nKeskeytys havaittu. Tallennetaan edistyminen...")

        # Tallenna edistymistiedot
        temp_buy_file = os.path.join(output_dir, f"temp_buy_opportunities_{timestamp}.csv")
        if buy_opportunities:
            pd.DataFrame(buy_opportunities).to_csv(temp_buy_file, index=False)

        progress_data = {
            "timestamp": datetime.now().strftime("%Y%m%d_%H%M%S"),
            "total_pools": total_pools,
            "pools_processed": total_processed,
            "processed_pools": processed_pools,
            "successful_pools": successful_pools,
            "failed_pools": failed_pools,
            "skipped_pools": skipped_pools,
            "buy_opportunities_file": temp_buy_file if buy_opportunities else None,
            "buy_params": buy_params,
        }
        save_progress(progress_data, run_grid_test=(timestamp_suffix != ""), completed=False)

        # Lopeta prosessit
        for p in processes:
            if p.is_alive():
                p.terminate()

        print("Prosessit lopetettu. Edistyminen tallennettu.")
        print(f"Voit jatkaa myöhemmin käynnistämällä ohjelman uudelleen tai käyttämällä --continue parametria.")
        sys.exit(1)

    # Odota että kaikki prosessit päättyvät
    for p in processes:
        p.join()

    # Tallenna ostomahdollisuudet CSV-tiedostoon
    if buy_opportunities:
        pd.DataFrame(buy_opportunities).to_csv(buy_opportunities_file, index=False)
        print(f"\nTallennettu {len(buy_opportunities)} ostomahdollisuutta tiedostoon {buy_opportunities_file}")
    else:
        print("\nEi löydetty ostomahdollisuuksia.")

    # Luo tuottoraportti
    create_return_report(buy_opportunities, return_report_file)

    # Luo yhteenveto
    create_summary(buy_opportunities, summary_file)

    # Poista väliaikaiset tiedostot
    temp_buy_file = os.path.join(output_dir, f"temp_buy_opportunities_{timestamp}.csv")
    if os.path.exists(temp_buy_file):
        os.remove(temp_buy_file)

    # Merkitse ajo valmiiksi
    final_progress_data = {
        "timestamp": datetime.now().strftime("%Y%m%d_%H%M%S"),
        "total_pools": total_pools,
        "pools_processed": total_pools,  # Kaikki poolit on nyt käsitelty
        "processed_pools": processed_pools,
        "successful_pools": successful_pools,
        "failed_pools": failed_pools,
        "skipped_pools": skipped_pools,
        "buy_opportunities_file": buy_opportunities_file if buy_opportunities else None,
        "buy_params": buy_params,
        "results_summary": {
            "total_opportunities": len(buy_opportunities),
            "return_report_file": return_report_file,
            "summary_file": summary_file,
        },
    }

    # Tallenna lopulliset edistymistiedot merkittynä valmiiksi
    save_progress(final_progress_data, run_grid_test=(timestamp_suffix != ""), completed=True)

    # Poista vanhat edistymistiedostot
    progress_files = glob.glob(
        os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "progress",
            f"*{'grid' if timestamp_suffix else 'normal'}*.json",
        )
    )

    # Säilytä vain viimeisin valmis edistymistiedosto
    latest_progress = None
    latest_timestamp = None

    for file in progress_files:
        try:
            progress_data = load_progress(file)
            if progress_data and progress_data.get("completed", False):
                file_timestamp = progress_data.get("timestamp", "")
                if latest_timestamp is None or file_timestamp > latest_timestamp:
                    latest_timestamp = file_timestamp
                    latest_progress = file
        except Exception:
            pass

    # Poista muut paitsi viimeisin valmis edistymistiedosto
    for file in progress_files:
        if file != latest_progress:
            try:
                os.remove(file)
                print(f"Poistettu vanha edistymistiedosto: {file}")
            except Exception as e:
                print(f"Varoitus: Edistymistiedoston poisto epäonnistui: {str(e)}")

    return buy_opportunities


def create_parameter_grid():
    """Luo parametriruudukko grid-testausta varten"""
    # Hae parametrit
    parameters = get_parameters()

    # Kerää parametrit, joilla on useita arvoja
    grid_params = {}
    default_params = {}

    for key, value in parameters.items():
        if isinstance(value, list) and len(value) > 1:
            grid_params[key] = value
        else:
            # Jos parametrilla on vain yksi arvo (lista tai ei), käytä sitä oletusarvona
            default_params[key] = value[0] if isinstance(value, list) else value

    # Luo kaikki parametriyhdistelmät grid-parametreista
    param_keys = list(grid_params.keys())
    param_values = list(grid_params.values())

    combinations = list(itertools.product(*param_values))

    # Muunna yhdistelmät dictionaryiksi
    param_combinations = []

    for combo in combinations:
        # Luo parametriyhdistelmä käyttäen oletusarvoja ja vaihtelevia arvoja
        params = default_params.copy()
        for i, key in enumerate(param_keys):
            params[key] = combo[i]
        param_combinations.append(params)

    return param_combinations


# Lisätään funktiot edistymisen tallentamiseen ja lataamiseen
def save_progress(progress_data, run_grid_test=False, completed=False):
    """Tallentaa edistymistiedot tiedostoon"""
    # Määritä projektin juurihakemisto
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    # Luo kansio edistymistiedoille
    progress_dir = os.path.join(base_dir, "progress")
    if not os.path.exists(progress_dir):
        os.makedirs(progress_dir)

    # Luo tiedostonimi
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    grid_suffix = "_grid" if run_grid_test else ""
    progress_file = os.path.join(progress_dir, f"progress_{timestamp}{grid_suffix}.json")

    # Lisää tieto siitä, onko ajo valmis
    progress_data["completed"] = completed
    progress_data["timestamp"] = timestamp

    # Tallenna edistymistiedot
    with open(progress_file, "w") as f:
        json.dump(progress_data, f)

    print(f"\nEdistymistiedot tallennettu: {progress_file}")
    return progress_file


def load_progress(progress_file):
    """Lataa edistymistiedot tiedostosta"""
    try:
        with open(progress_file, "r") as f:
            progress_data = json.load(f)
        return progress_data
    except Exception as e:
        print(f"Virhe edistymistietojen latauksessa: {str(e)}")
        return None


def get_latest_progress_file(grid_test=False):
    """Hakee viimeisimmän edistymistiedoston"""
    # Määritä projektin juurihakemisto
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    # Edistymistietojen kansio
    progress_dir = os.path.join(base_dir, "progress")
    if not os.path.exists(progress_dir):
        return None

    # Hae kaikki edistymistiedostot
    grid_suffix = "_grid" if grid_test else ""
    progress_files = [
        f for f in os.listdir(progress_dir) if f.startswith("progress_") and f.endswith(f"{grid_suffix}.json")
    ]

    if not progress_files:
        return None

    # Järjestä tiedostot aikaleiman mukaan (uusin ensin)
    progress_files.sort(reverse=True)

    # Palauta uusimman tiedoston polku
    return os.path.join(progress_dir, progress_files[0])


def check_previous_run(grid_test=False, force_new=False):
    """Tarkistaa onko keskeneräisiä ajoja ja kysyy käyttäjältä haluaako jatkaa"""
    latest_progress_file = get_latest_progress_file(grid_test)

    if not latest_progress_file or force_new:
        return None

    # Lataa edistymistiedot
    progress_data = load_progress(latest_progress_file)

    if not progress_data:
        return None

    # Tarkista onko ajo valmis
    if progress_data.get("completed", False):
        print(f"\nEdellinen ajo on suoritettu loppuun ({latest_progress_file}).")
        choice = input("Haluatko aloittaa uuden ajon?\n1: Kyllä\n2: Ei\nValintasi: ")

        if choice == "1":
            return None
        else:
            print("Lopetetaan.")
            sys.exit(0)

    # Kysy käyttäjältä haluaako hän jatkaa
    print(f"\nLöydettiin keskeneräinen ajo ({latest_progress_file}).")
    print(
        f"Edistyminen: {progress_data.get('pools_processed', 0)}/{progress_data.get('total_pools', 0)} poolia käsitelty."
    )

    choice = input(
        "Haluatko jatkaa tästä ajosta vai aloittaa uuden?\n1: Jatka keskeneräistä ajoa\n2: Aloita uusi ajo\nValintasi: "
    )

    if choice == "1":
        print(f"Jatketaan keskeneräistä ajoa: {latest_progress_file}")
        return progress_data
    else:
        print("Aloitetaan uusi ajo.")
        return None


def calculate_buy_sell_ratio(pool_data, current_row, window):
    """Laske osto/myynti suhde annetulla aikaikkunalla"""
    if current_row < window:
        return 0

    # Käytä oikeita kenttänimiä
    if window == 5:
        buy_vol = pool_data.iloc[current_row]["buyVolume5s"]
        sell_vol = pool_data.iloc[current_row]["trade_last5Seconds.volume.sell"]
    elif window == 10:
        buy_vol = pool_data.iloc[current_row]["buyVolume10s"]
        sell_vol = pool_data.iloc[current_row]["trade_last10Seconds.volume.sell"]
    else:
        # Jos aikaikkunaa ei tueta, käytä 10s ikkunaa
        buy_vol = pool_data.iloc[current_row]["buyVolume10s"]
        sell_vol = pool_data.iloc[current_row]["trade_last10Seconds.volume.sell"]

    return buy_vol / sell_vol if sell_vol > 0 else 100


def calculate_holder_delta(pool_data, current_row, window):
    """Laske haltijoiden muutos annetulla aikaikkunalla"""
    if current_row < window:
        return 0

    current = pool_data.iloc[current_row]["holdersCount"]
    previous = pool_data.iloc[current_row - window]["holdersCount"]

    return current - previous


def calculate_mc_change(pool_data, current_row, window):
    """Laske markkinaarvon muutos prosentteina annetulla aikaikkunalla"""
    if current_row < window:
        return 0

    current = pool_data.iloc[current_row]["marketCap"]
    previous = pool_data.iloc[current_row - window]["marketCap"]

    return ((current / previous) - 1) * 100 if previous > 0 else 0


def calculate_holder_growth_from_start(pool_data, current_row, initial_metrics):
    """Laske haltijoiden kasvu alusta prosentteina"""
    current = pool_data.iloc[current_row]["holdersCount"]
    initial = initial_metrics["holdersCount"]

    return ((current - initial) / initial) * 100 if initial > 0 else 0


def calculate_mc_growth_from_start(pool_data, current_row, initial_metrics):
    """Laske markkinaarvon kasvu alusta prosentteina"""
    current = pool_data.iloc[current_row]["marketCap"]
    initial = initial_metrics["marketCap"]

    return ((current / initial) - 1) * 100 if initial > 0 else 0


def calculate_buy_volume(pool_data, current_row, window):
    """Laske ostovolyymi annetulla aikaikkunalla"""
    if current_row < window:
        return 0

    # Käytä oikeita kenttänimiä
    if window == 5:
        return pool_data.iloc[current_row]["buyVolume5s"]
    elif window == 10:
        return pool_data.iloc[current_row]["buyVolume10s"]
    else:
        # Jos aikaikkunaa ei tueta, käytä 10s ikkunaa
        return pool_data.iloc[current_row]["buyVolume10s"]


def calculate_net_volume(pool_data, current_row, window):
    """Laske nettovolyymi annetulla aikaikkunalla"""
    if current_row < window:
        return 0

    # Käytä oikeita kenttänimiä
    if window == 5:
        buy_vol = pool_data.iloc[current_row]["buyVolume5s"]
        sell_vol = pool_data.iloc[current_row]["trade_last5Seconds.volume.sell"]
        return buy_vol - sell_vol
    elif window == 10:
        buy_vol = pool_data.iloc[current_row]["buyVolume10s"]
        sell_vol = pool_data.iloc[current_row]["trade_last10Seconds.volume.sell"]
        return buy_vol - sell_vol
    else:
        # Jos aikaikkunaa ei tueta, käytä 10s ikkunaa
        buy_vol = pool_data.iloc[current_row]["buyVolume10s"]
        sell_vol = pool_data.iloc[current_row]["trade_last10Seconds.volume.sell"]
        return buy_vol - sell_vol


def calculate_large_buys(pool_data, current_row, window):
    """Laske suurten ostojen määrä annetulla aikaikkunalla"""
    if current_row < window:
        return 0

    # Käytä oikeita kenttänimiä
    if window == 5:
        return pool_data.iloc[current_row]["largeBuy5s"]
    elif window == 10:
        return pool_data.iloc[current_row]["largeBuy10s"]
    else:
        # Jos aikaikkunaa ei tueta, käytä 5s ikkunaa
        return pool_data.iloc[current_row]["largeBuy5s"]


def calculate_price_change(pool_data, current_row, window):
    """Laske hinnan muutos prosentteina annetulla aikaikkunalla"""
    if current_row < window:
        return 0

    current = pool_data.iloc[current_row]["marketCap"]
    previous = pool_data.iloc[current_row - window]["marketCap"]

    return ((current / previous) - 1) * 100 if previous > 0 else 0


def get_default_buy_params():
    """Palauta oletusostoparametrit"""
    return {
        "buy_sell_ratio_10s": -999,  # Erittäin alhainen, jotta täyttyy aina
        "holder_delta_30s": -999,  # Erittäin alhainen, jotta täyttyy aina
        "mc_change_30s": -999,  # Erittäin alhainen, jotta täyttyy aina
        "mc_change_5s": -999,  # Erittäin alhainen, jotta täyttyy aina
        "holder_growth_from_start": -999,  # Erittäin alhainen, jotta täyttyy aina
        "mc_growth_from_start": -999,  # Erittäin alhainen, jotta täyttyy aina
        "buy_volume_5s": -999,  # Erittäin alhainen, jotta täyttyy aina
        "net_volume_5s": -999,  # Erittäin alhainen, jotta täyttyy aina
        "large_buy_5s": -999,  # Erittäin alhainen, jotta täyttyy aina
        "price_change": -999,  # Erittäin alhainen, jotta täyttyy aina
    }


def create_console_summary_report(buy_opportunities, output_file):
    """Luo konsoliin tulostetun yhteenvedon mukainen raportti tiedostoon"""
    if not buy_opportunities:
        with open(output_file, "w") as f:
            f.write("Ei löydetty ostomahdollisuuksia.\n")
        return

    metrics = calculate_metrics(buy_opportunities)

    with open(output_file, "w") as f:
        f.write("=== SIMULAATION YHTEENVETO ===\n\n")
        f.write(f"Yhteensä {metrics['total_opportunities']} ostomahdollisuutta löydetty.\n\n")

        # Kirjoita tuottojakauma
        f.write("=== TUOTTOJAKAUMA ===\n")
        f.write(
            f"alle 1x: {metrics['return_distribution']['under_1x']} kpl ({metrics['return_distribution_percentage']['under_1x']:.1f}%)\n"
        )
        f.write(
            f"1-1.8x: {metrics['return_distribution']['1x_to_1_8x']} kpl ({metrics['return_distribution_percentage']['1x_to_1_8x']:.1f}%)\n"
        )
        f.write(
            f"1.8-3x: {metrics['return_distribution']['1_8x_to_3x']} kpl ({metrics['return_distribution_percentage']['1_8x_to_3x']:.1f}%)\n"
        )
        f.write(
            f"3-4x: {metrics['return_distribution']['3x_to_4x']} kpl ({metrics['return_distribution_percentage']['3x_to_4x']:.1f}%)\n"
        )
        f.write(
            f"4-5x: {metrics['return_distribution']['4x_to_5x']} kpl ({metrics['return_distribution_percentage']['4x_to_5x']:.1f}%)\n"
        )
        f.write(
            f"5-6x: {metrics['return_distribution']['5x_to_6x']} kpl ({metrics['return_distribution_percentage']['5x_to_6x']:.1f}%)\n"
        )
        f.write(
            f"6-7x: {metrics['return_distribution']['6x_to_7x']} kpl ({metrics['return_distribution_percentage']['6x_to_7x']:.1f}%)\n"
        )
        f.write(
            f"7-8x: {metrics['return_distribution']['7x_to_8x']} kpl ({metrics['return_distribution_percentage']['7x_to_8x']:.1f}%)\n"
        )
        f.write(
            f"8-9x: {metrics['return_distribution']['8x_to_9x']} kpl ({metrics['return_distribution_percentage']['8x_to_9x']:.1f}%)\n"
        )
        f.write(
            f"9-10x: {metrics['return_distribution']['9x_to_10x']} kpl ({metrics['return_distribution_percentage']['9x_to_10x']:.1f}%)\n"
        )
        f.write(
            f"10x+: {metrics['return_distribution']['over_10x']} kpl ({metrics['return_distribution_percentage']['over_10x']:.1f}%)\n\n"
        )

        f.write(f"Yli 3x tuoton saavutti: {metrics['over_3x']} ({metrics['over_3x_percentage']:.1f}%)\n")
        f.write(f"Yli 6x tuoton saavutti: {metrics['over_6x']} ({metrics['over_6x_percentage']:.1f}%)\n")
        f.write(f"Alle 1.8x tuoton jäi: {metrics['under_1_8x']} ({metrics['under_1_8x_percentage']:.1f}%)\n")
        f.write(f"Yli 3x / Alle 1.8x suhdeluku: {metrics['ratio']:.2f}\n\n")

        f.write(f"Keskimääräinen tuotto per pooli: {metrics['avg_sol_return']:.2f}x\n")
        f.write(f"Realistinen keskimääräinen tuotto per pooli: {metrics['avg_realistic_sol_return']:.2f}x\n\n")

        # Kirjoita poolien osoitteet
        f.write("=== OSTETUT POOLIT ===\n")
        for i, opp in enumerate(buy_opportunities):
            f.write(f"{i+1}. {opp['pool_address']} - Tuotto: {opp.get('return_multiple', 0):.2f}x\n")

    print(f"Konsoliraportti tallennettu: {output_file}")


if __name__ == "__main__":
    # Aseta multiprocessing käyttämään spawn-metodia macOS:llä
    if platform.system() == "Darwin":
        mp.set_start_method("spawn")

    # Määritä projektin juurihakemisto (split_backtest-kansion yläpuolella)
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    data_file = os.path.join(base_dir, "data", "pool_data.csv")

    # Käsittele komentoriviparametrit
    parser = argparse.ArgumentParser(description="Suorita ostosimulaatio poolidatalle")
    parser.add_argument(
        "--grid",
        action="store_true",
        help="Suorita grid-testi useilla parametriyhdistelmillä",
    )
    parser.add_argument("--limit", type=int, help="Rajoita testattavien poolien määrää")
    parser.add_argument(
        "--continue",
        dest="continue_previous",
        action="store_true",
        help="Jatka edellistä simulaatiota automaattisesti ilman kyselyä",
    )
    parser.add_argument(
        "--force-new",
        action="store_true",
        help="Pakota uuden simulaation aloitus, vaikka edellinen olisi kesken",
    )
    parser.add_argument(
        "--params",
        type=str,
        help='Käytettävät parametrit JSON-muodossa (esim. \'{"net_volume_5s": 0.2, "price_change": 0.03}\')',
    )
    args = parser.parse_args()

    # Tarkista onko edellinen simulaatio kesken
    progress_data = None

    if args.continue_previous:
        # Jos --continue on annettu, yritä jatkaa edellistä simulaatiota
        latest_progress_file = get_latest_progress_file(args.grid)
        if latest_progress_file:
            progress_data = load_progress(latest_progress_file)
            if progress_data:
                print(
                    f"\nJatketaan edellistä simulaatiota. Edistyminen: {progress_data.get('pools_processed', 0)}/{progress_data.get('total_pools', 0)} poolia käsitelty."
                )
            else:
                print("\nEdistymistietojen lataus epäonnistui. Aloitetaan uusi simulaatio.")
        else:
            print("\nEi löydetty keskeneräistä simulaatiota. Aloitetaan uusi simulaatio.")
    else:
        # Muuten kysy käyttäjältä
        progress_data = check_previous_run(args.grid, args.force_new)

    # Käsittele parametrit jos ne on annettu
    custom_params = None
    if args.params:
        try:
            custom_params = json.loads(args.params)
            print(f"\nKäytetään mukautettuja parametreja: {custom_params}")
        except json.JSONDecodeError:
            print(f"\nVIRHE: Parametrien JSON-muoto on virheellinen: {args.params}")
            sys.exit(1)

    print("Ladataan data...")
    try:
        # Lataa koko data
        df = pd.read_csv(data_file)
        df = preprocess_pool_data(df)

        # Rajoita poolien määrää jos --limit on annettu
        if args.limit:
            unique_pools = df["poolAddress"].unique()
            if args.limit < len(unique_pools):
                limited_pools = unique_pools[: args.limit]
                df = df[df["poolAddress"].isin(limited_pools)]
                print(f"Rajoitettu testattavien poolien määrä: {args.limit}")

        # Suorita simulaatio
        if args.grid:
            print("Suoritetaan grid-testi useilla parametriyhdistelmillä...")
            best_params, all_results = run_buy_simulation(df, run_grid_test=True, continue_from=progress_data)
            print("\nParas parametriyhdistelmä:")
            for param, value in best_params.items():
                print(f"- {param}: {value}")
        else:
            print("Suoritetaan simulaatio parametreilla...")
            run_buy_simulation(df, buy_params=custom_params, continue_from=progress_data)

    except FileNotFoundError as e:
        print(f"\nVIRHE: Tiedostoa ei löydy: {str(e)}")
        sys.exit(1)
    except Exception as e:
        print(f"\nVirhe datan latauksessa: {str(e)}")
        traceback.print_exc()
        sys.exit(1)
