import pandas as pd
import pickle
import numpy as np
from datetime import datetime
from typing import Dict, List, Tuple, Optional
import psutil
import os
import time
import json
import sys
import glob
import platform
import multiprocessing as mp
from multiprocessing import Queue, Process, cpu_count, Manager
from queue import Empty
import math
import itertools
import argparse

class SellSimulator:
    def __init__(self, 
                 initial_investment: float = 1.0,  # 1 SOL
                 base_take_profit: float = 1.9,    # Optimaalinen TP 1.9x
                 stop_loss: float = 0.65,          # Optimaalinen SL 0.65
                 trailing_stop: float = 0.9,       # Optimaalinen TS 0.9
                 stoploss_params: Dict[str, float] = None,
                 momentum_params: Dict[str, float] = None):
        
        self.initial_investment = initial_investment
        self.base_take_profit = base_take_profit
        self.stop_loss = stop_loss
        self.trailing_stop = trailing_stop
        
        self.stoploss_params = stoploss_params if stoploss_params is not None else {
            'holder_growth_30s_strong': 10.0,     # Laskettu 15.0 → 10.0
            'holder_growth_60s_strong': 50.0,     # Nostettu 35.0 → 50.0
            'holder_growth_30s_moderate': 20.0,    # Pidetään ennallaan
            'holder_growth_60s_moderate': 30.0,    # Nostettu 20.0 → 30.0
            'buy_volume_moderate': 15.0,           # Lisätty minimivolyymi
            'mc_drop_limit': -40.0                 # Uusi parametri: sallittu MC lasku
        }
        
        self.momentum_params = momentum_params if momentum_params is not None else {
            'mc_change_threshold': 6.0,           # Optimaalinen MC muutos kynnys
            'holder_change_threshold': 24.5,      # Optimaalinen holder muutos kynnys
            'buy_volume_threshold': 13.0,         # Optimaalinen ostovolyymi kynnys
            'net_volume_threshold': 3.0,          # Optimaalinen nettovolyymi kynnys
            'required_strong': 1.0,               # Optimaalinen vaaditut vahvat signaalit
            'lp_holder_growth_threshold': 0.0     # Optimaalinen holder kasvu kynnys
        }
    
    def check_momentum(self, metrics: Dict[str, float], momentum_params: Dict[str, float] = None) -> bool:
        """Tarkista onko momentum vielä vahva.
        
        Args:
            metrics: Nykyiset metriikat
            momentum_params: Momentum-parametrit (valinnainen)
            
        Returns:
            bool: True jos momentum on vielä vahva
        """
        try:
            # Käytä annettuja parametreja tai oletusarvoja
            if momentum_params is None:
                momentum_params = {
                    'mc_change_threshold': 6.0,
                    'holder_change_threshold': 24.5,
                    'buy_volume_threshold': 13.0,
                    'net_volume_threshold': 3.0,   # Päivitetty 4.0 → 3.0
                    'required_strong': 1.0
                }
            
            momentum_score = 0
            required_strong = momentum_params['required_strong']
            
            # MC kasvu on vielä hyvä
            if metrics.get('mc_change_5s', 0) > momentum_params['mc_change_threshold']:
                momentum_score += 1
                
            # Holderit kasvavat vielä
            if metrics.get('holder_change_30s', 0) > momentum_params['holder_change_threshold']:
                momentum_score += 1
                
            # Volyymi pysyy korkeana
            if metrics.get('buy_volume_5s', 0) > momentum_params['buy_volume_threshold']:
                momentum_score += 1
                
            # Nettovolyymi on selvästi positiivinen
            if metrics.get('net_volume_5s', 0) > momentum_params['net_volume_threshold']:
                momentum_score += 1

            return momentum_score >= required_strong
        except Exception as e:
            print(f"Virhe momentumin tarkistuksessa: {str(e)}")
            return False  # Oletuksena ei vahvaa momentumia virhetilanteessa
    
    def log_trade_summary(self, pool_address: str, entry_price: float, exit_price: float, 
                          entry_time: str, exit_time: str, profit_ratio: float, 
                          initial_investment: float, exit_reason: str = None,
                          entry_row: int = None, exit_row: int = None,
                          entry_metrics: Dict[str, float] = None,
                          exit_metrics: Dict[str, float] = None):
        """Tulosta kaupankäynnin yhteenveto"""
        profit_sol = (profit_ratio - 1) * initial_investment
        total_sol = initial_investment + profit_sol
        
        summary = f"""
=== KAUPANKÄYNNIN YHTEENVETO ===
Pool: {pool_address}

Sijoitus: {initial_investment:.2f} SOL
Tuotto: {profit_sol:.3f} SOL ({(profit_ratio - 1) * 100:.1f}%)
Loppusumma: {total_sol:.3f} SOL

Kaupankäynnin tiedot:
Osto Rivi {entry_row}:
Timestamp: {entry_time}
MC muutos 5s: {entry_metrics.get('mc_change_5s', 'N/A'):.2f}%
Holderien muutos 5s: {entry_metrics.get('holder_change_5s', 'N/A')}
Holderien muutos 30s: {entry_metrics.get('holder_change_30s', 'N/A')}
Holderien muutos 60s: {entry_metrics.get('holder_change_60s', 'N/A')}
Ostovolyymi 5s: {entry_metrics.get('buy_volume_5s', 'N/A'):.2f}
Nettovolyymi 5s: {entry_metrics.get('net_volume_5s', 'N/A'):.2f}
Hinnan muutos: {entry_metrics.get('price_change', 'N/A'):.2f}%

Myynti Rivi {exit_row}:
Timestamp: {exit_time}
MC muutos 5s: {exit_metrics.get('mc_change_5s', 'N/A'):.2f}%
Holderien muutos 5s: {exit_metrics.get('holder_change_5s', 'N/A')}
Holderien muutos 30s: {exit_metrics.get('holder_change_30s', 'N/A')}
Holderien muutos 60s: {exit_metrics.get('holder_change_60s', 'N/A')}
Ostovolyymi 5s: {exit_metrics.get('buy_volume_5s', 'N/A'):.2f}
Nettovolyymi 5s: {exit_metrics.get('net_volume_5s', 'N/A'):.2f}
Hinnan muutos: {exit_metrics.get('price_change', 'N/A'):.2f}%
- Myyntityyppi: {exit_reason}

Kaupankäyntiparametrit:
Take Profit: {(self.base_take_profit - 1) * 100:.1f}%
Stop Loss: {(self.stop_loss - 1) * 100:.1f}%
Trailing Stop: {(self.trailing_stop) * 100:.1f}%

Momentum-parametrit:
MC muutos kynnys: {self.momentum_params.get('mc_change_threshold', 'N/A')}
Holder muutos kynnys: {self.momentum_params.get('holder_change_threshold', 'N/A')}
Ostovolyymi kynnys: {self.momentum_params.get('buy_volume_threshold', 'N/A')}
Nettovolyymi kynnys: {self.momentum_params.get('net_volume_threshold', 'N/A')}
Vaaditut vahvat signaalit: {self.momentum_params.get('required_strong', 'N/A')}
LP holder kasvu kynnys: {self.momentum_params.get('lp_holder_growth_threshold', 'N/A')}

StopLoss-parametrit:
Holder kasvu 30s vahva: {self.stoploss_params.get('holder_growth_30s_strong', 'N/A')}
Holder kasvu 60s vahva: {self.stoploss_params.get('holder_growth_60s_strong', 'N/A')}
Holder kasvu 30s kohtalainen: {self.stoploss_params.get('holder_growth_30s_moderate', 'N/A')}
Holder kasvu 60s kohtalainen: {self.stoploss_params.get('holder_growth_60s_moderate', 'N/A')}
Ostovolyymi kohtalainen: {self.stoploss_params.get('buy_volume_moderate', 'N/A')}
MC lasku raja: {self.stoploss_params.get('mc_drop_limit', 'N/A')}

Kaupan kesto: {(pd.to_datetime(exit_time) - pd.to_datetime(entry_time)).total_seconds() / 60:.1f} min
"""
        print(summary)
        return summary
    
    def simulate_sell(self, buy_opportunity: Dict) -> Dict:
        """Simuloi myyntiä annetulla ostomahdollisuudella"""
        try:
            pool_address = buy_opportunity['pool_address']
            entry_price = buy_opportunity['entry_price']
            entry_time = buy_opportunity['entry_time']
            entry_row = buy_opportunity['entry_row']
            entry_metrics = buy_opportunity['entry_metrics']
            
            # Hae ostohetken jälkeinen data
            pool_data = buy_opportunity['post_entry_data']
            
            if len(pool_data) < 10:
                print(f"Liian vähän dataa myynnin simulointiin poolille {pool_address}")
                return None
            
            print(f"\nSimuloidaan myyntiä poolille: {pool_address}")
            print(f"Ostohinta: {entry_price:.2f}, Ostoaika: {entry_time}")
            
            max_profit = 0
            max_price = entry_price
            exit_reason = ""
            
            # Seuraa positiota
            for index in range(len(pool_data)):
                try:
                    current_price = pool_data.iloc[index]['marketCap']
                    current_time = pd.to_datetime(pool_data.iloc[index]['timestamp'])
                    profit_ratio = current_price / entry_price
                    
                    # Päivitä maksimihinta
                    max_price = max(max_price, current_price)
                    
                    # Päivitä maksimituotto
                    max_profit = max(max_profit, profit_ratio)
                    
                    # Kerää nykyiset metriikat
                    current_metrics = {}
                    try:
                        current_metrics = {
                            'mc_change_5s': pool_data.iloc[index]['marketCapChange5s'],
                            'holder_change_5s': pool_data.iloc[index]['holderDelta5s'],
                            'holder_change_30s': pool_data.iloc[index]['holderDelta30s'],
                            'holder_change_60s': pool_data.iloc[index]['holderDelta60s'],
                            'buy_volume_5s': pool_data.iloc[index]['buyVolume5s'],
                            'net_volume_5s': pool_data.iloc[index]['netVolume5s'],
                            'price_change': pool_data.iloc[index]['priceChangePercent']
                        }
                    except Exception as e:
                        print(f"Virhe metriikoiden keräämisessä: {str(e)}")
                        # Aseta oletusarvot puuttuville metriikoille
                        for key in ['mc_change_5s', 'holder_change_5s', 'holder_change_30s', 'holder_change_60s', 
                                   'buy_volume_5s', 'net_volume_5s', 'price_change']:
                            if key not in current_metrics:
                                current_metrics[key] = 0
                    
                    # Tarkista myyntiehdot
                    if profit_ratio >= self.base_take_profit:
                        try:
                            # Tarkista momentum käyttäen momentum_params-parametreja, jos ne on annettu
                            momentum_strong = self.check_momentum(current_metrics, self.momentum_params)
                            price_dropped = current_price < max_price * self.trailing_stop
                            
                            # Myy vain jos momentum on heikko JA hinta on tippunut merkittävästi huipusta
                            if not momentum_strong and price_dropped:
                                exit_reason = "Momentum Lost + Price Drop"
                                break
                        except Exception as e:
                            print(f"Virhe take profit -ehtojen tarkistuksessa: {str(e)}")
                            continue
                    
                    # Tarkista Low Performance -ehto (uusi myyntityyppi)
                    # Myy jos holderien kasvu on hidastunut merkittävästi
                    # mutta ennen kuin hinta laskee merkittävästi tai stop loss laukeaa
                    elif profit_ratio < 1.2 and profit_ratio > self.stop_loss:
                        try:
                            # Hae Low Performance -parametrit momentum_params-parametreista tai käytä oletusarvoja
                            lp_holder_growth_threshold = 2.0  # Oletusarvo: holderien kasvun hidastumisen raja
                            
                            if self.momentum_params and 'lp_holder_growth_threshold' in self.momentum_params:
                                lp_holder_growth_threshold = self.momentum_params['lp_holder_growth_threshold']
                            
                            # Tarkista holderien kasvun hidastuminen
                            holder_growth_30s = current_metrics.get('holder_change_30s', 0)
                            holder_growth_60s = current_metrics.get('holder_change_60s', 0)
                            
                            # Myy jos holderien kasvu on hidastunut alle kynnysarvon
                            # Tarkista sekä 30s että 60s holderien muutos
                            if holder_growth_30s < lp_holder_growth_threshold and holder_growth_60s < lp_holder_growth_threshold * 2:
                                exit_reason = "Low Performance"
                                break
                        except Exception as e:
                            print(f"Virhe low performance -ehtojen tarkistuksessa: {str(e)}")
                            continue
                            
                    elif profit_ratio <= self.stop_loss:
                        try:
                            # Tarkista stoploss-parametrit
                            holder_30s = current_metrics.get('holder_change_30s', 0)
                            holder_60s = current_metrics.get('holder_change_60s', 0)
                            buy_volume = current_metrics.get('buy_volume_5s', 0)
                            
                            # Vahva holder-kasvu - älä myy
                            if (holder_30s > self.stoploss_params['holder_growth_30s_strong'] and 
                                holder_60s > self.stoploss_params['holder_growth_60s_strong'] and
                                buy_volume > self.stoploss_params['buy_volume_moderate']):
                                continue
                                
                            # Kohtalainen holder-kasvu - älä myy
                            if (holder_30s > self.stoploss_params['holder_growth_30s_moderate'] and 
                                holder_60s > self.stoploss_params['holder_growth_60s_moderate'] and
                                buy_volume > self.stoploss_params['buy_volume_moderate']):
                                continue
                            
                            exit_reason = "Stop Loss"
                            break
                        except Exception as e:
                            print(f"Virhe stop loss -ehtojen tarkistuksessa: {str(e)}")
                            continue
                            
                    elif index == len(pool_data) - 1:
                        exit_reason = "Force Sell"
                        break
                except Exception as e:
                    print(f"Virhe kaupan seurannassa indeksillä {index}: {str(e)}")
                    continue
            
            # Jos exit_reason on tyhjä, pakota myynti
            if not exit_reason:
                exit_reason = "Force Sell"
                index = len(pool_data) - 1
                current_price = pool_data.iloc[index]['marketCap']
                current_time = pd.to_datetime(pool_data.iloc[index]['timestamp'])
                profit_ratio = current_price / entry_price
            
            # Laske tuotto
            profit_sol = (profit_ratio - 1) * self.initial_investment
            
            # Analysoi mitä tapahtui myynnin jälkeen
            post_exit_max_ratio = 1.0
            time_to_max = 0
            
            try:
                if index + 1 < len(pool_data):
                    post_exit_window = min(index + 300, len(pool_data))
                    post_exit_prices = pool_data.iloc[index+1:post_exit_window]['marketCap']
                    if not post_exit_prices.empty:
                        max_post_price = post_exit_prices.max()
                        post_exit_max_ratio = max_post_price / current_price
                        time_to_max = post_exit_prices.idxmax() - index
            except Exception as e:
                print(f"Virhe post-exit analyysissä: {str(e)}")
            
            # Luo kaupan tulos
            trade_result = {
                'pool_address': pool_address,
                'entry_time': entry_time,
                'entry_price': entry_price,
                'exit_time': pool_data.iloc[index]['timestamp'],
                'exit_price': current_price,
                'exit_reason': exit_reason,
                'profit_ratio': profit_ratio,
                'max_profit': max_profit,
                'trade_duration': (current_time - pd.to_datetime(entry_time)).total_seconds(),
                'investment_sol': self.initial_investment,
                'profit_sol': profit_sol,
                'entry_metrics': entry_metrics,
                'exit_metrics': current_metrics,
                'post_exit_max_ratio': post_exit_max_ratio,
                'post_exit_max_time': time_to_max,
                'max_x_after_stoploss': post_exit_max_ratio if exit_reason == "Stop Loss" else None,
                'max_x_after_tp': post_exit_max_ratio if exit_reason == "Momentum Lost + Price Drop" else None,
                'max_x_after_lp': post_exit_max_ratio if exit_reason == "Low Performance" else None,
                'stoploss_quality': 'Good' if exit_reason == "Stop Loss" and post_exit_max_ratio < 2.0 else 'Bad' if exit_reason == "Stop Loss" else None,
                'tp_quality': 'Good' if exit_reason == "Momentum Lost + Price Drop" and post_exit_max_ratio < 1.5 else 'Bad' if exit_reason == "Momentum Lost + Price Drop" else None,
                'lp_quality': 'Good' if exit_reason == "Low Performance" and post_exit_max_ratio < 1.5 else 'Bad' if exit_reason == "Low Performance" else None,
                'entry_row': entry_row,
                'exit_row': index
            }
            
            # Loki
            self.log_trade_summary(
                pool_address=pool_address,
                entry_price=entry_price,
                exit_price=current_price,
                entry_time=entry_time,
                exit_time=pool_data.iloc[index]['timestamp'],
                profit_ratio=profit_ratio,
                initial_investment=self.initial_investment,
                exit_reason=exit_reason,
                entry_row=entry_row,
                exit_row=index,
                entry_metrics=entry_metrics,
                exit_metrics=current_metrics
            )
            
            return trade_result
            
        except Exception as e:
            print(f"Vakava virhe simulate_sell-funktiossa: {str(e)}")
            return None

def create_stoploss_params():
    """Luo stoploss-parametrit"""
    stoploss_params = {
        'holder_growth_30s_strong': 10.0,     # Laskettu 15.0 → 10.0
        'holder_growth_60s_strong': 50.0,     # Nostettu 35.0 → 50.0
        'holder_growth_30s_moderate': 20.0,    # Pidetään ennallaan
        'holder_growth_60s_moderate': 30.0,    # Nostettu 20.0 → 30.0
        'buy_volume_moderate': 15.0,           # Lisätty minimivolyymi
        'mc_drop_limit': -40.0                 # Uusi parametri: sallittu MC lasku
    }
    return stoploss_params

def create_parameter_combinations():
    """Luo parametriyhdistelmät grid-testausta varten"""
    
    # Käytä optimaalisia parametreja, jotka löydettiin testeissä
    combinations = []
    
    # Optimaaliset parametrit: TP=1.9, SL=0.65, TS=0.9
    stoploss_params = create_stoploss_params()
    combinations.append({
        'initial_investment': 1.0,
        'base_take_profit': 1.9,
        'stop_loss': 0.65,
        'trailing_stop': 0.9,
        'stoploss_params': stoploss_params
    })
    
    print(f"Käytetään optimaalisia parametreja: TP=1.9, SL=0.65, TS=0.9")
    
    return combinations

def create_momentum_test_combinations():
    """Luo parametriyhdistelmät momentum-testausta varten"""
    print("\nLuodaan momentum-testauksen parametriyhdistelmät...")
    
    # Käytä optimaalisia kaupankäyntiparametreja
    base_take_profit = 1.9
    stop_loss = 0.65
    trailing_stop = 0.9
    
    # Momentum-parametrit testaukseen
    mc_change_thresholds = [6.0]
    holder_change_thresholds = [24.5]
    buy_volume_thresholds = [13.0]
    net_volume_thresholds = [3.0]
    required_strong_values = [1.0]
    lp_holder_growth_thresholds = [0.0]  # Käytä vain optimaalista arvoa 0.0
    
    print("Testataan seuraavat parametrit:")
    print(f"- Kaupankäyntiparametrit: TP={base_take_profit}, SL={stop_loss}, TS={trailing_stop}")
    print(f"- MC muutos kynnykset: {mc_change_thresholds}")
    print(f"- Holder muutos kynnykset: {holder_change_thresholds}")
    print(f"- Ostovolyymi kynnykset: {buy_volume_thresholds}")
    print(f"- Nettovolyymi kynnykset: {net_volume_thresholds}")
    print(f"- Vaaditut vahvat signaalit: {required_strong_values}")
    print(f"- Holder kasvu kynnykset: {lp_holder_growth_thresholds}")
    
    # Luo kaikki yhdistelmät
    combinations = []
    for mc_change in mc_change_thresholds:
        for holder_change in holder_change_thresholds:
            for buy_volume in buy_volume_thresholds:
                for net_volume in net_volume_thresholds:
                    for required_strong in required_strong_values:
                        for lp_holder_growth in lp_holder_growth_thresholds:
                            momentum_params = {
                                'mc_change_threshold': mc_change,
                                'holder_change_threshold': holder_change,
                                'buy_volume_threshold': buy_volume,
                                'net_volume_threshold': net_volume,
                                'required_strong': required_strong,
                                'lp_holder_growth_threshold': lp_holder_growth
                            }
                            
                            combo = {
                                'base_take_profit': base_take_profit,
                                'stop_loss': stop_loss,
                                'trailing_stop': trailing_stop,
                                'momentum_params': momentum_params
                            }
                            combinations.append(combo)
    
    print(f"Luotu {len(combinations)} parametriyhdistelmää momentum-testaukseen")
    return combinations

def create_fine_tuning_combinations():
    """Luo parametriyhdistelmät hienosäätöä varten.
    
    Käyttää parhaita löydettyjä parametreja lähtökohtana ja luo tarkempia
    testiyhdistelmiä niiden ympärille.
    
    Returns:
        List[Dict]: Lista parametriyhdistelmistä
    """
    # Parhaat löydetyt parametrit
    best_take_profit = 1.9
    best_stop_loss = 0.65
    best_trailing_stop = 0.9
    
    # Momentum parametrit
    best_mc_change = 6.0
    best_holder_change = 24.5
    best_buy_volume = 13.0
    best_net_volume = 3.0
    best_required_strong = 1.0
    best_lp_holder_growth = 0.0
    
    # Luo pienet vaihtelut parhaiden parametrien ympärille
    take_profits = [best_take_profit * 0.95, best_take_profit, best_take_profit * 1.05]
    stop_losses = [best_stop_loss * 0.95, best_stop_loss, best_stop_loss * 1.05]
    trailing_stops = [best_trailing_stop * 0.95, best_trailing_stop, best_trailing_stop * 1.05]
    
    # Momentum parametrien vaihtelut
    mc_changes = [best_mc_change * 0.9, best_mc_change, best_mc_change * 1.1]
    holder_changes = [best_holder_change * 0.9, best_holder_change, best_holder_change * 1.1]
    buy_volumes = [best_buy_volume * 0.9, best_buy_volume, best_buy_volume * 1.1]
    net_volumes = [best_net_volume * 0.9, best_net_volume, best_net_volume * 1.1]
    
    # Luo kaikki yhdistelmät
    combinations = []
    
    # Testaa vain muutamia yhdistelmiä kerrallaan
    for tp in take_profits:
        for sl in stop_losses:
            for ts in trailing_stops:
                # Käytä parhaita momentum parametreja
                momentum_params = {
                    'mc_change_threshold': best_mc_change,
                    'holder_change_threshold': best_holder_change,
                    'buy_volume_threshold': best_buy_volume,
                    'net_volume_threshold': best_net_volume,
                    'required_strong': best_required_strong,
                    'lp_holder_growth_threshold': best_lp_holder_growth
                }
                
                # Lisää yhdistelmä
                combinations.append({
                    'base_take_profit': tp,
                    'stop_loss': sl,
                    'trailing_stop': ts,
                    'momentum_params': momentum_params
                })
    
    # Testaa myös momentum parametrien vaihteluja
    for mc in mc_changes:
        for hc in holder_changes:
            for bv in buy_volumes:
                for nv in net_volumes:
                    # Käytä parhaita kaupankäyntiparametreja
                    momentum_params = {
                        'mc_change_threshold': mc,
                        'holder_change_threshold': hc,
                        'buy_volume_threshold': bv,
                        'net_volume_threshold': nv,
                        'required_strong': best_required_strong,
                        'lp_holder_growth_threshold': best_lp_holder_growth
                    }
                    
                    # Lisää yhdistelmä
                    combinations.append({
                        'base_take_profit': best_take_profit,
                        'stop_loss': best_stop_loss,
                        'trailing_stop': best_trailing_stop,
                        'momentum_params': momentum_params
                    })
    
    # Tulosta parametriyhdistelmien määrä
    print(f"\nLuotu {len(combinations)} parametriyhdistelmää hienosäätöä varten")
    
    # Tulosta muutama esimerkki
    print("\nEsimerkkejä parametriyhdistelmistä:")
    for i, combo in enumerate(combinations[:3]):
        print(f"\nYhdistelmä {i+1}:")
        print(f"Take Profit: {combo['base_take_profit']:.2f}")
        print(f"Stop Loss: {combo['stop_loss']:.2f}")
        print(f"Trailing Stop: {combo['trailing_stop']:.2f}")
        print("Momentum parametrit:")
        for param, value in combo['momentum_params'].items():
            print(f"- {param}: {value:.2f}")
    
    return combinations

def calculate_metrics(trades, show_detailed_analysis=False):
    """Laske metriikat kaupoista.
    
    Args:
        trades: Lista kaupoista
        show_detailed_analysis: Näytä yksityiskohtainen analyysi
        
    Returns:
        Dict: Metriikat
    """
    if not trades:
        print("Ei kauppoja analysoitavaksi!")
        return {}
    
    # Laske perustilastot
    total_trades = len(trades)
    
    # Laske voitolliset ja tappiolliset kaupat
    profitable_trades = [t for t in trades if t['profit_ratio'] > 1.0]
    losing_trades = [t for t in trades if t['profit_ratio'] <= 1.0]
    
    # Laske voittoaste
    win_rate = len(profitable_trades) / total_trades if total_trades > 0 else 0
    
    # Laske kokonaistuotto
    total_profit_ratio = sum([t['profit_ratio'] - 1.0 for t in trades])
    
    # Laske keskimääräinen tuotto per kauppa
    avg_profit_per_trade = total_profit_ratio / total_trades if total_trades > 0 else 0
    
    # Laske maksimi drawdown
    # Oletetaan että jokainen kauppa on 1 SOL
    cumulative_returns = np.cumsum([t['profit_ratio'] - 1.0 for t in trades])
    max_drawdown = 0
    peak = 0
    
    for i, ret in enumerate(cumulative_returns):
        if ret > peak:
            peak = ret
        drawdown = (peak - ret) / (peak + 1e-10) * 100  # Vältä jako nollalla
        max_drawdown = max(max_drawdown, drawdown)
    
    # Laske keskimääräinen pitoaika
    hold_times = []
    for trade in trades:
        entry_time = pd.to_datetime(trade['entry_time'])
        exit_time = pd.to_datetime(trade['exit_time'])
        hold_time = (exit_time - entry_time).total_seconds() / 60  # minuutteina
        hold_times.append(hold_time)
    
    avg_hold_time = sum(hold_times) / len(hold_times) if hold_times else 0
    
    # Laske myyntityyppien määrät
    stoploss_trades = [t for t in trades if t['exit_reason'] == 'Stop Loss']
    tp_trades = [t for t in trades if t['exit_reason'] == 'Momentum Lost + Price Drop']
    lp_trades = [t for t in trades if t['exit_reason'] == 'Low Performance']
    force_trades = [t for t in trades if t['exit_reason'] == 'Force Sell']
    
    # Laske myyntityyppien prosenttiosuudet
    stoploss_percent = len(stoploss_trades) / total_trades * 100 if total_trades > 0 else 0
    tp_percent = len(tp_trades) / total_trades * 100 if total_trades > 0 else 0
    lp_percent = len(lp_trades) / total_trades * 100 if total_trades > 0 else 0
    force_percent = len(force_trades) / total_trades * 100 if total_trades > 0 else 0
    
    # Näytä yksityiskohtainen analyysi jos pyydetty
    if show_detailed_analysis:
        print("\n=== YKSITYISKOHTAINEN KAUPPA-ANALYYSI ===\n")
        
        # Analysoi Stop Loss -kaupat
        sl_profitable = [t for t in stoploss_trades if t['profit_ratio'] > 1.0]
        sl_losing = [t for t in stoploss_trades if t['profit_ratio'] <= 1.0]
        sl_total_profit = sum([t['profit_ratio'] - 1.0 for t in stoploss_trades])
        sl_avg_profit = sl_total_profit / len(stoploss_trades) if stoploss_trades else 0
        sl_best = max([t['profit_ratio'] for t in stoploss_trades]) if stoploss_trades else 0
        sl_worst = min([t['profit_ratio'] for t in stoploss_trades]) if stoploss_trades else 0
        
        print("STOP LOSS KAUPAT:")
        print(f"Kauppoja yhteensä: {len(stoploss_trades)}")
        print(f"Voitollisia: {len(sl_profitable)} ({len(sl_profitable)/len(stoploss_trades)*100:.1f}% jos kauppoja)")
        print(f"Tappiollisia: {len(sl_losing)}")
        print(f"Kokonaistuotto: {sl_total_profit:.2f} SOL")
        print(f"Keskimääräinen tuotto: {sl_avg_profit:.3f} SOL per kauppa")
        print(f"Paras kauppa: {sl_best:.2f}X")
        print(f"Huonoin kauppa: {sl_worst:.2f}X")
        print()
        
        # Analysoi Take Profit -kaupat
        tp_profitable = [t for t in tp_trades if t['profit_ratio'] > 1.0]
        tp_losing = [t for t in tp_trades if t['profit_ratio'] <= 1.0]
        tp_total_profit = sum([t['profit_ratio'] - 1.0 for t in tp_trades])
        tp_avg_profit = tp_total_profit / len(tp_trades) if tp_trades else 0
        tp_best = max([t['profit_ratio'] for t in tp_trades]) if tp_trades else 0
        tp_worst = min([t['profit_ratio'] for t in tp_trades]) if tp_trades else 0
        
        print("TAKE PROFIT KAUPAT:")
        print(f"Kauppoja yhteensä: {len(tp_trades)}")
        print(f"Voitollisia: {len(tp_profitable)} ({len(tp_profitable)/len(tp_trades)*100:.1f}% jos kauppoja)")
        print(f"Tappiollisia: {len(tp_losing)}")
        print(f"Kokonaistuotto: {tp_total_profit:.2f} SOL")
        print(f"Keskimääräinen tuotto: {tp_avg_profit:.3f} SOL per kauppa")
        print(f"Paras kauppa: {tp_best:.2f}X")
        print(f"Huonoin kauppa: {tp_worst:.2f}X")
        print()
        
        # Analysoi Low Performance -kaupat
        lp_profitable = [t for t in lp_trades if t['profit_ratio'] > 1.0]
        lp_losing = [t for t in lp_trades if t['profit_ratio'] <= 1.0]
        lp_total_profit = sum([t['profit_ratio'] - 1.0 for t in lp_trades])
        lp_avg_profit = lp_total_profit / len(lp_trades) if lp_trades else 0
        lp_best = max([t['profit_ratio'] for t in lp_trades]) if lp_trades else 0
        lp_worst = min([t['profit_ratio'] for t in lp_trades]) if lp_trades else 0
        
        print("LOW PERFORMANCE KAUPAT:")
        print(f"Kauppoja yhteensä: {len(lp_trades)}")
        print(f"Voitollisia: {len(lp_profitable)} ({len(lp_profitable)/len(lp_trades)*100:.1f}%)")
        print(f"Tappiollisia: {len(lp_losing)}")
        print(f"Kokonaistuotto: {lp_total_profit:.2f} SOL")
        print(f"Keskimääräinen tuotto: {lp_avg_profit:.3f} SOL per kauppa")
        print(f"Paras kauppa: {lp_best:.2f}X")
        print(f"Huonoin kauppa: {lp_worst:.2f}X")
        print()
        
        # Analysoi Force Sell -kaupat
        force_profitable = [t for t in force_trades if t['profit_ratio'] > 1.0]
        force_losing = [t for t in force_trades if t['profit_ratio'] <= 1.0]
        force_total_profit = sum([t['profit_ratio'] - 1.0 for t in force_trades])
        force_avg_profit = force_total_profit / len(force_trades) if force_trades else 0
        force_best = max([t['profit_ratio'] for t in force_trades]) if force_trades else 0
        force_worst = min([t['profit_ratio'] for t in force_trades]) if force_trades else 0
        
        print("FORCE SELL KAUPAT:")
        print(f"Kauppoja yhteensä: {len(force_trades)}")
        print(f"Voitollisia: {len(force_profitable)} ({len(force_profitable)/len(force_trades)*100:.1f}%)")
        print(f"Tappiollisia: {len(force_losing)}")
        print(f"Kokonaistuotto: {force_total_profit:.2f} SOL")
        print(f"Keskimääräinen tuotto: {force_avg_profit:.3f} SOL per kauppa")
        print(f"Paras kauppa: {force_best:.2f}X")
        print(f"Huonoin kauppa: {force_worst:.2f}X")
    
    # Luo metriikat
    metrics = {
        'total_trades': total_trades,
        'profitable_trades': len(profitable_trades),
        'losing_trades': len(losing_trades),
        'win_rate': win_rate,
        'total_profit': total_profit_ratio,
        'avg_profit_per_trade': avg_profit_per_trade,
        'max_drawdown': max_drawdown,
        'avg_hold_time': avg_hold_time,
        'stoploss_trades': len(stoploss_trades),
        'tp_trades': len(tp_trades),
        'lp_trades': len(lp_trades),
        'force_trades': len(force_trades)
    }
    
    return metrics

def save_results(trades, metrics, timestamp, params=None, is_grid_test=False):
    """Tallenna tulokset tiedostoon.
    
    Args:
        trades: Lista kaupoista
        metrics: Lasketut metriikat
        timestamp: Aikaleima
        params: Simulaattorin parametrit (valinnainen)
        is_grid_test: Onko kyseessä grid-testaus
        
    Returns:
        Dict: Metriikat parametreilla
    """
    # Määritä results-kansio
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    results_dir = os.path.join(base_dir, 'results')
    
    # Luo results kansio jos ei ole
    if not os.path.exists(results_dir):
        os.makedirs(results_dir)
    
    # Luo tiedostonimi
    if params and 'momentum_params' in params and params['momentum_params']:
        # Momentum-parametrit
        mp = params['momentum_params']
        filename = f"momentum_tp{params['base_take_profit']}_sl{params['stop_loss']}_ts{params['trailing_stop']}" + \
                  f"_mc{mp['mc_change_threshold']}_hc{mp['holder_change_threshold']}" + \
                  f"_bv{mp['buy_volume_threshold']}_nv{mp['net_volume_threshold']}" + \
                  f"_rs{mp['required_strong']}_{timestamp}"
    else:
        # Perusparametrit
        filename = f"tp{params['base_take_profit']}_sl{params['stop_loss']}_ts{params['trailing_stop']}_{timestamp}" if params else f"results_{timestamp}"
    
    # Tallenna kaupat CSV-tiedostoon
    trades_df = pd.DataFrame(trades)
    trades_file = os.path.join(results_dir, f"{filename}.csv")
    trades_df.to_csv(trades_file, index=False)
    
    # Tallenna metriikat JSON-tiedostoon
    metrics_file = os.path.join(results_dir, f"metrics_{filename}.json")
    
    # Lisää parametrit metriikoihin
    metrics_with_params = {
        'parameters': params,
        'total_trades': metrics['total_trades'],
        'win_rate': metrics['win_rate'],
        'total_profit': metrics['total_profit'],
        'avg_profit': metrics['avg_profit_per_trade'],
        'max_drawdown': metrics['max_drawdown'],
        'avg_hold_time': metrics['avg_hold_time'],
        'stoploss_trades': metrics['stoploss_trades'],
        'tp_trades': metrics['tp_trades'],
        'lp_trades': metrics['lp_trades'],
        'force_trades': metrics['force_trades']
    }
    
    # Tallenna metriikat
    with open(metrics_file, 'w') as f:
        json.dump(metrics_with_params, f, indent=4)
    
    # Näytä tulokset jos ei ole grid-testaus
    if not is_grid_test:
        # Näytä backtesting-tulokset
        print("\n=== BACKTESTING TULOKSET ===")
        if params:
            print(f"Parametrit: TP={params['base_take_profit']}, SL={params['stop_loss']}, TS={params['trailing_stop']}")
            
            # Näytä momentum-parametrit jos ne ovat olemassa
            if 'momentum_params' in params and params['momentum_params']:
                mp = params['momentum_params']
                print(f"Momentum-parametrit:")
                print(f"  MC muutos kynnys: {mp['mc_change_threshold']}")
                print(f"  Holder muutos kynnys: {mp['holder_change_threshold']}")
                print(f"  Ostovolyymi kynnys: {mp['buy_volume_threshold']}")
                print(f"  Nettovolyymi kynnys: {mp['net_volume_threshold']}")
                print(f"  Vaaditut vahvat signaalit: {mp['required_strong']}")
                
                # Näytä Low Performance -parametrit jos ne ovat olemassa
                if 'lp_holder_growth_threshold' in mp:
                    print(f"Low Performance -parametrit:")
                    print(f"  Holder kasvu kynnys: {mp['lp_holder_growth_threshold']}")
        
        # Näytä kaupankäynnin tulokset
        print("\n=== KAUPANKÄYNNIN TULOKSET ===")
        print(f"Kauppoja yhteensä: {metrics['total_trades']}")
        print(f"Voitollisia kauppoja: {metrics['profitable_trades']} ({metrics['win_rate']*100:.1f}%)")
        print(f"Kokonaistuotto: {metrics['total_profit']:.2f} SOL")
        print(f"Keskimääräinen tuotto per kauppa: {metrics['avg_profit_per_trade']:.3f} SOL")
        print(f"Maksimi drawdown: {metrics['max_drawdown']*100:.1f}%")
        print(f"Keskimääräinen pitoaika: {metrics['avg_hold_time']:.1f} minuuttia")
        
        # Näytä myyntityypit
        print("\n=== MYYNTITYYPIT ===")
        print(f"Stop Loss -kauppoja: {metrics['stoploss_trades']} ({metrics['stoploss_trades']/metrics['total_trades']*100:.1f}%)")
        print(f"Take Profit -kauppoja: {metrics['tp_trades']} ({metrics['tp_trades']/metrics['total_trades']*100:.1f}%)")
        print(f"Low Performance -kauppoja: {metrics['lp_trades']} ({metrics['lp_trades']/metrics['total_trades']*100:.1f}%)")
        print(f"Force Sell -kauppoja: {metrics['force_trades']} ({metrics['force_trades']/metrics['total_trades']*100:.1f}%)")
        
        # Näytä tiedostojen sijainnit
        print("\nTulokset tallennettu:")
        print(f"- Kaupat: {trades_file}")
        print(f"- Metriikat: {metrics_file}")
    
    return metrics_with_params

def save_grid_test_summary(grid_results, timestamp):
    """Tallenna grid-testauksen yhteenveto.
    
    Args:
        grid_results: Lista grid-testauksen tuloksista
        timestamp: Aikaleima
    """
    # Määritä results-kansio
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    results_dir = os.path.join(base_dir, 'results')
    
    # Luo results kansio jos ei ole
    if not os.path.exists(results_dir):
        os.makedirs(results_dir)
    
    # Luo tiedostonimi
    filename = f"momentum_test_summary_{timestamp}.csv"
    
    # Luo DataFrame grid-testauksen tuloksista
    results_df = pd.DataFrame([{
        'take_profit': r['parameters']['base_take_profit'],
        'stop_loss': r['parameters']['stop_loss'],
        'trailing_stop': r['parameters']['trailing_stop'],
        'mc_change_threshold': r['parameters']['momentum_params']['mc_change_threshold'] if 'momentum_params' in r['parameters'] else 0,
        'holder_change_threshold': r['parameters']['momentum_params']['holder_change_threshold'] if 'momentum_params' in r['parameters'] else 0,
        'buy_volume_threshold': r['parameters']['momentum_params']['buy_volume_threshold'] if 'momentum_params' in r['parameters'] else 0,
        'net_volume_threshold': r['parameters']['momentum_params']['net_volume_threshold'] if 'momentum_params' in r['parameters'] else 0,
        'required_strong': r['parameters']['momentum_params']['required_strong'] if 'momentum_params' in r['parameters'] else 0,
        'lp_holder_growth_threshold': r['parameters']['momentum_params'].get('lp_holder_growth_threshold', 0) if 'momentum_params' in r['parameters'] else 0,
        'total_trades': r['total_trades'],
        'win_rate': r['win_rate'],
        'total_profit': r['total_profit'],
        'avg_profit': r['avg_profit'],
        'max_drawdown': r['max_drawdown'],
        'avg_hold_time': r['avg_hold_time'],
        'stoploss_trades': r['stoploss_trades'],
        'tp_trades': r['tp_trades'],
        'lp_trades': r['lp_trades'],
        'force_trades': r['force_trades']
    } for r in grid_results])
    
    # Tallenna DataFrame CSV-tiedostoon
    results_df.to_csv(os.path.join(results_dir, filename), index=False)
    
    # Näytä momentum-testauksen parhaat tulokset
    print("\n\n=== MOMENTUM-TESTAUKSEN PARHAAT TULOKSET ===")
    print("Top 5 parametriyhdistelmää kokonaistuoton mukaan:")
    
    # Järjestä tulokset kokonaistuoton mukaan
    sorted_results = sorted(grid_results, key=lambda x: x['total_profit'], reverse=True)
    
    # Näytä top 5 tulosta
    for i, result in enumerate(sorted_results[:5]):
        print(f"{i+1}. Momentum-parametrit:")
        print(f"   MC muutos kynnys: {result['parameters']['momentum_params']['mc_change_threshold']}")
        print(f"   Holder muutos kynnys: {result['parameters']['momentum_params']['holder_change_threshold']}")
        print(f"   Ostovolyymi kynnys: {result['parameters']['momentum_params']['buy_volume_threshold']}")
        print(f"   Nettovolyymi kynnys: {result['parameters']['momentum_params']['net_volume_threshold']}")
        print(f"   Vaaditut vahvat signaalit: {result['parameters']['momentum_params']['required_strong']}")
        print(f"   Low Performance -parametrit:")
        print(f"   Holder kasvu kynnys: {result['parameters']['momentum_params'].get('lp_holder_growth_threshold', 0)}")
        print(f"   Kokonaistuotto: {result['total_profit']:.2f} SOL")
        print(f"   Kauppoja: {result['total_trades']}")
        print(f"   Voittoaste: {result['win_rate']*100:.1f}%")
        print()
    
    # Näytä tiedoston sijainti
    print(f"Kaikki momentum-testauksen tulokset tallennettu: {os.path.join(results_dir, filename)}")

def process_opportunity_batch(opportunity_files, simulator_params, results_queue, progress_queue):
    """Käsittele erä ostomahdollisuuksia erillisessä prosessissa"""
    try:
        # Luo simulator
        simulator = SellSimulator(
            initial_investment=simulator_params['initial_investment'],
            base_take_profit=simulator_params['base_take_profit'],
            stop_loss=simulator_params['stop_loss'],
            trailing_stop=simulator_params['trailing_stop'],
            stoploss_params=simulator_params['stoploss_params'],
            momentum_params=simulator_params['momentum_params']
        )
        
        # Käsittele jokainen ostomahdollisuus
        for i, opportunity_file in enumerate(opportunity_files):
            try:
                # Ilmoita prosessin alku
                progress_queue.put(f"START:{opportunity_file}")
                
                # Lataa ostomahdollisuus
                buy_opportunity = pd.read_pickle(opportunity_file)
                
                # Simuloi myynti
                trade_result = simulator.simulate_sell(buy_opportunity)
                
                # Jos kauppa onnistui, lähetä se tuloksiin
                if trade_result:
                    results_queue.put(trade_result)
                    progress_queue.put(f"SUCCESS:{opportunity_file}")
                else:
                    progress_queue.put(f"FAIL:{opportunity_file}:ei kauppatulosta")
                    
            except Exception as e:
                progress_queue.put(f"ERROR:{opportunity_file}:{str(e)}")
                continue
    
    except Exception as e:
        progress_queue.put(f"BATCH_ERROR:{str(e)}")

def run_sell_simulation(summary_file=None, grid_test=False, param_combinations=None, timestamp=None):
    """Suorita myyntisimulaatio kaikille ostomahdollisuuksille.
    
    Args:
        summary_file: Yhteenvetotiedosto (valinnainen)
        grid_test: Suorita grid-testaus (valinnainen)
        param_combinations: Parametriyhdistelmät (valinnainen)
        timestamp: Aikaleima (valinnainen)
    """
    # Tarkista argumentit
    if len(sys.argv) > 1:
        if sys.argv[1] == '--momentum':
            print("Käynnistän momentum-testin löytääksemme parhaat parametrit momentum-parametreille")
            grid_test = True
            param_combinations = create_momentum_test_combinations()
        elif sys.argv[1] == '--fine-tune':
            print("Käynnistän hienosäätötestin löytääksemme parhaat parametrit")
            grid_test = True
            param_combinations = create_fine_tuning_combinations()
        elif sys.argv[1] == '--summary' and len(sys.argv) > 2:
            summary_file = sys.argv[2]
    
    # Määritä projektin juurihakemisto
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    
    # Luo kansio tuloksille
    results_dir = os.path.join(base_dir, 'results')
    if not os.path.exists(results_dir):
        os.makedirs(results_dir)
    
    # Luo aikaleima
    if timestamp is None:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # Etsi ostomahdollisuudet
    buy_opportunities_dir = os.path.join(base_dir, 'buy_opportunities')
    
    # Jos yhteenvetotiedosto on annettu, käytä vain siinä mainittuja pooleja
    if summary_file:
        summary_path = os.path.join(buy_opportunities_dir, summary_file)
        summary_df = pd.read_csv(summary_path)
        pool_addresses = summary_df['pool_address'].tolist()
        
        # Etsi vastaavat tiedostot
        opportunity_files = []
        for file in os.listdir(buy_opportunities_dir):
            if file.endswith('.pkl'):
                for pool in pool_addresses:
                    if file.startswith(pool):
                        opportunity_files.append(os.path.join(buy_opportunities_dir, file))
                        break
    else:
        # Käytä kaikkia .pkl-tiedostoja
        opportunity_files = [os.path.join(buy_opportunities_dir, f) for f in os.listdir(buy_opportunities_dir) 
                           if f.endswith('.pkl')]
    
    # Tarkista että tiedostoja löytyi
    if not opportunity_files:
        print("Ei löydetty ostomahdollisuuksia!")
        return
    
    print(f"\nLöydettiin {len(opportunity_files)} ostomahdollisuutta")
    
    # Jos grid-testaus, suorita simulaatio eri parametriyhdistelmillä
    if grid_test and param_combinations:
        # Luo jaettu lista tuloksille
        manager = Manager()
        grid_results = manager.list()
        
        # Suorita simulaatio jokaisella parametriyhdistelmällä
        for i, params in enumerate(param_combinations):
            print(f"\nTestataan parametriyhdistelmää {i+1}/{len(param_combinations)}")
            
            # Luo simulator parametrit
            simulator_params = {
                'initial_investment': 1.0
            }
            
            # Lisää kaupankäyntiparametrit
            if 'base_take_profit' in params:
                simulator_params['base_take_profit'] = params['base_take_profit']
            else:
                simulator_params['base_take_profit'] = 1.9  # Optimaalinen TP
                
            if 'stop_loss' in params:
                simulator_params['stop_loss'] = params['stop_loss']
            else:
                simulator_params['stop_loss'] = 0.65  # Optimaalinen SL
                
            if 'trailing_stop' in params:
                simulator_params['trailing_stop'] = params['trailing_stop']
            else:
                simulator_params['trailing_stop'] = 0.9  # Optimaalinen TS
            
            # Lisää stoploss-parametrit
            simulator_params['stoploss_params'] = create_stoploss_params()
            
            # Lisää momentum-parametrit
            if 'momentum_params' in params:
                simulator_params['momentum_params'] = params['momentum_params']
            else:
                # Käytä optimaalisia momentum-parametreja
                simulator_params['momentum_params'] = {
                    'mc_change_threshold': 6.0,
                    'holder_change_threshold': 24.5,
                    'buy_volume_threshold': 13.0,
                    'net_volume_threshold': 3.0,
                    'required_strong': 1.0,
                    'lp_holder_growth_threshold': 0.0
                }
            
            # Suorita simulaatio tällä parametriyhdistelmällä
            run_simulation_with_params(opportunity_files, simulator_params, grid_test, grid_results, timestamp, i, len(param_combinations))
        
        # Tallenna grid-testin yhteenveto
        save_grid_test_summary(grid_results, timestamp)
        
    else:
        # Suorita normaali simulaatio optimaalisilla parametreilla
        simulator_params = {
            'initial_investment': 1.0,
            'base_take_profit': 1.9,    # Optimaalinen TP
            'stop_loss': 0.65,          # Optimaalinen SL
            'trailing_stop': 0.9,       # Optimaalinen TS
            'stoploss_params': create_stoploss_params(),
            'momentum_params': {
                'mc_change_threshold': 6.0,           # Optimaalinen MC muutos kynnys
                'holder_change_threshold': 24.5,      # Optimaalinen holder muutos kynnys
                'buy_volume_threshold': 13.0,         # Optimaalinen ostovolyymi kynnys
                'net_volume_threshold': 3.0,          # Optimaalinen nettovolyymi kynnys
                'required_strong': 1.0,               # Optimaalinen vaaditut vahvat signaalit
                'lp_holder_growth_threshold': 0.0     # Optimaalinen holder kasvu kynnys
            }
        }
        
        # Suorita simulaatio
        run_simulation_with_params(opportunity_files, simulator_params, False, None, timestamp)

def run_simulation_with_params(opportunity_files, simulator_params, grid_test, grid_results, timestamp, param_idx=0, total_params=1):
    """Suorita simulaatio tietyillä parametreilla.
    
    Args:
        opportunity_files: Lista ostomahdollisuustiedostoista
        simulator_params: Simulaattorin parametrit
        grid_test: Onko kyseessä grid-testaus
        grid_results: Lista grid-testauksen tuloksista
        timestamp: Aikaleima
        param_idx: Parametriyhdistelmän indeksi
        total_params: Parametriyhdistelmien kokonaismäärä
        
    Returns:
        Tuple: (trades, metrics) - kaupat ja metriikat
    """
    # Määritä kokonaismahdollisuuksien määrä
    total_opportunities = len(opportunity_files)
    
    # Tulosta parametrit
    if grid_test:
        print(f"\n=== PARAMETRIYHDISTELMÄ {param_idx+1}/{total_params} ===")
        print(f"Take Profit: {simulator_params['base_take_profit']}")
        print(f"Stop Loss: {simulator_params['stop_loss']}")
        print(f"Trailing Stop: {simulator_params['trailing_stop']}")
        
        # Tulosta momentum-parametrit jos ne ovat olemassa
        if 'momentum_params' in simulator_params and simulator_params['momentum_params']:
            mp = simulator_params['momentum_params']
            print(f"Momentum-parametrit:")
            print(f"  MC muutos kynnys: {mp['mc_change_threshold']}")
            print(f"  Holder muutos kynnys: {mp['holder_change_threshold']}")
            print(f"  Ostovolyymi kynnys: {mp['buy_volume_threshold']}")
            print(f"  Nettovolyymi kynnys: {mp['net_volume_threshold']}")
            print(f"  Vaaditut vahvat signaalit: {mp['required_strong']}")
            
            # Näytä Low Performance -parametrit jos ne ovat olemassa
            if 'lp_holder_growth_threshold' in mp:
                print(f"Low Performance -parametrit:")
                print(f"  Holder kasvu kynnys: {mp['lp_holder_growth_threshold']}")
    
    # Määritä prosessorien määrä (käytä kaikki saatavilla olevat ytimet)
    num_processes = cpu_count()
    print(f"Käytetään {num_processes} prosessoria")
    
    # Jaa ostomahdollisuudet tasaisesti prosesseille
    batch_size = math.ceil(len(opportunity_files) / num_processes)
    opportunity_batches = [opportunity_files[i:i + batch_size] for i in range(0, len(opportunity_files), batch_size)]
    
    print(f"Jaettu {total_opportunities} ostomahdollisuutta {len(opportunity_batches)} erään, noin {batch_size} per erä")
    
    # Luo jaetut jonot tulosten ja edistymisen seurantaan
    manager = Manager()
    results_queue = manager.Queue()
    progress_queue = manager.Queue()
    
    # Jaa ostomahdollisuudet prosesseille
    processes = []
    
    # Tallenna kauppatulokset - TÄRKEÄ: Nollataan kauppatulokset jokaiselle parametriyhdistelmälle
    trades = []
    
    # Aloitusaika
    start_time = time.time()
    
    # Käynnistä prosessit
    for i, batch in enumerate(opportunity_batches):
        p = Process(target=process_opportunity_batch, 
                   args=(batch, simulator_params, results_queue, progress_queue))
        p.start()
        processes.append(p)
        print(f"Käynnistetty prosessi {i+1}/{len(opportunity_batches)}, {len(batch)} ostomahdollisuutta")
    
    # Seuraa edistymistä
    opportunities_processed = 0
    opportunities_succeeded = 0
    opportunities_failed = 0
    opportunities_error = 0
    
    # Lisää myyntityyppien seuranta
    stoploss_trades = 0
    tp_trades = 0
    lp_trades = 0
    force_trades = 0
    
    try:
        # Seuraa edistymistä kunnes kaikki prosessit ovat valmiita
        while any(p.is_alive() for p in processes):
            try:
                # Tarkista edistymisjonosta
                while True:
                    try:
                        progress_msg = progress_queue.get_nowait()
                        
                        if progress_msg.startswith("START:"):
                            # Prosessi aloitti ostomahdollisuuden käsittelyn
                            pass
                        elif progress_msg.startswith("SUCCESS:"):
                            # Ostomahdollisuus käsitelty onnistuneesti
                            opportunities_succeeded += 1
                            opportunities_processed += 1
                        elif progress_msg.startswith("FAIL:"):
                            # Ostomahdollisuuden käsittely epäonnistui
                            opportunities_failed += 1
                            opportunities_processed += 1
                        elif progress_msg.startswith("ERROR:"):
                            # Virhe ostomahdollisuuden käsittelyssä
                            opportunities_error += 1
                            opportunities_processed += 1
                        elif progress_msg.startswith("BATCH_ERROR:"):
                            # Virhe erän käsittelyssä
                            _, error = progress_msg.split(":", 1)
                            print(f"\nVirhe erän käsittelyssä: {error}")
                    except Empty:
                        break
                
                # Tarkista tulokset
                while True:
                    try:
                        trade_result = results_queue.get_nowait()
                        trades.append(trade_result)
                        
                        # Päivitä myyntityyppien laskurit
                        if trade_result['exit_reason'] == 'Stop Loss':
                            stoploss_trades += 1
                        elif trade_result['exit_reason'] == 'Momentum Lost + Price Drop':
                            tp_trades += 1
                        elif trade_result['exit_reason'] == 'Low Performance':
                            lp_trades += 1
                        elif trade_result['exit_reason'] == 'Force Sell':
                            force_trades += 1
                    except Empty:
                        break
                
                # Näytä edistyminen
                elapsed_time = time.time() - start_time
                if opportunities_processed > 0:
                    avg_time_per_opportunity = elapsed_time / opportunities_processed
                    estimated_time_left = avg_time_per_opportunity * (total_opportunities - opportunities_processed)
                    
                    # Tarkista prosessorien käyttö
                    cpu_percent = psutil.cpu_percent()
                    memory_percent = psutil.virtual_memory().percent
                    
                    print(f"\rEdistyminen: {opportunities_processed}/{total_opportunities} ostomahdollisuutta käsitelty " +
                         f"({opportunities_processed/total_opportunities*100:.1f}%) | " +
                         f"Onnistuneet: {opportunities_succeeded} | Epäonnistuneet: {opportunities_failed} | " +
                         f"Virheet: {opportunities_error} | " +
                         f"Löydetty: {len(trades)} kauppaa | " +
                         f"CPU: {cpu_percent:.1f}% | RAM: {memory_percent:.1f}% | " +
                         f"Aikaa kulunut: {elapsed_time/60:.1f} min | " +
                         f"Arvioitu jäljellä: {estimated_time_left/60:.1f} min", end="")
                
                # Pieni tauko ennen seuraavaa tarkistusta
                time.sleep(0.1)
            except Exception as e:
                print(f"\nVirhe edistymisen seurannassa: {str(e)}")
                continue
    
    except KeyboardInterrupt:
        print("\nKeskeytys havaittu, lopetetaan prosessit...")
        for p in processes:
            if p.is_alive():
                p.terminate()
                p.join()
    
    # Odota että kaikki prosessit päättyvät
    for p in processes:
        p.join()
    
    print(f"\n\nSimulaatio valmis! Löydettiin {len(trades)} kauppaa.")
    
    # Laske metriikat
    if trades:
        # Laske metriikat
        metrics = calculate_metrics(trades, show_detailed_analysis=True)
        
        # Tallenna tulokset
        metrics_with_params = save_results(trades, metrics, timestamp, simulator_params, grid_test)
        
        # Lisää grid-testauksen tuloksiin
        if grid_test:
            grid_results.append(metrics_with_params)
        
        return trades, metrics
    else:
        print("Ei löydetty kauppoja!")
        return [], {}

if __name__ == "__main__":
    # Aseta multiprocessing käyttämään spawn-metodia macOS:llä
    if platform.system() == 'Darwin':
        mp.set_start_method('spawn')
    
    # Käsittele komentoriviparametrit
    parser = argparse.ArgumentParser(description='Suorita myyntisimulaatio')
    parser.add_argument('--summary', type=str, help='Yhteenvetotiedosto')
    parser.add_argument('--momentum', action='store_true', help='Suorita momentum-testaus')
    parser.add_argument('--fine-tune', action='store_true', help='Suorita hienosäätötestaus')
    args = parser.parse_args()
    
    # Suorita simulaatio
    if args.momentum:
        # Suorita momentum-testaus
        run_sell_simulation(summary_file=args.summary, grid_test=True)
    elif args.fine_tune:
        # Suorita hienosäätötestaus
        param_combinations = create_fine_tuning_combinations()
        run_sell_simulation(summary_file=args.summary, grid_test=True, param_combinations=param_combinations)
    else:
        # Suorita normaali simulaatio
        run_sell_simulation(summary_file=args.summary) 